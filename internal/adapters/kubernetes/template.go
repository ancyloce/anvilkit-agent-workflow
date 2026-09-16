package kubernetes

import (
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"

	"github.com/ancyloce/anvilkit-agent-contracts/go/jobschema"
	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
)

// Options are the environment placements of the launcher (DD-03 §4): the
// backend identity Control records, the registry that completes profile
// images naming none, the profiles this environment enabled, and what the
// harness template tells the access sidecar. Nothing in them changes an
// image digest, an entrypoint, a mount or a privilege of a template.
type Options struct {
	Backend                 string
	ImageRegistry           string
	EnabledProfiles         []string
	SidecarControlAddress   string
	SidecarIdentityMode     string
	CandidateSeccompProfile string
}

// Fixed identities of the harness layout (DD-03 §5).
const (
	labelCandidateCode   = "anvilkit.io/candidate-code"
	nodePoolLabel        = "anvilkit.io/pool"
	nodePoolComponents   = "components"
	containerSupervisor  = "supervisor"
	containerSidecar     = "access-sidecar"
	containerFixture     = "fixture"
	candidateUID         = int64(10001)
	sidecarUID           = int64(10002)
	workspaceMount       = "/workspace"
	verdictMount         = "/anvilkit/verdict"
	socketsMount         = "/run/anvilkit"
	socketsDir           = "/run/anvilkit/sockets"
	envLaunchID          = "ANVILKIT_LAUNCH_ID"
	envAttemptID         = "ANVILKIT_ATTEMPT_ID"
	envLaunchEnvelope    = "ANVILKIT_LAUNCH_ENVELOPE"
	envSidecarControl    = "ANVILKIT_SIDECAR_CONTROL_ADDRESS"
	envSidecarIdentity   = "ANVILKIT_SIDECAR_IDENTITY_MODE"
	envSidecarBackend    = "ANVILKIT_SIDECAR_BACKEND"
	envSidecarLaunchKey  = "ANVILKIT_SIDECAR_LAUNCH_KEY"
	envSidecarPodUID     = "ANVILKIT_SIDECAR_POD_UID"
	envSidecarEnvelope   = "ANVILKIT_SIDECAR_LAUNCH_ENVELOPE"
	sidecarCPU           = "100m"
	sidecarMemory        = "64Mi"
	workspaceSizeLimit   = "1Gi"
	verdictSizeLimit     = "256Mi"
	socketsSizeLimit     = "1Mi"
	launchEnvelopeSchema = 1
)

var (
	digestPattern    = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	inputNamePattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,63}$`)
	sequencePattern  = regexp.MustCompile(`^(0|[1-9][0-9]{0,19})$`)
)

// enabled reports whether the environment enabled the profile.
func (l *Launcher) enabled(profileID string) bool {
	return slices.Contains(l.options.EnabledProfiles, profileID)
}

// imageRef completes a profile image: a repository that names a registry
// (its first component has a dot or a colon, or is "localhost") is used
// as pinned; one without is prefixed with the environment's registry.
// The digest is the identity either way.
func (l *Launcher) imageRef(repository, digest string) (string, error) {
	first, _, _ := strings.Cut(repository, "/")
	if strings.ContainsAny(first, ".:") || first == "localhost" {
		return repository + "@" + digest, nil
	}
	if l.options.ImageRegistry == "" {
		return "", fmt.Errorf("profile image %s names no registry and this environment configures none (kubernetes.image_registry)", repository)
	}
	return strings.TrimSuffix(l.options.ImageRegistry, "/") + "/" + repository + "@" + digest, nil
}

// launchEnvelope renders the typed launch envelope of the jobs contract
// and validates it against the schema before it enters a template.
func launchEnvelope(in activities.CreateJobInput, profile jobschema.Profile) ([]byte, error) {
	for name, v := range map[string]string{"operation id": in.OperationID, "execution epoch": in.ExecutionEpoch, "launch epoch": in.LaunchEpoch} {
		if v == "" {
			return nil, fmt.Errorf("harness profile %s needs the %s in the launch envelope", profile.ProfileID, name)
		}
	}
	for _, e := range []string{in.ExecutionEpoch, in.LaunchEpoch, profile.Revision} {
		if !sequencePattern.MatchString(e) {
			return nil, fmt.Errorf("launch envelope sequence %q is not canonical", e)
		}
	}
	inputs := make([]map[string]string, 0, len(in.Inputs))
	seen := map[string]bool{}
	for _, i := range in.Inputs {
		if !inputNamePattern.MatchString(i.Name) || !digestPattern.MatchString(i.Digest) || seen[i.Name] || len(i.Handle) > 256 {
			return nil, fmt.Errorf("launch input %q is not a typed input (name, sha256 digest, optional handle)", i.Name)
		}
		seen[i.Name] = true
		entry := map[string]string{"name": i.Name, "digest": i.Digest}
		if i.Handle != "" {
			entry["handle"] = i.Handle
		}
		inputs = append(inputs, entry)
	}
	raw, err := json.Marshal(map[string]any{
		"schemaVersion": launchEnvelopeSchema, "launchId": in.Launch.LaunchID, "launchKey": in.Launch.LaunchKey, "operationId": in.OperationID,
		"attemptId": in.Launch.AttemptID, "profileId": profile.ProfileID, "profileRevision": profile.Revision, "jobKind": profile.JobKind,
		"executionEpoch": in.ExecutionEpoch, "launchEpoch": in.LaunchEpoch, "deadline": in.Deadline.UTC().Format("2006-01-02T15:04:05Z"), "inputs": inputs,
	})
	if err != nil {
		return nil, err
	}
	if err := jobschema.ValidateLaunchEnvelope(raw); err != nil {
		return nil, fmt.Errorf("launch envelope: %w", err)
	}
	return raw, nil
}

// harnessPodSpec renders the fixed two-container layout of a harness
// profile: the supervisor (UID 0 with SETUID, SETGID and SETPCAP only, the
// candidate syscall profile, a read-only root, the workspace and verdict
// volumes, the sockets read-only) and the access sidecar (UID 10002, no
// capability, supplementary groups 0 and 10001 through the Pod, the
// sockets volume writable, the Pod UID from the downward API). The sidecar
// is a Kubernetes native sidecar (an init container with restartPolicy
// Always): it starts before the supervisor and is stopped by the kubelet
// once the supervisor exited, so the Pod completes exactly when the
// trusted flow ends and nothing keeps a finished launch alive. No
// ServiceAccount token, no service links, no host namespace, the
// components node pool, and the RuntimeClass the profile pins.
func (l *Launcher) harnessPodSpec(in activities.CreateJobInput, profile jobschema.Profile, envelope []byte) (corev1.PodSpec, error) {
	image, err := l.imageRef(profile.Image.Repository, profile.Image.Digest)
	if err != nil {
		return corev1.PodSpec{}, err
	}
	sidecarImage, err := l.imageRef(profile.SidecarImage.Repository, profile.SidecarImage.Digest)
	if err != nil {
		return corev1.PodSpec{}, err
	}
	if l.options.SidecarIdentityMode == "" || l.options.SidecarIdentityMode == "disabled" {
		return corev1.PodSpec{}, fmt.Errorf("harness profile %s needs a sidecar identity for this environment (kubernetes.sidecar.identity_mode)", profile.ProfileID)
	}
	if l.options.SidecarControlAddress == "" || l.options.Backend == "" {
		return corev1.PodSpec{}, fmt.Errorf("harness profile %s needs the sidecar's Control address and the launch backend identity", profile.ProfileID)
	}
	if l.options.CandidateSeccompProfile == "" {
		return corev1.PodSpec{}, fmt.Errorf("harness profile %s needs the candidate syscall profile (kubernetes.candidate_seccomp_profile)", profile.ProfileID)
	}
	res := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(profile.Resources.CPU), corev1.ResourceMemory: resource.MustParse(profile.Resources.Memory)},
		Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(profile.Resources.CPU), corev1.ResourceMemory: resource.MustParse(profile.Resources.Memory)},
	}
	sidecarRes := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(sidecarCPU), corev1.ResourceMemory: resource.MustParse(sidecarMemory)},
		Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(sidecarCPU), corev1.ResourceMemory: resource.MustParse(sidecarMemory)},
	}
	supervisor := corev1.Container{
		Name: containerSupervisor, Image: image, Command: profile.Entrypoint,
		Env: []corev1.EnvVar{
			{Name: envLaunchID, Value: in.Launch.LaunchID}, {Name: envAttemptID, Value: in.Launch.AttemptID}, {Name: envLaunchEnvelope, Value: string(envelope)},
		},
		Resources: res,
		VolumeMounts: []corev1.VolumeMount{
			{Name: "workspace", MountPath: workspaceMount}, {Name: "verdict", MountPath: verdictMount}, {Name: "sockets", MountPath: socketsMount, ReadOnly: true},
		},
		TerminationMessagePolicy: corev1.TerminationMessageReadFile,
		SecurityContext: &corev1.SecurityContext{
			RunAsUser: ptr.To[int64](0), RunAsGroup: ptr.To[int64](0), RunAsNonRoot: ptr.To(false), Privileged: ptr.To(false),
			AllowPrivilegeEscalation: ptr.To(false), ReadOnlyRootFilesystem: ptr.To(true),
			Capabilities:   &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}, Add: []corev1.Capability{"SETUID", "SETGID", "SETPCAP"}},
			SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeLocalhost, LocalhostProfile: ptr.To(l.options.CandidateSeccompProfile)},
		},
	}
	sidecar := corev1.Container{
		Name: containerSidecar, Image: sidecarImage, RestartPolicy: ptr.To(corev1.ContainerRestartPolicyAlways),
		Env: []corev1.EnvVar{
			{Name: envSidecarControl, Value: l.options.SidecarControlAddress}, {Name: envSidecarIdentity, Value: l.options.SidecarIdentityMode},
			{Name: envSidecarBackend, Value: l.options.Backend}, {Name: envSidecarLaunchKey, Value: in.Launch.LaunchKey},
			{Name: envSidecarPodUID, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.uid"}}},
			{Name: envSidecarEnvelope, Value: string(envelope)},
		},
		Resources:                sidecarRes,
		VolumeMounts:             []corev1.VolumeMount{{Name: "sockets", MountPath: socketsMount}},
		TerminationMessagePolicy: corev1.TerminationMessageReadFile,
		SecurityContext: &corev1.SecurityContext{
			RunAsUser: ptr.To(sidecarUID), RunAsGroup: ptr.To(sidecarUID), RunAsNonRoot: ptr.To(true), Privileged: ptr.To(false),
			AllowPrivilegeEscalation: ptr.To(false), ReadOnlyRootFilesystem: ptr.To(true),
			Capabilities:   &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
			SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
		},
	}
	spec := corev1.PodSpec{
		RestartPolicy:                corev1.RestartPolicyNever,
		AutomountServiceAccountToken: ptr.To(false),
		EnableServiceLinks:           ptr.To(false),
		HostNetwork:                  false,
		HostPID:                      false,
		HostIPC:                      false,
		ShareProcessNamespace:        ptr.To(false),
		NodeSelector:                 map[string]string{nodePoolLabel: nodePoolComponents},
		SecurityContext:              &corev1.PodSecurityContext{SupplementalGroups: []int64{0, candidateUID}},
		InitContainers:               []corev1.Container{sidecar},
		Containers:                   []corev1.Container{supervisor},
		Volumes: []corev1.Volume{
			{Name: "workspace", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: ptr.To(resource.MustParse(workspaceSizeLimit))}}},
			{Name: "verdict", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: ptr.To(resource.MustParse(verdictSizeLimit))}}},
			{Name: "sockets", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory, SizeLimit: ptr.To(resource.MustParse(socketsSizeLimit))}}},
		},
	}
	if profile.RuntimeClass != "" {
		spec.RuntimeClassName = ptr.To(profile.RuntimeClass)
	}
	return spec, nil
}

// fixturePodSpec is the single-container layout of a fixture profile
// (P05's LocalCheck): no candidate code, no sidecar, no privilege.
func (l *Launcher) fixturePodSpec(in activities.CreateJobInput, profile jobschema.Profile) (corev1.PodSpec, error) {
	image, err := l.imageRef(profile.Image.Repository, profile.Image.Digest)
	if err != nil {
		return corev1.PodSpec{}, err
	}
	container := corev1.Container{
		Name:    containerFixture,
		Image:   image,
		Command: profile.Entrypoint,
		Env: []corev1.EnvVar{
			{Name: envLaunchID, Value: in.Launch.LaunchID},
			{Name: envAttemptID, Value: in.Launch.AttemptID},
		},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(profile.Resources.CPU), corev1.ResourceMemory: resource.MustParse(profile.Resources.Memory)},
			Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(profile.Resources.CPU), corev1.ResourceMemory: resource.MustParse(profile.Resources.Memory)},
		},
		TerminationMessagePolicy: corev1.TerminationMessageReadFile,
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: ptr.To(false),
			ReadOnlyRootFilesystem:   ptr.To(true),
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
			SeccompProfile:           &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
		},
	}
	spec := corev1.PodSpec{
		RestartPolicy:                corev1.RestartPolicyNever,
		AutomountServiceAccountToken: ptr.To(false),
		EnableServiceLinks:           ptr.To(false),
		Containers:                   []corev1.Container{container},
	}
	if profile.RuntimeClass != "" {
		spec.RuntimeClassName = ptr.To(profile.RuntimeClass)
	}
	return spec, nil
}

// remainingDeadline bounds the Job by the launch deadline and the
// profile's own bound.
func remainingDeadline(in activities.CreateJobInput, profile jobschema.Profile, now time.Time) int64 {
	remaining := int64(in.Deadline.Sub(now).Seconds())
	if remaining < 1 {
		remaining = 1
	}
	if int64(profile.DeadlineSeconds) < remaining {
		remaining = int64(profile.DeadlineSeconds)
	}
	return remaining
}

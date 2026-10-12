package kubernetes_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"

	"github.com/ancyloce/anvilkit-agent-contracts/go/jobschema"
	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
	k8s "github.com/ancyloce/anvilkit-agent-workflow/internal/adapters/kubernetes"
)

const harnessProfile = "harness-wiring-dev-v1"

func harnessOptions(enabled ...string) k8s.Options {
	return k8s.Options{
		Backend: "kind-anvilkit-dev", ImageRegistry: "localhost:5001", EnabledProfiles: enabled,
		SidecarControlAddress: "172.22.0.1:9101", SidecarIdentityMode: "mtls", SidecarIdentitySecret: "anvilkit-job-access-sidecar-identity", CandidateSeccompProfile: "anvilkit/candidate.json",
		// The wiring profile is in the development state (P0.8).
		Development: true,
	}
}

func harnessInput(t *testing.T) activities.CreateJobInput {
	t.Helper()
	profile, err := jobschema.ProfileByID(harnessProfile)
	require.NoError(t, err)
	return activities.CreateJobInput{
		Launch:    activities.LaunchRef{LaunchID: "lch_h1", AttemptID: "att_h1", LaunchKey: "cg-h1", ImageDigest: profile.Image.Digest},
		ProfileID: harnessProfile, Deadline: time.Now().Add(5 * time.Minute), Request: 1,
		OperationID: "op_h1", ExecutionEpoch: "1", LaunchEpoch: "1",
		Inputs: []activities.LaunchInput{{Name: "fixed-input", Digest: "sha256:abeb263b000189efdd8206ff39eeb4f2f7f3217f1c480491868b46da4a21c6a8"}},
	}
}

// The harness template is fixed: two containers with exactly the reviewed
// images, entrypoint, privileges, mounts and environment; no token, no
// host namespace, the components node pool, and the RuntimeClass the
// profile pins. The request contributed identities, inputs and a deadline
// only.
func TestHarnessTemplateIsFixed(t *testing.T) {
	l := k8s.NewWithClient(fake.NewClientset(), namespace, harnessOptions(harnessProfile))
	in := harnessInput(t)
	job, err := l.Render(in, time.Now())
	require.NoError(t, err)
	profile, _ := jobschema.ProfileByID(harnessProfile)
	require.Equal(t, int32(0), *job.Spec.BackoffLimit)
	require.Equal(t, int32(1), *job.Spec.Completions)
	require.Equal(t, int32(1), *job.Spec.Parallelism)
	require.LessOrEqual(t, *job.Spec.ActiveDeadlineSeconds, int64(300))
	require.Equal(t, "false", job.Spec.Template.Labels["anvilkit.io/candidate-code"])
	spec := job.Spec.Template.Spec
	require.Equal(t, corev1.RestartPolicyNever, spec.RestartPolicy)
	require.False(t, *spec.AutomountServiceAccountToken)
	require.False(t, *spec.EnableServiceLinks)
	require.False(t, spec.HostNetwork)
	require.False(t, spec.HostPID)
	require.False(t, spec.HostIPC)
	require.Nil(t, spec.RuntimeClassName, "the development wiring profile pins no RuntimeClass")
	require.Equal(t, map[string]string{"anvilkit.io/pool": "components"}, spec.NodeSelector)
	require.Equal(t, []int64{0, 10001}, spec.SecurityContext.SupplementalGroups)
	require.Len(t, spec.Containers, 1)
	require.Len(t, spec.InitContainers, 1)
	sup, side := spec.Containers[0], spec.InitContainers[0]
	require.Equal(t, "supervisor", sup.Name)
	require.Equal(t, corev1.ContainerRestartPolicyAlways, *side.RestartPolicy, "the sidecar is a native sidecar: started first, stopped when the supervisor exits")
	require.Equal(t, "localhost:5001/"+profile.Image.Repository+"@"+profile.Image.Digest, sup.Image, "the registry completes the repository, the digest is the profile's")
	require.Equal(t, profile.Entrypoint, sup.Command)
	require.Empty(t, sup.Args)
	require.Equal(t, int64(0), *sup.SecurityContext.RunAsUser)
	require.False(t, *sup.SecurityContext.AllowPrivilegeEscalation)
	require.True(t, *sup.SecurityContext.ReadOnlyRootFilesystem)
	require.Equal(t, []corev1.Capability{"ALL"}, sup.SecurityContext.Capabilities.Drop)
	require.Equal(t, []corev1.Capability{"SETUID", "SETGID", "SETPCAP"}, sup.SecurityContext.Capabilities.Add)
	require.Equal(t, corev1.SeccompProfileTypeLocalhost, sup.SecurityContext.SeccompProfile.Type)
	require.Equal(t, "anvilkit/candidate.json", *sup.SecurityContext.SeccompProfile.LocalhostProfile)
	mounts := map[string]bool{}
	for _, m := range sup.VolumeMounts {
		mounts[m.MountPath] = m.ReadOnly
	}
	require.Equal(t, map[string]bool{"/workspace": false, "/anvilkit/verdict": false, "/run/anvilkit": true}, mounts)
	names := map[string]string{}
	for _, e := range sup.Env {
		names[e.Name] = e.Value
	}
	require.Equal(t, []string{"ANVILKIT_ATTEMPT_ID", "ANVILKIT_LAUNCH_ENVELOPE", "ANVILKIT_LAUNCH_ID"}, sortedKeys(names))
	var envelope map[string]any
	require.NoError(t, json.Unmarshal([]byte(names["ANVILKIT_LAUNCH_ENVELOPE"]), &envelope))
	require.Equal(t, "op_h1", envelope["operationId"])
	require.Equal(t, "cg-h1", envelope["launchKey"])
	require.Equal(t, "codegen", envelope["jobKind"])
	require.Equal(t, "fixed-input", envelope["inputs"].([]any)[0].(map[string]any)["name"])

	require.Equal(t, "access-sidecar", side.Name)
	require.Equal(t, "localhost:5001/"+profile.SidecarImage.Repository+"@"+profile.SidecarImage.Digest, side.Image)
	require.Empty(t, side.Command)
	require.Equal(t, int64(10002), *side.SecurityContext.RunAsUser)
	require.True(t, *side.SecurityContext.RunAsNonRoot)
	require.Equal(t, []corev1.Capability{"ALL"}, side.SecurityContext.Capabilities.Drop)
	require.Empty(t, side.SecurityContext.Capabilities.Add)
	require.Len(t, side.VolumeMounts, 2)
	require.Equal(t, "/run/anvilkit", side.VolumeMounts[0].MountPath)
	require.False(t, side.VolumeMounts[0].ReadOnly)
	require.Equal(t, "identity", side.VolumeMounts[1].Name)
	require.Equal(t, "/etc/anvilkit/identity", side.VolumeMounts[1].MountPath)
	require.True(t, side.VolumeMounts[1].ReadOnly, "the sidecar's certificate is read-only in the sidecar alone")
	sideEnv := map[string]corev1.EnvVar{}
	for _, e := range side.Env {
		sideEnv[e.Name] = e
	}
	require.Equal(t, "metadata.uid", sideEnv["ANVILKIT_SIDECAR_POD_UID"].ValueFrom.FieldRef.FieldPath, "the Pod UID comes from the kubelet, never from a value")
	require.Equal(t, "172.22.0.1:9101", sideEnv["ANVILKIT_SIDECAR_CONTROL_ADDRESS"].Value)
	require.Equal(t, "mtls", sideEnv["ANVILKIT_SIDECAR_IDENTITY_MODE"].Value)
	require.Equal(t, "kind-anvilkit-dev", sideEnv["ANVILKIT_SIDECAR_BACKEND"].Value)
	require.Equal(t, "cg-h1", sideEnv["ANVILKIT_SIDECAR_LAUNCH_KEY"].Value)
	require.Equal(t, names["ANVILKIT_LAUNCH_ENVELOPE"], sideEnv["ANVILKIT_SIDECAR_LAUNCH_ENVELOPE"].Value)
	require.Len(t, spec.Volumes, 4)
	for _, v := range spec.Volumes[:3] {
		require.NotNil(t, v.EmptyDir, "only emptyDir volumes beside the identity Secret: %s", v.Name)
	}
	identity := spec.Volumes[3]
	require.Equal(t, "identity", identity.Name)
	require.NotNil(t, identity.Secret)
	require.Equal(t, "anvilkit-job-access-sidecar-identity", identity.Secret.SecretName)
	require.Equal(t, int32(0o440), *identity.Secret.DefaultMode)
	for _, m := range sup.VolumeMounts {
		require.NotEqual(t, "identity", m.Name, "the supervisor never mounts the sidecar's certificate")
	}
}

// Under the development sidecar identity (plaintext, DEVELOPMENT_ONLY) no
// Secret enters the Pod; under mtls the Secret name is required.
func TestHarnessSidecarIdentityVolume(t *testing.T) {
	in := harnessInput(t)
	dev := harnessOptions(harnessProfile)
	dev.SidecarIdentityMode, dev.SidecarIdentitySecret = "development", ""
	l := k8s.NewWithClient(fake.NewClientset(), namespace, dev)
	job, err := l.Render(in, time.Now())
	require.NoError(t, err)
	require.Len(t, job.Spec.Template.Spec.Volumes, 3)
	require.Len(t, job.Spec.Template.Spec.InitContainers[0].VolumeMounts, 1)
	noSecret := harnessOptions(harnessProfile)
	noSecret.SidecarIdentitySecret = ""
	_, err = k8s.NewWithClient(fake.NewClientset(), namespace, noSecret).CreateJob(context.Background(), in)
	require.Equal(t, "PROFILE_UNQUALIFIED", activities.RefusalCode(err))
	require.Contains(t, err.Error(), "identity_secret")
}

// P0.8: a profile's state is enforced before the environment's allowlist:
// disabled everywhere, development only behind the worker's guard.
func TestProfileStateIsEnforced(t *testing.T) {
	source, err := jobschema.ProfileByID("validator-source-v1")
	require.NoError(t, err)
	require.Equal(t, jobschema.StateDisabled, source.State)
	in := sourceInput(t, "validator-source-v1")
	_, err = k8s.NewWithClient(fake.NewClientset(), namespace, harnessOptions("validator-source-v1")).CreateJob(context.Background(), in)
	require.Equal(t, "PROFILE_DISABLED", activities.RefusalCode(err))

	dev := sourceInput(t, "validator-source-dev-v1")
	noGuard := harnessOptions("validator-source-dev-v1")
	noGuard.Development = false
	_, err = k8s.NewWithClient(fake.NewClientset(), namespace, noGuard).CreateJob(context.Background(), dev)
	require.Equal(t, "PROFILE_UNQUALIFIED", activities.RefusalCode(err))
	require.Contains(t, err.Error(), "development guard")
	job, err := k8s.NewWithClient(fake.NewClientset(), namespace, harnessOptions("validator-source-dev-v1")).CreateJob(context.Background(), dev)
	require.NoError(t, err)
	require.Equal(t, 1, job.Request, "the development profile is created behind the guard")
}

// P0.8: only a bound-source profile receives a source archive, a validator
// that binds source needs one, and the allocated identity rides in the
// launch envelope of both the supervisor and the sidecar.
func TestBoundSourceAndIdentityInTheEnvelope(t *testing.T) {
	l := k8s.NewWithClient(fake.NewClientset(), namespace, harnessOptions("validator-fixture-v1", "validator-source-dev-v1"))
	fixture := sourceInput(t, "validator-fixture-v1")
	_, err := l.CreateJob(context.Background(), fixture)
	require.Equal(t, "PROFILE_UNQUALIFIED", activities.RefusalCode(err))
	require.Contains(t, err.Error(), "does not execute bound source")

	unbound := sourceInput(t, "validator-source-dev-v1")
	unbound.Inputs = nil
	_, err = l.CreateJob(context.Background(), unbound)
	require.Equal(t, "PROFILE_UNQUALIFIED", activities.RefusalCode(err))
	require.Contains(t, err.Error(), "needs a \"source\" input")

	// The certified revision is the launch's: no component, no revision, a
	// non-sequence revision or a partial identity is refused before any Job.
	for name, c := range map[string]*activities.ComponentIdentity{
		"no component":      nil,
		"no revision":       {ComponentID: "cmp_hero_fixed", PuckType: "Hero", PackageName: "@anvilkit/hero-fixed"},
		"a non-sequence":    {SourceRevision: "07"},
		"a partial":         {PuckType: "Hero", SourceRevision: "7"},
		"a lower-case type": {ComponentID: "cmp_hero_fixed", PuckType: "hero", PackageName: "@anvilkit/hero-fixed", SourceRevision: "7"},
	} {
		bad := sourceInput(t, "validator-source-dev-v1")
		bad.Component = c
		_, err = l.Render(bad, time.Now())
		require.Error(t, err, name)
	}
	// A preview or release launch names the revision alone.
	revOnly := sourceInput(t, "validator-source-dev-v1")
	revOnly.Component = &activities.ComponentIdentity{SourceRevision: "4"}
	job, err := l.Render(revOnly, time.Now())
	require.NoError(t, err)
	require.Equal(t, map[string]string{"sourceRevision": "4"}, envelopeComponent(t, job))

	in := sourceInput(t, "validator-source-dev-v1")
	job, err = l.Render(in, time.Now())
	require.NoError(t, err)
	require.Equal(t, "true", job.Spec.Template.Labels["anvilkit.io/candidate-code"])
	require.Nil(t, job.Spec.Template.Spec.RuntimeClassName, "the development profile runs on the foundation's runtime")
	for _, c := range append(job.Spec.Template.Spec.InitContainers, job.Spec.Template.Spec.Containers...) {
		for _, e := range c.Env {
			if e.Name != "ANVILKIT_LAUNCH_ENVELOPE" && e.Name != "ANVILKIT_SIDECAR_LAUNCH_ENVELOPE" {
				continue
			}
			var env struct {
				Component map[string]string `json:"component"`
			}
			require.NoError(t, json.Unmarshal([]byte(e.Value), &env))
			require.Equal(t, map[string]string{"componentId": "cmp_hero_fixed", "puckType": "Hero", "packageName": "@anvilkit/hero-fixed", "sourceRevision": "7"}, env.Component, e.Name)
		}
	}
}

func envelopeComponent(t *testing.T, job *batchv1.Job) map[string]string {
	t.Helper()
	for _, c := range job.Spec.Template.Spec.Containers {
		for _, e := range c.Env {
			if e.Name == "ANVILKIT_LAUNCH_ENVELOPE" {
				var env struct {
					Component map[string]string `json:"component"`
				}
				require.NoError(t, json.Unmarshal([]byte(e.Value), &env))
				return env.Component
			}
		}
	}
	t.Fatal("no container carries the launch envelope")
	return nil
}

func sourceInput(t *testing.T, id string) activities.CreateJobInput {
	t.Helper()
	profile, err := jobschema.ProfileByID(id)
	require.NoError(t, err)
	return activities.CreateJobInput{
		Launch:    activities.LaunchRef{LaunchID: "lch_s1", AttemptID: "att_s1", LaunchKey: "val-s1", ImageDigest: profile.Image.Digest},
		ProfileID: id, Deadline: time.Now().Add(5 * time.Minute), Request: 1,
		OperationID: "op_s1", ExecutionEpoch: "1", LaunchEpoch: "1",
		Inputs:    []activities.LaunchInput{{Name: "source", Digest: "sha256:" + strings.Repeat("5", 64), Handle: "h_src"}},
		Component: &activities.ComponentIdentity{ComponentID: "cmp_hero_fixed", PuckType: "Hero", PackageName: "@anvilkit/hero-fixed", SourceRevision: "7"},
	}
}

// The gVisor profile pins the RuntimeClass and marks candidate code; it is
// disabled until its qualification (state disabled, P0.8) even where the
// environment lists it, and Render still shows its template to the
// admission checks.
func TestCandidateProfilePinsRuntimeClassAndIsDisabledByDefault(t *testing.T) {
	l := k8s.NewWithClient(fake.NewClientset(), namespace, harnessOptions(harnessProfile, "codegen-fixed-v1"))
	in := harnessInput(t)
	in.ProfileID = "codegen-fixed-v1"
	profile, _ := jobschema.ProfileByID("codegen-fixed-v1")
	in.Launch.ImageDigest = profile.Image.Digest
	_, err := l.CreateJob(context.Background(), in)
	require.Equal(t, "PROFILE_DISABLED", activities.RefusalCode(err))
	require.Contains(t, err.Error(), "disabled until its qualification")

	enabled := k8s.NewWithClient(fake.NewClientset(), namespace, harnessOptions(harnessProfile, "codegen-fixed-v1"))
	job, err := enabled.Render(in, time.Now())
	require.NoError(t, err)
	require.Equal(t, "gvisor", *job.Spec.Template.Spec.RuntimeClassName)
	require.Equal(t, "true", job.Spec.Template.Labels["anvilkit.io/candidate-code"])
}

// A harness profile is refused without the environment inputs its sidecar
// needs, without a registry for an unqualified repository, and with
// inputs that are not typed.
func TestHarnessTemplateRefusesMissingEnvironmentInputs(t *testing.T) {
	in := harnessInput(t)
	for name, o := range map[string]k8s.Options{
		"no identity": {Backend: "b", ImageRegistry: "localhost:5001", EnabledProfiles: []string{harnessProfile}, SidecarControlAddress: "c:1", SidecarIdentityMode: "disabled", CandidateSeccompProfile: "p"},
		"no control":  {Backend: "b", ImageRegistry: "localhost:5001", EnabledProfiles: []string{harnessProfile}, SidecarIdentityMode: "development", CandidateSeccompProfile: "p"},
		"no registry": {Backend: "b", EnabledProfiles: []string{harnessProfile}, SidecarControlAddress: "c:1", SidecarIdentityMode: "development", CandidateSeccompProfile: "p"},
		"no seccomp":  {Backend: "b", ImageRegistry: "localhost:5001", EnabledProfiles: []string{harnessProfile}, SidecarControlAddress: "c:1", SidecarIdentityMode: "development"},
	} {
		l := k8s.NewWithClient(fake.NewClientset(), namespace, o)
		_, err := l.CreateJob(context.Background(), in)
		require.Equal(t, "PROFILE_UNQUALIFIED", activities.RefusalCode(err), name)
	}
	l := k8s.NewWithClient(fake.NewClientset(), namespace, harnessOptions(harnessProfile))
	bad := in
	bad.Inputs = []activities.LaunchInput{{Name: "Fixed Input", Digest: "sha256:abc"}}
	_, err := l.CreateJob(context.Background(), bad)
	require.Equal(t, "PROFILE_UNQUALIFIED", activities.RefusalCode(err))
	require.Contains(t, err.Error(), "not a typed input")
	bad = in
	bad.OperationID = ""
	_, err = l.CreateJob(context.Background(), bad)
	require.Contains(t, err.Error(), "operation id")
}

// The fixture layout of local-check-v1 is unchanged: one container, no
// sidecar, no privilege, and a registry-qualified image is used as pinned.
func TestFixtureTemplateUnchanged(t *testing.T) {
	l := k8s.NewWithClient(fake.NewClientset(), namespace, harnessOptions("local-check-v1"))
	job, err := l.Render(activities.CreateJobInput{Launch: launch, ProfileID: "local-check-v1", Deadline: time.Now().Add(time.Minute), Request: 1}, time.Now())
	require.NoError(t, err)
	spec := job.Spec.Template.Spec
	require.Len(t, spec.Containers, 1)
	require.Equal(t, "fixture", spec.Containers[0].Name)
	require.Equal(t, "docker.io/library/busybox@"+launch.ImageDigest, spec.Containers[0].Image)
	require.Nil(t, spec.Containers[0].SecurityContext.RunAsUser)
	require.Empty(t, spec.Containers[0].SecurityContext.Capabilities.Add)
	require.Empty(t, spec.Volumes)
	require.Nil(t, spec.NodeSelector)
}

// AwaitOwner returns as soon as the Job's own Pod exists (running or not)
// so the launcher can register it, and never on a duplicate Pod alone.
func TestAwaitOwnerReturnsTheJobsOwnPodWhileRunning(t *testing.T) {
	job, _ := runningLaunch("att_1")
	cs := fake.NewClientset(job)
	l := k8s.NewWithClient(cs, namespace)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan activities.JobObservation, 1)
	go func() {
		obs, err := l.AwaitOwner(ctx, activities.ObserveJobInput{Launch: launch, Deadline: time.Now().Add(time.Minute)}, func() {})
		require.NoError(t, err)
		done <- obs
	}()
	// A duplicate under the label but owned by another Job does not end the wait.
	dup := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "dup", Namespace: namespace, Labels: labels("att_1"), UID: types.UID("dup-uid"),
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: "other", UID: types.UID("job-uid-2"), Controller: ptr.To(true)}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	_, err := cs.CoreV1().Pods(namespace).Create(ctx, dup, metav1.CreateOptions{})
	require.NoError(t, err)
	select {
	case obs := <-done:
		t.Fatalf("the wait ended on a duplicate Pod: %+v", obs)
	case <-time.After(1500 * time.Millisecond):
	}
	own := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "own", Namespace: namespace, Labels: labels("att_1"), UID: types.UID("own-uid"),
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: launch.LaunchKey, UID: job.UID, Controller: ptr.To(true)}}},
		Status: corev1.PodStatus{Phase: corev1.PodPending}}
	_, err = cs.CoreV1().Pods(namespace).Create(ctx, own, metav1.CreateOptions{})
	require.NoError(t, err)
	select {
	case obs := <-done:
		require.Equal(t, "own-uid", obs.Pods[0].PodUID, "the Job's own Pod is the owner, whatever its phase")
		require.Equal(t, "pending", obs.Pods[0].Phase)
		require.Len(t, obs.Pods, 2)
		require.Empty(t, obs.Reason)
	case <-time.After(5 * time.Second):
		t.Fatal("AwaitOwner did not return once the Job's own Pod existed")
	}
}

func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	for i := range out {
		for j := i + 1; j < len(out); j++ {
			if out[j] < out[i] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

// TestSidecarRelayPlacements (P2.4 AC2, B-32): under identity mode mtls the
// sidecar receives the Model Proxy, MCP and Knowledge placements after its
// six launch variables, in a fixed order, and reaches them with its mounted
// workload certificate; the supervisor (and so the candidate) never sees
// them. Unset placements render nothing.
func TestSidecarRelayPlacements(t *testing.T) {
	opts := harnessOptions(harnessProfile)
	opts.SidecarModelProxyURL = "https://anvilkit-agent-model-proxy.anvilkit-apps.svc.cluster.local:9103"
	opts.SidecarMCPAddress = "anvilkit-agent-mcp.anvilkit-apps.svc.cluster.local:9106"
	opts.SidecarKnowledgeAddress = "anvilkit-agent-knowledge.anvilkit-apps.svc.cluster.local:9105"
	job, err := k8s.NewWithClient(fake.NewClientset(), namespace, opts).Render(harnessInput(t), time.Now())
	require.NoError(t, err)
	side, sup := job.Spec.Template.Spec.InitContainers[0], job.Spec.Template.Spec.Containers[0]
	var names []string
	for _, e := range side.Env {
		names = append(names, e.Name)
	}
	require.Equal(t, []string{
		"ANVILKIT_SIDECAR_CONTROL_ADDRESS", "ANVILKIT_SIDECAR_IDENTITY_MODE", "ANVILKIT_SIDECAR_BACKEND", "ANVILKIT_SIDECAR_LAUNCH_KEY", "ANVILKIT_SIDECAR_POD_UID", "ANVILKIT_SIDECAR_LAUNCH_ENVELOPE",
		"ANVILKIT_SIDECAR_MODEL_PROXY_URL", "ANVILKIT_SIDECAR_MCP_ADDRESS", "ANVILKIT_SIDECAR_KNOWLEDGE_ADDRESS",
	}, names)
	require.Contains(t, side.VolumeMounts, corev1.VolumeMount{Name: "identity", MountPath: "/etc/anvilkit/identity", ReadOnly: true})
	for _, e := range sup.Env {
		require.NotContains(t, e.Name, "SIDECAR", "the candidate side never learns a placement")
	}

	partial := harnessOptions(harnessProfile)
	partial.SidecarMCPAddress = "anvilkit-agent-mcp.anvilkit-apps.svc.cluster.local:9106"
	job, err = k8s.NewWithClient(fake.NewClientset(), namespace, partial).Render(harnessInput(t), time.Now())
	require.NoError(t, err)
	last := job.Spec.Template.Spec.InitContainers[0].Env
	require.Equal(t, "ANVILKIT_SIDECAR_MCP_ADDRESS", last[len(last)-1].Name, "only the set placements are rendered")
	require.Len(t, last, 7)
}

// TestTeamAndPreviewProfileStates (P2.4 AC1): the production team and
// preview profiles are disabled until G-04 (PROFILE_DISABLED); the
// development team twin runs only behind the development guard
// (PROFILE_UNQUALIFIED outside it).
func TestTeamAndPreviewProfileStates(t *testing.T) {
	for _, id := range []string{"codegen-team-v1", "preview-build-v1"} {
		p, err := jobschema.ProfileByID(id)
		require.NoError(t, err)
		require.Equal(t, jobschema.StateDisabled, p.State)
		require.Equal(t, "gvisor", p.RuntimeClass)
		require.True(t, p.CandidateCode)
		_, err = k8s.NewWithClient(fake.NewClientset(), namespace, harnessOptions(id)).CreateJob(context.Background(), sourceInput(t, id))
		require.Equal(t, "PROFILE_DISABLED", activities.RefusalCode(err), id)
	}
	dev, err := jobschema.ProfileByID("codegen-team-dev-v1")
	require.NoError(t, err)
	require.Equal(t, jobschema.StateDevelopment, dev.State)
	require.Empty(t, dev.RuntimeClass, "the development twin runs on the foundation's containerd runtime")
	production := harnessOptions("codegen-team-dev-v1")
	production.Development = false
	_, err = k8s.NewWithClient(fake.NewClientset(), namespace, production).CreateJob(context.Background(), sourceInput(t, "codegen-team-dev-v1"))
	require.Equal(t, "PROFILE_UNQUALIFIED", activities.RefusalCode(err))
}

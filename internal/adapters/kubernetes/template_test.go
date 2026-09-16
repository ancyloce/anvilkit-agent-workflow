package kubernetes_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
		SidecarControlAddress: "172.22.0.1:9101", SidecarIdentityMode: "development", CandidateSeccompProfile: "anvilkit/candidate.json",
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
	require.Len(t, side.VolumeMounts, 1)
	require.Equal(t, "/run/anvilkit", side.VolumeMounts[0].MountPath)
	require.False(t, side.VolumeMounts[0].ReadOnly)
	sideEnv := map[string]corev1.EnvVar{}
	for _, e := range side.Env {
		sideEnv[e.Name] = e
	}
	require.Equal(t, "metadata.uid", sideEnv["ANVILKIT_SIDECAR_POD_UID"].ValueFrom.FieldRef.FieldPath, "the Pod UID comes from the kubelet, never from a value")
	require.Equal(t, "172.22.0.1:9101", sideEnv["ANVILKIT_SIDECAR_CONTROL_ADDRESS"].Value)
	require.Equal(t, "development", sideEnv["ANVILKIT_SIDECAR_IDENTITY_MODE"].Value)
	require.Equal(t, "kind-anvilkit-dev", sideEnv["ANVILKIT_SIDECAR_BACKEND"].Value)
	require.Equal(t, "cg-h1", sideEnv["ANVILKIT_SIDECAR_LAUNCH_KEY"].Value)
	require.Equal(t, names["ANVILKIT_LAUNCH_ENVELOPE"], sideEnv["ANVILKIT_SIDECAR_LAUNCH_ENVELOPE"].Value)
	require.Len(t, spec.Volumes, 3)
	for _, v := range spec.Volumes {
		require.NotNil(t, v.EmptyDir, "only emptyDir volumes: %s", v.Name)
	}
}

// The gVisor profile pins the RuntimeClass and marks candidate code; it is
// disabled unless the environment lists it.
func TestCandidateProfilePinsRuntimeClassAndIsDisabledByDefault(t *testing.T) {
	l := k8s.NewWithClient(fake.NewClientset(), namespace, harnessOptions(harnessProfile))
	in := harnessInput(t)
	in.ProfileID = "codegen-fixed-v1"
	profile, _ := jobschema.ProfileByID("codegen-fixed-v1")
	in.Launch.ImageDigest = profile.Image.Digest
	_, err := l.CreateJob(context.Background(), in)
	require.Equal(t, "PROFILE_UNQUALIFIED", activities.RefusalCode(err))
	require.Contains(t, err.Error(), "not enabled in this environment")

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

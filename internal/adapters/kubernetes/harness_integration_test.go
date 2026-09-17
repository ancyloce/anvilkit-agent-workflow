//go:build integration

package kubernetes_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"
	"github.com/ancyloce/anvilkit-agent-contracts/go/jobschema"
	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
	k8s "github.com/ancyloce/anvilkit-agent-workflow/internal/adapters/kubernetes"
)

const (
	clusterNamespace = "anvilkit-components"
	clusterBackend   = "kind-anvilkit-dev"
	wiringProfile    = "harness-wiring-dev-v1"
	validatorProfile = "validator-fixed-dev-v1"
	fixtureDigest    = "sha256:abeb263b000189efdd8206ff39eeb4f2f7f3217f1c480491868b46da4a21c6a8"
)

// clusterEnv is what the in-cluster scenario needs from the development
// foundation (.local/dev/env.sh after deploy/dev/up.sh) and from whoever
// prepares the cross-service dependency: a running Control that a Job's
// sidecar can reach through the kind network. This repository builds
// nothing but itself; the parent repository (anvilkit-services) starts
// Control from its own repository and names it through the environment.
type clusterEnv struct {
	kubeconfig, gateway, registry string
	controlAddr                   string // host side, where this test dials
	sidecarControlAddr            string // the same Control as the Pod reaches it
}

func requireCluster(t *testing.T) clusterEnv {
	t.Helper()
	c := clusterEnv{
		kubeconfig: os.Getenv("KUBECONFIG"), gateway: os.Getenv("ANVILKIT_DEV_KIND_GATEWAY"), registry: os.Getenv("ANVILKIT_DEV_REGISTRY"),
		controlAddr: os.Getenv("ANVILKIT_INTEGRATION_CONTROL_ADDRESS"), sidecarControlAddr: os.Getenv("ANVILKIT_INTEGRATION_SIDECAR_CONTROL_ADDRESS"),
	}
	for name, v := range map[string]string{"KUBECONFIG": c.kubeconfig, "ANVILKIT_DEV_KIND_GATEWAY": c.gateway, "ANVILKIT_DEV_REGISTRY": c.registry} {
		if v == "" {
			t.Skipf("%s not set; source .local/dev/env.sh after deploy/dev/up.sh", name)
		}
	}
	for name, v := range map[string]string{"ANVILKIT_INTEGRATION_CONTROL_ADDRESS": c.controlAddr, "ANVILKIT_INTEGRATION_SIDECAR_CONTROL_ADDRESS": c.sidecarControlAddr} {
		if v == "" {
			t.Skipf("%s not set; a running Control (bound so that a Job Pod reaches it through the kind network gateway, with the artifact store as the cluster reaches it) is prepared by the parent repository's verification chain, not built here", name)
		}
	}
	conn, err := net.DialTimeout("tcp", c.controlAddr, 5*time.Second)
	require.NoError(t, err, "the Control named by ANVILKIT_INTEGRATION_CONTROL_ADDRESS is not reachable")
	conn.Close()
	return c
}

// adminClient is the cluster administrator's client of the development
// cluster (kind's kubeconfig context), used only to read what the launcher
// identity may not: the events the Job controller records.
func adminClient(t *testing.T) kubernetes.Interface {
	t.Helper()
	path := os.Getenv("ANVILKIT_DEV_ADMIN_KUBECONFIG")
	if path == "" {
		path = filepath.Join(os.Getenv("HOME"), ".kube", "config")
	}
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(&clientcmd.ClientConfigLoadingRules{ExplicitPath: path}, &clientcmd.ConfigOverrides{CurrentContext: "kind-" + strings.TrimPrefix(clusterBackend, "kind-")}).ClientConfig()
	if err != nil {
		t.Skipf("no administrator kubeconfig for the development cluster at %s: %v", path, err)
	}
	cs, err := kubernetes.NewForConfig(cfg)
	require.NoError(t, err)
	return cs
}

type summary struct {
	Outcome       string                              `json:"outcome"`
	Verdict       string                              `json:"verdict"`
	StageID       string                              `json:"stageId"`
	ResultHandle  string                              `json:"resultHandle"`
	Evidence      string                              `json:"evidenceHandle"`
	Duplicate     *struct{ Existing, SameStage bool } `json:"duplicateSubmission"`
	CandidateStop string                              `json:"candidateStop"`
	CandidateExit *int                                `json:"candidateExit"`
	Probes        map[string]string                   `json:"probes"`
	Error         string                              `json:"error"`
}

func k8sDigest(s string) string { return fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(s))) }

// TestHarnessOnTheDevelopmentCluster launches the development wiring
// profile through the real launcher into the kind cluster of the
// foundation, with Kyverno's admission, the candidate syscall profile and
// the default-deny NetworkPolicy in place: the launcher registers the
// Job's own Pod while it runs, the Pod's sidecar resolves its scope from
// that registration, uploads to the foundation's MinIO through the kind
// network and Control accepts the stage; a duplicate Pod gets no
// authority; a canceled launch is stopped with its Pods gone and nothing
// accepted; template substitutions are refused by the cluster. It runs on
// runc without gVisor: it verifies wiring and the runc-path controls, not
// candidate isolation (G-04/G-05 stay NOT_RUN).
func TestHarnessOnTheDevelopmentCluster(t *testing.T) {
	c := requireCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	conn, err := grpc.NewClient(c.controlAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	ops, execSvc := controlv1.NewOperationServiceClient(conn), controlv1.NewExecutionServiceClient(conn)
	cfg, err := clientcmd.BuildConfigFromFlags("", c.kubeconfig)
	require.NoError(t, err)
	cs, err := kubernetes.NewForConfig(cfg)
	require.NoError(t, err)
	launcher, err := k8s.New(c.kubeconfig, clusterNamespace, k8s.Options{
		Backend: clusterBackend, ImageRegistry: c.registry, EnabledProfiles: []string{wiringProfile, validatorProfile},
		SidecarControlAddress: c.sidecarControlAddr, SidecarIdentityMode: "development", CandidateSeccompProfile: "anvilkit/candidate.json",
	})
	require.NoError(t, err)
	profile, err := jobschema.ProfileByID(wiringProfile)
	require.NoError(t, err)
	run := fmt.Sprint(time.Now().UnixNano())
	cmdID := func(s string) *controlv1.CommandIdentity {
		return &controlv1.CommandIdentity{TenantId: "tenant_a", CommandId: s + "-" + run, ActorId: "anvilkit-agent-workflow", RequestDigest: k8sDigest(s)}
	}
	newLaunch := func(name string, deadline time.Time) (opID string, attempt *controlv1.Attempt, launch *controlv1.PrepareLaunchResponse, in activities.CreateJobInput) {
		created, err := ops.CreateOperation(ctx, &controlv1.CreateOperationRequest{
			Command: cmdID("op-" + name), Scope: &controlv1.Scope{TenantId: "tenant_a", ProjectId: "proj_a", ActorId: "user_a"},
			Kind: controlv1.OperationKind_OPERATION_KIND_LOCAL_CHECK, Subject: &controlv1.OperationSubject{ProfileId: "local-check-v1", SubjectDigest: "sha256:0dc7fa9db7237a2b5c96f70f59bb00f73bb86a0ca5554e91c312f9ada26e18b3"},
		})
		require.NoError(t, err)
		opID = created.GetOperation().GetOperationId()
		opened, err := execSvc.OpenAttempt(ctx, &controlv1.OpenAttemptRequest{Command: cmdID("open-" + name), OperationId: opID, StepId: "codegen", VisitOrdinal: "0", ProfileId: wiringProfile})
		require.NoError(t, err)
		attempt = opened.GetAttempt()
		key := "hw-" + name + "-" + strings.ToLower(run[len(run)-6:])
		launch, err = execSvc.PrepareLaunch(ctx, &controlv1.PrepareLaunchRequest{Command: cmdID("launch-" + name), AttemptId: attempt.GetAttemptId(), LaunchKey: key, Backend: clusterBackend, ImageDigest: profile.Image.Digest, Deadline: timestamppb.New(deadline)})
		require.NoError(t, err)
		in = activities.CreateJobInput{
			Launch:    activities.LaunchRef{LaunchID: launch.GetLaunchId(), AttemptID: attempt.GetAttemptId(), LaunchKey: key, ImageDigest: profile.Image.Digest},
			ProfileID: wiringProfile, Deadline: deadline, Request: 1, OperationID: opID, ExecutionEpoch: attempt.GetExecutionEpoch(), LaunchEpoch: launch.GetLaunchEpoch(),
			Inputs: []activities.LaunchInput{{Name: "fixed-input", Digest: fixtureDigest}},
		}
		return
	}
	register := func(attempt *controlv1.Attempt, key, jobUID, podUID string) *controlv1.PhysicalInstance {
		resp, err := execSvc.RegisterInstance(ctx, &controlv1.RegisterInstanceRequest{
			Command: cmdID("reg-" + podUID), AttemptId: attempt.GetAttemptId(), LaunchKey: key, Backend: clusterBackend, JobUid: jobUID, PodUid: podUID, ImageDigest: profile.Image.Digest,
		})
		require.NoError(t, err)
		return resp.GetInstance()
	}
	cleanup := func(in activities.CreateJobInput) {
		dctx, dcancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer dcancel()
		_, _ = launcher.DeleteJob(dctx, activities.DeleteJobInput{Launch: in.Launch}, func() {})
	}
	stagesOf := func(attemptID string) (*controlv1.AcceptedStage, bool) {
		st, err := execSvc.GetAcceptedStage(ctx, &controlv1.GetAcceptedStageRequest{AttemptId: attemptID, TenantId: "tenant_a"})
		if err != nil {
			return nil, false
		}
		return st.GetStage(), true
	}

	t.Run("owner registered while running, trusted flow accepted, duplicate Pod without authority", func(t *testing.T) {
		deadline := time.Now().Add(6 * time.Minute)
		_, attempt, _, in := newLaunch("flow", deadline)
		defer cleanup(in)
		job, err := launcher.CreateJob(ctx, in)
		require.NoError(t, err)
		obs, err := launcher.AwaitOwner(ctx, activities.ObserveJobInput{Launch: in.Launch, Deadline: deadline}, func() {})
		require.NoError(t, err)
		require.NotEmpty(t, obs.Pods, "the Job's own Pod exists: %+v", obs)
		require.Equal(t, job.JobUID, obs.JobUID)
		owner := register(attempt, in.Launch.LaunchKey, obs.JobUID, obs.Pods[0].PodUID)
		require.True(t, owner.GetCurrent())

		// A duplicate Pod: a second Job carrying the launch key (the
		// launcher identity can create Jobs only), registered like the
		// launcher would register any Pod it observes: without authority.
		rendered, err := launcher.Render(in, time.Now())
		require.NoError(t, err)
		dup := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: in.Launch.LaunchKey + "-dup", Namespace: clusterNamespace, Labels: rendered.Labels, Annotations: rendered.Annotations}, Spec: rendered.Spec}
		_, err = cs.BatchV1().Jobs(clusterNamespace).Create(ctx, dup, metav1.CreateOptions{})
		require.NoError(t, err, "the duplicate is a valid template and admitted")
		defer func() {
			policy := metav1.DeletePropagationForeground
			_ = cs.BatchV1().Jobs(clusterNamespace).Delete(context.Background(), dup.Name, metav1.DeleteOptions{PropagationPolicy: &policy})
		}()
		var dupPod *corev1.Pod
		require.Eventually(t, func() bool {
			list, err := cs.CoreV1().Pods(clusterNamespace).List(ctx, metav1.ListOptions{LabelSelector: "batch.kubernetes.io/job-name=" + dup.Name})
			if err != nil || len(list.Items) == 0 {
				return false
			}
			dupPod = &list.Items[0]
			return true
		}, 2*time.Minute, time.Second, "the duplicate Job's Pod exists")
		dupInst := register(attempt, in.Launch.LaunchKey, string(dupPod.OwnerReferences[0].UID), string(dupPod.UID))
		require.False(t, dupInst.GetCurrent())

		final, err := launcher.ObserveJob(ctx, activities.ObserveJobInput{Launch: in.Launch, Deadline: deadline}, func() {})
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(final.Pods), 1)
		ownerPod := final.Pods[0]
		require.Equal(t, obs.Pods[0].PodUID, ownerPod.PodUID, "the physical owner is the Job's own Pod")
		require.Equal(t, "succeeded", ownerPod.Phase, "termination message: %s", ownerPod.TerminationMessage)
		var s summary
		require.NoError(t, json.Unmarshal([]byte(ownerPod.TerminationMessage), &s), ownerPod.TerminationMessage)
		require.Equal(t, "completed", s.Outcome, s.Error)
		require.Equal(t, "exited", s.CandidateStop, "acceptance follows a confirmed stop")
		pods, err := cs.CoreV1().Pods(clusterNamespace).List(ctx, metav1.ListOptions{LabelSelector: "batch.kubernetes.io/job-name=" + in.Launch.LaunchKey})
		require.NoError(t, err)
		require.NotEmpty(t, pods.Items)
		running := pods.Items[0]
		require.Equal(t, ownerPod.PodUID, string(running.UID))
		for _, status := range append(running.Status.ContainerStatuses, running.Status.InitContainerStatuses...) {
			expected := profile.Image.Digest
			if status.Name == "access-sidecar" {
				expected = profile.SidecarImage.Digest
			}
			require.Contains(t, status.ImageID, expected, "actual running artifact for %s", status.Name)
			t.Logf("profile=%s revision=%s container=%s imageID=%s node=%s", profile.ProfileID, profile.Revision, status.Name, status.ImageID, running.Spec.NodeName)
		}
		require.Equal(t, "certified", s.Verdict)
		require.NotEmpty(t, s.StageID)
		require.True(t, s.Duplicate != nil && s.Duplicate.Existing && s.Duplicate.SameStage)
		stage, ok := stagesOf(attempt.GetAttemptId())
		require.True(t, ok)
		require.Equal(t, s.StageID, stage.GetStageId())
		require.Equal(t, owner.GetInstanceId(), stage.GetInstanceId())
		require.Len(t, stage.GetArtifacts(), 2, "result and evidence uploaded from inside the cluster to the foundation's store")
		// The boundary the candidate observed inside the Pod, on runc with
		// the candidate syscall profile and the namespace's default deny.
		p := s.Probes
		require.Equal(t, "true", p["capabilities-empty"], "%v", p)
		require.Equal(t, "1", p["no-new-privs"])
		require.Equal(t, "0", p["inherited-fds"])
		require.Equal(t, "EACCES", p["read:verdict.json"])
		require.Equal(t, "EACCES", p["read:resources.json"])
		require.Equal(t, "EACCES", p["connect:trusted.sock"])
		require.Equal(t, "403 ROUTE_FORBIDDEN", p["candidate:results"])
		require.Equal(t, "CLOSED", p["candidate:fd-delegation"])
		require.NotEqual(t, "CREATED", p["socket:af-inet"], "the syscall profile denies AF_INET sockets in the candidate container: %v", p)
		require.NotEqual(t, "CREATED", p["socket:af-inet6"], "%v", p)
		require.Equal(t, "CREATED", p["socket:af-unix"])
		for k, v := range p {
			if strings.HasPrefix(k, "connect:") && k != "connect:trusted.sock" {
				require.NotEqual(t, "CONNECTED", v, "%s", k)
			}
		}

		// The duplicate's harness ran nothing and accepted nothing.
		require.Eventually(t, func() bool {
			pod, err := cs.CoreV1().Pods(clusterNamespace).Get(ctx, dupPod.Name, metav1.GetOptions{})
			return err == nil && pod.Status.Phase == corev1.PodFailed
		}, 3*time.Minute, time.Second, "the duplicate Pod fails without authority")
		pod, err := cs.CoreV1().Pods(clusterNamespace).Get(ctx, dupPod.Name, metav1.GetOptions{})
		require.NoError(t, err)
		var ds summary
		for _, st := range pod.Status.ContainerStatuses {
			if st.Name == "supervisor" && st.State.Terminated != nil {
				require.NoError(t, json.Unmarshal([]byte(st.State.Terminated.Message), &ds), st.State.Terminated.Message)
			}
		}
		require.Equal(t, "infrastructure_failed", ds.Outcome)
		require.Contains(t, ds.Error, "NO_AUTHORITY")
		require.Nil(t, ds.CandidateExit)
		again, ok := stagesOf(attempt.GetAttemptId())
		require.True(t, ok)
		require.Equal(t, stage.GetStageId(), again.GetStageId(), "still the one stage")
	})

	t.Run("the fixed validator Job certifies its component in the cluster and Control binds npm, browser, css and evidence", func(t *testing.T) {
		// P10d: the anvilkit-validator image (validator-fixed-dev-v1) under
		// the same harness layout, admission and sidecar; the chain runs on
		// the reviewed fixed component baked into the image, the deliverables
		// reach the foundation's store through the kind network and Control
		// accepts the stage binding their exact object versions.
		vprofile, err := jobschema.ProfileByID(validatorProfile)
		require.NoError(t, err)
		deadline := time.Now().Add(10 * time.Minute)
		created, err := ops.CreateOperation(ctx, &controlv1.CreateOperationRequest{
			Command: cmdID("op-validator"), Scope: &controlv1.Scope{TenantId: "tenant_a", ProjectId: "proj_a", ActorId: "user_a"},
			Kind: controlv1.OperationKind_OPERATION_KIND_LOCAL_CHECK, Subject: &controlv1.OperationSubject{ProfileId: "local-check-v1", SubjectDigest: "sha256:0dc7fa9db7237a2b5c96f70f59bb00f73bb86a0ca5554e91c312f9ada26e18b3"},
		})
		require.NoError(t, err)
		opID := created.GetOperation().GetOperationId()
		opened, err := execSvc.OpenAttempt(ctx, &controlv1.OpenAttemptRequest{Command: cmdID("open-validator"), OperationId: opID, StepId: "validator", VisitOrdinal: "0", ProfileId: validatorProfile})
		require.NoError(t, err)
		attempt := opened.GetAttempt()
		key := "hv-" + strings.ToLower(run[len(run)-6:])
		launch, err := execSvc.PrepareLaunch(ctx, &controlv1.PrepareLaunchRequest{Command: cmdID("launch-validator"), AttemptId: attempt.GetAttemptId(), LaunchKey: key, Backend: clusterBackend, ImageDigest: vprofile.Image.Digest, Deadline: timestamppb.New(deadline)})
		require.NoError(t, err)
		in := activities.CreateJobInput{
			Launch:    activities.LaunchRef{LaunchID: launch.GetLaunchId(), AttemptID: attempt.GetAttemptId(), LaunchKey: key, ImageDigest: vprofile.Image.Digest},
			ProfileID: validatorProfile, Deadline: deadline, Request: 1, OperationID: opID, ExecutionEpoch: attempt.GetExecutionEpoch(), LaunchEpoch: launch.GetLaunchEpoch(),
		}
		defer cleanup(in)
		_, err = launcher.CreateJob(ctx, in)
		require.NoError(t, err)
		obs, err := launcher.AwaitOwner(ctx, activities.ObserveJobInput{Launch: in.Launch, Deadline: deadline}, func() {})
		require.NoError(t, err)
		require.NotEmpty(t, obs.Pods, "%+v", obs)
		reg, err := execSvc.RegisterInstance(ctx, &controlv1.RegisterInstanceRequest{
			Command: cmdID("reg-validator"), AttemptId: attempt.GetAttemptId(), LaunchKey: key, Backend: clusterBackend, JobUid: obs.JobUID, PodUid: obs.Pods[0].PodUID, ImageDigest: vprofile.Image.Digest,
		})
		require.NoError(t, err)
		require.True(t, reg.GetInstance().GetCurrent())
		final, err := launcher.ObserveJob(ctx, activities.ObserveJobInput{Launch: in.Launch, Deadline: deadline}, func() {})
		require.NoError(t, err)
		require.NotEmpty(t, final.Pods)
		ownerPod := final.Pods[0]
		require.Equal(t, "succeeded", ownerPod.Phase, "termination message: %s", ownerPod.TerminationMessage)
		var vs struct {
			Outcome      string                              `json:"outcome"`
			Verdict      string                              `json:"verdict"`
			FailureCode  string                              `json:"failureCode"`
			StageID      string                              `json:"stageId"`
			Handles      map[string][]string                 `json:"handles"`
			Duplicate    *struct{ Existing, SameStage bool } `json:"duplicateSubmission"`
			StepIdentity string                              `json:"stepIdentity"`
			Detail       string                              `json:"detail"`
			Error        string                              `json:"error"`
		}
		require.NoError(t, json.Unmarshal([]byte(ownerPod.TerminationMessage), &vs), ownerPod.TerminationMessage)
		require.Equal(t, "completed", vs.Outcome, vs.Error)
		require.Equal(t, "certified", vs.Verdict, "%s %s", vs.FailureCode, vs.Detail)
		require.Equal(t, "setpriv", vs.StepIdentity, "the build and SSR steps ran under the candidate identity")
		require.True(t, vs.Duplicate != nil && vs.Duplicate.Existing && vs.Duplicate.SameStage)
		pods, err := cs.CoreV1().Pods(clusterNamespace).List(ctx, metav1.ListOptions{LabelSelector: "batch.kubernetes.io/job-name=" + key})
		require.NoError(t, err)
		require.NotEmpty(t, pods.Items)
		for _, status := range append(pods.Items[0].Status.ContainerStatuses, pods.Items[0].Status.InitContainerStatuses...) {
			expected := vprofile.Image.Digest
			if status.Name == "access-sidecar" {
				expected = vprofile.SidecarImage.Digest
			}
			require.Contains(t, status.ImageID, expected, "actual running artifact for %s", status.Name)
			t.Logf("profile=%s revision=%s container=%s imageID=%s", vprofile.ProfileID, vprofile.Revision, status.Name, status.ImageID)
		}
		stage, ok := stagesOf(attempt.GetAttemptId())
		require.True(t, ok)
		require.Equal(t, vs.StageID, stage.GetStageId())
		require.Equal(t, reg.GetInstance().GetInstanceId(), stage.GetInstanceId())
		classes := map[string][]string{}
		for _, a := range stage.GetArtifacts() {
			require.NotEmpty(t, a.GetObjectVersion())
			classes[a.GetClass()] = append(classes[a.GetClass()], a.GetHandle())
		}
		for _, class := range []string{"npm", "browser", "css", "evidence"} {
			require.Len(t, classes[class], 1, "%v", classes)
			require.Equal(t, vs.Handles[class], classes[class], "the stage binds the handles the validator finalized")
		}
	})

	t.Run("a canceled launch stops with its Pods gone and nothing accepted", func(t *testing.T) {
		deadline := time.Now().Add(6 * time.Minute)
		_, attempt, _, in := newLaunch("cancel", deadline)
		_, err := launcher.CreateJob(ctx, in)
		require.NoError(t, err)
		obs, err := launcher.AwaitOwner(ctx, activities.ObserveJobInput{Launch: in.Launch, Deadline: deadline}, func() {})
		require.NoError(t, err)
		register(attempt, in.Launch.LaunchKey, obs.JobUID, obs.Pods[0].PodUID)
		dctx, dcancel := context.WithTimeout(ctx, 3*time.Minute)
		defer dcancel()
		stopped, err := launcher.DeleteJob(dctx, activities.DeleteJobInput{Launch: in.Launch}, func() {})
		require.NoError(t, err)
		require.True(t, stopped.Empty())
		_, err = cs.BatchV1().Jobs(clusterNamespace).Get(ctx, in.Launch.LaunchKey, metav1.GetOptions{})
		require.True(t, apierrors.IsNotFound(err))
		list, err := cs.CoreV1().Pods(clusterNamespace).List(ctx, metav1.ListOptions{LabelSelector: "anvilkit.io/launch-key=" + in.Launch.LaunchKey})
		require.NoError(t, err)
		require.Empty(t, list.Items)
		closed, err := execSvc.CloseAttempt(ctx, &controlv1.CloseAttemptRequest{Command: cmdID("close-cancel"), AttemptId: attempt.GetAttemptId(), Outcome: controlv1.AttemptOutcome_ATTEMPT_OUTCOME_CANCELED, Cleanup: controlv1.CleanupState_CLEANUP_STATE_COMPLETE})
		require.NoError(t, err)
		require.Equal(t, controlv1.AttemptState_ATTEMPT_STATE_CLOSED, closed.GetAttempt().GetState())
		_, ok := stagesOf(attempt.GetAttemptId())
		require.False(t, ok, "nothing was accepted for the canceled attempt")
	})

	t.Run("live cancellation fence refuses the repaired sidecar scope", func(t *testing.T) {
		deadline := time.Now().Add(6 * time.Minute)
		opID, attempt, _, in := newLaunch("fenced", deadline)
		defer cleanup(in)
		_, err := launcher.CreateJob(ctx, in)
		require.NoError(t, err)
		obs, err := launcher.AwaitOwner(ctx, activities.ObserveJobInput{Launch: in.Launch, Deadline: deadline}, func() {})
		require.NoError(t, err)
		require.NotEmpty(t, obs.Pods)
		// Install the real Control fence before registration unblocks the
		// supervisor. No older scope can confer authority on this instance.
		view, err := ops.GetOperation(ctx, &controlv1.GetOperationRequest{Scope: &controlv1.Scope{TenantId: "tenant_a", ProjectId: "proj_a", ActorId: "user_a"}, OperationId: opID})
		require.NoError(t, err)
		_, err = ops.SubmitCommand(ctx, &controlv1.SubmitCommandRequest{Command: cmdID("fence"), Scope: &controlv1.Scope{TenantId: "tenant_a", ProjectId: "proj_a", ActorId: "user_a"}, OperationId: opID, Kind: controlv1.CommandKind_COMMAND_KIND_CANCEL, ExpectedRevision: view.GetOperation().GetRevision()})
		require.NoError(t, err)
		register(attempt, in.Launch.LaunchKey, obs.JobUID, obs.Pods[0].PodUID)
		final, err := launcher.ObserveJob(ctx, activities.ObserveJobInput{Launch: in.Launch, Deadline: deadline}, func() {})
		require.NoError(t, err)
		require.NotEmpty(t, final.Pods)
		var stopped summary
		require.NoError(t, json.Unmarshal([]byte(final.Pods[0].TerminationMessage), &stopped))
		require.Equal(t, "infrastructure_failed", stopped.Outcome)
		require.Contains(t, stopped.Error, "STALE_EXECUTION")
		require.Nil(t, stopped.CandidateExit)
		_, accepted := stagesOf(attempt.GetAttemptId())
		require.False(t, accepted, "no candidate or observer ran after the fence")
	})

	t.Run("template substitutions are refused by the cluster's admission", func(t *testing.T) {
		deadline := time.Now().Add(6 * time.Minute)
		_, _, _, in := newLaunch("admission", deadline)
		rendered, err := launcher.Render(in, time.Now())
		require.NoError(t, err)
		rendered.Namespace = clusterNamespace
		retries := rendered.DeepCopy()
		retries.Name += "-retries"
		three := int32(3)
		retries.Spec.BackoffLimit = &three
		_, err = cs.BatchV1().Jobs(clusterNamespace).Create(ctx, retries, metav1.CreateOptions{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "anvilkit-components-jobs", "the Job policy of the cluster refused retries: %v", err)

		// A Pod-level substitution is refused when the launcher submits the
		// Job: Kyverno autogen evaluates the Pod policy against the Job's
		// template at Job admission (sixteenth-pass R1), so no Job and no
		// Pod of the substituted template ever exists. The same for a
		// lifecycle hook and a subPath mount.
		for name, mutate := range map[string]func(j *batchv1.Job){
			"env": func(j *batchv1.Job) {
				j.Spec.Template.Spec.Containers[0].Env = append(j.Spec.Template.Spec.Containers[0].Env, corev1.EnvVar{Name: "AWS_SECRET_ACCESS_KEY", Value: "x"})
			},
			"poststart": func(j *batchv1.Job) {
				j.Spec.Template.Spec.Containers[0].Lifecycle = &corev1.Lifecycle{PostStart: &corev1.LifecycleHandler{Exec: &corev1.ExecAction{Command: []string{"/bin/sh", "-c", "id"}}}}
			},
			"subpath": func(j *batchv1.Job) {
				j.Spec.Template.Spec.Containers[0].VolumeMounts[0].SubPath = "x"
			},
		} {
			substituted := rendered.DeepCopy()
			substituted.Name += "-" + name
			mutate(substituted)
			_, err = cs.BatchV1().Jobs(clusterNamespace).Create(ctx, substituted, metav1.CreateOptions{})
			require.Error(t, err, name)
			require.Contains(t, err.Error(), "anvilkit-components-pods", "the Pod policy's template rules refused the substituted Job (%s) at admission: %v", name, err)
			_, err = cs.BatchV1().Jobs(clusterNamespace).Get(ctx, substituted.Name, metav1.GetOptions{})
			require.True(t, apierrors.IsNotFound(err), "no Job of the substituted template (%s) exists", name)
			list, err := cs.CoreV1().Pods(clusterNamespace).List(ctx, metav1.ListOptions{LabelSelector: "batch.kubernetes.io/job-name=" + substituted.Name})
			require.NoError(t, err)
			require.Empty(t, list.Items, "no Pod of the substituted template (%s) runs", name)
		}
	})
}

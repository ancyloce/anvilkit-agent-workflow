package kubernetes_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
	k8s "github.com/ancyloce/anvilkit-agent-workflow/internal/adapters/kubernetes"
)

const namespace = "anvilkit-components"

var launch = activities.LaunchRef{LaunchID: "lch_1", AttemptID: "att_1", LaunchKey: "lc-abc", ImageDigest: "sha256:9db7b59979c38555a39def84a31fb98b5296952f9e3afd4f6f11f05b07adfab0"}

func labels(attempt string) map[string]string {
	return map[string]string{"anvilkit.io/launch-key": launch.LaunchKey, "anvilkit.io/attempt-id": attempt, "anvilkit.io/profile-id": "local-check-v1"}
}

func runningLaunch(attempt string) (*batchv1.Job, *corev1.Pod) {
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: launch.LaunchKey, Namespace: namespace, Labels: labels(attempt), UID: types.UID("job-uid-1")}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: launch.LaunchKey + "-x1", Namespace: namespace, Labels: labels(attempt), UID: types.UID("pod-uid-1")},
		Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	return job, pod
}

// DeleteJob returns only after the API server reports neither the Job nor
// any Pod of the launch; until then it heartbeats and keeps observing.
func TestDeleteJobConfirmsJobAndPodsGone(t *testing.T) {
	job, pod := runningLaunch("att_1")
	cs := fake.NewClientset(job, pod)
	l := k8s.NewWithClient(cs, namespace)
	var beats atomic.Int32
	done := make(chan error, 1)
	go func() {
		_, err := l.DeleteJob(context.Background(), activities.DeleteJobInput{Launch: launch}, func() { beats.Add(1) })
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("cleanup declared complete while the Pod still exists: %v", err)
	case <-time.After(2500 * time.Millisecond):
	}
	_, err := cs.BatchV1().Jobs(namespace).Get(context.Background(), launch.LaunchKey, metav1.GetOptions{})
	require.Error(t, err, "the Job deletion was requested")
	require.GreaterOrEqual(t, beats.Load(), int32(2), "the wait heartbeats")
	// The Pod finally terminates and disappears: only now is cleanup complete.
	require.NoError(t, cs.CoreV1().Pods(namespace).Delete(context.Background(), pod.Name, metav1.DeleteOptions{}))
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("DeleteJob did not return after the Pod was gone")
	}
}

// A Pod that never disappears within the bound is not complete cleanup.
func TestDeleteJobUnconfirmedWithinBoundIsAnError(t *testing.T) {
	job, pod := runningLaunch("att_1")
	l := k8s.NewWithClient(fake.NewClientset(job, pod), namespace)
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	_, err := l.DeleteJob(ctx, activities.DeleteJobInput{Launch: launch}, func() {})
	require.Error(t, err)
	require.Contains(t, err.Error(), "not confirmed stopped")
	require.Contains(t, err.Error(), "pods: 1")
}

// An absent Job with no Pods is complete cleanup at once.
func TestDeleteJobAbsentIsComplete(t *testing.T) {
	l := k8s.NewWithClient(fake.NewClientset(), namespace)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stopped, err := l.DeleteJob(ctx, activities.DeleteJobInput{Launch: launch}, func() {})
	require.NoError(t, err)
	require.True(t, stopped.Empty())
}

// No Job is created once the launch deadline has passed; an already
// existing Job of the same attempt is still returned for observation.
func TestCreateJobRefusesPassedDeadline(t *testing.T) {
	cs := fake.NewClientset()
	l := k8s.NewWithClient(cs, namespace)
	_, err := l.CreateJob(context.Background(), activities.CreateJobInput{Launch: launch, ProfileID: "local-check-v1", Deadline: time.Now().Add(-time.Second)})
	require.Error(t, err)
	require.Equal(t, "DEADLINE_EXCEEDED", activities.RefusalCode(err))
	list, err := cs.BatchV1().Jobs(namespace).List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	require.Empty(t, list.Items, "nothing was created after the deadline")

	job, _ := runningLaunch("att_1")
	_, err = cs.BatchV1().Jobs(namespace).Create(context.Background(), job, metav1.CreateOptions{})
	require.NoError(t, err)
	ref, err := l.CreateJob(context.Background(), activities.CreateJobInput{Launch: launch, ProfileID: "local-check-v1", Deadline: time.Now().Add(-time.Second)})
	require.NoError(t, err, "the original Job stays observable under its identity")
	require.Equal(t, "job-uid-1", ref.JobUID)
}

// CreateJob is idempotent by launch key and refuses a key owned by another
// attempt. Every request marks the object it creates with its ordinal; a
// later request that finds the Job reports the marker of the request that
// committed it, not its own, so the caller's ledger settles the right one.
func TestCreateJobIdempotentByLaunchKey(t *testing.T) {
	cs := fake.NewClientset()
	l := k8s.NewWithClient(cs, namespace)
	in := activities.CreateJobInput{Launch: launch, ProfileID: "local-check-v1", Deadline: time.Now().Add(time.Minute), Request: 1}
	first, err := l.CreateJob(context.Background(), in)
	require.NoError(t, err)
	require.Equal(t, 1, first.Request)
	in.Request = 2
	second, err := l.CreateJob(context.Background(), in)
	require.NoError(t, err)
	require.Equal(t, first.JobUID, second.JobUID)
	require.Equal(t, 1, second.Request, "the Job was committed by the first request")
	list, err := cs.BatchV1().Jobs(namespace).List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	require.Equal(t, "1", list.Items[0].Annotations["anvilkit.io/create-request"])
	require.Equal(t, "1", list.Items[0].Spec.Template.Annotations["anvilkit.io/create-request"], "the Pods inherit the marker")
	require.Equal(t, int32(0), *list.Items[0].Spec.BackoffLimit)
	require.LessOrEqual(t, *list.Items[0].Spec.ActiveDeadlineSeconds, int64(60))
	require.False(t, *list.Items[0].Spec.Template.Spec.AutomountServiceAccountToken)

	other := launch
	other.AttemptID = "att_2"
	_, err = l.CreateJob(context.Background(), activities.CreateJobInput{Launch: other, ProfileID: "local-check-v1", Deadline: time.Now().Add(time.Minute)})
	require.Equal(t, "STALE_EXECUTION", activities.RefusalCode(err))
}

// For a create request that never resolved, absence is not evidence: the
// observation of the launch key lasts the settle window and then reports
// that the create is not known to have finished, deleting nothing. A create
// that commits after that window is found by the next observation under
// the same launch key (the Job and its Pod, in creation order); a repeated
// observation sees the same objects because observing deletes nothing. The
// launch is then a materialized one: DeleteJob stops it and is complete
// only once its Pod is gone, and a repeated DeleteJob after that confirms
// the same absence.
func TestObserveLaunchAbsenceIsNotEvidenceAndALateJobIsObserved(t *testing.T) {
	cs := fake.NewClientset()
	l := k8s.NewWithClient(cs, namespace)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	in := activities.ObserveLaunchInput{Launch: launch, SettleWindow: 2 * time.Second}
	var beats atomic.Int32
	started := time.Now()
	_, err := l.ObserveLaunch(ctx, in, func() { beats.Add(1) })
	require.Error(t, err, "absence for the whole window is not evidence that the create finished")
	require.Contains(t, err.Error(), "create unresolved")
	require.Contains(t, err.Error(), "cleanup stays unknown")
	require.GreaterOrEqual(t, time.Since(started), 2*time.Second, "the window paced the observation")
	require.Less(t, time.Since(started), 10*time.Second, "the window bounds one observation; the caller decides how to continue")
	require.GreaterOrEqual(t, beats.Load(), int32(2), "the observation heartbeats")

	// The original create commits after the window: the next observation
	// under the same launch key reports the Job and its Pod without deleting.
	job, pod := runningLaunch("att_1")
	_, err = cs.BatchV1().Jobs(namespace).Create(ctx, job, metav1.CreateOptions{})
	require.NoError(t, err)
	_, err = cs.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{})
	require.NoError(t, err)
	for i := 0; i < 2; i++ {
		obs, err := l.ObserveLaunch(ctx, in, func() {})
		require.NoError(t, err)
		require.Equal(t, "job-uid-1", obs.JobUID)
		require.Len(t, obs.Pods, 1)
		require.Equal(t, "pod-uid-1", obs.Pods[0].PodUID)
		require.Equal(t, "running", obs.Pods[0].Phase)
		_, err = cs.BatchV1().Jobs(namespace).Get(ctx, launch.LaunchKey, metav1.GetOptions{})
		require.NoError(t, err, "observing deletes nothing, so a repeated observation sees the same launch")
	}

	// Now known to have materialized: the delete completes once the Pod is gone.
	done := make(chan error, 1)
	go func() {
		_, err := l.DeleteJob(ctx, activities.DeleteJobInput{Launch: launch}, func() {})
		done <- err
	}()
	require.Eventually(t, func() bool {
		_, err := cs.BatchV1().Jobs(namespace).Get(ctx, launch.LaunchKey, metav1.GetOptions{})
		return apierrors.IsNotFound(err)
	}, 5*time.Second, 50*time.Millisecond, "the late Job was deleted under the original launch key")
	select {
	case err := <-done:
		t.Fatalf("cleanup declared complete while the late Pod still exists: %v", err)
	case <-time.After(1500 * time.Millisecond):
	}
	require.NoError(t, cs.CoreV1().Pods(namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{}))
	select {
	case err := <-done:
		require.NoError(t, err, "the launch was seen and its Pod is gone: complete")
	case <-time.After(5 * time.Second):
		t.Fatal("DeleteJob did not return after the late launch was gone")
	}
	repeated, err := l.DeleteJob(ctx, activities.DeleteJobInput{Launch: launch}, func() {})
	require.NoError(t, err, "a repeated delete of a materialized launch confirms the same absence")
	require.True(t, repeated.Empty())

	// Lingering Pods after the Job is gone still identify the launch through
	// their controller owner reference.
	pod2 := pod.DeepCopy()
	pod2.ResourceVersion = ""
	pod2.OwnerReferences = []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: launch.LaunchKey, UID: types.UID("job-uid-1"), Controller: ptr.To(true)}}
	_, err = cs.CoreV1().Pods(namespace).Create(ctx, pod2, metav1.CreateOptions{})
	require.NoError(t, err)
	obs, err := l.ObserveLaunch(ctx, in, func() {})
	require.NoError(t, err)
	require.Equal(t, "job-uid-1", obs.JobUID, "the Job identity comes from the Pod's owner reference")
	require.Len(t, obs.Pods, 1)
}

// A create that lands during the observation of an unresolved launch is
// reported as soon as the backend shows it, well inside the window.
func TestObserveLaunchReportsALateJobInsideTheWindow(t *testing.T) {
	cs := fake.NewClientset()
	l := k8s.NewWithClient(cs, namespace)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	type result struct {
		obs activities.LaunchObservation
		err error
	}
	done := make(chan result, 1)
	started := time.Now()
	go func() {
		obs, err := l.ObserveLaunch(ctx, activities.ObserveLaunchInput{Launch: launch, SettleWindow: 8 * time.Second}, func() {})
		done <- result{obs, err}
	}()
	time.Sleep(1200 * time.Millisecond) // inside the window the original create lands
	job, _ := runningLaunch("att_1")
	_, err := cs.BatchV1().Jobs(namespace).Create(ctx, job, metav1.CreateOptions{})
	require.NoError(t, err)
	select {
	case r := <-done:
		require.NoError(t, r.err)
		require.Equal(t, "job-uid-1", r.obs.JobUID)
		require.Empty(t, r.obs.Pods, "the Job was seen before its controller ran a Pod")
		require.Less(t, time.Since(started), 5*time.Second, "reported as soon as seen, not at the end of the window")
	case <-time.After(10 * time.Second):
		t.Fatal("ObserveLaunch did not report the late Job")
	}
	// Observation deleted nothing.
	_, err = cs.BatchV1().Jobs(namespace).Get(ctx, launch.LaunchKey, metav1.GetOptions{})
	require.NoError(t, err)
}

// apiServer is a stand-in API server for the transport classification of a
// create request: the handler decides whether the request is answered.
func apiServer(t *testing.T, handler http.HandlerFunc) (*httptest.Server, *k8s.Launcher) {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	l, err := k8s.NewForConfig(&rest.Config{Host: srv.URL}, namespace, k8s.Options{})
	require.NoError(t, err)
	return srv, l
}

// statusAnswer writes a Kubernetes Status answer with the given code and
// reason, as the API server does for a request it validated and refused.
func statusAnswer(w http.ResponseWriter, code int, reason metav1.StatusReason, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"}, Status: metav1.StatusFailure, Code: int32(code), Reason: reason, Message: message})
}

// A create request the API server received but did not answer before the
// caller's deadline is unresolved: the server may still commit it, so the
// launcher reports neither a refusal nor NotCreated. A request that never
// reached the wire (nothing listens) and one the server rejected (4xx) are
// NotCreated: nothing can appear under the launch key from them. A server
// error (5xx) is unresolved, because the write may have gone through.
func TestCreateJobClassifiesUnansweredRejectedAndUnsentRequests(t *testing.T) {
	in := activities.CreateJobInput{Launch: launch, ProfileID: "local-check-v1", Deadline: time.Now().Add(time.Minute), Request: 1}
	received := make(chan struct{}, 1)
	srv, l := apiServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		received <- struct{}{}
		<-r.Context().Done() // the server holds the request until the client gives up
	})
	ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
	defer cancel()
	_, err := l.CreateJob(ctx, in)
	require.Error(t, err)
	<-received
	require.False(t, activities.IsNotCreated(err), "the request was written and not answered: its fate is unknown")
	require.Empty(t, activities.RefusalCode(err))
	require.Contains(t, err.Error(), "create request 1 unanswered")

	srv.Close() // nothing listens any more: the request cannot reach a server
	_, err = l.CreateJob(context.Background(), in)
	require.Error(t, err)
	require.True(t, activities.IsNotCreated(err), "connection refused before the request was written")

	// Answers that refuse the request unprocessed are definitive; answers
	// that only say the request did not finish in time, or that no
	// standard gives a meaning, are not, whether the API server or a proxy
	// in front of it wrote them.
	for _, tc := range []struct {
		name       string
		answer     http.HandlerFunc
		notCreated bool
	}{
		{"403 Status from the API server", func(w http.ResponseWriter, r *http.Request) {
			statusAnswer(w, http.StatusForbidden, metav1.StatusReasonForbidden, "jobs is forbidden")
		}, true},
		{"422 Status from the API server", func(w http.ResponseWriter, r *http.Request) {
			statusAnswer(w, http.StatusUnprocessableEntity, metav1.StatusReasonInvalid, "Job.batch is invalid")
		}, true},
		{"429 plain text with Retry-After (priority and fairness, or a proxy's rate limit)", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "Too many requests, please try again later.", http.StatusTooManyRequests)
		}, true},
		{"408 from a proxy whose upstream did not finish", func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "request timeout", http.StatusRequestTimeout)
		}, false},
		{"499 from a proxy that saw the client leave", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(499)
		}, false},
		{"500 Status from the API server", func(w http.ResponseWriter, r *http.Request) {
			statusAnswer(w, http.StatusInternalServerError, metav1.StatusReasonInternalError, "etcdserver: request timed out")
		}, false},
		{"504 from a gateway", func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "upstream timed out", http.StatusGatewayTimeout)
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, l := apiServer(t, func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				tc.answer(w, r)
			})
			_, err := l.CreateJob(context.Background(), in)
			require.Error(t, err)
			require.Empty(t, activities.RefusalCode(err))
			require.Equal(t, tc.notCreated, activities.IsNotCreated(err))
		})
	}
}

// One create request is one HTTP request. client-go re-sends a POST whose
// 429 or 5xx answer carries Retry-After; behind a proxy that answers so
// while the API server still processes the original, a second HTTP request
// of the same ordinal could commit first and hide the original, which then
// lands after the second Job is deleted. The launcher's transport removes
// the header from POST answers, so the request is answered once and stays
// unresolved for the Workflow's ledger; the next request is the Workflow's,
// under its own ordinal.
func TestCreateJobSendsOneRequestPerOrdinal(t *testing.T) {
	in := activities.CreateJobInput{Launch: launch, ProfileID: "local-check-v1", Deadline: time.Now().Add(time.Minute), Request: 1}
	var posts atomic.Int32
	answer := func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if r.Method == http.MethodPost {
			posts.Add(1)
		}
		w.Header().Set("Retry-After", "0")
		statusAnswer(w, http.StatusInternalServerError, metav1.StatusReasonInternalError, "upstream still working")
	}
	srv, l := apiServer(t, answer)
	_, err := l.CreateJob(context.Background(), in)
	require.Error(t, err)
	require.False(t, activities.IsNotCreated(err), "the answer does not say the create finished")
	require.Equal(t, int32(1), posts.Load(), "the launcher sends the create once")

	// The same answer through a client without the launcher's transport:
	// client-go itself re-sends the POST, which is what the wrapper prevents.
	posts.Store(0)
	plain, err := kubernetes.NewForConfig(&rest.Config{Host: srv.URL})
	require.NoError(t, err)
	_, err = plain.BatchV1().Jobs(namespace).Create(context.Background(), &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: launch.LaunchKey}}, metav1.CreateOptions{})
	require.Error(t, err)
	require.Greater(t, posts.Load(), int32(1), "client-go re-sends a POST on Retry-After")
}

// The physical owner of the launch is the Job's own Pod. A duplicate Pod
// under the launch-key label that belongs to another owner (here another
// Job) is reported after the Job's Pods and never ends the observation,
// even when it was created first: creation timestamps have one-second
// resolution and the API server lists Pods by name, so ownership, not
// order of appearance, decides.
func TestObserveJobOwnerIsTheJobsOwnPod(t *testing.T) {
	job, _ := runningLaunch("att_1")
	created := metav1.NewTime(time.Now().Truncate(time.Second))
	own := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: launch.LaunchKey + "-zzzz1", Namespace: namespace, Labels: labels("att_1"), UID: types.UID("pod-own"), CreationTimestamp: created,
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: launch.LaunchKey, UID: job.UID, Controller: ptr.To(true)}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	dup := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: launch.LaunchKey + "-dup-aaaa1", Namespace: namespace, Labels: labels("att_1"), UID: types.UID("pod-dup"), CreationTimestamp: created,
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: launch.LaunchKey + "-dup", UID: types.UID("job-uid-dup"), Controller: ptr.To(true)}}},
		Status: corev1.PodStatus{Phase: corev1.PodSucceeded, ContainerStatuses: []corev1.ContainerStatus{{Name: "fixture", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}}}}}
	cs := fake.NewClientset(job, own, dup)
	l := k8s.NewWithClient(cs, namespace)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	in := activities.ObserveJobInput{Launch: launch, Deadline: time.Now().Add(time.Minute)}
	type result struct {
		obs activities.JobObservation
		err error
	}
	done := make(chan result, 1)
	go func() {
		obs, err := l.ObserveJob(ctx, in, func() {})
		done <- result{obs, err}
	}()
	select {
	case r := <-done:
		t.Fatalf("the observation ended on the duplicate's completion: %+v %v", r.obs, r.err)
	case <-time.After(2500 * time.Millisecond):
	}
	// The Job's own Pod finishes: only now is the observation terminal.
	own.Status = corev1.PodStatus{Phase: corev1.PodSucceeded, ContainerStatuses: []corev1.ContainerStatus{{Name: "fixture", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0, Message: "{}"}}}}}
	_, err := cs.CoreV1().Pods(namespace).UpdateStatus(ctx, own, metav1.UpdateOptions{})
	require.NoError(t, err)
	select {
	case r := <-done:
		require.NoError(t, r.err)
		require.Len(t, r.obs.Pods, 2)
		require.Equal(t, "pod-own", r.obs.Pods[0].PodUID, "the Job's own Pod is the physical owner")
		require.Equal(t, "succeeded", r.obs.Pods[0].Phase)
		require.Equal(t, "{}", r.obs.Pods[0].TerminationMessage)
		require.Equal(t, "pod-dup", r.obs.Pods[1].PodUID, "the duplicate is reported, after the owner")
	case <-time.After(5 * time.Second):
		t.Fatal("ObserveJob did not end on the owner's completion")
	}
}

// DeleteJob stops nothing that the caller has not recorded: while it waits
// for B's Job and Pod to go, A's Job (an unaccounted request) lands under
// the released launch key; the call returns A's object as its observation
// without deleting it, so a lost completion receipt finds it again. Once the
// caller has accounted for A, the next call stops A and confirms the key
// empty.
func TestDeleteJobReportsAnUnaccountedObjectInsteadOfDeletingIt(t *testing.T) {
	cs := fake.NewClientset()
	l := k8s.NewWithClient(cs, namespace)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	b, err := l.CreateJob(ctx, activities.CreateJobInput{Launch: launch, ProfileID: "local-check-v1", Deadline: time.Now().Add(time.Minute), Request: 2})
	require.NoError(t, err)
	require.Equal(t, 2, b.Request)
	podB := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: launch.LaunchKey + "-b", Namespace: namespace, Labels: labels("att_1"), Annotations: map[string]string{"anvilkit.io/create-request": "2"}, UID: types.UID("pod-uid-b")}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	_, err = cs.CoreV1().Pods(namespace).Create(ctx, podB, metav1.CreateOptions{})
	require.NoError(t, err)

	type result struct {
		obs activities.LaunchObservation
		err error
	}
	done := make(chan result, 1)
	go func() {
		obs, err := l.DeleteJob(ctx, activities.DeleteJobInput{Launch: launch, Unaccounted: []int{1}}, func() {})
		done <- result{obs, err}
	}()
	require.Eventually(t, func() bool {
		_, err := cs.BatchV1().Jobs(namespace).Get(ctx, launch.LaunchKey, metav1.GetOptions{})
		return apierrors.IsNotFound(err)
	}, 5*time.Second, 50*time.Millisecond, "B's Job deletion was requested")
	// A commits while B's Pod is still stopping.
	a, err := l.CreateJob(ctx, activities.CreateJobInput{Launch: launch, ProfileID: "local-check-v1", Deadline: time.Now().Add(time.Minute), Request: 1})
	require.NoError(t, err)
	require.Equal(t, 1, a.Request)
	select {
	case r := <-done:
		require.NoError(t, r.err)
		require.Equal(t, []int{1, 2}, r.obs.Requests, "A's object is reported as the unaccounted evidence, with B's stopping Pod")
		require.Len(t, r.obs.Pods, 1, "B's stopping Pod is part of what the key shows")
	case <-time.After(5 * time.Second):
		t.Fatal("DeleteJob did not report A's object")
	}
	_, err = cs.BatchV1().Jobs(namespace).Get(ctx, launch.LaunchKey, metav1.GetOptions{})
	require.NoError(t, err, "A's Job was not deleted before the caller recorded it")
	again, err := l.DeleteJob(ctx, activities.DeleteJobInput{Launch: launch, Unaccounted: []int{1}}, func() {})
	require.NoError(t, err)
	require.Equal(t, []int{1, 2}, again.Requests, "a repeated call (lost receipt) reports the same evidence")

	// Recorded by the caller: the next call stops A and confirms the key empty.
	require.NoError(t, cs.CoreV1().Pods(namespace).Delete(ctx, podB.Name, metav1.DeleteOptions{}))
	stopped, err := l.DeleteJob(ctx, activities.DeleteJobInput{Launch: launch}, func() {})
	require.NoError(t, err)
	require.True(t, stopped.Empty())
	_, err = cs.BatchV1().Jobs(namespace).Get(ctx, launch.LaunchKey, metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err))
}

// The interleaving at the API server: request A (1) is unanswered, request
// B (2) commits the Job first and its Pod runs; the cleanup stops B and
// confirms it gone; then A commits under the released launch key. The
// observation attributes each object to the request that made it: B's
// while B ran, nothing while the key is empty, and A's once A's Job (or
// only its lingering Pod) is there, so the caller can tell that A, not B,
// is what it now sees.
func TestObserveLaunchAttributesObjectsToTheirCreateRequests(t *testing.T) {
	cs := fake.NewClientset()
	l := k8s.NewWithClient(cs, namespace)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	in := activities.ObserveLaunchInput{Launch: launch, SettleWindow: time.Second}
	b, err := l.CreateJob(ctx, activities.CreateJobInput{Launch: launch, ProfileID: "local-check-v1", Deadline: time.Now().Add(time.Minute), Request: 2})
	require.NoError(t, err)
	require.Equal(t, 2, b.Request)
	podB := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: launch.LaunchKey + "-b", Namespace: namespace, Labels: labels("att_1"), Annotations: map[string]string{"anvilkit.io/create-request": "2"}, UID: types.UID("pod-uid-b")}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	_, err = cs.CoreV1().Pods(namespace).Create(ctx, podB, metav1.CreateOptions{})
	require.NoError(t, err)
	obs, err := l.ObserveLaunch(ctx, in, func() {})
	require.NoError(t, err)
	require.Equal(t, []int{2}, obs.Requests, "B's object is evidence for B only")

	// Cleanup stops B; the Pod goes with the Job.
	done := make(chan error, 1)
	go func() {
		_, err := l.DeleteJob(ctx, activities.DeleteJobInput{Launch: launch, Unaccounted: []int{1}}, func() {})
		done <- err
	}()
	require.Eventually(t, func() bool {
		_, err := cs.BatchV1().Jobs(namespace).Get(ctx, launch.LaunchKey, metav1.GetOptions{})
		return apierrors.IsNotFound(err)
	}, 5*time.Second, 50*time.Millisecond)
	require.NoError(t, cs.CoreV1().Pods(namespace).Delete(ctx, podB.Name, metav1.DeleteOptions{}))
	require.NoError(t, <-done, "B is confirmed gone")
	_, err = l.ObserveLaunch(ctx, in, func() {})
	require.Error(t, err, "an empty launch key is not evidence for A")

	// A commits under the released name: a distinct Job carrying A's marker.
	a, err := l.CreateJob(ctx, activities.CreateJobInput{Launch: launch, ProfileID: "local-check-v1", Deadline: time.Now().Add(time.Minute), Request: 1})
	require.NoError(t, err)
	require.Equal(t, 1, a.Request, "the reappeared Job carries A's marker, not B's")
	obs, err = l.ObserveLaunch(ctx, in, func() {})
	require.NoError(t, err)
	require.Equal(t, []int{1}, obs.Requests, "the reappeared Job is A's")

	// Only A's Pod lingers after A's Job is gone: the marker survives on it.
	require.NoError(t, cs.BatchV1().Jobs(namespace).Delete(ctx, launch.LaunchKey, metav1.DeleteOptions{}))
	podA := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: launch.LaunchKey + "-a", Namespace: namespace, Labels: labels("att_1"), Annotations: map[string]string{"anvilkit.io/create-request": "1"}, UID: types.UID("pod-uid-a"),
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: launch.LaunchKey, UID: types.UID("job-uid-a"), Controller: ptr.To(true)}}}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	_, err = cs.CoreV1().Pods(namespace).Create(ctx, podA, metav1.CreateOptions{})
	require.NoError(t, err)
	obs, err = l.ObserveLaunch(ctx, in, func() {})
	require.NoError(t, err)
	require.Equal(t, "job-uid-a", obs.JobUID, "the Job identity comes from the Pod's owner reference")
	require.Equal(t, []int{1}, obs.Requests)
	require.Len(t, obs.Pods, 1)
}

// uidPreconditions plays the API server's deletion precondition on the fake
// clientset (whose tracker ignores DeleteOptions): a delete that names a UID
// other than the one under the name is refused with a Conflict and deletes
// nothing, as the API server does. interleave, when set, runs before the
// first delete of the launch key and plays what may happen between the
// launcher's read and its delete.
func uidPreconditions(t *testing.T, cs *fake.Clientset, interleave func(tracker k8stesting.ObjectTracker)) *atomic.Int32 {
	t.Helper()
	var deletes atomic.Int32
	cs.PrependReactor("delete", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		del := action.(k8stesting.DeleteActionImpl)
		tracker := cs.Tracker()
		if deletes.Add(1) == 1 && interleave != nil {
			interleave(tracker)
		}
		obj, err := tracker.Get(del.GetResource(), del.GetNamespace(), del.GetName())
		if err != nil {
			return true, nil, err
		}
		current := obj.(metav1.Object).GetUID()
		if pre := del.DeleteOptions.Preconditions; pre != nil && pre.UID != nil && *pre.UID != current {
			return true, nil, apierrors.NewConflict(del.GetResource().GroupResource(), del.GetName(), fmt.Errorf("Precondition failed: UID in precondition: %v, UID in object meta: %v", *pre.UID, current))
		}
		return false, nil, nil // the tracker deletes the object under the name
	})
	return &deletes
}

// markedJob is a Job under the launch key committed by the given create
// request, with the UID the API server assigned it.
func markedJob(uid string, request int) *batchv1.Job {
	return &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: launch.LaunchKey, Namespace: namespace, Labels: labels("att_1"), Annotations: map[string]string{"anvilkit.io/create-request": strconv.Itoa(request)}, UID: types.UID(uid)}}
}

// The delete is bound to the Job the launcher observed. Between its read of
// B (request 2, accounted) and its delete, B disappears and the unaccounted
// request A commits a Job under the same name; a delete by name alone would
// destroy A's Job before the caller ever recorded it. Bound to B's UID the
// delete is refused by the API server's precondition, the launcher
// re-observes the key, finds A's marker among the unaccounted requests and
// reports A's Job unstopped. Once the caller has accounted for A, the next
// call stops A's Job under its own UID and confirms the key empty.
func TestDeleteJobIsBoundToTheObservedJobUID(t *testing.T) {
	cs := fake.NewClientset(markedJob("job-uid-b", 2))
	deletes := uidPreconditions(t, cs, func(tracker k8stesting.ObjectTracker) {
		gvr := batchv1.SchemeGroupVersion.WithResource("jobs")
		require.NoError(t, tracker.Delete(gvr, namespace, launch.LaunchKey))
		require.NoError(t, tracker.Add(markedJob("job-uid-a", 1)))
	})
	l := k8s.NewWithClient(cs, namespace)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	obs, err := l.DeleteJob(ctx, activities.DeleteJobInput{Launch: launch, Unaccounted: []int{1}}, func() {})
	require.NoError(t, err)
	require.Equal(t, "job-uid-a", obs.JobUID, "the Job now under the name is A's, re-observed after the refused delete")
	require.Equal(t, []int{1}, obs.Requests, "A's Job is reported as the unaccounted evidence")
	current, err := cs.BatchV1().Jobs(namespace).Get(ctx, launch.LaunchKey, metav1.GetOptions{})
	require.NoError(t, err, "A's Job survives until its evidence is recorded")
	require.Equal(t, types.UID("job-uid-a"), current.UID)
	require.Equal(t, int32(1), deletes.Load(), "one delete was requested, for B, and it deleted nothing")

	// Recorded by the caller: the next call stops A under its own UID and
	// confirms the key empty.
	stopped, err := l.DeleteJob(ctx, activities.DeleteJobInput{Launch: launch}, func() {})
	require.NoError(t, err)
	require.True(t, stopped.Empty())
	_, err = cs.BatchV1().Jobs(namespace).Get(ctx, launch.LaunchKey, metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err), "A's Job was stopped once accounted for")
	require.Equal(t, int32(2), deletes.Load())
}

// One create request is one HTTP request also across redirects: the
// standard client re-sends a POST whose answer is 307 or 308 to the new
// location, with the body, which would be a second physical create of the
// same ordinal behind a proxy that redirects while the API server still
// processes the original. The launcher's client does not follow a redirect
// of a create; the redirect answer is not a definitive rejection, so the
// request stays unresolved for the Workflow's ledger. Reads keep following
// redirects.
func TestCreateJobDoesNotFollowARedirectOfTheCreate(t *testing.T) {
	in := activities.CreateJobInput{Launch: launch, ProfileID: "local-check-v1", Deadline: time.Now().Add(time.Minute), Request: 1}
	for _, code := range []int{http.StatusTemporaryRedirect, http.StatusPermanentRedirect} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			var posts, redirectedPosts atomic.Int32
			job, _ := runningLaunch("att_1")
			srv, l := apiServer(t, func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				if r.Method == http.MethodPost {
					posts.Add(1)
				}
				if !strings.HasPrefix(r.URL.Path, "/redirected/") {
					w.Header().Set("Location", "/redirected"+r.URL.Path)
					w.WriteHeader(code)
					return
				}
				if r.Method == http.MethodPost {
					redirectedPosts.Add(1)
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusCreated)
				_ = json.NewEncoder(w).Encode(job)
			})
			_, err := l.CreateJob(context.Background(), in)
			require.Error(t, err, "the redirect answer is not the Job")
			require.False(t, activities.IsNotCreated(err), "a redirect does not say the create was refused: the request stays unresolved")
			require.Empty(t, activities.RefusalCode(err))
			require.Equal(t, int32(1), posts.Load(), "the launcher sends the create once")
			require.Equal(t, int32(0), redirectedPosts.Load(), "nothing was re-sent to the redirect target")

			// Reads still follow the redirect: the existing Job is read
			// through the redirect target.
			ref, err := l.CreateJob(context.Background(), activities.CreateJobInput{Launch: launch, ProfileID: "local-check-v1", Deadline: time.Now().Add(-time.Second)})
			require.NoError(t, err)
			require.Equal(t, "job-uid-1", ref.JobUID)
			require.Equal(t, int32(1), posts.Load(), "the read sent no POST")

			// The same answer through the standard client-go client: the
			// POST is re-sent to the redirect target, which is what the
			// launcher's client prevents.
			posts.Store(0)
			plain, err := kubernetes.NewForConfig(&rest.Config{Host: srv.URL})
			require.NoError(t, err)
			_, err = plain.BatchV1().Jobs(namespace).Create(context.Background(), &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: launch.LaunchKey}}, metav1.CreateOptions{})
			require.NoError(t, err)
			require.Equal(t, int32(2), posts.Load(), "the standard client sends the create twice across the redirect")
			require.Equal(t, int32(1), redirectedPosts.Load())
		})
	}
}

// The physical owner is the Pod whose controller owner reference names the
// observed Job. A Pod with an owner reference to the same Job that is not
// the controller reference (controller false, or the field absent) is not
// the Job's own Pod, however early it was created or finished: it never
// ends the observation and is reported after the owner, also when the
// three Pods were created within the same second and the API server lists
// them by name before the owner.
func TestObserveJobOwnerIsTheControllerReference(t *testing.T) {
	for _, tc := range []struct {
		name       string
		controller *bool
	}{
		{"controller false", ptr.To(false)},
		{"controller field absent", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			job, _ := runningLaunch("att_1")
			created := metav1.NewTime(time.Now().Truncate(time.Second))
			terminated := corev1.PodStatus{Phase: corev1.PodSucceeded, ContainerStatuses: []corev1.ContainerStatus{{Name: "fixture", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0, Message: `{"dup":true}`}}}}}
			dup := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: launch.LaunchKey + "-aaaa1", Namespace: namespace, Labels: labels("att_1"), UID: types.UID("pod-dup"), CreationTimestamp: created,
				OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: launch.LaunchKey, UID: job.UID, Controller: tc.controller}}},
				Status: terminated}
			own := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: launch.LaunchKey + "-zzzz1", Namespace: namespace, Labels: labels("att_1"), UID: types.UID("pod-own"), CreationTimestamp: created,
				OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: launch.LaunchKey, UID: job.UID, Controller: ptr.To(true)}}},
				Status: corev1.PodStatus{Phase: corev1.PodRunning}}
			cs := fake.NewClientset(job, dup, own)
			l := k8s.NewWithClient(cs, namespace)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			type result struct {
				obs activities.JobObservation
				err error
			}
			done := make(chan result, 1)
			go func() {
				obs, err := l.ObserveJob(ctx, activities.ObserveJobInput{Launch: launch, Deadline: time.Now().Add(time.Minute)}, func() {})
				done <- result{obs, err}
			}()
			select {
			case r := <-done:
				t.Fatalf("the observation ended on a Pod that is not the Job's own: %+v %v", r.obs, r.err)
			case <-time.After(2500 * time.Millisecond):
			}
			own.Status = corev1.PodStatus{Phase: corev1.PodSucceeded, ContainerStatuses: []corev1.ContainerStatus{{Name: "fixture", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0, Message: "{}"}}}}}
			_, err := cs.CoreV1().Pods(namespace).UpdateStatus(ctx, own, metav1.UpdateOptions{})
			require.NoError(t, err)
			select {
			case r := <-done:
				require.NoError(t, r.err)
				require.Len(t, r.obs.Pods, 2)
				require.Equal(t, "pod-own", r.obs.Pods[0].PodUID, "the Pod under the Job's controller reference is the physical owner")
				require.Equal(t, "{}", r.obs.Pods[0].TerminationMessage)
				require.Equal(t, "pod-dup", r.obs.Pods[1].PodUID, "the other Pod is reported after the owner, never promoted")
			case <-time.After(5 * time.Second):
				t.Fatal("ObserveJob did not end on the owner's completion")
			}
		})
	}
}

// When only Pods linger under the launch key, the Job identity comes from
// their controller owner reference to a Job of the launch key's name, from
// the earliest such Pod; an owner reference that is not the controller
// reference identifies nothing.
func TestObserveLaunchRecoversTheJobIdentityFromTheControllerReference(t *testing.T) {
	created := metav1.NewTime(time.Now().Truncate(time.Second))
	later := metav1.NewTime(created.Add(time.Second))
	noncontroller := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: launch.LaunchKey + "-aaaa1", Namespace: namespace, Labels: labels("att_1"), UID: types.UID("pod-non"), CreationTimestamp: created,
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: launch.LaunchKey, UID: types.UID("job-uid-other"), Controller: ptr.To(false)}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	in := activities.ObserveLaunchInput{Launch: launch, SettleWindow: time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	l := k8s.NewWithClient(fake.NewClientset(noncontroller), namespace)
	obs, err := l.ObserveLaunch(ctx, in, func() {})
	require.NoError(t, err, "the Pod is reported")
	require.Empty(t, obs.JobUID, "a non-controller reference does not identify the Job")
	require.Len(t, obs.Pods, 1)

	first := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: launch.LaunchKey + "-zzzz1", Namespace: namespace, Labels: labels("att_1"), UID: types.UID("pod-first"), CreationTimestamp: created,
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: launch.LaunchKey, UID: types.UID("job-uid-1"), Controller: ptr.To(true)}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	second := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: launch.LaunchKey + "-bbbb1", Namespace: namespace, Labels: labels("att_1"), UID: types.UID("pod-second"), CreationTimestamp: later,
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: launch.LaunchKey, UID: types.UID("job-uid-2"), Controller: ptr.To(true)}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	l = k8s.NewWithClient(fake.NewClientset(noncontroller, second, first), namespace)
	obs, err = l.ObserveLaunch(ctx, in, func() {})
	require.NoError(t, err)
	require.Equal(t, "job-uid-1", obs.JobUID, "the earliest controller-owned Pod names the Job")
	require.Len(t, obs.Pods, 3)
	require.Equal(t, "pod-first", obs.Pods[0].PodUID, "the identified Job's own Pod comes first")
}

// In evidence-first mode (a recovery that owns no ledger of the launch's
// create requests) DeleteJob stops only objects whose marker the caller
// recorded; an object of any other request is reported unstopped, so
// nothing is deleted before its evidence is recorded.
func TestDeleteJobEvidenceFirstStopsOnlyRecordedObjects(t *testing.T) {
	cs := fake.NewClientset()
	l := k8s.NewWithClient(cs, namespace)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	b, err := l.CreateJob(ctx, activities.CreateJobInput{Launch: launch, ProfileID: "local-check-v1", Deadline: time.Now().Add(time.Minute), Request: 2})
	require.NoError(t, err)
	podB := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: launch.LaunchKey + "-b", Namespace: namespace, Labels: labels("att_1"), Annotations: map[string]string{"anvilkit.io/create-request": "2"}, UID: types.UID("pod-uid-b")}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	_, err = cs.CoreV1().Pods(namespace).Create(ctx, podB, metav1.CreateOptions{})
	require.NoError(t, err)

	reported, err := l.DeleteJob(ctx, activities.DeleteJobInput{Launch: launch, EvidenceFirst: true, Recorded: []int{1}}, func() {})
	require.NoError(t, err)
	require.Equal(t, b.JobUID, reported.JobUID, "the unrecorded object is reported, with its identity")
	require.Equal(t, []int{2}, reported.Requests)
	require.Len(t, reported.Pods, 1)
	_, err = cs.BatchV1().Jobs(namespace).Get(ctx, launch.LaunchKey, metav1.GetOptions{})
	require.NoError(t, err, "nothing was deleted before the caller recorded the object")

	type result struct {
		obs activities.LaunchObservation
		err error
	}
	done := make(chan result, 1)
	go func() {
		obs, err := l.DeleteJob(ctx, activities.DeleteJobInput{Launch: launch, EvidenceFirst: true, Recorded: []int{1, 2}}, func() {})
		done <- result{obs, err}
	}()
	require.Eventually(t, func() bool {
		_, err := cs.BatchV1().Jobs(namespace).Get(ctx, launch.LaunchKey, metav1.GetOptions{})
		return apierrors.IsNotFound(err)
	}, 5*time.Second, 50*time.Millisecond, "the recorded object is deleted")
	require.NoError(t, cs.CoreV1().Pods(namespace).Delete(ctx, podB.Name, metav1.DeleteOptions{}))
	select {
	case r := <-done:
		require.NoError(t, r.err)
		require.True(t, r.obs.Empty(), "the key is confirmed empty only after the Pod is gone")
	case <-time.After(5 * time.Second):
		t.Fatal("DeleteJob did not confirm the key empty")
	}
}

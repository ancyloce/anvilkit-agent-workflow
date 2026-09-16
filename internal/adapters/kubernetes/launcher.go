// Package kubernetes is the fixed-template Job launcher (DD-03 §4). It
// accepts a reviewed profile id and typed inputs, never an arbitrary image,
// command, volume or environment; the image is pinned by digest, the Job has
// one completion, no retries, no ServiceAccount token and an absolute
// deadline. Observation reads Job/Pod state from the API server, which is
// the trusted backend evidence Control registers.
package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptrace"
	"slices"
	"sort"
	"strconv"
	"sync/atomic"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/ptr"

	"github.com/ancyloce/anvilkit-agent-contracts/go/jobschema"
	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
)

const (
	labelLaunchKey = "anvilkit.io/launch-key"
	labelAttempt   = "anvilkit.io/attempt-id"
	labelProfile   = "anvilkit.io/profile-id"
	// annotationRequest marks the Job and its Pods with the ordinal of the
	// create request that made them, so an object observed under the launch
	// key is attributed to the request that committed it.
	annotationRequest = "anvilkit.io/create-request"
	pollInterval      = time.Second
)

type Launcher struct {
	client    kubernetes.Interface
	namespace string
	options   Options
}

// New builds the launcher from the launcher identity: a kubeconfig file for
// a worker outside the cluster, or, with an empty path, the Pod's mounted
// ServiceAccount token and CA (client-go's in-cluster configuration). Both
// are the same client-go REST configuration; the identity's permissions
// (Jobs create/get/list/watch/delete and Pods read in the components
// namespace) are granted by the deployment, never assumed here.
func New(kubeconfig, namespace string, o Options) (*Launcher, error) {
	var cfg *rest.Config
	var err error
	if kubeconfig == "" {
		cfg, err = rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("launcher in-cluster identity (no kubernetes.kubeconfig): %w", err)
		}
	} else {
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, fmt.Errorf("launcher kubeconfig: %w", err)
		}
	}
	return NewForConfig(cfg, namespace, o)
}

// NewForConfig builds the launcher's client from a REST configuration so
// that every create is one HTTP request: the transport is wrapped (see
// oneRequestPerCreate) and the HTTP client does not follow a redirect of a
// POST (see createRedirects).
func NewForConfig(cfg *rest.Config, namespace string, o Options) (*Launcher, error) {
	cfg = rest.CopyConfig(cfg)
	cfg.Wrap(func(rt http.RoundTripper) http.RoundTripper { return oneRequestPerCreate{next: rt} })
	transport, err := rest.TransportFor(cfg)
	if err != nil {
		return nil, err
	}
	cs, err := kubernetes.NewForConfigAndClient(cfg, &http.Client{Transport: transport, Timeout: cfg.Timeout, CheckRedirect: createRedirects})
	if err != nil {
		return nil, err
	}
	return &Launcher{client: cs, namespace: namespace, options: withDefaults(o)}, nil
}

// withDefaults keeps the P05 fixture launchable when no profile list was
// configured (tests); an explicit list is used as given.
func withDefaults(o Options) Options {
	if o.EnabledProfiles == nil {
		o.EnabledProfiles = []string{"local-check-v1"}
	}
	return o
}

// createRedirects is the HTTP client's redirect policy: a redirect of a
// POST is returned as the answer instead of being followed. The standard
// client re-sends a POST whose answer is 307 or 308 to the new location
// with its body, which behind a proxy that redirects while the API server
// still processes the original would be a second physical create of the
// same ordinal. The redirect answer is not a rejection (rejectionCodes), so
// the create stays unresolved for the Workflow's ledger. Every other
// request keeps the standard policy, so reads follow redirects as before.
func createRedirects(req *http.Request, via []*http.Request) error {
	if via[0].Method == http.MethodPost {
		return http.ErrUseLastResponse
	}
	if len(via) >= 10 {
		return errors.New("stopped after 10 redirects")
	}
	return nil
}

// NewWithClient is used by tests with a fake clientset; the options
// default to the P05 fixture profile alone.
func NewWithClient(cs kubernetes.Interface, namespace string, opts ...Options) *Launcher {
	var o Options
	if len(opts) > 0 {
		o = opts[0]
	}
	return &Launcher{client: cs, namespace: namespace, options: withDefaults(o)}
}

// oneRequestPerCreate removes the Retry-After header from every answer to a
// POST. client-go re-sends a request whose 429 or 5xx answer carries
// Retry-After, up to ten times and also for a POST whose body it holds as
// bytes (rest.Request.request, checkWait); a proxy that answers so while the
// API server still processes the original create would make a second HTTP
// request of the same ordinal succeed and hide the first, which could commit
// after the second Job is deleted. Without the header client-go returns that
// answer, the launcher classifies the request unresolved, and only the
// Workflow issues another request, under the next ordinal. Reads are not
// affected: their answers keep the header, and the Activity's own retry
// covers them.
type oneRequestPerCreate struct{ next http.RoundTripper }

func (t oneRequestPerCreate) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.next.RoundTrip(req)
	if resp != nil && req.Method == http.MethodPost {
		resp.Header.Del("Retry-After")
	}
	return resp, err
}

// jobSpec renders the fixed template of a reviewed profile: the fixture
// layout for a profile without candidate code and sidecar, the harness
// layout (DD-03 §5) for a profile that names the access sidecar. Requests
// supply identities, typed inputs and the deadline; every image, command,
// environment, volume and privilege comes from the profile and this
// launcher's fixed templates.
func (l *Launcher) jobSpec(in activities.CreateJobInput, profile jobschema.Profile, now time.Time) (*batchv1.Job, error) {
	labels := map[string]string{labelLaunchKey: in.Launch.LaunchKey, labelAttempt: in.Launch.AttemptID, labelProfile: profile.ProfileID, labelCandidateCode: strconv.FormatBool(profile.CandidateCode)}
	var annotations map[string]string
	if in.Request > 0 {
		annotations = map[string]string{annotationRequest: strconv.Itoa(in.Request)}
	}
	var podSpec corev1.PodSpec
	var err error
	if profile.SidecarImage != nil {
		envelope, eerr := launchEnvelope(in, profile)
		if eerr != nil {
			return nil, eerr
		}
		podSpec, err = l.harnessPodSpec(in, profile, envelope)
	} else {
		if profile.CandidateCode {
			return nil, fmt.Errorf("profile %s runs candidate code without the harness layout", profile.ProfileID)
		}
		podSpec, err = l.fixturePodSpec(in, profile)
	}
	if err != nil {
		return nil, err
	}
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: in.Launch.LaunchKey, Labels: labels, Annotations: annotations},
		Spec: batchv1.JobSpec{
			Completions:           ptr.To[int32](1),
			Parallelism:           ptr.To[int32](1),
			BackoffLimit:          ptr.To[int32](0),
			ActiveDeadlineSeconds: ptr.To(remainingDeadline(in, profile, now)),
			Template:              corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: annotations}, Spec: podSpec},
		},
	}, nil
}

// Render returns the Job a create request would submit, for offline
// policy checks and inspection; nothing is created.
func (l *Launcher) Render(in activities.CreateJobInput, now time.Time) (*batchv1.Job, error) {
	profile, err := l.profile(in.ProfileID)
	if err != nil {
		return nil, err
	}
	return l.jobSpec(in, profile, now)
}

// profile resolves a reviewed profile and checks that this environment
// enabled it; a profile that is reviewed but not enabled here is refused
// exactly like an unreviewed one.
func (l *Launcher) profile(id string) (jobschema.Profile, error) {
	profile, err := jobschema.ProfileByID(id)
	if err != nil {
		return jobschema.Profile{}, activities.Refused("PROFILE_UNQUALIFIED", err)
	}
	if !l.enabled(id) {
		return jobschema.Profile{}, activities.Refused("PROFILE_UNQUALIFIED", fmt.Errorf("profile %s is reviewed but not enabled in this environment (kubernetes.enabled_profiles)", id))
	}
	return profile, nil
}

// requestOf reads the create request marker of an object; 0 when it has none.
func requestOf(meta metav1.ObjectMeta) int {
	n, err := strconv.Atoi(meta.Annotations[annotationRequest])
	if err != nil || n < 1 {
		return 0
	}
	return n
}

// traceWrite attaches a client trace to ctx that records whether a request
// was written to the wire in full. A request that failed before that point
// cannot have reached the API server, which is the not-sent evidence a
// failed create needs to count as not created; after it the fate of the
// request is unknown until the launch key is observed.
func traceWrite(ctx context.Context) (context.Context, func() bool) {
	var written atomic.Bool
	trace := &httptrace.ClientTrace{WroteRequest: func(info httptrace.WroteRequestInfo) {
		if info.Err == nil {
			written.Store(true)
		}
	}}
	return httptrace.WithClientTrace(ctx, trace), written.Load
}

// rejectionCodes are the client-error answers that refuse a request before
// it is processed, by the API server after validation or authorization, or
// by a proxy before forwarding: definitive evidence that nothing was
// stored. 408 is absent on purpose: a proxy answers it when the upstream did
// not finish in time, which proves nothing about the create; any other
// code, including non-standard ones such as 499, is not evidence either.
var rejectionCodes = []int32{
	http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusMethodNotAllowed,
	http.StatusNotAcceptable, http.StatusConflict, http.StatusGone, http.StatusLengthRequired, http.StatusRequestEntityTooLarge,
	http.StatusRequestURITooLong, http.StatusUnsupportedMediaType, http.StatusExpectationFailed, http.StatusUnprocessableEntity,
	http.StatusTooManyRequests, http.StatusRequestHeaderFieldsTooLarge,
}

// rejected reports whether the answer to a request is a definitive
// rejection (rejectionCodes): the request was refused unprocessed, so the
// backend holds nothing from it.
func rejected(err error) bool {
	var status apierrors.APIStatus
	if !errors.As(err, &status) {
		return false
	}
	return slices.Contains(rejectionCodes, status.Status().Code)
}

// CreateJob is idempotent by launch key: an existing Job with the name is
// returned, never recreated, so a lost create response is repaired by the
// same call. One call is one HTTP request (oneRequestPerCreate). The Job
// reports the marker of the request that committed it, which may be an
// earlier request of this launch whose answer was lost. A request that
// fails is classified for the caller's ledger: NotCreated when the answer
// is a definitive rejection or the request never reached the wire,
// otherwise unresolved (unanswered, timed out on the way, a server or
// gateway error), because the server may still be processing a request
// that was written, and a later create could find its object.
func (l *Launcher) CreateJob(ctx context.Context, in activities.CreateJobInput) (activities.JobRef, error) {
	profile, err := l.profile(in.ProfileID)
	if err != nil {
		return activities.JobRef{}, err
	}
	if profile.Image.Digest != in.Launch.ImageDigest {
		return activities.JobRef{}, activities.Refused("STALE_EXECUTION", fmt.Errorf("launch %s was inventoried for image %s, profile pins %s", in.Launch.LaunchID, in.Launch.ImageDigest, profile.Image.Digest))
	}
	jobs := l.client.BatchV1().Jobs(l.namespace)
	existingRef := func() (activities.JobRef, error) {
		existing, err := jobs.Get(ctx, in.Launch.LaunchKey, metav1.GetOptions{})
		if err != nil {
			return activities.JobRef{}, err
		}
		if existing.Labels[labelAttempt] != in.Launch.AttemptID {
			return activities.JobRef{}, activities.Refused("STALE_EXECUTION", fmt.Errorf("job %s belongs to attempt %s", in.Launch.LaunchKey, existing.Labels[labelAttempt]))
		}
		return activities.JobRef{JobUID: string(existing.UID), Request: requestOf(existing.ObjectMeta)}, nil
	}
	// A retry that lands after the absolute deadline creates nothing; the
	// original Job, if one exists, is still returned so it can be observed
	// and cleaned up under its identity.
	now := time.Now()
	if !in.Deadline.After(now) {
		if ref, err := existingRef(); err == nil {
			return ref, nil
		}
		return activities.JobRef{}, activities.Refused("DEADLINE_EXCEEDED", fmt.Errorf("launch %s deadline %s passed before the Job was created", in.Launch.LaunchID, in.Deadline.UTC().Format(time.RFC3339)))
	}
	spec, err := l.jobSpec(in, profile, now)
	if err != nil {
		return activities.JobRef{}, activities.Refused("PROFILE_UNQUALIFIED", err)
	}
	createCtx, written := traceWrite(ctx)
	created, err := jobs.Create(createCtx, spec, metav1.CreateOptions{})
	switch {
	case err == nil:
		return activities.JobRef{JobUID: string(created.UID), Request: requestOf(created.ObjectMeta)}, nil
	case apierrors.IsAlreadyExists(err):
		// This request stored nothing; the object belongs to an earlier one.
		ref, err := existingRef()
		if err != nil && activities.RefusalCode(err) == "" {
			return activities.JobRef{}, activities.NotCreated(fmt.Errorf("job %s exists but could not be read: %w", in.Launch.LaunchKey, err))
		}
		return ref, err
	case rejected(err) || !written():
		return activities.JobRef{}, activities.NotCreated(err)
	default:
		return activities.JobRef{}, fmt.Errorf("job %s create request %d unanswered: %w", in.Launch.LaunchKey, in.Request, err)
	}
}

// ObserveJob polls until the Job reaches a terminal condition, its physical
// owner is terminal, or the deadline passes, heartbeating on every poll. The
// physical owner is the first Pod, in creation order, that the Job itself
// owns (its controller owner reference); a Pod under the launch-key label
// that belongs to another owner is a duplicate, reported after the Job's
// own Pods and never the owner, however early it was created. It returns
// every Pod the API server reports for the launch, in that order.
func (l *Launcher) ObserveJob(ctx context.Context, in activities.ObserveJobInput, heartbeat func()) (activities.JobObservation, error) {
	jobs := l.client.BatchV1().Jobs(l.namespace)
	pods := l.client.CoreV1().Pods(l.namespace)
	for {
		heartbeat()
		job, err := jobs.Get(ctx, in.Launch.LaunchKey, metav1.GetOptions{})
		if err != nil {
			return activities.JobObservation{}, err
		}
		list, err := pods.List(ctx, metav1.ListOptions{LabelSelector: labelLaunchKey + "=" + in.Launch.LaunchKey})
		if err != nil {
			return activities.JobObservation{}, err
		}
		obs := activities.JobObservation{JobUID: string(job.UID)}
		for _, cond := range job.Status.Conditions {
			if (cond.Type == batchv1.JobFailed || cond.Type == batchv1.JobComplete) && cond.Status == corev1.ConditionTrue {
				obs.Reason = cond.Reason
			}
		}
		terminal := obs.Reason != ""
		for i, pod := range podsInOwnerOrder(list.Items, job.UID) {
			po := observePod(pod)
			// The observation ends on the Job's own condition or on the
			// physical owner (the Job's first own Pod): a duplicate Pod
			// finishing earlier or later never cuts the owner's observation
			// short, and a duplicate never ends it.
			if i == 0 && ownedBy(pod, job.UID) && (po.Reason == "ErrImagePull" || po.Reason == "ImagePullBackOff" || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed) {
				terminal = true
			}
			obs.Pods = append(obs.Pods, po)
		}
		if terminal || time.Now().After(in.Deadline.Add(30*time.Second)) {
			if !terminal && obs.Reason == "" {
				obs.Reason = "DeadlineExceeded"
			}
			return obs, nil
		}
		select {
		case <-ctx.Done():
			return activities.JobObservation{}, ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}

// AwaitOwner polls until the Job's physical owner (its first own Pod)
// exists, so the caller can register the real Pod UID with Control while
// the Pod runs: a harness profile's sidecar receives its execution scope
// only from that registration. It returns the current observation as soon
// as the owner is reported (whatever its phase), or when the Job is
// already terminal, or when the deadline passes; it never deletes and
// never ends the observation on a duplicate Pod.
func (l *Launcher) AwaitOwner(ctx context.Context, in activities.ObserveJobInput, heartbeat func()) (activities.JobObservation, error) {
	jobs := l.client.BatchV1().Jobs(l.namespace)
	pods := l.client.CoreV1().Pods(l.namespace)
	for {
		heartbeat()
		job, err := jobs.Get(ctx, in.Launch.LaunchKey, metav1.GetOptions{})
		if err != nil {
			return activities.JobObservation{}, err
		}
		list, err := pods.List(ctx, metav1.ListOptions{LabelSelector: labelLaunchKey + "=" + in.Launch.LaunchKey})
		if err != nil {
			return activities.JobObservation{}, err
		}
		obs := activities.JobObservation{JobUID: string(job.UID)}
		for _, cond := range job.Status.Conditions {
			if (cond.Type == batchv1.JobFailed || cond.Type == batchv1.JobComplete) && cond.Status == corev1.ConditionTrue {
				obs.Reason = cond.Reason
			}
		}
		owned := false
		for i, pod := range podsInOwnerOrder(list.Items, job.UID) {
			if i == 0 && ownedBy(pod, job.UID) {
				owned = true
			}
			obs.Pods = append(obs.Pods, observePod(pod))
		}
		if owned || obs.Reason != "" || time.Now().After(in.Deadline) {
			if !owned && obs.Reason == "" {
				obs.Reason = "DeadlineExceeded"
			}
			return obs, nil
		}
		select {
		case <-ctx.Done():
			return activities.JobObservation{}, ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}

// ownedBy reports whether the Job with the given UID is the Pod's
// controller: the Job controller sets the controller owner reference on
// every Pod it creates, and an owner reference to the same Job that is
// not the controller reference does not make a Pod the Job's own.
func ownedBy(pod corev1.Pod, jobUID types.UID) bool {
	return jobUID != "" && controllerJobUID(pod, "") == jobUID
}

// controllerJobUID returns the UID of the Job that controls the Pod (its
// controller owner reference of kind Job), or "" when the Pod has none. A
// non-empty name restricts the match to a Job of that name.
func controllerJobUID(pod corev1.Pod, name string) types.UID {
	for _, owner := range pod.OwnerReferences {
		if owner.Kind == "Job" && owner.Controller != nil && *owner.Controller && (name == "" || owner.Name == name) {
			return owner.UID
		}
	}
	return ""
}

// podsInOwnerOrder orders the Pods under the launch key: the Job's own Pods
// first, then every other Pod, each group in creation order. Creation
// timestamps have one-second resolution, so ownership, not the timestamp,
// keeps a duplicate created in the same second from sorting before the
// Job's own Pod.
func podsInOwnerOrder(items []corev1.Pod, jobUID types.UID) []corev1.Pod {
	sort.SliceStable(items, func(i, j int) bool {
		if oi, oj := ownedBy(items[i], jobUID), ownedBy(items[j], jobUID); oi != oj {
			return oi
		}
		return items[i].CreationTimestamp.Before(&items[j].CreationTimestamp)
	})
	return items
}

// launchObservation is what the API server holds under the launch key: the
// Job (or, from lingering Pods, its identity), the Pods in owner order and
// the create request markers of all of them.
func launchObservation(job *batchv1.Job, jobGone bool, pods []corev1.Pod, launchKey string) activities.LaunchObservation {
	obs := activities.LaunchObservation{}
	requests := map[int]bool{}
	var jobUID types.UID
	if !jobGone {
		jobUID = job.UID
		obs.JobUID = string(job.UID)
		requests[requestOf(job.ObjectMeta)] = true
	} else {
		// The Job identity survives on its Pods' controller owner
		// reference; the earliest such Pod names it when Pods of more than
		// one Job of the name linger (a stopping one and a later one).
		for _, pod := range podsInOwnerOrder(pods, "") {
			if uid := controllerJobUID(pod, launchKey); uid != "" {
				jobUID = uid
				obs.JobUID = string(uid)
				break
			}
		}
	}
	for _, pod := range podsInOwnerOrder(pods, jobUID) {
		requests[requestOf(pod.ObjectMeta)] = true
		obs.Pods = append(obs.Pods, observePod(pod))
	}
	for r := range requests {
		if r > 0 {
			obs.Requests = append(obs.Requests, r)
		}
	}
	sort.Ints(obs.Requests)
	return obs
}

// observePod maps what the API server reports for one Pod of the fixture
// into the trusted observation Control records.
func observePod(pod corev1.Pod) activities.PodObservation {
	po := activities.PodObservation{PodUID: string(pod.UID), Phase: phaseOf(pod), Reason: pod.Status.Reason, ObservedAt: time.Now().UTC()}
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name != containerFixture && cs.Name != containerSupervisor {
			continue
		}
		if cs.State.Terminated != nil {
			code := cs.State.Terminated.ExitCode
			po.ExitCode = &code
			po.TerminationMessage = cs.State.Terminated.Message
			if po.Reason == "" {
				po.Reason = cs.State.Terminated.Reason
			}
		} else if cs.State.Waiting != nil && po.Reason == "" {
			po.Reason = cs.State.Waiting.Reason
		}
	}
	return po
}

func phaseOf(pod corev1.Pod) string {
	switch pod.Status.Phase {
	case corev1.PodPending:
		return "pending"
	case corev1.PodRunning:
		return "running"
	case corev1.PodSucceeded:
		return "succeeded"
	case corev1.PodFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// ObserveLaunch reports what the API server holds under the launch key
// while a create request of the launch is unaccounted for: the Job and
// every Pod labeled with the key, with the create request markers they
// carry, as soon as any is reported. It deletes nothing, so a call whose
// completion receipt is lost is repeated against the same objects. Absence
// is not an observation: the in-flight create may still land, so the call
// keeps looking for SettleWindow and then returns an error, which leaves
// the cleanup unknown; the caller continues recovery under the same launch
// identity and elapsed time never completes it.
func (l *Launcher) ObserveLaunch(ctx context.Context, in activities.ObserveLaunchInput, heartbeat func()) (activities.LaunchObservation, error) {
	jobs := l.client.BatchV1().Jobs(l.namespace)
	pods := l.client.CoreV1().Pods(l.namespace)
	started := time.Now()
	for {
		heartbeat()
		job, err := jobs.Get(ctx, in.Launch.LaunchKey, metav1.GetOptions{})
		jobGone := apierrors.IsNotFound(err)
		if err != nil && !jobGone {
			return activities.LaunchObservation{}, err
		}
		list, err := pods.List(ctx, metav1.ListOptions{LabelSelector: labelLaunchKey + "=" + in.Launch.LaunchKey})
		if err != nil {
			return activities.LaunchObservation{}, err
		}
		if !jobGone || len(list.Items) > 0 {
			return launchObservation(job, jobGone, list.Items, in.Launch.LaunchKey), nil
		}
		if time.Since(started) >= in.SettleWindow {
			return activities.LaunchObservation{}, fmt.Errorf("job %s create unresolved: neither the Job nor a Pod was observed within %s, so the create is not known to have finished; cleanup stays unknown", in.Launch.LaunchKey, in.SettleWindow)
		}
		select {
		case <-ctx.Done():
			return activities.LaunchObservation{}, fmt.Errorf("job %s create unresolved: observation ended before the Job or a Pod was seen: %w", in.Launch.LaunchKey, ctx.Err())
		case <-time.After(pollInterval):
		}
	}
}

// DeleteJob stops the launch: it requests foreground deletion of the Job
// and polls until the API server reports neither the Job nor any Pod of the
// launch, which it returns as an empty observation. Only that observation
// stops the launch: the delete call itself returns before the Pods have
// stopped. Evidence comes before deletion: whenever an object under the
// launch key carries the marker of a request in in.Unaccounted, the call
// deletes nothing more and returns what it sees, so the caller records the
// object (its Workflow history, Control's instance registration) before
// asking again with that request accounted for; a lost completion receipt
// therefore finds the object still there. Objects of accounted requests,
// and objects without a marker (no launcher of this code leaves one), are
// deleted, re-deleted while the API server still reports the Job. Each
// delete is bound to the Job that was observed, by its UID (the API
// server's deletion precondition): a Job that took the name between the
// read and the delete is never deleted unseen, the refused delete makes
// the call observe the key again, and the marker of that Job decides
// whether it is reported or deleted. When ctx ends first the Job may still
// be running and the error makes the caller record cleanup as unknown,
// never complete.
//
// In EvidenceFirst mode (a recovery that owns no ledger of the launch's
// create requests) the rule is inverted: only objects whose marker is in
// in.Recorded are deleted, any other marked object is reported unstopped,
// so nothing is stopped before the caller recorded what it saw.
func (l *Launcher) DeleteJob(ctx context.Context, in activities.DeleteJobInput, heartbeat func()) (activities.LaunchObservation, error) {
	jobs := l.client.BatchV1().Jobs(l.namespace)
	pods := l.client.CoreV1().Pods(l.namespace)
	for {
		heartbeat()
		job, err := jobs.Get(ctx, in.Launch.LaunchKey, metav1.GetOptions{})
		jobGone := apierrors.IsNotFound(err)
		if err != nil && !jobGone {
			return activities.LaunchObservation{}, err
		}
		list, err := pods.List(ctx, metav1.ListOptions{LabelSelector: labelLaunchKey + "=" + in.Launch.LaunchKey})
		if err != nil {
			return activities.LaunchObservation{}, err
		}
		obs := launchObservation(job, jobGone, list.Items, in.Launch.LaunchKey)
		for _, r := range obs.Requests {
			if slices.Contains(in.Unaccounted, r) || (in.EvidenceFirst && !slices.Contains(in.Recorded, r)) {
				return obs, nil // evidence the caller has not recorded: reported, not deleted
			}
		}
		if jobGone && len(list.Items) == 0 {
			return activities.LaunchObservation{}, nil
		}
		if !jobGone {
			// The observed Job is still reported: it is deleted (again)
			// under its name and UID until the API server reports it gone.
			err := jobs.Delete(ctx, in.Launch.LaunchKey, metav1.DeleteOptions{
				PropagationPolicy: ptr.To(metav1.DeletePropagationForeground),
				Preconditions:     &metav1.Preconditions{UID: ptr.To(job.UID)},
			})
			switch {
			case err == nil, apierrors.IsNotFound(err):
			case apierrors.IsConflict(err):
				// The name holds another Job than the one observed: the
				// observed one is gone and a create of the launch landed.
				// It is observed, and its marker checked against the
				// unaccounted requests, before anything is deleted.
				continue
			default:
				return activities.LaunchObservation{}, err
			}
		}
		select {
		case <-ctx.Done():
			return activities.LaunchObservation{}, fmt.Errorf("job %s not confirmed stopped (job present: %t, pods: %d): %w", in.Launch.LaunchKey, !jobGone, len(list.Items), ctx.Err())
		case <-time.After(pollInterval):
		}
	}
}

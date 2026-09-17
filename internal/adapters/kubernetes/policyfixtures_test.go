package kubernetes_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/yaml"

	"github.com/ancyloce/anvilkit-agent-contracts/go/jobschema"
	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
	k8s "github.com/ancyloce/anvilkit-agent-workflow/internal/adapters/kubernetes"
)

// TestRenderPolicyFixtures writes the Jobs and Pods the launcher renders
// for every enabled profile into deploy/policies/tests/resources when
// ANVILKIT_RENDER_POLICY_FIXTURES names that directory: the admission
// policies are checked offline (deploy/policies/check.sh) against exactly
// what the launcher submits, and the negative variants are derived from
// these files. Without the variable it only checks that the render works.
func TestRenderPolicyFixtures(t *testing.T) {
	l := k8s.NewWithClient(fake.NewClientset(), namespace, k8s.Options{
		Backend: "kind-anvilkit-dev", ImageRegistry: "localhost:5001", EnabledProfiles: []string{"local-check-v1", "harness-wiring-dev-v1", "codegen-fixed-v1", "validator-fixed-dev-v1"},
		SidecarControlAddress: "172.22.0.1:9101", SidecarIdentityMode: "development", CandidateSeccompProfile: "anvilkit/candidate.json",
	})
	now := time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)
	out := os.Getenv("ANVILKIT_RENDER_POLICY_FIXTURES")
	for _, id := range []string{"local-check-v1", "harness-wiring-dev-v1", "codegen-fixed-v1", "validator-fixed-dev-v1"} {
		profile, err := jobschema.ProfileByID(id)
		require.NoError(t, err)
		in := activities.CreateJobInput{
			Launch:    activities.LaunchRef{LaunchID: "lch_policy", AttemptID: "att_policy", LaunchKey: "policy-" + id, ImageDigest: profile.Image.Digest},
			ProfileID: id, Deadline: now.Add(10 * time.Minute), Request: 1, OperationID: "op_policy", ExecutionEpoch: "1", LaunchEpoch: "1",
		}
		if profile.SidecarImage != nil {
			in.Inputs = []activities.LaunchInput{{Name: "fixed-input", Digest: "sha256:abeb263b000189efdd8206ff39eeb4f2f7f3217f1c480491868b46da4a21c6a8"}}
		}
		job, err := l.Render(in, now)
		require.NoError(t, err, id)
		job.TypeMeta = metav1.TypeMeta{APIVersion: "batch/v1", Kind: "Job"}
		job.Namespace = namespace
		pod := &corev1.Pod{
			TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
			ObjectMeta: metav1.ObjectMeta{Name: job.Name + "-x1", Namespace: namespace, Labels: job.Spec.Template.Labels, Annotations: job.Spec.Template.Annotations},
			Spec:       job.Spec.Template.Spec,
		}
		if out == "" {
			continue
		}
		require.NoError(t, os.MkdirAll(out, 0o755))
		for name, obj := range map[string]any{id + "-job.yaml": job, id + "-pod.yaml": pod} {
			raw, err := yaml.Marshal(obj)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(filepath.Join(out, name), append([]byte("# Rendered by the launcher (TestRenderPolicyFixtures); do not edit.\n"), raw...), 0o644))
		}
	}
	_ = batchv1.SchemeGroupVersion
}

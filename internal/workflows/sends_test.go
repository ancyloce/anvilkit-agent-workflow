package workflows_test

import (
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
	"github.com/ancyloce/anvilkit-agent-workflow/internal/workflows"
)

// TestExternalSendsAreSingleAttemptOrReentrant (RB-A20, DD-01 §5): every
// external send these Workflows schedule is recorded with why a retry
// cannot send it twice, and the single-attempt ones are sent once per
// identity when their answer is lost.
func TestExternalSendsAreSingleAttemptOrReentrant(t *testing.T) {
	t.Run("the record covers the inventory, each with a single attempt or its reentry", func(t *testing.T) {
		var names []string
		for name, p := range workflows.ExternalSends {
			names = append(names, name)
			require.True(t, p.SingleAttempt != (p.Reentry != ""), "%s: exactly one of a single attempt or a recorded reentry", name)
		}
		sort.Strings(names)
		require.Equal(t, []string{
			activities.NameAcquireLease, activities.NameActivateRelease, activities.NameAnalyzeRequirements, activities.NamePublishTarget,
			activities.NameRegisterCandidate, activities.NameRegisterReview, activities.NameReleaseLease, activities.NameRenewLease, activities.NameSaveSource,
		}, names, "a new external send is recorded here first")
	})

	sends := func(r []string, kind string) []string {
		var out []string
		for _, s := range r {
			if strings.HasPrefix(s, kind+":") {
				out = append(out, s)
			}
		}
		return out
	}

	t.Run("an acquisition whose answer is lost is sent once and resolved by a query", func(t *testing.T) {
		env, r := generationEnv(t, lifecycleBounds, genScript{
			scope:     activities.ScopeDecision{Allowed: true},
			codegen:   map[uint64]activities.AcceptedStage{0: certified("c0", sourceA, stageA)},
			validator: map[uint64]activities.AcceptedStage{0: certified("v0", evidence)},
			candidate: activities.CandidateRef{EffectID: "eff_1", State: "succeeded", CandidateID: "cand_1"},
			leaseErr:  map[string]bool{"acquire": true},
		})
		env.ExecuteWorkflow(workflows.GenerationWorkflowName, genInput)
		require.True(t, env.IsWorkflowCompleted())
		require.NoError(t, env.GetWorkflowError())
		require.Equal(t, []string{"acquire:1"}, sends(r.leaseSends, "acquire"), "one acquisition, never retried")
		require.Contains(t, r.leaseSends, "query:1", "the original occurrence is asked about")
		require.Equal(t, "succeeded", r.last().Outcome, "the query found the lease held: the generation ran under it")
	})

	t.Run("a renewal whose answer is lost is sent once per occurrence", func(t *testing.T) {
		env, r := generationEnvWith(t, slowBounds, lifecycleBounds, genScript{
			scope: activities.ScopeDecision{Allowed: true}, createNotCreated: 5,
			codegen: map[uint64]activities.AcceptedStage{0: certified("c0", sourceA, stageA)}, validator: map[uint64]activities.AcceptedStage{0: certified("v0", evidence)},
			candidate: activities.CandidateRef{EffectID: "eff_1", State: "succeeded", CandidateID: "cand_1"},
			leaseErr:  map[string]bool{"renew": true},
		})
		env.ExecuteWorkflow(workflows.GenerationWorkflowName, genInput)
		require.True(t, env.IsWorkflowCompleted())
		require.NoError(t, env.GetWorkflowError())
		renewals := sends(r.leaseSends, "renew")
		require.NotEmpty(t, renewals)
		compact := slices.Compact(slices.Clone(renewals))
		require.Equal(t, compact, renewals, "no occurrence is sent twice")
	})

	t.Run("a release whose answer is lost is sent once and observed by queries", func(t *testing.T) {
		env, r := generationEnv(t, lifecycleBounds, genScript{
			scope:     activities.ScopeDecision{Allowed: true},
			codegen:   map[uint64]activities.AcceptedStage{0: certified("c0", sourceA, stageA)},
			validator: map[uint64]activities.AcceptedStage{0: certified("v0", evidence)},
			candidate: activities.CandidateRef{EffectID: "eff_1", State: "succeeded", CandidateID: "cand_1"},
			lease:     map[string]activities.LeaseResult{"query": {State: "lost", Reason: "RELEASED"}},
			leaseErr:  map[string]bool{"release": true},
		})
		env.ExecuteWorkflow(workflows.GenerationWorkflowName, genInput)
		require.True(t, env.IsWorkflowCompleted())
		require.NoError(t, env.GetWorkflowError())
		require.Len(t, sends(r.leaseSends, "release"), 1, "one release, never retried")
		require.Equal(t, "released", r.leases[len(r.leases)-1].State, "the query confirmed it")
	})
}

package control

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
)

// TestControlStatusClassification is P1.6 AC7 at the adapter: a genuine
// BUDGET_EXHAUSTED, a denial and a data-loss verdict are refusals (not
// retried); Control's capacity refusal and an outage stay retryable.
func TestControlStatusClassification(t *testing.T) {
	for _, c := range []struct {
		code    codes.Code
		msg     string
		refusal string
	}{
		{codes.ResourceExhausted, "BUDGET_EXHAUSTED: funding of op_1 exceeds the cap", "BUDGET_EXHAUSTED"},
		{codes.ResourceExhausted, "CAPACITY_EXHAUSTED", ""},
		{codes.PermissionDenied, "FORBIDDEN: not the operation's tenant", "FORBIDDEN"},
		{codes.DataLoss, "RESULT_INTEGRITY: digest mismatch", "RESULT_INTEGRITY"},
		{codes.FailedPrecondition, "STALE_EXECUTION: fenced", "STALE_EXECUTION"},
		{codes.Unavailable, "DEPENDENCY_UNAVAILABLE: database", ""},
		{codes.DeadlineExceeded, "context deadline exceeded", ""},
	} {
		err := nonRetryable(status.Error(c.code, c.msg))
		require.Equal(t, c.refusal, activities.RefusalCode(err), "%s %q", c.code, c.msg)
	}
	plain := errors.New("dial tcp: connection refused")
	require.Equal(t, plain, nonRetryable(plain), "a transport error is not a refusal")
}

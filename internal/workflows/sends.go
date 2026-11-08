package workflows

import "github.com/ancyloce/anvilkit-agent-workflow/internal/activities"

// SendPolicy is why an external send's retry policy cannot send twice
// (DD-01 §5, RB-A20). SingleAttempt: scheduled with MaximumAttempts 1, so a
// lost answer is UNKNOWN and the caller queries the original identity.
// Reentry: the mechanism that makes a retried attempt send nothing new.
type SendPolicy struct {
	SingleAttempt bool
	Reentry       string
}

// ExternalSends records every Activity these Workflows schedule that sends
// to a system outside the Workflow with an effect: DD-01 §5's external send
// after admission and Pagix mutation rows. Job create, observe and stop
// follow their own row (a stable launch key and the trusted backend query).
// A new external send needs an entry here and its proof in sends_test.go.
var ExternalSends = map[string]SendPolicy{
	activities.NameAcquireLease: {SingleAttempt: true},
	activities.NameRenewLease:   {SingleAttempt: true},
	activities.NameReleaseLease: {SingleAttempt: true},
	activities.NameAnalyzeRequirements: {Reentry: "the stable call id (operation, round, call): AdmitModel admits one physical send per call id " +
		"and the Model Proxy answers a repeated call id from its record (P11, DD-02); a new call id is a new admission"},
	activities.NameRegisterCandidate: {Reentry: "Control's single-use effect permit under the command id: a retried attempt finds the permit " +
		"consumed and reads the original effect, querying the upstream under the original identity; never registered again"},
	activities.NameSaveSource: {Reentry: "Control's single-use effect permit under the command id: a retried attempt finds the permit " +
		"consumed and reads the original effect, querying the source authority under the original identity; never saved again"},
	activities.NameRegisterReview:  {Reentry: guardedReentry},
	activities.NamePublishTarget:   {Reentry: guardedReentry},
	activities.NameActivateRelease: {Reentry: guardedReentry},
}

const guardedReentry = "the guarded permit protocol (activities.guarded): one send per consumed permit; a failed send is recorded " +
	"UNKNOWN and never resent; a retried attempt finds the permit consumed and queries the original identity"

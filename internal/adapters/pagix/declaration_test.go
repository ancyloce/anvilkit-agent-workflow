package pagix

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// proven is a declaration that records every DD-06 §2 field and proves some
// of them: publication.publish's not-sent status, and the declaration-wide
// receipts and rate.
const proven = `schemaVersion: 1
apiVersion: test-v1
auth: {issuer: issuer.example, audience: anvilkit-agent-workflow}
receipts: {signature: ed25519, digest: sha256}
pagination: cursor
rate: {requestsPerMinute: 600, timeoutSeconds: 30}
maxExposure: "0 USD (publication is free)"
verified: [receipts, rate]
evidence: outputs/pagix-env07/declaration-proof.md
operations:
  catalog.revision: {method: GET, path: /catalog/revision}
  publication.publish:
    method: PUT
    path: "/publications/{effectId}"
    requestSchema: "#/components/schemas/Publish"
    responseSchema: "#/components/schemas/Receipt"
    conflictStatuses: [409, 423]
    rejectionStatuses: [451]
    notSentStatuses: [429]
    idempotency: {scope: credential, retentionSeconds: 86400}
    verified: [not-sent, idempotency]
    evidence: outputs/pagix-env07/publish-not-sent.md
  activation.activate: {method: PUT, path: "/activations/{effectId}", notSentStatuses: [429]}
`

// TestDeclarationRecordsDD06FieldsAndMarksTheUnproven (RB-A02): a declaration
// carries every DD-06 §2 field, and every capability it does not prove with
// evidence is UNVERIFIED.
func TestDeclarationRecordsDD06FieldsAndMarksTheUnproven(t *testing.T) {
	t.Run("the reference declaration proves nothing", func(t *testing.T) {
		d, err := LoadDeclaration(filepath.Join("..", "..", "..", "tests", "conformance", "pagix", "reference-declaration.yaml"))
		require.NoError(t, err)
		u := d.Unverified()
		require.Len(t, u, len(Capabilities)+len(d.Operations)*len(OperationCapabilities), "every capability of every declared operation")
		require.Contains(t, u, "publication.publish: not-sent")
		require.Contains(t, u, "receipts")
	})

	t.Run("verified capabilities with their records and evidence are proven, nothing else", func(t *testing.T) {
		d, err := ParseDeclaration([]byte(proven))
		require.NoError(t, err)
		u := d.Unverified()
		for _, c := range []string{"receipts", "rate", "publication.publish: not-sent", "publication.publish: idempotency"} {
			require.NotContains(t, u, c)
		}
		for _, c := range []string{"auth", "pagination", "exposure", "publication.publish: schema", "publication.publish: conflict-statuses",
			"activation.activate: not-sent", "catalog.revision: schema"} {
			require.Contains(t, u, c, "recorded but not verified is UNVERIFIED")
		}
		require.Equal(t, []int{409, 423}, d.Operations[OpPublish].ConflictStatuses)
		require.Equal(t, &Idempotency{Scope: "credential", RetentionSeconds: 86400}, d.Operations[OpPublish].Idempotency)
		require.Equal(t, &Receipts{Signature: "ed25519", Digest: "sha256"}, d.Receipts)
		require.True(t, d.Operations[OpPublish].verifies(CapNotSent))
		require.False(t, d.Operations[OpActivate].verifies(CapNotSent))
	})

	for _, tc := range []struct{ name, edit, want string }{
		{"a verified capability without evidence", "evidence: outputs/pagix-env07/publish-not-sent.md\n", "verified capabilities need their evidence"},
		{"a verified capability without its record", "notSentStatuses: [429]\n    idempotency", "verified not-sent without its record"},
		{"an unknown capability", "verified: [not-sent, idempotency]", "unknown capability \"retries\""},
		{"a success status as a class", "rejectionStatuses: [451]", "names 200, not a 4xx or 5xx status"},
		{"a server error as a conflict", "conflictStatuses: [409, 423]", "names 503, not a 4xx status"},
		{"a status in two classes", "notSentStatuses: [429]\n    idempotency", "status 451 is in both"},
		{"an idempotency without retention", "retentionSeconds: 86400", "positive retentionSeconds"},
		{"a rate without a bound", "requestsPerMinute: 600", "positive requestsPerMinute"},
		{"receipts without a digest scheme", "digest: sha256", "receipts need their signature and digest scheme"},
		{"an unknown declaration-wide capability", "verified: [receipts, rate]", "unknown capability \"retries\""},
	} {
		t.Run("refuses "+tc.name, func(t *testing.T) {
			raw := proven
			switch tc.name {
			case "a verified capability without evidence":
				raw = strings.Replace(raw, "    "+tc.edit, "", 1)
			case "a verified capability without its record":
				raw = strings.Replace(raw, tc.edit, "notSentStatuses: []\n    idempotency", 1)
			case "an unknown capability":
				raw = strings.Replace(raw, tc.edit, "verified: [not-sent, idempotency, retries]", 1)
			case "a success status as a class":
				raw = strings.Replace(raw, tc.edit, "rejectionStatuses: [451, 200]", 1)
			case "a server error as a conflict":
				raw = strings.Replace(raw, tc.edit, "conflictStatuses: [409, 503]", 1)
			case "a status in two classes":
				raw = strings.Replace(raw, tc.edit, "notSentStatuses: [429, 451]\n    idempotency", 1)
			case "an idempotency without retention":
				raw = strings.Replace(raw, tc.edit, "retentionSeconds: 0", 1)
			case "a rate without a bound":
				raw = strings.Replace(raw, tc.edit, "requestsPerMinute: 0", 1)
			case "receipts without a digest scheme":
				raw = strings.Replace(raw, tc.edit, `digest: ""`, 1)
			case "an unknown declaration-wide capability":
				raw = strings.Replace(raw, tc.edit, "verified: [receipts, rate, retries]", 1)
			}
			require.NotEqual(t, proven, raw, "the edit applied")
			_, err := ParseDeclaration([]byte(raw))
			require.ErrorContains(t, err, tc.want)
		})
	}

	t.Run("unknown keys stay refused", func(t *testing.T) {
		_, err := ParseDeclaration([]byte(strings.Replace(proven, "pagination: cursor", "pagination: cursor\nretries: 3", 1)))
		require.Error(t, err)
	})
}

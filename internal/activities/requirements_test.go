package activities_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
)

// DEC-055: the brief's editable fields follow Puck's field API at the
// profile's Puck version (0.24.0): a Puck field type and that type's data
// parameters. A fixed choice reaches the team as a select or radio field with
// its options (the i18n Generations of 2026-11-11 failed when it could only be
// text), a list as an array field with its sub-fields.

// analysisModel answers every call with one submit_requirements call carrying args.
type analysisModel struct {
	args string
	sent *activities.ModelCallInput
}

func (m *analysisModel) CallModel(_ context.Context, in activities.ModelCallInput, _ func()) (activities.ModelCallResult, error) {
	if m.sent != nil {
		*m.sent = in
	}
	return activities.ModelCallResult{CallID: in.CallID, State: "succeeded",
		ToolCalls: []activities.ModelToolCall{{ToolCallID: "tc_1", Name: activities.ToolSubmitRequirements, Arguments: m.args}}}, nil
}

func (m *analysisModel) GetModelCall(context.Context, string) (activities.ModelCallResult, error) {
	return activities.ModelCallResult{}, errors.New("not used")
}

func (m *analysisModel) CancelModelCall(context.Context, string) (activities.ModelCallResult, error) {
	return activities.ModelCallResult{}, errors.New("not used")
}

// analyze runs the analysis on requirements whose editableFields are fields.
func analyze(t *testing.T, fields string, sent *activities.ModelCallInput) activities.Analysis {
	t.Helper()
	args := `{"componentName":"PricingPlans","packageName":"@acme/pricing-plans","purpose":"p","content":"c","interaction":"","editableFields":` +
		fields + `,"constraints":[],"acceptance":[],"unknowns":[]}`
	acts := &activities.LifecycleActivities{
		Artifacts: fakeArtifacts{bodies: map[string][]byte{"h_prompt": []byte("A pricing section")}},
		Model:     &analysisModel{args: args, sent: sent},
	}
	out, err := acts.AnalyzeRequirements(context.Background(), activities.AnalyzeRequirementsInput{
		OperationID: "op_1", TenantID: "tenant_a", CallID: "call_1", RouteID: "controlled-openai-v1",
		Prompt: activities.ArtifactBinding{Handle: "h_prompt"}, MaxQuestions: 3,
	})
	require.NoError(t, err)
	return out
}

func TestRequirementsEditableFieldsFollowPuckFields(t *testing.T) {
	fields := `[
		{"name":"heading","type":"text","default":"Plans"},
		{"name":"intro","type":"richtext"},
		{"name":"note","type":"textarea","default":"Billed monthly"},
		{"name":"seats","type":"number","default":"5"},
		{"name":"variant","type":"select","default":"pro","options":[{"label":"Basic","value":"basic"},{"label":"Pro","value":"pro"}]},
		{"name":"scalePrice","type":"radio","options":[{"label":"Yes","value":true},{"label":"No","value":false}]},
		{"name":"columns","type":"radio","options":[{"label":"Two","value":2},{"label":"Three","value":3.0}]},
		{"name":"perks","type":"checkbox","options":[{"label":"Support","value":"support"}]},
		{"name":"plans","type":"array","arrayFields":{"name":{"type":"text"},"features":{"type":"array","arrayFields":{"label":{"type":"text"}}}}},
		{"name":"cta","type":"object","objectFields":{"label":{"type":"text"},"tone":{"type":"select","options":[{"label":"Bold","value":"bold"}]}}},
		{"name":"featured","type":"external"},
		{"name":"accent","type":"custom"},
		{"name":"content","type":"slot"}
	]`
	var sent activities.ModelCallInput
	out := analyze(t, fields, &sent)
	require.Empty(t, out.ErrorCode)
	require.NotEmpty(t, out.Requirements)

	var got struct {
		EditableFields []map[string]any `json:"editableFields"`
	}
	require.NoError(t, json.Unmarshal(out.Requirements, &got))
	require.Len(t, got.EditableFields, 13)
	var want []map[string]any
	require.NoError(t, json.Unmarshal([]byte(fields), &want))
	require.Equal(t, want, got.EditableFields, "the canonical requirements keep every field with its type and parameters")
	// Option values keep their JSON type and spelling (a number stays as written).
	require.Contains(t, string(out.Requirements), `{"label":"Three","value":3.0}`)
	require.Contains(t, string(out.Requirements), `{"label":"Yes","value":true}`)

	// The call names the analysis tool by the canonical digest of exactly the
	// schema the Model Proxy routes declare.
	require.Equal(t, activities.ToolSubmitRequirements, sent.Tools[0].Name)
	require.Equal(t, requirementsSchemaDigest, sent.Tools[0].InputSchemaDigest)
}

// requirementsSchemaDigest pins the analysis tool's input schema. The Model
// Proxy serves the tool only under this digest: a change here changes every
// route that declares submit_requirements in the same release
// (services/agent/model-proxy/config.yaml and the placements' Model Proxy
// configuration, the Compose one among them).
const requirementsSchemaDigest = "sha256:2654b0dbedfec32cc3df828de8b8b4d4d119b93d01cc7d0d279d2bd525aee1cc"

func TestRequirementsRefuseFieldsPuckDoesNotDefine(t *testing.T) {
	cases := map[string]string{
		"a type of the old brief vocabulary":                      `[{"name":"accent","type":"color"}]`,
		"a boolean type":                                          `[{"name":"open","type":"boolean","default":"true"}]`,
		"a select without options":                                `[{"name":"variant","type":"select"}]`,
		"a radio with empty options":                              `[{"name":"variant","type":"radio","options":[]}]`,
		"a text field with options":                               `[{"name":"title","type":"text","options":[{"label":"A","value":"a"}]}]`,
		"an option without a label":                               `[{"name":"variant","type":"select","options":[{"label":"","value":"a"}]}]`,
		"an option without a value":                               `[{"name":"variant","type":"select","options":[{"label":"A"}]}]`,
		"an object option value":                                  `[{"name":"variant","type":"select","options":[{"label":"A","value":{"v":1}}]}]`,
		"a null option value":                                     `[{"name":"variant","type":"select","options":[{"label":"A","value":null}]}]`,
		"an array without arrayFields":                            `[{"name":"plans","type":"array"}]`,
		"an array with empty arrayFields":                         `[{"name":"plans","type":"array","arrayFields":{}}]`,
		"an object with arrayFields":                              `[{"name":"cta","type":"object","arrayFields":{"label":{"type":"text"}}}]`,
		"a text field with objectFields":                          `[{"name":"title","type":"text","objectFields":{"label":{"type":"text"}}}]`,
		"a sub-field of an unknown type":                          `[{"name":"plans","type":"array","arrayFields":{"icon":{"type":"image"}}}]`,
		"a sub-field without a name":                              `[{"name":"plans","type":"array","arrayFields":{"":{"type":"text"}}}]`,
		"a select sub-field without options":                      `[{"name":"plans","type":"array","arrayFields":{"tier":{"type":"select"}}}]`,
		"nesting three levels deep":                               `[{"name":"a","type":"array","arrayFields":{"b":{"type":"array","arrayFields":{"c":{"type":"array","arrayFields":{"d":{"type":"text"}}}}}}}]`,
		"a field parameter Puck has but the brief does not carry": `[{"name":"title","type":"text","placeholder":"Title"}]`,
		"a default over 2000 characters":                          `[{"name":"title","type":"text","default":"` + strings.Repeat("x", 2001) + `"}]`,
	}
	seventeen := make([]string, 17)
	for i := range seventeen {
		seventeen[i] = `"f` + string(rune('a'+i)) + `":{"type":"text"}`
	}
	cases["seventeen sub-fields"] = `[{"name":"cta","type":"object","objectFields":{` + strings.Join(seventeen, ",") + `}}]`
	options := make([]string, 33)
	for i := range options {
		options[i] = `{"label":"L","value":` + string(rune('0'+i%10)) + `}`
	}
	cases["thirty-three options"] = `[{"name":"variant","type":"select","options":[` + strings.Join(options, ",") + `]}]`

	for name, fields := range cases {
		t.Run(name, func(t *testing.T) {
			out := analyze(t, fields, nil)
			require.Equal(t, "CONTENT_INVALID", out.ErrorCode)
			require.Empty(t, out.Requirements)
		})
	}
}

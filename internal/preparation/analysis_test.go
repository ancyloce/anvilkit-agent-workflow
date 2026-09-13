package preparation

import (
	"encoding/json"
	"testing"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/contracts"
)

// The fixed rules: a Prompt that settles the call to action and the image is
// clear; one that settles neither yields exactly those two material unknowns,
// which accepted answers settle; every document they produce validates.
func TestFixedAnalysisRules(t *testing.T) {
	clear := Analyze(clearPrompt, 1, 1, nil)
	if len(clear.MaterialUnknowns) != 0 || len(clear.EditableFields) != 5 || clear.Purpose == "" {
		t.Fatalf("clear prompt %+v", clear)
	}
	raw, _ := json.Marshal(clear)
	if _, err := contracts.ValidatePreparationDocument(raw, "RequirementsV1"); err != nil {
		t.Fatal(err)
	}
	vague := Analyze(vaguePrompt, 0, 0, nil)
	if len(vague.MaterialUnknowns) != 2 || vague.MaterialUnknowns[0].Key != "interactions.primaryCta" || vague.MaterialUnknowns[1].Key != "content.image" {
		t.Fatalf("vague prompt %+v", vague.MaterialUnknowns)
	}
	questions, err := PoseQuestions("op-prep-test", 1, 1, vague, 3)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := contracts.ValidatePreparationDocument(questions, "QuestionSetV1"); err != nil {
		t.Fatal(err)
	}
	one, _ := PoseQuestions("op-prep-test", 1, 1, vague, 1)
	var posed struct{ Questions []struct{ QuestionID string } }
	if json.Unmarshal(one, &posed) != nil || len(posed.Questions) != 1 || posed.Questions[0].QuestionID != "q-1-interactions-primaryCta" {
		t.Fatalf("question bound %+v", posed)
	}
	negatives := Analyze("A text only announcement without a button.", 0, 0, nil)
	if len(negatives.MaterialUnknowns) != 0 || len(negatives.EditableFields) != 2 {
		t.Fatalf("declined unknowns are settled %+v", negatives)
	}
	settledByAnswers := Analyze(vaguePrompt, 1, 1, []Answer{{QuestionID: "q-1-interactions-primaryCta", Key: "interactions.primaryCta", Text: "No call to action"}, {QuestionID: "q-1-content-image", Key: "content.image", Text: "Show the selected image"}})
	if len(settledByAnswers.MaterialUnknowns) != 0 || len(settledByAnswers.Interactions) != 0 || len(settledByAnswers.EditableFields) != 3 || settledByAnswers.EditableFields[2].Name != "image" {
		t.Fatalf("answers settle the unknowns %+v", settledByAnswers)
	}
	if settled, affirmative := answered([]Answer{{Key: "content.image", Text: "I'd rather not know; another asset later"}}, "content.image"); !settled || !affirmative {
		t.Fatal("substrings such as 'know' and 'another' never read as a decline")
	}
	inputRef, _ := json.Marshal(contracts.ArtifactRefV1{Kind: "preparation-input", RefID: "input-1", SubjectDigest: "sha256:3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855e", ContentDigest: "sha256:7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f", SizeBytes: "1536", ObjectVersion: "v1"})
	brief, err := ComposeBrief("op-prep-test", 1, inputRef, settledByAnswers, []ResolvedAnswer{{QuestionID: "q-1-content-image", Key: "content.image", Answer: "Show the selected image"}}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := contracts.ValidatePreparationDocument(brief, "BriefV1"); err != nil {
		t.Fatal(err)
	}
	if _, err := contracts.ValidatePreparationDocument(brief, "QuestionSetV1"); err == nil {
		t.Fatal("a brief is not a question set")
	}
}

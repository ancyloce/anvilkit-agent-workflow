package preparation

import (
	"encoding/json"
	"strconv"
	"strings"
	"unicode"
)

// The local fixture analysis route (DD-01 #preparation-workflow step 2 until
// the S3 model route opens): fixed rules over the Prompt produce the
// structured requirements record and separate the two material unknowns of a
// static marketing component — its primary call to action and its image —
// from details that take acceptable defaults. No model is called, nothing is
// invented: an unknown the Prompt does not settle becomes a question, and an
// accepted answer settles it. The real route replaces this file's rules with
// the schema-validated model answer and keeps every contract around it.

// Requirements mirrors urn:anvilkit:preparation:v1#/$defs/RequirementsV1.
type Requirements struct {
	SchemaVersion    int               `json:"schemaVersion"`
	Purpose          string            `json:"purpose"`
	Content          []string          `json:"content"`
	Structure        []string          `json:"structure"`
	Interactions     []string          `json:"interactions"`
	EditableFields   []EditableField   `json:"editableFields"`
	Constraints      []string          `json:"constraints"`
	AcceptancePoints []string          `json:"acceptancePoints"`
	MaterialUnknowns []MaterialUnknown `json:"materialUnknowns"`
}

type EditableField struct {
	Name        string `json:"name"`
	Kind        string `json:"kind"`
	Description string `json:"description,omitempty"`
}

type MaterialUnknown struct {
	Key      string   `json:"key"`
	Question string   `json:"question"`
	Options  []string `json:"options,omitempty"`
}

// Answer is one accepted answer folded into the analysis.
type Answer struct {
	QuestionID, Key, Text string
}

// ResolvedAnswer mirrors BriefV1.resolvedAnswers[].
type ResolvedAnswer struct {
	QuestionID string `json:"questionId"`
	Key        string `json:"key"`
	Answer     string `json:"answer"`
}

const (
	unknownCTA   = "interactions.primaryCta"
	unknownImage = "content.image"
)

var (
	ctaWords      = []string{"call to action", "call-to-action", "cta", "button", "link to", "sign up", "get started", "learn more", "buy now", "book "}
	noCTAWords    = []string{"no call to action", "without a call to action", "no cta", "no button", "without a button", "without buttons", "no buttons"}
	imageWords    = []string{"image", "photo", "picture", "illustration", "screenshot", "visual", "logo"}
	noImageWords  = []string{"no image", "without an image", "without image", "imageless", "text only", "text-only", "no photo", "no picture", "no visual"}
	negativeWords = map[string]bool{"no": true, "none": true, "without": true, "imageless": true, "skip": true, "omit": true, "text-only": true}
)

// negative reads an answer as declining the unknown: a whole-word "no",
// "without", "none", "imageless", "skip", "omit" or "text only". Substrings
// such as "another" or "know" never count.
func negative(text string) bool {
	for _, word := range strings.FieldsFunc(text, func(r rune) bool { return !unicode.IsLetter(r) && r != '-' }) {
		if negativeWords[word] {
			return true
		}
	}
	return strings.Contains(text, "text only")
}

func mentions(text string, words []string) bool {
	for _, word := range words {
		if strings.Contains(text, word) {
			return true
		}
	}
	return false
}

// firstSentence bounds the purpose to the Prompt's opening statement.
func firstSentence(prompt string) string {
	text := strings.TrimSpace(strings.Join(strings.Fields(prompt), " "))
	if index := strings.IndexFunc(text, func(r rune) bool { return r == '.' || r == '!' || r == '?' || r == '\n' }); index > 0 {
		text = text[:index+1]
	}
	runes := []rune(text)
	if len(runes) > 1000 {
		text = strings.TrimRightFunc(string(runes[:1000]), unicode.IsSpace) + "…"
	}
	return text
}

// answered reports whether an accepted answer settles the key and how.
func answered(answers []Answer, key string) (settled, affirmative bool) {
	for _, answer := range answers {
		if answer.Key != key {
			continue
		}
		return true, !negative(strings.ToLower(answer.Text))
	}
	return false, false
}

// Analyze applies the fixed rules to the Prompt and the accepted answers.
func Analyze(prompt string, brandRefs, assetRefs int, answers []Answer) Requirements {
	text := strings.ToLower(prompt)
	r := Requirements{SchemaVersion: 1, Purpose: firstSentence(prompt),
		Content:          []string{"heading", "description"},
		Structure:        []string{"one full-width section", "text column beside the optional media", "stacked on narrow viewports"},
		Interactions:     []string{},
		EditableFields:   []EditableField{{Name: "heading", Kind: "text"}, {Name: "description", Kind: "richText"}},
		Constraints:      []string{"responsive at 360px and 1280px viewports", "no nested Puck slots, forms, data connections or complex animation"},
		AcceptancePoints: []string{"renders the heading and description from its props", "keeps its layout at narrow and wide viewports"},
		MaterialUnknowns: []MaterialUnknown{}}
	if brandRefs > 0 {
		r.Constraints = append(r.Constraints, "brand colours and typography from the supplied brand reference; nothing invented beyond it")
		r.AcceptancePoints = append(r.AcceptancePoints, "uses only colours traced to the supplied brand reference")
	}

	cta, ctaSettled := false, false
	switch {
	case mentions(text, noCTAWords):
		ctaSettled = true
	case mentions(text, ctaWords):
		cta, ctaSettled = true, true
	default:
		ctaSettled, cta = answered(answers, unknownCTA)
	}
	if !ctaSettled {
		r.MaterialUnknowns = append(r.MaterialUnknowns, MaterialUnknown{Key: unknownCTA, Question: "Should the component include a primary call to action? If yes, state its label and where it should lead.", Options: []string{"Yes, with the label and destination I describe", "No call to action"}})
	} else if cta {
		r.Content = append(r.Content, "primary call to action")
		r.Interactions = append(r.Interactions, "the primary call to action navigates to its configured destination")
		r.EditableFields = append(r.EditableFields, EditableField{Name: "ctaLabel", Kind: "text"}, EditableField{Name: "ctaHref", Kind: "link"})
		r.AcceptancePoints = append(r.AcceptancePoints, "the call to action shows its label and links to the configured destination")
	}

	image, imageSettled := false, false
	switch {
	case mentions(text, noImageWords):
		imageSettled = true
	case mentions(text, imageWords):
		image, imageSettled = true, true
	default:
		imageSettled, image = answered(answers, unknownImage)
	}
	if !imageSettled {
		question := "Should the component show an image, or is an imageless result acceptable?"
		if assetRefs > 0 {
			question = "Should the component show the selected asset as its image, or is an imageless result acceptable?"
		}
		r.MaterialUnknowns = append(r.MaterialUnknowns, MaterialUnknown{Key: unknownImage, Question: question, Options: []string{"Show the selected image", "Imageless result"}})
	} else if image {
		r.Content = append(r.Content, "image")
		r.EditableFields = append(r.EditableFields, EditableField{Name: "image", Kind: "image", Description: "Optional; the layout stays intact without it"})
		r.AcceptancePoints = append(r.AcceptancePoints, "the image is omitted cleanly when absent")
		if assetRefs > 0 {
			r.Constraints = append(r.Constraints, "the image comes from the selected asset reference; no other source is fetched")
		}
	} else {
		r.Constraints = append(r.Constraints, "imageless result by decision; no placeholder image is generated")
	}
	return r
}

// PoseQuestions turns the first unknowns (bounded per round) into a
// QuestionSetV1 document.
func PoseQuestions(operationID string, round, revision int64, r Requirements, maximum int64) ([]byte, error) {
	type question struct {
		QuestionID      string   `json:"questionId"`
		Text            string   `json:"text"`
		Options         []string `json:"options,omitempty"`
		MaterialUnknown string   `json:"materialUnknown"`
	}
	set := struct {
		SchemaVersion       int        `json:"schemaVersion"`
		OperationID         string     `json:"operationId"`
		QuestionSetRevision string     `json:"questionSetRevision"`
		Round               string     `json:"round"`
		Questions           []question `json:"questions"`
	}{1, operationID, itoa(revision), itoa(round), nil}
	for index, unknown := range r.MaterialUnknowns {
		if int64(index) >= maximum {
			break
		}
		set.Questions = append(set.Questions, question{QuestionID: "q-" + itoa(revision) + "-" + strings.ReplaceAll(unknown.Key, ".", "-"), Text: unknown.Question, Options: unknown.Options, MaterialUnknown: unknown.Key})
	}
	return json.Marshal(set)
}

// ComposeBrief freezes the sufficient requirements with the answers that
// settled them and the user's brand and asset references.
func ComposeBrief(operationID string, briefRevision int64, inputRef json.RawMessage, r Requirements, resolved []ResolvedAnswer, brandRefs, assetRefs []json.RawMessage) ([]byte, error) {
	if resolved == nil {
		resolved = []ResolvedAnswer{}
	}
	if brandRefs == nil {
		brandRefs = []json.RawMessage{}
	}
	if assetRefs == nil {
		assetRefs = []json.RawMessage{}
	}
	return json.Marshal(struct {
		SchemaVersion   int               `json:"schemaVersion"`
		OperationID     string            `json:"operationId"`
		BriefRevision   string            `json:"briefRevision"`
		InputRef        json.RawMessage   `json:"inputRef"`
		Requirements    Requirements      `json:"requirements"`
		ResolvedAnswers []ResolvedAnswer  `json:"resolvedAnswers"`
		BrandRefs       []json.RawMessage `json:"brandRefs"`
		AssetRefs       []json.RawMessage `json:"assetRefs"`
	}{1, operationID, itoa(briefRevision), inputRef, r, resolved, brandRefs, assetRefs})
}

func itoa(value int64) string { return strconv.FormatInt(value, 10) }

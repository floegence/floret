package tools

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestQuestionActivityPayloadAnswersRoundTrip(t *testing.T) {
	presentation := ActivityPresentation{
		Renderer: ActivityRendererQuestion,
		Payload: QuestionActivityPayload{
			PromptID: "prompt-1",
			Questions: []QuestionActivityItem{{
				ID: "branch", Question: "Which branch?",
				Options: []QuestionActivityOption{{Label: "main"}},
			}},
			Answers: []QuestionActivityAnswer{
				{QuestionID: "branch", Values: []string{"main"}},
				{QuestionID: "token", Redacted: true},
			},
		},
	}

	wire, err := json.Marshal(presentation)
	if err != nil {
		t.Fatal(err)
	}
	var decoded ActivityPresentation
	if err := json.Unmarshal(wire, &decoded); err != nil {
		t.Fatal(err)
	}
	payload, ok := decoded.Payload.(QuestionActivityPayload)
	if !ok || len(payload.Answers) != 2 || payload.Answers[0].Values[0] != "main" || !payload.Answers[1].Redacted {
		t.Fatalf("decoded payload = %#v", decoded.Payload)
	}
}

func TestQuestionActivityPayloadRejectsUnsafeAnswers(t *testing.T) {
	tests := []struct {
		name   string
		answer QuestionActivityAnswer
		want   string
	}{
		{name: "missing question", answer: QuestionActivityAnswer{Values: []string{"main"}}, want: "question id"},
		{name: "missing value", answer: QuestionActivityAnswer{QuestionID: "branch"}, want: "value or redaction"},
		{name: "empty value", answer: QuestionActivityAnswer{QuestionID: "branch", Values: []string{" "}}, want: "non-empty"},
		{name: "redacted value", answer: QuestionActivityAnswer{QuestionID: "token", Values: []string{"secret"}, Redacted: true}, want: "must not include values"},
		{name: "long value", answer: QuestionActivityAnswer{QuestionID: "branch", Values: []string{strings.Repeat("x", maxActivityTextRunes+1)}}, want: "size limit"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			presentation := ActivityPresentation{
				Renderer: ActivityRendererQuestion,
				Payload:  QuestionActivityPayload{Answers: []QuestionActivityAnswer{test.answer}},
			}
			if err := presentation.Validate(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Validate() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestQuestionActivityPayloadCloneAndMergeDetachAnswers(t *testing.T) {
	left := &ActivityPresentation{
		Renderer: ActivityRendererQuestion,
		Payload: QuestionActivityPayload{
			PromptID:  "prompt-1",
			Questions: []QuestionActivityItem{{ID: "branch", Question: "Which branch?"}},
		},
	}
	right := &ActivityPresentation{
		Renderer: ActivityRendererQuestion,
		Payload:  QuestionActivityPayload{Answers: []QuestionActivityAnswer{{QuestionID: "branch", Values: []string{"main"}}}},
	}

	merged := MergeActivityPresentations(left, right)
	payload, ok := merged.Payload.(QuestionActivityPayload)
	if !ok || payload.PromptID != "prompt-1" || len(payload.Questions) != 1 || len(payload.Answers) != 1 {
		t.Fatalf("merged payload = %#v", merged.Payload)
	}
	rightPayload := right.Payload.(QuestionActivityPayload)
	rightPayload.Answers[0].Values[0] = "changed"
	if payload.Answers[0].Values[0] != "main" {
		t.Fatalf("merged answers alias source: %#v", payload.Answers)
	}

	cloned := CloneActivityPresentation(merged)
	clonedPayload := cloned.Payload.(QuestionActivityPayload)
	clonedPayload.Answers[0].Values[0] = "clone"
	if merged.Payload.(QuestionActivityPayload).Answers[0].Values[0] != "main" {
		t.Fatalf("cloned answers alias source: %#v", merged.Payload)
	}
}

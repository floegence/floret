package tools_test

import (
	"testing"

	"github.com/floegence/floret/v6/tools"
)

func TestQuestionActivityAnswerIsAvailableToExternalHosts(t *testing.T) {
	payload := tools.QuestionActivityPayload{
		Answers: []tools.QuestionActivityAnswer{{QuestionID: "branch", Values: []string{"main"}}},
	}
	if payload.Answers[0].QuestionID != "branch" {
		t.Fatalf("answer = %#v", payload.Answers[0])
	}
}

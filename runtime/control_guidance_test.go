package runtime

import (
	"strings"
	"testing"

	"github.com/floegence/floret/v7/tools"
)

func TestThreadControlGuidanceReachesProvider(t *testing.T) {
	service, gateway := goalService(t, parentEventDone())
	thread, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.Send(t.Context(), SendInput{ThreadID: thread.ThreadID, RequestKey: "send", Input: UserInput{Text: "Give a brief answer."}}); err != nil {
		t.Fatal(err)
	}
	view := waitThreadView(t, service, thread.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if view.Failure != nil {
		t.Fatalf("turn failed: %+v", view.Failure)
	}
	requests := gateway.Requests()
	if len(requests) != 1 {
		t.Fatalf("requests=%d", len(requests))
	}
	definitions := map[string]tools.ToolDefinition{}
	for _, definition := range requests[0].Tools {
		definitions[definition.Name] = definition
	}
	ask := definitions[CoreControlAskUser].InputSchema["properties"].(map[string]any)
	question := ask["questions"].(map[string]any)["items"].(map[string]any)["properties"].(map[string]any)
	for _, field := range []struct {
		schema        map[string]any
		name, meaning string
	}{
		{ask, "evidence_refs", "[]"},
		{question, "response_mode", "custom-text answer"},
	} {
		description, _ := field.schema[field.name].(map[string]any)["description"].(string)
		if !strings.Contains(description, field.meaning) {
			t.Errorf("provider cannot interpret %s: %q", field.name, description)
		}
	}
	goal := definitions["goal"].InputSchema["properties"].(map[string]any)
	for field, operation := range map[string]string{"text": "set", "reason": "delete", "outcome": "review", "evidence": "review"} {
		description, _ := goal[field].(map[string]any)["description"].(string)
		if !strings.Contains(description, operation) || !strings.Contains(description, "Required") {
			t.Errorf("provider lacks conditional requirement for %s: %q", field, description)
		}
	}
	fact, interpretation := false, false
	for _, message := range requests[0].Messages {
		fact = fact || strings.HasPrefix(message.Text, "Goal runtime fact: ")
		interpretation = interpretation || strings.Contains(message.Text, "not a new user instruction")
	}
	if !fact || !interpretation {
		t.Errorf("Goal context interpretation: fact=%v instruction=%v", fact, interpretation)
	}
	for _, item := range view.Items {
		if item.Kind == ThreadItemUser && item.Text != "Give a brief answer." {
			t.Errorf("runtime fact appeared as user input: %q", item.Text)
		}
	}
}

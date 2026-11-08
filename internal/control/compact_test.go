package control

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/internal/provider"
)

func TestAskUserCompactModelContract(t *testing.T) {
	encoded, err := json.Marshal(ToolDefinitions()[0].InputSchema)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"reason_code", "required_from_user", "choices_exhaustive", `"kind"`} {
		if strings.Contains(string(encoded), field) {
			t.Errorf("model schema exposes retired field %s", field)
		}
	}
	if len(encoded) > 1400 {
		t.Errorf("compact schema bytes=%d, budget=1400", len(encoded))
	}
}

func TestAskUserCompactAndReleasedArguments(t *testing.T) {
	const compact = `{"evidence_refs":["inspect-1"],"questions":[{"id":"route","header":"Route","question":"Which route?","response_mode":"select_or_write","is_secret":false,"choices":[{"choice_id":"safe","label":"Staged rollout"}]}]}`
	const released = `{"reason_code":"user_decision_required","required_from_user":["route"],"evidence_refs":["inspect-1"],"questions":[{"id":"route","header":"Route","question":"Which route?","response_mode":"select_or_write","is_secret":false,"choices_exhaustive":false,"choices":[{"choice_id":"safe","label":"Staged rollout","kind":"select"}]}]}`
	for _, raw := range []string{compact, released} {
		signal, ok, err := Project(provider.ToolCall{Name: AskUserTool, Args: raw})
		if err != nil || !ok || signal.Kind != SignalAskUser || signal.Prompt != "Which route?" {
			t.Fatalf("signal=%+v ok=%v err=%v", signal, ok, err)
		}
		var expected map[string]any
		if err := json.Unmarshal([]byte(raw), &expected); err != nil {
			t.Fatal(err)
		}
		actual, _ := json.Marshal(signal.Payload)
		want, _ := json.Marshal(expected)
		if string(actual) != string(want) {
			t.Fatalf("source fields changed: got=%s want=%s", actual, want)
		}
	}
	for _, invalid := range []string{
		strings.Replace(compact, `"evidence_refs":["inspect-1"]`, `"evidence_refs":null`, 1),
		strings.Replace(compact, `"evidence_refs":["inspect-1"],`, "", 1),
		strings.Replace(compact, `"is_secret":false`, `"is_secret":"false"`, 1),
		strings.Replace(compact, `"label":"Staged rollout"`, `"label":"Staged rollout","kind":"execute"`, 1),
		strings.Replace(released, `"reason_code":"user_decision_required"`, `"reason_code":"invented"`, 1),
		strings.Replace(released, `"required_from_user":["route"]`, `"required_from_user":false`, 1),
		strings.Replace(released, `"choices_exhaustive":false`, `"choices_exhaustive":"false"`, 1),
		strings.Replace(compact, `"header":"Route"`, `"header":"Route","unknown":true`, 1),
	} {
		if _, _, err := Project(provider.ToolCall{Name: AskUserTool, Args: invalid}); err == nil {
			t.Fatalf("accepted invalid input: %s", invalid)
		}
	}
}

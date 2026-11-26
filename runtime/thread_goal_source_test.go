package runtime

import (
	"encoding/json"
	"testing"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
)

func TestGoalItemMutationSource(t *testing.T) {
	for _, tc := range []struct{ op, actor, run, key, want string }{
		{"set", "model", "r", "model:r:call:a", "call:a"},
		{"delete", "model", "r", "model:r:call:b", "call:b"},
		{"set", "user", "r", "model:r:call:a", ""},
		{"set", "model", "r", "model:other:call:a", ""},
		{"set", "model", "r", "legacy", ""},
		{"set", "model", "r", "model:r:", ""},
		{"set", "model", "", "model::a", ""},
	} {
		payload, err := json.Marshal(sessiontree.GoalChange{State: session.GoalState{Version: 3, Actor: tc.actor}, Operation: tc.op})
		if err != nil {
			t.Fatal(err)
		}
		item, err := goalItem(sessiontree.Entry{Type: sessiontree.EntryGoal, TurnID: "t", RunID: tc.run, RequestKey: tc.key, Payload: payload})
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := json.Marshal(item.Goal)
		if err != nil {
			t.Fatal(err)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(encoded, &fields); err != nil {
			t.Fatal(err)
		}
		got, present := fields["source_tool_call_id"]
		if tc.want == "" && present || tc.want != "" && string(got) != `"`+tc.want+`"` {
			t.Errorf("%s %s %q: source %s, want %q", tc.op, tc.actor, tc.key, got, tc.want)
		}
	}
}

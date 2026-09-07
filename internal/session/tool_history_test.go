package session

import "testing"

func TestValidateToolHistory(t *testing.T) {
	call := func(id string) Message { return Message{Role: Assistant, ToolCallID: id, ToolName: "tool"} }
	result := func(id string) Message { return Message{Role: Tool, ToolCallID: id, ToolName: "tool"} }
	for _, test := range []struct {
		name     string
		messages []Message
		invalid  bool
	}{
		{"text", []Message{{Role: User, Content: "hi"}}, false},
		{"batch reverse result order", []Message{call("a"), call("b"), result("b"), result("a")}, false},
		{"reused ID in later exchange", []Message{call("a"), result("a"), call("a"), result("a")}, false},
		{"duplicate call", []Message{call("a"), call("a"), result("a")}, true},
		{"duplicate result", []Message{call("a"), result("a"), result("a")}, true},
		{"orphan result", []Message{result("a")}, true},
		{"missing result", []Message{call("a")}, true},
		{"interrupted exchange", []Message{call("a"), {Role: User, Content: "next"}, result("a")}, true},
		{"new call before batch finishes", []Message{call("a"), call("b"), result("a"), call("c"), result("b"), result("c")}, true},
		{"wrong name", []Message{call("a"), {Role: Tool, ToolCallID: "a", ToolName: "other"}}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := ValidateToolHistory(test.messages); (err != nil) != test.invalid {
				t.Fatalf("ValidateToolHistory error=%v, invalid=%v", err, test.invalid)
			}
		})
	}
}

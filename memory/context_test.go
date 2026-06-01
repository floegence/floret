package memory

import (
	"testing"

	"github.com/floegence/floret/session"
)

func TestAssembleAddsSystemPromptAndBoundsHistory(t *testing.T) {
	m := &Manager{SystemPrompt: "system", MaxMessages: 2}
	got := m.Assemble([]session.Message{
		{Role: session.User, Content: "old"},
		{Role: session.Assistant, Content: "newer"},
		{Role: session.User, Content: "newest"},
	})
	if len(got) != 3 {
		t.Fatalf("messages = %d, want system plus two history messages", len(got))
	}
	if got[0].Role != session.System || got[0].Content != "system" {
		t.Fatalf("first message = %#v, want system prompt", got[0])
	}
	if got[1].Content != "newer" || got[2].Content != "newest" {
		t.Fatalf("tail was not preserved: %#v", got)
	}
}

func TestCompactPreservesToolResultPair(t *testing.T) {
	m := &Manager{MaxMessages: 1}
	got := m.Compact([]session.Message{
		{Role: session.User, Content: "old"},
		{Role: session.Assistant, Content: "called tool"},
		{Role: session.Tool, ToolCallID: "call-1", Content: "tool result"},
	})
	if len(got) != 3 {
		t.Fatalf("messages = %d, want summary plus assistant/tool pair", len(got))
	}
	if got[0].Role != session.System {
		t.Fatalf("first = %#v, want compact summary", got[0])
	}
	if got[1].Role != session.Assistant || got[2].Role != session.Tool || got[2].ToolCallID != "call-1" {
		t.Fatalf("tool pair not preserved: %#v", got)
	}
}

func TestAssembleProviderVisibleShapePreservesSystemAndToolPair(t *testing.T) {
	m := &Manager{SystemPrompt: "system", MaxMessages: 1}
	got := m.Assemble([]session.Message{
		{Role: session.User, Content: "old"},
		{Role: session.Assistant, Content: "called tool"},
		{Role: session.Tool, ToolCallID: "call-1", ToolName: "read", Content: "tool result"},
	})
	if len(got) != 3 {
		t.Fatalf("messages = %#v", got)
	}
	if got[0].Role != session.System || got[0].Content != "system" {
		t.Fatalf("system prompt not first: %#v", got)
	}
	if got[1].Role != session.Assistant || got[2].Role != session.Tool || got[2].ToolCallID != "call-1" {
		t.Fatalf("provider-visible tool pair broken: %#v", got)
	}
}

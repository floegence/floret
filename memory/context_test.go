package memory

import (
	"testing"

	"github.com/floegence/floret/session"
)

func TestAssembleAddsSystemPromptWithoutMessageCountTrimming(t *testing.T) {
	m := &Manager{SystemPrompt: "system"}
	got := m.Assemble([]session.Message{
		{Role: session.User, Content: "old"},
		{Role: session.Assistant, Content: "newer"},
		{Role: session.User, Content: "newest"},
	})
	if len(got) != 4 {
		t.Fatalf("messages = %d, want system plus all history messages", len(got))
	}
	if got[0].Role != session.System || got[0].Content != "system" {
		t.Fatalf("first message = %#v, want system prompt", got[0])
	}
	if got[1].Content != "old" || got[3].Content != "newest" {
		t.Fatalf("history should remain intact for token-aware context policy: %#v", got)
	}
}

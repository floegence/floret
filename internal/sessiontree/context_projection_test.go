package sessiontree

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/floegence/floret/v6/internal/session"
)

func TestBuildContextProjectsInteractionAnswerOnceAsCanonicalUserMessage(t *testing.T) {
	payload := json.RawMessage(`{"accepted":true,"input":{"choice":"yes"},"at":"0001-01-01T00:00:00Z"}`)
	path := []Entry{
		{ID: "user-1", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "begin"}},
		{ID: "interaction", ParentID: "user-1", Type: EntryInteractionDone, Payload: payload},
		{ID: "assistant", ParentID: "interaction", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "continued"}},
		{ID: "user-2", ParentID: "assistant", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "next"}},
	}
	messages, err := BuildContextChecked(path, ContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"type":"interaction_response","answers":{"choice":"yes"}}`
	count := 0
	for _, message := range messages {
		if message.Content == want {
			count++
			if message.Role != session.User || message.EntryID != "interaction" {
				t.Fatalf("interaction response message=%#v", message)
			}
		}
	}
	if count != 1 {
		t.Fatalf("interaction answer appeared %d times in %#v", count, messages)
	}
}

func TestBuildContextProjectsOnlyRedactedMarkerForSecretAnswer(t *testing.T) {
	path := []Entry{{
		ID: "interaction", Type: EntryInteractionDone,
		Payload: json.RawMessage(`{"accepted":true,"redacted":true,"at":"0001-01-01T00:00:00Z"}`),
	}}
	messages, err := BuildContextChecked(path, ContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 || messages[0].Role != session.User || messages[0].Content != `{"type":"interaction_response","secret_answers_redacted":true}` {
		t.Fatalf("secret projection=%#v", messages)
	}
	if strings.Contains(messages[0].Content, "secret-value") {
		t.Fatalf("secret leaked into canonical message: %q", messages[0].Content)
	}
}

func TestBuildContextRejectsMalformedInteractionResolution(t *testing.T) {
	_, err := BuildContextChecked([]Entry{{
		ID: "interaction", Type: EntryInteractionDone, Payload: json.RawMessage(`{"accepted":"yes"}`),
	}}, ContextOptions{})
	if !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("malformed resolution error=%v, want ErrAuthorityCorrupt", err)
	}
}

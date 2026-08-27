package agentharness

import (
	"strings"
	"testing"

	"github.com/floegence/floret/v5/internal/event"
	"github.com/floegence/floret/v5/internal/session"
	"github.com/floegence/floret/v5/internal/sessiontree"
)

func TestTurnProjectionDropsSupersededProviderAttemptFromLiveAndCanonicalOutput(t *testing.T) {
	repo := sessiontree.NewMemoryRepo()
	if _, err := repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendTurnMarker(t.Context(), repo, "thread", "turn", sessiontree.TurnStarted, map[string]string{"run_id": "run"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendMessage(t.Context(), repo, "thread", "turn", session.Message{Role: session.User, Content: "work"}); err != nil {
		t.Fatal(err)
	}
	recorder := &event.Recorder{}
	projection := &turnProjection{
		ctx: t.Context(), turnID: "turn", runID: "run", downstream: recorder,
		thread: &Thread{id: "thread", harness: &AgentHarness{options: Options{Repo: repo}}},
	}

	projection.Emit(providerAttemptEvent(event.ProviderRequest, "logical", "attempt-1", 1, ""))
	projection.Emit(providerAttemptEvent(event.ProviderDelta, "logical", "attempt-1", 1, "superseded draft"))
	projection.Emit(providerAttemptEvent(event.ProviderRequest, "logical", "attempt-2", 2, ""))
	projection.Emit(providerAttemptEvent(event.ProviderDelta, "logical", "attempt-1", 1, "late stale delta"))
	projection.Emit(providerAttemptEvent(event.ProviderDelta, "logical", "attempt-2", 2, "canonical"))
	if err := projection.Flush(); err != nil {
		t.Fatal(err)
	}

	for _, observed := range recorder.Events {
		if observed.Type == event.ProviderDelta && observed.Message == "late stale delta" {
			t.Fatalf("stale provider delta reached live sink: %#v", recorder.Events)
		}
	}
	entries, err := repo.Entries(t.Context(), "thread")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Type == sessiontree.EntryAssistantMessage && strings.Contains(entry.Message.Content, "superseded") {
			t.Fatalf("superseded draft entered canonical timeline: %#v", entries)
		}
	}
	if got := countAssistantEntriesWithContent(entries, "canonical"); got != 1 {
		t.Fatalf("canonical assistant count=%d, want 1", got)
	}
}

func TestTurnProjectionFailsClosedForConflictingProviderAttemptIdentity(t *testing.T) {
	projection := &turnProjection{turnID: "turn", runID: "run"}
	projection.Emit(providerAttemptEvent(event.ProviderRequest, "logical", "attempt-a", 1, ""))
	projection.Emit(providerAttemptEvent(event.ProviderRequest, "logical", "attempt-b", 1, ""))
	if projection.err == nil || !strings.Contains(projection.err.Error(), "provider attempt identity conflict") {
		t.Fatalf("projection error=%v, want provider attempt identity conflict", projection.err)
	}
	if origin := turnProjectionFailureOrigin(projection.err); origin != "contract" {
		t.Fatalf("projection failure origin=%q, want contract", origin)
	}
}

func TestTurnProjectionFailsClosedWhenNextAttemptCrossesIncompletePublicToolBatch(t *testing.T) {
	projection := &turnProjection{turnID: "turn", runID: "run"}
	projection.Emit(providerAttemptEvent(event.ProviderRequest, "logical", "attempt-1", 1, ""))
	projection.Emit(event.Event{
		Type: event.ToolCall, ToolID: "tool-1", ToolName: "read", Args: `{}`,
		Metadata: map[string]any{"batch_index": 0, "batch_size": 2},
	})
	projection.Emit(providerAttemptEvent(event.ProviderRequest, "logical", "attempt-2", 2, ""))
	if projection.err == nil || !strings.Contains(projection.err.Error(), "pending canonical tool batch") {
		t.Fatalf("projection error=%v, want pending canonical tool batch", projection.err)
	}
	if origin := turnProjectionFailureOrigin(projection.err); origin != "contract" {
		t.Fatalf("projection failure origin=%q, want contract", origin)
	}
}

func providerAttemptEvent(eventType event.Type, logicalRequestID, attemptID string, attemptEpoch int, message string) event.Event {
	return event.Event{
		Type: eventType, RunID: "run", ThreadID: "thread", TurnID: "turn", Message: message,
		Metadata: map[string]any{
			"logical_request_id": logicalRequestID,
			"attempt_id":         attemptID,
			"attempt_epoch":      attemptEpoch,
		},
	}
}

func countAssistantEntriesWithContent(entries []sessiontree.Entry, content string) int {
	count := 0
	for _, entry := range entries {
		if entry.Type == sessiontree.EntryAssistantMessage && entry.Message.Content == content {
			count++
		}
	}
	return count
}

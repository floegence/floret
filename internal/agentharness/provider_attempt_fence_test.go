package agentharness

import (
	"context"
	"strings"
	"testing"

	"github.com/floegence/floret/v3/internal/event"
	"github.com/floegence/floret/v3/internal/session"
	"github.com/floegence/floret/v3/internal/sessiontree"
)

func TestTurnProjectionDropsSupersededProviderAttemptFromLiveAndCanonicalOutput(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendTurnMarker(ctx, repo, "thread", "turn", sessiontree.TurnStarted, map[string]string{"run_id": "run"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendMessage(ctx, repo, "thread", "turn", session.Message{Role: session.User, Content: "work"}); err != nil {
		t.Fatal(err)
	}
	recorder := &event.Recorder{}
	projection := &turnProjection{
		ctx: ctx, turnID: "turn", runID: "run", downstream: recorder,
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
	entries, err := repo.Entries(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if got := countEntriesWithContent(entries, sessiontree.EntryAssistantMessage, "canonical"); got != 1 {
		t.Fatalf("canonical assistant count = %d in %#v", got, entries)
	}
	for _, entry := range entries {
		if entry.Type == sessiontree.EntryAssistantMessage && strings.Contains(entry.Message.Content, "superseded") {
			t.Fatalf("superseded draft entered canonical timeline: %#v", entries)
		}
	}
}

func TestTurnProjectionFailsClosedForConflictingProviderAttemptIdentity(t *testing.T) {
	projection := &turnProjection{turnID: "turn", runID: "run"}
	projection.Emit(providerAttemptEvent(event.ProviderRequest, "logical", "attempt-a", 1, ""))
	projection.Emit(providerAttemptEvent(event.ProviderRequest, "logical", "attempt-b", 1, ""))
	if projection.err == nil || !strings.Contains(projection.err.Error(), "provider attempt identity conflict") {
		t.Fatalf("projection error = %v, want provider attempt identity conflict", projection.err)
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

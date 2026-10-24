package sessiontree

import (
	"fmt"
	"testing"

	"github.com/floegence/floret/v7/internal/session"
)

func TestFieldWindowDoesNotReplayOrdinarySavepoints(t *testing.T) {
	_, repo := projectionSourcesFixture(t, 0)
	for i := range 160 {
		_, err := repo.Append(t.Context(), Entry{ID: fmt.Sprint("save-", i), ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryTurnMarker, TurnStatus: TurnSavePoint, Metadata: map[string]string{"reason": "tool_result_batch"}}, AppendOptions{})
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := repo.Append(t.Context(), Entry{ID: "visible", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "visible"}}, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	got, err := repo.ReadWindowProjection(t.Context(), ItemMembershipWindowOptions{ThreadID: "thread", Limit: 1, ActivityFields: true}, func(PathMetadata, bool) []string { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Entries) > 8 {
		t.Fatalf("one visible item replayed %d canonical sources", len(got.Entries))
	}
}

func TestWindowNeutralSavepointKeepsSemanticMetadata(t *testing.T) {
	for _, metadata := range []map[string]string{
		{"reason": "context_continue"},
		{"failure_reason": "failure"},
		{TurnFailureCodeMetadataKey: "provider_error"},
		{GoalCompletedMetadataKey: "1"},
		{FinalResponseEntriesMetadataKey: "[]"},
		{"provider_timeout": "{}"},
		{"future_semantic_fact": "present"},
	} {
		entry := Entry{Type: EntryTurnMarker, TurnStatus: TurnSavePoint, TurnID: "turn", Metadata: metadata}
		if windowNeutralSavepoint(entry) || !windowTurnSource(entry) {
			t.Fatalf("semantic savepoint omitted: %v", metadata)
		}
	}
}

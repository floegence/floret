package agentharness

import (
	"errors"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
)

func TestHistoricalToolActivityUsesExactInvocationRun(t *testing.T) {
	entries := []sessiontree.Entry{
		{Type: sessiontree.EntryTurnMarker, TurnID: "turn", RunID: "run-first", TurnStatus: sessiontree.TurnStarted},
		{Type: sessiontree.EntryToolCall, TurnID: "turn", RunID: "run-first", Message: session.Message{ToolCallID: "first"}},
		{Type: sessiontree.EntryToolCall, TurnID: "turn", RunID: "run-resumed", Message: session.Message{ToolCallID: "resumed"}},
		{ID: "old-activity", Type: sessiontree.EntryCustom, TurnID: "turn", Message: session.Message{ToolCallID: "first"}, Metadata: map[string]string{threadDetailKindKey: toolActivityEntryKind, "run_id": "[redacted]#first"}},
		{ID: "resumed-activity", Type: sessiontree.EntryCustom, TurnID: "turn", Message: session.Message{ToolCallID: "resumed"}, Metadata: map[string]string{threadDetailKindKey: toolActivityEntryKind, "run_id": "[redacted]#resumed"}},
	}
	activity := newThreadDetailActivityContext(entries)
	if activity.err != nil {
		t.Fatal(activity.err)
	}
	for index, want := range map[int]string{3: "run-first", 4: "run-resumed"} {
		if got := threadDetailRunID(entries[index], activity); got != want {
			t.Fatalf("activity %q run=%q want %q", entries[index].ID, got, want)
		}
	}
	entries[4].Message.ToolCallID = "unknown"
	if got := newThreadDetailActivityContext(entries).err; !errors.Is(got, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("missing invocation error=%v", got)
	}
}

func TestIncrementalContextSharesHistoricalInvocationAuthority(t *testing.T) {
	cases := [][]sessiontree.Entry{
		{
			{ID: "observation", Type: sessiontree.EntryCustom, TurnID: "turn", Message: session.Message{ToolCallID: "call"}, Metadata: map[string]string{threadDetailKindKey: toolActivityEntryKind}},
			{Type: sessiontree.EntryToolCall, TurnID: "turn", RunID: "run", Message: session.Message{ToolCallID: "call"}},
		},
		{
			{Type: sessiontree.EntryToolCall, TurnID: "turn", RunID: "first", Message: session.Message{ToolCallID: "call"}},
			{Type: sessiontree.EntryToolCall, TurnID: "turn", RunID: "second", Message: session.Message{ToolCallID: "call"}},
		},
	}
	for _, entries := range cases {
		projection := ThreadContextProjection{}
		for i, entry := range entries {
			observedErr := projection.Observe(entry)
			_, gotErr := projection.Snapshot(time.Time{})
			if observedErr != nil {
				gotErr = observedErr
			}
			_, wantErr := ProjectThreadContext(entries[:i+1])
			if errors.Is(gotErr, sessiontree.ErrAuthorityCorrupt) != errors.Is(wantErr, sessiontree.ErrAuthorityCorrupt) {
				t.Fatalf("prefix=%d incremental=%v full=%v", i, gotErr, wantErr)
			}
		}
	}
}

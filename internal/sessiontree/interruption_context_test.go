package sessiontree

import (
	"context"
	"encoding/json"
	"github.com/floegence/floret/v7/internal/session/compaction"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
	"reflect"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/internal/session"
)

func TestCancelledTerminalProjectsTypedInterruptionAfterPartialOutput(t *testing.T) {
	path := []Entry{
		{ID: "user", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "work"}},
		{ID: "partial", ParentID: "user", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "```go\npartial 🌸"}},
		{ID: "terminal", ParentID: "partial", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryTurnMarker, TurnStatus: TurnAborted, Metadata: map[string]string{TurnFailureCodeMetadataKey: TurnFailureCancelled}},
	}
	var reducer ContextReducer
	for _, entry := range path {
		if err := reducer.Append([]Entry{entry}); err != nil {
			t.Fatal(err)
		}
	}
	messages := reducer.Messages()
	if len(messages) != 3 {
		t.Fatalf("messages=%+v; missing interruption fact", messages)
	}
	notice := messages[2]
	var payload map[string]string
	if err := json.Unmarshal([]byte(notice.Content), &payload); err != nil {
		t.Fatal(err)
	}
	if notice.Kind != session.MessageKindTurnInterruption || notice.Role != session.User || notice.EntryID != "terminal" || payload["type"] != "turn_interruption" || payload["turn_id"] != "turn" || payload["run_id"] != "run" || payload["outcome"] != "cancelled" {
		t.Fatalf("notice=%+v payload=%v", notice, payload)
	}
	if messages[1].Content != path[1].Message.Content {
		t.Fatal("partial output changed")
	}
}

func TestInterruptionPreservesReleasedCheckpointBoundaries(t *testing.T) {
	for _, tail := range []bool{false, true} {
		path := []Entry{
			{ID: "user", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "work"}},
			{ID: "partial", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "incomplete"}},
			{ID: "stop", Type: EntryTurnMarker, TurnStatus: TurnAborted, TurnID: "turn", RunID: "run"},
		}
		checkpoint := Entry{ID: "checkpoint", Type: EntryCompaction, Summary: "Released summary", CompactedThroughEntryID: "partial"}
		if tail {
			path = append(path, Entry{ID: "next", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "next"}})
			checkpoint.FirstKeptEntryID = "next"
		}
		path = append(path, checkpoint)
		messages, err := BuildContextChecked(path, ContextOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if len(messages) < 2 || messages[1].Kind != session.MessageKindTurnInterruption {
			t.Fatalf("lost gap fact: %+v", messages)
		}
	}
}

func TestInterruptionContextCompactionAndReopen(t *testing.T) {
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	entries := []Entry{
		{ID: "start", Type: EntryTurnMarker, TurnStatus: TurnStarted},
		{ID: "user", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "work"}},
		{ID: "partial", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: strings.Repeat("partial output ", 500)}},
		{ID: "stop", Type: EntryTurnMarker, TurnStatus: TurnAborted, Metadata: map[string]string{TurnFailureCodeMetadataKey: TurnFailureCancelled}},
		{ID: "next", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "continue"}},
		{ID: "next-answer", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: strings.Repeat("recent text ", 50)}},
	}
	for _, entry := range entries {
		entry.ThreadID = "thread"
		entry.TurnID = "turn"
		entry.RunID = "run"
		if entry.ID == "next" || entry.ID == "next-answer" {
			entry.TurnID = "next-turn"
			entry.RunID = "next-run"
		}
		if _, err := repo.Append(t.Context(), entry, AppendOptions{ID: entry.ID}); err != nil {
			t.Fatal(err)
		}
	}
	path, _ := repo.Path(t.Context(), "thread", "")
	history, err := BuildContextChecked(path, ContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	generator := &interruptionSummary{t: t}
	prep, err := compaction.Prepare(t.Context(), compaction.Request{CompactionID: "compact", OperationID: "op", RequestID: "request", Source: "test", History: history, Policy: contextpolicy.Policy{ContextWindowTokens: 20000, ReservedOutputTokens: 500, ReservedSummaryTokens: 100, RecentTailTokens: 20}}, generator)
	if err != nil {
		t.Fatal(err)
	}
	if !generator.seen {
		t.Fatal("summary writer did not receive interruption fact")
	}
	checkpoint, err := CompactionEntry("thread", "next-turn", prep.Result)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint.ID, checkpoint.RunID = "checkpoint", "next-run"
	checkpoint, err = repo.Append(t.Context(), checkpoint, AppendOptions{ID: checkpoint.ID})
	if err != nil {
		t.Fatal(err)
	}
	prep.ActiveMessages[0].EntryID, prep.ActiveMessages[0].ParentEntryID = checkpoint.ID, checkpoint.ParentID
	data, err := repo.EncodeMemoryState()
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := DecodeMemoryState(data, repo.now)
	if err != nil {
		t.Fatal(err)
	}
	path, err = reopened.Path(t.Context(), "thread", "")
	if err != nil {
		t.Fatal(err)
	}
	restored, err := BuildContextChecked(path, ContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(restored, prep.ActiveMessages) {
		t.Fatalf("reopened context differs: %+v / %+v", restored, prep.ActiveMessages)
	}
}

type interruptionSummary struct {
	t    *testing.T
	seen bool
}

func (g *interruptionSummary) GenerateSummary(_ context.Context, prep compaction.Preparation) (string, error) {
	for _, message := range prep.CompactedHead {
		if message.Kind == session.MessageKindTurnInterruption {
			g.seen = true
		}
	}
	return "The execution was cancelled; the partial assistant output is incomplete.", nil
}

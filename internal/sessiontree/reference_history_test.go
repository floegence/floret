package sessiontree

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/tools"
)

func TestReferenceHistorySeparatesConversationFromSourceControl(t *testing.T) {
	path := []Entry{
		{ID: "user", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "Thursday only.", References: []session.MessageReference{{Kind: session.MessageReferenceText, Label: "plan", Text: "Two hours.", ResourceRef: "private-handle"}}}},
		{ID: "control", Type: EntryGoalContext, Message: session.Message{Role: session.User, Kind: session.MessageKindGoalContext, Content: "Continue source work"}},
		{ID: "call", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "lookup", ToolCallID: "c1", ToolArgs: `{"q":"plan"}`}},
		{ID: "result", Type: EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "lookup", ToolCallID: "c1", Content: "Confirmed.", ToolResult: &session.ToolResultView{Status: "success"}}},
		{ID: "answer", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "Start at 10%."}},
	}
	sources, err := referenceHistoryAt(t.Context(), path)
	if err != nil || len(sources) != 4 {
		t.Fatalf("facts: %+v, err=%v", sources, err)
	}
	if first := sources[0].Fact; first.Role != session.User || !strings.Contains(first.Content, "Thursday only.") || !strings.Contains(first.Content, "Two hours.") || strings.Contains(first.Content, "private-handle") {
		t.Fatal(first)
	}
	if sources[1].Fact.Arguments != `{"q":"plan"}` || sources[2].Fact.Outcome != "success" || sources[1].Fact.CallID != sources[2].Fact.CallID || sources[3].Fact.Content != "Start at 10%." {
		t.Fatal(sources)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := referenceHistoryAt(ctx, path); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestReferenceHistoryKeepsUncertainEffectsDistinctFromToolStatus(t *testing.T) {
	path := []Entry{{ID: "call", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, ToolCallID: "request", ToolName: "write", ToolArgs: `{}`}}, {ID: "result", Type: EntryToolResult, Message: session.Message{Role: session.Tool, ToolCallID: "request", ToolName: "write", Content: "Arrival not confirmed", ToolResult: &session.ToolResultView{Status: "error", EffectOutcome: tools.EffectOutcomeUncertain, Truncated: true, OutputUnavailable: "storage_error"}}}}
	archive, err := referenceForkPath(t.Context(), "source", path, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	message, err := referenceProviderMessage(archive[len(archive)-1])
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"effect_outcome":"uncertain"`, `"outcome":"error"`, `"truncated":true`, `"unavailable":"storage_error"`} {
		if !strings.Contains(message.Content, want) {
			t.Fatalf("lost %s in %s", want, message.Content)
		}
	}
}

func TestReferenceHistoryPreservesCanonicalCheckpointAcrossForks(t *testing.T) {
	ctx := t.Context()
	path := []Entry{{ID: "old", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "removed by compaction"}}, {ID: "tail", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "retained answer"}}, {ID: "checkpoint", Type: EntryCompaction, Summary: "original summary", CompactedThroughEntryID: "old", FirstKeptEntryID: "tail"}}
	first, err := referenceForkPath(ctx, "parent", path, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	second, err := referenceForkPath(ctx, "child", first, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 || len(second) != 2 {
		t.Fatalf("reference counts %d / %d", len(first), len(second))
	}
	for i, entry := range first {
		a, err := decodeReferenceHistory(entry)
		if err != nil {
			t.Fatal(err)
		}
		b, err := decodeReferenceHistory(second[i])
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(a.Fact, b.Fact) || strings.Contains(a.Fact.Content, "removed by compaction") {
			t.Fatalf("unexpected reference fact %d: %+v / %+v", i, a.Fact, b.Fact)
		}
		if i == 0 && (a.Fact.Kind != session.MessageKindCompactionSummary || !strings.Contains(a.Fact.Content, "original summary")) || i == 1 && a.Fact.Content != "retained answer" {
			t.Fatal(a.Fact)
		}
	}
}

package sessiontree

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/session/compaction"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
)

type modelContextSummary struct{}

func (modelContextSummary) GenerateSummary(context.Context, compaction.Preparation) (string, error) {
	return "The repository inspection continues.", nil
}

func TestModelContextCompactionBaselineAndTailReopenIdentically(t *testing.T) {
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	snapshot := func(text string) *session.ModelContextSnapshot {
		return &session.ModelContextSnapshot{Sections: []session.ModelContextSection{{Key: "repository", Title: "Repository", Text: text}}}
	}
	appendMessage := func(id string, message session.Message) {
		entry := RunMessageEntry("thread", "turn", "run", message, repo.now())
		entry.ID = id
		if _, err := repo.Append(t.Context(), entry, AppendOptions{ID: id}); err != nil {
			t.Fatal(err)
		}
	}
	appendMessage("u1", session.Message{Role: session.User, Content: "inspect", ModelContextSnapshot: snapshot("first")})
	appendMessage("m1", session.Message{Role: session.User, Kind: session.MessageKindModelContext, ModelContextSnapshot: snapshot("at cut")})
	appendMessage("a1", session.Message{Role: session.Assistant, Content: strings.Repeat("historical result ", 500)})
	appendMessage("u2", session.Message{Role: session.User, Content: "continue"})
	appendMessage("a2", session.Message{Role: session.Assistant, Content: "working"})
	appendMessage("m2", session.Message{Role: session.User, Kind: session.MessageKindModelContext, ModelContextSnapshot: snapshot("tail current")})
	path, _ := repo.Path(t.Context(), "thread", "")
	history, err := BuildContextChecked(path, ContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	prep, err := compaction.Prepare(t.Context(), compaction.Request{CompactionID: "compact1", OperationID: "operation1", RequestID: "request1", Source: "test", History: history, Policy: contextpolicy.Policy{ContextWindowTokens: 20000, ReservedOutputTokens: 500, ReservedSummaryTokens: 100, RecentTailTokens: 20}}, modelContextSummary{})
	if err != nil {
		t.Fatal(err)
	}
	if !session.EqualModelContext(prep.Result.ModelContextBaseline, snapshot("at cut")) || !session.EqualModelContext(session.CurrentModelContext(prep.ActiveMessages), snapshot("tail current")) {
		t.Fatalf("cut=%s baseline=%+v active=%+v", prep.Result.FirstKeptEntryID, prep.Result.ModelContextBaseline, prep.ActiveMessages)
	}
	entry, err := CompactionEntry("thread", "turn", prep.Result)
	if err != nil {
		t.Fatal(err)
	}
	entry.ID, entry.RunID = "checkpoint", "run"
	entry, err = repo.Append(t.Context(), entry, AppendOptions{ID: entry.ID})
	if err != nil {
		t.Fatal(err)
	}
	prep.ActiveMessages[0].EntryID, prep.ActiveMessages[0].ParentEntryID = entry.ID, entry.ParentID
	encoded, err := repo.EncodeMemoryState()
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := DecodeMemoryState(encoded, repo.now)
	if err != nil {
		t.Fatal(err)
	}
	path, _ = reopened.Path(t.Context(), "thread", "")
	restored, err := BuildContextChecked(path, ContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	liveWire, _, _ := session.ProjectProviderHistory(prep.ActiveMessages, "")
	restoredWire, _, _ := session.ProjectProviderHistory(restored, "")
	if !reflect.DeepEqual(liveWire, restoredWire) {
		t.Fatalf("live=%+v restored=%+v", liveWire, restoredWire)
	}
	second, err := compaction.Prepare(t.Context(), compaction.Request{CompactionID: "compact2", OperationID: "operation2", RequestID: "request2", Source: "test", PreviousCompactionID: prep.Result.CompactionID, PreviousGeneration: prep.Result.CompactionGeneration, PreviousWindowID: prep.Result.CompactionWindowID, PreviousSummary: prep.Result.Summary, History: restored, Policy: contextpolicy.Policy{ContextWindowTokens: 20000, ReservedOutputTokens: 500, ReservedSummaryTokens: 100, RecentTailTokens: 20}}, modelContextSummary{})
	if err != nil {
		t.Fatal(err)
	}
	if !session.EqualModelContext(session.CurrentModelContext(second.ActiveMessages), snapshot("tail current")) {
		t.Fatal("continuous compaction lost baseline")
	}
}

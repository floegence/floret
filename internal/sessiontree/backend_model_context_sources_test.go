package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/session/compaction"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
	"github.com/floegence/floret/v7/storage/spi"
)

func assertModelContextSources(t *testing.T, repo *BackendRepo, thread string) ModelContextSources {
	t.Helper()
	path, err := repo.Path(t.Context(), thread, "")
	if err != nil {
		t.Fatal(err)
	}
	history, err := BuildContextChecked(path, ContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var want []session.Message
	for _, message := range history {
		if message.ModelContextSnapshot != nil {
			want = append(want, session.Message{EntryID: message.EntryID, ModelContextSnapshot: message.ModelContextSnapshot})
		}
	}
	if len(want) > 2 {
		want = want[len(want)-2:]
	}
	got, err := repo.ReadModelContextSources(t.Context(), thread, "")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Messages, want) {
		a, _ := json.Marshal(got.Messages)
		b, _ := json.Marshal(want)
		t.Fatalf("managed facts differ\ngot: %s\nwant: %s", a, b)
	}
	for i, entry := range got.Entries {
		if entry.ID != got.Messages[i].EntryID {
			t.Fatal("source identity differs")
		}
	}
	return got
}

func modelSourceSnapshot(text string) *session.ModelContextSnapshot {
	if text == "" {
		return &session.ModelContextSnapshot{Sections: []session.ModelContextSection{}}
	}
	return &session.ModelContextSnapshot{Sections: []session.ModelContextSection{{Key: "repository", Title: "Repository", Text: text}}}
}

func TestStoredModelContextSourcesSteeringUsesAppliedOrder(t *testing.T) {
	backend, repo := projectionSourcesFixture(t, 0)
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "model"}); err != nil {
		t.Fatal(err)
	}
	entries := []Entry{
		{ID: "start", Type: EntryTurnMarker, TurnStatus: TurnStarted},
		{ID: "input", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "inspect", ModelContextSnapshot: modelSourceSnapshot("initial")}},
		{ID: "steering", Type: EntrySteeringRequested, RequestKey: "steering", RequestFingerprint: "fingerprint", Message: session.Message{Role: session.User, Content: "change course", ModelContextSnapshot: modelSourceSnapshot("steered")}},
		{ID: "intervening", Type: EntryModelContext, Message: session.Message{Role: session.User, Kind: session.MessageKindModelContext, ModelContextSnapshot: modelSourceSnapshot("intervening")}},
		{ID: "applied", Type: EntrySteeringApplied, Payload: []byte(`{"steering_id":"steering"}`)},
		{ID: "clear", Type: EntryModelContext, Message: session.Message{Role: session.User, Kind: session.MessageKindModelContext, ModelContextSnapshot: modelSourceSnapshot("")}},
	}
	for i, entry := range entries {
		entry.ThreadID, entry.TurnID, entry.RunID = "model", "turn", "run"
		if _, err := repo.Append(t.Context(), entry, AppendOptions{ID: entry.ID}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			assertModelContextSources(t, repo, "model")
			if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
				var err error
				repo, _, err = NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, false)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			assertModelContextSources(t, repo, "model")
		})
	}
}

func TestStoredModelContextSourcesCompactionBaselineAndTail(t *testing.T) {
	_, repo := projectionSourcesFixture(t, 0)
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "model"}); err != nil {
		t.Fatal(err)
	}
	messages := []session.Message{
		{Role: session.User, Content: "inspect", ModelContextSnapshot: modelSourceSnapshot("initial")},
		{Role: session.User, Kind: session.MessageKindModelContext, ModelContextSnapshot: modelSourceSnapshot("at cut")},
		{Role: session.Assistant, Content: strings.Repeat("historical result ", 500)},
		{Role: session.User, Content: "continue"},
		{Role: session.Assistant, Content: "working"},
		{Role: session.User, Kind: session.MessageKindModelContext, ModelContextSnapshot: modelSourceSnapshot("tail current")},
	}
	for i, message := range messages {
		entry := RunMessageEntry("model", "turn", "run", message, time.Now())
		if _, err := repo.Append(t.Context(), entry, AppendOptions{ID: fmt.Sprint("message-", i)}); err != nil {
			t.Fatal(err)
		}
		assertModelContextSources(t, repo, "model")
	}
	path, err := repo.Path(t.Context(), "model", "")
	if err != nil {
		t.Fatal(err)
	}
	history, err := BuildContextChecked(path, ContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	prep, err := compaction.Prepare(t.Context(), compaction.Request{CompactionID: "compact1", OperationID: "operation1", RequestID: "request1", Source: "test", History: history, Policy: contextpolicy.Policy{ContextWindowTokens: 20000, ReservedOutputTokens: 500, ReservedSummaryTokens: 100, RecentTailTokens: 20}}, modelContextSummary{})
	if err != nil {
		t.Fatal(err)
	}
	entry, err := CompactionEntry("model", "turn", prep.Result)
	if err != nil {
		t.Fatal(err)
	}
	entry.RunID = "run"
	if _, err := repo.Append(t.Context(), entry, AppendOptions{ID: "checkpoint"}); err != nil {
		t.Fatal(err)
	}
	assertModelContextSources(t, repo, "model")
	if _, err := repo.Fork(t.Context(), ForkOptions{SourceThreadID: "model", NewThreadID: "fork"}); err != nil {
		t.Fatal(err)
	}
	assertModelContextSources(t, repo, "fork")
}

type modelSourceReadFailureTx struct {
	spi.WriteTx
	failure error
}

func (tx modelSourceReadFailureTx) Get(namespace string, key []byte) ([]byte, error) {
	if namespace == backendEntryIndexNamespace {
		return nil, tx.failure
	}
	return tx.WriteTx.Get(namespace, key)
}

func TestModelContextSourceReselectionPreservesStorageFailure(t *testing.T) {
	for _, failure := range []error{errors.New("injected storage failure"), context.Canceled} {
		t.Run(failure.Error(), func(t *testing.T) {
			backend, repo := projectionSourcesFixture(t, 0)
			if _, err := repo.ReadWindowProjection(t.Context(), ItemMembershipWindowOptions{ThreadID: "thread", Limit: 10}, func(PathMetadata, bool) []string { return nil }); err != nil {
				t.Fatal(err)
			}
			before := cloneMigrationRecords(backend.records)
			meta, err := repo.Thread(t.Context(), "thread")
			if err != nil {
				t.Fatal(err)
			}
			err = backend.Update(t.Context(), func(tx spi.WriteTx) error {
				return appendModelContextSources(t.Context(), modelSourceReadFailureTx{WriteTx: tx, failure: failure}, Entry{ThreadID: "thread", ParentID: meta.LeafID, ID: "compaction", Type: EntryCompaction}, nil)
			})
			if !errors.Is(err, failure) {
				t.Fatalf("source read failure was hidden: %v", err)
			}
			if !reflect.DeepEqual(before, backend.records) {
				t.Fatal("failed source read changed derived or canonical records")
			}
		})
	}
}

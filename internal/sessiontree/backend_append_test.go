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
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/threadquery"
)

type appendBoundedBackend struct {
	spi.Backend
	reads int
}

func (backend *appendBoundedBackend) Update(ctx context.Context, mutate func(spi.WriteTx) error) error {
	return backend.Backend.Update(ctx, func(tx spi.WriteTx) error { return mutate(appendBoundedTx{tx, backend}) })
}

type appendBoundedTx struct {
	spi.WriteTx
	backend *appendBoundedBackend
}

func (tx appendBoundedTx) Get(namespace string, key []byte) ([]byte, error) {
	tx.backend.reads++
	return tx.WriteTx.Get(namespace, key)
}
func (tx appendBoundedTx) Scan(request spi.ScanRequest) (spi.ScanPage, error) {
	if request.Namespace == backendDomainV32Namespace {
		return spi.ScanPage{}, errors.New("append scanned the journal")
	}
	return tx.WriteTx.Scan(request)
}

func TestV32AppendDoesNotLoadHistoricalJournal(t *testing.T) {
	for _, size := range []int{32, 512} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			memory := newMemoryRepo(time.Now)
			if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
				t.Fatal(err)
			}
			for i := range size {
				if _, err := memory.Append(t.Context(), Entry{ThreadID: "thread", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "retained history"}}, AppendOptions{ID: fmt.Sprint(i)}); err != nil {
					t.Fatal(err)
				}
			}
			backend, repo := openV31ReviewFixture(t, memory)
			if err := repo.PrepareQueryThread(t.Context(), "thread"); err != nil {
				t.Fatal(err)
			}
			repo.threadMemory = nil
			bounded := &appendBoundedBackend{Backend: backend}
			repo.backend = bounded
			if _, err := repo.Append(t.Context(), Entry{ThreadID: "thread", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "new canonical message"}}, AppendOptions{ID: "new"}); err != nil {
				t.Fatal(err)
			}
			if bounded.reads > 80 {
				t.Fatalf("append read %d records", bounded.reads)
			}
			if repo.threadMemory != nil {
				t.Fatal("append retained a complete journal")
			}
			page, err := repo.Query(t.Context(), queryScope("thread"), threadquery.QueryInput{Object: threadquery.Messages, Filter: threadquery.Filter{Text: threadquery.TextMatch{Contains: "new canonical"}}})
			if err != nil || page.Total != 1 {
				t.Fatalf("appended query = %+v, %v", page, err)
			}
		})
	}
}

func TestV32AppendPreservesUnicodeSearchAcrossRollbackRestartAndBranch(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	backend, repo := openV31ReviewFixture(t, memory)
	if err := repo.PrepareQueryThread(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	var full strings.Builder
	for i, text := range []string{"İ", "中", "🌸", "é", "\r", "\n", "Alpha", "\x00", "終"} {
		if i == 3 {
			if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
				var err error
				repo, _, err = NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, false)
				return err
			}); err != nil {
				t.Fatal(err)
			}
		}
		entry := Entry{ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: text, ContentSourceID: "answer"}}
		if i == 5 {
			before := cloneMigrationRecords(backend.records)
			rollback := errors.New("rollback append")
			repo.backend = &domainCommitBoundaryBackend{Backend: backend, afterCallback: func() error { return rollback }}
			if _, err := repo.Append(t.Context(), entry, AppendOptions{ID: fmt.Sprint(i)}); !errors.Is(err, rollback) {
				t.Fatalf("rollback=%v", err)
			}
			repo.backend = backend
			if !reflect.DeepEqual(before, backend.records) {
				t.Fatal("failed append changed canonical or query records")
			}
		}
		if _, err := repo.Append(t.Context(), entry, AppendOptions{ID: fmt.Sprint(i)}); err != nil {
			t.Fatal(err)
		}
		full.WriteString(text)
		for _, term := range []string{"İ中🌸", "中🌸é", "é\r\n", "Alpha\x00終", "missing"} {
			page, err := repo.Query(t.Context(), queryScope("thread"), threadquery.QueryInput{Object: threadquery.Messages, Filter: threadquery.Filter{Text: threadquery.TextMatch{Contains: term, CaseSensitive: true}}})
			want := 0
			if strings.Contains(full.String(), term) {
				want = 1
			}
			if err != nil || page.Total != want {
				t.Fatalf("segment=%d term=%q page=%+v err=%v", i, term, page, err)
			}
		}
	}
	if _, err := repo.Append(t.Context(), Entry{ThreadID: "thread", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "new branch"}}, AppendOptions{ID: "branch", ParentID: "0"}); err != nil {
		t.Fatal(err)
	}
	page, err := repo.Query(t.Context(), queryScope("thread"), threadquery.QueryInput{Object: threadquery.Messages, Filter: threadquery.Filter{Text: threadquery.TextMatch{Contains: "Alpha"}}})
	if err != nil || page.Total != 0 {
		t.Fatalf("inactive source remained searchable: %+v, %v", page, err)
	}
	if old, err := repo.Entry(t.Context(), "thread", "6"); err != nil || old.Message.Content != "Alpha" {
		t.Fatalf("branch lost historical source: %+v, %v", old, err)
	}
}

func TestV32AppendRejectsInvalidTypedFactsWithoutMutation(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	backend, repo := openV31ReviewFixture(t, memory)
	if err := repo.PrepareQueryThread(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	for _, entry := range []Entry{
		{ID: "cancel", ThreadID: "thread", Type: EntryCancelRequested, TurnID: "turn", RunID: "run", Metadata: map[string]string{"cancellation_source": "guessed", "cancellation_mode": "immediate"}},
		{ID: "goal", ThreadID: "thread", Type: EntryGoal, Payload: json.RawMessage("{}")},
		{ID: "parent", ThreadID: "thread", Type: EntryParentEvent, Payload: json.RawMessage("{}")},
	} {
		before := cloneMigrationRecords(backend.records)
		if _, err := repo.Append(t.Context(), entry, AppendOptions{}); !errors.Is(err, ErrAuthorityCorrupt) {
			t.Fatalf("invalid %s = %v", entry.ID, err)
		}
		if !reflect.DeepEqual(before, backend.records) {
			t.Fatalf("invalid %s changed canonical state", entry.ID)
		}
	}
	started := Entry{ID: "start", ThreadID: "thread", TurnID: "turn", RunID: "run", RequestKey: "request", Type: EntryTurnMarker, TurnStatus: TurnStarted, Metadata: map[string]string{TurnKindMetadataKey: TurnKindContextCompaction}}
	if _, err := repo.Append(t.Context(), started, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	before := cloneMigrationRecords(backend.records)
	if _, err := repo.Append(t.Context(), Entry{ID: "invalid-message", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "not allowed in context-only turn"}}, AppendOptions{}); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("context-only message = %v", err)
	}
	if !reflect.DeepEqual(before, backend.records) {
		t.Fatal("context-only rejection changed records")
	}
}

func TestV32AppendCommitsParentReceiptWithChildFact(t *testing.T) {
	_, backend := parentEventRepoFixture(t)
	var repo *BackendRepo
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		var err error
		repo, _, err = NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, true)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"parent", "child"} {
		if err := repo.PrepareQueryThread(t.Context(), id); err != nil {
			t.Fatal(err)
		}
	}
	before := cloneMigrationRecords(backend.records)
	rollback := errors.New("rollback child and parent")
	repo.backend = &domainCommitBoundaryBackend{Backend: backend, afterCallback: func() error { return rollback }}
	entry := Entry{ID: "completed", ThreadID: "child", TurnID: "turn", RunID: "run", Type: EntryTurnMarker, TurnStatus: TurnCompleted}
	if _, err := repo.Append(t.Context(), entry, AppendOptions{}); !errors.Is(err, rollback) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, backend.records) {
		t.Fatal("child or parent survived rollback")
	}
	repo.backend = backend
	if _, err := repo.Append(t.Context(), entry, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	page, err := repo.ParentEvents(t.Context(), ParentEventQuery{ThreadID: "parent"})
	if err != nil || len(page.Events) != 1 || page.Events[0].Kind != "completed" || page.Events[0].SourceEntryID != "completed" {
		t.Fatalf("parent receipt=%+v, %v", page, err)
	}
	if _, err := repo.Append(t.Context(), Entry{ID: "late", ThreadID: "child", TurnID: "turn", RunID: "run", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "late"}}, AppendOptions{}); !errors.Is(err, ErrStaleAuthority) {
		t.Fatalf("append after terminal=%v", err)
	}
}

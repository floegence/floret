package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/threadquery"
)

func TestV32RuntimeBatchAndProviderStateShareOneCommit(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	request := providerContextFixture(t, memory)
	backend, repo := openV31ReviewFixture(t, memory)
	if err := repo.PrepareQueryThread(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	before := cloneMigrationRecords(backend.records)
	rollback := errors.New("commit boundary failure")
	repo.backend = &domainCommitBoundaryBackend{Backend: backend, afterCallback: func() error { return rollback }}
	if _, err := repo.CommitProviderContext(t.Context(), request); !errors.Is(err, rollback) {
		t.Fatalf("rollback = %v", err)
	}
	if !reflect.DeepEqual(before, backend.records) {
		t.Fatal("provider output or state survived rollback")
	}
	bounded := &appendBoundedBackend{Backend: backend}
	repo.backend = bounded
	entries, err := repo.CommitProviderContext(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	state, err := repo.ProviderState(t.Context(), "thread")
	if err != nil || state.LeafEntryID != entries[len(entries)-1].ID {
		t.Fatalf("state=%+v, %v", state, err)
	}
	if repo.threadMemory != nil {
		t.Fatal("provider commit loaded a complete journal")
	}
	facts := []Entry{{ID: "queue-one", Type: EntryQueueAdded, Payload: []byte("{}")}, {ID: "queue-two", Type: EntryQueueReordered, Payload: []byte("[]")}}
	saved, err := repo.AppendRuntimeFacts(t.Context(), "thread", facts)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := repo.AppendRuntimeFacts(t.Context(), "thread", facts)
	savedJSON, _ := json.Marshal(saved)
	replayedJSON, _ := json.Marshal(replayed)
	if err != nil || string(savedJSON) != string(replayedJSON) {
		t.Fatalf("replay=%+v, %v", replayed, err)
	}
	state, err = repo.ProviderState(t.Context(), "thread")
	if err != nil || state.LeafEntryID != saved[1].ID {
		t.Fatalf("queue anchor=%+v, %v", state, err)
	}
	before = cloneMigrationRecords(backend.records)
	if _, err := repo.AppendRuntimeFacts(t.Context(), "thread", []Entry{facts[0], {ID: "new", Type: EntryQueueAdded}}); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("partial replay=%v", err)
	}
	if !reflect.DeepEqual(before, backend.records) {
		t.Fatal("partial replay mutated storage")
	}
	page, err := repo.Query(t.Context(), queryScope("thread"), threadquery.QueryInput{Object: threadquery.Messages, Filter: threadquery.Filter{Text: threadquery.TextMatch{Contains: "Inspecting."}}})
	if err != nil || page.Total != 1 {
		t.Fatalf("query=%+v, %v", page, err)
	}
}

func TestV32ProviderCommitHonorsCancellationAndRollsBackInvalidTail(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	request := providerContextFixture(t, memory)
	backend, repo := openV31ReviewFixture(t, memory)
	if err := repo.PrepareQueryThread(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	before := cloneMigrationRecords(backend.records)
	request.Entries[1].ParentID = "missing"
	if _, err := repo.CommitProviderContext(t.Context(), request); !errors.Is(err, ErrInvalidParent) {
		t.Fatalf("invalid tail=%v", err)
	}
	if !reflect.DeepEqual(before, backend.records) {
		t.Fatal("invalid tail retained first output")
	}
	request.Entries[1].ParentID = ""
	if _, err := repo.Append(t.Context(), Entry{ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryCancelRequested, Payload: []byte("{}")}, AppendOptions{ID: "cancel"}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CommitProviderContext(t.Context(), request); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled provider=%v", err)
	}
	request.Partial, request.State = true, nil
	request.Entries = []Entry{RunMessageEntry("thread", "turn", "run", session.Message{Role: session.Assistant, Content: "retained partial"}, time.Time{})}
	if _, err := repo.CommitProviderContext(t.Context(), request); err != nil {
		t.Fatal(err)
	}
}

func TestV32TurnLocatorsRejectMissingAndUnexpectedFacts(t *testing.T) {
	for _, mode := range []string{"missing-member", "extra-member", "wrong-marker", "extra-active"} {
		t.Run(mode, func(t *testing.T) {
			memory := newMemoryRepo(time.Now)
			providerContextFixture(t, memory)
			backend, repo := openV31ReviewFixture(t, memory)
			if err := repo.PrepareQueryThread(t.Context(), "thread"); err != nil {
				t.Fatal(err)
			}
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				switch mode {
				case "missing-member":
					return tx.Delete(backendEntryIndexNamespace, turnMemberLocatorKey("thread", "turn", 0))
				case "extra-member":
					return tx.Put(backendEntryIndexNamespace, turnMemberLocatorKey("thread", "turn", 100), []byte("missing"))
				case "wrong-marker":
					return tx.Put(backendEntryIndexNamespace, backendDomainKey("turn", "thread", "turn"), []byte("{}"))
				default:
					return tx.Put(backendEntryIndexNamespace, backendDomainKey("active_turn", "thread", "other"), []byte("missing"))
				}
			}); err != nil {
				t.Fatal(err)
			}
			if err := backend.View(t.Context(), func(tx spi.ReadTx) error { return verifyThreadEntryLocators(t.Context(), tx, memory) }); !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("verification=%v", err)
			}
		})
	}
}

func TestV32CanonicalTurnAndExchangeReadOnlyExactSources(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	request := providerContextFixture(t, memory)
	request.State = nil
	request.Entries = append(request.Entries, RunMessageEntry("thread", "turn", "run", session.Message{Role: session.Tool, ToolCallID: "call", ToolName: "inspect", Content: "confirmed", ToolResult: &session.ToolResultView{Status: "success"}}, time.Time{}))
	_, repo := openV31ReviewFixture(t, memory)
	if err := repo.PrepareQueryThread(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CommitProviderContext(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	repo.threadMemory = nil
	entries, found, err := repo.CanonicalTurnEntries(t.Context(), "thread", "turn", "run")
	if err != nil || !found || len(entries) != 5 {
		t.Fatalf("turn entries=%d, %v, %v", len(entries), found, err)
	}
	call, result, found, err := repo.CanonicalToolExchange(t.Context(), "thread", "turn", "run", "call")
	if err != nil || !found || call.Message.ToolArgs != "{}" || result.Message.Content != "confirmed" {
		t.Fatalf("exchange=%+v, %+v, %v, %v", call, result, found, err)
	}
	if repo.threadMemory != nil {
		t.Fatal("turn or tool read hydrated a journal")
	}
	journal, err := repo.Entries(t.Context(), "thread")
	if err != nil || len(journal) != 5 {
		t.Fatalf("journal=%d, %v", len(journal), err)
	}
	if repo.threadMemory != nil {
		t.Fatal("explicit journal read retained a duplicate owner")
	}
}

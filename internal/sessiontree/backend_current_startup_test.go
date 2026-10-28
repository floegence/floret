package sessiontree

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/threadquery"
)

type currentStartupBackend struct {
	spi.Backend
	gets, bodies, journalRows, turnScans, writes int
}

func (backend *currentStartupBackend) View(ctx context.Context, read func(spi.ReadTx) error) error {
	return backend.Backend.View(ctx, func(tx spi.ReadTx) error { return read(currentStartupTx{tx, backend}) })
}

func (backend *currentStartupBackend) Update(ctx context.Context, write func(spi.WriteTx) error) error {
	backend.writes++
	return backend.Backend.Update(ctx, write)
}

type currentStartupTx struct {
	spi.ReadTx
	backend *currentStartupBackend
}

func (tx currentStartupTx) Get(namespace string, key []byte) ([]byte, error) {
	tx.backend.gets++
	if namespace == canonicalBodyNamespace {
		tx.backend.bodies++
	}
	return tx.ReadTx.Get(namespace, key)
}

func (tx currentStartupTx) Scan(request spi.ScanRequest) (spi.ScanPage, error) {
	if request.Namespace == backendEntryIndexNamespace && bytes.HasPrefix(request.Start, backendDomainKey("turn_member")) {
		tx.backend.turnScans++
	}
	page, err := tx.ReadTx.Scan(request)
	if request.Namespace == backendDomainV32Namespace {
		for _, row := range page.Records {
			record, decodeErr := decodeBackendDomainRecord(backendDomainV32Format, row.Value)
			if decodeErr == nil && record.Kind == backendDomainRecordEntry {
				tx.backend.journalRows++
			}
		}
	}
	return page, err
}

func reopenCurrentStartupRepo(t *testing.T, backend spi.Backend, parentEvents bool) *BackendRepo {
	t.Helper()
	var repo *BackendRepo
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		var err error
		repo, _, err = NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, parentEvents)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return repo
}

func drainCurrentStartup(t *testing.T, repo *BackendRepo) {
	t.Helper()
	for i := 0; i < 100; i++ {
		more, err := repo.MigrateNextThread(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if !more {
			return
		}
	}
	t.Fatal("current startup did not finish")
}

func TestCurrentStartupReusesPublishedReadiness(t *testing.T) {
	for _, count := range []int{16, 512} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			memory := newMemoryRepo(time.Now)
			request := compactionCommitFixture(t, memory)
			if _, err := memory.CommitContextCompaction(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			for i := range count {
				if _, err := memory.Append(t.Context(), Entry{ThreadID: "thread", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "retained history"}}, AppendOptions{ID: fmt.Sprint(i)}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "child", ParentThreadID: "thread", TaskName: "child", HostProfileRef: "test"}); err != nil {
				t.Fatal(err)
			}
			for i := range count {
				if _, err := memory.Append(t.Context(), Entry{ThreadID: "child", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "retained child history"}}, AppendOptions{ID: fmt.Sprint("child-", i)}); err != nil {
					t.Fatal(err)
				}
			}
			base := newMigrationTestBackend()
			if err := base.Update(t.Context(), func(tx spi.WriteTx) error {
				if err := saveCompleteBackendDomainContext(t.Context(), tx, memory, backendDomainV32Format); err != nil {
					return err
				}
				manifest, err := readV32Manifest(tx)
				if err != nil {
					return err
				}
				return migrateBackendDomainV32ToV33(t.Context(), tx, manifest)
			}); err != nil {
				t.Fatal(err)
			}
			repo := reopenCurrentStartupRepo(t, base, true)
			for _, id := range []string{"thread", "child"} {
				if err := repo.PrepareQueryThread(t.Context(), id); err != nil {
					t.Fatal(err)
				}
			}
			drainCurrentStartup(t, repo)
			before := cloneMigrationRecords(base.records)
			for range 2 {
				measured := &currentStartupBackend{Backend: base}
				repo = reopenCurrentStartupRepo(t, measured, true)
				drainCurrentStartup(t, repo)
				// Recovery reads the child's final covered source directly, regardless
				// of either journal's length. Startup's one-row boundary check remains.
				if measured.bodies != 0 || measured.journalRows != 1 || measured.writes != 0 || measured.gets > 120 {
					t.Fatalf("restart read %d bodies, %d journal rows, %d records; wrote %d transactions", measured.bodies, measured.journalRows, measured.gets, measured.writes)
				}
				if len(repo.verifiedThreadLeaves) != 0 {
					t.Fatal("readiness was misrepresented as complete history verification")
				}
				if !equalMigrationRecords(before, base.records) {
					t.Fatal("current restart rewrote persisted state")
				}
				t.Logf("restart: %d records, %d child metadata rows, no body reads or writes", measured.gets, measured.journalRows)
			}
		})
	}
}

func TestCurrentStartupRepairsMissingQueryMetadata(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "history"}); err != nil {
		t.Fatal(err)
	}
	if _, err := memory.Append(t.Context(), Entry{ID: "fact", ThreadID: "history", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "retained fact"}}, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	base := newMigrationTestBackend()
	if err := base.Update(t.Context(), func(tx spi.WriteTx) error {
		if err := saveCompleteBackendDomainContext(t.Context(), tx, memory, backendDomainV32Format); err != nil {
			return err
		}
		manifest, err := readV32Manifest(tx)
		if err != nil {
			return err
		}
		return migrateBackendDomainV32ToV33(t.Context(), tx, manifest)
	}); err != nil {
		t.Fatal(err)
	}
	repo := reopenCurrentStartupRepo(t, base, false)
	if _, err := repo.Query(t.Context(), queryScope("history"), threadquery.QueryInput{Object: threadquery.Messages}); !errors.Is(err, threadquery.ErrThreadQueryIndexNotReady) {
		t.Fatalf("missing index query = %v", err)
	}
	drainCurrentStartup(t, repo)
	page, err := repo.Query(t.Context(), queryScope("history"), threadquery.QueryInput{Object: threadquery.Messages})
	if err != nil || page.Total != 1 {
		t.Fatalf("rebuilt query = %+v, %v", page, err)
	}
	for _, state := range []string{"stale", "malformed", "failed"} {
		if err := base.Update(t.Context(), func(tx spi.WriteTx) error {
			switch state {
			case "stale":
				return putQueryManifest(tx, "history", "previous-leaf")
			case "malformed":
				return tx.Put(queryManifestNamespace, []byte("history"), []byte("invalid derived metadata"))
			default:
				return writeThreadMigrationFailure(tx, "history", ErrAuthorityCorrupt)
			}
		}); err != nil {
			t.Fatal(err)
		}
		repo = reopenCurrentStartupRepo(t, base, false)
		drainCurrentStartup(t, repo)
		if ready, err := repo.ThreadPrepared(t.Context(), "history"); err != nil || !ready {
			t.Fatalf("%s repair readiness = %v, %v", state, ready, err)
		}
	}
}

func TestCurrentStartupPreservesOnDemandCorruptionChecks(t *testing.T) {
	for _, damage := range []string{"body", "leaf", "count"} {
		t.Run(damage, func(t *testing.T) {
			memory := newMemoryRepo(time.Now)
			if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "history"}); err != nil {
				t.Fatal(err)
			}
			if _, err := memory.Append(t.Context(), Entry{ID: "fact", ThreadID: "history", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "retained fact"}}, AppendOptions{}); err != nil {
				t.Fatal(err)
			}
			base := newMigrationTestBackend()
			if err := base.Update(t.Context(), func(tx spi.WriteTx) error {
				if err := saveCompleteBackendDomainContext(t.Context(), tx, memory, backendDomainV32Format); err != nil {
					return err
				}
				manifest, err := readV32Manifest(tx)
				if err != nil {
					return err
				}
				return migrateBackendDomainV32ToV33(t.Context(), tx, manifest)
			}); err != nil {
				t.Fatal(err)
			}
			repo := reopenCurrentStartupRepo(t, base, false)
			if err := repo.PrepareQueryThread(t.Context(), "history"); err != nil {
				t.Fatal(err)
			}
			drainCurrentStartup(t, repo)
			if err := base.Update(t.Context(), func(tx spi.WriteTx) error {
				switch damage {
				case "body":
					entry, err := readCanonicalEntryMetadata(tx, "history", "fact")
					if err != nil {
						return err
					}
					return tx.Delete(canonicalBodyNamespace, canonicalBodyBlockKey(entry.bodyFields["content"].SHA256, 0))
				case "leaf":
					return tx.Delete(backendEntryIndexNamespace, entryLocatorKey("history", "fact"))
				default:
					return putJournalCount(tx, "history", 0)
				}
			}); err != nil {
				t.Fatal(err)
			}
			before := cloneMigrationRecords(base.records)
			repo = reopenCurrentStartupRepo(t, base, false)
			if damage == "body" {
				drainCurrentStartup(t, repo)
				if _, err := repo.Entry(t.Context(), "history", "fact"); !errors.Is(err, ErrAuthorityCorrupt) {
					t.Fatalf("requested corrupt body = %v", err)
				}
			} else if _, err := repo.ThreadPrepared(t.Context(), "history"); !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("corrupt readiness anchor = %v", err)
			}
			if err := repo.PrepareQueryThread(t.Context(), "history"); !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("explicit preparation accepted corruption: %v", err)
			}
			if damage == "body" {
				if err := base.Update(t.Context(), func(tx spi.WriteTx) error {
					inspection, err := NewBackendRepoInTransaction(t.Context(), base, tx, time.Now, nil)
					if err != nil {
						return err
					}
					return inspection.VerifyInspectionStateInTransaction(t.Context(), tx)
				}); !errors.Is(err, ErrAuthorityCorrupt) {
					t.Fatalf("inspection accepted corruption: %v", err)
				}
			}
			if !equalMigrationRecords(before, base.records) {
				t.Fatal("corruption checks rewrote stored state")
			}
		})
	}
}

func TestCurrentStartupRecoversParentEventsWithReadyIndexes(t *testing.T) {
	legacy, base := parentEventRepoFixture(t)
	legacy.domainMemory.parentEvents = false
	if _, err := legacy.AppendRuntimeFacts(t.Context(), "child", []Entry{{ID: "interaction-requested:pending", ThreadID: "child", TurnID: "turn", RunID: "run", Type: EntryInteractionAsked, Payload: json.RawMessage(`{"kind":"approval"}`)}}); err != nil {
		t.Fatal(err)
	}
	var migrated *BackendRepo
	if err := base.Update(t.Context(), func(tx spi.WriteTx) error {
		var err error
		migrated, _, err = NewCurrentBackendRepoInTransaction(t.Context(), base, tx, time.Now, false)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	drainCurrentStartup(t, migrated)
	for range 2 {
		repo := reopenCurrentStartupRepo(t, base, true)
		for _, id := range []string{"parent", "child"} {
			if ready, err := repo.ThreadPrepared(t.Context(), id); err != nil || !ready {
				t.Fatalf("fixture readiness %s = %v, %v", id, ready, err)
			}
		}
		drainCurrentStartup(t, repo)
		page, err := repo.ParentEvents(t.Context(), ParentEventQuery{ThreadID: "parent"})
		if err != nil || len(page.Events) != 1 || page.Events[0].InteractionID != "pending" {
			t.Fatalf("recovered events = %+v, %v", page, err)
		}
	}
}

func TestCurrentStartupSkipsOrdinaryToolResultRecovery(t *testing.T) {
	legacy, base := parentEventRepoFixture(t)
	legacy.domainMemory.parentEvents = false
	call := Entry{ID: "call", ThreadID: "child", TurnID: "turn", RunID: "run", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, Content: "tool_call", ToolName: "work", ToolCallID: "call", ToolArgs: "{}"}}
	result := Entry{ID: "result", ThreadID: "child", TurnID: "turn", RunID: "run", Type: EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "work", ToolCallID: "call", Content: "finished", ToolResult: &session.ToolResultView{Status: "success"}}}
	if _, err := legacy.AppendRuntimeFacts(t.Context(), "child", []Entry{call, result}); err != nil {
		t.Fatal(err)
	}
	var repo *BackendRepo
	if err := base.Update(t.Context(), func(tx spi.WriteTx) error {
		var err error
		repo, _, err = NewCurrentBackendRepoInTransaction(t.Context(), base, tx, time.Now, false)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	drainCurrentStartup(t, repo)
	canonical := cloneMigrationRecords(base.records)[backendDomainV32Namespace]
	measured := &currentStartupBackend{Backend: base}
	repo = reopenCurrentStartupRepo(t, measured, true)
	drainCurrentStartup(t, repo)
	// First opt-in records the verified source prefix without changing any
	// canonical entry. Later starts reuse it without writes or turn scans.
	if measured.turnScans != 0 || measured.bodies != 0 || measured.writes != 1 {
		t.Fatalf("ordinary result recovery: turns=%d bodies=%d writes=%d", measured.turnScans, measured.bodies, measured.writes)
	}
	if !equalMigrationRecords(map[string]map[string][]byte{backendDomainV32Namespace: canonical}, map[string]map[string][]byte{backendDomainV32Namespace: base.records[backendDomainV32Namespace]}) {
		t.Fatal("ordinary result recovery changed canonical records")
	}
	measured = &currentStartupBackend{Backend: base}
	repo = reopenCurrentStartupRepo(t, measured, true)
	drainCurrentStartup(t, repo)
	if measured.turnScans != 0 || measured.bodies != 0 || measured.writes != 0 || measured.journalRows > 1 {
		t.Fatalf("prepared ordinary result recovery revisited history: %+v", measured)
	}
}

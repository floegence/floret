package sessiontree

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

func projectionSourcesFixture(t *testing.T, count int) (*migrationTestBackend, *BackendRepo) {
	t.Helper()
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	add := func(entry Entry) {
		entry.ThreadID, entry.TurnID, entry.RunID = "thread", "turn", "run"
		if _, err := memory.Append(t.Context(), entry, AppendOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	add(Entry{ID: "start", Type: EntryTurnMarker, TurnStatus: TurnStarted})
	add(Entry{ID: "call", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: "work", ToolArgs: "{}"}})
	for i := range count {
		add(Entry{ID: fmt.Sprint("unrelated-", i), Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "preserved"}})
	}
	add(Entry{ID: "result", Type: EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "inspect", ToolCallID: "work", Content: "complete", ToolResult: &session.ToolResultView{Status: "success"}}})
	backend, repo := openV31ReviewFixture(t, memory)
	if err := repo.PrepareThread(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	if err := repo.prepareProjectionSources(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	return backend, repo
}
func projectionGroupIDs(ctx context.Context, repo *BackendRepo, group projectionSourceGroup) ([]string, error) {
	var ids []string
	err := repo.canonicalThreadView(ctx, "thread", func(tx spi.ReadTx, meta ThreadMeta) error {
		leaf, err := readEntryLocator(tx, meta.ID, meta.LeafID)
		if err != nil {
			return err
		}
		return visitProjectionSources(ctx, tx, meta, leaf, group, func(entry Entry) error { ids = append(ids, entry.ID); return nil })
	}, func(*MemoryRepo) error { return errors.New("expected canonical backend") })
	return ids, err
}

type projectionReadBackend struct {
	spi.Backend
	sources int
	scans   int
}
type projectionReadTx struct {
	spi.ReadTx
	owner *projectionReadBackend
}

func (b *projectionReadBackend) View(ctx context.Context, f func(spi.ReadTx) error) error {
	return b.Backend.View(ctx, func(tx spi.ReadTx) error { return f(projectionReadTx{tx, b}) })
}
func (tx projectionReadTx) Get(ns string, key []byte) ([]byte, error) {
	if ns == canonicalBodyNamespace {
		return nil, errors.New("dependency lookup read a body")
	}
	if ns == backendDomainV32Namespace {
		tx.owner.sources++
	}
	return tx.ReadTx.Get(ns, key)
}
func (tx projectionReadTx) Scan(req spi.ScanRequest) (spi.ScanPage, error) {
	if req.Namespace != projectionSourcesNamespace {
		return spi.ScanPage{}, errors.New("dependency lookup scanned history")
	}
	tx.owner.scans++
	return tx.ReadTx.Scan(req)
}
func TestProjectionSourcesColdReadDoesNotScanUnrelatedHistory(t *testing.T) {
	var sourceReads int
	for _, count := range []int{128, 1280} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			backend, repo := projectionSourcesFixture(t, count)
			if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
				var err error
				repo, _, err = NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, false)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			counted := &projectionReadBackend{Backend: backend}
			repo.backend = counted
			got, err := projectionGroupIDs(t.Context(), repo, projectionSourceGroup{"tool", "turn", "run", "work"})
			if err != nil || !reflect.DeepEqual(got, []string{"call", "result"}) {
				t.Fatalf("ids=%v err=%v", got, err)
			}
			if counted.scans != 1 {
				t.Fatalf("scans=%d", counted.scans)
			}
			if sourceReads == 0 {
				sourceReads = counted.sources
			} else if counted.sources != sourceReads {
				t.Fatalf("source reads grew: %d -> %d", sourceReads, counted.sources)
			}
		})
	}
}
func TestProjectionSourcesExcludeInactiveBranchesAndTrackAppend(t *testing.T) {
	backend, repo := projectionSourcesFixture(t, 8)
	group := projectionSourceGroup{"tool", "turn", "run", "work"}
	if err := repo.MoveLeaf(t.Context(), "thread", "call"); err != nil {
		t.Fatal(err)
	}
	got, err := projectionGroupIDs(t.Context(), repo, group)
	if err != nil || !reflect.DeepEqual(got, []string{"call"}) {
		t.Fatalf("branch=%v err=%v", got, err)
	}
	entry := Entry{ID: "branch-result", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "inspect", ToolCallID: "work", Content: "branch"}}
	before := cloneMigrationRecords(backend.records)
	failure := errors.New("commit failed")
	repo.backend = &domainCommitBoundaryBackend{Backend: backend, afterCallback: func() error { return failure }}
	if _, err := repo.Append(t.Context(), entry, AppendOptions{}); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, backend.records) {
		t.Fatal("failed append changed index or sources")
	}
	repo.backend = backend
	if _, err := repo.Append(t.Context(), entry, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	got, err = projectionGroupIDs(t.Context(), repo, group)
	if err != nil || !reflect.DeepEqual(got, []string{"call", "branch-result"}) {
		t.Fatalf("branch=%v err=%v", got, err)
	}
	if _, err := repo.DeleteRootTree(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		page, err := tx.Scan(spi.ScanRequest{Namespace: projectionSourcesNamespace, Limit: 1})
		if len(page.Records) != 0 {
			t.Fatal("deleted thread retained projection sources")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}
func TestProjectionSourcesRejectMissingExtraAndDriftedMembers(t *testing.T) {
	for _, corruption := range []string{"missing", "extra", "hash", "count", "identity"} {
		t.Run(corruption, func(t *testing.T) {
			backend, repo := projectionSourcesFixture(t, 2)
			group := projectionSourceGroup{"tool", "turn", "run", "work"}
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				key := projectionMemberKey("thread", group, 1)
				switch corruption {
				case "missing":
					return tx.Delete(projectionSourcesNamespace, key)
				case "count":
					return putProjectionCount(tx, projectionGroupKey("thread", group), 1)
				case "extra":
					raw, err := tx.Get(projectionSourcesNamespace, key)
					if err != nil {
						return err
					}
					return tx.Put(projectionSourcesNamespace, projectionMemberKey("thread", group, 2), raw)
				default:
					member, err := readProjectionMember(tx, "thread", group, 1)
					if err != nil {
						return err
					}
					if corruption == "hash" {
						member.RawHash = "drift"
					} else {
						member.EntryID = "unrelated-0"
					}
					raw, err := json.Marshal(member)
					if err != nil {
						return err
					}
					return tx.Put(projectionSourcesNamespace, key, raw)
				}
			}); err != nil {
				t.Fatal(err)
			}
			before := cloneMigrationRecords(backend.records)
			if _, err := projectionGroupIDs(t.Context(), repo, group); !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("accepted %s: %v", corruption, err)
			}
			if !reflect.DeepEqual(before, backend.records) {
				t.Fatal("corrupt read mutated source")
			}
		})
	}
}

func TestProjectionSourcesPreparationRollsBackAndResumes(t *testing.T) {
	backend, repo := projectionSourcesFixture(t, 260)
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		for {
			page, err := tx.Scan(spi.ScanRequest{Namespace: projectionSourcesNamespace, Limit: 128})
			if err != nil {
				return err
			}
			for _, row := range page.Records {
				if err := tx.Delete(projectionSourcesNamespace, row.Key); err != nil {
					return err
				}
			}
			if !page.HasMore {
				return nil
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	before := cloneMigrationRecords(backend.records)
	failure := errors.New("preparation commit failed")
	repo.backend = &domainCommitBoundaryBackend{Backend: backend, afterCallback: func() error { return failure }}
	if err := repo.prepareProjectionSources(t.Context(), "thread"); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, backend.records) {
		t.Fatal("failed preparation changed sources")
	}
	ctx, cancel := context.WithCancel(t.Context())
	repo.backend = &domainCommitBoundaryBackend{Backend: backend, afterCallback: func() error { cancel(); return ctx.Err() }}
	if err := repo.prepareProjectionSources(ctx, "thread"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, backend.records) {
		t.Fatal("cancelled preparation changed sources")
	}
	repo.backend = backend
	if err := repo.prepareProjectionSources(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		count, err := readJournalCount(tx, "thread")
		if err != nil {
			return err
		}
		coverage, err := readSparseCoverage(tx, "thread")
		if err == nil && coverage.Through != count {
			t.Fatalf("shared scan left queue and parent sources unprepared: %d of %d", coverage.Through, count)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	got, err := projectionGroupIDs(t.Context(), repo, projectionSourceGroup{"tool", "turn", "run", "work"})
	if err != nil || !reflect.DeepEqual(got, []string{"call", "result"}) {
		t.Fatalf("ids=%v err=%v", got, err)
	}
	prepared := cloneMigrationRecords(backend.records)
	if err := repo.prepareProjectionSources(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(prepared, backend.records) {
		t.Fatal("idempotent preparation rewrote metadata")
	}
}

type projectionPreparationBatchTx struct {
	spi.WriteTx
	goalReads, individualGoalWrites, individualParentWrites, batches int
}

func (tx *projectionPreparationBatchTx) Get(namespace string, key []byte) ([]byte, error) {
	if namespace == projectionSourcesNamespace && bytes.HasPrefix(key, backendDomainKey("thread", "goal-source")) {
		tx.goalReads++
	}
	return tx.WriteTx.Get(namespace, key)
}
func (tx *projectionPreparationBatchTx) Put(namespace string, key, value []byte) error {
	if namespace == projectionSourcesNamespace && bytes.HasPrefix(key, backendDomainKey("thread", "parent-source")) {
		tx.individualParentWrites++
	}
	if namespace == projectionSourcesNamespace && bytes.HasPrefix(key, backendDomainKey("thread", "goal-source")) {
		tx.individualGoalWrites++
	}
	return tx.WriteTx.Put(namespace, key, value)
}
func (tx *projectionPreparationBatchTx) WriteFloretRecords(namespace string, records []spi.Record) error {
	tx.batches++
	for _, record := range records {
		if err := tx.WriteTx.Put(namespace, record.Key, record.Value); err != nil {
			return err
		}
	}
	return nil
}

func TestProjectionPreparationCoalescesGoalSourcesWithinCanonicalBatch(t *testing.T) {
	backend, repo := projectionSourcesFixture(t, 260)
	var entries []Entry
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		for ordinal := 1; ordinal < 129; ordinal++ {
			raw, err := tx.Get(backendDomainV32Namespace, backendDomainEntryKey("thread", ordinal))
			if err != nil {
				return err
			}
			_, stored, err := decodeCanonicalEntryRecord(backendDomainV32Format, raw)
			if err != nil {
				return err
			}
			stored.Entry.bodyFields = stored.Fields
			entries = append(entries, stored.Entry)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		// Rebuild the same canonical batch in a private transaction. Retain the
		// predecessor's Goal references while deleting only this batch's groups.
		groups := map[projectionSourceGroup]bool{}
		for _, entry := range entries {
			values, err := projectionSourceGroups(entry)
			if err != nil {
				return err
			}
			for _, group := range values {
				groups[group] = true
			}
			if err := tx.Delete(projectionSourcesNamespace, goalSourceKey("thread", entry.ID)); err != nil {
				return err
			}
		}
		for group := range groups {
			if err := tx.Delete(projectionSourcesNamespace, projectionGroupKey("thread", group)); err != nil {
				return err
			}
		}
		counted := &projectionPreparationBatchTx{WriteTx: tx}
		if err := putProjectionSourceBatch(t.Context(), counted, "thread", entries, 1); err != nil {
			return err
		}
		if counted.goalReads > 1 || counted.individualGoalWrites != 0 || counted.individualParentWrites != 0 {
			t.Fatalf("per-entry Goal roundtrips remain: reads=%d writes=%d parent_writes=%d batches=%d", counted.goalReads, counted.individualGoalWrites, counted.individualParentWrites, counted.batches)
		}
		for _, entry := range entries {
			if _, err := readGoalSourceContext(tx, "thread", entry.ID); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	_ = repo
}

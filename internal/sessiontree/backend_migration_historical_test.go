package sessiontree

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

type historicalClosureReader struct {
	spi.ReadTx
	allowed map[string]bool
	scans   int
}

func (reader *historicalClosureReader) Scan(request spi.ScanRequest) (spi.ScanPage, error) {
	reader.scans++
	if request.Limit > backendThreadEntryPageSize {
		return spi.ScanPage{}, errors.New("unbounded historical page")
	}
	page, err := reader.ReadTx.Scan(request)
	for _, row := range page.Records {
		for _, format := range []backendDomainFormat{backendDomainV24Format, backendDomainV26Format} {
			record, decodeErr := decodeBackendDomainRecord(format, row.Value)
			if decodeErr == nil && record.Kind == backendDomainRecordEntry && !reader.allowed[record.ThreadID] {
				return spi.ScanPage{}, errors.New("unrelated history read")
			}
		}
	}
	return page, err
}

func TestHistoricalThreadClosureMatchesReleasedWriterMigration(t *testing.T) {
	roots, err := filepath.Glob(filepath.Join("testdata", "compaction-v*"))
	if err != nil {
		t.Fatal(err)
	}
	for _, root := range roots {
		t.Run(filepath.Base(root), func(t *testing.T) {
			backend := historicalCompactionBackendAt(t, root)
			clock := func() time.Time { return time.Unix(1900000000, 0).UTC() }
			var source backendDomainFormat
			var oracle *MemoryRepo
			if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
				detected, err := detectBackendDomainSource(t.Context(), tx)
				if err != nil {
					return err
				}
				switch detected {
				case backendDomainSourceV24:
					source = backendDomainV24Format
				case backendDomainSourceV26:
					source = backendDomainV26Format
				default:
					return ErrAuthorityCorrupt
				}
				oracle, _, err = loadBackendDomain(t.Context(), tx, clock, source)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			ids := make([]string, 0, len(oracle.threads))
			for id := range oracle.threads {
				ids = append(ids, id)
			}
			if err := migrateHistoricalMemoryThroughV29(t.Context(), oracle, source.version); err != nil {
				t.Fatal(err)
			}
			before := cloneMigrationRecords(backend.records)
			for _, id := range ids {
				memory, err := loadHistoricalThreadClosure(t.Context(), migrationSourceReader{ctx: t.Context(), backend: backend}, source, id, clock)
				if err != nil {
					t.Fatal(err)
				}
				if err := migrateHistoricalMemoryThroughV29(t.Context(), memory, source.version); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(memory.entries[id], oracle.entries[id]) || !reflect.DeepEqual(memory.threads[id], oracle.threads[id]) {
					t.Fatalf("closure changed migration for %s", id)
				}
			}
			if !reflect.DeepEqual(before, backend.records) {
				t.Fatal("source closure read mutated storage")
			}
		})
	}
}

func TestHistoricalThreadClosureSkipsUnrelatedCorruptHistory(t *testing.T) {
	backend := newMigrationTestBackend()
	installV24OutputSourceStore(t, backend)
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		return tx.Put(backendDomainV24Namespace, backendDomainEntryKey("unrelated", 0), []byte("corrupt"))
	}); err != nil {
		t.Fatal(err)
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		reader := &historicalClosureReader{ReadTx: tx, allowed: map[string]bool{"thread": true}}
		memory, err := loadHistoricalThreadClosure(t.Context(), reader, backendDomainV24Format, "thread", time.Now)
		if err != nil {
			return err
		}
		if len(memory.threads) != 1 || reader.scans == 0 {
			t.Fatal("closure was not scoped")
		}
		return migrateHistoricalMemoryThroughV29(t.Context(), memory, 24)
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := loadHistoricalThreadClosure(ctx, migrationSourceReader{ctx: ctx, backend: backend}, backendDomainV24Format, "thread", time.Now)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation=%v", err)
	}
}

func TestHistoricalSegmentedPreparationPublishesWithoutBlockingNewThreads(t *testing.T) {
	for _, version := range []int{6, 7, 8, 13, 24, 26} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			var backend *migrationTestBackend
			var ids []string
			switch version {
			case 6:
				backend = newMigrationTestBackend()
				memory := legacyV6ForkEffectFixture(t, time.Now().UTC(), true)
				if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return saveBackendDomainV6Fixture(tx, memory) }); err != nil {
					t.Fatal(err)
				}
				ids = []string{"fork-two", "source", "fork-one"}
			case 7:
				backend = newMigrationTestBackend()
				memory, _ := v7ContextContinueFixture(t, time.Now().UTC())
				if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return saveCompleteBackendDomainV7(tx, memory) }); err != nil {
					t.Fatal(err)
				}
				ids = []string{"thread"}
			case 8:
				backend = newMigrationTestBackend()
				fixture := installV8ThreadContextStore(t, backend, nil)
				ids = []string{fixture.nestedThreadID, fixture.directThreadID, fixture.rootThreadID}
			case 13:
				backend = newMigrationTestBackend()
				installV13RunIdentityStore(t, backend)
				ids = []string{"nested-fork", "thread", "fork"}
			case 24:
				backend = newMigrationTestBackend()
				installV24OutputSourceStore(t, backend)
				ids = []string{"nested", "thread", "fork"}
			case 26:
				backend = historicalCompactionBackend(t)
			}
			source, err := migrationSourceFormat(version)
			if err != nil {
				t.Fatal(err)
			}
			original := cloneMigrationRecords(backend.records)[source.namespace]
			var repo *BackendRepo
			reopen := func() {
				t.Helper()
				if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
					var err error
					var recognized bool
					repo, recognized, err = NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, false)
					if err == nil && !recognized {
						return ErrAuthorityCorrupt
					}
					return err
				}); err != nil {
					t.Fatal(err)
				}
			}
			reopen()
			if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "fresh-during-migration"}); err != nil {
				t.Fatal(err)
			}
			if len(ids) == 0 {
				metas, err := repo.ListThreads(t.Context(), ListThreadsOptions{IncludeArchived: true})
				if err != nil {
					t.Fatal(err)
				}
				for _, meta := range metas {
					if meta.ID != "fresh-during-migration" {
						ids = append(ids, meta.ID)
					}
				}
			}
			if err := repo.PrepareThread(t.Context(), ids[0]); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(original, backend.records[source.namespace]) {
				t.Fatal("publication mutated source")
			}
			reopen()
			for _, id := range ids {
				if err := repo.PrepareThread(t.Context(), id); err != nil {
					t.Fatal(err)
				}
				if _, err := repo.Path(t.Context(), id, ""); err != nil {
					t.Fatal(err)
				}
			}
			for i := 0; repo.HasPendingMigration(); i++ {
				if i > 200 {
					t.Fatal("migration did not finish")
				}
				if _, err := repo.MigrateNextThread(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			if len(backend.records[source.namespace]) != 0 {
				t.Fatal("source cleanup incomplete")
			}
			before := cloneMigrationRecords(backend.records)
			reopen()
			if !reflect.DeepEqual(before, backend.records) {
				t.Fatal("restart rewrote canonical bytes")
			}
		})
	}
}

func TestHistoricalPreparationSettlesUnknownEffectsOnce(t *testing.T) {
	now := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
	memory := newMemoryRepo(func() time.Time { return now })
	prepareLegacyEffectTurn(t, memory, now, false)
	attempts := canonicalEffectAttemptsForTurn(memory.entries["source"], "turn")
	for _, attempt := range attempts {
		if _, err := memory.BeginEffectDispatch(t.Context(), BeginEffectDispatchRequest{EffectAttemptID: attempt.EffectAttemptID, RequestFingerprint: attempt.RequestFingerprint, AuthorizationProofHash: "proof", Now: now}); err != nil {
			t.Fatal(err)
		}
	}
	backend := newMigrationTestBackend()
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return saveBackendDomainV6Fixture(tx, memory) }); err != nil {
		t.Fatal(err)
	}
	var repo *BackendRepo
	open := func() {
		t.Helper()
		if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
			var err error
			repo, _, err = NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, false)
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	open()
	if _, err := repo.prepareThreadBatch(t.Context(), "source"); err != nil {
		t.Fatal(err)
	}
	open()
	if err := repo.PrepareThread(t.Context(), "source"); err != nil {
		t.Fatal(err)
	}
	entries, err := repo.Entries(t.Context(), "source")
	if err != nil {
		t.Fatal(err)
	}
	failures, results := 0, 0
	for _, entry := range entries {
		if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnFailed {
			failures++
		}
		if entry.Type == EntryToolResult {
			results++
		}
	}
	if failures != 1 || results != len(attempts) {
		t.Fatalf("settlement failures=%d results=%d", failures, results)
	}
	before := cloneMigrationRecords(backend.records)
	open()
	if err := repo.PrepareThread(t.Context(), "source"); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, backend.records) {
		t.Fatal("restart replayed historical settlement")
	}
}

func TestHistoricalMigrationManifestSurvivesSequenceWrites(t *testing.T) {
	for _, path := range []string{"snapshot-diff", "mutation"} {
		t.Run(path, func(t *testing.T) {
			backend := newMigrationTestBackend()
			installV24OutputSourceStore(t, backend)
			var beforeManifest backendDomainManifest
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				if _, _, err := NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, false); err != nil {
					return err
				}
				var err error
				beforeManifest, err = readV32Manifest(tx)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				before := newMemoryRepo(time.Now)
				before.seq = beforeManifest.Sequence
				if path == "snapshot-diff" {
					after := cloneMemoryRepoForStartup(before)
					after.seq++
					_, err := persistBackendDomainChangesContext(t.Context(), tx, before, after, backendDomainV32Format)
					return err
				}
				mutation := newBackendDomainMutation(before)
				mutation.format = backendDomainV32Format
				mutation.memory.seq++
				return mutation.persist(t.Context(), tx)
			}); err != nil {
				t.Fatal(err)
			}
			if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
				actual, err := readV32Manifest(tx)
				if err != nil {
					return err
				}
				want := beforeManifest
				want.Sequence++
				if !reflect.DeepEqual(actual, want) {
					t.Fatalf("manifest=%+v, want %+v", actual, want)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestHistoricalPreparationPublishesUnknownEffectParentEventAtomically(t *testing.T) {
	for _, order := range [][]string{{"parent", "source"}, {"source", "parent"}} {
		t.Run(order[0]+"-first", func(t *testing.T) {
			now := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
			memory := newMemoryRepo(func() time.Time { return now })
			prepareLegacyEffectTurn(t, memory, now, false)
			if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "parent"}); err != nil {
				t.Fatal(err)
			}
			child := memory.threads["source"]
			child.ParentThreadID, child.ParentTurnID = "parent", "delegation"
			child.TaskName, child.TaskDescription = "child", "work"
			child.AgentPath, child.HostProfileRef = "/child", "test"
			memory.threads[child.ID] = child
			for _, attempt := range canonicalEffectAttemptsForTurn(memory.entries["source"], "turn") {
				if _, err := memory.BeginEffectDispatch(t.Context(), BeginEffectDispatchRequest{EffectAttemptID: attempt.EffectAttemptID, RequestFingerprint: attempt.RequestFingerprint, AuthorizationProofHash: "proof", Now: now}); err != nil {
					t.Fatal(err)
				}
			}
			backend := newMigrationTestBackend()
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return saveBackendDomainV6Fixture(tx, memory) }); err != nil {
				t.Fatal(err)
			}
			var repo *BackendRepo
			open := func() {
				t.Helper()
				if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
					var err error
					repo, _, err = NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, func() time.Time { return now }, true)
					return err
				}); err != nil {
					t.Fatal(err)
				}
			}
			open()
			for _, id := range order {
				if err := repo.PrepareThread(t.Context(), id); err != nil {
					t.Fatal(err)
				}
				open()
			}
			page, err := repo.ParentEvents(t.Context(), ParentEventQuery{ThreadID: "parent"})
			if err != nil || len(page.Events) != 1 || page.Events[0].ReasonCode != TurnFailureEffectOutcomeUnknown {
				t.Fatalf("events=%+v err=%v", page, err)
			}
			source, err := repo.Entry(t.Context(), "source", page.Events[0].SourceEntryID)
			if err != nil || source.TurnStatus != TurnFailed {
				t.Fatalf("terminal=%+v err=%v", source, err)
			}
			for iteration := 0; repo.HasPendingMigration(); iteration++ {
				if iteration > 50 {
					t.Fatal("migration did not complete")
				}
				if _, err := repo.MigrateNextThread(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			open()
			page, err = repo.ParentEvents(t.Context(), ParentEventQuery{ThreadID: "parent"})
			if err != nil || len(page.Events) != 1 {
				t.Fatalf("restarted events=%+v err=%v", page, err)
			}
		})
	}
}

func TestHistoricalCandidateScanHonorsEmptyAndBoundedRanges(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	for _, id := range []string{"empty", "history"} {
		if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: id}); err != nil {
			t.Fatal(err)
		}
	}
	for ordinal := range 3 {
		memory.entries["history"] = append(memory.entries["history"], Entry{ID: fmt.Sprint(ordinal), ThreadID: "history"})
	}
	reader := historicalCandidateReader{memory: memory, source: backendDomainV29Format}
	for _, test := range []struct {
		name    string
		request spi.ScanRequest
		count   int
		more    bool
	}{
		{"empty", spi.ScanRequest{Namespace: reader.source.namespace, Start: backendDomainKey(backendDomainRecordEntry, "empty"), Limit: 2}, 0, false},
		{"page", spi.ScanRequest{Namespace: reader.source.namespace, Start: backendDomainKey(backendDomainRecordEntry, "history"), Limit: 2}, 2, true},
		{"bounded-end", spi.ScanRequest{Namespace: reader.source.namespace, Start: backendDomainKey(backendDomainRecordEntry, "history"), End: backendDomainEntryKey("history", 1), Limit: 2}, 1, false},
		{"after-end", spi.ScanRequest{Namespace: reader.source.namespace, Start: backendDomainEntryKey("history", 9), Limit: int(^uint(0) >> 1)}, 0, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			page, err := reader.Scan(test.request)
			if err != nil || len(page.Records) != test.count || page.HasMore != test.more {
				t.Fatalf("page=%+v err=%v", page, err)
			}
		})
	}
}

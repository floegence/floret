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

func snapshotLineageFixture(t *testing.T, version int) (*migrationTestBackend, []byte) {
	t.Helper()
	state, _, _ := legacyActiveSubAgentState(t)
	if version >= 3 {
		if err := migrateMemoryStateV2ToV3(&state); err != nil {
			t.Fatal(err)
		}
	}
	if version >= 4 {
		if err := migrateMemoryStateV3ToV4(&state); err != nil {
			t.Fatal(err)
		}
	}
	if version == 5 {
		if err := migrateMemoryStateV4ToV5(&state); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if version < 5 {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil {
			t.Fatal(err)
		}
		fields["root_create_intents"] = json.RawMessage("{}")
		raw, err = json.Marshal(fields)
		if err != nil {
			t.Fatal(err)
		}
	}
	backend, _ := installSnapshotImportFixture(t, raw)
	memory, err := DecodeMemoryState(raw, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		raw, err := encodeRootThreadInventory(memory)
		if err != nil {
			return err
		}
		return tx.Put(backendDomainNamespace, backendRootThreadInventoryKey, raw)
	}); err != nil {
		t.Fatal(err)
	}
	return backend, raw
}

func TestSnapshotPreparationPublishesExactLineageAndResumes(t *testing.T) {
	for version := 2; version <= 5; version++ {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			backend, payload := snapshotLineageFixture(t, version)
			clock := func() time.Time { return time.Unix(1900000000, 0).UTC() }
			source := bytes.Clone(backend.records[backendDomainNamespace][string(backendStateKey)])
			if err := PrepareHistoricalSnapshot(t.Context(), backend, clock); err != nil {
				t.Fatal(err)
			}
			var repo *BackendRepo
			open := func() {
				t.Helper()
				if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
					var err error
					var recognized bool
					repo, recognized, err = NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, clock, false)
					if err == nil && !recognized {
						t.Fatal("source not recognized")
					}
					return err
				}); err != nil {
					t.Fatal(err)
				}
			}
			open()
			if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "new-thread"}); err != nil {
				t.Fatal(err)
			}
			if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "child"}); !errors.Is(err, ErrThreadExists) {
				t.Fatalf("historical identity reuse: %v", err)
			}
			if err := repo.PrepareThread(t.Context(), "child"); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(source, backend.records[backendDomainNamespace][string(backendStateKey)]) {
				t.Fatal("publication changed retained source")
			}
			oracle, err := DecodeMemoryState(payload, clock)
			if err != nil {
				t.Fatal(err)
			}
			if err := migrateHistoricalMemoryThroughV29(t.Context(), oracle, 6); err != nil {
				t.Fatal(err)
			}
			got, err := repo.Entries(t.Context(), "child")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, oracle.entries["child"]) {
				t.Fatal("published history differs from released migration")
			}
			for step := 0; step < 100 && repo.HasPendingMigration(); step++ {
				open()
				if _, err := repo.MigrateNextThread(t.Context()); err != nil {
					t.Fatalf("batch %d: %v", step, err)
				}
			}
			if repo.HasPendingMigration() {
				t.Fatal("migration did not finish")
			}
			if _, found := backend.records[backendDomainNamespace][string(backendStateKey)]; found {
				t.Fatal("completed migration retained original source")
			}
			if len(backend.records[snapshotImportNamespace]) != 0 || len(backend.records[snapshotAuthorityNamespace]) != 0 {
				t.Fatal("completed migration retained staging")
			}
			open()
			if _, err := repo.Thread(t.Context(), "new-thread"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSnapshotPreparationCancellationKeepsSource(t *testing.T) {
	backend, _ := snapshotLineageFixture(t, 5)
	before := cloneMigrationRecords(backend.records)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := PrepareHistoricalSnapshot(ctx, backend, time.Now); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation=%v", err)
	}
	if !reflect.DeepEqual(before, backend.records) {
		t.Fatal("cancelled preparation modified source")
	}
}

func TestSnapshotMigrationIsolatesDamagedThreadAndRetainsSource(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	for _, id := range []string{"broken", "healthy"} {
		if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: id}); err != nil {
			t.Fatal(err)
		}
	}
	payload, err := memory.EncodeMemoryState()
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		t.Fatal(err)
	}
	fields["entries"] = json.RawMessage(`{"broken":[{"unsupported":true}]}`)
	payload, err = json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	backend, source := installSnapshotImportFixture(t, payload)
	if err := PrepareHistoricalSnapshot(t.Context(), backend, time.Now); err != nil {
		t.Fatalf("independent thread blocked before preparation: %v", err)
	}
	var repo *BackendRepo
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		var err error
		repo, _, err = NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, false)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := repo.PrepareThread(t.Context(), "healthy"); err != nil {
		t.Fatal(err)
	}
	if err := repo.PrepareThread(t.Context(), "broken"); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("damaged source accepted: %v", err)
	}
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "new"}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(source, backend.records[backendDomainNamespace][string(backendStateKey)]) {
		t.Fatal("damaged source removed")
	}
}

func TestSnapshotPreparationRejectsEmptyLegacyShapeDrift(t *testing.T) {
	for version := 2; version <= 4; version++ {
		for _, field := range []string{"leases", "root_create_intents", "turn_admissions"} {
			t.Run(fmt.Sprintf("v%d/%s", version, field), func(t *testing.T) {
				var fields map[string]json.RawMessage
				if err := json.Unmarshal(legacyEmptyMemoryState(t, version), &fields); err != nil {
					t.Fatal(err)
				}
				delete(fields, field)
				raw, _ := json.Marshal(fields)
				backend, source := installSnapshotImportFixture(t, raw)
				if err := PrepareHistoricalSnapshot(t.Context(), backend, time.Now); !errors.Is(err, ErrAuthorityCorrupt) {
					t.Fatalf("invalid empty legacy source accepted: %v", err)
				}
				if !bytes.Equal(source, backend.records[backendDomainNamespace][string(backendStateKey)]) {
					t.Fatal("invalid source changed")
				}
				if len(backend.records[backendDomainV32Namespace]) != 0 {
					t.Fatal("invalid source published")
				}
			})
		}
	}
}

func TestSnapshotPreparationRejectsUnownedDirectoryRows(t *testing.T) {
	backend, source := snapshotLineageFixture(t, 5)
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		return tx.Put(snapshotAuthorityNamespace, []byte("unexpected"), []byte("{}"))
	}); err != nil {
		t.Fatal(err)
	}
	before := bytes.Clone(backend.records[backendDomainNamespace][string(backendStateKey)])
	if err := PrepareHistoricalSnapshot(t.Context(), backend, time.Now); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("unowned directory accepted: %v", err)
	}
	if len(source) == 0 || !bytes.Equal(before, backend.records[backendDomainNamespace][string(backendStateKey)]) {
		t.Fatal("source changed")
	}
	if len(backend.records[backendDomainV32Namespace]) != 0 {
		t.Fatal("unowned directory published")
	}
}

func TestSnapshotCleanupBatchesJournalKeysAcrossRestarts(t *testing.T) {
	backend := newMigrationTestBackend()
	clock := time.Unix(1900000000, 0).UnixNano()
	const count = backendThreadEntryPageSize*2 + 3
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		manifest := backendDomainManifest{Version: 32, SourceVersion: 5, MigrationTime: clock, MigrationPhase: "cleanup"}
		if err := putBackendDomainRecordContext(t.Context(), tx, backendDomainV32Format, backendDomainKey(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0, manifest); err != nil {
			return err
		}
		format, _ := migrationSourceFormat(5)
		if err := putBackendDomainRecordContext(t.Context(), tx, format, backendDomainKey(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0, backendDomainManifest{Version: 5}); err != nil {
			return err
		}
		for i := uint64(1); i <= count; i++ {
			if err := tx.Put(backendDomainJournalNamespace, backendJournalKey(i), []byte("retained source frame")); err != nil {
				return err
			}
		}
		return tx.Put(backendDomainNamespace, backendStateKey, []byte("retained snapshot"))
	}); err != nil {
		t.Fatal(err)
	}
	for step := 0; step < 3; step++ {
		before := len(backend.records[backendDomainJournalNamespace])
		var repo *BackendRepo
		if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
			var err error
			repo, _, err = NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, false)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := repo.MigrateNextThread(t.Context()); err != nil {
			t.Fatal(err)
		}
		more := repo.HasPendingMigration()
		after := len(backend.records[backendDomainJournalNamespace])
		if removed := before - after; removed > backendThreadEntryPageSize {
			t.Fatalf("unbounded cleanup: %d", removed)
		}
		if more != (step < 2) {
			t.Fatalf("step %d more=%v", step, more)
		}
		if step < 2 && len(backend.records[backendDomainNamespace][string(backendStateKey)]) == 0 {
			t.Fatal("snapshot removed before journal cleanup completed")
		}
	}
	if len(backend.records[backendDomainJournalNamespace]) != 0 || len(backend.records[backendDomainNamespace]) != 0 {
		t.Fatal("source cleanup incomplete")
	}
}

type snapshotDirectoryFailureBackend struct {
	spi.Backend
	mode           string
	writes, failAt int
	cancel         context.CancelFunc
}
type snapshotDirectoryFailureTx struct {
	spi.WriteTx
	owner *snapshotDirectoryFailureBackend
}

func (backend *snapshotDirectoryFailureBackend) Update(ctx context.Context, mutate func(spi.WriteTx) error) error {
	return backend.Backend.Update(ctx, func(tx spi.WriteTx) error { return mutate(snapshotDirectoryFailureTx{tx, backend}) })
}
func (tx snapshotDirectoryFailureTx) Put(namespace string, key, value []byte) error {
	if err := tx.WriteTx.Put(namespace, key, value); err != nil {
		return err
	}
	if namespace != snapshotAuthorityNamespace {
		return nil
	}
	tx.owner.writes++
	if tx.owner.writes != tx.owner.failAt {
		return nil
	}
	switch tx.owner.mode {
	case "panic":
		panic("injected directory panic")
	case "cancel":
		tx.owner.cancel()
		return context.Canceled
	default:
		return errors.New("injected directory write failure")
	}
}

func TestSnapshotDirectoryResumesAfterTransactionFailure(t *testing.T) {
	for _, mode := range []string{"write", "cancel", "panic"} {
		t.Run(mode, func(t *testing.T) {
			memory := newMemoryRepo(time.Now)
			for i := 0; i < backendThreadEntryPageSize*2+1; i++ {
				if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: fmt.Sprintf("thread-%04d", i)}); err != nil {
					t.Fatal(err)
				}
			}
			payload, err := memory.EncodeMemoryState()
			if err != nil {
				t.Fatal(err)
			}
			base, source := installSnapshotImportFixture(t, payload)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			backend := &snapshotDirectoryFailureBackend{Backend: base, mode: mode, failAt: backendThreadEntryPageSize + 1, cancel: cancel}
			func() {
				defer func() {
					if recovered := recover(); (recovered != nil) != (mode == "panic") {
						t.Fatalf("panic=%v mode=%s", recovered, mode)
					}
				}()
				if err := PrepareHistoricalSnapshot(ctx, backend, time.Now); err == nil {
					t.Fatal("injected failure did not fail")
				}
			}()
			if !bytes.Equal(source, base.records[backendDomainNamespace][string(backendStateKey)]) {
				t.Fatal("failed directory mutation changed source")
			}
			if got := len(base.records[snapshotAuthorityNamespace]); got != backendThreadEntryPageSize {
				t.Fatalf("committed partial directory=%d", got)
			}
			if len(base.records[backendDomainV32Namespace]) != 0 {
				t.Fatal("failed directory published")
			}
			backend.failAt, backend.writes = 0, 0
			if err := PrepareHistoricalSnapshot(t.Context(), backend, time.Now); err != nil {
				t.Fatal(err)
			}
			// One final source manifest is added after the remaining identities.
			if backend.writes != backendThreadEntryPageSize+2 {
				t.Fatalf("restart rewrote completed directory: %d writes", backend.writes)
			}
		})
	}
}

func TestSnapshotPublicationPreservesReleasedRecoveryJournal(t *testing.T) {
	clock := func() time.Time { return time.Unix(1900000000, 0).UTC() }
	memory := newMemoryRepo(clock)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "history"}); err != nil {
		t.Fatal(err)
	}
	before, err := memory.EncodeMemoryState()
	if err != nil {
		t.Fatal(err)
	}
	backend, source := installSnapshotImportFixture(t, before)
	for i := 0; i < 3; i++ {
		turnID, runID := fmt.Sprintf("turn-%d", i), fmt.Sprintf("run-%d", i)
		if _, err := memory.AcceptTurn(t.Context(), AcceptTurnRequest{
			ThreadID: "history", TurnID: turnID, RunID: runID, LogicalRequestID: fmt.Sprintf("request-%d", i),
			RequestFingerprint: fmt.Sprintf("accept-%d", i), InputRequestFingerprint: fmt.Sprintf("input-%d", i),
			Input: session.Message{Role: session.User, Content: fmt.Sprintf("journal fact %d", i)}, Now: clock(),
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := memory.FinishTurn(t.Context(), FinishTurnRequest{
			ThreadID: "history", TurnID: turnID, RunID: runID, TerminalEntryID: fmt.Sprintf("terminal-%d", i),
			Status: TurnCompleted, OutcomeFingerprint: fmt.Sprintf("outcome-%d", i), Now: clock(),
		}); err != nil {
			t.Fatal(err)
		}
		after, err := memory.EncodeMemoryState()
		if err != nil {
			t.Fatal(err)
		}
		frame, changed, err := buildBackendDomainJournalFrame(uint64(i+1), before, after)
		if err != nil || !changed {
			t.Fatalf("frame=%v changed=%v", err, changed)
		}
		raw, err := encodeBackendDomainJournalFrame(frame)
		if err != nil {
			t.Fatal(err)
		}
		if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
			return tx.Put(backendDomainJournalNamespace, backendJournalKey(uint64(i+1)), raw)
		}); err != nil {
			t.Fatal(err)
		}
		before = after
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		roots, err := encodeRootThreadInventory(memory)
		if err != nil {
			return err
		}
		return tx.Put(backendDomainNamespace, backendRootThreadInventoryKey, roots)
	}); err != nil {
		t.Fatal(err)
	}
	if err := PrepareHistoricalSnapshot(t.Context(), backend, clock); err != nil {
		t.Fatal(err)
	}
	var repo *BackendRepo
	open := func() {
		t.Helper()
		if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
			var err error
			repo, _, err = NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, clock, false)
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	open()
	if err := repo.PrepareThread(t.Context(), "history"); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(source, backend.records[backendDomainNamespace][string(backendStateKey)]) {
		t.Fatal("published history rewrote source")
	}
	for i := 0; i < 100 && repo.HasPendingMigration(); i++ {
		open()
		if _, err := repo.MigrateNextThread(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if repo.HasPendingMigration() {
		t.Fatal("cleanup unfinished")
	}
	open()
	entries, err := repo.Entries(t.Context(), "history")
	if err != nil || !reflect.DeepEqual(entries, memory.entries["history"]) {
		t.Fatalf("released replay facts changed: %v", err)
	}
	if len(backend.records[backendDomainJournalNamespace]) != 0 {
		t.Fatal("journal retained after complete migration")
	}
}

func TestSnapshotMigrationPreservesReleasedFallbackTitleRepair(t *testing.T) {
	clock := func() time.Time { return time.Unix(1900000000, 0).UTC() }
	memory := newMemoryRepo(clock)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "history"}); err != nil {
		t.Fatal(err)
	}
	if _, err := memory.AcceptTurn(t.Context(), AcceptTurnRequest{ThreadID: "history", TurnID: "turn", RunID: "run", LogicalRequestID: "request", RequestFingerprint: "accept", InputRequestFingerprint: "input", Input: session.Message{Role: session.User, Content: "retained fallback title"}, Now: clock()}); err != nil {
		t.Fatal(err)
	}
	if _, err := memory.FinishTurn(t.Context(), FinishTurnRequest{ThreadID: "history", TurnID: "turn", RunID: "run", TerminalEntryID: "end", Status: TurnCompleted, OutcomeFingerprint: "done", Now: clock()}); err != nil {
		t.Fatal(err)
	}
	meta := memory.threads["history"]
	meta.Title, meta.TitleStatus, meta.TitleSource = "", "", ""
	meta.TitleGeneration, meta.TitleUpdatedAt = 0, time.Time{}
	memory.threads["history"] = meta
	payload, err := memory.EncodeMemoryState()
	if err != nil {
		t.Fatal(err)
	}
	backend, _ := installSnapshotImportFixture(t, payload)
	// Released startup repairs the title and therefore regenerates its old
	// inventory, which may be missing at this exact historical boundary.
	if _, err := DecodeMemoryState(payload, clock); err != nil {
		t.Fatal(err)
	}
	if err := PrepareHistoricalSnapshot(t.Context(), backend, clock); err != nil {
		t.Fatal(err)
	}
	var repo *BackendRepo
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		var err error
		repo, _, err = NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, clock, false)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100 && repo.HasPendingMigration(); i++ {
		if _, err := repo.MigrateNextThread(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if repo.HasPendingMigration() {
		t.Fatal("repair did not finish")
	}
	got, err := repo.Thread(t.Context(), "history")
	if err != nil || got.Title != "retained fallback title" {
		t.Fatalf("title=%q err=%v", got.Title, err)
	}
}

func TestSnapshotCleanupRejectsMissingDirectoryIdentity(t *testing.T) {
	backend, _ := snapshotLineageFixture(t, 5)
	if err := PrepareHistoricalSnapshot(t.Context(), backend, time.Now); err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		return tx.Delete(snapshotAuthorityNamespace, backendDomainKey(backendDomainRecordThread, "child"))
	}); err != nil {
		t.Fatal(err)
	}
	var repo *BackendRepo
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		var err error
		repo, _, err = NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, false)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	var failure error
	for i := 0; i < 100 && repo.HasPendingMigration(); i++ {
		_, failure = repo.MigrateNextThread(t.Context())
		if failure != nil {
			break
		}
	}
	if !errors.Is(failure, ErrAuthorityCorrupt) {
		t.Fatalf("missing source identity accepted: %v", failure)
	}
	if len(backend.records[backendDomainNamespace][string(backendStateKey)]) == 0 {
		t.Fatal("unpublished source removed")
	}
}

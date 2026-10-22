package sessiontree

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestV32MigrationHistoricalInlineBodies(t *testing.T) {
	for _, source := range []backendDomainFormat{backendDomainV27Format, backendDomainV28Format, backendDomainV29Format} {
		t.Run(source.label, func(t *testing.T) {
			base := newMigrationTestBackend()
			memory := newMemoryRepo(time.Now)
			if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "history"}); err != nil {
				t.Fatal(err)
			}
			if _, err := memory.AcceptTurn(t.Context(), AcceptTurnRequest{
				ThreadID: "history", TurnID: "turn", RunID: "run", RequestFingerprint: "accept", InputRequestFingerprint: "input",
				Input: session.Message{Role: session.User, Content: strings.Repeat("preserved 中文🌸\n", 30000)}, Now: time.Now(),
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := memory.FinishTurn(t.Context(), FinishTurnRequest{ThreadID: "history", TurnID: "turn", RunID: "run", TerminalEntryID: "done", Status: TurnCompleted, OutcomeFingerprint: "done", Now: time.Now()}); err != nil {
				t.Fatal(err)
			}
			if _, err := memory.Fork(t.Context(), ForkOptions{SourceThreadID: "history", NewThreadID: "fork", Now: time.Now()}); err != nil {
				t.Fatal(err)
			}
			if err := base.Update(t.Context(), func(tx spi.WriteTx) error { return saveCompleteBackendDomainContext(t.Context(), tx, memory, source) }); err != nil {
				t.Fatal(err)
			}
			original := cloneMigrationRecords(base.records)
			backend := &activityMigrationBackend{Backend: base}
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
			if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "foreground"}); err != nil {
				t.Fatal(err)
			}
			backend.failCheckpoint = true
			beforeFailure := cloneMigrationRecords(base.records)
			if err := repo.PrepareThread(t.Context(), "history"); err == nil {
				t.Fatal("checkpoint failure accepted")
			}
			if !reflect.DeepEqual(base.records, beforeFailure) {
				t.Fatal("failed batch mutated storage")
			}
			backend.failCheckpoint = false
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				_, err := repo.stageSourceThread(t.Context(), tx, "history")
				return err
			}); err != nil {
				t.Fatal(err)
			}
			cancelled, cancel := context.WithCancel(t.Context())
			cancel()
			beforeCancel := cloneMigrationRecords(base.records)
			if err := repo.PrepareThread(cancelled, "history"); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancel=%v", err)
			}
			if !reflect.DeepEqual(base.records, beforeCancel) {
				t.Fatal("cancelled migration mutated storage")
			}
			reopen()
			for _, id := range []string{"fork", "history"} {
				if err := repo.PrepareThread(t.Context(), id); err != nil {
					t.Fatal(err)
				}
				want, err := memory.Path(t.Context(), id, "")
				if err != nil {
					t.Fatal(err)
				}
				got, err := repo.Path(t.Context(), id, "")
				if err != nil || !reflect.DeepEqual(want, got) {
					t.Fatalf("%s facts changed: %v", id, err)
				}
			}
			if !reflect.DeepEqual(base.records[source.namespace], original[source.namespace]) {
				t.Fatal("immutable source changed")
			}
			if backend.maxBodyBytes > backendThreadMigrationBatchBytes {
				t.Fatalf("unbounded body batch: %d", backend.maxBodyBytes)
			}
			for i := 0; repo.HasPendingMigration(); i++ {
				if i == 100 {
					t.Fatal("migration did not finish")
				}
				if _, err := repo.MigrateNextThread(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			if len(base.records[source.namespace]) != 0 || len(base.records[migrationBodyNamespace]) != 0 {
				t.Fatal("migration state leaked")
			}
			beforeRestart := cloneMigrationRecords(base.records)
			reopen()
			if err := repo.PrepareThread(t.Context(), "history"); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(base.records, beforeRestart) {
				t.Fatal("current restart rewrote canonical bytes")
			}
		})
	}
}

func TestV32MigrationRejectsFutureHistoricalTurnKind(t *testing.T) {
	for _, source := range []backendDomainFormat{backendDomainV27Format, backendDomainV28Format} {
		t.Run(source.label, func(t *testing.T) {
			base := newMigrationTestBackend()
			memory := installV28ContinuationSourceStore(t, base)
			for id, entries := range memory.entries {
				for i := range entries {
					if entries[i].Type == EntryTurnMarker && entries[i].TurnStatus == TurnStarted {
						entries[i].Metadata[TurnKindMetadataKey] = TurnKindGoalContinuation
						entries[i].Metadata[ContinueGoalMetadataKey] = "1"
						entries[i] = PrepareEntry(entries[i])
					}
				}
				memory.entries[id] = entries
			}
			source.validate = func(*MemoryRepo) error { return nil }
			if err := base.Update(t.Context(), func(tx spi.WriteTx) error { return saveCompleteBackendDomainContext(t.Context(), tx, memory, source) }); err != nil {
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
			before := cloneMigrationRecords(base.records)
			if err := repo.PrepareThread(t.Context(), "thread"); !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("future turn accepted: %v", err)
			}
			if !reflect.DeepEqual(before, base.records) {
				t.Fatal("invalid source published")
			}
		})
	}
}

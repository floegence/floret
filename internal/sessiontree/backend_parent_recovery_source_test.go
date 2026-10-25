package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

func preparedParentRecoveryFixture(t *testing.T) (*BackendRepo, *migrationTestBackend) {
	t.Helper()
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
	if err := repo.BootstrapParentEvents(t.Context()); err != nil {
		t.Fatal(err)
	}
	return repo, backend
}

func assertParentRecoveryPrepared(t *testing.T, backend spi.Backend, thread string, want bool) {
	t.Helper()
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		count, err := readJournalCount(tx, thread)
		if err != nil {
			return err
		}
		got, err := parentRecoveryPrepared(tx, thread, count)
		if err == nil && got != want {
			t.Errorf("recovery coverage for %s = %v, want %v", thread, got, want)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func rewriteParentRecoverySource(t *testing.T, backend spi.Backend, mutate func(*queryManifest)) {
	t.Helper()
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		raw, err := tx.Get(queryManifestNamespace, []byte("child"))
		if err != nil {
			return err
		}
		var manifest queryManifest
		if err := json.Unmarshal(raw, &manifest); err != nil {
			return err
		}
		mutate(&manifest)
		raw, err = json.Marshal(manifest)
		if err != nil {
			return err
		}
		return tx.Put(queryManifestNamespace, []byte("child"), raw)
	}); err != nil {
		t.Fatal(err)
	}
}

func TestParentRecoveryCoverageTracksEnabledAndDisabledWrites(t *testing.T) {
	repo, backend := preparedParentRecoveryFixture(t)
	appendApproval := func(id string) {
		t.Helper()
		payload, err := json.Marshal(map[string]string{"kind": "approval"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := repo.AppendRuntimeFacts(t.Context(), "child", []Entry{{ID: "interaction-requested:" + id, ThreadID: "child", TurnID: "turn", RunID: "run", Type: EntryInteractionAsked, Payload: payload}}); err != nil {
			t.Fatal(err)
		}
	}
	appendApproval("enabled")
	assertParentRecoveryPrepared(t, backend, "child", true)
	repo = reopenCurrentStartupRepo(t, backend, false)
	appendApproval("disabled")
	assertParentRecoveryPrepared(t, backend, "child", false)
	repo = reopenCurrentStartupRepo(t, backend, true)
	if err := repo.BootstrapParentEvents(t.Context()); err != nil {
		t.Fatal(err)
	}
	assertParentRecoveryPrepared(t, backend, "child", true)
	page, err := repo.ParentEvents(t.Context(), ParentEventQuery{ThreadID: "parent"})
	if err != nil || len(page.Events) != 2 || page.Events[0].InteractionID != "enabled" || page.Events[1].InteractionID != "disabled" {
		t.Fatalf("recovered receipts = %+v, %v", page, err)
	}
	before := cloneMigrationRecords(backend.records)
	measured := &currentStartupBackend{Backend: backend}
	repo = reopenCurrentStartupRepo(t, measured, true)
	if err := repo.BootstrapParentEvents(t.Context()); err != nil {
		t.Fatal(err)
	}
	if measured.journalRows != 1 || measured.turnScans != 0 || measured.writes != 0 || !equalMigrationRecords(before, backend.records) {
		t.Fatalf("restart revisited history or rewrote records: %+v", measured)
	}
}

type parentRecoveryCommitBackend struct {
	spi.Backend
	beforeCommit func() error
}

func (backend parentRecoveryCommitBackend) Update(ctx context.Context, mutate func(spi.WriteTx) error) error {
	return backend.Backend.Update(ctx, func(tx spi.WriteTx) error {
		if err := mutate(tx); err != nil {
			return err
		}
		return backend.beforeCommit()
	})
}

func TestParentRecoveryCoveragePublicationIsTransactional(t *testing.T) {
	for _, failure := range []string{"error", "cancel", "panic"} {
		t.Run(failure, func(t *testing.T) {
			_, backend := preparedParentRecoveryFixture(t)
			rewriteParentRecoverySource(t, backend, func(manifest *queryManifest) { manifest.ParentRecovery = nil })
			before := cloneMigrationRecords(backend.records)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			injected := errors.New("injected recovery commit failure")
			failing := parentRecoveryCommitBackend{Backend: backend, beforeCommit: func() error {
				switch failure {
				case "cancel":
					cancel()
					return ctx.Err()
				case "panic":
					panic(injected)
				default:
					return injected
				}
			}}
			repo := reopenCurrentStartupRepo(t, failing, true)
			var recovered any
			var err error
			func() {
				defer func() { recovered = recover() }()
				err = repo.BootstrapParentEvents(ctx)
			}()
			if failure == "panic" && recovered != injected || failure != "panic" && err == nil {
				t.Fatalf("failure did not surface: error=%v, panic=%v", err, recovered)
			}
			if !equalMigrationRecords(before, backend.records) {
				t.Fatal("failed coverage publication changed canonical or derived records")
			}
			repo = reopenCurrentStartupRepo(t, backend, true)
			if err := repo.BootstrapParentEvents(t.Context()); err != nil {
				t.Fatal(err)
			}
			assertParentRecoveryPrepared(t, backend, "child", true)
			before = cloneMigrationRecords(backend.records)
			if err := repo.BootstrapParentEvents(t.Context()); err != nil || !equalMigrationRecords(before, backend.records) {
				t.Fatalf("repeated recovery is not idempotent: %v", err)
			}
		})
	}
}

func TestParentRecoveryCoverageRejectsSourceDrift(t *testing.T) {
	for _, damage := range []string{"count", "hash", "depth", "identity"} {
		t.Run(damage, func(t *testing.T) {
			_, backend := preparedParentRecoveryFixture(t)
			rewriteParentRecoverySource(t, backend, func(manifest *queryManifest) {
				switch damage {
				case "count":
					manifest.ParentRecovery.Count++
				case "hash":
					manifest.ParentRecovery.Last.Hash = "changed"
				case "depth":
					manifest.ParentRecovery.Last.Depth++
				case "identity":
					manifest.ParentRecovery.Last.ID = "unrelated"
				}
			})
			before := cloneMigrationRecords(backend.records)
			repo := reopenCurrentStartupRepo(t, backend, true)
			if err := repo.BootstrapParentEvents(t.Context()); !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("source drift accepted: %v", err)
			}
			if !equalMigrationRecords(before, backend.records) {
				t.Fatal("corrupt source changed during recovery")
			}
		})
	}
}

func TestParentRecoveryCoverageRetainsBranchHistoryAndDoesNotTransferToFork(t *testing.T) {
	repo, backend := preparedParentRecoveryFixture(t)
	meta, err := repo.Thread(t.Context(), "child")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Append(t.Context(), Entry{ThreadID: "child", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "retained branch"}}, AppendOptions{ID: "branch"}); err != nil {
		t.Fatal(err)
	}
	if err := repo.MoveLeaf(t.Context(), "child", meta.LeafID); err != nil {
		t.Fatal(err)
	}
	assertParentRecoveryPrepared(t, backend, "child", true)
	destination := &ForkDestinationMeta{ParentThreadID: "parent", ParentTurnID: "delegation", TaskName: "fork", AgentPath: "/fork", HostProfileRef: "test"}
	if _, err := repo.Fork(t.Context(), ForkOptions{SourceThreadID: "child", NewThreadID: "fork", DestinationMeta: destination}); err != nil {
		t.Fatal(err)
	}
	assertParentRecoveryPrepared(t, backend, "fork", false)
	if err := repo.BootstrapParentEvents(t.Context()); err != nil {
		t.Fatal(err)
	}
	assertParentRecoveryPrepared(t, backend, "fork", true)
	if _, err := repo.Entry(t.Context(), "child", "branch"); err != nil {
		t.Fatalf("inactive history lost: %v", err)
	}
}

func TestParentRecoveryCoverageStartsWithNewChild(t *testing.T) {
	repo, backend := preparedParentRecoveryFixture(t)
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "new-child", ParentThreadID: "parent", TaskName: "new-child", HostProfileRef: "test"}); err != nil {
		t.Fatal(err)
	}
	assertParentRecoveryPrepared(t, backend, "new-child", true)
	for range 32 {
		if _, err := repo.Append(t.Context(), Entry{ThreadID: "new-child", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "new canonical history"}}, AppendOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	assertParentRecoveryPrepared(t, backend, "new-child", true)
	measured := &currentStartupBackend{Backend: backend}
	repo = reopenCurrentStartupRepo(t, measured, true)
	if err := repo.BootstrapParentEvents(t.Context()); err != nil {
		t.Fatal(err)
	}
	if measured.journalRows > 2 || measured.turnScans != 0 || measured.writes != 0 || measured.gets > 80 {
		t.Fatalf("new canonical child needed historical preparation: %+v", measured)
	}
}

func TestParentRecoveryCoverageRollsBackWithSourceAndReceipt(t *testing.T) {
	repo, backend := preparedParentRecoveryFixture(t)
	before := cloneMigrationRecords(backend.records)
	repo.backend = parentEventFailingBackend{Backend: backend}
	request := FinishTurnRequest{ThreadID: "child", TurnID: "turn", RunID: "run", TerminalEntryID: "terminal", OutcomeFingerprint: "terminal", Status: TurnCompleted}
	if _, err := repo.FinishTurn(t.Context(), request); err == nil {
		t.Fatal("failed transaction published a source or receipt")
	}
	if !equalMigrationRecords(before, backend.records) {
		t.Fatal("failed source publication changed recovery coverage")
	}
	repo.backend = backend
	if _, err := repo.FinishTurn(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	assertParentRecoveryPrepared(t, backend, "child", true)
	page, err := repo.ParentEvents(t.Context(), ParentEventQuery{ThreadID: "parent"})
	if err != nil || len(page.Events) != 1 || page.Events[0].Kind != "completed" {
		t.Fatalf("source retry lost receipt: %+v, %v", page, err)
	}
}

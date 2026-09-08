package runtime

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
)

func maintenanceFixture(t *testing.T) (string, identity.ThreadID, identity.TurnID) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state.sqlite")
	host, err := Open(t.Context(), Options{Storage: storage.SQLite(path), DeferExecution: true})
	if err != nil {
		t.Fatal(err)
	}
	service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) {
		t.Error("fixture dispatched")
		return nil, errors.New("unexpected")
	}))
	if err != nil {
		t.Fatal(err)
	}
	view, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	turnID, runID, err := host.nextTurnRunIDs()
	if err != nil {
		t.Fatal(err)
	}
	input := UserInput{Text: "unfinished input"}
	fingerprint, _ := stableFingerprint(input)
	request := sessiontree.AcceptTurnRequest{ThreadID: view.ThreadID.String(), TurnID: turnID.String(), RunID: runID.String(), LogicalRequestID: "unfinished", Input: session.Message{Role: session.User, Content: input.Text}, InputRequestFingerprint: fingerprint, Now: time.Now().UTC()}
	request.RequestFingerprint, err = sessiontree.TurnAcceptanceRequestFingerprint(request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = host.store.repo.(sessiontree.RuntimeTurnRepo).AcceptTurn(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	queued := QueuedInput{ID: "queue:pending", RequestKey: "pending", Input: UserInput{Text: "queued input"}, CreatedAt: time.Now().UTC()}
	fingerprint, _ = stableFingerprint(queued.Input)
	if err = service.(*threadRuntimeService).appendQueueFact(t.Context(), view.ThreadID, sessiontree.EntryQueueAdded, queued.ID, queued.RequestKey, fingerprint, queued); err != nil {
		t.Fatal(err)
	}
	if err = host.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	return path, view.ThreadID, turnID
}

func TestDeferredHostDoesNotDispatchViewOrImport(t *testing.T) {
	path, threadID, _ := maintenanceFixture(t)
	host, err := Open(t.Context(), Options{Storage: storage.SQLite(path), DeferExecution: true})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Shutdown(context.Background())
	called := make(chan struct{}, 4)
	service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) {
		called <- struct{}{}
		return nil, errors.New("test factory")
	}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.View(t.Context(), threadID); err != nil {
		t.Fatal(err)
	}
	if _, err = service.ImportPendingInputs(t.Context(), ImportPendingInputsInput{ThreadID: threadID, Items: []ImportedPendingInput{{RequestKey: "another", Input: UserInput{Text: "another queued input"}}}}); err != nil {
		t.Fatal(err)
	}
	if _, err = service.Send(t.Context(), SendInput{ThreadID: threadID, RequestKey: "send", Input: UserInput{Text: "blocked"}}); !errors.Is(err, ErrExecutionDeferred) {
		t.Fatal(err)
	}
	select {
	case <-called:
		t.Fatal("provider preparation ran before activation")
	case <-time.After(30 * time.Millisecond):
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if err = host.Activate(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if !errors.Is(host.requireExecution(), ErrExecutionDeferred) {
		t.Fatal("cancelled activation enabled execution")
	}
	if err = host.Activate(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("activation did not release hydration")
	}
}

func TestPrepareRestorePreservesInputsAndStopsExecutionAcrossRestart(t *testing.T) {
	path, threadID, turnID := maintenanceFixture(t)
	var calls atomic.Int32
	for attempt := 0; attempt < 2; attempt++ {
		host, err := Open(t.Context(), Options{Storage: storage.SQLite(path), DeferExecution: true})
		if err != nil {
			t.Fatal(err)
		}
		service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) {
			calls.Add(1)
			return nil, errors.New("unexpected dispatch")
		}))
		if err != nil {
			t.Fatal(err)
		}
		result, err := host.PrepareRestore(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if attempt == 0 && (result.StoppedThreads != 1 || result.StoppedQueueInputs != 1) {
			t.Fatalf("result=%+v", result)
		}
		if attempt == 1 && result != (RestorePreparation{}) {
			t.Fatalf("restore repeated=%+v", result)
		}
		view, err := service.View(t.Context(), threadID)
		if err != nil {
			t.Fatal(err)
		}
		if view.Activity != ThreadActivityIdle || len(view.Queue) != 0 || len(view.RestoredInputs) != 1 || view.RestoredInputs[0].Input.Text != "queued input" {
			t.Fatalf("view=%+v", view)
		}
		if err = host.Activate(t.Context()); err != nil {
			t.Fatal(err)
		}
		if _, err = service.Retry(t.Context(), RetryInput{ThreadID: threadID, SourceTurnID: turnID, RequestKey: "unsafe-retry"}); !errors.Is(err, ErrRestoredTurn) {
			t.Fatalf("retry=%v", err)
		}
		if err = host.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("restore dispatched %d calls", calls.Load())
	}
}

func TestSQLiteInspectionAndBackupLeaveSourceUnchanged(t *testing.T) {
	path, _, _ := maintenanceFixture(t)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	result, err := InspectSQLite(t.Context(), path)
	if err != nil || !result.Exists || result.MigrationRequired {
		t.Fatalf("inspection=%+v err=%v", result, err)
	}
	backup := filepath.Join(t.TempDir(), "snapshot.sqlite")
	if err = storage.BackupSQLite(t.Context(), path, backup); err != nil {
		t.Fatal(err)
	}
	if err = storage.BackupSQLite(t.Context(), path, backup); !errors.Is(err, os.ErrExist) {
		t.Fatalf("overwrote backup: %v", err)
	}
	if _, err = InspectSQLite(t.Context(), backup); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("read-only inspection changed source")
	}
	missing := filepath.Join(t.TempDir(), "missing.sqlite")
	if result, err = InspectSQLite(t.Context(), missing); err != nil || result.Exists {
		t.Fatalf("fresh=%+v %v", result, err)
	}
	if _, err = os.Stat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("inspection created database")
	}
}

func TestSQLiteInspectionRejectsFutureLogicalStateWithoutWrites(t *testing.T) {
	path, _, _ := maintenanceFixture(t)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`UPDATE floret_backend_records SET value=? WHERE namespace=? AND key=?`, []byte(`{"version":"999","fingerprint":"future"}`), logicalSchemaNamespace, []byte(logicalSchemaKey)); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(path)
	if _, err = InspectSQLite(t.Context(), path); !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatalf("future=%v", err)
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(before, after) {
		t.Fatal("future source changed")
	}
}

func TestSQLiteSnapshotIncludesUncheckpointedWAL(t *testing.T) {
	path, _, _ := maintenanceFixture(t)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	for _, statement := range []string{`PRAGMA journal_mode=WAL`, `PRAGMA wal_autocheckpoint=0`} {
		if _, err = db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	previous, err := json.Marshal(logicalSchemaEnvelope{Version: previousLogicalSchemaVersion, Fingerprint: previousLogicalSchemaFingerprint})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`UPDATE floret_backend_records SET value=? WHERE namespace=? AND key=?`, previous, logicalSchemaNamespace, []byte(logicalSchemaKey)); err != nil {
		t.Fatal(err)
	}
	walBefore, err := os.ReadFile(path + "-wal")
	if err != nil || len(walBefore) == 0 {
		t.Fatalf("WAL=%d %v", len(walBefore), err)
	}
	inspection, err := InspectSQLite(t.Context(), path)
	if err != nil || !inspection.MigrationRequired {
		t.Fatalf("WAL inspection=%+v %v", inspection, err)
	}
	destination := filepath.Join(t.TempDir(), "backup.sqlite")
	if err = storage.BackupSQLite(t.Context(), path, destination); err != nil {
		t.Fatal(err)
	}
	inspection, err = InspectSQLite(t.Context(), destination)
	if err != nil || !inspection.MigrationRequired {
		t.Fatalf("snapshot lost WAL=%+v %v", inspection, err)
	}
	walAfter, err := os.ReadFile(path + "-wal")
	if err != nil || !bytes.Equal(walBefore, walAfter) {
		t.Fatal("source WAL changed")
	}
}

func TestSnapshotMaintenanceRejectsLiveHostAndCancelledContext(t *testing.T) {
	path, _, _ := maintenanceFixture(t)
	host, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Shutdown(context.Background())
	if _, err = InspectSQLite(t.Context(), path); !errors.Is(err, spi.ErrConflict) {
		t.Fatalf("live inspection=%v", err)
	}
	destination := filepath.Join(t.TempDir(), "backup.sqlite")
	if err = storage.BackupSQLite(t.Context(), path, destination); !errors.Is(err, spi.ErrConflict) {
		t.Fatalf("live snapshot=%v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err = storage.BackupSQLite(ctx, path, destination); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err = os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("failed snapshot created a target")
	}
}

func TestPrepareRestoreInvalidatesInteractionsAndIncludesChildThreads(t *testing.T) {
	path, parentID, _ := maintenanceFixture(t)
	host, err := Open(t.Context(), Options{Storage: storage.SQLite(path), DeferExecution: true})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Shutdown(context.Background())
	service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) {
		t.Error("restored interaction dispatched")
		return nil, errors.New("unexpected")
	}))
	if err != nil {
		t.Fatal(err)
	}
	child, err := service.Create(t.Context(), CreateThreadInput{ParentThreadID: parentID, TaskName: "child", HostProfileRef: "default", RequestKey: "create-child"})
	if err != nil {
		t.Fatal(err)
	}
	turnID, runID, err := host.nextTurnRunIDs()
	if err != nil {
		t.Fatal(err)
	}
	request := sessiontree.AcceptTurnRequest{ThreadID: child.ThreadID.String(), TurnID: turnID.String(), RunID: runID.String(), LogicalRequestID: "child-input", Input: session.Message{Role: session.User, Content: "child task"}, Now: time.Now().UTC()}
	request.InputRequestFingerprint, _ = stableFingerprint(UserInput{Text: "child task"})
	request.RequestFingerprint, err = sessiontree.TurnAcceptanceRequestFingerprint(request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = host.store.repo.(sessiontree.RuntimeTurnRepo).AcceptTurn(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	ids := []identity.ThreadID{parentID, child.ThreadID}
	for i, id := range ids {
		actor := service.(*threadRuntimeService).runtime(id)
		actor.mu.Lock()
		actor.state.view = ThreadView{}
		actor.mu.Unlock()
		view, err := service.View(t.Context(), id)
		if err != nil {
			t.Fatal(err)
		}
		interaction := ThreadInteraction{ID: "pending", TurnID: view.TurnID, RunID: view.RunID, Kind: ThreadInteractionApproval, Approval: &ApprovalPresentation{Label: "Run command", ToolName: "terminal", ToolCallID: "command"}}
		if i == 1 {
			interaction.Kind, interaction.Approval = ThreadInteractionInput, nil
			interaction.Input = &InputPresentation{Questions: []InputQuestion{{ID: "q", Prompt: "Continue?", Kind: "write"}}}
		}
		if _, err = service.(*threadRuntimeService).requestInteraction(t.Context(), service.(*threadRuntimeService).runtime(id), id, interaction); err != nil {
			t.Fatal(err)
		}
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err = host.PrepareRestore(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	result, err := host.PrepareRestore(t.Context())
	if err != nil || result.StoppedThreads != 2 {
		t.Fatalf("restore=%+v %v", result, err)
	}
	if err = host.Activate(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		view, err := service.View(t.Context(), id)
		if err != nil || view.Activity != ThreadActivityIdle || len(view.Interactions) != 1 || !view.Interactions[0].Resolved {
			t.Fatalf("restored=%+v %v", view, err)
		}
		approved := true
		if _, err = service.Respond(t.Context(), RespondInput{ThreadID: id, InteractionID: "pending", Answers: []InteractionAnswer{{Approved: &approved}}, RequestKey: "stale-approval"}); err == nil {
			t.Fatal("restored approval accepted")
		}
	}
}

func TestDeferredHostActivationAndShutdownSerialize(t *testing.T) {
	for i := 0; i < 25; i++ {
		host, err := Open(t.Context(), Options{Storage: storage.Memory(), DeferExecution: true})
		if err != nil {
			t.Fatal(err)
		}
		var workers sync.WaitGroup
		workers.Add(2)
		go func() {
			defer workers.Done()
			if err := host.Activate(t.Context()); err != nil && !errors.Is(err, ErrHostClosed) {
				t.Errorf("activate=%v", err)
			}
		}()
		go func() {
			defer workers.Done()
			if err := host.Shutdown(t.Context()); err != nil {
				t.Errorf("shutdown=%v", err)
			}
		}()
		workers.Wait()
		if err := host.Activate(t.Context()); !errors.Is(err, ErrHostClosed) {
			t.Fatalf("closed activation=%v", err)
		}
	}
}

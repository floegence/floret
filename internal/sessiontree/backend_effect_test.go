package sessiontree

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/session/artifact"
	"github.com/floegence/floret/v7/internal/storagebridge"
	publicstorage "github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
)

func canonicalEffectRequest(attempt EffectAttempt) FinishEffectDispatchRequest {
	return FinishEffectDispatchRequest{EffectAttemptID: attempt.EffectAttemptID, RequestFingerprint: "effect", OutcomeFingerprint: "done",
		Result: Entry{ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolResult, Message: session.Message{Role: session.Tool, ToolCallID: "call", ToolName: "shell", Content: "result", ToolResult: &session.ToolResultView{Status: "success"}}}}
}

func TestV32EffectDispatchArtifactRollbackAndReplay(t *testing.T) {
	backend, repo, attempt := canonicalSettlementFixture(t, false)
	prepared, err := repo.PrepareEffectAttempt(t.Context(), PrepareEffectAttemptRequest{Invocation: attempt.Invocation, RequestFingerprint: "effect"})
	if err != nil || !prepared.Replayed {
		t.Fatalf("prepare=%+v, %v", prepared, err)
	}
	if _, err := repo.BeginEffectDispatch(t.Context(), BeginEffectDispatchRequest{EffectAttemptID: attempt.EffectAttemptID, RequestFingerprint: "effect", AuthorizationProofHash: "proof"}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.BeginEffectDispatch(t.Context(), BeginEffectDispatchRequest{EffectAttemptID: attempt.EffectAttemptID, RequestFingerprint: "effect", AuthorizationProofHash: "proof"}); !errors.Is(err, ErrEffectOutcomeUnknown) {
		t.Fatalf("dispatch replay=%v", err)
	}
	request := canonicalEffectRequest(attempt)
	request.FullOutput = &artifact.FullOutput{Text: "complete immutable output"}
	before := cloneMigrationRecords(backend.records)
	rollback := errors.New("effect commit failed")
	repo.backend = &domainCommitBoundaryBackend{Backend: backend, afterCallback: func() error { return rollback }}
	if _, err := repo.FinishEffectDispatch(t.Context(), request); !errors.Is(err, rollback) {
		t.Fatalf("finish rollback=%v", err)
	}
	if !reflect.DeepEqual(before, backend.records) {
		t.Fatal("effect output or artifact survived rollback")
	}
	repo.backend = backend
	finished, err := repo.FinishEffectDispatch(t.Context(), request)
	if err != nil || finished.Artifact == nil || finished.Attempt.State != EffectAttemptCompleted {
		t.Fatalf("finish=%+v, %v", finished, err)
	}
	stored, err := repo.EffectAttempt(t.Context(), "thread", attempt.EffectAttemptID)
	if err != nil || stored.ResultEntryID != finished.Result.ID {
		t.Fatalf("stored=%+v, %v", stored, err)
	}
	if repo.threadMemory != nil {
		t.Fatal("effect dispatch loaded the journal")
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		var err error
		repo, _, err = NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, false)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	replayed, err := repo.FinishEffectDispatch(t.Context(), request)
	if err != nil || !replayed.Replayed || replayed.Result.ID != finished.Result.ID || !reflect.DeepEqual(replayed.Artifact, finished.Artifact) {
		t.Fatalf("replay=%+v, %v", replayed, err)
	}
	request.FullOutput = &artifact.FullOutput{Text: "changed output"}
	if _, err := repo.FinishEffectDispatch(t.Context(), request); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("changed artifact=%v", err)
	}
	if repo.threadMemory != nil {
		t.Fatal("effect replay loaded the journal")
	}
}

func TestV32EffectStopAndCompletionSerialize(t *testing.T) {
	for iteration := range 12 {
		t.Run(fmt.Sprint(iteration), func(t *testing.T) {
			fixture, _, attempt := canonicalSettlementFixture(t, true)
			backend, err := storagebridge.Open(t.Context(), storagebridge.Source(publicstorage.Memory()))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = backend.Close() })
			var repo *BackendRepo
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				for namespace, rows := range fixture.records {
					for key, value := range rows {
						if err := tx.Put(namespace, []byte(key), value); err != nil {
							return err
						}
					}
				}
				var err error
				repo, _, err = NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, false)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			start := make(chan struct{})
			var wait sync.WaitGroup
			var cancelErr, finishErr error
			var cancelled CancelTurnResult
			wait.Add(2)
			go func() {
				defer wait.Done()
				<-start
				cancelled, cancelErr = repo.CancelTurn(t.Context(), testCancelTurnRequest(time.Now()))
			}()
			go func() {
				defer wait.Done()
				<-start
				_, finishErr = repo.FinishEffectDispatch(t.Context(), canonicalEffectRequest(attempt))
			}()
			close(start)
			wait.Wait()
			if cancelErr != nil || finishErr != nil && !errors.Is(finishErr, ErrStaleAuthority) {
				t.Fatalf("cancel=%v finish=%v", cancelErr, finishErr)
			}
			wantStatus, wantState := TurnAborted, EffectAttemptCompleted
			if finishErr != nil {
				wantStatus, wantState = TurnFailed, EffectAttemptUnknown
			}
			if cancelled.Terminal.TurnStatus != wantStatus {
				t.Fatalf("terminal=%+v", cancelled.Terminal)
			}
			stored, err := repo.EffectAttempt(t.Context(), "thread", attempt.EffectAttemptID)
			if err != nil || stored.State != wantState {
				t.Fatalf("attempt=%+v, %v", stored, err)
			}
			entries, err := repo.Entries(t.Context(), "thread")
			if err != nil {
				t.Fatal(err)
			}
			terminals, results := 0, 0
			for _, entry := range entries {
				if entry.Type == EntryTurnMarker && terminalTurnMarker(entry.TurnStatus) {
					terminals++
				}
				if entry.Type == EntryToolResult {
					results++
				}
			}
			if terminals != 1 || results != 1 || repo.threadMemory != nil {
				t.Fatalf("terminals=%d results=%d", terminals, results)
			}
		})
	}
}

func TestV32StopBeforeDispatchPreservesCancellation(t *testing.T) {
	_, repo, attempt := canonicalSettlementFixture(t, false)
	if _, err := repo.CancelTurn(t.Context(), testCancelTurnRequest(time.Now())); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.BeginEffectDispatch(t.Context(), BeginEffectDispatchRequest{EffectAttemptID: attempt.EffectAttemptID, RequestFingerprint: "effect", AuthorizationProofHash: "proof"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("dispatch after stop=%v", err)
	}
}

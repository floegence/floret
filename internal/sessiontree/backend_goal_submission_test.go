package sessiontree

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

func openCurrentGoalRepo(t *testing.T, backend spi.Backend) (*BackendRepo, error) {
	t.Helper()
	var repo *BackendRepo
	err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		fresh, err := IsFreshBackendRepoInTransaction(t.Context(), tx)
		if err != nil {
			return err
		}
		if fresh {
			repo, err = NewFreshCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, false)
		} else {
			repo, _, err = NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, false)
		}
		return err
	})
	return repo, err
}

func TestV32GoalSubmissionPersistsDecisionAcrossRestart(t *testing.T) {
	for _, active := range []bool{false, true} {
		t.Run(map[bool]string{false: "idle", true: "active"}[active], func(t *testing.T) {
			backend := newMigrationTestBackend()
			r, err := openCurrentGoalRepo(t, backend)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := r.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
				t.Fatal(err)
			}
			if active {
				if _, err := r.AcceptTurn(t.Context(), AcceptTurnRequest{ThreadID: "thread", TurnID: "old", RunID: "old-run", LogicalRequestID: "old", RequestFingerprint: "old", Input: session.Message{Role: session.User, Content: "Original task"}}); err != nil {
					t.Fatal(err)
				}
			}
			in := goalSubmissionInput()
			first, err := r.SubmitGoal(t.Context(), in)
			if err != nil || (first.Accepted != nil) == active {
				t.Fatalf("first=%+v err=%v", first, err)
			}
			turn, run := "goal-turn", "goal-run"
			if active {
				turn, run = "old", "old-run"
			}
			if _, err := r.CancelTurn(t.Context(), CancelTurnRequest{ThreadID: "thread", TurnID: turn, RunID: run, CancelEntryID: "stop", TerminalEntryID: "stopped", RequestKey: "stop", RequestFingerprint: "stop", OutcomeFingerprint: "stop", InteractionResolutionPayload: json.RawMessage(`{"cancelled":true}`), Metadata: map[string]string{TurnFailureCodeMetadataKey: TurnFailureCancelled}}); err != nil {
				t.Fatal(err)
			}
			r, err = openCurrentGoalRepo(t, backend)
			if err != nil {
				t.Fatal(err)
			}
			if err := r.PrepareQueryThread(t.Context(), "thread"); err != nil {
				t.Fatal(err)
			}
			before := cloneMigrationRecords(backend.records)
			in.Turn.TurnID, in.Turn.RunID = "different-turn", "different-run"
			replay, err := r.SubmitGoal(t.Context(), in)
			if err != nil || !replay.Mutation.Replayed || (replay.Accepted != nil) == active {
				t.Fatalf("replay=%+v err=%v", replay, err)
			}
			if !reflect.DeepEqual(before, backend.records) {
				t.Fatal("replay changed journal after Stop and restart")
			}
			in.Mutation.Text = "Changed request"
			in.Turn.Input.Content = in.Mutation.Text
			if _, err := r.SubmitGoal(t.Context(), in); !errors.Is(err, ErrRequestConflict) {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestV32GoalSubmissionCommitFailureLeavesNoAdmission(t *testing.T) {
	backend := newMigrationTestBackend()
	repo, err := openCurrentGoalRepo(t, backend)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	before := cloneMigrationRecords(backend.records)
	repo.backend = goalFailingBackend{Backend: backend}
	if _, err := repo.SubmitGoal(t.Context(), goalSubmissionInput()); err == nil {
		t.Fatal("expected commit failure")
	}
	if !reflect.DeepEqual(before, backend.records) {
		t.Fatal("partial goal or admission survived rollback")
	}
	repo.backend = backend
	if repo.threadMemory != nil || repo.domainMemory != nil {
		t.Fatal("goal submission used a MemoryRepo")
	}
	accepted, err := repo.SubmitGoal(t.Context(), goalSubmissionInput())
	if err != nil || accepted.Accepted == nil || accepted.Mutation.State.Version != 1 {
		t.Fatalf("accepted=%+v err=%v", accepted, err)
	}
}

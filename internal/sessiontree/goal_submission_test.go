package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

func goalSubmissionInput() GoalSubmission {
	return GoalSubmission{Mutation: GoalMutation{ThreadID: "thread", RequestKey: "goal", Actor: "user", Operation: "set", Text: "User submitted objective"}, Turn: AcceptTurnRequest{ThreadID: "thread", TurnID: "goal-turn", RunID: "goal-run", LogicalRequestID: "goal-input", RequestFingerprint: "admission", InputRequestFingerprint: "input", Input: session.Message{Role: session.User, Content: "User submitted objective"}}}
}

func TestGoalSubmissionPersistsDecisionAcrossRestart(t *testing.T) {
	for _, active := range []bool{false, true} {
		t.Run(map[bool]string{false: "idle", true: "active"}[active], func(t *testing.T) {
			backend := newMigrationTestBackend()
			r, err := NewBackendRepo(t.Context(), backend, time.Now)
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
			r, err = NewBackendRepo(t.Context(), backend, time.Now)
			if err != nil {
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

func TestGoalSubmissionBackendFailureRollsBackGoalAndAdmission(t *testing.T) {
	backend := newMigrationTestBackend()
	r, err := NewBackendRepo(t.Context(), backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	before := cloneMigrationRecords(backend.records)
	r.backend = goalFailingBackend{Backend: backend}
	if _, err := r.SubmitGoal(t.Context(), goalSubmissionInput()); err == nil {
		t.Fatal("expected failed commit")
	}
	if !reflect.DeepEqual(before, backend.records) {
		t.Fatal("partial durable goal submission")
	}
	r.backend = backend
	raw, _ := r.domainMemory.EncodeMemoryState()
	if len(r.domainMemory.entries["thread"]) != 0 {
		t.Fatalf("partial memory submission: %s", raw)
	}
	if _, err := r.SubmitGoal(t.Context(), goalSubmissionInput()); err != nil {
		t.Fatal(err)
	}
}

type goalFailingBackend struct{ spi.Backend }

func (b goalFailingBackend) Update(ctx context.Context, mutate func(spi.WriteTx) error) error {
	return b.Backend.Update(ctx, func(tx spi.WriteTx) error {
		if err := mutate(tx); err != nil {
			return err
		}
		return errors.New("commit failed")
	})
}

func TestGoalSubmissionRacesTerminalWithOneDecision(t *testing.T) {
	for i := 0; i < 40; i++ {
		r := goalTestTurn(t)
		var wg sync.WaitGroup
		wg.Add(2)
		var result GoalSubmissionResult
		var submitErr, finishErr error
		go func() { defer wg.Done(); result, submitErr = r.SubmitGoal(t.Context(), goalSubmissionInput()) }()
		go func() {
			defer wg.Done()
			_, finishErr = r.FinishTurn(t.Context(), FinishTurnRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", TerminalEntryID: "done", Status: TurnCompleted, OutcomeFingerprint: "done"})
		}()
		wg.Wait()
		if submitErr != nil || finishErr != nil {
			t.Fatalf("submit=%v finish=%v", submitErr, finishErr)
		}
		in := goalSubmissionInput()
		in.Turn.TurnID = "retry-turn"
		in.Turn.RunID = "retry-run"
		replay, err := r.SubmitGoal(t.Context(), in)
		if err != nil || !replay.Mutation.Replayed || (replay.Accepted == nil) != (result.Accepted == nil) {
			t.Fatalf("replay=%+v original=%+v err=%v", replay, result, err)
		}
	}
}

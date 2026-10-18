package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
)

func goalTestTurn(t *testing.T) *MemoryRepo {
	t.Helper()
	r := newMemoryRepo(time.Now)
	if _, err := r.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.AcceptTurn(t.Context(), AcceptTurnRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", RequestFingerprint: "turn", InputRequestFingerprint: "input", Input: session.Message{Role: session.User, Content: "Deliver result"}, Now: time.Now()}); err != nil {
		t.Fatal(err)
	}
	return r
}
func testGoalPath(t *testing.T, r *MemoryRepo) []Entry {
	t.Helper()
	p, err := r.Path(t.Context(), "thread", "")
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func TestGoalReviewInvalidationAndTerminalCommit(t *testing.T) {
	for _, mutation := range []string{"none", "goal", "tool", "steering"} {
		t.Run(mutation, func(t *testing.T) {
			r := goalTestTurn(t)
			if _, err := r.MutateGoal(t.Context(), GoalMutation{ThreadID: "thread", RequestKey: "set", Actor: "user", Operation: "set", Text: "Deliver result"}); err != nil {
				t.Fatal(err)
			}
			review, err := r.RecordGoalReview(t.Context(), "thread", "turn", "run", "review", 1, "completed", "Verified the result")
			if err != nil {
				t.Fatal(err)
			}
			state, _ := GoalFromPath(testGoalPath(t, r))
			if state.Status != "active" {
				t.Fatal("review prematurely completed goal")
			}
			switch mutation {
			case "goal":
				_, err = r.MutateGoal(t.Context(), GoalMutation{ThreadID: "thread", RequestKey: "update", Actor: "user", Operation: "set", Text: "Deliver updated result", ExpectedVersion: 1})
			case "tool":
				_, err = r.Append(t.Context(), RunMessageEntry("thread", "turn", "run", session.Message{Role: session.Assistant, ToolName: "read", ToolCallID: "later", ToolArgs: "{}"}, time.Now()), AppendOptions{})
			case "steering":
				_, err = r.Append(t.Context(), Entry{ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntrySteeringApplied, Payload: json.RawMessage("{}")}, AppendOptions{})
			}
			if err != nil {
				t.Fatal(err)
			}
			_, err = r.FinishTurn(t.Context(), FinishTurnRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", TerminalEntryID: "finish", Status: TurnCompleted, OutcomeFingerprint: "finish", Metadata: map[string]string{"goal_guard": "1", GoalReviewMetadataKey: review.ID}, Now: time.Now()})
			if mutation != "none" {
				if err == nil {
					t.Fatal("stale review allowed completion")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			state, err = GoalFromPath(testGoalPath(t, r))
			if err != nil || state.Status != "completed" {
				t.Fatalf("state=%+v err=%v", state, err)
			}
		})
	}
}
func TestGoalUserDeletionSuppressionAndFork(t *testing.T) {
	r := goalTestTurn(t)
	set := GoalMutation{ThreadID: "thread", RequestKey: "set", Actor: "user", Operation: "set", Text: "Result"}
	if _, err := r.MutateGoal(t.Context(), set); err != nil {
		t.Fatal(err)
	}
	if _, err := r.MutateGoal(t.Context(), GoalMutation{ThreadID: "thread", RequestKey: "delete", Actor: "user", Operation: "delete", ExpectedVersion: 1}); err != nil {
		t.Fatal(err)
	}
	for _, newTask := range []bool{false, true} {
		if _, err := r.MutateGoal(t.Context(), GoalMutation{ThreadID: "thread", TurnID: "turn", RunID: "run", RequestKey: "model", Actor: "model", Operation: "set", Text: "Result", ExpectedVersion: 2, NewTask: newTask}); err == nil {
			t.Fatal("recreated deleted goal for same user input")
		}
	}
	if _, err := r.Fork(t.Context(), ForkOptions{SourceThreadID: "thread", NewThreadID: "fork", Now: time.Now()}); err != nil {
		t.Fatal(err)
	}
	path, err := r.Path(t.Context(), "fork", "")
	if err != nil {
		t.Fatal(err)
	}
	state, err := GoalFromPath(path)
	if err != nil || state.SuppressedAtInput != latestGoalInput(path) {
		t.Fatalf("fork suppression=%+v err=%v", state, err)
	}
	if _, err := r.MutateGoal(t.Context(), GoalMutation{ThreadID: "fork", RequestKey: "set-fork", Actor: "user", Operation: "set", Text: "Fork result", ExpectedVersion: 2}); err != nil {
		t.Fatal(err)
	}
	state, _ = GoalFromPath(testGoalPath(t, r))
	if state.Status != "absent" {
		t.Fatal("fork changed source goal")
	}
}
func TestGoalReviewRejectsPendingWorkAndCancelledContext(t *testing.T) {
	r := goalTestTurn(t)
	if _, err := r.MutateGoal(t.Context(), GoalMutation{ThreadID: "thread", RequestKey: "set", Actor: "user", Operation: "set", Text: "Result"}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Append(t.Context(), RunMessageEntry("thread", "turn", "run", session.Message{Role: session.Assistant, ToolName: "read", ToolCallID: "pending", ToolArgs: "{}"}, time.Now()), AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.RecordGoalReview(t.Context(), "thread", "turn", "run", "review", 1, "completed", "Verified"); err == nil {
		t.Fatal("review accepted pending work")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := r.RecordGoalReview(ctx, "thread", "turn", "run", "review", 1, "blocked", "Cancelled"); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
}

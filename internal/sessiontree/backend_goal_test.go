package sessiontree

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestGuardedFinishReadsOnlyCurrentGoalSources(t *testing.T) {
	for _, history := range []int{16, 512} {
		t.Run(fmt.Sprint(history), func(t *testing.T) {
			memory := newMemoryRepo(time.Now)
			if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
				t.Fatal(err)
			}
			for index := range history {
				if _, err := memory.Append(t.Context(), Entry{ThreadID: "thread", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "prior history"}}, AppendOptions{ID: fmt.Sprint(index)}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := memory.AcceptTurn(t.Context(), AcceptTurnRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", RequestFingerprint: "turn", InputRequestFingerprint: "input", Input: session.Message{Role: session.User, Content: "Reply"}}); err != nil {
				t.Fatal(err)
			}
			base, repo := openV31ReviewFixture(t, memory)
			if err := repo.PrepareQueryThread(t.Context(), "thread"); err != nil {
				t.Fatal(err)
			}
			bounded := &appendBoundedBackend{Backend: base}
			repo.backend = bounded
			if _, err := repo.FinishTurn(t.Context(), FinishTurnRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", TerminalEntryID: "done", OutcomeFingerprint: "finish", Status: TurnCompleted, Metadata: map[string]string{"goal_guard": "1"}}); err != nil {
				t.Fatal(err)
			}
			if bounded.reads > 180 {
				t.Fatalf("finishing a turn without a goal read %d records for %d historical messages", bounded.reads, history)
			}
		})
	}
}

func TestV32GoalReviewAndSuppressionUseMetadataOnly(t *testing.T) {
	memory := goalTestTurn(t)
	base, repo := openV31ReviewFixture(t, memory)
	if err := repo.PrepareQueryThread(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	repo.backend = goalMetadataBackend{base}
	set := GoalMutation{ThreadID: "thread", RequestKey: "set", Actor: "user", Operation: "set", Text: "Deliver result"}
	created, err := repo.MutateGoal(t.Context(), set)
	if err != nil || created.State.Version != 1 {
		t.Fatalf("goal=%+v, %v", created, err)
	}
	if replay, err := repo.MutateGoal(t.Context(), set); err != nil || !replay.Replayed {
		t.Fatalf("replay=%+v, %v", replay, err)
	}
	review, err := repo.RecordGoalReview(t.Context(), "thread", "turn", "run", "review", 1, "completed", "Verified result")
	if err != nil {
		t.Fatal(err)
	}
	checkReview := func(valid bool) {
		t.Helper()
		facts, err := ReadExecutionGoal(t.Context(), repo, "thread", "turn", "run")
		if err != nil || facts.ValidReview != valid {
			t.Fatalf("review facts=%+v err=%v", facts, err)
		}
	}
	checkReview(true)
	if _, err := repo.MutateGoal(t.Context(), GoalMutation{ThreadID: "thread", RequestKey: "delete", Actor: "user", Operation: "delete", ExpectedVersion: 1}); err != nil {
		t.Fatal(err)
	}
	checkReview(false)
	if _, err := repo.MutateGoal(t.Context(), GoalMutation{ThreadID: "thread", TurnID: "turn", RunID: "run", RequestKey: "model", Actor: "model", Operation: "set", Text: "Deliver result", ExpectedVersion: 2, NewTask: true}); err == nil {
		t.Fatal("recreated suppressed goal")
	}
	if _, err := repo.MutateGoal(t.Context(), GoalMutation{ThreadID: "thread", RequestKey: "set-again", Actor: "user", Operation: "set", Text: "Updated result", ExpectedVersion: 2}); err != nil {
		t.Fatal(err)
	}
	finish := FinishTurnRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", TerminalEntryID: "done", OutcomeFingerprint: "done", Status: TurnCompleted, Metadata: map[string]string{"goal_guard": "1", GoalReviewMetadataKey: review.ID}}
	if _, err := repo.FinishTurn(t.Context(), finish); err == nil {
		t.Fatal("stale review completed changed goal")
	}
	review, err = repo.RecordGoalReview(t.Context(), "thread", "turn", "run", "review-again", 3, "completed", "Verified updated result")
	if err != nil {
		t.Fatal(err)
	}
	finish.Metadata[GoalReviewMetadataKey] = review.ID
	done, err := repo.FinishTurn(t.Context(), finish)
	if err != nil || done.Terminal.Metadata[GoalCompletedMetadataKey] != "3" {
		t.Fatalf("finish=%+v, %v", done, err)
	}
	if repo.threadMemory != nil {
		t.Fatal("Goal operation loaded a MemoryRepo")
	}
}

type goalMetadataBackend struct{ spi.Backend }

func (b goalMetadataBackend) Update(ctx context.Context, mutate func(spi.WriteTx) error) error {
	return b.Backend.Update(ctx, func(tx spi.WriteTx) error { return mutate(goalMetadataTx{tx}) })
}

type goalMetadataTx struct{ spi.WriteTx }

func (tx goalMetadataTx) Get(namespace string, key []byte) ([]byte, error) {
	if namespace == canonicalBodyNamespace {
		return nil, errors.New("Goal operation read historical body")
	}
	return tx.WriteTx.Get(namespace, key)
}

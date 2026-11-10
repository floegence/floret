package sessiontree

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
)

func TestMessageRevisionAtomicHistoryAndReplay(t *testing.T) {
	for _, durable := range []bool{false, true} {
		t.Run(map[bool]string{false: "memory", true: "backend"}[durable], func(t *testing.T) {
			memory := newMemoryRepo(time.Now)
			if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
				t.Fatal(err)
			}
			var repo interface {
				JournalRepo
				RuntimeTurnRepo
			} = memory
			if durable {
				_, backend := openV31ReviewFixture(t, memory)
				repo = backend
			}
			original := AcceptTurnRequest{ThreadID: "thread", TurnID: "old", RunID: "old-run", LogicalRequestID: "old", RequestFingerprint: "old-fp", InputRequestFingerprint: "old-input", Input: session.Message{Role: session.User, Content: "original"}}
			first, err := repo.AcceptTurn(t.Context(), original)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = repo.Append(t.Context(), Entry{ID: "answer", ThreadID: "thread", TurnID: "old", RunID: "old-run", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "old answer"}}, AppendOptions{}); err != nil {
				t.Fatal(err)
			}
			if _, err = repo.Append(t.Context(), Entry{ID: "end", ThreadID: "thread", TurnID: "old", RunID: "old-run", Type: EntryTurnMarker, TurnStatus: TurnCompleted}, AppendOptions{}); err != nil {
				t.Fatal(err)
			}
			before, err := repo.Entries(t.Context(), "thread")
			if err != nil {
				t.Fatal(err)
			}
			edit := AcceptTurnRequest{ThreadID: "thread", TurnID: "new", RunID: "new-run", LogicalRequestID: "edit", RequestFingerprint: "edit-fp", InputRequestFingerprint: "edit-input", Input: session.Message{Role: session.User, Content: "replacement"}, EditSourceEntryID: first.UserMessage.ID, EditExpectedInput: original.Input}
			accepted, err := repo.AcceptTurn(t.Context(), edit)
			if err != nil {
				t.Fatal(err)
			}
			path, err := repo.Path(t.Context(), "thread", "")
			if err != nil {
				t.Fatal(err)
			}
			messages, err := BuildContextChecked(path, ContextOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if len(messages) != 1 || messages[0].Content != "replacement" {
				t.Fatalf("context: %+v", messages)
			}
			all, err := repo.Entries(t.Context(), "thread")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, all[:len(before)]) {
				t.Fatal("revision rewrote historical entries")
			}
			replay, err := repo.AcceptTurn(t.Context(), edit)
			if err != nil || !replay.Replayed || replay.UserMessage.ID != accepted.UserMessage.ID {
				t.Fatalf("replay: %+v %v", replay, err)
			}
			old, err := repo.Path(t.Context(), "thread", "end")
			if err != nil {
				t.Fatal(err)
			}
			history, err := BuildContextChecked(old, ContextOptions{})
			if err != nil || len(history) != 2 || history[0].Content != "original" {
				t.Fatalf("history: %+v %v", history, err)
			}
		})
	}
}

func TestMessageRevisionRejectsStaleSourceWithoutMutation(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	_, _ = memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"})
	req := AcceptTurnRequest{ThreadID: "thread", TurnID: "one", RunID: "run-one", LogicalRequestID: "one", RequestFingerprint: "one", Input: session.Message{Role: session.User, Content: "one"}}
	first, err := memory.AcceptTurn(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	_, err = memory.Append(t.Context(), Entry{ThreadID: "thread", TurnID: "one", RunID: "run-one", Type: EntryTurnMarker, TurnStatus: TurnCompleted}, AppendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	before, _ := memory.Entries(t.Context(), "thread")
	req.TurnID, req.RunID, req.LogicalRequestID = "two", "run-two", "two"
	req.EditSourceEntryID = first.UserMessage.ID
	req.EditExpectedInput = session.Message{Role: session.User, Content: "different"}
	_, err = memory.AcceptTurn(t.Context(), req)
	if !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("want conflict, got %v", err)
	}
	after, _ := memory.Entries(t.Context(), "thread")
	if !reflect.DeepEqual(before, after) {
		t.Fatal("conflict mutated history")
	}
}

func TestMessageRevisionPreservesGoalAndRollsBack(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	_, _ = memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"})
	backend, repo := openV31ReviewFixture(t, memory)
	old := AcceptTurnRequest{ThreadID: "thread", TurnID: "one", RunID: "run", LogicalRequestID: "one", RequestFingerprint: "one", Input: session.Message{Role: session.User, Content: "one"}}
	accepted, err := repo.AcceptTurn(t.Context(), old)
	if err != nil {
		t.Fatal(err)
	}
	mutation, err := repo.MutateGoal(t.Context(), GoalMutation{ThreadID: "thread", RequestKey: "goal", Actor: "user", Operation: "set", Text: "keep this goal"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = repo.Append(t.Context(), Entry{ID: "terminal", ThreadID: "thread", TurnID: "one", RunID: "run", Type: EntryTurnMarker, TurnStatus: TurnCompleted}, AppendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	edit := AcceptTurnRequest{ThreadID: "thread", TurnID: "two", RunID: "run-two", LogicalRequestID: "two", RequestFingerprint: "two", Input: session.Message{Role: session.User, Content: "two"}, EditSourceEntryID: accepted.UserMessage.ID, EditExpectedInput: old.Input}
	if err := repo.PrepareQueryThread(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	before := cloneMigrationRecords(backend.records)
	failure := errors.New("commit failed")
	repo.backend = &domainCommitBoundaryBackend{Backend: backend, afterCallback: func() error { return failure }}
	if _, err = repo.AcceptTurn(t.Context(), edit); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, backend.records) {
		t.Fatal("failed edit changed canonical storage")
	}
	repo.backend = backend
	if _, err = repo.AcceptTurn(t.Context(), edit); err != nil {
		t.Fatal(err)
	}
	path, err := repo.Path(t.Context(), "thread", "")
	if err != nil {
		t.Fatal(err)
	}
	goal, err := GoalFromPath(path)
	if err != nil || !reflect.DeepEqual(goal, mutation.State) {
		t.Fatalf("goal %+v %v", goal, err)
	}
	if _, err = repo.MutateGoal(t.Context(), GoalMutation{ThreadID: "thread", RequestKey: "goal-next", Actor: "user", Operation: "set", ExpectedVersion: goal.Version, Text: "next goal"}); err != nil {
		t.Fatal(err)
	}
}

func TestMessageRevisionEligibilityAndUsage(t *testing.T) {
	user := PrepareEntry(Entry{ID: "user:one", ThreadID: "thread", ParentID: "start", TurnID: "one", RunID: "run", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "one"}})
	start := PrepareEntry(Entry{ID: "start", ThreadID: "thread", TurnID: "one", RunID: "run", Type: EntryTurnMarker, TurnStatus: TurnStarted})
	for _, tail := range []Entry{{Type: EntrySteeringApplied}, {Type: EntryTurnMarker, Metadata: map[string]string{TurnFailureCodeMetadataKey: TurnFailureEffectOutcomeUnknown}}} {
		if _, err := LatestEditableInput([]Entry{start, user, tail}); err == nil {
			t.Fatalf("accepted disallowed boundary %+v", tail)
		}
	}
	totals := &ThreadTokenUsageTotals{InputTokens: 123, OutputTokens: 45}
	raw, _ := json.Marshal(MessageRevision{SourceThreadID: "thread", SourceEntryID: "older", PreviousLeafID: "previous", Goal: session.GoalState{Status: "absent"}, UsageTotals: totals})
	oldRevision := PrepareEntry(Entry{ID: "revision", ThreadID: "thread", Type: EntryMessageRevision, Payload: raw})
	revision, err := prepareMessageRevision([]Entry{oldRevision, start, user}, AcceptTurnRequest{ThreadID: "thread", LogicalRequestID: "edit", EditSourceEntryID: user.ID, EditExpectedInput: user.Message}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeMessageRevision(revision)
	if err != nil || !reflect.DeepEqual(decoded.UsageTotals, totals) {
		t.Fatalf("usage %+v %v", decoded, err)
	}
	var projection ThreadContextProjection
	if err := projection.Observe(revision); err != nil {
		t.Fatal(err)
	}
	snapshot, err := projection.Snapshot(time.Time{})
	if err != nil || !reflect.DeepEqual(snapshot.UsageTotals, totals) {
		t.Fatalf("projection %+v %v", snapshot, err)
	}
}

package sessiontree

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

func goalSourceFixture(t *testing.T, count int) (*migrationTestBackend, *BackendRepo) {
	t.Helper()
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= count; i++ {
		raw, _ := json.Marshal(GoalChange{State: session.GoalState{Version: int64(i), Text: fmt.Sprint("goal ", i), Status: "active", Actor: "user"}, Operation: "set"})
		if _, err := memory.Append(t.Context(), Entry{ID: fmt.Sprint("goal-", i), ThreadID: "thread", Type: EntryGoal, Payload: raw}, AppendOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := memory.Append(t.Context(), Entry{ID: "tail", ThreadID: "thread", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "visible"}}, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	backend, repo := openV31ReviewFixture(t, memory)
	if err := repo.PrepareThread(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.ReadWindowProjection(t.Context(), ItemMembershipWindowOptions{ThreadID: "thread", Limit: 1}, func(PathMetadata, bool) []string { return nil }); err != nil {
		t.Fatal(err)
	}
	return backend, repo
}
func TestGoalWindowCurrentSourcesDoNotReadGoalHistory(t *testing.T) {
	baseline := 0
	for _, count := range []int{100, 1000} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			backend, repo := goalSourceFixture(t, count)
			repo = reopenCurrentStartupRepo(t, backend, false)
			measured := &projectionReadBackend{Backend: backend}
			repo.backend = measured
			got, err := repo.ReadWindowProjection(t.Context(), ItemMembershipWindowOptions{ThreadID: "thread", Limit: 1}, func(PathMetadata, bool) []string { return nil })
			if err != nil {
				t.Fatal(err)
			}
			if got.Facts.Goal == nil || got.Facts.Goal.Version != int64(count) || got.Facts.Goal.Text != fmt.Sprint("goal ", count) {
				t.Fatalf("goal=%+v", got.Facts.Goal)
			}
			if len(got.Entries) != 1 || measured.sources > 32 {
				t.Fatalf("history leaked: entries=%d sources=%d", len(got.Entries), measured.sources)
			}
			if baseline == 0 {
				baseline = measured.sources
			} else if baseline != measured.sources {
				t.Fatalf("reads grew: %d -> %d", baseline, measured.sources)
			}
			t.Logf("goals=%d entries=%d canonical reads=%d", count, len(got.Entries), measured.sources)
		})
	}
}
func TestGoalSourceStateRetainsResetCompletionForkAndRollback(t *testing.T) {
	backend, repo := goalSourceFixture(t, 3)
	check := func() {
		t.Helper()
		path, err := repo.Path(t.Context(), "thread", "")
		if err != nil {
			t.Fatal(err)
		}
		want, err := GoalFromPath(path)
		if err != nil {
			t.Fatal(err)
		}
		got, err := repo.ReadWindowProjection(t.Context(), ItemMembershipWindowOptions{ThreadID: "thread", Limit: 1}, func(PathMetadata, bool) []string { return nil })
		if err != nil || got.Facts.Goal == nil || !reflect.DeepEqual(*got.Facts.Goal, want) {
			t.Fatalf("got=%+v want=%+v err=%v", got.Facts.Goal, want, err)
		}
	}
	add := func(e Entry) {
		t.Helper()
		e.ThreadID = "thread"
		if _, err := repo.Append(t.Context(), e, AppendOptions{}); err != nil {
			t.Fatal(err)
		}
		check()
	}
	check()
	add(Entry{ID: "completed", Type: EntryTurnMarker, TurnStatus: TurnCompleted, TurnID: "t", RunID: "r", Metadata: map[string]string{GoalCompletedMetadataKey: "3"}})
	add(Entry{ID: "stale-completion", Type: EntryTurnMarker, TurnStatus: TurnCompleted, TurnID: "t2", RunID: "r2", Metadata: map[string]string{GoalCompletedMetadataKey: "2"}})
	add(Entry{ID: "reset", Type: EntryUserMessage, Metadata: map[string]string{SubAgentUserMessageOriginMetadataKey: SubAgentUserMessageOriginDelegatedMission}, Message: session.Message{Role: session.User, Content: "new mission"}})
	if err := repo.MoveLeaf(t.Context(), "thread", "goal-2"); err != nil {
		t.Fatal(err)
	}
	check()
	raw, _ := json.Marshal(GoalChange{State: session.GoalState{Version: 3, Text: "branch", Status: "active", Actor: "user"}, Operation: "set"})
	branch := Entry{ID: "branch-goal", ThreadID: "thread", Type: EntryGoal, Payload: raw}
	before := cloneMigrationRecords(backend.records)
	failure := errors.New("commit failure")
	repo.backend = &domainCommitBoundaryBackend{Backend: backend, afterCallback: func() error { return failure }}
	if _, err := repo.Append(t.Context(), branch, AppendOptions{}); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, backend.records) {
		t.Fatal("rollback changed source metadata")
	}
	repo.backend = backend
	add(branch)
	repo = reopenCurrentStartupRepo(t, backend, false)
	check()
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		refs, err := readGoalSourceContext(tx, "thread", "branch-goal")
		if err != nil {
			return err
		}
		refs.Change.Hash = "drift"
		raw, _ := encodeActivityMetadata("goal-sources-v1", refs)
		return tx.Put(projectionSourcesNamespace, goalSourceKey("thread", "branch-goal"), raw)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.ReadWindowProjection(t.Context(), ItemMembershipWindowOptions{ThreadID: "thread", Limit: 1}, func(PathMetadata, bool) []string { return nil }); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("corruption accepted: %v", err)
	}
}

func TestGoalSourceMetadataRejectsOmittedAndDamagedReferences(t *testing.T) {
	for _, encoded := range []string{"{}", "null", "invalid"} {
		t.Run(encoded, func(t *testing.T) {
			backend, repo := goalSourceFixture(t, 2)
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				return tx.Put(projectionSourcesNamespace, goalSourceKey("thread", "tail"), []byte(encoded))
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := repo.ReadWindowProjection(t.Context(), ItemMembershipWindowOptions{ThreadID: "thread", Limit: 1}, func(PathMetadata, bool) []string { return nil }); !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("damaged Goal source metadata accepted: %v", err)
			}
		})
	}
}

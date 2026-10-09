package sessiontree

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/session/compaction"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
	"github.com/floegence/floret/v7/observation"
)

type compactionFixtureRepo interface {
	Repo
	RuntimeTurnRepo
	ProviderStateStore
}

func compactionCommitFixture(t *testing.T, repo compactionFixtureRepo) ContextCompactionCommit {
	t.Helper()
	ctx := t.Context()
	now := time.Now()
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.AcceptTurn(ctx, AcceptTurnRequest{ThreadID: "thread", TurnID: "seed", RunID: "seed-run", LogicalRequestID: "seed", RequestFingerprint: "seed", InputRequestFingerprint: "seed", Input: session.Message{Role: session.User, Content: strings.Repeat("durable fact ", 500)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Append(ctx, Entry{ThreadID: "thread", TurnID: "seed", RunID: "seed-run", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "Acknowledged."}}, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.FinishTurn(ctx, FinishTurnRequest{ThreadID: "thread", TurnID: "seed", RunID: "seed-run", TerminalEntryID: "seed-done", Status: TurnCompleted, OutcomeFingerprint: "seed-done"}); err != nil {
		t.Fatal(err)
	}
	if err := repo.PutProviderState(ctx, ProviderStateRecord{ThreadID: "thread", LeafEntryID: "seed-done", CompatibilityKey: "test", State: provider.State{Kind: "test", ID: "original"}, CreatedByRunID: "seed-run", CreatedByTurnID: "seed", UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.AcceptTurn(ctx, AcceptTurnRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", LogicalRequestID: "compact", RequestFingerprint: "compact", TurnKind: TurnKindContextCompaction}); err != nil {
		t.Fatal(err)
	}
	policy := contextpolicy.Policy{ContextWindowTokens: 256000, RecentTailTokens: 20, RecentUserTokens: 20}
	policyEntry, err := NewThreadContextPolicyEntry("thread", "turn", "run", "test", "test", policy)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Append(ctx, policyEntry, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	lifecycle := ThreadContextCompaction{ThreadID: "thread", TurnID: "turn", RunID: "run", Step: 1, OperationID: "operation", RequestID: "compact", Source: "test", Trigger: "manual", Reason: "manual", Phase: "start", Status: "running", ObservedAt: now}
	start, err := NewThreadContextCompactionEntry(lifecycle)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Append(ctx, start, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	path, err := repo.Path(ctx, "thread", "")
	if err != nil {
		t.Fatal(err)
	}
	history, err := BuildContextChecked(path, ContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := compaction.Prepare(ctx, compaction.Request{CompactionID: "summary", OperationID: "operation", RequestID: "compact", Source: "test", History: history, Policy: policy, Trigger: compaction.TriggerManual, Reason: compaction.ReasonManual, Phase: compaction.PhaseInstall}, compaction.ExtractiveSummaryGenerator{})
	if err != nil {
		t.Fatal(err)
	}
	summary, err := CompactionEntry("thread", "turn", prepared.Result)
	if err != nil {
		t.Fatal(err)
	}
	summary.RunID = "run"
	lifecycle.Phase, lifecycle.Status = "complete", "compacted"
	complete, err := NewThreadContextCompactionEntry(lifecycle)
	if err != nil {
		t.Fatal(err)
	}
	estimate, err := NewThreadContextStatusEntry(observation.ContextStatus{ThreadID: identity.ThreadID("thread"), TurnID: identity.TurnID("turn"), RunID: identity.RunID("run"), Provider: "test", Model: "test", ObservedAt: now, Phase: observation.ContextPhaseProjectedRequest, Status: observation.ContextStatusEstimated})
	if err != nil {
		t.Fatal(err)
	}
	return ContextCompactionCommit{Summary: summary, Lifecycle: complete, Estimate: estimate, Finish: &FinishTurnRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", TerminalEntryID: "terminal", Status: TurnCompleted, OutcomeFingerprint: "installed", ClearProviderState: true, Now: now}}
}

func TestContextCompactionInstallationAndCancellationHaveOneWinner(t *testing.T) {
	for i := 0; i < 30; i++ {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			repo := NewMemoryRepo()
			install := compactionCommitFixture(t, repo)
			var installErr, cancelErr error
			start := make(chan struct{})
			var workers sync.WaitGroup
			workers.Add(2)
			go func() {
				defer workers.Done()
				<-start
				_, installErr = repo.CommitContextCompaction(t.Context(), install)
			}()
			go func() {
				defer workers.Done()
				<-start
				_, cancelErr = repo.CancelTurn(t.Context(), testCancelTurnRequest(time.Now()))
			}()
			close(start)
			workers.Wait()
			if installErr == nil && cancelErr == nil {
				t.Fatal("installation and cancellation both won")
			}
			entries, err := repo.Entries(t.Context(), "thread")
			if err != nil {
				t.Fatal(err)
			}
			summaries, terminals := 0, 0
			for _, entry := range entries {
				if entry.Type == EntryCompaction {
					summaries++
				}
				if entry.Type == EntryTurnMarker && entry.TurnID == "turn" && entry.TurnStatus != TurnStarted {
					terminals++
				}
			}
			if terminals != 1 {
				t.Fatalf("terminal count=%d", terminals)
			}
			state, stateErr := repo.ProviderState(t.Context(), "thread")
			if installErr == nil {
				if summaries != 1 || !errors.Is(stateErr, ErrProviderStateNotFound) {
					t.Fatal("installation did not atomically invalidate continuation")
				}
			} else {
				if summaries != 0 || stateErr != nil || state.State.ID != "original" {
					t.Fatal("cancellation changed model context")
				}
			}
		})
	}
}

func TestContextCompactionTransactionFailureKeepsMemoryAndDurableFacts(t *testing.T) {
	backend := newMigrationTestBackend()
	injected := errors.New("install write failed")
	failing := &toggleFailBackend{Backend: backend, err: injected}
	repo, err := NewBackendRepo(t.Context(), failing, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	install := compactionCommitFixture(t, repo)
	before := cloneMigrationRecords(backend.records)
	beforeEntries, err := repo.Entries(t.Context(), "thread")
	if err != nil {
		t.Fatal(err)
	}
	failing.setFail(true)
	if _, err := repo.CommitContextCompaction(t.Context(), install); !errors.Is(err, injected) {
		t.Fatalf("failure=%v", err)
	}
	afterEntries, err := repo.Entries(t.Context(), "thread")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(beforeEntries, afterEntries) || !reflect.DeepEqual(before, backend.records) {
		t.Fatal("failed installation published partial state")
	}
	failing.setFail(false)
	if _, err := repo.CommitContextCompaction(context.Background(), install); err != nil {
		t.Fatal(err)
	}
	if _, err := NewBackendRepo(t.Context(), failing, time.Now); err != nil {
		t.Fatal(err)
	}
}

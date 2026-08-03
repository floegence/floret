package agentharness

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/v3/internal/engine"
	"github.com/floegence/floret/v3/internal/provider/cache"
	"github.com/floegence/floret/v3/internal/sessiontree"
	"github.com/floegence/floret/v3/internal/storage"
	scriptharness "github.com/floegence/floret/v3/internal/testing/harness"
)

func TestTerminalSettlementAbsorbsConcurrentStaleRenewal(t *testing.T) {
	repo := &terminalSettlementRaceRepo{
		MemoryRepo:      sessiontree.NewMemoryRepo(),
		finishCommitted: make(chan struct{}),
		releaseFinish:   make(chan struct{}),
		renewalStale:    make(chan struct{}),
	}
	provider := scriptharness.NewScriptedProvider(
		scriptharness.Step(scriptharness.Text("done"), scriptharness.Done()),
	)
	harness := newTestHarness(provider, repo, cache.NewMemoryStore())
	harness.options.TitleGenerator = nil
	harness.options.ForkOperations = storage.NewMemoryForkOperationStore(repo.MemoryRepo)
	thread, err := harness.StartThread(context.Background(), StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}

	type outcome struct {
		result TurnResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, runErr := thread.Run(context.Background(), "finish during renewal", RunOptions{
			TurnID: "turn", RunID: "run",
		})
		done <- outcome{result: result, err: runErr}
	}()

	waitLeaseSettlementSignal(t, repo.finishCommitted, "terminal commit")
	waitLeaseSettlementSignal(t, repo.renewalStale, "stale renewal")
	close(repo.releaseFinish)

	select {
	case got := <-done:
		if got.err != nil || got.result.Status != engine.Completed {
			t.Fatalf("Run result = %#v, err = %v, want completed settlement", got.result, got.err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("turn did not return after terminal settlement")
	}
}

func waitLeaseSettlementSignal(t *testing.T, signal <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

type terminalSettlementRaceRepo struct {
	*sessiontree.MemoryRepo
	finishCommitted chan struct{}
	releaseFinish   chan struct{}
	renewalStale    chan struct{}
	finishOnce      sync.Once
	renewOnce       sync.Once
}

func (*terminalSettlementRaceRepo) AuthorityLeasePolicy() sessiontree.LeasePolicy {
	return sessiontree.LeasePolicy{
		TTL: 30 * time.Millisecond, RenewInterval: time.Millisecond,
	}
}

func (r *terminalSettlementRaceRepo) FinishTurn(ctx context.Context, req sessiontree.FinishTurnRequest) (sessiontree.FinishTurnResult, error) {
	result, err := r.MemoryRepo.FinishTurn(ctx, req)
	if err != nil {
		return result, err
	}
	r.finishOnce.Do(func() { close(r.finishCommitted) })
	<-r.releaseFinish
	return result, nil
}

func (r *terminalSettlementRaceRepo) RenewTurnLease(context.Context, sessiontree.TurnLease) (sessiontree.TurnLease, error) {
	<-r.finishCommitted
	r.renewOnce.Do(func() { close(r.renewalStale) })
	return sessiontree.TurnLease{}, sessiontree.ErrStaleAuthority
}

var _ sessiontree.TurnAuthorityRepo = (*terminalSettlementRaceRepo)(nil)
var _ sessiontree.TurnLeaseRepo = (*terminalSettlementRaceRepo)(nil)

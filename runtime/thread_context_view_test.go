package runtime

import (
	"context"
	"errors"
	"fmt"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
	"github.com/floegence/floret/v7/internal/sessiontree"
	internalstorage "github.com/floegence/floret/v7/internal/storage"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/provider"
)

func TestThreadViewPublishesCompactionLifecycleWhileActive(t *testing.T) {
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "compact", StateCompatibilityKey: "test:compact"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		compactionTestStep("ack"), compactionTestStep("ack"), compactionTestStep("ack"), florettest.Step{BlockUntil: release, Events: compactionTestStep("A complete summary preserving CERULEAN-739 and /work/first.").Events})
	_, service := testThreadServiceWithAgent(t, compactionTestAgent(t, gateway))
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if created.Context == nil || len(created.Context.Compactions) != 0 {
		t.Fatalf("fresh context=%+v", created.Context)
	}
	before := seedCompactionHistory(t, service, created.ThreadID)
	admitted, err := service.(ThreadContextCompactor).CompactContext(t.Context(), CompactContextInput{ThreadID: created.ThreadID, RequestKey: "compact"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if err := gateway.WaitForRequests(ctx, 4); err != nil {
		t.Fatal(err)
	}
	active, err := service.View(ctx, created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	if active.Activity != ThreadActivityActive || active.Context == nil || len(active.Context.Compactions) != 1 {
		t.Fatalf("active context=%+v", active)
	}
	start := active.Context.Compactions[0]
	if start.TurnID != admitted.TurnID || start.RunID != admitted.RunID || start.CanonicalOrdinal == 0 || start.Phase != "start" || start.AfterItemID != before.Items[len(before.Items)-1].ID {
		t.Fatalf("start=%+v", start)
	}
	active.Context.Compactions[0].OperationID = "corrupt"
	shared, err := service.(ThreadContextReader).Context(ctx, created.ThreadID)
	if err != nil || shared.Compactions[0].OperationID != start.OperationID {
		t.Fatalf("shared=%+v err=%v", shared, err)
	}
	unblock()
	terminal := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if terminal.Context == nil || len(terminal.Context.Compactions) != 1 {
		t.Fatalf("terminal context=%+v", terminal.Context)
	}
	done := terminal.Context.Compactions[0]
	if done.OperationID != start.OperationID || done.CanonicalOrdinal != start.CanonicalOrdinal || done.AfterItemID != start.AfterItemID || done.Status != "compacted" {
		t.Fatalf("done=%+v start=%+v", done, start)
	}
}

func TestThreadViewKeepsCompactionCompleteVisibleBeforeTurnSettlement(t *testing.T) {
	releaseSummary, releaseAnswer := make(chan struct{}), make(chan struct{})
	finishSummary := sync.OnceFunc(func() { close(releaseSummary) })
	finishAnswer := sync.OnceFunc(func() { close(releaseAnswer) })
	t.Cleanup(finishSummary)
	t.Cleanup(finishAnswer)
	source := &controlledViewCompaction{}
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "compact", StateCompatibilityKey: "test:compact"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		compactionTestStep("ack"), compactionTestStep("ack"), compactionTestStep("ack"),
		florettest.Step{BlockUntil: releaseSummary, Events: compactionTestStep("Preserve CERULEAN-739 and /work/first; historical filler acknowledged.").Events},
		florettest.Step{BlockUntil: releaseAnswer, Events: compactionTestStep("Done.").Events})
	agent := compactionTestAgent(t, gateway)
	agent.manualCompactions = source
	_, service := testThreadServiceWithAgent(t, agent)
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	seedCompactionHistory(t, service, created.ThreadID)
	subscription, err := service.Subscribe(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	source.pending.Store(true)
	admitted, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "follow", Input: UserInput{Text: "continue"}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var start ThreadContextCompaction
	for {
		view, err := subscription.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if view.Context == nil || len(view.Context.Compactions) == 0 {
			continue
		}
		start = view.Context.Compactions[0]
		if start.Phase != "start" || view.Activity != ThreadActivityActive || start.TurnID != admitted.TurnID {
			t.Fatalf("start=%+v", view)
		}
		break
	}
	finishSummary()
	if err := gateway.WaitForRequests(ctx, 5); err != nil {
		t.Fatal(err)
	}
	for {
		view, err := subscription.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if view.Context == nil || len(view.Context.Compactions) == 0 || view.Context.Compactions[0].Phase != "complete" {
			continue
		}
		done := view.Context.Compactions[0]
		if view.Activity != ThreadActivityActive || done.CanonicalOrdinal != start.CanonicalOrdinal || done.AfterItemID != start.AfterItemID || done.OperationID != start.OperationID || view.Context.ContextUsage == nil || view.Context.ContextUsage.Confirmed != nil {
			t.Fatalf("complete=%+v", view)
		}
		break
	}
	finishAnswer()
	waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
}

type controlledViewCompaction struct{ pending atomic.Bool }

func (source *controlledViewCompaction) PollManualCompaction(context.Context, ManualCompactionPollRequest) (ManualCompactionRequest, bool, error) {
	if !source.pending.CompareAndSwap(true, false) {
		return ManualCompactionRequest{}, false, nil
	}
	return ManualCompactionRequest{RequestID: "view-compaction", Source: "test"}, true, nil
}

type failingContextPathRepo struct {
	sessiontree.Repo
	fail bool
}

func (repo *failingContextPathRepo) Path(ctx context.Context, threadID, leafID string) ([]sessiontree.Entry, error) {
	if repo.fail {
		return nil, errors.New("context read unavailable")
	}
	return repo.Repo.Path(ctx, threadID, leafID)
}

func TestThreadViewRecoversFailedContextProjectionOnExplicitRead(t *testing.T) {
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "compact", StateCompatibilityKey: "test:compact"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported})
	host, service := testThreadServiceWithAgent(t, compactionTestAgent(t, gateway))
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	typed := service.(*threadRuntimeService)
	actor := typed.runtime(created.ThreadID)
	repo := &failingContextPathRepo{Repo: host.store.repo, fail: true}
	original := host.store.repo
	host.store.repo = repo
	defer func() { host.store.repo = original }()
	actor.state.contextErr = errors.New("previous projection failed")
	if _, err := service.View(t.Context(), created.ThreadID); err == nil {
		t.Fatal("failed projection returned success")
	}
	repo.fail = false
	recovered, err := service.View(t.Context(), created.ThreadID)
	if err != nil || recovered.Context == nil || recovered.ViewVersion <= created.ViewVersion || actor.state.contextErr != nil {
		t.Fatalf("recovery=%+v err=%v", recovered, err)
	}
}

func TestThreadContextCompactionOrderComesFromFirstCanonicalEntry(t *testing.T) {
	host, _ := testThreadService(t, florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "compact", StateCompatibilityKey: "test"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}))
	service := &threadRuntimeService{host: host}
	policy, err := sessiontree.NewThreadContextPolicyEntry("thread-order", "turn-order", "run-order", "test", "compact", contextpolicy.Policy{})
	if err != nil {
		t.Fatal(err)
	}
	makeEntry := func(operation, phase, status string, at int64) sessiontree.Entry {
		entry, err := sessiontree.NewThreadContextCompactionEntry(sessiontree.ThreadContextCompaction{ThreadID: "thread-order", TurnID: "turn-order", RunID: "run-order", OperationID: operation, RequestID: operation, Phase: phase, Status: status, Trigger: "manual", ObservedAt: time.Unix(at, 0)})
		if err != nil {
			t.Fatal(err)
		}
		return entry
	}
	entries := []sessiontree.Entry{policy, makeEntry("first", "start", "running", 20), makeEntry("second", "start", "running", 10), makeEntry("second", "failed", "failed", 9), makeEntry("first", "cancelled", "cancelled", 8)}
	for i := range entries {
		entries[i].ID = fmt.Sprintf("entry-%d", i)
		entries[i].PathDepth = int64(i + 1)
	}
	snapshot, err := service.projectThreadContext(t.Context(), "thread-order", entries)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Compactions) != 2 || snapshot.Compactions[0].OperationID != "first" || snapshot.Compactions[0].CanonicalOrdinal != 2 || snapshot.Compactions[1].OperationID != "second" || snapshot.Compactions[1].CanonicalOrdinal != 3 {
		t.Fatalf("order=%+v", snapshot.Compactions)
	}
	for i := range entries {
		entries[i].ThreadID = "thread-fork"
	}
	forked, err := service.projectThreadContext(t.Context(), "thread-fork", entries)
	if err != nil || len(forked.Compactions) != 2 || forked.Compactions[0].ThreadID != "thread-fork" || forked.Compactions[0].TurnID != "turn-order" || forked.Compactions[0].RunID != "run-order" || forked.Compactions[0].CanonicalOrdinal != 2 {
		t.Fatalf("fork=%+v err=%v", forked, err)
	}
}

type unavailableViewRepo struct{ *internalstorage.BackendKernel }

func (repo unavailableViewRepo) Path(context.Context, string, string) ([]sessiontree.Entry, error) {
	return nil, errors.New("display read unavailable")
}

func TestThreadStopDoesNotDependOnContextProjection(t *testing.T) {
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "compact", StateCompatibilityKey: "test"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, florettest.Step{WaitForCancellation: true})
	host, service := testThreadService(t, gateway)
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "send", Input: UserInput{Text: "wait"}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if err := gateway.WaitForRequests(ctx, 1); err != nil {
		t.Fatal(err)
	}
	typed := service.(*threadRuntimeService)
	actor := typed.runtime(created.ThreadID)
	original := host.store.repo
	// The provider is blocked and no writer can run until Stop is admitted.
	actor.mu.Lock()
	host.store.repo = unavailableViewRepo{original.(*internalstorage.BackendKernel)}
	actor.state.contextErr = errors.New("display unavailable")
	actor.mu.Unlock()
	stopped, err := service.Cancel(ctx, CancelInput{ThreadID: created.ThreadID, RequestKey: "stop"})
	if err != nil || stopped.Activity != ThreadActivityIdle || stopped.LastOutcome == nil || *stopped.LastOutcome != TurnOutcomeCancelled {
		t.Fatalf("stop=%+v err=%v", stopped, err)
	}
	waitThreadExecutionDone(t, typed, created.ThreadID)
	host.store.repo = original
	recovered, err := service.View(ctx, created.ThreadID)
	if err != nil || recovered.Context == nil || recovered.LastOutcome == nil || *recovered.LastOutcome != TurnOutcomeCancelled {
		t.Fatalf("recovery=%+v err=%v", recovered, err)
	}
}

package runtime_test

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/v3/config"
	"github.com/floegence/floret/v3/florettest"
	"github.com/floegence/floret/v3/identity"
	"github.com/floegence/floret/v3/provider"
	"github.com/floegence/floret/v3/runtime"
	"github.com/floegence/floret/v3/storage"
)

type blockingAdmissionSink struct {
	threadID identity.ThreadID
	entered  chan struct{}
	release  chan struct{}
	once     sync.Once
}

func TestTwentyFourThreadAdmissionsStayIndependentAndUnderBudget(t *testing.T) {
	ctx := context.Background()
	const threadCount = 24
	ids := &deterministicIDs{}
	for index := 0; index < threadCount; index++ {
		ids.threads = append(ids.threads, identity.ThreadID(fmt.Sprintf("thread-concurrent-%02d", index)))
		ids.turns = append(ids.turns, identity.TurnID(fmt.Sprintf("turn-concurrent-%02d", index)))
		ids.runs = append(ids.runs, identity.RunID(fmt.Sprintf("run-concurrent-%02d", index)))
	}
	host, err := runtime.Open(ctx, runtime.Options{Storage: storage.Memory(), IDSource: ids})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = host.Shutdown(context.Background()) }()
	agent, err := runtime.NewAgent(config.AgentConfig{
		Profile: config.AgentProfile{ID: "assistant", Name: "Assistant"}, SystemPrompt: "Be concise.",
		Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}, florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "model", StateCompatibilityKey: "test:model:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
	))
	if err != nil {
		t.Fatal(err)
	}
	executors := make([]runtime.TurnExecutor, 0, threadCount)
	for index := 0; index < threadCount; index++ {
		created, createErr := host.Threads().CreateThread(ctx, runtime.CreateThreadCommand{
			LogicalRequestID: identity.LogicalRequestID(fmt.Sprintf("create-concurrent-%02d", index)),
		})
		if createErr != nil {
			t.Fatal(createErr)
		}
		thread, threadErr := host.Thread(ctx, created.ThreadID)
		if threadErr != nil {
			t.Fatal(threadErr)
		}
		executor, executorErr := thread.TurnExecutor(agent)
		if executorErr != nil {
			t.Fatal(executorErr)
		}
		executors = append(executors, executor)
	}

	start := make(chan struct{})
	durations := make(chan time.Duration, threadCount)
	errorsByThread := make(chan error, threadCount)
	var wait sync.WaitGroup
	wait.Add(threadCount)
	for index, executor := range executors {
		index, executor := index, executor
		go func() {
			defer wait.Done()
			<-start
			started := time.Now()
			_, admitErr := executor.AdmitTurn(ctx, runtime.StartTurnCommand{
				LogicalRequestID: identity.LogicalRequestID(fmt.Sprintf("admit-concurrent-%02d", index)),
				UserMessage:      runtime.TurnInput{Text: fmt.Sprintf("message %d", index)},
			})
			durations <- time.Since(started)
			errorsByThread <- admitErr
		}()
	}
	close(start)
	wait.Wait()
	close(durations)
	close(errorsByThread)
	for admitErr := range errorsByThread {
		if admitErr != nil {
			t.Fatal(admitErr)
		}
	}
	observed := make([]time.Duration, 0, threadCount)
	for duration := range durations {
		observed = append(observed, duration)
	}
	sort.Slice(observed, func(left, right int) bool { return observed[left] < observed[right] })
	p95 := observed[(len(observed)*95+99)/100-1]
	if p95 > 50*time.Millisecond {
		t.Fatalf("concurrent memory admission p95=%s, want <=50ms (all=%v)", p95, observed)
	}
}

func (sink *blockingAdmissionSink) EmitEvent(event runtime.Event) {
	if event.ThreadID != sink.threadID || event.Committed == nil || event.Committed.Kind != runtime.ThreadDetailEventUserMessage {
		return
	}
	sink.once.Do(func() {
		close(sink.entered)
		<-sink.release
	})
}

func TestIndependentThreadAdmissionDoesNotWaitForAnotherThreadObserver(t *testing.T) {
	ctx := context.Background()
	ids := &deterministicIDs{
		threads: []identity.ThreadID{"thread-admission-a", "thread-admission-b"},
		turns:   []identity.TurnID{"turn-admission-a", "turn-admission-b"},
		runs:    []identity.RunID{"run-admission-a", "run-admission-b"},
	}
	host, err := runtime.Open(ctx, runtime.Options{Storage: storage.Memory(), IDSource: ids})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = host.Shutdown(context.Background()) }()

	createdA, err := host.Threads().CreateThread(ctx, runtime.CreateThreadCommand{LogicalRequestID: "create-admission-a"})
	if err != nil {
		t.Fatal(err)
	}
	createdB, err := host.Threads().CreateThread(ctx, runtime.CreateThreadCommand{LogicalRequestID: "create-admission-b"})
	if err != nil {
		t.Fatal(err)
	}
	sink := &blockingAdmissionSink{threadID: createdA.ThreadID, entered: make(chan struct{}), release: make(chan struct{})}
	agent, err := runtime.NewAgent(config.AgentConfig{
		Profile: config.AgentProfile{ID: "assistant", Name: "Assistant"}, SystemPrompt: "Be concise.",
		Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}, florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "model", StateCompatibilityKey: "test:model:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
	), runtime.WithAgentEventSink(sink))
	if err != nil {
		t.Fatal(err)
	}

	turnsFor := func(threadID identity.ThreadID) runtime.TurnExecutor {
		thread, threadErr := host.Thread(ctx, threadID)
		if threadErr != nil {
			t.Fatal(threadErr)
		}
		turns, turnsErr := thread.TurnExecutor(agent)
		if turnsErr != nil {
			t.Fatal(turnsErr)
		}
		return turns
	}
	turnsA := turnsFor(createdA.ThreadID)
	turnsB := turnsFor(createdB.ThreadID)

	admittedA := make(chan error, 1)
	go func() {
		_, admitErr := turnsA.AdmitTurn(ctx, runtime.StartTurnCommand{
			LogicalRequestID: "admit-a", UserMessage: runtime.TurnInput{Text: "first"},
		})
		admittedA <- admitErr
	}()
	select {
	case <-sink.entered:
	case <-time.After(time.Second):
		t.Fatal("thread A admission did not reach its observer")
	}

	admittedB := make(chan error, 1)
	go func() {
		_, admitErr := turnsB.AdmitTurn(ctx, runtime.StartTurnCommand{
			LogicalRequestID: "admit-b", UserMessage: runtime.TurnInput{Text: "second"},
		})
		admittedB <- admitErr
	}()
	select {
	case err := <-admittedB:
		if err != nil {
			close(sink.release)
			<-admittedA
			t.Fatal(err)
		}
	case <-time.After(200 * time.Millisecond):
		close(sink.release)
		<-admittedA
		t.Fatal("thread B admission waited for thread A observer")
	}

	close(sink.release)
	if err := <-admittedA; err != nil {
		t.Fatal(err)
	}
}

package runtime_test

import (
	"context"
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

package runtime_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/floegence/floret/v3/config"
	"github.com/floegence/floret/v3/florettest"
	"github.com/floegence/floret/v3/identity"
	"github.com/floegence/floret/v3/provider"
	"github.com/floegence/floret/v3/runtime"
	"github.com/floegence/floret/v3/storage"
	"github.com/floegence/floret/v3/tools"
)

func TestThreadCapabilityViewsExposeNarrowNativeContracts(t *testing.T) {
	ctx := context.Background()
	host, err := runtime.Open(ctx, runtime.Options{
		Storage:  storage.Memory(),
		IDSource: &deterministicIDs{threads: []identity.ThreadID{"thread-capabilities"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = host.Shutdown(context.Background()) }()

	created, err := host.Threads().CreateThread(ctx, runtime.CreateThreadCommand{LogicalRequestID: "create-capabilities"})
	if err != nil {
		t.Fatal(err)
	}
	thread, err := host.Thread(ctx, created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := thread.Reader()
	if err != nil {
		t.Fatal(err)
	}
	var _ runtime.ThreadReader = reader
	if reader.ID() != created.ThreadID {
		t.Fatalf("reader id = %q", reader.ID())
	}

	bootstrap, err := reader.Bootstrap(ctx, runtime.ThreadBootstrapRequest{TurnLimit: 20})
	if err != nil {
		t.Fatal(err)
	}
	if bootstrap.Revision != created.Receipt.Revision || bootstrap.Thread.ID != created.ThreadID ||
		bootstrap.Turns.ThreadID != created.ThreadID || bootstrap.AgentTodos.ThreadID != created.ThreadID ||
		bootstrap.Context.ThreadID != created.ThreadID || bootstrap.Approvals.RootThreadID != created.ThreadID {
		t.Fatalf("bootstrap = %#v", bootstrap)
	}
	if err := bootstrap.Validate(); err != nil {
		t.Fatal(err)
	}

	subscription, err := reader.Subscribe(ctx, runtime.SubscribeOptions{AfterRevision: bootstrap.Revision})
	if err != nil {
		t.Fatal(err)
	}
	if err := subscription.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestThreadReaderBootstrapKeepsOneSnapshotDuringConcurrentMutationsAndHandsOffToSubscription(t *testing.T) {
	ctx := context.Background()
	host, err := runtime.Open(ctx, runtime.Options{
		Storage:  storage.Memory(),
		IDSource: &deterministicIDs{threads: []identity.ThreadID{"thread-bootstrap-concurrent"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = host.Shutdown(context.Background()) }()
	created, err := host.Threads().CreateThread(ctx, runtime.CreateThreadCommand{LogicalRequestID: "create-bootstrap-concurrent"})
	if err != nil {
		t.Fatal(err)
	}
	thread, err := host.Thread(ctx, created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := thread.Reader()
	if err != nil {
		t.Fatal(err)
	}
	lifecycle, err := thread.Lifecycle()
	if err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	mutationErr := make(chan error, 1)
	var startedOnce sync.Once
	go func() {
		for index := 0; index < 100; index++ {
			_, err := lifecycle.SetTitle(ctx, runtime.SetThreadTitleCommand{
				LogicalRequestID: identity.LogicalRequestID(fmt.Sprintf("bootstrap-title-%03d", index)),
				Title:            fmt.Sprintf("Title %03d", index),
			})
			if err != nil {
				mutationErr <- err
				return
			}
			startedOnce.Do(func() { close(started) })
		}
		mutationErr <- nil
	}()
	<-started
	for index := 0; index < 25; index++ {
		bootstrap, err := reader.Bootstrap(ctx, runtime.ThreadBootstrapRequest{TurnLimit: 20})
		if err != nil {
			t.Fatal(err)
		}
		if err := bootstrap.Validate(); err != nil {
			t.Fatalf("bootstrap %d: %v", index, err)
		}
	}
	if err := <-mutationErr; err != nil {
		t.Fatal(err)
	}

	bootstrap, err := reader.Bootstrap(ctx, runtime.ThreadBootstrapRequest{TurnLimit: 20})
	if err != nil {
		t.Fatal(err)
	}
	subscription, err := reader.Subscribe(ctx, runtime.SubscribeOptions{AfterRevision: bootstrap.Revision})
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	updated, err := lifecycle.SetTitle(ctx, runtime.SetThreadTitleCommand{
		LogicalRequestID: "bootstrap-title-after-subscribe", Title: "After subscribe",
	})
	if err != nil {
		t.Fatal(err)
	}
	message, err := subscription.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	durable, ok := message.Durable()
	if !ok || durable.Revision() != updated.Receipt.Revision || durable.Revision() <= bootstrap.Revision {
		t.Fatalf("subscription handoff = %#v, bootstrap revision = %d, update revision = %d", message, bootstrap.Revision, updated.Receipt.Revision)
	}
}

func TestExecuteAdmissionLoadsCanonicalPlanAfterRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "floret.db")
	host, err := runtime.Open(ctx, runtime.Options{
		Storage: storage.SQLite(path),
		IDSource: &deterministicIDs{
			threads: []identity.ThreadID{"thread-plan"},
			turns:   []identity.TurnID{"turn-plan"},
			runs:    []identity.RunID{"run-plan"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	created, err := host.Threads().CreateThread(ctx, runtime.CreateThreadCommand{LogicalRequestID: "create-plan"})
	if err != nil {
		t.Fatal(err)
	}
	thread, err := host.Thread(ctx, created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	agent := boundaryTestAgent(t, florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "model", StateCompatibilityKey: "test:model:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported, AttachmentPayload: provider.AttachmentDescriptors},
	))
	executor, err := thread.TurnExecutor(agent)
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := executor.AdmitTurn(ctx, runtime.StartTurnCommand{
		LogicalRequestID: "admit-plan",
		UserMessage:      runtime.TurnInput{Text: "durable canonical input"},
		SupplementalContext: []runtime.TurnSupplementalContextItem{{
			Kind: "host_context", Title: "ephemeral-before-restart", Text: "must not enter the durable fingerprint",
		}},
		Labels: runtime.RunLabels{
			Correlation: map[string]string{"request": "canonical"},
		},
		Signals: runtime.TurnSignalSpec{
			Definitions: []tools.ToolDefinition{{
				Name: "host_wait", Title: "Wait", Description: "Wait for host input.",
				InputSchema: map[string]any{"type": "object", "additionalProperties": false}, Strict: true,
			}},
			Identity: "host-signals:v1",
			Project: func(tools.ToolCall) (runtime.TurnSignal, bool, error) {
				return runtime.TurnSignal{}, false, nil
			},
		},
		Limits: runtime.TurnLimits{MaxToolCalls: 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	databaseBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(databaseBytes, []byte("must not enter the durable fingerprint")) {
		t.Fatal("ephemeral supplemental context was persisted by admission")
	}

	restarted, err := runtime.Open(ctx, runtime.Options{Storage: storage.SQLite(path), IDSource: &deterministicIDs{}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = restarted.Shutdown(context.Background()) }()
	restartedThread, err := restarted.Thread(ctx, created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	gateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "model", StateCompatibilityKey: "test:model:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported, AttachmentPayload: provider.AttachmentDescriptors},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "done"}, {Type: provider.EventDone}}},
	)
	restartedExecutor, err := restartedThread.TurnExecutor(boundaryTestAgent(t, gateway))
	if err != nil {
		t.Fatal(err)
	}
	replayedAdmission, err := restartedExecutor.AdmitTurn(ctx, runtime.StartTurnCommand{
		LogicalRequestID: "admit-plan",
		UserMessage:      runtime.TurnInput{Text: "durable canonical input"},
		SupplementalContext: []runtime.TurnSupplementalContextItem{{
			Kind: "host_context", Title: "ephemeral-after-restart", Text: "different current host context",
		}},
		Labels: runtime.RunLabels{
			Correlation: map[string]string{"request": "canonical"},
		},
		Signals: runtime.TurnSignalSpec{
			Definitions: []tools.ToolDefinition{{
				Name: "host_wait", Title: "Wait", Description: "Wait for host input.",
				InputSchema: map[string]any{"type": "object", "additionalProperties": false}, Strict: true,
			}},
			Identity: "host-signals:v1",
			Project: func(tools.ToolCall) (runtime.TurnSignal, bool, error) {
				return runtime.TurnSignal{}, false, nil
			},
		},
		Limits: runtime.TurnLimits{MaxToolCalls: 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !replayedAdmission.Receipt.Replayed || replayedAdmission.TurnID != admitted.TurnID {
		t.Fatalf("replayed admission = %#v, admitted = %#v", replayedAdmission, admitted)
	}
	if _, err := restartedExecutor.ExecuteAdmission(ctx, admitted.Receipt, runtime.ExecutionContext{}); !errors.Is(err, runtime.ErrExecutionContextIncomplete) {
		t.Fatalf("missing signal projector error = %v, want ErrExecutionContextIncomplete", err)
	}
	executed, err := restartedExecutor.ExecuteAdmission(ctx, admitted.Receipt, runtime.ExecutionContext{
		SupplementalContext: []runtime.TurnSupplementalContextItem{{
			Kind: "host_context", Title: "ephemeral-execution", Text: "current execution only",
		}},
		SignalProjector: func(tools.ToolCall) (runtime.TurnSignal, bool, error) {
			return runtime.TurnSignal{}, false, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if executed.TurnID != admitted.TurnID || executed.RunID != admitted.RunID {
		t.Fatalf("executed = %#v, admitted = %#v", executed, admitted)
	}
	requests := gateway.Requests()
	if len(requests) != 1 || len(requests[0].Messages) == 0 {
		t.Fatalf("provider requests = %#v", requests)
	}
	foundCanonicalInput := false
	for _, message := range requests[0].Messages {
		if message.Role == provider.RoleUser && message.Text == "durable canonical input" {
			foundCanonicalInput = true
			break
		}
	}
	if !foundCanonicalInput {
		t.Fatalf("provider did not receive canonical admission input: %#v", requests[0].Messages)
	}

	differentGateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "different", StateCompatibilityKey: "test:different:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported, AttachmentPayload: provider.AttachmentDescriptors},
	)
	differentExecutor, err := restartedThread.TurnExecutor(boundaryTestAgent(t, differentGateway))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := differentExecutor.ExecuteAdmission(ctx, admitted.Receipt, runtime.ExecutionContext{}); !errors.Is(err, runtime.ErrExecutionPlanMismatch) {
		t.Fatalf("different Agent execute error = %v, want ErrExecutionPlanMismatch", err)
	}
}

func boundaryTestAgent(t *testing.T, gateway provider.Gateway) *runtime.Agent {
	t.Helper()
	agent, err := runtime.NewAgent(config.AgentConfig{
		Profile:      config.AgentProfile{ID: "assistant", Name: "Assistant"},
		SystemPrompt: "Be concise.",
		Context:      config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}, gateway)
	if err != nil {
		t.Fatal(err)
	}
	return agent
}

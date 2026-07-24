package florettest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/config"
	"github.com/floegence/floret/runtime"
)

// ModelGatewayFactory creates a gateway that implements the supplied steps.
// Adapter authors can translate the same steps into a fake wire transport and
// use RunModelGatewayContract against their production ModelGateway adapter.
type ModelGatewayFactory func(testing.TB, []ModelStep) runtime.ModelGateway

// ScriptedModelGatewayFactory is the default in-process contract factory.
func ScriptedModelGatewayFactory(_ testing.TB, steps []ModelStep) runtime.ModelGateway {
	return NewScriptedModelGateway(steps...)
}

// RunModelGatewayContract runs the consumer-visible baseline expected from a
// ModelGateway and its host adapter.
func RunModelGatewayContract(t *testing.T, factory ModelGatewayFactory) {
	t.Helper()
	if factory == nil {
		t.Fatal("florettest: model gateway factory is required")
	}
	t.Run("completed terminal", func(t *testing.T) {
		fixture := newContractGateway(t, factory, []ModelStep{{Events: []runtime.ModelEvent{
			{Type: runtime.ModelEventDelta, Text: "contract complete"},
			{Type: runtime.ModelEventDone, Reason: "stop"},
		}}})
		host := newContractTurnHost(t, fixture)
		result, err := runContractTurn(context.Background(), host, 1)
		if err != nil {
			t.Fatalf("RunTurn: %v", err)
		}
		if result.Status != runtime.TurnStatusCompleted || result.Output != "contract complete" {
			t.Fatalf("completed result = %#v", result)
		}
	})

	t.Run("provider error", func(t *testing.T) {
		providerErr := errors.New("contract provider error")
		fixture := newContractGateway(t, factory, []ModelStep{{ReturnError: providerErr}})
		host := newContractTurnHost(t, fixture)
		result, err := runContractTurn(context.Background(), host, 1)
		if !errors.Is(err, providerErr) {
			t.Fatalf("RunTurn error = %v, want provider error", err)
		}
		if result.Status != runtime.TurnStatusFailed {
			t.Fatalf("provider error status = %q, want %q", result.Status, runtime.TurnStatusFailed)
		}
	})

	t.Run("unknown event", func(t *testing.T) {
		fixture := newContractGateway(t, factory, []ModelStep{{Events: []runtime.ModelEvent{
			{Type: runtime.ModelEventType("contract_unknown")},
			{Type: runtime.ModelEventDone, Reason: "stop"},
		}}})
		host := newContractTurnHost(t, fixture)
		result, err := runContractTurn(context.Background(), host, 1)
		if err == nil || !strings.Contains(err.Error(), "unknown provider event type") {
			t.Fatalf("RunTurn error = %v, want unknown event contract failure", err)
		}
		if result.Status != runtime.TurnStatusFailed {
			t.Fatalf("unknown event status = %q, want %q", result.Status, runtime.TurnStatusFailed)
		}
	})

	t.Run("missing terminal", func(t *testing.T) {
		fixture := newContractGateway(t, factory, []ModelStep{{Events: []runtime.ModelEvent{
			{Type: runtime.ModelEventDelta, Text: "unterminated"},
		}}})
		host := newContractTurnHost(t, fixture)
		result, err := runContractTurn(context.Background(), host, 1)
		if err == nil || !strings.Contains(err.Error(), "without terminal event") {
			t.Fatalf("RunTurn error = %v, want missing terminal failure", err)
		}
		if result.Status != runtime.TurnStatusFailed {
			t.Fatalf("missing terminal status = %q, want %q", result.Status, runtime.TurnStatusFailed)
		}
	})

	t.Run("event after terminal", func(t *testing.T) {
		fixture := newContractGateway(t, factory, []ModelStep{{Events: []runtime.ModelEvent{
			{Type: runtime.ModelEventDone, Reason: "stop"},
			{Type: runtime.ModelEventDelta, Text: "too late"},
		}}})
		host := newContractTurnHost(t, fixture)
		result, err := runContractTurn(context.Background(), host, 1)
		if err == nil || !strings.Contains(err.Error(), "after terminal event") {
			t.Fatalf("RunTurn error = %v, want post-terminal event failure", err)
		}
		if result.Status != runtime.TurnStatusFailed {
			t.Fatalf("post-terminal event status = %q, want %q", result.Status, runtime.TurnStatusFailed)
		}
	})

	t.Run("cancellation", func(t *testing.T) {
		fixture := newContractGateway(t, factory, []ModelStep{{WaitForCancellation: true}})
		host := newContractTurnHost(t, fixture)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan contractTurnOutcome, 1)
		go func() {
			result, err := runContractTurn(ctx, host, 1)
			done <- contractTurnOutcome{result: result, err: err}
		}()
		waitCtx, stopWaiting := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopWaiting()
		if err := fixture.waitForRequests(waitCtx, 1); err != nil {
			cancel()
			t.Fatalf("wait for gateway request: %v", err)
		}
		cancel()
		select {
		case outcome := <-done:
			if !errors.Is(outcome.err, context.Canceled) {
				t.Fatalf("RunTurn error = %v, want context cancellation", outcome.err)
			}
			if outcome.result.Status != runtime.TurnStatusCancelled {
				t.Fatalf("cancelled status = %q, want %q", outcome.result.Status, runtime.TurnStatusCancelled)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("RunTurn did not observe cancellation")
		}
	})

	t.Run("continuation state", func(t *testing.T) {
		firstState := &runtime.ModelState{Kind: "contract", ID: "state-1", Attributes: map[string]string{"cursor": "one"}}
		fixture := newContractGateway(t, factory, []ModelStep{
			{Events: []runtime.ModelEvent{
				{Type: runtime.ModelEventDelta, Text: "first"},
				{Type: runtime.ModelEventDone, Reason: "stop", ResponseState: firstState},
			}},
			{Events: []runtime.ModelEvent{
				{Type: runtime.ModelEventDelta, Text: "second"},
				{Type: runtime.ModelEventDone, Reason: "stop", ResponseState: &runtime.ModelState{Kind: "contract", ID: "state-2"}},
			}},
		})
		host := newContractTurnHost(t, fixture)
		if result, err := runContractTurn(context.Background(), host, 1); err != nil || result.Status != runtime.TurnStatusCompleted {
			t.Fatalf("first RunTurn result=%#v err=%v", result, err)
		}
		if result, err := runContractTurn(context.Background(), host, 2); err != nil || result.Status != runtime.TurnStatusCompleted {
			t.Fatalf("second RunTurn result=%#v err=%v", result, err)
		}
		requests := fixture.requestsSnapshot()
		if len(requests) != 2 {
			t.Fatalf("gateway requests = %d, want 2", len(requests))
		}
		if requests[0].PreviousState != nil {
			t.Fatalf("first request previous state = %#v, want nil", requests[0].PreviousState)
		}
		if state := requests[1].PreviousState; state == nil || state.Kind != firstState.Kind || state.ID != firstState.ID || state.Attributes["cursor"] != "one" {
			t.Fatalf("second request previous state = %#v", state)
		}
	})

	t.Run("capability mismatch", func(t *testing.T) {
		fixture := newContractGateway(t, factory, []ModelStep{{Events: []runtime.ModelEvent{{Type: runtime.ModelEventDone, Reason: "stop"}}}})
		turnFactory := newContractTurnFactory(t)
		_, err := turnFactory.NewHost(context.Background(), runtime.TurnExecutionHostOptions{
			Config: config.Config{
				SystemPrompt:  "Exercise a missing capability declaration.",
				ContextPolicy: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
			},
			ModelGateway: fixture,
			ModelGatewayIdentity: runtime.ModelGatewayIdentity{
				Provider: "florettest", Model: "contract-model", StateCompatibilityKey: "florettest:contract-model",
			},
		})
		if err == nil || !strings.Contains(err.Error(), "reasoning capability is required") {
			t.Fatalf("NewHost capability mismatch error=%v", err)
		}
	})
}

type contractGateway struct {
	next     runtime.ModelGateway
	mu       sync.Mutex
	requests []runtime.ModelRequest
	changed  chan struct{}
}

func newContractGateway(t testing.TB, factory ModelGatewayFactory, steps []ModelStep) *contractGateway {
	t.Helper()
	next := factory(t, cloneModelSteps(steps))
	if next == nil {
		t.Fatal("florettest: model gateway factory returned nil")
	}
	return &contractGateway{next: next, changed: make(chan struct{})}
}

func (g *contractGateway) StreamModel(ctx context.Context, request runtime.ModelRequest) (<-chan runtime.ModelEvent, error) {
	g.mu.Lock()
	g.requests = append(g.requests, cloneModelRequest(request))
	close(g.changed)
	g.changed = make(chan struct{})
	g.mu.Unlock()
	return g.next.StreamModel(ctx, request)
}

func (g *contractGateway) requestsSnapshot() []runtime.ModelRequest {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]runtime.ModelRequest, len(g.requests))
	for index := range g.requests {
		out[index] = cloneModelRequest(g.requests[index])
	}
	return out
}

func (g *contractGateway) waitForRequests(ctx context.Context, count int) error {
	for {
		g.mu.Lock()
		if len(g.requests) >= count {
			g.mu.Unlock()
			return nil
		}
		changed := g.changed
		g.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

type contractTurnOutcome struct {
	result runtime.TurnResult
	err    error
}

func newContractTurnHost(t testing.TB, gateway runtime.ModelGateway) *runtime.TurnExecutionHost {
	return newContractTurnHostWithOptions(t, runtime.TurnExecutionHostOptions{
		ModelGateway: gateway,
	})
}

func newContractTurnHostWithOptions(t testing.TB, options runtime.TurnExecutionHostOptions) *runtime.TurnExecutionHost {
	t.Helper()
	factory := newContractTurnFactory(t)
	reasoning := config.ReasoningCapability{Kind: config.ReasoningKindNone}
	var idMu sync.Mutex
	nextID := 0
	options.Config = config.Config{
		SystemPrompt:  "Exercise the public Floret contract.",
		ContextPolicy: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}
	options.ModelGatewayIdentity = runtime.ModelGatewayIdentity{Provider: "florettest", Model: "contract-model", StateCompatibilityKey: "florettest:contract-model"}
	options.ModelGatewayCapabilities = runtime.ModelGatewayCapabilities{Reasoning: &reasoning}
	options.IDGenerator = func(prefix string) string {
		idMu.Lock()
		defer idMu.Unlock()
		nextID++
		return fmt.Sprintf("%s-%d", prefix, nextID)
	}
	host, err := factory.NewHost(context.Background(), options)
	if err != nil {
		t.Fatalf("create turn host: %v", err)
	}
	return host
}

func newContractTurnFactory(t testing.TB) *runtime.TurnExecutionHostFactory {
	t.Helper()
	store := runtime.NewMemoryStore()
	t.Cleanup(func() { _ = store.Close() })
	var createBinder *runtime.ThreadCreateHostBinder
	var turnBinder *runtime.TurnExecutionHostBinder
	if err := runtime.ConfigureHostCapabilities(store, func(bootstrap *runtime.HostBootstrap) error {
		var err error
		createBinder, err = runtime.NewThreadCreateHostBinder(bootstrap)
		if err != nil {
			return err
		}
		turnBinder, err = runtime.NewTurnExecutionHostBinder(bootstrap)
		return err
	}); err != nil {
		t.Fatalf("configure host capabilities: %v", err)
	}
	creator, err := createBinder.Bind("contract-thread", "contract-create")
	if err != nil {
		t.Fatalf("bind thread creator: %v", err)
	}
	if _, err := creator.CreateThread(context.Background(), runtime.CreateThreadRequest{
		ThreadID: "contract-thread", CreateIntentID: "contract-create",
	}); err != nil {
		t.Fatalf("create thread: %v", err)
	}
	factory, err := turnBinder.Bind("contract-thread")
	if err != nil {
		t.Fatalf("bind turn factory: %v", err)
	}
	return factory
}

func runContractTurn(ctx context.Context, host *runtime.TurnExecutionHost, turn int) (runtime.TurnResult, error) {
	return host.RunTurn(ctx, runtime.RunTurnRequest{
		ThreadID: "contract-thread",
		TurnID:   runtime.TurnID(fmt.Sprintf("contract-turn-%d", turn)),
		RunID:    runtime.RunID(fmt.Sprintf("contract-run-%d", turn)),
		Input:    runtime.TurnInput{Text: fmt.Sprintf("contract input %d", turn)},
	})
}

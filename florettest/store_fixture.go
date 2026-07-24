package florettest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/floegence/floret/config"
	"github.com/floegence/floret/runtime"
)

// StoreFixtureTurn contains only public inputs for one durable turn.
type StoreFixtureTurn struct {
	Request    runtime.RunTurnRequest
	ModelSteps []ModelStep
}

// StoreFixtureInput describes a public-API-only Store fixture. PopulateStoreFixture
// requires a fresh, unconfigured Store and never exposes its backend tables or
// internal storage handles.
type StoreFixtureInput struct {
	ThreadID       runtime.ThreadID
	CreateIntentID runtime.CreateIntentID
	Turns          []StoreFixtureTurn
}

// StoreFixtureResult reports only public durable creation and turn outcomes.
type StoreFixtureResult struct {
	Thread runtime.ThreadSummary
	Turns  []runtime.TurnResult
}

// PopulateStoreFixture creates a thread and runs scripted turns exclusively
// through runtime's public capability binders and ModelGateway contract.
func PopulateStoreFixture(ctx context.Context, store *runtime.Store, input StoreFixtureInput) (StoreFixtureResult, error) {
	if store == nil {
		return StoreFixtureResult{}, errors.New("florettest: Store fixture requires a Store")
	}
	input.ThreadID = runtime.ThreadID(strings.TrimSpace(string(input.ThreadID)))
	input.CreateIntentID = runtime.CreateIntentID(strings.TrimSpace(string(input.CreateIntentID)))
	if input.ThreadID == "" || input.CreateIntentID == "" {
		return StoreFixtureResult{}, errors.New("florettest: Store fixture requires thread and create intent identities")
	}
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
		return StoreFixtureResult{}, fmt.Errorf("florettest: configure Store fixture capabilities: %w", err)
	}
	creator, err := createBinder.Bind(input.ThreadID, input.CreateIntentID)
	if err != nil {
		return StoreFixtureResult{}, fmt.Errorf("florettest: bind Store fixture creator: %w", err)
	}
	thread, err := creator.CreateThread(ctx, runtime.CreateThreadRequest{ThreadID: input.ThreadID, CreateIntentID: input.CreateIntentID})
	if err != nil {
		return StoreFixtureResult{}, fmt.Errorf("florettest: create Store fixture thread: %w", err)
	}
	factory, err := turnBinder.Bind(input.ThreadID)
	if err != nil {
		return StoreFixtureResult{}, fmt.Errorf("florettest: bind Store fixture turn factory: %w", err)
	}
	reasoning := config.ReasoningCapability{Kind: config.ReasoningKindNone}
	var generated atomic.Int64
	result := StoreFixtureResult{Thread: thread, Turns: make([]runtime.TurnResult, 0, len(input.Turns))}
	for index, turn := range input.Turns {
		request := turn.Request
		if request.ThreadID == "" {
			request.ThreadID = input.ThreadID
		}
		if request.ThreadID != input.ThreadID {
			return StoreFixtureResult{}, fmt.Errorf("florettest: Store fixture turn %d thread identity mismatch", index)
		}
		if request.TurnID == "" || request.RunID == "" {
			return StoreFixtureResult{}, fmt.Errorf("florettest: Store fixture turn %d requires turn and run identities", index)
		}
		host, err := factory.NewHost(ctx, runtime.TurnExecutionHostOptions{
			Config: config.Config{
				SystemPrompt:  "Populate a public Floret Store fixture.",
				ContextPolicy: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
			},
			ModelGateway: NewScriptedModelGateway(turn.ModelSteps...),
			ModelGatewayIdentity: runtime.ModelGatewayIdentity{
				Provider: "florettest", Model: "store-fixture", StateCompatibilityKey: "florettest:store-fixture:v1",
			},
			ModelGatewayCapabilities: runtime.ModelGatewayCapabilities{Reasoning: &reasoning},
			IDGenerator: func(prefix string) string {
				return fmt.Sprintf("%s-store-fixture-%d", prefix, generated.Add(1))
			},
		})
		if err != nil {
			return StoreFixtureResult{}, fmt.Errorf("florettest: create Store fixture turn %d host: %w", index, err)
		}
		outcome, err := host.RunTurn(ctx, request)
		if err != nil {
			return StoreFixtureResult{}, fmt.Errorf("florettest: run Store fixture turn %d: %w", index, err)
		}
		result.Turns = append(result.Turns, outcome)
	}
	return result, nil
}

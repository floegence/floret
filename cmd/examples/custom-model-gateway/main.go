package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/floegence/floret/config"
	floretruntime "github.com/floegence/floret/runtime"
)

type scriptedGateway struct {
	mu               sync.Mutex
	calls            int
	secondPreviousID string
	cancelEntered    chan struct{}
}

func (g *scriptedGateway) StreamModel(ctx context.Context, req floretruntime.ModelRequest) (<-chan floretruntime.ModelEvent, error) {
	g.mu.Lock()
	g.calls++
	call := g.calls
	if call == 2 && req.PreviousState != nil {
		g.secondPreviousID = req.PreviousState.ID
	}
	g.mu.Unlock()

	if call == 3 {
		close(g.cancelEntered)
		<-ctx.Done()
		return nil, ctx.Err()
	}

	stateID := fmt.Sprintf("opaque-response-%d", call)
	events := make(chan floretruntime.ModelEvent, 2)
	events <- floretruntime.ModelEvent{Type: floretruntime.ModelEventDelta, Text: fmt.Sprintf("gateway response %d", call)}
	events <- floretruntime.ModelEvent{
		Type:       floretruntime.ModelEventDone,
		Reason:     "stop",
		ResponseID: stateID,
		ResponseState: &floretruntime.ModelState{
			Kind: "example-gateway", ID: stateID, Attributes: map[string]string{"region": "local"},
		},
	}
	close(events)
	return events, nil
}

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	store := floretruntime.NewMemoryStore()
	defer store.Close()

	var createBinder *floretruntime.ThreadCreateHostBinder
	var turnBinder *floretruntime.TurnExecutionHostBinder
	if err := floretruntime.ConfigureHostCapabilities(store, func(bootstrap *floretruntime.HostBootstrap) error {
		var configureErr error
		createBinder, configureErr = floretruntime.NewThreadCreateHostBinder(bootstrap)
		if configureErr != nil {
			return configureErr
		}
		turnBinder, configureErr = floretruntime.NewTurnExecutionHostBinder(bootstrap)
		return configureErr
	}); err != nil {
		return err
	}

	const threadID = floretruntime.ThreadID("gateway-thread")
	const createIntentID = floretruntime.CreateIntentID("create-gateway-thread")
	createHost, err := createBinder.Bind(threadID, createIntentID)
	if err != nil {
		return err
	}
	if _, err := createHost.CreateThread(ctx, floretruntime.CreateThreadRequest{ThreadID: threadID, CreateIntentID: createIntentID}); err != nil {
		return err
	}

	gateway := &scriptedGateway{cancelEntered: make(chan struct{})}
	reasoning := config.ReasoningCapability{Kind: config.ReasoningKindNone}
	turnFactory, err := turnBinder.Bind(threadID)
	if err != nil {
		return err
	}
	turnHost, err := turnFactory.NewHost(ctx, floretruntime.TurnExecutionHostOptions{
		Config: config.Config{
			SystemPrompt:  "Use the host model gateway.",
			ContextPolicy: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
		},
		ModelGateway: gateway,
		ModelGatewayIdentity: floretruntime.ModelGatewayIdentity{
			Provider: "example-gateway", Model: "local-scripted-model", StateCompatibilityKey: "example-gateway:v1",
		},
		ModelGatewayCapabilities: floretruntime.ModelGatewayCapabilities{Reasoning: &reasoning},
	})
	if err != nil {
		return err
	}

	for index := 1; index <= 2; index++ {
		result, runErr := turnHost.RunTurn(ctx, floretruntime.RunTurnRequest{
			ThreadID: threadID,
			TurnID:   floretruntime.TurnID(fmt.Sprintf("gateway-turn-%d", index)),
			RunID:    floretruntime.RunID(fmt.Sprintf("gateway-run-%d", index)),
			Input:    floretruntime.TurnInput{Text: fmt.Sprintf("Request %d", index)},
		})
		if runErr != nil {
			return runErr
		}
		fmt.Printf("turn=%d output=%q\n", index, result.Output)
	}

	cancelCtx, cancel := context.WithCancel(ctx)
	type outcome struct {
		result floretruntime.TurnResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, runErr := turnHost.RunTurn(cancelCtx, floretruntime.RunTurnRequest{
			ThreadID: threadID, TurnID: "gateway-turn-cancel", RunID: "gateway-run-cancel",
			Input: floretruntime.TurnInput{Text: "Cancel this request."},
		})
		done <- outcome{result: result, err: runErr}
	}()
	select {
	case <-gateway.cancelEntered:
	case <-time.After(2 * time.Second):
		cancel()
		return fmt.Errorf("timed out waiting for cancellable gateway request")
	}
	cancel()
	var cancelled outcome
	select {
	case cancelled = <-done:
	case <-time.After(2 * time.Second):
		return fmt.Errorf("cancelled gateway request did not return")
	}
	if !errors.Is(cancelled.err, context.Canceled) || cancelled.result.Status != floretruntime.TurnStatusCancelled {
		return fmt.Errorf("cancellation result status=%s error=%v", cancelled.result.Status, cancelled.err)
	}

	gateway.mu.Lock()
	previousID := gateway.secondPreviousID
	gateway.mu.Unlock()
	if previousID != "opaque-response-1" {
		return fmt.Errorf("second request previous state=%q", previousID)
	}
	fmt.Printf("previous_state=%q cancellation=%s\n", previousID, cancelled.result.Status)
	return nil
}

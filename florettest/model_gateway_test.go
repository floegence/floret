package florettest_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/floegence/floret/florettest"
	"github.com/floegence/floret/runtime"
)

func TestScriptedModelGateway(t *testing.T) {
	release := make(chan struct{})
	gateway := florettest.NewScriptedModelGateway(
		florettest.ModelStep{
			BlockUntil: release,
			Events: []runtime.ModelEvent{
				{Type: runtime.ModelEventDelta, Text: "hello"},
				{Type: runtime.ModelEventDone, Reason: "stop", ResponseState: &runtime.ModelState{Kind: "test", ID: "state"}},
			},
		},
	)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := gateway.StreamModel(ctx, runtime.ModelRequest{RunID: "run", Labels: runtime.RunLabels{Host: map[string]string{"mode": "test"}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := gateway.WaitForRequests(ctx, 1); err != nil {
		t.Fatal(err)
	}
	requests := gateway.Requests()
	requests[0].Labels.Host["mode"] = "mutated"
	if got := gateway.Requests()[0].Labels.Host["mode"]; got != "test" {
		t.Fatalf("request snapshot was mutable: %q", got)
	}
	close(release)
	var events []runtime.ModelEvent
	for event := range stream {
		events = append(events, event)
	}
	if len(events) != 2 || events[0].Text != "hello" || events[1].ResponseState == nil || events[1].ResponseState.ID != "state" {
		t.Fatalf("events = %#v", events)
	}
	if _, err := gateway.StreamModel(ctx, runtime.ModelRequest{RunID: "extra"}); !errors.Is(err, florettest.ErrModelScriptExhausted) {
		t.Fatalf("exhausted error = %v", err)
	}
	var zero florettest.ScriptedModelGateway
	if _, err := zero.StreamModel(ctx, runtime.ModelRequest{RunID: "zero"}); !errors.Is(err, florettest.ErrModelScriptExhausted) {
		t.Fatalf("zero-value error = %v", err)
	}
}

func TestScriptedModelGatewayCancellation(t *testing.T) {
	gateway := florettest.NewScriptedModelGateway(florettest.ModelStep{WaitForCancellation: true})
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := gateway.StreamModel(ctx, runtime.ModelRequest{RunID: "run"})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	event, ok := <-stream
	if !ok || event.Type != runtime.ModelEventError || !errors.Is(event.Err, context.Canceled) {
		t.Fatalf("cancellation event = %#v, ok=%v", event, ok)
	}
	if _, ok := <-stream; ok {
		t.Fatal("cancellation stream remained open")
	}
}

func TestModelGatewayContract(t *testing.T) {
	florettest.RunModelGatewayContract(t, florettest.ScriptedModelGatewayFactory)
}

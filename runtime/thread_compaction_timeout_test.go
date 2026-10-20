package runtime

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
)

type summaryStallGateway struct {
	*florettest.ScriptedGateway
	idle bool
}

func (g *summaryStallGateway) Stream(ctx context.Context, req provider.Request) (<-chan provider.Event, error) {
	stream, err := g.ScriptedGateway.Stream(ctx, req)
	count := len(g.Requests())
	if err != nil || !g.idle || count < 4 || count > 5 {
		return stream, err
	}
	out := make(chan provider.Event, 1)
	out <- provider.Event{Type: provider.EventDelta, Text: "partial summary"}
	go func() {
		defer close(out)
		for event := range stream {
			select {
			case out <- event:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

func TestIndependentCompactionTimeoutRecoveryAndRestart(t *testing.T) {
	for _, scenario := range []struct {
		name                        string
		idle, recover, window, stop bool
	}{
		{name: "first_output"},
		{name: "output_idle", idle: true},
		{name: "recovery_window", idle: true, window: true},
		{name: "automatic_recovery", idle: true, recover: true},
		{name: "stop_during_recovery", idle: true, stop: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			last := florettest.Step{WaitForCancellation: true}
			if scenario.recover {
				last = compactionTestStep("The launch code remains CERULEAN-739.")
			}
			gateway := &summaryStallGateway{idle: scenario.idle, ScriptedGateway: florettest.NewScriptedGateway(
				provider.Identity{Provider: "test", Model: "compact", StateCompatibilityKey: "test:compact"},
				provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
				compactionTestStep("ack"), compactionTestStep("ack"), compactionTestStep("ack"),
				florettest.Step{WaitForCancellation: true}, last, compactionTestStep("The launch code remains CERULEAN-739."))}
			retry := DefaultProviderRetryPolicy()
			retry.MaxRetries, retry.MaxTimeoutRetries = 1, 1
			retry.BaseDelay, retry.MaxDelay, retry.Jitter = time.Millisecond, time.Millisecond, 0
			if scenario.window {
				retry.RecoveryWindow = 15 * time.Millisecond
			}
			agent := compactionTestAgent(t, gateway, WithAgentProviderRetryPolicy(retry),
				WithAgentProviderRequestTimeoutPolicy(ProviderRequestTimeoutPolicy{FirstOutputTimeout: 40 * time.Millisecond, IdleTimeout: 40 * time.Millisecond}))
			path := filepath.Join(t.TempDir(), "threads.sqlite")
			open := func() (*Host, ThreadService) {
				host, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
				if err != nil {
					t.Fatal(err)
				}
				service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
				return host, service
			}
			host, service := open()
			created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
			if err != nil {
				t.Fatal(err)
			}
			before := seedCompactionHistory(t, service, created.ThreadID)
			admission, err := service.(ThreadContextCompactor).CompactContext(t.Context(), CompactContextInput{ThreadID: created.ThreadID, RequestKey: "compact"})
			if err != nil {
				t.Fatal(err)
			}
			if scenario.stop {
				ready, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				if err := gateway.WaitForRequests(ready, 5); err != nil {
					t.Fatal(err)
				}
				if _, err := service.Cancel(t.Context(), CancelInput{ThreadID: created.ThreadID, RequestKey: "stop"}); err != nil {
					t.Fatal(err)
				}
			}
			terminal := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool {
				return v.TurnID == admission.TurnID && v.Activity == ThreadActivityIdle && v.LastOutcome != nil
			})
			requests := gateway.Requests()
			if len(requests) != 5 || requests[3].LogicalRequestID != requests[4].LogicalRequestID || requests[3].AttemptID == requests[4].AttemptID || !reflect.DeepEqual(requests[3].Messages, requests[4].Messages) {
				t.Fatalf("summary did not recover one frozen request: %+v", requests)
			}
			if scenario.stop {
				if *terminal.LastOutcome != TurnOutcomeCancelled || terminal.Failure != nil || terminal.Cancellation == nil {
					t.Fatalf("stop outcome=%v failure=%+v", terminal.LastOutcome, terminal.Failure)
				}
				return
			}
			if scenario.recover {
				if *terminal.LastOutcome != TurnOutcomeCompleted || terminal.Failure != nil {
					t.Fatalf("recovery outcome=%v failure=%+v", terminal.LastOutcome, terminal.Failure)
				}
				return
			}
			if *terminal.LastOutcome != TurnOutcomeFailed || terminal.Failure == nil || terminal.Failure.Code != ThreadTurnFailureProvider || terminal.Failure.ProviderTimeout == nil || terminal.Cancellation != nil || len(terminal.Items) != len(before.Items) {
				t.Fatalf("timeout outcome=%v failure=%+v cancellation=%+v", terminal.LastOutcome, terminal.Failure, terminal.Cancellation)
			}
			stage := "first_output"
			if scenario.idle {
				stage = "output_idle"
			}
			if terminal.Failure.ProviderTimeout.Stage != stage {
				t.Fatalf("timeout=%+v", terminal.Failure.ProviderTimeout)
			}
			snapshot, err := service.(ThreadContextReader).Context(t.Context(), created.ThreadID)
			if err != nil || len(snapshot.Compactions) != 1 || snapshot.Compactions[0].Status != "failed" {
				t.Fatalf("context=%+v err=%v", snapshot, err)
			}
			state, err := host.store.repo.(sessiontree.ProviderStateReader).ProviderState(t.Context(), created.ThreadID.String())
			if err != nil || state.State.ID != "continuation" {
				t.Fatalf("lost original continuation: %v", err)
			}
			entries, err := host.store.repo.Entries(t.Context(), created.ThreadID.String())
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				if entry.Type == sessiontree.EntryCompaction {
					t.Fatal("failed compaction installed a summary")
				}
			}
			if err := host.Shutdown(t.Context()); err != nil {
				t.Fatal(err)
			}
			_, service = open()
			restored, err := service.View(t.Context(), created.ThreadID)
			if err != nil || !reflect.DeepEqual(restored.Failure, terminal.Failure) {
				t.Fatalf("restart=%+v err=%v", restored.Failure, err)
			}
			if _, err := service.(ThreadContextCompactor).CompactContext(t.Context(), CompactContextInput{ThreadID: created.ThreadID, RequestKey: "retry-compact"}); err != nil {
				t.Fatal(err)
			}
			after := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool {
				return v.TurnID != terminal.TurnID && v.Activity == ThreadActivityIdle && v.LastOutcome != nil
			})
			if *after.LastOutcome != TurnOutcomeCompleted || after.Failure != nil {
				t.Fatalf("retry=%+v", after)
			}
		})
	}
}

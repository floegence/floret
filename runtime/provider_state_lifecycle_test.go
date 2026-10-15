package runtime

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
)

func nativeHistoryStep(id string, length bool) florettest.Step {
	reason := "stop"
	if length {
		reason = "length"
	}
	return florettest.Step{Events: []provider.Event{
		{Type: provider.EventDelta, Text: "Completed output " + id},
		{Type: provider.EventDone, Reason: reason, ResponseState: &provider.State{Kind: "native", ID: id, ReusePolicy: provider.StateReuseCanonicalHistory, Attributes: map[string]string{"opaque": "preserved"}}},
	}}
}

func waitNativeRequests(t *testing.T, gateway *florettest.ScriptedGateway, count int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if err := gateway.WaitForRequests(ctx, count); err != nil {
		t.Fatal(err)
	}
}

func sendNativeTurn(t *testing.T, service ThreadService, threadID identity.ThreadID, key string) ThreadView {
	t.Helper()
	admitted, err := service.Send(t.Context(), SendInput{ThreadID: threadID, RequestKey: RequestKey(key), Input: UserInput{Text: key}})
	if err != nil {
		t.Fatal(err)
	}
	return waitThreadView(t, service, threadID, func(view ThreadView) bool {
		return view.TurnID == admitted.TurnID && view.Activity == ThreadActivityIdle && view.LastOutcome != nil
	})
}

func TestProviderNativeHistorySurvivesRuntimeStopAndRestart(t *testing.T) {
	for _, mode := range []CancelMode{CancelModeImmediate, CancelModeGraceful} {
		for _, scenario := range []string{"completed_step", "prior_turn", "first_request"} {
			t.Run(string(mode)+"/"+scenario, func(t *testing.T) {
				var steps []florettest.Step
				expected, requestsBeforeStop := "", 1
				if scenario == "prior_turn" {
					steps = append(steps, nativeHistoryStep("prior", false))
					expected = "prior"
					requestsBeforeStop++
				}
				if scenario == "completed_step" {
					steps = append(steps, nativeHistoryStep("first", true))
					expected = "first"
					requestsBeforeStop++
				}
				steps = append(steps, florettest.Step{WaitForCancellation: true}, nativeHistoryStep("probe", false))
				gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "native", StateCompatibilityKey: "account:model"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, steps...)
				path := filepath.Join(t.TempDir(), "threads.sqlite")
				open := func() (*Host, ThreadService) {
					host, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
					service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return testAgent(gateway) }))
					if err != nil {
						t.Fatal(err)
					}
					return host, service
				}
				host, service := open()
				created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
				if err != nil {
					t.Fatal(err)
				}
				if scenario == "prior_turn" {
					sendNativeTurn(t, service, created.ThreadID, "seed")
				}
				started, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "cancelled", Input: UserInput{Text: "continue"}})
				if err != nil {
					t.Fatal(err)
				}
				waitNativeRequests(t, gateway, requestsBeforeStop)
				var author sessiontree.ProviderStateRecord
				if expected != "" {
					author, err = host.store.repo.(sessiontree.ProviderStateReader).ProviderState(t.Context(), created.ThreadID.String())
					if err != nil || author.State.ID != expected {
						t.Fatalf("completed response was not committed before next request: %+v %v", author, err)
					}
				}
				if _, err := service.Cancel(t.Context(), CancelInput{ThreadID: created.ThreadID, RequestKey: "stop", Mode: mode}); err != nil {
					t.Fatal(err)
				}
				cancelled := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
				if *cancelled.LastOutcome != TurnOutcomeCancelled {
					t.Fatalf("stop outcome=%+v", cancelled.LastOutcome)
				}
				if expected != "" {
					saved, err := host.store.repo.(sessiontree.ProviderStateReader).ProviderState(t.Context(), created.ThreadID.String())
					if err != nil || saved.State.ID != expected || saved.CreatedByRunID != author.CreatedByRunID || saved.CreatedByTurnID != author.CreatedByTurnID {
						t.Fatalf("stop changed state authorship: %+v %v", saved, err)
					}
				}
				if err := host.Shutdown(t.Context()); err != nil {
					t.Fatal(err)
				}
				_, service = open()
				probe := sendNativeTurn(t, service, created.ThreadID, "probe")
				if probe.Failure != nil {
					t.Fatal(probe.Failure)
				}
				request := gateway.Requests()[requestsBeforeStop]
				if expected == "" {
					if request.PreviousState != nil {
						t.Fatal("first incomplete response installed state")
					}
				} else if request.PreviousState == nil || request.PreviousState.ID != expected || request.PreviousState.ReusePolicy != provider.StateReuseCanonicalHistory {
					t.Fatalf("restart lost native history: %+v", request.PreviousState)
				}
				if started.TurnID == probe.TurnID {
					t.Fatal("probe reused cancelled turn identity")
				}
			})
		}
	}
}

func TestProviderNativeHistorySurvivesNextStepFailure(t *testing.T) {
	for _, cause := range []error{errors.New("network unavailable"), context.DeadlineExceeded} {
		t.Run(cause.Error(), func(t *testing.T) {
			gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "native", StateCompatibilityKey: "account:model"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, nativeHistoryStep("first", true), florettest.Step{ReturnError: cause}, nativeHistoryStep("probe", false))
			agent, agentErr := testAgent(gateway, WithAgentProviderRetryPolicy(ProviderRetryPolicy{Disabled: true}))
			if agentErr != nil {
				t.Fatal(agentErr)
			}
			host, service := testThreadServiceWithAgent(t, agent)
			created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
			if err != nil {
				t.Fatal(err)
			}
			failed := sendNativeTurn(t, service, created.ThreadID, "fail")
			if *failed.LastOutcome == TurnOutcomeCompleted {
				t.Fatal("failed request completed")
			}
			saved, err := host.store.repo.(sessiontree.ProviderStateReader).ProviderState(t.Context(), created.ThreadID.String())
			if err != nil || saved.State.ID != "first" {
				t.Fatalf("failure lost completed state: %+v %v", saved, err)
			}
			if probe := sendNativeTurn(t, service, created.ThreadID, "probe"); probe.Failure != nil {
				t.Fatal(probe.Failure)
			}
			if state := gateway.Requests()[2].PreviousState; state == nil || state.ID != "first" {
				t.Fatalf("continuation=%+v", state)
			}
		})
	}
}

func TestSupplementalTurnFailureCannotRestoreEarlierNativeState(t *testing.T) {
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "native", StateCompatibilityKey: "account:model"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, nativeHistoryStep("prior", false), florettest.Step{ReturnError: errors.New("network unavailable")}, nativeHistoryStep("probe", false))
	_, service := testThreadService(t, gateway)
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	sendNativeTurn(t, service, created.ThreadID, "seed")
	admitted, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "secret", Input: UserInput{Text: "use secret once"}, SupplementalContext: []TurnSupplementalContextItem{{Kind: "secret", Title: "Fixture", Text: "private-value", Sensitive: true}}})
	if err != nil {
		t.Fatal(err)
	}
	waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.TurnID == admitted.TurnID && v.Activity == ThreadActivityIdle })
	sendNativeTurn(t, service, created.ThreadID, "probe")
	policy := gateway.Requests()[1].ReusePolicy
	if policy == nil || policy.PromptCacheEnabled || policy.ProviderStateAllowed || policy.PreferContinuation {
		t.Fatalf("secret reuse policy=%+v", policy)
	}
	for _, index := range []int{1, 2} {
		if gateway.Requests()[index].PreviousState != nil {
			t.Fatalf("secret fence reused state in request %d", index)
		}
	}
}

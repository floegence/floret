package runtime_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/runtime"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/threadquery"
)

func TestHistoricalTurnFailureSurvivesContinuationAndRestart(t *testing.T) {
	gateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "failure", StateCompatibilityKey: "failure:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{ReturnError: errors.New(strings.Repeat("original provider failure", 30000))},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "Recovered"}, {Type: provider.EventDone, Reason: "stop"}}},
	)
	path := t.TempDir() + "/failures.sqlite"
	open := func() (*runtime.Host, runtime.ThreadService) {
		return inputPolicyService(t, storage.SQLite(path), gateway)
	}
	host, service := open()
	created, err := service.Create(t.Context(), runtime.CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(t.Context(), runtime.SendInput{ThreadID: created.ThreadID, RequestKey: "fail", Input: runtime.UserInput{Text: "Fail"}}); err != nil {
		t.Fatal(err)
	}
	failed := inputPolicyWait(t, service, created.ThreadID, func(v runtime.ThreadView) bool {
		return v.Activity == runtime.ThreadActivityIdle && v.LastOutcome != nil
	})
	check := func(turns []runtime.ThreadTurn) {
		t.Helper()
		raw, err := json.Marshal(turns)
		if err != nil {
			t.Fatal(err)
		}
		if len(turns) == 0 || !turns[0].HasFailure {
			t.Fatalf("historical turn lost failure availability: %s", raw)
		}
		if len(raw) > 4096 {
			t.Fatalf("turn metadata contains diagnostic body: %d bytes", len(raw))
		}
		if len(turns) > 1 && turns[1].HasFailure {
			t.Fatal("failure leaked into later turn")
		}
		scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{created.ThreadID}}
		reader := service.(runtime.ThreadTurnFailureReader)
		input := runtime.ThreadTurnFailureInput{ThreadID: created.ThreadID, TurnID: turns[0].TurnID, RunID: turns[0].RunID}
		value, err := reader.ReadTurnFailure(t.Context(), scope, input)
		if err != nil || value == nil || value.Code != runtime.ThreadTurnFailureProvider || value.Message != strings.Repeat("original provider failure", 30000) {
			t.Fatalf("historical failure unavailable: %v", err)
		}
		value.Message = "consumer mutation"
		again, err := reader.ReadTurnFailure(t.Context(), scope, input)
		if err != nil || again == nil || again.Message != strings.Repeat("original provider failure", 30000) {
			t.Fatalf("consumer changed canonical failure: %v", err)
		}
		input.RunID = "wrong-run"
		if _, err := reader.ReadTurnFailure(t.Context(), scope, input); !errors.Is(err, threadquery.ErrRecordUnavailable) {
			t.Fatalf("stale execution: %v", err)
		}
		input.RunID = turns[0].RunID
		if _, err := reader.ReadTurnFailure(t.Context(), threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{"other"}}, input); err == nil {
			t.Fatal("unauthorized failure read succeeded")
		}
		if len(turns) > 1 {
			input.TurnID, input.RunID = turns[1].TurnID, turns[1].RunID
			if value, err := reader.ReadTurnFailure(t.Context(), scope, input); err != nil || value != nil {
				t.Fatalf("successful turn has failure: %v", err)
			}
		}
	}
	check(failed.Turns)
	if _, err := service.Send(t.Context(), runtime.SendInput{ThreadID: created.ThreadID, RequestKey: "continue", Input: runtime.UserInput{Text: "Continue"}}); err != nil {
		t.Fatal(err)
	}
	completed := inputPolicyWait(t, service, created.ThreadID, func(v runtime.ThreadView) bool {
		return v.Activity == runtime.ThreadActivityIdle && v.TurnID != failed.TurnID && v.LastOutcome != nil
	})
	if completed.Failure != nil {
		t.Fatal("current failure was not cleared")
	}
	check(completed.Turns)
	if err := host.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	_, service = open()
	scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{created.ThreadID}}
	page, err := service.(runtime.ThreadItemPageReader).ItemPage(t.Context(), scope, runtime.ThreadItemPageInput{ThreadID: created.ThreadID, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	check(page.Turns)
	sub, err := service.(runtime.ThreadWindowSource).SubscribeWindowUpdates(t.Context(), scope)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if err := sub.SetWindow(t.Context(), scope, runtime.ThreadWindowInput{ThreadID: created.ThreadID, Limit: 100}); err != nil {
		t.Fatal(err)
	}
	baseline := historicalFailureUpdate(t, sub)
	for baseline.Window == nil {
		baseline = historicalFailureUpdate(t, sub)
	}
	if baseline.Turns == nil {
		t.Fatal("window omitted turn facts")
	}
	check(baseline.Turns.Value)
	restored, err := service.View(t.Context(), created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	check(restored.Turns)
	again, err := service.View(t.Context(), created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	check(again.Turns)
	history, err := service.History(t.Context(), created.ThreadID, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	check(history.Turns)
}

func historicalFailureUpdate(t *testing.T, sub *runtime.ThreadUpdateSubscription) runtime.ThreadUpdate {
	t.Helper()
	value, err := sub.Next(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return value
}

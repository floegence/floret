package runtime

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
)

func TestThreadContextBudgetFailureSurvivesRestartAndAllowsRecovery(t *testing.T) {
	path := t.TempDir() + "/context-budget.db"
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "scripted", StateCompatibilityKey: "test:scripted:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "recovered"}, {Type: provider.EventDone, Reason: "stop"}}})
	agent, err := NewAgent(config.AgentConfig{Profile: config.AgentProfile{ID: "test", Name: "Test"}, SystemPrompt: "Test.", Context: config.ContextPolicy{ContextWindowTokens: 3891, MaxOutputTokens: 4096}}, gateway)
	if err != nil {
		t.Fatal(err)
	}
	host, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
	if err != nil {
		t.Fatal(err)
	}
	service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
	if err != nil {
		t.Fatal(err)
	}
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-budget"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "weather"}, RequestKey: "send-budget"}); err != nil {
		t.Fatal(err)
	}
	waitTerminal := func() ThreadView {
		t.Helper()
		waited, err := service.(ThreadWaiter).WaitThreads(t.Context(), WaitThreadsInput{
			ThreadIDs: []identity.ThreadID{created.ThreadID}, Timeout: time.Minute,
		})
		if err != nil || waited.Reason != ThreadWaitCompleted {
			t.Fatalf("terminal wait=%+v err=%v", waited, err)
		}
		view, err := service.View(t.Context(), created.ThreadID)
		if err != nil || view.Activity != ThreadActivityIdle || view.LastOutcome == nil {
			t.Fatalf("terminal activity=%s outcome=%v err=%v", view.Activity, view.LastOutcome, err)
		}
		return view
	}
	failed := waitTerminal()
	if failed.Failure == nil || failed.Failure.Code != ThreadTurnFailureContextBudgetInvalid || !strings.Contains(failed.Failure.Message, "request_safe_limit=-205") {
		t.Fatalf("failure=%#v", failed.Failure)
	}
	if len(gateway.Requests()) != 0 {
		t.Fatal("invalid budget reached the provider")
	}
	summaries, err := service.List(t.Context(), ThreadScope{})
	if err != nil || len(summaries) != 1 || summaries[0].Failure.Code != ThreadTurnFailureContextBudgetInvalid {
		t.Fatalf("summaries=%#v err=%v", summaries, err)
	}
	if err := host.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	host, err = Open(t.Context(), Options{Storage: storage.SQLite(path)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	agent, err = testAgent(gateway)
	if err != nil {
		t.Fatal(err)
	}
	service, err = host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
	if err != nil {
		t.Fatal(err)
	}
	restored, err := service.View(t.Context(), created.ThreadID)
	if err != nil || restored.Failure == nil || *restored.Failure != *failed.Failure {
		t.Fatalf("restored failure=%#v err=%v", restored.Failure, err)
	}
	if _, err := service.Retry(t.Context(), RetryInput{ThreadID: created.ThreadID, SourceTurnID: restored.TurnID, RequestKey: "retry-budget"}); err != nil {
		t.Fatal(err)
	}
	recovered := waitTerminal()
	if *recovered.LastOutcome != TurnOutcomeCompleted || recovered.Failure != nil || len(gateway.Requests()) != 1 {
		t.Fatalf("recovered=%#v requests=%d", recovered, len(gateway.Requests()))
	}
}

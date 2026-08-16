// Package adoption_test exercises the released v4 API from a blank module.
package adoption_test

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/v4/config"
	"github.com/floegence/floret/v4/florettest"
	"github.com/floegence/floret/v4/provider"
	"github.com/floegence/floret/v4/runtime"
	"github.com/floegence/floret/v4/storage"
)

type oneShotCompaction struct {
	consumed atomic.Bool
}

func (source *oneShotCompaction) PollManualCompaction(context.Context, runtime.ManualCompactionPollRequest) (runtime.ManualCompactionRequest, bool, error) {
	if !source.consumed.CompareAndSwap(false, true) {
		return runtime.ManualCompactionRequest{}, false, nil
	}
	return runtime.ManualCompactionRequest{RequestID: "adoption-compact", Source: "adoption_test"}, true, nil
}

func TestPublishedThreadContextReaderSurvivesSQLiteRestart(t *testing.T) {
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "floret.db")
	gateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "adoption", Model: "scripted", StateCompatibilityKey: "adoption:scripted:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "done"}, {Type: provider.EventDone, Reason: "stop"}}},
	)
	agent, err := runtime.NewAgent(config.AgentConfig{
		Profile:      config.AgentProfile{ID: "adoption", Name: "Adoption Agent"},
		SystemPrompt: "Complete the requested task.",
		Context:      config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}, gateway, runtime.WithAgentManualCompactions(&oneShotCompaction{}))
	if err != nil {
		t.Fatal(err)
	}
	factory := runtime.AgentFactoryFunc(func(context.Context, runtime.AgentRequest) (*runtime.Agent, error) { return agent, nil })

	firstHost, err := runtime.Open(ctx, runtime.Options{Storage: storage.SQLite(databasePath)})
	if err != nil {
		t.Fatal(err)
	}
	firstService, err := firstHost.ThreadService(factory)
	if err != nil {
		t.Fatal(err)
	}
	created, err := firstService.Create(ctx, runtime.CreateThreadInput{RequestKey: "create-adoption"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := firstService.Send(ctx, runtime.SendInput{ThreadID: created.ThreadID, Input: runtime.UserInput{Text: "compact then answer"}, RequestKey: "send-adoption"}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		view, viewErr := firstService.View(ctx, created.ThreadID)
		if viewErr != nil {
			t.Fatal(viewErr)
		}
		if view.Activity == runtime.ThreadActivityIdle && view.LastOutcome != nil {
			if len(view.Items) != 2 || view.Items[0].Ordinal != 1 || view.Items[1].Ordinal != 2 || view.Items[1].Kind != runtime.ThreadItemAssistant {
				t.Fatalf("published ordered presentation=%#v", view.Items)
			}
			_ = runtime.ThreadItem{ID: "adoption-thinking", Ordinal: 1, Kind: runtime.ThreadItemThinking, Live: true}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("typed thread did not complete")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := firstHost.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}

	secondHost, err := runtime.Open(ctx, runtime.Options{Storage: storage.SQLite(databasePath)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = secondHost.Shutdown(context.Background()) }()
	secondService, err := secondHost.ThreadService(factory)
	if err != nil {
		t.Fatal(err)
	}
	reader, ok := secondService.(runtime.ThreadContextReader)
	if !ok {
		t.Fatal("published ThreadService does not expose ThreadContextReader")
	}
	snapshot, err := reader.Context(ctx, created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Compactions) != 1 || snapshot.Compactions[0].RequestID != "adoption-compact" || snapshot.Compactions[0].Status != "noop" {
		t.Fatalf("canonical compactions=%#v", snapshot.Compactions)
	}
}

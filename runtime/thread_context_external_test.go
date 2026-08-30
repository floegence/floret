package runtime_test

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/v6/config"
	"github.com/floegence/floret/v6/florettest"
	"github.com/floegence/floret/v6/identity"
	"github.com/floegence/floret/v6/observation"
	"github.com/floegence/floret/v6/provider"
	"github.com/floegence/floret/v6/runtime"
	"github.com/floegence/floret/v6/storage"
)

func TestThreadContextReaderAccumulatesFinalProviderUsageFromEngineEvents(t *testing.T) {
	gateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "context-usage", StateCompatibilityKey: "test:context-usage:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{
			{Type: provider.EventUsage, Usage: provider.Usage{
				InputTokens: 80, OutputTokens: 20, CacheReadTokens: 15, CacheWriteTokens: 5,
				WindowInputTokens: 100, TotalTokens: 120, Source: "native", Available: true,
			}},
			{Type: provider.EventDelta, Text: "done"},
			{Type: provider.EventDone, Reason: "stop"},
		}},
		florettest.Step{Events: []provider.Event{
			{Type: provider.EventUsage, Usage: provider.Usage{
				InputTokens: 40, OutputTokens: 10, CacheReadTokens: 30,
				WindowInputTokens: 70, TotalTokens: 80, Source: "native", Available: true,
			}},
			{Type: provider.EventDelta, Text: "done again"},
			{Type: provider.EventDone, Reason: "stop"},
		}},
	)
	recorder := &threadUsageEventRecorder{}
	agent, err := runtime.NewAgent(config.AgentConfig{
		Profile:      config.AgentProfile{ID: "test", Name: "Test"},
		SystemPrompt: "Test.",
		Context:      config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}, gateway, runtime.WithAgentEventSink(recorder))
	if err != nil {
		t.Fatal(err)
	}
	databasePath := filepath.Join(t.TempDir(), "thread-usage.db")
	host, err := runtime.Open(t.Context(), runtime.Options{Storage: storage.SQLite(databasePath)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	service, err := host.ThreadService(runtime.AgentFactoryFunc(func(context.Context, runtime.AgentRequest) (*runtime.Agent, error) {
		return agent, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	reader := service.(runtime.ThreadContextReader)
	created, err := service.Create(t.Context(), runtime.CreateThreadInput{RequestKey: "create-context-usage"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(t.Context(), runtime.SendInput{
		ThreadID: created.ThreadID, Input: runtime.UserInput{Text: "measure"}, RequestKey: "send-context-usage",
	}); err != nil {
		t.Fatal(err)
	}

	waitForThreadUsageTotals(t, reader, created.ThreadID, runtime.ThreadTokenUsageTotals{
		InputTokens: 80, OutputTokens: 20, CacheReadTokens: 15, CacheWriteTokens: 5,
	})
	waitForLiveUsageTotals(t, recorder, 1)
	if got := recorder.snapshot(); got[0] != (runtime.ThreadTokenUsageTotals{
		InputTokens: 80, OutputTokens: 20, CacheReadTokens: 15, CacheWriteTokens: 5,
	}) {
		t.Fatalf("first live totals=%#v", got)
	}

	if _, err := service.Send(t.Context(), runtime.SendInput{
		ThreadID: created.ThreadID, Input: runtime.UserInput{Text: "measure again"}, RequestKey: "send-context-usage-again",
	}); err != nil {
		t.Fatal(err)
	}
	want := runtime.ThreadTokenUsageTotals{
		InputTokens: 120, OutputTokens: 30, CacheReadTokens: 45, CacheWriteTokens: 5,
	}
	waitForThreadUsageTotals(t, reader, created.ThreadID, want)
	waitForLiveUsageTotals(t, recorder, 2)
	if got := recorder.snapshot(); got[1] != want {
		t.Fatalf("cumulative live totals=%#v", got)
	}

	if err := host.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	reopened, err := runtime.Open(t.Context(), runtime.Options{Storage: storage.SQLite(databasePath)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Shutdown(context.Background()) })
	reopenedService, err := reopened.ThreadService(runtime.AgentFactoryFunc(func(context.Context, runtime.AgentRequest) (*runtime.Agent, error) {
		return agent, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	waitForThreadUsageTotals(t, reopenedService.(runtime.ThreadContextReader), created.ThreadID, want)
	if got := recorder.snapshot(); len(got) != 2 {
		t.Fatalf("restart emitted duplicate live totals=%#v", got)
	}
}

type threadUsageEventRecorder struct {
	mu     sync.Mutex
	totals []runtime.ThreadTokenUsageTotals
}

func (recorder *threadUsageEventRecorder) EmitEvent(event runtime.Event) {
	if event.Type != observation.EventTypeProviderUsage || event.ThreadUsageTotals == nil {
		return
	}
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	recorder.totals = append(recorder.totals, *event.ThreadUsageTotals)
}

func (recorder *threadUsageEventRecorder) snapshot() []runtime.ThreadTokenUsageTotals {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	return append([]runtime.ThreadTokenUsageTotals(nil), recorder.totals...)
}

func waitForLiveUsageTotals(t *testing.T, recorder *threadUsageEventRecorder, count int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if got := recorder.snapshot(); len(got) >= count {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("live usage total count=%d, want at least %d", len(recorder.snapshot()), count)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitForThreadUsageTotals(t *testing.T, reader runtime.ThreadContextReader, threadID identity.ThreadID, want runtime.ThreadTokenUsageTotals) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		snapshot, err := reader.Context(t.Context(), threadID)
		if err != nil {
			t.Fatal(err)
		}
		if snapshot.UsageTotals != nil && *snapshot.UsageTotals == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("thread usage totals=%#v, want %#v", snapshot.UsageTotals, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestHostThreadServiceExposesCanonicalThreadContextReader(t *testing.T) {
	gateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "context-reader", StateCompatibilityKey: "test:context-reader:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
	)
	agent, err := runtime.NewAgent(config.AgentConfig{
		Profile:      config.AgentProfile{ID: "test", Name: "Test"},
		SystemPrompt: "Test.",
		Context:      config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}, gateway)
	if err != nil {
		t.Fatal(err)
	}
	host, err := runtime.Open(t.Context(), runtime.Options{Storage: storage.Memory()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	service, err := host.ThreadService(runtime.AgentFactoryFunc(func(context.Context, runtime.AgentRequest) (*runtime.Agent, error) {
		return agent, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	reader, ok := service.(runtime.ThreadContextReader)
	if !ok {
		t.Fatal("Host.ThreadService must implement runtime.ThreadContextReader")
	}
	created, err := service.Create(t.Context(), runtime.CreateThreadInput{RequestKey: "create-context-reader"})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := reader.Context(t.Context(), created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Compactions) != 0 {
		t.Fatalf("new thread compactions=%#v", snapshot.Compactions)
	}
	if snapshot.UsageTotals != nil {
		t.Fatalf("new thread usage totals=%#v, want nil", snapshot.UsageTotals)
	}
}

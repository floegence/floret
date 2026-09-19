// Package adoption_test exercises the released v7 API from a blank module.
package adoption_test

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/observation"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/runtime"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/tools"
)

type oneShotCompaction struct {
	consumed atomic.Bool
}

type usageEventRecorder struct {
	mu     sync.Mutex
	totals []runtime.ThreadTokenUsageTotals
}

func (recorder *usageEventRecorder) EmitEvent(event runtime.Event) {
	if event.Type != observation.EventTypeProviderUsage || event.ThreadUsageTotals == nil {
		return
	}
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	recorder.totals = append(recorder.totals, *event.ThreadUsageTotals)
}

func (recorder *usageEventRecorder) snapshot() []runtime.ThreadTokenUsageTotals {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	return append([]runtime.ThreadTokenUsageTotals(nil), recorder.totals...)
}

func (source *oneShotCompaction) PollManualCompaction(context.Context, runtime.ManualCompactionPollRequest) (runtime.ManualCompactionRequest, bool, error) {
	if !source.consumed.CompareAndSwap(false, true) {
		return runtime.ManualCompactionRequest{}, false, nil
	}
	return runtime.ManualCompactionRequest{RequestID: "adoption-compact", Source: "adoption_test"}, true, nil
}

func TestPublishedThreadContextReaderSurvivesSQLiteRestart(t *testing.T) {
	_ = runtime.ThreadTokenUsageTotals{InputTokens: 80, CacheReadTokens: 15, CacheWriteTokens: 5, OutputTokens: 20}
	_ = runtime.Event{ThreadUsageTotals: &runtime.ThreadTokenUsageTotals{InputTokens: 80}}
	_ = tools.WebFetchActivityPayload{
		ContentPreview: "preview", PreviewTruncated: true,
		SiteIcon: &tools.WebFetchActivityIcon{ContentType: "image/png", Data: []byte("png")},
	}
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "floret.db")
	gateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "adoption", Model: "scripted", StateCompatibilityKey: "adoption:scripted:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{
			{Type: provider.EventUsage, Usage: provider.Usage{
				InputTokens: 80, OutputTokens: 20, CacheReadTokens: 15, CacheWriteTokens: 5,
				WindowInputTokens: 100, TotalTokens: 120, Source: "native", Available: true,
			}},
			{Type: provider.EventDelta, Text: "done"}, {Type: provider.EventDone, Reason: "stop"},
		}},
	)
	recorder := &usageEventRecorder{}
	agent, err := runtime.NewAgent(config.AgentConfig{
		Profile:      config.AgentProfile{ID: "adoption", Name: "Adoption Agent"},
		SystemPrompt: "Complete the requested task.",
		Context:      config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}, gateway, runtime.WithAgentManualCompactions(&oneShotCompaction{}), runtime.WithAgentEventSink(recorder))
	if err != nil {
		t.Fatal(err)
	}
	factory := runtime.AgentFactoryFunc(func(context.Context, runtime.AgentRequest) (*runtime.Agent, error) { return agent, nil })

	var firstStartup []runtime.StartupPhase
	firstHost, err := runtime.Open(ctx, runtime.Options{
		Storage: storage.SQLite(databasePath),
		StartupProgress: runtime.StartupProgressFunc(func(phase runtime.StartupPhase) {
			firstStartup = append(firstStartup, phase)
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(firstStartup) != 1 || firstStartup[0] != runtime.StartupPhaseVerifying {
		t.Fatalf("fresh startup phases=%v", firstStartup)
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
			for _, item := range view.Items {
				if item.TurnID == "" || item.RunID == "" {
					t.Fatalf("published item identity=%#v", item)
				}
			}
			_ = runtime.ThreadItem{ID: "adoption-thinking", TurnID: "turn-adoption", RunID: "run-adoption", Ordinal: 1, Kind: runtime.ThreadItemThinking, Live: true}
			_ = runtime.ThreadInteraction{ID: "adoption-input", TurnID: "turn-adoption", RunID: "run-adoption", Kind: runtime.ThreadInteractionInput}
			if got := recorder.snapshot(); len(got) != 1 || got[0] != (runtime.ThreadTokenUsageTotals{
				InputTokens: 80, OutputTokens: 20, CacheReadTokens: 15, CacheWriteTokens: 5,
			}) {
				t.Fatalf("published live usage totals=%#v", got)
			}
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
	maintenance, err := storage.MaintainSQLite(ctx, databasePath, storage.SQLiteMaintenancePolicy{
		MinimumFileBytes: 1 << 40, MinimumReclaimBytes: 1 << 40, MinimumReclaimRatio: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if maintenance.Action != storage.SQLiteMaintenanceActionNone {
		t.Fatalf("unexpected maintenance action: %#v", maintenance)
	}

	var secondStartup []runtime.StartupPhase
	secondHost, err := runtime.Open(ctx, runtime.Options{
		Storage: storage.SQLite(databasePath),
		StartupProgress: runtime.StartupProgressFunc(func(phase runtime.StartupPhase) {
			secondStartup = append(secondStartup, phase)
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(secondStartup) != 1 || secondStartup[0] != runtime.StartupPhaseVerifying {
		t.Fatalf("current startup phases=%v", secondStartup)
	}
	defer func() { _ = secondHost.Shutdown(context.Background()) }()
	secondService, err := secondHost.ThreadService(factory)
	if err != nil {
		t.Fatal(err)
	}
	summaries, err := secondService.List(ctx, runtime.ThreadScope{})
	if err != nil || len(summaries) != 1 || summaries[0].TitleGeneration != 1 || summaries[0].Title != "compact then answer" || summaries[0].TitleStatus != runtime.ThreadTitleStatusReady {
		t.Fatalf("published title snapshot after restart=%#v err=%v", summaries, err)
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
	if snapshot.UsageTotals == nil || *snapshot.UsageTotals != (runtime.ThreadTokenUsageTotals{
		InputTokens: 80, OutputTokens: 20, CacheReadTokens: 15, CacheWriteTokens: 5,
	}) {
		t.Fatalf("published canonical usage totals=%#v", snapshot.UsageTotals)
	}
}

func TestDeepSeekResponsesConstructor(t *testing.T) {
	gateway, err := provider.NewDeepSeek(provider.DeepSeekOptions{
		Model: "deepseek-v4-pro", BaseURL: "https://api.deepseek.com", APIKey: "not-used",
		StateCompatibilityKey: "adoption:deepseek:responses:v1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if gateway.Identity().Provider != "deepseek" {
		t.Fatal("incorrect DeepSeek identity")
	}
	if _, ok := gateway.(provider.RequestPreparer); !ok {
		t.Fatal("DeepSeek must estimate the complete rendered request")
	}
	if _, err := runtime.NewAgent(config.AgentConfig{Profile: config.AgentProfile{ID: "adoption", Name: "Adoption"}, SystemPrompt: "Test.", Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens}}, gateway, runtime.WithAgentHostedTools(provider.HostedToolDefinition{Name: "web_search", Type: "web_search"})); err != nil {
		t.Fatal(err)
	}

}

func TestPublishedTerminalActivityContract(t *testing.T) {
	initial := &tools.ActivityPresentation{Renderer: tools.ActivityRendererTerminal, Label: "Read diagnostic output", Payload: tools.TerminalActivityPayload{Operation: "read", InputBytes: 12, FirstSeq: 1, LastSeq: 2, LatestSeq: 3, HasMore: true, TotalBytes: 128, ExecutionLocation: "local", TimedOut: true}}
	final := tools.MergeActivityPresentations(initial, &tools.ActivityPresentation{Renderer: tools.ActivityRendererTerminal, Payload: tools.TerminalActivityPayload{Operation: "read", FirstSeq: 3, LastSeq: 3, LatestSeq: 3}})
	if err := final.Validate(); err != nil {
		t.Fatal(err)
	}
	got := final.Payload.(tools.TerminalActivityPayload)
	if got.HasMore || got.LastSeq != 3 || got.InputBytes != 12 || !got.TimedOut {
		t.Fatalf("terminal facts: %#v", got)
	}
}

func TestWebSearchActivityPublicFacts(t *testing.T) {
	presentation := &tools.ActivityPresentation{Renderer: tools.ActivityRendererWebSearch, Payload: tools.WebSearchActivityPayload{Operation: "find_in_page", URL: "https://example.com", Pattern: "forecast", ResultsProvided: true, Results: []tools.WebSearchActivityResult{{Title: "Weather", URL: "https://example.com", Snippet: "Sunny"}}}}
	if err := presentation.Validate(); err != nil {
		t.Fatal(err)
	}
	result := provider.HostedToolResult{ResultsProvided: true}
	if !result.ResultsProvided {
		t.Fatal("explicit empty list lost")
	}
}

func TestPublishedStorageMaintenance(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source.sqlite")
	host, err := runtime.Open(t.Context(), runtime.Options{Storage: storage.SQLite(path), DeferExecution: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = host.PrepareRestore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err = host.Activate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err = host.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	inspection, err := runtime.InspectSQLite(t.Context(), path)
	if err != nil || !inspection.Exists || inspection.MigrationRequired {
		t.Fatalf("inspection=%+v error=%v", inspection, err)
	}
	if err = storage.BackupSQLite(t.Context(), path, filepath.Join(t.TempDir(), "snapshot.sqlite")); err != nil {
		t.Fatal(err)
	}
}

func TestDeepSeekVisionAttachmentPreparation(t *testing.T) {
	gateway, err := provider.NewDeepSeek(provider.DeepSeekOptions{
		Model: "deepseek-v4-flash-vision-exp", BaseURL: "https://api.deepseek.com", APIKey: "test", StateCompatibilityKey: "adoption-vision",
		ResolveAttachment: func(context.Context, provider.Attachment) ([]byte, error) { return []byte{1, 2, 3}, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := gateway.(provider.RequestPreparer).Prepare(context.Background(), provider.Request{
		RunID: "run", PromptScopeID: "scope", Messages: []provider.Message{{Role: provider.RoleUser, Attachments: []provider.Attachment{{ResourceRef: "opaque-photo", Name: "photo.png", MIMEType: "image/png", SizeBytes: 3}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	if gateway.Capabilities().AttachmentPayload != provider.AttachmentExpanded || prepared.TokenEstimate().EstimatedInputTokens <= 0 {
		t.Fatal("missing prepared image contract")
	}
}

func TestPublishedGracefulStopPreservesToolOutcome(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	started := make(chan struct{})
	tool := tools.Define[map[string]any](tools.Definition{
		Name: "work", InputSchema: tools.StrictObject(nil, nil), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow},
	}, nil, nil, func(ctx context.Context, inv tools.Invocation[map[string]any]) (tools.Result, error) {
		close(started)
		<-ctx.Done()
		return tools.CanceledResult(inv.CallID, inv.Name, "execution ended"), nil
	})
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "stop", StateCompatibilityKey: "test:stop"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "work", Name: "work", Args: `{}`}}}, {Type: provider.EventDone, Reason: "tool_calls"}}})
	agent, err := runtime.NewAgent(config.AgentConfig{
		Profile: config.AgentProfile{ID: "stop", Name: "Stop"}, SystemPrompt: "Complete the task.", Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}, gateway, runtime.WithAgentTools(tool), runtime.WithAgentEffectAuthorization(runtime.EffectAuthorizationGateFunc(func(ctx context.Context, req runtime.EffectAuthorizationRequest, effect runtime.AuthorizedEffect) (runtime.EffectDispatchResult, error) {
		return effect(ctx, runtime.EffectAuthorizationProof{EffectAttemptID: req.EffectAttemptID, RequestFingerprint: req.RequestFingerprint,
			ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, ToolCallID: req.ToolCallID,
			PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now()})
	})))
	if err != nil {
		t.Fatal(err)
	}
	host, err := runtime.Open(ctx, runtime.Options{Storage: storage.Memory()})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Shutdown(context.Background())
	service, err := host.ThreadService(runtime.AgentFactoryFunc(func(context.Context, runtime.AgentRequest) (*runtime.Agent, error) { return agent, nil }))
	if err != nil {
		t.Fatal(err)
	}
	created, err := service.Create(ctx, runtime.CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(ctx, runtime.SendInput{ThreadID: created.ThreadID, Input: runtime.UserInput{Text: "work"}, RequestKey: "send"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	sub, err := service.Subscribe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	accepted, err := service.Cancel(ctx, runtime.CancelInput{ThreadID: created.ThreadID, RequestKey: "stop", Mode: runtime.CancelModeGraceful})
	if err != nil || accepted.Cancellation == nil || accepted.Cancellation.Source != "user_stop" || accepted.Cancellation.ThreadID != created.ThreadID {
		t.Fatalf("stop acceptance=%#v err=%v", accepted, err)
	}
	for {
		view, err := sub.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if view.Activity != runtime.ThreadActivityIdle {
			continue
		}
		if view.LastOutcome == nil || *view.LastOutcome != runtime.TurnOutcomeCancelled || view.Failure != nil || view.Cancellation == nil {
			t.Fatalf("stop=%#v", view)
		}
		for _, item := range view.Items {
			if item.Activity != nil && item.Activity.ToolID == "work" && item.Activity.Status == observation.ActivityStatusCanceled {
				return
			}
		}
		t.Fatal("confirmed cancellation was lost")
	}
}

func TestPublishedToolInputSurvivesRestartWithoutReplay(t *testing.T) {
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "input", StateCompatibilityKey: "test:input"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "inspect-1", Name: "inspect", Args: `{}`}}}, {Type: provider.EventDone, Reason: "tool_calls"}}},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "resumed"}, {Type: provider.EventDone, Reason: "stop"}}},
	)
	var calls atomic.Int32
	inspect := tools.Define[map[string]any](tools.Definition{Name: "inspect", InputSchema: tools.StrictObject(map[string]any{}, []string{}), ReadOnly: true, Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, nil, nil,
		func(context.Context, tools.Invocation[map[string]any]) (tools.Result, error) {
			calls.Add(1)
			return tools.Result{Text: "Observation complete; user input required before continuing.", InputRequired: &tools.InputRequest{Summary: "Complete the external step", Questions: []tools.InputQuestion{{ID: "ready", Prompt: "Return control when ready", Kind: "select", Options: []string{"Continue", "Stop"}}}}}, nil
		})
	agent, err := runtime.NewAgent(config.AgentConfig{Profile: config.AgentProfile{ID: "input", Name: "Input"}, SystemPrompt: "Complete the requested task.", Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens}}, gateway, runtime.WithAgentTools(inspect), runtime.WithAgentEffectAuthorization(runtime.EffectAuthorizationGateFunc(func(ctx context.Context, req runtime.EffectAuthorizationRequest, effect runtime.AuthorizedEffect) (runtime.EffectDispatchResult, error) {
		return effect(ctx, runtime.EffectAuthorizationProof{EffectAttemptID: req.EffectAttemptID, RequestFingerprint: req.RequestFingerprint, ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, ToolCallID: req.ToolCallID, PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now().UTC()})
	})))
	if err != nil {
		t.Fatal(err)
	}
	path := t.TempDir() + "/input.db"
	open := func() (*runtime.Host, runtime.ThreadService) {
		t.Helper()
		host, err := runtime.Open(t.Context(), runtime.Options{Storage: storage.SQLite(path)})
		if err != nil {
			t.Fatal(err)
		}
		svc, err := host.ThreadService(runtime.AgentFactoryFunc(func(context.Context, runtime.AgentRequest) (*runtime.Agent, error) { return agent, nil }))
		if err != nil {
			t.Fatal(err)
		}
		return host, svc
	}
	host, svc := open()
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	created, err := svc.Create(t.Context(), runtime.CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Send(t.Context(), runtime.SendInput{ThreadID: created.ThreadID, RequestKey: "send", Input: runtime.UserInput{Text: "inspect"}}); err != nil {
		t.Fatal(err)
	}
	waiting := waitPublishedToolInputView(t, svc, created.ThreadID, func(v runtime.ThreadView) bool { return len(v.Interactions) == 1 || v.LastOutcome != nil })
	if len(waiting.Interactions) != 1 || waiting.Interactions[0].Resolved || len(gateway.Requests()) != 1 {
		t.Fatalf("tool did not stop model for input: interactions=%+v requests=%d outcome=%v", waiting.Interactions, len(gateway.Requests()), waiting.LastOutcome)
	}
	interaction := waiting.Interactions[0]
	if interaction.ToolCallID != "inspect-1" || interaction.Input == nil || interaction.Input.Summary != "Complete the external step" {
		t.Fatalf("input lost source identity: %+v", interaction)
	}
	if err := host.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	host, svc = open()
	restored, err := svc.View(t.Context(), created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	if len(restored.Interactions) != 1 || restored.Interactions[0].ID != interaction.ID || restored.Interactions[0].Resolved {
		t.Fatalf("input not restored: %+v", restored.Interactions)
	}
	if _, err := svc.Respond(t.Context(), runtime.RespondInput{ThreadID: created.ThreadID, InteractionID: interaction.ID, RequestKey: "respond", Answers: []runtime.InteractionAnswer{{Input: map[string]string{"ready": "Continue"}}}}); err != nil {
		t.Fatal(err)
	}
	final := waitPublishedToolInputView(t, svc, created.ThreadID, func(v runtime.ThreadView) bool {
		return v.Activity == runtime.ThreadActivityIdle && v.LastOutcome != nil
	})
	if final.Failure != nil || *final.LastOutcome != runtime.TurnOutcomeCompleted || calls.Load() != 1 {
		t.Fatalf("resume failed or replayed tool: failure=%+v outcome=%v calls=%d", final.Failure, final.LastOutcome, calls.Load())
	}
	if final.TurnID != waiting.TurnID {
		t.Fatal("input response created another turn")
	}
	requests := gateway.Requests()
	if len(requests) != 2 {
		t.Fatalf("requests=%d", len(requests))
	}
	var callCount, resultCount int
	for _, msg := range requests[1].Messages {
		for _, call := range msg.ToolCalls {
			if call.ID == "inspect-1" {
				callCount++
			}
		}
		if msg.ToolResult != nil && msg.ToolResult.CallID == "inspect-1" {
			resultCount++
		}
	}

	if callCount != 1 || resultCount != 1 {
		t.Fatalf("tool history is unpaired: calls=%d results=%d", callCount, resultCount)
	}
}

func waitPublishedToolInputView(t *testing.T, service runtime.ThreadService, threadID identity.ThreadID, ready func(runtime.ThreadView) bool) runtime.ThreadView {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		view, err := service.View(t.Context(), threadID)
		if err != nil {
			t.Fatal(err)
		}
		if ready(view) {
			return view
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("published tool input view did not converge")
	return runtime.ThreadView{}
}

func TestPublishedStructuredInputAndEmptyOutput(t *testing.T) {
	call := &tools.ActivityPresentation{Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Inputs: []tools.StructuredActivityRow{{Content: "log('page');", Format: tools.StructuredActivityRowFormatCode, Language: "javascript"}}}}
	result := tools.MergeActivityPresentations(call, &tools.ActivityPresentation{Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{RowsProvided: true}})
	if err := result.Validate(); err != nil {
		t.Fatal(err)
	}
	payload := result.Payload.(tools.StructuredActivityPayload)
	if !payload.RowsProvided || len(payload.Rows) != 0 || len(payload.Inputs) != 1 || payload.Inputs[0].Truncated {
		t.Fatal("published activity input/output contract changed")
	}
}

func TestPublishedOfflineRequestEstimate(t *testing.T) {
	estimate, err := provider.EstimateRenderedRequest(provider.RenderedRequest{Provider: "openai", Model: "gpt-4o", Format: provider.RequestFormatOpenAIChat, Payload: []byte(`{"messages":[{"role":"user","content":"Check this request."}]}`)})
	if err != nil || estimate.EstimatedInputTokens <= 0 || estimate.Source != "rendered_o200k_base_media_v1" {
		t.Fatalf("offline estimate: %+v %v", estimate, err)
	}
}

func TestLiveProviderSurfaceOptIn(t *testing.T) {
	surface := runtime.ToolSurface{RefreshProviderSurface: true}
	if !surface.RefreshProviderSurface {
		t.Fatal("live surface option lost")
	}
}

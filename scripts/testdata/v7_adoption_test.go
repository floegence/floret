// Package adoption_test exercises the released v7 API from a blank module.
package adoption_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
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
	"github.com/floegence/floret/v7/threadquery"
	"github.com/floegence/floret/v7/tools"
)

type oneShotCompaction struct {
	consumed atomic.Bool
}

func TestPublishedCompleteQueueEditSurvivesSQLiteRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.db")
	open := func() (*runtime.Host, runtime.ThreadService) {
		t.Helper()
		host, err := runtime.Open(t.Context(), runtime.Options{Storage: storage.SQLite(path)})
		if err != nil {
			t.Fatal(err)
		}
		service, err := host.ThreadService(runtime.AgentFactoryFunc(func(context.Context, runtime.AgentRequest) (*runtime.Agent, error) {
			return nil, errors.New("execution not expected")
		}))
		if err != nil {
			t.Fatal(err)
		}
		return host, service
	}
	host, service := open()
	created, err := service.Create(t.Context(), runtime.CreateThreadInput{RequestKey: "create-queue-edit"})
	if err != nil {
		t.Fatal(err)
	}
	original := runtime.UserInput{Text: "original", Attachments: []runtime.MessageAttachment{{ResourceRef: "host:old", Name: "old.png", MIMEType: "image/png"}}}
	if _, err := service.ImportPendingInputs(t.Context(), runtime.ImportPendingInputsInput{ThreadID: created.ThreadID, Items: []runtime.ImportedPendingInput{{RequestKey: "queued", Input: original}}}); err != nil {
		t.Fatal(err)
	}
	replacement := runtime.UserInput{References: []runtime.MessageReference{{ReferenceID: "text", Kind: runtime.MessageReferenceText, Label: "Snapshot", Text: "replacement"}}, Attachments: []runtime.MessageAttachment{{ResourceRef: "host:new", Name: "new.png", MIMEType: "image/png"}}}
	command := runtime.EditQueuedInput{ThreadID: created.ThreadID, QueueItemID: "queue:queued", ExpectedInput: &original, Input: &replacement, RequestKey: "edit"}
	if _, err := service.(runtime.ThreadQueueController).EditQueued(t.Context(), command); err != nil {
		t.Fatal(err)
	}
	if err := host.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	host, service = open()
	defer host.Shutdown(context.Background())
	view, err := service.View(t.Context(), created.ThreadID)
	if err != nil || len(view.Queue) != 1 || !reflect.DeepEqual(view.Queue[0].Input, replacement) {
		t.Fatalf("queue=%#v err=%v", view.Queue, err)
	}
	if _, err := service.(runtime.ThreadQueueController).EditQueued(t.Context(), command); err != nil {
		t.Fatalf("replay=%v", err)
	}
	edit, found, err := service.(runtime.ThreadQueueEditReader).LookupQueuedEdit(t.Context(), runtime.LookupQueuedEditInput{ThreadID: created.ThreadID, RequestKey: "edit"})
	if err != nil || !found || !reflect.DeepEqual(edit.Input, replacement) || !reflect.DeepEqual(edit.ExpectedInput, original) {
		t.Fatalf("edit lookup=%#v %v %v", edit, found, err)
	}
	command.RequestKey = "stale"
	if _, err := service.(runtime.ThreadQueueController).EditQueued(t.Context(), command); !errors.Is(err, runtime.ErrRequestConflict) {
		t.Fatalf("stale=%v", err)
	}
	inventory, err := host.MediaInventory(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	refs := map[string]bool{}
	for _, ref := range inventory.References {
		refs[ref.Attachment.ResourceRef] = true
	}
	if !refs["host:old"] || !refs["host:new"] {
		t.Fatalf("missing durable media: %#v", refs)
	}
	input, found, err := service.(runtime.ThreadSendReader).LookupSend(t.Context(), runtime.LookupSendInput{ThreadID: created.ThreadID, RequestKey: "queued"})
	if err != nil || !found || !reflect.DeepEqual(input, original) {
		t.Fatalf("original send=%#v %v %v", input, found, err)
	}
	raw, _ := json.Marshal(replacement)
	digest := sha256.Sum256(raw)
	removal := runtime.DeleteQueuedInput{ThreadID: created.ThreadID, QueueItemID: "queue:queued", RequestKey: "take-back", ExpectedInputSHA256: fmt.Sprintf("%x", digest)}
	if view, err := service.DeleteQueued(t.Context(), removal); err != nil || len(view.Queue) != 0 {
		t.Fatalf("take-back=%#v %v", view.Queue, err)
	}
	if _, err := service.DeleteQueued(t.Context(), removal); err != nil {
		t.Fatalf("take-back replay=%v", err)
	}

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

func TestThreadReferenceDirectValidation(t *testing.T) {
	reference := runtime.MessageReference{ReferenceID: "source", Kind: runtime.MessageReferenceThread, ReferencedThreadID: "source-thread", Label: "Source", Text: "Captured facts"}
	if err := reference.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestPublishedThreadContextReaderSurvivesSQLiteRestart(t *testing.T) {
	if err := (provider.Capabilities{Reasoning: provider.ReasoningUnsupported, ReasoningHistory: provider.ReasoningHistoryCurrentUser}).Validate(); err != nil {
		t.Fatal(err)
	}
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
	resultReader, ok := firstService.(runtime.ThreadToolResultReader)
	if !ok {
		t.Fatal("published thread service is missing canonical tool result lookup")
	}
	media, err := firstHost.MediaInventory(ctx)
	if err != nil || !media.Idle || len(media.References) != 0 {
		t.Fatalf("fresh canonical media inventory=%+v, error=%v", media, err)
	}
	created, err := firstService.Create(ctx, runtime.CreateThreadInput{RequestKey: "create-adoption"})
	if err != nil {
		t.Fatal(err)
	}
	queryReader, ok := firstService.(runtime.ThreadQueryReader)
	if !ok {
		t.Fatal("published service is missing canonical query capability")
	}
	queryScope := threadquery.Scope{Authority: "adoption-user-runtime", ThreadIDs: []identity.ThreadID{created.ThreadID}}
	query, err := queryReader.Query(ctx, queryScope, threadquery.QueryInput{Object: threadquery.Threads})
	if err != nil || query.Total != 1 {
		t.Fatalf("published index=%#v error=%v", query, err)
	}
	if _, err := queryReader.Read(ctx, queryScope, threadquery.ReadInput{Ref: query.Records[0].Ref}); err != nil {
		t.Fatal(err)
	}
	count, err := queryReader.Aggregate(ctx, queryScope, threadquery.AggregateInput{Object: threadquery.Threads})
	if err != nil || count.Total != 1 {
		t.Fatalf("published count=%#v error=%v", count, err)
	}
	if _, found, err := resultReader.LookupToolResult(ctx, runtime.LookupToolResultInput{ThreadID: created.ThreadID, TurnID: "missing-turn", RunID: "missing-run", ToolCallID: "missing-call"}); err != nil || found {
		t.Fatalf("absent published tool result found=%t error=%v", found, err)
	}
	if _, err := firstService.Send(ctx, runtime.SendInput{ThreadID: created.ThreadID, Input: runtime.UserInput{Text: "compact then answer", References: []runtime.MessageReference{{ReferenceID: "source", Kind: runtime.MessageReferenceThread, ReferencedThreadID: "referenced-thread", Label: "Frozen title", Text: "Captured thread metadata"}}}, RequestKey: "send-adoption"}); err != nil {
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
	sendReader, ok := secondService.(runtime.ThreadSendReader)
	if !ok {
		t.Fatal("published ThreadService does not expose ThreadSendReader")
	}
	input, found, err := sendReader.LookupSend(ctx, runtime.LookupSendInput{ThreadID: created.ThreadID, RequestKey: "send-adoption"})
	if err != nil || !found || input.Text != "compact then answer" {
		t.Fatalf("published original input after restart: %#v %v %v", input, found, err)
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
	for _, model := range []string{"deepseek-flash", "deepseek-v4-pro", "deepseek-v4-flash", "deepseek-v4-flash-vision-exp"} {
		t.Run(model, func(t *testing.T) {
			gateway, err := provider.NewDeepSeek(provider.DeepSeekOptions{
				Model: model, BaseURL: "https://api.deepseek.com", APIKey: "not-used",
				StateCompatibilityKey: "adoption:deepseek:responses:v1",
			})
			if err != nil {
				t.Fatal(err)
			}
			if gateway.Identity().Provider != "deepseek" || gateway.Identity().Model != model {
				t.Fatal("incorrect DeepSeek identity")
			}
			if _, ok := gateway.(provider.RequestPreparer); !ok {
				t.Fatal("DeepSeek must estimate the complete rendered request")
			}
			if _, err := runtime.NewAgent(config.AgentConfig{Profile: config.AgentProfile{ID: "adoption", Name: "Adoption"}, SystemPrompt: "Test.", Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens}}, gateway, runtime.WithAgentHostedTools(provider.HostedToolDefinition{Name: "web_search", Type: "web_search"})); err != nil {
				t.Fatal(err)
			}

		})
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
	accepted, err := service.Cancel(ctx, runtime.CancelInput{ThreadID: created.ThreadID, RequestKey: "stop", Mode: runtime.CancelModeGraceful, IncludeDescendants: true})
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
	if len(final.Turns) != 1 || !final.Turns[0].FinalBoundaryComplete || len(final.Turns[0].FinalItemIDs) != 1 || final.Turns[0].RunID != final.RunID || final.Turns[0].Outcome == nil || *final.Turns[0].Outcome != runtime.TurnOutcomeCompleted {
		t.Fatalf("accepted final presentation missing: %+v", final.Turns)
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

func TestInitialProviderSurfaceContract(t *testing.T) {
	request := runtime.ToolSurfaceRequest{InitialProviderSurface: &runtime.ProviderToolSurface{
		SystemPrompt: "original", ToolDefinitions: []tools.ToolDefinition{{Name: "read"}},
		HostedToolDefinitions: []provider.HostedToolDefinition{{Name: "search", Type: "web_search"}},
	}}
	if request.InitialProviderSurface.SystemPrompt != "original" {
		t.Fatal("checkpoint surface lost")
	}
}

func TestPublishedIndependentContextCompaction(t *testing.T) {
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "adoption", Model: "compact", StateCompatibilityKey: "adoption:compact"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported})
	agent, err := runtime.NewAgent(config.AgentConfig{Profile: config.AgentProfile{ID: "compact", Name: "Compact"}, SystemPrompt: "Follow the task.", Context: config.ContextPolicy{ContextWindowTokens: 256000}}, gateway)
	if err != nil {
		t.Fatal(err)
	}
	host, err := runtime.Open(t.Context(), runtime.Options{Storage: storage.Memory()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	service, err := host.ThreadService(runtime.AgentFactoryFunc(func(_ context.Context, request runtime.AgentRequest) (*runtime.Agent, error) {
		if request.TurnKind != runtime.TurnKindContextCompaction || request.Input.Text != "" {
			t.Error("invalid context-only factory request")
		}
		return agent, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	thread, err := service.Create(t.Context(), runtime.CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	compactor, ok := service.(runtime.ThreadContextCompactor)
	if !ok {
		t.Fatal("missing optional compaction contract")
	}
	admitted, err := compactor.CompactContext(t.Context(), runtime.CompactContextInput{ThreadID: thread.ThreadID, RequestKey: "compact"})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		view, err := service.View(t.Context(), thread.ThreadID)
		if err != nil {
			t.Fatal(err)
		}
		if view.Activity == runtime.ThreadActivityIdle && view.LastOutcome != nil {
			if view.TurnKind != runtime.TurnKindContextCompaction || *view.LastOutcome != runtime.TurnOutcomeCompleted || len(view.Items) != 0 || len(gateway.Requests()) != 0 {
				t.Fatalf("independent compaction=%+v", view)
			}
			snapshot, err := service.(runtime.ThreadContextReader).Context(t.Context(), thread.ThreadID)
			if err != nil {
				t.Fatal(err)
			}
			if len(snapshot.Compactions) != 1 || snapshot.Compactions[0].Status != "noop" || snapshot.Compactions[0].AfterItemID != "" {
				t.Fatalf("snapshot=%+v", snapshot)
			}
			replay, err := compactor.CompactContext(t.Context(), runtime.CompactContextInput{ThreadID: thread.ThreadID, RequestKey: "compact"})
			if err != nil || replay.TurnID != admitted.TurnID {
				t.Fatalf("replay=%+v err=%v", replay, err)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("compaction did not complete")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestPublishedNativeHistorySurvivesStopAndSQLiteRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "native", StateCompatibilityKey: "account:model"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "Completed prefix"}, {Type: provider.EventDone, Reason: "length", ResponseState: &provider.State{Kind: "native", ID: "completed", ReusePolicy: provider.StateReuseCanonicalHistory}}}},
		florettest.Step{WaitForCancellation: true},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "Continued"}, {Type: provider.EventDone, Reason: "stop"}}})
	path := filepath.Join(t.TempDir(), "native.db")
	open := func() (*runtime.Host, runtime.ThreadService) {
		host, err := runtime.Open(ctx, runtime.Options{Storage: storage.SQLite(path)})
		if err != nil {
			t.Fatal(err)
		}
		service, err := host.ThreadService(runtime.AgentFactoryFunc(func(context.Context, runtime.AgentRequest) (*runtime.Agent, error) {
			return runtime.NewAgent(config.AgentConfig{Profile: config.AgentProfile{ID: "native", Name: "Native"}, SystemPrompt: "Test.", Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens}}, gateway)
		}))
		if err != nil {
			t.Fatal(err)
		}
		return host, service
	}
	host, service := open()
	created, err := service.Create(ctx, runtime.CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(ctx, runtime.SendInput{ThreadID: created.ThreadID, RequestKey: "send", Input: runtime.UserInput{Text: "inspect"}}); err != nil {
		t.Fatal(err)
	}
	if err := gateway.WaitForRequests(ctx, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Cancel(ctx, runtime.CancelInput{ThreadID: created.ThreadID, RequestKey: "stop", Mode: runtime.CancelModeImmediate}); err != nil {
		t.Fatal(err)
	}
	waitPublishedToolInputView(t, service, created.ThreadID, func(v runtime.ThreadView) bool { return v.Activity == runtime.ThreadActivityIdle })
	if err := host.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	host, service = open()
	defer host.Shutdown(context.Background())
	if _, err := service.Send(ctx, runtime.SendInput{ThreadID: created.ThreadID, RequestKey: "probe", Input: runtime.UserInput{Text: "continue"}}); err != nil {
		t.Fatal(err)
	}
	if err := gateway.WaitForRequests(ctx, 3); err != nil {
		t.Fatal(err)
	}
	state := gateway.Requests()[2].PreviousState
	if state == nil || state.ID != "completed" || state.ReusePolicy != provider.StateReuseCanonicalHistory {
		t.Fatalf("native history=%+v", state)
	}
}

func TestPublishedSteeringCapability(t *testing.T) {
	host, err := runtime.Open(t.Context(), runtime.Options{Storage: storage.Memory()})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Shutdown(context.Background())
	service, err := host.ThreadService(runtime.AgentFactoryFunc(func(context.Context, runtime.AgentRequest) (*runtime.Agent, error) {
		return nil, errors.New("execution not expected")
	}))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := service.(runtime.ThreadSteeringController); !ok {
		t.Fatal("published steering capability is missing")
	}
	_ = runtime.SteerInput{Input: runtime.UserInput{Text: "guide"}, RequestKey: "guide"}
	_ = runtime.SteerQueuedInput{ExpectedInput: runtime.UserInput{Text: "guide"}, RequestKey: "guide"}
	if runtime.ThreadSteeringPending != "pending" || runtime.ThreadSteeringApplied != "applied" || runtime.ThreadSteeringCancelled != "cancelled" {
		t.Fatal("unstable steering status")
	}
}

func TestPublishedEffectiveReusePolicy(t *testing.T) {
	policy := provider.RequestReusePolicy{PromptCacheEnabled: true, PromptCacheNamespace: "scope", ProviderStateAllowed: true, PreferContinuation: true}
	if err := policy.Validate(); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(provider.Request{RunID: identity.RunID("run"), PromptScopeID: identity.PromptScopeID("scope"), ReusePolicy: &policy})
	if err != nil {
		t.Fatal(err)
	}
	var restored provider.Request
	if err = json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	if restored.ReusePolicy == nil || *restored.ReusePolicy != policy {
		t.Fatalf("policy lost: %s", raw)
	}
}

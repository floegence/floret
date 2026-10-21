// Package adoption_test exercises the released v7 API from a blank module.
package adoption_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
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
	"github.com/floegence/floret/v7/tools/webfetch"
	"github.com/floegence/floret/v7/tools/websearch"
)

func TestPublishedNativeThreadUpdateSource(t *testing.T) {
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
	source, ok := service.(runtime.ThreadUpdateSource)
	if !ok {
		t.Fatal("released service lacks native thread updates")
	}
	subscription, err := source.SubscribeUpdates(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	factsSource, ok := service.(runtime.ThreadFactsSource)
	if !ok {
		t.Fatal("released service lacks lightweight runtime facts")
	}
	facts, err := factsSource.SubscribeFacts(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer facts.Close()
	view, err := service.Create(t.Context(), runtime.CreateThreadInput{RequestKey: "native-create"})
	if err != nil {
		t.Fatal(err)
	}
	changed, err := service.ImportPendingInputs(t.Context(), runtime.ImportPendingInputsInput{ThreadID: view.ThreadID, Items: []runtime.ImportedPendingInput{{RequestKey: "native-queue", Input: runtime.UserInput{Text: "queued"}}}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	for {
		update, err := subscription.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if update.ViewVersion <= view.ViewVersion {
			continue
		}
		if update.BaseVersion != view.ViewVersion || update.ViewVersion != changed.View.ViewVersion || update.Queue == nil || len(update.Queue.Value) != 1 || update.Facts.QueueCount != 1 {
			t.Fatalf("invalid released mutation: %+v", update)
		}
		break
	}
	for {
		update, err := facts.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if update.ViewVersion <= view.ViewVersion {
			continue
		}
		if update.Facts.QueueCount != 1 || update.Items != nil || update.Queue != nil || update.Turns != nil || update.Window != nil {
			t.Fatalf("invalid facts projection: %+v", update)
		}
		break
	}

}

func TestPublishedToolSchemaOrder(t *testing.T) {
	schema := map[string]any{"type": "object", "properties": map[string]any{"content": tools.String("body"), "file_path": tools.String("path")}, "required": []string{"file_path", "content"}}
	order, err := tools.InputPropertyOrder(schema)
	if err != nil || !reflect.DeepEqual(order, []string{"file_path", "content"}) {
		t.Fatalf("order=%v err=%v", order, err)
	}
	raw, err := json.Marshal(map[string]any{"tools": []any{map[string]any{"type": "function", "parameters": schema}}})
	if err != nil {
		t.Fatal(err)
	}
	ordered, err := provider.OrderToolSchemas(provider.RequestFormatOpenAIResponses, raw)
	if err != nil || !strings.Contains(string(ordered), `"properties":{"file_path":`) {
		t.Fatalf("ordered=%s err=%v", ordered, err)
	}
}

func TestPublishedSubAgentStatusActivity(t *testing.T) {
	presentation := tools.ActivityPresentation{Renderer: tools.ActivityRendererSubAgentOperation, Payload: tools.SubAgentOperationActivityPayload{Action: tools.SubAgentOperationStatus, Status: "success", RequestedCount: 1, Targets: []tools.SubAgentOperationTarget{{ThreadID: "child", Status: "running"}}}}
	data, err := json.Marshal(presentation)
	if err != nil {
		t.Fatal(err)
	}
	var decoded tools.ActivityPresentation
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	payload, ok := decoded.Payload.(tools.SubAgentOperationActivityPayload)
	if !ok || payload.Action != tools.SubAgentOperationStatus || payload.Targets[0].Status != "running" {
		t.Fatalf("status activity = %#v", decoded)
	}
}

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
	original := runtime.UserInput{ModelContextSnapshot: &runtime.ModelContextSnapshot{Sections: []runtime.ModelContextSection{{Key: "date", Title: "Date", Text: "2026-10-15"}}}, Text: "original", Attachments: []runtime.MessageAttachment{{ResourceRef: "host:old", Name: "old.png", MIMEType: "image/png"}}}
	if _, err := service.ImportPendingInputs(t.Context(), runtime.ImportPendingInputsInput{ThreadID: created.ThreadID, Items: []runtime.ImportedPendingInput{{RequestKey: "queued", Input: original}}}); err != nil {
		t.Fatal(err)
	}
	replacement := runtime.UserInput{ModelContextSnapshot: &runtime.ModelContextSnapshot{Sections: []runtime.ModelContextSection{{Key: "date", Title: "Date", Text: "2026-10-16"}}}, References: []runtime.MessageReference{{ReferenceID: "text", Kind: runtime.MessageReferenceText, Label: "Snapshot", Text: "replacement"}}, Attachments: []runtime.MessageAttachment{{ResourceRef: "host:new", Name: "new.png", MIMEType: "image/png"}}}
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
	if err != nil || estimate.EstimatedInputTokens <= 0 || estimate.Source != "rendered_o200k_base_content_v5" {
		t.Fatalf("offline estimate: %+v %v", estimate, err)
	}
}

func TestPublishedOpaqueReasoningEstimate(t *testing.T) {
	var baseline int64
	for i, size := range []int{0, 285984} {
		raw, err := json.Marshal(map[string]any{"input": []any{map[string]any{"type": "reasoning", "encrypted_content": strings.Repeat("A", size)}, map[string]any{"role": "user", "content": "Continue."}}})
		if err != nil {
			t.Fatal(err)
		}
		estimate, err := provider.EstimateRenderedRequest(provider.RenderedRequest{Provider: "openai", Model: "gpt-6.1-sol", Format: provider.RequestFormatOpenAIResponses, Payload: raw})
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			baseline = estimate.EstimatedInputTokens
		}
		if estimate.EstimatedInputTokens != baseline || estimate.Breakdown.OpaqueBytes != int64(size) || estimate.Breakdown.AttributionCoverage != "partial" {
			t.Fatalf("ciphertext changed token budget or hid uncertainty: %+v", estimate)
		}
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

func TestPublishedProviderRetryContracts(t *testing.T) {
	cause := &provider.ProviderHTTPError{StatusCode: 500, Code: "server_error"}
	advice := provider.HTTPRetryAdvice(500, cause.Code, nil, time.Now())
	wrapped := provider.WithRetryAdvice(cause, advice)
	var retry provider.RetryAdviser
	if !errors.Is(wrapped, cause) || !errors.As(wrapped, &retry) || retry.ProviderRetryAdvice().Category != provider.RetryTransient {
		t.Fatal("retry advice or cause lost")
	}
	policy := runtime.DefaultProviderRetryPolicy()
	if policy.MaxRetries != 4 || policy.MaxTimeoutRetries != 3 || policy.Validate() != nil {
		t.Fatal("invalid defaults")
	}
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "retry", StateCompatibilityKey: "retry:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported})
	_, err := runtime.NewAgent(config.AgentConfig{Profile: config.AgentProfile{ID: "retry", Name: "Retry"}, SystemPrompt: "Test", Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens}}, gateway, runtime.WithAgentProviderRetryPolicy(policy), runtime.WithAgentHostedTools(provider.HostedToolDefinition{Name: "search", Type: "web_search", ReplaySafe: true}))
	if err != nil {
		t.Fatal(err)
	}
	_ = runtime.ThreadRunProgress{Phase: runtime.ThreadRunPhaseRetrying, Retry: &runtime.ProviderRetryProgress{Retry: 1, MaxRetries: 4, Reason: string(provider.RetryTransient), NextAttemptAt: time.Now()}}
}

func TestPublishedPhysicalRequestTimeoutContract(t *testing.T) {
	policy := runtime.DefaultProviderRequestTimeoutPolicy()
	if err := policy.Validate(); err != nil {
		t.Fatal(err)
	}
	if policy.FirstOutputTimeout != 5*time.Minute || policy.IdleTimeout != 2*time.Minute || policy.HostedToolTimeout != 5*time.Minute || policy.MaxAttemptDuration != 15*time.Minute {
		t.Fatalf("defaults=%+v", policy)
	}
	now := time.Now()
	details := provider.RequestTimeoutDetails{Stage: "first_output", LogicalRequestID: "logical", AttemptID: "attempt", AttemptEpoch: 1, StartedAt: now, TimedOutAt: now.Add(5 * time.Minute)}
	if err := details.Validate(); err != nil {
		t.Fatal(err)
	}
	failure := &provider.RequestTimeoutError{Details: details}
	if !errors.Is(failure, provider.ErrRequestTimeout) || !errors.Is(failure, context.DeadlineExceeded) {
		t.Fatal("timeout classification")
	}
	if err := (runtime.ThreadTurnFailure{Code: runtime.ThreadTurnFailureProvider, Message: failure.Error(), ProviderTimeout: &details}).Validate(); err != nil {
		t.Fatal(err)
	}
	_ = runtime.WithAgentProviderRequestTimeoutPolicy(policy)
	_ = runtime.ProviderRequestProgress{LastProgressAt: &now}
}

func TestPublishedInputEvidenceContract(t *testing.T) {
	input := runtime.InputPresentation{ReasonCode: "missing_external_input", RequiredFromUser: []string{"ready"}, EvidenceRefs: []string{"inspect-1"}}
	encoded, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	var decoded runtime.InputPresentation
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.ReasonCode != input.ReasonCode || len(decoded.RequiredFromUser) != 1 || decoded.RequiredFromUser[0] != "ready" || len(decoded.EvidenceRefs) != 1 || decoded.EvidenceRefs[0] != "inspect-1" {
		t.Fatalf("input evidence lost: %s", encoded)
	}
}

func TestPublishedContextCompositionContract(t *testing.T) {
	var comparable = map[config.RequestEstimate]bool{}
	sample := config.RequestEstimate{Breakdown: config.ContextBreakdown{Source: "semantic_input_bpe_v1", ToolResultTokens: 1234}}
	comparable[sample] = true
	raw, err := json.Marshal(sample)
	if err != nil {
		t.Fatal(err)
	}
	var restored config.RequestEstimate
	if err := json.Unmarshal(raw, &restored); err != nil || !comparable[restored] {
		t.Fatalf("composition roundtrip: %+v, %v", restored, err)
	}
	measured, err := provider.EstimateRenderedRequest(provider.RenderedRequest{Provider: "openai", Model: "gpt-4o", Format: provider.RequestFormatOpenAIResponses, Payload: []byte("{\"input\":[{\"role\":\"user\",\"content\":\"Hi\"}]}")})
	if err != nil || measured.AttachmentTokens != 0 {
		t.Fatalf("rendered composition=%+v, %v", measured, err)
	}
}

func TestPublishedContextAttributionContract(t *testing.T) {
	sample := config.ContextBreakdown{Source: "semantic_input_bpe_v2", ToolDefinitionTokens: 10, Details: &config.ContextBreakdownDetails{Items: []config.ContextTokenDetail{{Category: "tool_definition", Key: "docs.read", Source: "mcp", Namespace: "docs", Tokens: 10}}}}
	raw, err := json.Marshal(sample)
	if err != nil {
		t.Fatal(err)
	}
	var restored config.ContextBreakdown
	if err := json.Unmarshal(raw, &restored); err != nil || !reflect.DeepEqual(sample, restored) {
		t.Fatalf("attribution roundtrip=%+v err=%v", restored, err)
	}
	status := observation.ContextStatus{Phase: observation.ContextPhaseProviderUsage, Status: observation.ContextStatusStable, RequestEstimate: config.RequestEstimate{Breakdown: restored}}
	if err := status.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestPublishedPreparedContentDistribution(t *testing.T) {
	raw := []byte(`{"input":[{"role":"user","content":"skills rules"}]}`)
	attrs := []provider.RenderedContentAttribution{{Path: "/input/0/content", Category: "model_context", Parts: []provider.ContentAttributionPart{{Start: 0, End: 12, Key: "skills"}}}}
	got, err := provider.EstimateRenderedRequest(provider.RenderedRequest{Provider: "deepseek", Model: "deepseek-flash", Format: provider.RequestFormatOpenAIResponses, Payload: raw, Attribution: attrs})
	if err != nil || got.Breakdown.Quality != "token_estimate" || got.Breakdown.Tokenizer != "deepseek_v4" || got.Breakdown.ModelContextTokens <= 0 || got.Breakdown.UserTokens != 0 {
		t.Fatalf("distribution=%+v err=%v", got, err)
	}
	attrs[0].Path = "/missing"
	omitted, err := provider.EstimateRenderedRequest(provider.RenderedRequest{Provider: "deepseek", Model: "deepseek-flash", Format: provider.RequestFormatOpenAIResponses, Payload: raw, Attribution: attrs})
	if err != nil || omitted.EstimatedInputTokens != got.EstimatedInputTokens || omitted.Breakdown.Source != "" || omitted.Breakdown.Diagnostic != "attribution_mismatch" {
		t.Fatalf("optional attribution blocked budget: %+v %v", omitted, err)
	}
}

func TestPublishedParentEventsAuthorizedReportAndPassiveDelivery(t *testing.T) {
	host, err := runtime.Open(t.Context(), runtime.Options{Storage: storage.SQLite(filepath.Join(t.TempDir(), "parent-events.db")), EnableParentEvents: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	var service runtime.ThreadService
	report := tools.Define[struct{ Message string }](tools.Definition{Name: "report_to_parent", ReadOnly: true, InputSchema: tools.StrictObject(map[string]any{"message": map[string]any{"type": "string"}}, []string{"message"}), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, nil, nil, func(ctx context.Context, in tools.Invocation[struct{ Message string }]) (tools.Result, error) {
		sum := sha256.Sum256([]byte(strings.TrimSpace(in.RawArgs)))
		receipt, err := service.(runtime.ThreadParentEventService).ReportToParent(ctx, runtime.ReportToParentInput{ThreadID: in.ThreadID, TurnID: in.TurnID, RunID: in.RunID, ToolCallID: in.CallID, ToolName: in.Name, ArgumentHash: fmt.Sprintf("%x", sum), RequestKey: runtime.RequestKey(in.CallID), Message: in.Args.Message})
		data, _ := json.Marshal(receipt)
		return tools.Result{Text: string(data)}, err
	})
	done := florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "done"}, {Type: provider.EventDone, Reason: "stop"}}}
	childGateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "events", StateCompatibilityKey: "test:events"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "report", Name: "report_to_parent", Args: "{\"message\":\"Important finding\"}"}}}, {Type: provider.EventDone, Reason: "tool_calls"}}}, done)
	parentGateway := florettest.NewScriptedGateway(childGateway.Identity(), childGateway.Capabilities(), done, done)
	config := config.AgentConfig{Profile: config.AgentProfile{ID: "events", Name: "Events"}, SystemPrompt: "Review work.", Context: config.ContextPolicy{ContextWindowTokens: 256000}}
	parentAgent, err := runtime.NewAgent(config, parentGateway)
	if err != nil {
		t.Fatal(err)
	}
	childAgent, err := runtime.NewAgent(config, childGateway, runtime.WithAgentTools(report), runtime.WithAgentEffectAuthorization(runtime.EffectAuthorizationGateFunc(func(ctx context.Context, req runtime.EffectAuthorizationRequest, effect runtime.AuthorizedEffect) (runtime.EffectDispatchResult, error) {
		return effect(ctx, runtime.EffectAuthorizationProof{EffectAttemptID: req.EffectAttemptID, RequestFingerprint: req.RequestFingerprint, ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, ToolCallID: req.ToolCallID, PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now()})
	})))
	if err != nil {
		t.Fatal(err)
	}
	var parentID identity.ThreadID
	service, err = host.ThreadService(runtime.AgentFactoryFunc(func(_ context.Context, r runtime.AgentRequest) (*runtime.Agent, error) {
		if r.ThreadID == parentID {
			return parentAgent, nil
		}
		return childAgent, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	parent, err := service.Create(t.Context(), runtime.CreateThreadInput{RequestKey: "parent"})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(parent)
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &wire); err != nil {
		t.Fatal(err)
	}
	var emptyPage map[string]json.RawMessage
	if err := json.Unmarshal(wire["parent_events"], &emptyPage); err != nil {
		t.Fatal(err)
	}
	if string(emptyPage["events"]) != "[]" || string(emptyPage["thread_id"]) != strconv.Quote(parent.ThreadID.String()) {
		t.Fatalf("public empty events=%s, want []", emptyPage["events"])
	}
	parentID = parent.ThreadID
	if _, err := service.Send(t.Context(), runtime.SendInput{ThreadID: parentID, RequestKey: "initial", Input: runtime.UserInput{Text: "start"}}); err != nil {
		t.Fatal(err)
	}
	initial := waitPublishedToolInputView(t, service, parentID, func(v runtime.ThreadView) bool {
		return v.Activity == runtime.ThreadActivityIdle && v.LastOutcome != nil
	})
	child, err := service.Create(t.Context(), runtime.CreateThreadInput{ParentThreadID: parentID, TaskName: "research", HostProfileRef: "test", RequestKey: "child"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(t.Context(), runtime.SendInput{ThreadID: child.ThreadID, RequestKey: "work", Input: runtime.UserInput{Text: "research"}}); err != nil {
		t.Fatal(err)
	}
	final := waitPublishedToolInputView(t, service, child.ThreadID, func(v runtime.ThreadView) bool {
		return v.Activity == runtime.ThreadActivityIdle && v.LastOutcome != nil
	})
	if final.Failure != nil {
		t.Fatal(final.Failure)
	}
	events := service.(runtime.ThreadParentEventService)
	page, err := events.ListParentEvents(t.Context(), runtime.ListParentEventsInput{ThreadID: parentID, PendingOnly: true})
	if err != nil || page.ThreadID != parentID || len(page.Events) != 2 {
		t.Fatalf("events=%+v %v", page, err)
	}
	for _, event := range page.Events {
		if event.CanonicalOrdinal == 0 {
			t.Fatal("parent event has no canonical position")
		}
		if event.ParentTurnID != initial.TurnID {
			t.Fatalf("late event owner=%s want=%s", event.ParentTurnID, initial.TurnID)
		}
	}
	full, err := events.ReadParentEvents(t.Context(), runtime.ReadParentEventsInput{ThreadID: parentID, FirstEventID: page.FirstEventID, LastEventID: page.LastEventID})
	if err != nil || full.ThreadID != parentID || len(full.Events) != 2 || full.Events[0].Message != "Important finding" {
		t.Fatalf("full=%+v %v", full, err)
	}
	fork, err := service.Fork(t.Context(), runtime.ForkThreadInput{SourceThreadID: parentID, RequestKey: "event-fork"})
	if err != nil {
		t.Fatal(err)
	}
	if fork.ParentEvents == nil || fork.ParentEvents.ThreadID != fork.ThreadID || len(fork.ParentEvents.Events) != 2 {
		t.Fatalf("fork scope=%+v", fork.ParentEvents)
	}
	inherited, err := events.ReadParentEvents(t.Context(), runtime.ReadParentEventsInput{ThreadID: fork.ThreadID, FirstEventID: page.FirstEventID, LastEventID: page.LastEventID})
	if err != nil || inherited.ThreadID != fork.ThreadID || len(inherited.Events) != 2 || inherited.Events[0] != full.Events[0] || inherited.Events[1] != full.Events[1] {
		t.Fatalf("inherited=%+v %v", inherited, err)
	}
	if len(parentGateway.Requests()) != 1 {
		t.Fatal("child report woke parent")
	}
	if _, err := service.Send(t.Context(), runtime.SendInput{ThreadID: parentID, RequestKey: "review", Input: runtime.UserInput{Text: "review"}}); err != nil {
		t.Fatal(err)
	}
	waitPublishedToolInputView(t, service, parentID, func(v runtime.ThreadView) bool {
		return v.Activity == runtime.ThreadActivityIdle && v.LastOutcome != nil
	})
	requests := parentGateway.Requests()
	if len(requests) != 2 {
		t.Fatal("unexpected provider sampling")
	}
	found := false
	for _, m := range requests[1].Messages {
		if strings.Contains(m.Text, "Important finding") && strings.Contains(m.Text, "completed") {
			found = true
		}
	}
	if !found {
		t.Fatal("published service did not deliver reports and automatic completion")
	}
}

func TestPublishedThreadWaitContract(t *testing.T) {
	host, err := runtime.Open(t.Context(), runtime.Options{Storage: storage.Memory()})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Shutdown(context.Background())
	service, err := host.ThreadService(runtime.AgentFactoryFunc(func(context.Context, runtime.AgentRequest) (*runtime.Agent, error) {
		return nil, errors.New("not expected")
	}))
	if err != nil {
		t.Fatal(err)
	}
	waiter, ok := service.(runtime.ThreadWaiter)
	if !ok {
		t.Fatal("missing ThreadWaiter")
	}
	result, err := waiter.WaitThreads(t.Context(), runtime.WaitThreadsInput{ThreadIDs: []identity.ThreadID{"missing"}, Timeout: time.Second})
	if err != nil || result.Reason != runtime.ThreadWaitMissing || len(result.MissingThreadIDs) != 1 {
		t.Fatalf("wait=%#v err=%v", result, err)
	}
	failure := runtime.ThreadTurnFailure{Code: runtime.ThreadTurnFailureEngineContract, Reason: runtime.ThreadTurnFailureRepeatedToolCalls, Message: "agent loop repeated identical tool calls"}
	if err := failure.Validate(); err != nil {
		t.Fatal(err)
	}
	if (tools.Result{BoundedWait: &tools.BoundedWait{Timeout: time.Second, TimedOut: true}}).CompletedBoundedWait() {
		t.Fatal("unmeasured result qualified")
	}
}

func TestPublishedAttachmentLocatorIsStableAndOpaque(t *testing.T) {
	attachment := runtime.MessageAttachment{ResourceRef: "host:private-snapshot", Name: "image.png", MIMEType: "image/png", SizeBytes: 42}
	locator, err := attachment.Locator()
	if err != nil || !strings.HasPrefix(locator, "media:v1:") || strings.Contains(locator, attachment.ResourceRef) {
		t.Fatalf("locator=%q err=%v", locator, err)
	}
	raw, _ := json.Marshal(attachment)
	var replay runtime.MessageAttachment
	if err := json.Unmarshal(raw, &replay); err != nil {
		t.Fatal(err)
	}
	if got, err := replay.Locator(); err != nil || got != locator {
		t.Fatalf("replayed locator=%q err=%v", got, err)
	}
	replay.Name = "another.png"
	if got, _ := replay.Locator(); got == locator {
		t.Fatal("locator omitted descriptor metadata")
	}
}

func TestPublishedIndependentOutputArchival(t *testing.T) {
	text := strings.Repeat("x", 65537)
	projection := tools.BuildOutputProjection(tools.Result{Text: text}, tools.OutputPolicy{VisibleMaxBytes: 1 << 20, ArchiveAboveBytes: 65536})
	digest := sha256.Sum256([]byte(text))
	if projection.Truncated || projection.VisibleText != text || projection.FullOutputPlan == nil || projection.FullOutputPlan.Text != text || projection.ContentSHA256 != fmt.Sprintf("%x", digest) {
		t.Fatal("independent archive contract changed")
	}
}

func TestPublishedTransportRetryAdvice(t *testing.T) {
	if got := provider.RetryAdviceForError(fmt.Errorf("frame: %w", io.EOF)); got.Category != provider.RetryStreamInterrupted {
		t.Fatalf("advice=%#v", got)
	}
	advice := provider.RetryAdvice{Forbidden: true}
	if got := provider.RetryAdviceForError(provider.WithRetryAdvice(io.EOF, advice)); got != advice {
		t.Fatalf("advice=%#v", got)
	}
}

func TestPublishedGoalManagementSurvivesRestartWithoutExecution(t *testing.T) {
	path := filepath.Join(t.TempDir(), "goals.db")
	open := func() (*runtime.Host, runtime.ThreadService) {
		t.Helper()
		host, err := runtime.Open(t.Context(), runtime.Options{Storage: storage.SQLite(path)})
		if err != nil {
			t.Fatal(err)
		}
		service, err := host.ThreadService(runtime.AgentFactoryFunc(func(context.Context, runtime.AgentRequest) (*runtime.Agent, error) {
			t.Fatal("goal management started execution")
			return nil, errors.New("unexpected execution")
		}))
		if err != nil {
			t.Fatal(err)
		}
		return host, service
	}
	host, service := open()
	thread, err := service.Create(t.Context(), runtime.CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	input := runtime.SetGoalInput{ThreadID: thread.ThreadID, RequestKey: "set", Text: "Deliver a verified result"}
	if _, err = service.(runtime.ThreadGoalController).SetGoal(t.Context(), input); err != nil {
		t.Fatal(err)
	}
	if err = host.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	host, service = open()
	defer host.Shutdown(context.Background())
	view, err := service.View(t.Context(), thread.ThreadID)
	if err != nil || view.Goal == nil || view.Goal.Text != input.Text {
		t.Fatalf("goal=%+v err=%v", view.Goal, err)
	}
	replay, err := service.(runtime.ThreadGoalController).SetGoal(t.Context(), input)
	if err != nil || !replay.Replayed {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	conflict, err := service.(runtime.ThreadGoalController).DeleteGoal(t.Context(), runtime.DeleteGoalInput{ThreadID: thread.ThreadID, RequestKey: "stale", ExpectedVersion: 0})
	if err != nil || !conflict.Conflict {
		t.Fatalf("conflict=%+v err=%v", conflict, err)

	}
}

func TestPreparedCharacterContract(t *testing.T) {
	value := &config.ContextCharacterBreakdown{Source: "prepared_characters_v1", Unit: "unicode_code_points", TotalCharacters: 3, Categories: []config.ContextCharacterCategory{{Category: "user", Characters: 3}}}
	if err := value.Validate(); err != nil {
		t.Fatal(err)
	}
	estimate := provider.TokenEstimate{Characters: value.Clone()}
	request := config.RequestEstimate{Characters: estimate.Characters.Clone()}
	status := observation.ContextStatus{Phase: observation.ContextPhaseProviderUsage, Status: observation.ContextStatusStable, RequestID: "request", PayloadFingerprint: "sha256:frozen", Provider: "test", Model: "test", RequestEstimate: request, Usage: observation.ProviderUsage{InputTokensReported: true, InputTokenScope: "provider_execution", Available: true, Source: "native", InputTokens: 100}}
	result, err := observation.ReconcileContextStatus(status)
	if err != nil || !result.CharactersAvailable || result.Characters.TotalCharacters != 3 || result.InputTokens != 100 {
		t.Fatalf("%+v %v", result, err)
	}
}

func TestPublishedCanonicalPresentationContract(t *testing.T) {
	item := runtime.ThreadItem{ID: "answer", CanonicalOrdinal: 7, Ordinal: 2, Kind: runtime.ThreadItemAssistant}
	event := runtime.ParentEvent{CanonicalOrdinal: 9}
	turn := runtime.ThreadTurn{IntermediateItemIDs: []string{item.ID}}
	if item.CanonicalOrdinal >= event.CanonicalOrdinal || turn.IntermediateItemIDs[0] != item.ID {
		t.Fatal("canonical presentation contract unavailable")
	}
}

func TestPublishedDeepSeekWebTools(t *testing.T) {
	cap, ok := provider.DeepSeekModelCapabilities("deepseek-flash")
	if !ok || !cap.IndependentSearch {
		t.Fatalf("capability=%+v", cap)
	}
	client, err := websearch.NewDeepSeekClient(websearch.ClientOptions{APIKey: "test"})
	if err != nil {
		t.Fatal(err)
	}
	search := websearch.NewTool(client, websearch.ToolOptions{})
	fetch, err := webfetch.NewDeepSeek(webfetch.DeepSeekOptions{APIKey: "test"})
	if err != nil {
		t.Fatal(err)
	}
	_ = tools.NewRegistry(search, fetch)
}

func TestPublishedGoalCompactionContinuation(t *testing.T) {
	review := `{"operation":"review","expected_version":1,"outcome":"completed","evidence":"Verified the result."}`
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "adoption", Model: "goal", StateCompatibilityKey: "adoption:goal"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "review", Name: "goal", Args: review}}}, {Type: provider.EventDone, Reason: "tool_calls"}}}, florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "Verified and finished."}, {Type: provider.EventDone, Reason: "stop"}}})
	host, err := runtime.Open(t.Context(), runtime.Options{Storage: storage.Memory()})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Shutdown(context.Background())
	service, err := host.ThreadService(runtime.AgentFactoryFunc(func(_ context.Context, req runtime.AgentRequest) (*runtime.Agent, error) {
		if req.TurnKind != runtime.TurnKindGoalContinuation || req.CanonicalTurnInput.Text != "" {
			t.Errorf("invalid typed request: %+v", req)
		}
		return runtime.NewAgent(config.AgentConfig{Profile: config.AgentProfile{ID: "goal", Name: "Goal"}, SystemPrompt: "Finish the Goal.", Context: config.ContextPolicy{ContextWindowTokens: 256000}}, gateway, runtime.WithAgentGoals())
	}))
	if err != nil {
		t.Fatal(err)
	}
	thread, err := service.Create(t.Context(), runtime.CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.(runtime.ThreadGoalController).SetGoal(t.Context(), runtime.SetGoalInput{ThreadID: thread.ThreadID, RequestKey: "goal", Text: "Deliver a verified result"}); err != nil {
		t.Fatal(err)
	}
	input := runtime.CompactContextInput{ThreadID: thread.ThreadID, RequestKey: "compact", ContinueGoal: true}
	admitted, err := service.(runtime.ThreadContextCompactor).CompactContext(t.Context(), input)
	if err != nil {
		t.Fatal(err)
	}
	final := waitPublishedToolInputView(t, service, thread.ThreadID, func(v runtime.ThreadView) bool {
		return v.Activity == runtime.ThreadActivityIdle && v.LastOutcome != nil
	})
	if final.Failure != nil || final.Goal.Status != "completed" || final.TurnID != admitted.TurnID || final.TurnKind != runtime.TurnKindGoalContinuation {
		t.Fatalf("goal=%+v failure=%+v", final.Goal, final.Failure)
	}
	for _, item := range final.Items {
		if item.Kind == runtime.ThreadItemUser {
			t.Fatal("fabricated user input")
		}
	}
	if _, err = service.(runtime.ThreadContextCompactor).CompactContext(t.Context(), input); err != nil {
		t.Fatal(err)
	}
	if len(gateway.Requests()) != 2 {
		t.Fatal("replay restarted execution")
	}
}

func TestPublishedThreadCommandAcknowledgments(t *testing.T) {
	host, err := runtime.Open(t.Context(), runtime.Options{Storage: storage.SQLite(filepath.Join(t.TempDir(), "ack.sqlite"))})
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
	commands, ok := service.(runtime.ThreadCommandService)
	if !ok {
		t.Fatal("service lacks acknowledgment commands")
	}
	created, err := commands.CreateAck(t.Context(), runtime.CreateThreadInput{RequestKey: "  create-ack  "})
	if err != nil || created.ThreadID == "" || created.RequestKey != "create-ack" || created.ViewVersion != 0 {
		t.Fatalf("ack=%+v err=%v", created, err)
	}
	replay, err := commands.CreateAck(t.Context(), runtime.CreateThreadInput{RequestKey: "create-ack"})
	if err != nil || replay != created {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	if _, err := commands.SetTitleAck(t.Context(), runtime.SetTitleInput{ThreadID: created.ThreadID, Title: "Acknowledged", RequestKey: "title-ack"}); err != nil {
		t.Fatal(err)
	}
	forked, err := commands.ForkAck(t.Context(), runtime.ForkThreadInput{SourceThreadID: created.ThreadID, RequestKey: "fork-ack"})
	if err != nil || forked.ThreadID == created.ThreadID {
		t.Fatalf("fork=%+v err=%v", forked, err)
	}
	view, err := service.View(t.Context(), created.ThreadID)
	if err != nil || view.ThreadID != created.ThreadID {
		t.Fatalf("view=%+v err=%v", view, err)
	}
}

func TestPublishedCanonicalBodyReadAndSearch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "body.db")
	source := strings.Repeat("abc中文🌸\r\n", 10000)
	open := func() (*runtime.Host, runtime.ThreadService) {
		host, err := runtime.Open(t.Context(), runtime.Options{Storage: storage.SQLite(path)})
		if err != nil {
			t.Fatal(err)
		}
		service, err := host.ThreadService(runtime.AgentFactoryFunc(func(context.Context, runtime.AgentRequest) (*runtime.Agent, error) {
			gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "body", StateCompatibilityKey: "body:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "done"}, {Type: provider.EventDone, Reason: "stop"}}})
			return runtime.NewAgent(config.AgentConfig{Profile: config.AgentProfile{ID: "body", Name: "Body"}, SystemPrompt: "Test.", Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens}}, gateway)
		}))
		if err != nil {
			t.Fatal(err)
		}
		return host, service
	}
	host, service := open()
	created, err := service.Create(t.Context(), runtime.CreateThreadInput{RequestKey: "body-create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.(runtime.ThreadCommandService).SendAck(t.Context(), runtime.SendInput{ThreadID: created.ThreadID, RequestKey: "body-send", Input: runtime.UserInput{Text: source}}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	var view runtime.ThreadView
	for {
		view, err = service.View(t.Context(), created.ThreadID)
		if err != nil {
			t.Fatal(err)
		}
		if view.Activity == runtime.ThreadActivityIdle && view.LastOutcome != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("body fixture did not finish")
		}
		time.Sleep(time.Millisecond)
	}
	var entry string
	for _, item := range view.Items {
		if item.Kind == runtime.ThreadItemUser {
			entry = item.ID
		}
	}
	if entry == "" {
		t.Fatal("missing canonical user item")
	}
	scope := threadquery.Scope{Authority: "body-reader", ThreadIDs: []identity.ThreadID{created.ThreadID}}
	for restart := range 2 {
		reader, ok := service.(runtime.ThreadBodyReader)
		if !ok {
			t.Fatal("missing reader")
		}
		searcher, ok := service.(runtime.ThreadBodySearcher)
		if !ok {
			t.Fatal("missing searcher")
		}
		input := runtime.ThreadBodyInput{ThreadID: created.ThreadID, EntryID: entry, Field: runtime.ThreadBodyContent}
		digest := sha256.New()
		for input.OffsetBytes < len(source) {
			part, err := reader.ReadBody(t.Context(), scope, input)
			if err != nil {
				t.Fatal(err)
			}
			if len(part.Text) == 0 || part.Bytes != len(source) {
				t.Fatal("invalid source chunk")
			}
			digest.Write([]byte(part.Text))
			input.OffsetBytes += len(part.Text)
			input.SHA256 = part.SHA256
		}
		expected := sha256.Sum256([]byte(source))
		if !reflect.DeepEqual(digest.Sum(nil), expected[:]) {
			t.Fatal("body hash mismatch")
		}
		input.OffsetBytes = 0
		page, err := searcher.SearchBody(t.Context(), scope, runtime.ThreadBodySearchInput{Body: input, Query: "中文🌸", Limit: 2})
		if err != nil || len(page.Matches) != 2 || !page.HasMore {
			t.Fatalf("search=%+v err=%v", page, err)
		}
		if _, err := searcher.SearchBody(t.Context(), threadquery.Scope{Authority: scope.Authority}, runtime.ThreadBodySearchInput{Body: input, Query: "中文"}); !errors.Is(err, threadquery.ErrRecordUnavailable) {
			t.Fatalf("revoked scope: %v", err)
		}
		if err := host.Shutdown(t.Context()); err != nil {
			t.Fatal(err)
		}
		if restart == 0 {
			host, service = open()
		}
	}
}

func TestPublishedScopedWindowAndCanonicalManifest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "window.sqlite")
	text := strings.Repeat("中é🌸\r\n", 18000)
	open := func() (*runtime.Host, runtime.ThreadService) {
		host, err := runtime.Open(t.Context(), runtime.Options{Storage: storage.SQLite(path)})
		if err != nil {
			t.Fatal(err)
		}
		service, err := host.ThreadService(runtime.AgentFactoryFunc(func(context.Context, runtime.AgentRequest) (*runtime.Agent, error) {
			gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "window", StateCompatibilityKey: "window:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: text}, {Type: provider.EventDone, Reason: "stop"}}})
			return runtime.NewAgent(config.AgentConfig{Profile: config.AgentProfile{ID: "window", Name: "Window"}, SystemPrompt: "Test.", Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens}}, gateway)
		}))
		if err != nil {
			t.Fatal(err)
		}
		return host, service
	}
	host, service := open()
	commands := service.(runtime.ThreadCommandService)
	created, err := commands.CreateAck(t.Context(), runtime.CreateThreadInput{RequestKey: "window-create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = commands.SendAck(t.Context(), runtime.SendInput{ThreadID: created.ThreadID, RequestKey: "window-send", Input: runtime.UserInput{Text: "generate"}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	for {
		summary, err := service.(runtime.ThreadSummaryReader).Summary(ctx, created.ThreadID)
		if err != nil {
			t.Fatal(err)
		}
		if summary.Activity == runtime.ThreadActivityIdle && summary.LastOutcome != nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	scope := threadquery.Scope{Authority: "window-reader", ThreadIDs: []identity.ThreadID{created.ThreadID}}
	for restart := range 2 {
		windows, ok := service.(runtime.ThreadWindowSource)
		if !ok {
			t.Fatal("missing bounded window source")
		}
		subscription, err := windows.SubscribeWindowUpdates(t.Context(), scope)
		if err != nil {
			t.Fatal(err)
		}
		if err := subscription.SetWindow(ctx, scope, runtime.ThreadWindowInput{ThreadID: created.ThreadID}); err != nil {
			t.Fatal(err)
		}
		update, err := subscription.Next(ctx)
		for err == nil && update.Window == nil {
			update, err = subscription.Next(ctx)
		}
		if err != nil || update.Window == nil || update.Window.Value == nil {
			t.Fatalf("baseline=%+v err=%v", update, err)
		}
		page := update.Window.Value
		if page.ViewVersion != update.ViewVersion || len(page.Items) > 150 {
			t.Fatal("unbounded or mismatched baseline")
		}
		raw, err := json.Marshal(page)
		if err != nil || len(raw) > 512<<10 {
			t.Fatal("page exceeds wire budget", err)
		}
		var body *runtime.ThreadItemBody
		for _, item := range page.Items {
			if item.Kind == runtime.ThreadItemAssistant {
				body = item.Body
			}
		}
		if body == nil || body.SourceID == "" || body.Bytes != len(text) || body.PreviewBytes >= len(text) {
			t.Fatalf("body=%+v", body)
		}
		locator, ok := service.(runtime.ThreadItemBodyReader)
		if !ok {
			t.Fatal("missing item body locator")
		}
		resolved, err := locator.ItemBody(ctx, scope, runtime.ThreadItemBodyInput{ThreadID: created.ThreadID, ItemID: body.SourceID})
		if err != nil || resolved.SourceID != body.SourceID || resolved.Field != body.Field || resolved.Bytes != body.Bytes || resolved.PreviewBytes != 0 {
			t.Fatalf("item body locator=%+v error=%v", resolved, err)
		}
		if _, err := locator.ItemBody(ctx, threadquery.Scope{Authority: "revoked"}, runtime.ThreadItemBodyInput{ThreadID: created.ThreadID, ItemID: body.SourceID}); !errors.Is(err, threadquery.ErrRecordUnavailable) {
			t.Fatalf("revoked body locator=%v", err)
		}
		manifest := service.(runtime.ThreadBodyManifestReader)
		input := runtime.ThreadBodyManifestInput{ThreadID: created.ThreadID, SourceID: body.SourceID, Field: body.Field}
		hash := sha256.New()
		for {
			parts, err := manifest.BodyManifest(ctx, scope, input)
			if err != nil {
				t.Fatal(err)
			}
			if len(parts.Parts) > 150 || parts.LeafID == "" {
				t.Fatal("invalid manifest")
			}
			for _, part := range parts.Parts {
				for offset := 0; offset < part.Bytes; {
					chunk, err := service.(runtime.ThreadBodyReader).ReadBody(ctx, scope, runtime.ThreadBodyInput{ThreadID: created.ThreadID, EntryID: part.EntryID, Field: part.Field, SHA256: part.SHA256, OffsetBytes: offset})
					if err != nil || len(chunk.Text) == 0 {
						t.Fatal("body read failed", err)
					}
					hash.Write([]byte(chunk.Text))
					offset += len(chunk.Text)
				}
			}
			if !parts.HasMore {
				break
			}
			input.LeafID, input.AfterEntryID = parts.LeafID, parts.NextEntryID
		}
		if fmt.Sprintf("%x", hash.Sum(nil)) != fmt.Sprintf("%x", sha256.Sum256([]byte(text))) {
			t.Fatal("manifest lost canonical bytes")
		}
		subscription.ClearWindow(created.ThreadID)
		if err := subscription.SetWindow(ctx, threadquery.Scope{Authority: scope.Authority}, runtime.ThreadWindowInput{ThreadID: created.ThreadID}); !errors.Is(err, threadquery.ErrRecordUnavailable) {
			t.Fatal("revoked scope accepted", err)
		}
		subscription.Close()
		if err := host.Shutdown(t.Context()); err != nil {
			t.Fatal(err)
		}
		if restart == 0 {
			host, service = open()
		}
	}
}

func TestPublishedAckGoalBeforeReadAndAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ack-goal.db")
	factory := runtime.AgentFactoryFunc(func(context.Context, runtime.AgentRequest) (*runtime.Agent, error) {
		return nil, errors.New("goal mutation must not execute")
	})
	open := func() (*runtime.Host, runtime.ThreadService) {
		host, err := runtime.Open(t.Context(), runtime.Options{Storage: storage.SQLite(path)})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
		service, err := host.ThreadService(factory)
		if err != nil {
			t.Fatal(err)
		}
		return host, service
	}
	firstHost, first := open()
	created, err := first.(runtime.ThreadCommandService).CreateAck(t.Context(), runtime.CreateThreadInput{RequestKey: "ack-goal-create"})
	if err != nil {
		t.Fatal(err)
	}
	set := runtime.SetGoalInput{ThreadID: created.ThreadID, RequestKey: "ack-goal-set", Text: "Retain the canonical objective"}
	if _, err := first.(runtime.ThreadGoalController).SetGoal(t.Context(), set); err != nil {
		t.Fatal(err)
	}
	view, err := first.View(t.Context(), created.ThreadID)
	if err != nil || view.ThreadID != created.ThreadID || view.Activity != runtime.ThreadActivityIdle || view.TurnID != "" || view.RunID != "" || view.Goal == nil || view.Goal.Text != set.Text || len(view.Items) != 1 {
		t.Fatalf("initial goal-only view=%+v err=%v", view, err)
	}
	if err := firstHost.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, second := open()
	deleted, err := second.(runtime.ThreadGoalController).DeleteGoal(t.Context(), runtime.DeleteGoalInput{ThreadID: created.ThreadID, RequestKey: "ack-goal-delete", ExpectedVersion: 1})
	if err != nil || deleted.Goal.Version != 2 {
		t.Fatalf("delete=%+v err=%v", deleted, err)
	}
	view, err = second.View(t.Context(), created.ThreadID)
	if err != nil || view.ThreadID != created.ThreadID || view.Activity != runtime.ThreadActivityIdle || view.TurnID != "" || view.RunID != "" || view.Goal == nil || view.Goal.Status != "absent" || len(view.Items) != 2 {
		t.Fatalf("restarted goal-only view=%+v err=%v", view, err)
	}
	if _, err := json.Marshal(view); err != nil {
		t.Fatal(err)
	}
}

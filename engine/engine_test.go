package engine_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/compaction"
	"github.com/floegence/floret/contextpolicy"
	"github.com/floegence/floret/engine"
	"github.com/floegence/floret/event"
	"github.com/floegence/floret/harness"
	"github.com/floegence/floret/memory"
	"github.com/floegence/floret/promptcache"
	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/session"
	"github.com/floegence/floret/tools"
)

func TestRunDirectAnswerCompletesThroughNaturalStop(t *testing.T) {
	rec := &event.Recorder{}
	p := harness.NewScriptedProvider(
		harness.Step(harness.Text("I checked it."), harness.Done()),
	)
	e := newTestEngine(p, rec)

	got := e.Run(context.Background(), "do the thing")

	if got.Status != engine.Completed || got.Output != "I checked it." {
		t.Fatalf("status = %s, want completed: %v", got.Status, got.Err)
	}
	if got.CompletionReason != engine.CompletionReasonNaturalStop || got.FinishReason != provider.FinishStop {
		t.Fatalf("completion metadata = %#v", got)
	}
	messages, err := e.Store.Messages("run")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(messages, func(msg session.Message) bool {
		return msg.Role == session.Assistant && msg.Content == "I checked it."
	}) {
		t.Fatalf("assistant final text was not persisted: %#v", messages)
	}
	if !slices.ContainsFunc(rec.Events, func(ev event.Event) bool {
		return ev.Type == event.ProviderFinish && ev.FinishReason == string(provider.FinishStop) && ev.RawFinishReason == "stop"
	}) {
		t.Fatalf("provider finish event missing: %#v", rec.Events)
	}
	assertEventOrder(t, rec.Events, event.StepStart, event.ProviderRequest, event.ProviderDelta, event.ProviderFinish, event.StepEnd, event.RunEnd)
}

func TestNaturalStopHookCanRequestAuditableContinuation(t *testing.T) {
	rec := &event.Recorder{}
	p := harness.NewScriptedProvider(
		harness.Step(harness.Text("draft "), harness.Done()),
		harness.Step(harness.Text("final"), harness.Done()),
	)
	e := newTestEngine(p, rec)
	hookCalls := 0
	e.StopHook = func(_ context.Context, ctx engine.StopHookContext) (engine.StopHookResult, error) {
		hookCalls++
		if hookCalls == 1 {
			if ctx.LastAssistant.Content != "draft " || ctx.FinishReason != provider.FinishStop {
				t.Fatalf("hook context = %#v", ctx)
			}
			return engine.StopHookResult{Continue: true, Prompt: "Verify the remaining work.", Reason: "verify"}, nil
		}
		return engine.StopHookResult{}, nil
	}

	got := e.Run(context.Background(), "do the thing")

	if got.Status != engine.Completed || got.Output != "draft final" || got.CompletionReason != engine.CompletionReasonNaturalStop {
		t.Fatalf("result = %#v", got)
	}
	if hookCalls != 2 {
		t.Fatalf("hook calls = %d, want 2", hookCalls)
	}
	if len(p.Requests) != 2 {
		t.Fatalf("requests = %d, want 2", len(p.Requests))
	}
	if !slices.ContainsFunc(p.Requests[1].Messages, func(msg session.Message) bool {
		return msg.Role == session.User && msg.Content == "Verify the remaining work."
	}) {
		t.Fatalf("continuation prompt missing from second provider request: %#v", p.Requests[1].Messages)
	}
	if !slices.ContainsFunc(rec.Events, func(ev event.Event) bool {
		return ev.Type == event.StepEnd && ev.ContinuationReason == string(engine.ContinueHook) && ev.Message == "verify"
	}) {
		t.Fatalf("hook continuation decision missing: %#v", rec.Events)
	}
}

func TestNaturalStopHookContinuationLimitPreventsLoops(t *testing.T) {
	p := harness.NewScriptedProvider(
		harness.Step(harness.Text("again "), harness.Done()),
		harness.Step(harness.Text("again"), harness.Done()),
	)
	e := newTestEngine(p, &event.Recorder{})
	e.Options.MaxStopHookContinuations = 1
	e.StopHook = func(context.Context, engine.StopHookContext) (engine.StopHookResult, error) {
		return engine.StopHookResult{Continue: true, Prompt: "continue"}, nil
	}

	got := e.Run(context.Background(), "loop")

	if got.Status != engine.Failed || !errors.Is(got.Err, engine.ErrStopHookLoop) {
		t.Fatalf("result = %#v, want stop hook loop failure", got)
	}
}

func TestProviderErrorFinishFailsWithFinishMetadata(t *testing.T) {
	p := harness.NewScriptedProvider(harness.Step(harness.Text("bad"), harness.DoneReason("error")))
	e := newTestEngine(p, &event.Recorder{})

	got := e.Run(context.Background(), "work")

	if got.Status != engine.Failed || !errors.Is(got.Err, engine.ErrProviderFinishError) || got.FinishReason != provider.FinishError || got.Output != "" {
		t.Fatalf("result = %#v, want provider finish error before committing text", got)
	}
}

func TestProviderCancelledFinishReturnsCancelled(t *testing.T) {
	p := harness.NewScriptedProvider(harness.Step(harness.DoneReason("cancelled")))
	e := newTestEngine(p, &event.Recorder{})

	got := e.Run(context.Background(), "work")

	if got.Status != engine.Cancelled || !errors.Is(got.Err, context.Canceled) || got.FinishReason != provider.FinishCancelled {
		t.Fatalf("result = %#v, want cancelled finish", got)
	}
}

func TestEmptyDoneRetriesThenCompletes(t *testing.T) {
	rec := &event.Recorder{}
	p := harness.NewScriptedProvider(
		harness.Step(harness.DoneReason("unknown")),
		harness.Step(harness.Text("ok"), harness.Done()),
	)
	e := newTestEngine(p, rec)

	got := e.Run(context.Background(), "work")

	if got.Status != engine.Completed || got.Output != "ok" {
		t.Fatalf("result = %#v", got)
	}
	if !hasEvent(rec.Events, event.ProviderRetry) {
		t.Fatalf("empty done did not trigger provider retry: %#v", rec.Events)
	}
}

func TestTaskCompleteOnlyCompletesWhenExplicitSignalPolicyIsEnabled(t *testing.T) {
	p := harness.NewScriptedProvider(
		harness.Step(harness.Tool("done", "task_complete", `{"output":"done"}`), harness.DoneReason("tool_calls")),
	)
	e := newTestEngine(p, &event.Recorder{})
	e.Options.CompletionPolicy = engine.CompletionExplicitSignal

	got := e.Run(context.Background(), "do the thing")

	if got.Status != engine.Completed || got.Output != "done" || got.CompletionReason != engine.CompletionReasonToolSignal {
		t.Fatalf("result = %#v, want legacy tool-signal completion", got)
	}
}

func TestTaskCompleteIsNormalToolUnderNaturalStopPolicy(t *testing.T) {
	p := harness.NewScriptedProvider(
		harness.Step(harness.Tool("done", "task_complete", `{"output":"done"}`), harness.DoneReason("tool_calls")),
	)
	e := newTestEngine(p, &event.Recorder{})
	got := e.Run(context.Background(), "do the thing")
	if got.Status != engine.Failed || got.Err == nil || !strings.Contains(got.Err.Error(), "provider returned empty output") {
		t.Fatalf("result = %#v, want ordinary unknown-tool path to fail without explicit-signal policy", got)
	}
}

func TestRunTurnUsesCallerSuppliedHistoryWithoutAppendingUserText(t *testing.T) {
	p := harness.NewScriptedProvider(harness.Step(harness.Text("ok"), harness.Done()))
	e := newTestEngine(p, &event.Recorder{})
	originalStore := session.NewMemoryStore()
	if err := originalStore.Append("run", session.Message{Role: session.User, Content: "existing"}); err != nil {
		t.Fatal(err)
	}
	e.Store = originalStore
	e.Options.RunID = "original-run"
	e.Options.SessionID = "original-session"
	result := e.RunTurn(context.Background(), engine.RunInput{
		RunID:     "turn",
		SessionID: "thread",
		TraceID:   "turn",
		History: []session.Message{
			{Role: session.User, Content: "caller-owned user"},
			{Role: session.Assistant, Content: "previous"},
		},
	})
	if result.Status != engine.Completed {
		t.Fatalf("result = %#v", result)
	}
	if len(p.Requests) != 1 {
		t.Fatalf("requests = %d", len(p.Requests))
	}
	if got := p.Requests[0].Messages; len(got) != 3 || got[1].Content != "caller-owned user" || got[2].Content != "previous" {
		t.Fatalf("RunTurn should use supplied history exactly after system prompt: %#v", got)
	}
	if countUserMessages(result.Messages, "caller-owned user") != 1 {
		t.Fatalf("RunTurn duplicated caller-owned user message: %#v", result.Messages)
	}
	if e.Store != originalStore {
		t.Fatalf("RunTurn did not restore original store")
	}
	if e.Options.RunID != "original-run" || e.Options.SessionID != "original-session" {
		t.Fatalf("RunTurn did not restore options: %#v", e.Options)
	}
	originalMessages, err := originalStore.Messages("run")
	if err != nil {
		t.Fatal(err)
	}
	if len(originalMessages) != 1 || originalMessages[0].Content != "existing" {
		t.Fatalf("RunTurn polluted original store: %#v", originalMessages)
	}
}

func TestLegacyTaskCompleteSignalIsProviderSafeWhenRunContinues(t *testing.T) {
	store := session.NewMemoryStore()
	promptStore := promptcache.NewMemoryStore()
	p1 := harness.NewScriptedProvider(harness.Step(harness.Tool("done", "task_complete", `{"output":"first done"}`), harness.DoneReason("tool_calls")))
	e1 := newTestEngine(p1, &event.Recorder{})
	e1.Store = store
	e1.Prompt = promptStore
	e1.Options.CompletionPolicy = engine.CompletionExplicitSignal
	got := e1.Run(context.Background(), "finish")
	if got.Status != engine.Completed {
		t.Fatalf("first result = %#v", got)
	}
	p2 := harness.NewScriptedProvider(harness.Step(harness.Text("second done"), harness.Done()))
	e2 := newTestEngine(p2, &event.Recorder{})
	e2.Store = store
	e2.Prompt = promptStore
	got = e2.Run(context.Background(), "continue anyway")
	if got.Status != engine.Completed {
		t.Fatalf("second result = %#v", got)
	}
	if slices.ContainsFunc(p2.Requests[0].RawPlan.Segments, func(seg promptcache.Segment) bool {
		return seg.Kind == promptcache.SegmentToolCall && seg.Message.ToolName == "task_complete"
	}) {
		t.Fatalf("continued run should not send orphan task_complete tool call: %#v", p2.Requests[0].RawPlan.Segments)
	}
	if !slices.ContainsFunc(p2.Requests[0].RawPlan.Segments, func(seg promptcache.Segment) bool {
		return seg.Kind == promptcache.SegmentAssistant && seg.Message.Content == "Agent completed the task: first done"
	}) {
		t.Fatalf("continued run missing provider-safe task_complete text: %#v", p2.Requests[0].RawPlan.Segments)
	}
	messages, err := store.Messages("run")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(messages, func(msg session.Message) bool {
		return msg.Role == session.Assistant && msg.ToolName == "task_complete" && msg.ToolCallID == "done"
	}) {
		t.Fatalf("raw session should still retain signal tool call for audit: %#v", messages)
	}
}

func TestRunToolLoopFeedsResultIntoNextProviderRequest(t *testing.T) {
	rec := &event.Recorder{}
	p := harness.NewScriptedProvider(
		[]provider.StreamEvent{
			{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{{ID: "read-1", Name: "read", Args: `{"value":"README.md"}`}}},
			{Type: provider.Done},
		},
		[]provider.StreamEvent{
			{Type: provider.Delta, Text: "saw file"},
			{Type: provider.Done, Reason: "stop"},
		},
	)
	reg := tools.NewRegistry()
	mustRegister(t, reg, stringTool("read", "Read", true, tools.PermissionSpec{}, func(context.Context, string) (string, error) {
		return "file contents", nil
	}))
	e := newTestEngine(p, rec)
	e.Tools = reg

	got := e.Run(context.Background(), "inspect")

	if got.Status != engine.Completed || got.Output != "saw file" || got.CompletionReason != engine.CompletionReasonNaturalStop {
		t.Fatalf("result = %#v", got)
	}
	if len(p.Requests) != 2 {
		t.Fatalf("requests = %d, want 2", len(p.Requests))
	}
	if p.Requests[1].RawPlan.NewSegments == 0 || p.Requests[1].RawPlan.ReusedSegments == 0 {
		t.Fatalf("second provider request did not expose reused and new raw segments: %#v", p.Requests[1].RawPlan)
	}
	second := p.Requests[1].Messages
	if !slices.ContainsFunc(second, func(m session.Message) bool {
		return m.Role == session.Assistant && m.ToolCallID == "read-1" && m.ToolName == "read"
	}) {
		t.Fatalf("second provider request did not include assistant tool call message: %#v", second)
	}
	if !slices.ContainsFunc(second, func(m session.Message) bool {
		return m.Role == session.Tool && m.ToolCallID == "read-1" && m.Content == "file contents"
	}) {
		t.Fatalf("second provider request did not include bound tool result: %#v", second)
	}
	assertEventOrder(t, rec.Events, event.StepStart, event.ProviderRequest, event.ToolCall, event.ToolResult, event.StepEnd, event.StepStart, event.ProviderRequest, event.RunEnd)
}

func TestRunMultipleToolCallsFeedAllResultsIntoNextProviderRequest(t *testing.T) {
	rec := &event.Recorder{}
	p := harness.NewScriptedProvider(
		harness.Step(
			harness.Text("checking "),
			provider.StreamEvent{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{
				{ID: "search-1", Name: "search", Args: `{"value":"weather"}`},
				{ID: "read-1", Name: "read", Args: `{"value":"forecast.md"}`},
			}},
			harness.DoneReason("tool_calls"),
		),
		harness.Step(harness.Text("done"), harness.Done()),
	)
	reg := tools.NewRegistry()
	mustRegister(t, reg, stringTool("search", "Search", true, tools.PermissionSpec{}, func(context.Context, string) (string, error) {
		return "search result", nil
	}))
	mustRegister(t, reg, stringTool("read", "Read", true, tools.PermissionSpec{}, func(context.Context, string) (string, error) {
		return "read result", nil
	}))
	e := newTestEngine(p, rec)
	e.Tools = reg

	got := e.Run(context.Background(), "inspect")

	if got.Status != engine.Completed || got.Output != "checking done" {
		t.Fatalf("result = %#v", got)
	}
	if len(p.Requests) != 2 {
		t.Fatalf("requests = %d, want 2", len(p.Requests))
	}
	second := p.Requests[1].Messages
	want := []session.Message{
		{Role: session.Assistant, ToolCallID: "search-1", ToolName: "search"},
		{Role: session.Assistant, ToolCallID: "read-1", ToolName: "read"},
		{Role: session.Tool, ToolCallID: "search-1", ToolName: "search"},
		{Role: session.Tool, ToolCallID: "read-1", ToolName: "read"},
	}
	for _, item := range want {
		if !slices.ContainsFunc(second, func(m session.Message) bool {
			return m.Role == item.Role && m.ToolCallID == item.ToolCallID && m.ToolName == item.ToolName
		}) {
			t.Fatalf("second provider request missing %#v in %#v", item, second)
		}
	}
	assertEventOrder(t, rec.Events, event.StepStart, event.ProviderRequest, event.ProviderDelta, event.ToolCall, event.ToolCall, event.ToolResult, event.ToolResult, event.StepEnd, event.StepStart, event.ProviderRequest, event.RunEnd)
}

func TestPromptCacheFreezesToolsetWhenRegistryChanges(t *testing.T) {
	p := harness.NewScriptedProvider(
		harness.Step(harness.Tool("read-1", "read", `{"value":"README.md"}`)),
		harness.Step(harness.Text("ok"), harness.Done()),
	)
	reg := tools.NewRegistry()
	mustRegister(t, reg, stringTool("read", "Read original", false, tools.PermissionSpec{}, func(context.Context, string) (string, error) {
		if err := reg.Register(stringTool("write", "Added later", false, tools.PermissionSpec{}, func(context.Context, string) (string, error) {
			return "write", nil
		})); err != nil {
			t.Fatalf("register during run: %v", err)
		}
		return "content", nil
	}))
	e := newTestEngine(p, &event.Recorder{})
	e.Tools = reg

	got := e.Run(context.Background(), "inspect")

	if got.Status != engine.Completed {
		t.Fatalf("result = %#v", got)
	}
	if len(p.Requests) != 2 {
		t.Fatalf("requests = %d", len(p.Requests))
	}
	for _, req := range p.Requests {
		if !slices.ContainsFunc(req.Tools, func(tool provider.ToolDefinition) bool {
			return tool.Name == "read" && tool.Description == "Read original"
		}) {
			t.Fatalf("request toolset changed after registry mutation: %#v", req.Tools)
		}
	}
	if p.Requests[0].RawPlan.ToolsetID != p.Requests[1].RawPlan.ToolsetID || p.Requests[1].RawPlan.ToolsetEpoch != 1 {
		t.Fatalf("toolset snapshot was not reused: first=%#v second=%#v", p.Requests[0].RawPlan, p.Requests[1].RawPlan)
	}
}

func TestPromptCacheActivatesNewToolsetOnNextTurnWhenRegistryChanges(t *testing.T) {
	store := session.NewMemoryStore()
	promptStore := promptcache.NewMemoryStore()
	reg := tools.NewRegistry()
	mustRegister(t, reg, stringTool("read", "Read", false, tools.PermissionSpec{}, func(context.Context, string) (string, error) {
		return "content", nil
	}))
	firstProvider := harness.NewScriptedProvider(harness.Step(harness.Text("first"), harness.Done()))
	first := newTestEngine(firstProvider, &event.Recorder{})
	first.Store = store
	first.Prompt = promptStore
	first.Tools = reg
	first.Options.RunID = "turn-1"
	first.Options.SessionID = "thread"
	if got := first.Run(context.Background(), "first"); got.Status != engine.Completed {
		t.Fatalf("first = %#v", got)
	}
	mustRegister(t, reg, stringTool("write", "Write", false, tools.PermissionSpec{}, func(context.Context, string) (string, error) {
		return "written", nil
	}))
	secondProvider := harness.NewScriptedProvider(harness.Step(harness.Text("second"), harness.Done()))
	second := newTestEngine(secondProvider, &event.Recorder{})
	second.Store = store
	second.Prompt = promptStore
	second.Tools = reg
	second.Options.RunID = "turn-2"
	second.Options.SessionID = "thread"
	if got := second.Run(context.Background(), "second"); got.Status != engine.Completed {
		t.Fatalf("second = %#v", got)
	}
	if firstProvider.Requests[0].RawPlan.ToolsetEpoch != 1 || secondProvider.Requests[0].RawPlan.ToolsetEpoch != 2 {
		t.Fatalf("toolset epochs: first=%#v second=%#v", firstProvider.Requests[0].RawPlan, secondProvider.Requests[0].RawPlan)
	}
	if !slices.ContainsFunc(secondProvider.Requests[0].Tools, func(tool provider.ToolDefinition) bool { return tool.Name == "read" }) ||
		!slices.ContainsFunc(secondProvider.Requests[0].Tools, func(tool provider.ToolDefinition) bool { return tool.Name == "write" }) {
		t.Fatalf("second turn should expose updated tools: %#v", secondProvider.Requests[0].Tools)
	}
	thirdProvider := harness.NewScriptedProvider(harness.Step(harness.Text("third"), harness.Done()))
	third := newTestEngine(thirdProvider, &event.Recorder{})
	third.Store = store
	third.Prompt = promptStore
	third.Tools = reg
	third.Options.RunID = "turn-3"
	third.Options.SessionID = "thread"
	if got := third.Run(context.Background(), "third"); got.Status != engine.Completed {
		t.Fatalf("third = %#v", got)
	}
	if thirdProvider.Requests[0].RawPlan.ToolsetEpoch != 2 {
		t.Fatalf("unchanged toolset should stay on epoch 2, got %#v", thirdProvider.Requests[0].RawPlan)
	}
}

func TestPromptCacheFileStoreKeepsPrefixStableAcrossEngineRestart(t *testing.T) {
	store := session.NewMemoryStore()
	root := t.TempDir()
	promptStore := promptcache.NewFileStore(root)
	firstProvider := harness.NewScriptedProvider(harness.Step(harness.Tool("ask", "ask_user", `{"question":"more?"}`)))
	first := newTestEngine(firstProvider, &event.Recorder{})
	first.Store = store
	first.Prompt = promptStore
	got := first.Run(context.Background(), "hello")
	if got.Status != engine.Waiting {
		t.Fatalf("first result = %#v", got)
	}
	firstHash := firstProvider.Requests[0].RawPlan.PrefixHash
	firstSegmentIDs := append([]string(nil), firstProvider.Requests[0].RawPlan.SegmentIDs...)
	firstSegmentRaws := segmentRawsForTest(firstProvider.Requests[0].RawPlan.Segments)
	secondProvider := harness.NewScriptedProvider(harness.Step(harness.Text("ok"), harness.Done()))
	second := newTestEngine(secondProvider, &event.Recorder{})
	second.Store = store
	second.Prompt = promptcache.NewFileStore(root)
	got = second.Run(context.Background(), "answer")
	if got.Status != engine.Completed {
		t.Fatalf("second result = %#v", got)
	}
	if secondProvider.Requests[0].RawPlan.ReusedSegments == 0 {
		t.Fatalf("resumed request did not reuse raw segments: %#v", secondProvider.Requests[0].RawPlan)
	}
	if firstHash == secondProvider.Requests[0].RawPlan.PrefixHash {
		t.Fatalf("prefix hash should include new suffix after resume")
	}
	if !slices.Equal(firstSegmentIDs, secondProvider.Requests[0].RawPlan.SegmentIDs[:len(firstSegmentIDs)]) {
		t.Fatalf("resumed request did not preserve first segment id prefix: first=%#v second=%#v", firstSegmentIDs, secondProvider.Requests[0].RawPlan.SegmentIDs)
	}
	if !slices.Equal(firstSegmentRaws, segmentRawsForTest(secondProvider.Requests[0].RawPlan.Segments[:len(firstSegmentRaws)])) {
		t.Fatalf("resumed request did not preserve first raw prefix")
	}
	for _, name := range []string{"raw_segments.jsonl", "toolsets.jsonl", "requests.jsonl", "responses.jsonl"} {
		if _, err := os.Stat(filepath.Join(root, "run", name)); err != nil {
			t.Fatalf("expected persisted %s: %v", name, err)
		}
	}
	data, err := os.ReadFile(filepath.Join(root, "run", "responses.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(strings.TrimSpace(string(data)), "\n") + 1; got != 2 {
		t.Fatalf("responses should append one record per run, got %d:\n%s", got, data)
	}
}

func TestProviderRequestRecordsActualPayloadHashWhenProviderExposesIt(t *testing.T) {
	p := &hashingProvider{ScriptedProvider: harness.NewScriptedProvider(harness.Step(harness.Text("ok"), harness.Done()))}
	p.hash = "provider-payload-hash"
	p.cache = promptcache.CachePolicy{Enabled: true, Namespace: "provider-ns", Retention: promptcache.RetentionLong}
	promptStore := promptcache.NewMemoryStore()
	e := newTestEngine(p, &event.Recorder{})
	e.Prompt = promptStore

	got := e.Run(context.Background(), "hello")

	if got.Status != engine.Completed {
		t.Fatalf("result = %#v", got)
	}
	requests, err := promptStore.ProviderRequests(context.Background(), "run")
	if err != nil {
		t.Fatal(err)
	}
	if len(requests) != 1 {
		t.Fatalf("requests = %d", len(requests))
	}
	if requests[0].ProviderPayloadHash != "provider-payload-hash" {
		t.Fatalf("provider payload hash = %q", requests[0].ProviderPayloadHash)
	}
	if requests[0].PrefixRawHash == "" || requests[0].PrefixRawHash == requests[0].ProviderPayloadHash {
		t.Fatalf("prefix hash should stay distinct from provider payload hash: %#v", requests[0])
	}
	if requests[0].CacheRetention != promptcache.RetentionLong || requests[0].CacheNamespace != "provider-ns" {
		t.Fatalf("cache policy was not normalized before recording: %#v", requests[0])
	}
	if p.Requests[0].RawPlan.PayloadHash != "provider-payload-hash" {
		t.Fatalf("provider request raw plan did not carry payload hash: %#v", p.Requests[0].RawPlan)
	}
}

func TestProviderRequestAndResponseRecordsCarryThreadAndTurnIDs(t *testing.T) {
	p := harness.NewScriptedProvider(harness.Step(
		provider.StreamEvent{Type: provider.UsageEvent, Usage: provider.Usage{CacheReadTokens: 10, CacheWriteTokens: 5}},
		provider.StreamEvent{Type: provider.Delta, Text: "ok"},
		provider.StreamEvent{Type: provider.Done, ResponseID: "resp-1"},
	))
	promptStore := promptcache.NewMemoryStore()
	e := newTestEngine(p, &event.Recorder{})
	e.Prompt = promptStore
	e.Options.RunID = "turn"
	e.Options.SessionID = "thread"

	got := e.Run(context.Background(), "hello")

	if got.Status != engine.Completed {
		t.Fatalf("result = %#v", got)
	}
	requests, err := promptStore.ProviderRequests(context.Background(), "turn")
	if err != nil {
		t.Fatal(err)
	}
	if len(requests) != 1 || requests[0].ThreadID != "thread" || requests[0].TurnID != "turn" {
		t.Fatalf("request thread/turn linkage missing: %#v", requests)
	}
	responses, err := promptStore.ProviderResponses(context.Background(), "turn")
	if err != nil {
		t.Fatal(err)
	}
	if len(responses) != 1 || responses[0].ThreadID != "thread" || responses[0].TurnID != "turn" || responses[0].ProviderResponseID != "resp-1" {
		t.Fatalf("response thread/turn linkage missing: %#v", responses)
	}
}

func TestAskUserSignalReturnsWaitingWithoutExecutingTool(t *testing.T) {
	rec := &event.Recorder{}
	p := harness.NewScriptedProvider(
		[]provider.StreamEvent{
			{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{{ID: "ask", Name: "ask_user", Args: `{"question":"Which file?"}`}}},
			{Type: provider.Done},
		},
	)
	e := newTestEngine(p, rec)

	got := e.Run(context.Background(), "continue")

	if got.Status != engine.Waiting || got.Output != "Which file?" {
		t.Fatalf("result = %#v, want waiting prompt", got)
	}
	if hasEvent(rec.Events, event.ToolCall) {
		t.Fatalf("ask_user should be an interrupt signal, not a normal tool call")
	}
	messages, err := e.Store.Messages("run")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(messages, func(msg session.Message) bool {
		return msg.Role == session.Assistant && msg.ToolCallID == "ask" && msg.ToolName == "ask_user"
	}) {
		t.Fatalf("ask_user tool call was not persisted before interrupt: %#v", messages)
	}
}

func TestMixedControlAndOrdinaryToolsFailBeforePersistingOrphans(t *testing.T) {
	p := harness.NewScriptedProvider(
		[]provider.StreamEvent{
			{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{
				{ID: "ask", Name: "ask_user", Args: `{"question":"Which file?"}`},
				{ID: "read", Name: "read", Args: `{"value":"x"}`},
			}},
		},
	)
	reg := tools.NewRegistry()
	mustRegister(t, reg, stringTool("read", "Read", false, tools.PermissionSpec{}, func(context.Context, string) (string, error) { return "ok", nil }))
	e := newTestEngine(p, &event.Recorder{})
	e.Tools = reg

	got := e.Run(context.Background(), "continue")

	if got.Status != engine.Failed || !errors.Is(got.Err, engine.ErrMixedControlTools) {
		t.Fatalf("result = %#v", got)
	}
	messages, err := e.Store.Messages("run")
	if err != nil {
		t.Fatal(err)
	}
	if slices.ContainsFunc(messages, func(msg session.Message) bool {
		return msg.ToolName == "read" || msg.ToolName == "ask_user"
	}) {
		t.Fatalf("mixed control tools should not persist orphan calls: %#v", messages)
	}
}

func TestWaitingCanResumeByAppendingUserAnswerToSameRun(t *testing.T) {
	store := session.NewMemoryStore()
	p1 := harness.NewScriptedProvider(harness.Step(harness.Tool("ask", "ask_user", `{"question":"Which file?"}`)))
	e1 := newTestEngine(p1, &event.Recorder{})
	e1.Store = store
	got := e1.Run(context.Background(), "continue")
	if got.Status != engine.Waiting {
		t.Fatalf("first result = %#v", got)
	}
	p2 := harness.NewScriptedProvider(harness.Step(harness.Text("resumed"), harness.Done()))
	e2 := newTestEngine(p2, &event.Recorder{})
	e2.Store = store
	got = e2.Run(context.Background(), "main.go")
	if got.Status != engine.Completed || got.Output != "resumed" {
		t.Fatalf("second result = %#v", got)
	}
	if len(p2.Requests) != 1 {
		t.Fatalf("requests = %d", len(p2.Requests))
	}
	var sawOriginal, sawAnswer bool
	for _, msg := range p2.Requests[0].Messages {
		if msg.Role == session.User && msg.Content == "continue" {
			sawOriginal = true
		}
		if msg.Role == session.User && msg.Content == "main.go" {
			sawAnswer = true
		}
	}
	if !sawOriginal || !sawAnswer {
		t.Fatalf("resume request missing context: %#v", p2.Requests[0].Messages)
	}
	if slices.ContainsFunc(p2.Requests[0].Messages, func(msg session.Message) bool {
		return msg.ToolName == "ask_user"
	}) {
		t.Fatalf("resume request should not include orphan ask_user tool call: %#v", p2.Requests[0].Messages)
	}
	if !slices.ContainsFunc(p2.Requests[0].Messages, func(msg session.Message) bool {
		return msg.Role == session.Assistant && msg.Content == "Agent requested user input: Which file?"
	}) {
		t.Fatalf("resume request missing provider-safe ask_user text: %#v", p2.Requests[0].Messages)
	}
}

func TestApprovalDeniedReturnsToolErrorAndAllowsModelRecovery(t *testing.T) {
	rec := &event.Recorder{}
	p := harness.NewScriptedProvider(
		[]provider.StreamEvent{
			{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{{ID: "write-1", Name: "write", Args: `{"value":"danger"}`}}},
			{Type: provider.Done},
		},
		[]provider.StreamEvent{
			{Type: provider.Delta, Text: "not changed"},
			{Type: provider.Done, Reason: "stop"},
		},
	)
	reg := tools.NewRegistry()
	called := false
	mustRegister(t, reg, stringTool("write", "Write", false, tools.PermissionSpec{Mode: tools.PermissionAsk}, func(context.Context, string) (string, error) {
		called = true
		return "changed", nil
	}))
	e := newTestEngine(p, rec)
	e.Tools = reg
	e.Approver = func(context.Context, tools.ApprovalRequest) (tools.PermissionDecision, error) {
		return tools.PermissionDecisionDeny, nil
	}

	got := e.Run(context.Background(), "write")

	if got.Status != engine.Completed || got.Output != "not changed" {
		t.Fatalf("result = %#v", got)
	}
	if called {
		t.Fatalf("approved-only tool handler ran after denial")
	}
	if !slices.ContainsFunc(rec.Events, func(ev event.Event) bool {
		return ev.Type == event.ToolResult && ev.Err == tools.ErrRejected.Error()
	}) {
		t.Fatalf("denial was not recorded as a structured tool result: %#v", rec.Events)
	}
}

func TestSchemaErrorReturnsToolResultAndAllowsModelRecovery(t *testing.T) {
	rec := &event.Recorder{}
	p := harness.NewScriptedProvider(
		harness.Step(harness.Tool("read-1", "read", `{"bad":true}`), harness.DoneReason("tool_calls")),
		harness.Step(harness.Text("recovered"), harness.Done()),
	)
	reg := tools.NewRegistry()
	called := false
	mustRegister(t, reg, stringTool("read", "Read", false, tools.PermissionSpec{}, func(context.Context, string) (string, error) {
		called = true
		return "bad", nil
	}))
	e := newTestEngine(p, rec)
	e.Tools = reg

	got := e.Run(context.Background(), "read")

	if got.Status != engine.Completed || got.Output != "recovered" {
		t.Fatalf("result = %#v", got)
	}
	if called {
		t.Fatalf("handler should not run for schema-invalid args")
	}
	if !slices.ContainsFunc(rec.Events, func(ev event.Event) bool {
		return ev.Type == event.ToolResult && strings.Contains(ev.Result, "invalid arguments")
	}) {
		t.Fatalf("schema error was not returned as tool result: %#v", rec.Events)
	}
}

func TestToolResultLimitAppliesBeforeHistoryAndNextProviderRequest(t *testing.T) {
	p := harness.NewScriptedProvider(
		harness.Step(harness.Tool("read-1", "read", `{"value":"x"}`), harness.DoneReason("tool_calls")),
		harness.Step(harness.Text("ok"), harness.Done()),
	)
	reg := tools.NewRegistry()
	mustRegister(t, reg, tools.Define[stringArgs](
		tools.Definition{
			Name:        "read",
			InputSchema: tools.StrictObject(map[string]any{"value": tools.String("value")}, []string{"value"}),
			ReadOnly:    true,
			ResultLimit: tools.ResultLimit{
				MaxBytes: 8,
				Strategy: "tail",
			},
		},
		nil,
		nil,
		func(context.Context, tools.Invocation[stringArgs]) (tools.Result, error) {
			return tools.Result{Text: "0123456789abcdef"}, nil
		},
	))
	e := newTestEngine(p, &event.Recorder{})
	e.Tools = reg

	got := e.Run(context.Background(), "read")

	if got.Status != engine.Completed {
		t.Fatalf("result = %#v", got)
	}
	if len(p.Requests) != 2 {
		t.Fatalf("requests = %d", len(p.Requests))
	}
	if !slices.ContainsFunc(p.Requests[1].Messages, func(msg session.Message) bool {
		return msg.Role == session.Tool && msg.Content == "89abcdef"
	}) {
		t.Fatalf("second request did not receive truncated tool result: %#v", p.Requests[1].Messages)
	}
	messages, err := e.Store.Messages("run")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(messages, func(msg session.Message) bool {
		return msg.Role == session.Tool && msg.ToolCallID == "read-1" && msg.Content == "89abcdef"
	}) {
		t.Fatalf("history did not store truncated tool result: %#v", messages)
	}
}

func TestErrorToolResultLimitPreservesErrorPrefixMetadataAndArtifacts(t *testing.T) {
	rec := &event.Recorder{}
	p := harness.NewScriptedProvider(
		harness.Step(harness.Tool("run-1", "shell", `{"value":"x"}`), harness.DoneReason("tool_calls")),
		harness.Step(harness.Text("recovered"), harness.Done()),
	)
	reg := tools.NewRegistry()
	mustRegister(t, reg, tools.Define[stringArgs](
		tools.Definition{
			Name:        "shell",
			InputSchema: tools.StrictObject(map[string]any{"value": tools.String("value")}, []string{"value"}),
			ResultLimit: tools.ResultLimit{MaxBytes: 8, Strategy: "tail"},
		},
		nil,
		nil,
		func(context.Context, tools.Invocation[stringArgs]) (tools.Result, error) {
			return tools.Result{
				Text:      "0123456789abcdef",
				Metadata:  map[string]any{"exit_code": 7},
				Artifacts: []tools.ArtifactRef{{Kind: "log", Path: "/tmp/full.log", MIME: "text/plain"}},
				IsError:   true,
			}, nil
		},
	))
	e := newTestEngine(p, rec)
	e.Tools = reg

	got := e.Run(context.Background(), "run")

	if got.Status != engine.Completed || got.Output != "recovered" {
		t.Fatalf("result = %#v", got)
	}
	messages, err := e.Store.Messages("run")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(messages, func(msg session.Message) bool {
		return msg.Role == session.Tool && msg.ToolCallID == "run-1" && msg.Content == "ERROR: 89abcdef"
	}) {
		t.Fatalf("history missing prefixed truncated error result: %#v", messages)
	}
	if !slices.ContainsFunc(rec.Events, func(ev event.Event) bool {
		if ev.Type != event.ToolCall || ev.ToolKind != "local" || ev.ToolName != "shell" {
			return false
		}
		return true
	}) {
		t.Fatalf("tool call event missing local kind: %#v", rec.Events)
	}
	if !slices.ContainsFunc(rec.Events, func(ev event.Event) bool {
		if ev.Type != event.ToolResult || ev.ToolKind != "local" || ev.ToolName != "shell" || ev.Result != "ERROR: 89abcdef" || ev.Err != "89abcdef" || len(ev.Artifacts) != 1 {
			return false
		}
		meta, ok := ev.Metadata.(map[string]any)
		return ok && meta["exit_code"] == 7 && meta["truncated"] == true
	}) {
		t.Fatalf("tool result event missing error metadata/artifact: %#v", rec.Events)
	}
}

func TestHostedToolsAreSentToProviderButNeverEnterLocalRunBatch(t *testing.T) {
	p := harness.NewScriptedProvider(harness.Step(harness.Text("searched"), harness.Done()))
	reg := tools.NewRegistry()
	called := false
	mustRegister(t, reg, tools.Define[stringArgs](
		tools.Definition{Name: "local_read", InputSchema: tools.StrictObject(map[string]any{"value": tools.String("value")}, []string{"value"})},
		nil,
		nil,
		func(context.Context, tools.Invocation[stringArgs]) (tools.Result, error) {
			called = true
			return tools.Result{Text: "bad"}, nil
		},
	))
	e := newTestEngine(p, &event.Recorder{})
	e.Tools = reg
	e.Options.ToolDefinitions = reg.Definitions()
	e.Options.HostedToolDefinitions = []provider.HostedToolDefinition{{Name: "web_search", Type: "web_search"}}

	got := e.Run(context.Background(), "search")

	if got.Status != engine.Completed || got.Output != "searched" {
		t.Fatalf("result = %#v", got)
	}
	if called {
		t.Fatalf("hosted-only provider response should not execute local tools")
	}
	if len(p.Requests) != 1 || len(p.Requests[0].HostedTools) != 1 || p.Requests[0].HostedTools[0].Name != "web_search" {
		t.Fatalf("hosted tools missing from request: %#v", p.Requests)
	}
	messages, err := e.Store.Messages("run")
	if err != nil {
		t.Fatal(err)
	}
	if slices.ContainsFunc(messages, func(msg session.Message) bool {
		return msg.Role == session.Tool
	}) {
		t.Fatalf("hosted tool should not create local tool result: %#v", messages)
	}
}

func TestReadOnlyToolsRunInParallelAndMutatingToolsKeepOrder(t *testing.T) {
	reg := tools.NewRegistry()
	order := make(chan string, 4)
	release := make(chan struct{})
	mustRegister(t, reg, stringTool("ro", "Read only", true, tools.PermissionSpec{}, func(_ context.Context, arg string) (string, error) {
		order <- "start-" + arg
		<-release
		order <- "end-" + arg
		return arg, nil
	}))
	mustRegister(t, reg, stringTool("mut", "Mutating", false, tools.PermissionSpec{}, func(_ context.Context, arg string) (string, error) {
		order <- "mut-" + arg
		return arg, nil
	}))
	done := make(chan []tools.Result, 1)
	go func() {
		done <- reg.RunBatch(context.Background(), []provider.ToolCall{
			{ID: "a", Name: "ro", Args: `{"value":"a"}`},
			{ID: "b", Name: "ro", Args: `{"value":"b"}`},
			{ID: "c", Name: "mut", Args: `{"value":"c"}`},
		}, nil)
	}()
	first := <-order
	second := <-order
	if !sameSet([]string{first, second}, []string{"start-a", "start-b"}) {
		t.Fatalf("read-only tools did not both start before release: %q %q", first, second)
	}
	close(release)
	results := <-done
	if len(results) != 3 || results[2].CallID != "c" {
		t.Fatalf("results are not in call order: %#v", results)
	}
}

func TestProviderEmptyOutputRetriesThenCompletes(t *testing.T) {
	rec := &event.Recorder{}
	p := harness.NewScriptedProvider(
		[]provider.StreamEvent{{Type: provider.Empty}},
		harness.Step(harness.Text("ok"), harness.Done()),
	)
	e := newTestEngine(p, rec)

	got := e.Run(context.Background(), "work")

	if got.Status != engine.Completed || got.Output != "ok" {
		t.Fatalf("result = %#v", got)
	}
	if !hasEvent(rec.Events, event.ProviderRetry) {
		t.Fatalf("empty provider output did not produce retry event")
	}
}

func TestRunAggregatesUsageMetricsAndEmitsProviderUsage(t *testing.T) {
	rec := &event.Recorder{}
	p := harness.NewScriptedProvider(
		harness.Step(
			harness.Usage(provider.Usage{InputTokens: 100, OutputTokens: 20, CostUSD: 0.12, Source: provider.UsageNative}),
			harness.Text("ok"),
			harness.Done(),
		),
	)
	e := newTestEngine(p, rec)
	e.Options.ProviderName = "fake"
	e.Options.Model = "fake-model"

	got := e.Run(context.Background(), "work")

	if got.Status != engine.Completed {
		t.Fatalf("result = %#v", got)
	}
	if got.Metrics.LLMRequests != 1 || got.Metrics.Usage.InputTokens != 100 || got.Metrics.Usage.OutputTokens != 20 || got.Metrics.Usage.CostUSD != 0.12 {
		t.Fatalf("metrics = %#v", got.Metrics)
	}
	if !hasEvent(rec.Snapshot(), event.ProviderUsage) {
		t.Fatalf("provider usage event missing: %#v", rec.Snapshot())
	}
	runEnd := rec.Snapshot()[len(rec.Snapshot())-1]
	runMetrics, ok := runEnd.Metrics.(engine.RunMetrics)
	if !ok || runMetrics.Usage.InputTokens != 100 || runMetrics.LLMRequests != 1 {
		t.Fatalf("run end metrics missing: %#v", runEnd.Metrics)
	}
}

func TestRunAggregatesUsageAcrossMultipleSteps(t *testing.T) {
	p := harness.NewScriptedProvider(
		harness.Step(
			harness.Usage(provider.Usage{InputTokens: 10, OutputTokens: 1, Source: provider.UsageNative}),
			harness.Tool("missing-1", "missing", "{}"),
		),
		harness.Step(
			harness.Usage(provider.Usage{InputTokens: 20, OutputTokens: 2, Source: provider.UsageEstimated}),
			harness.Text("ok"),
			harness.Done(),
		),
	)
	e := newTestEngine(p, &event.Recorder{})
	got := e.Run(context.Background(), "work")
	if got.Status != engine.Completed {
		t.Fatalf("result = %#v", got)
	}
	if got.Metrics.Usage.InputTokens != 30 || got.Metrics.Usage.OutputTokens != 3 || got.Metrics.Usage.TotalTokens != 33 || got.Metrics.Usage.Source != provider.UsageMixed {
		t.Fatalf("usage = %#v", got.Metrics.Usage)
	}
	if got.Metrics.LLMRequests != 2 || got.Metrics.ToolCalls != 1 {
		t.Fatalf("metrics = %#v", got.Metrics)
	}
}

func TestRunStopsOnTokenCostAndToolBudgets(t *testing.T) {
	t.Run("token budget", func(t *testing.T) {
		rec := &event.Recorder{}
		p := harness.NewScriptedProvider(harness.Step(harness.Usage(provider.Usage{InputTokens: 101}), harness.Text("ok"), harness.Done()))
		e := newTestEngine(p, rec)
		e.Options.MaxTotalTokens = 100
		got := e.Run(context.Background(), "work")
		if got.Status != engine.Failed || got.Err == nil || got.Err.Error() != "token budget exceeded" {
			t.Fatalf("result = %#v", got)
		}
		if !hasEvent(rec.Snapshot(), event.BudgetExceeded) {
			t.Fatalf("budget event missing")
		}
		for _, ev := range rec.Snapshot() {
			if ev.Type == event.BudgetExceeded {
				budget, ok := ev.Metrics.(engine.BudgetMetrics)
				if !ok || budget.Type != "tokens" || budget.Used != 101 || budget.Limit != 100 {
					t.Fatalf("budget payload = %#v", ev.Metrics)
				}
			}
		}
	})
	t.Run("cost budget", func(t *testing.T) {
		p := harness.NewScriptedProvider(harness.Step(harness.Usage(provider.Usage{CostUSD: 2, TotalTokens: 1}), harness.Text("ok"), harness.Done()))
		e := newTestEngine(p, &event.Recorder{})
		e.Options.MaxCostUSD = 1
		got := e.Run(context.Background(), "work")
		if got.Status != engine.Failed || got.Err == nil || got.Err.Error() != "cost budget exceeded" {
			t.Fatalf("result = %#v", got)
		}
	})
	t.Run("tool budget", func(t *testing.T) {
		p := harness.NewScriptedProvider(harness.Step(
			provider.StreamEvent{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{
				{ID: "a", Name: "missing", Args: `{"value":"a"}`},
				{ID: "b", Name: "missing", Args: `{"value":"b"}`},
			}},
		))
		e := newTestEngine(p, &event.Recorder{})
		e.Options.MaxToolCalls = 1
		got := e.Run(context.Background(), "work")
		if got.Status != engine.Failed || got.Err == nil || got.Err.Error() != "tool call budget exceeded" {
			t.Fatalf("result = %#v", got)
		}
	})
}

func TestProviderContextOverflowCompactsAndRetries(t *testing.T) {
	rec := &event.Recorder{}
	p := harness.NewScriptedProvider(
		nil,
		[]provider.StreamEvent{
			{Type: provider.Delta, Text: "after compact"},
			{Type: provider.Done, Reason: "stop"},
		},
	)
	p.Errs[1] = provider.ErrContextOverflow
	store := session.NewMemoryStore()
	if err := store.Append("run", session.Message{Role: session.User, Content: "older"}, session.Message{Role: session.User, Content: "newer"}); err != nil {
		t.Fatal(err)
	}
	e := newTestEngine(p, rec)
	e.Store = store
	e.Compactor = engine.LocalCompactionManager{Generator: compaction.ExtractiveSummaryGenerator{}}
	if _, err := buildProviderRequestForTest(context.Background(), e, 0, []session.Message{
		{Role: session.User, Content: "older"},
		{Role: session.User, Content: "newer"},
	}); err != nil {
		t.Fatal(err)
	}

	got := e.Run(context.Background(), "")

	if got.Status != engine.Completed || got.Output != "after compact" {
		t.Fatalf("result = %#v", got)
	}
	if !hasEvent(rec.Events, event.ContextCompact) {
		t.Fatalf("context overflow did not compact")
	}
	if got.Metrics.Compactions != 1 {
		t.Fatalf("compactions = %d, want 1", got.Metrics.Compactions)
	}
	segments, err := e.Prompt.Segments(context.Background(), "run", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(segments, func(seg promptcache.Segment) bool {
		return seg.Kind == promptcache.SegmentCompaction
	}) {
		t.Fatalf("compaction raw segment missing: %#v", segments)
	}
	for _, want := range []string{"older", "newer"} {
		if !slices.ContainsFunc(segments, func(seg promptcache.Segment) bool {
			return seg.Kind == promptcache.SegmentUserMessage && seg.Message.Content == want
		}) {
			t.Fatalf("raw segment %q should remain append-only after compaction: %#v", want, segments)
		}
	}
}

func TestPreRequestThresholdCompactsWithoutReplacingStore(t *testing.T) {
	rec := &event.Recorder{}
	p := harness.NewScriptedProvider(harness.Step(harness.Text("ok"), harness.Done()))
	store := &replaceCountingStore{inner: session.NewMemoryStore()}
	if err := store.Append("run",
		session.Message{Role: session.User, Content: strings.Repeat("old ", 1200)},
		session.Message{Role: session.User, Content: "new"},
	); err != nil {
		t.Fatal(err)
	}
	e := newTestEngine(p, rec)
	e.Store = store
	e.Compactor = engine.LocalCompactionManager{Generator: compaction.ExtractiveSummaryGenerator{}}
	e.Options.ContextPolicy = contextpolicy.Policy{ContextWindowTokens: 900, ReservedOutputTokens: 80, ReservedSummaryTokens: 80, RecentTailTokens: 20}

	got := e.Run(context.Background(), "")

	if got.Status != engine.Completed {
		t.Fatalf("result = %#v", got)
	}
	if store.replaceCalls != 0 {
		t.Fatalf("engine compaction must not install with Store.Replace, calls=%d", store.replaceCalls)
	}
	if got.Metrics.Compactions != 1 || len(p.Requests) != 1 {
		t.Fatalf("pre-request compaction not reflected in metrics/request count: result=%#v requests=%d", got, len(p.Requests))
	}
	if !slices.ContainsFunc(p.Requests[0].Messages, func(message session.Message) bool {
		return message.Kind == session.MessageKindCompactionSummary
	}) {
		t.Fatalf("provider request did not use compacted active projection: %#v", p.Requests[0].Messages)
	}
}

type replaceCountingStore struct {
	inner        *session.MemoryStore
	replaceCalls int
}

func (s *replaceCountingStore) Append(runID string, messages ...session.Message) error {
	return s.inner.Append(runID, messages...)
}

func (s *replaceCountingStore) Messages(runID string) ([]session.Message, error) {
	return s.inner.Messages(runID)
}

func (s *replaceCountingStore) Replace(runID string, messages []session.Message) error {
	s.replaceCalls++
	return s.inner.Replace(runID, messages)
}

func TestTruncatedProviderOutputContinuesWithoutFullCompactWhenInputPressureIsLow(t *testing.T) {
	rec := &event.Recorder{}
	p := harness.NewScriptedProvider(
		[]provider.StreamEvent{harness.Text("partial "), {Type: provider.Truncated}},
		[]provider.StreamEvent{
			{Type: provider.Delta, Text: "retried"},
			{Type: provider.Done, Reason: "stop"},
		},
	)
	e := newTestEngine(p, rec)
	e.Compactor = engine.LocalCompactionManager{Generator: compaction.ExtractiveSummaryGenerator{}}
	e.Options.ContextPolicy = contextpolicy.Policy{ContextWindowTokens: 8000, ReservedOutputTokens: 8, ReservedSummaryTokens: 8, RecentTailTokens: 8}

	got := e.Run(context.Background(), "work")

	if got.Status != engine.Completed || got.Output != "partial retried" {
		t.Fatalf("result = %#v", got)
	}
	if hasEvent(rec.Events, event.ContextCompact) {
		t.Fatalf("low-pressure truncation should not full compact: %#v", rec.Events)
	}
	messages, err := e.Store.Messages("run")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(messages, func(msg session.Message) bool {
		return msg.Role == session.Assistant && msg.Content == "partial "
	}) {
		t.Fatalf("partial assistant text was not persisted before continuation: %#v", messages)
	}
}

func TestTruncatedProviderOutputFailsAfterContinuationLimit(t *testing.T) {
	p := harness.NewScriptedProvider(
		[]provider.StreamEvent{{Type: provider.Truncated, Reason: "length"}},
		[]provider.StreamEvent{{Type: provider.Truncated, Reason: "length"}},
	)
	e := newTestEngine(p, &event.Recorder{})
	e.Options.MaxLengthContinuations = 1

	got := e.Run(context.Background(), "work")

	if got.Status != engine.Failed || !errors.Is(got.Err, engine.ErrProviderTruncated) || got.FinishReason != provider.FinishLength {
		t.Fatalf("result = %#v, want truncation failure", got)
	}
}

func TestContentFilterFinishFailsWithoutNaturalCompletion(t *testing.T) {
	p := harness.NewScriptedProvider(harness.Step(harness.Text("blocked"), harness.DoneReason("content_filter")))
	e := newTestEngine(p, &event.Recorder{})

	got := e.Run(context.Background(), "work")

	if got.Status != engine.Failed || !errors.Is(got.Err, engine.ErrContentFiltered) || got.Output != "" {
		t.Fatalf("result = %#v, want content-filter failure before assistant text is committed", got)
	}
}

func TestUnknownFinishWithTextIsInferredNaturalStop(t *testing.T) {
	rec := &event.Recorder{}
	p := harness.NewScriptedProvider(harness.Step(harness.Text("final"), harness.DoneReason("strange-provider-value")))
	e := newTestEngine(p, rec)

	got := e.Run(context.Background(), "work")

	if got.Status != engine.Completed || got.Output != "final" || got.FinishReason != provider.FinishStop || !got.FinishInferred {
		t.Fatalf("result = %#v, want inferred natural stop", got)
	}
	if !slices.ContainsFunc(rec.Events, func(ev event.Event) bool {
		return ev.Type == event.StepEnd && ev.CompletionReason == string(engine.CompletionReasonNaturalStop) && ev.FinishInferred
	}) {
		t.Fatalf("step end inference metadata missing: %#v", rec.Events)
	}
}

func TestLoopGuardsDuplicateToolsAndCancellation(t *testing.T) {
	t.Run("duplicate tools", func(t *testing.T) {
		p := harness.NewScriptedProvider(
			[]provider.StreamEvent{{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{{ID: "x1", Name: "missing", Args: `{"value":"same"}`}}}},
			[]provider.StreamEvent{{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{{ID: "x2", Name: "missing", Args: `{"value":"same"}`}}}},
		)
		e := newTestEngine(p, &event.Recorder{})
		e.Options.DuplicateToolLimit = 1
		got := e.Run(context.Background(), "loop")
		if !errors.Is(got.Err, engine.ErrDuplicateTools) {
			t.Fatalf("err = %v, want duplicate tools", got.Err)
		}
	})
	t.Run("duplicate call ids", func(t *testing.T) {
		p := harness.NewScriptedProvider(
			[]provider.StreamEvent{{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{
				{ID: "same", Name: "missing", Args: `{"value":"a"}`},
				{ID: "same", Name: "missing", Args: `{"value":"b"}`},
			}}},
		)
		e := newTestEngine(p, &event.Recorder{})
		got := e.Run(context.Background(), "loop")
		if !errors.Is(got.Err, engine.ErrDuplicateToolCallID) {
			t.Fatalf("err = %v, want duplicate tool call id", got.Err)
		}
	})
	t.Run("cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		p := harness.NewScriptedProvider()
		e := newTestEngine(p, &event.Recorder{})
		got := e.Run(ctx, "stop")
		if got.Status != engine.Cancelled {
			t.Fatalf("status = %s, want cancelled", got.Status)
		}
	})
}

func TestWallTimeCancelsSlowStream(t *testing.T) {
	p := blockingProvider{}
	e := newTestEngine(p, &event.Recorder{})
	e.Options.WallTime = 10 * time.Millisecond

	got := e.Run(context.Background(), "slow")

	if got.Status != engine.Cancelled || !errors.Is(got.Err, context.DeadlineExceeded) {
		t.Fatalf("result = %#v, want deadline failure", got)
	}
}

func TestContextCancelDuringProviderStreamReturnsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	p := &cancelAfterFirstDeltaProvider{cancel: cancel}
	e := newTestEngine(p, &event.Recorder{})

	got := e.Run(ctx, "slow")

	if got.Status != engine.Cancelled || !errors.Is(got.Err, context.Canceled) {
		t.Fatalf("result = %#v, want stream cancellation", got)
	}
}

type blockingProvider struct{}

func (blockingProvider) Stream(context.Context, provider.Request) (<-chan provider.StreamEvent, error) {
	return make(chan provider.StreamEvent), nil
}

type cancelAfterFirstDeltaProvider struct {
	cancel context.CancelFunc
}

func (p *cancelAfterFirstDeltaProvider) Stream(context.Context, provider.Request) (<-chan provider.StreamEvent, error) {
	ch := make(chan provider.StreamEvent, 1)
	ch <- provider.StreamEvent{Type: provider.Delta, Text: "partial"}
	p.cancel()
	return ch, nil
}

type hashingProvider struct {
	*harness.ScriptedProvider
	hash  string
	cache promptcache.CachePolicy
}

func (p *hashingProvider) NormalizeCachePolicy(promptcache.CachePolicy) (promptcache.CachePolicy, error) {
	return p.cache, nil
}

func (p *hashingProvider) PayloadHash(req provider.Request) (string, error) {
	raw := fmt.Sprintf("%s:%s:%s:%s", req.RunID, req.Cache.Namespace, req.Cache.Retention, req.RawPlan.PrefixHash)
	sum := sha256.Sum256([]byte(raw))
	if p.hash != "" {
		return p.hash, nil
	}
	return hex.EncodeToString(sum[:]), nil
}

func newTestEngine(p provider.Provider, rec *event.Recorder) *engine.Engine {
	return &engine.Engine{
		Provider: p,
		Store:    session.NewMemoryStore(),
		Memory:   &memory.Manager{SystemPrompt: "You are Floret."},
		Tools:    tools.NewRegistry(),
		Sink:     rec,
		Options: engine.Options{
			RunID:                   "run",
			MaxEmptyProviderRetries: 1,
			NoProgressLimit:         2,
			DuplicateToolLimit:      3,
		},
	}
}

func buildProviderRequestForTest(ctx context.Context, e *engine.Engine, step int, history []session.Message) (provider.Request, error) {
	if e.Prompt == nil {
		e.Prompt = promptcache.NewMemoryStore()
	}
	if e.Tools == nil {
		e.Tools = tools.NewRegistry()
	}
	opts := e.Options
	if opts.RunID == "" {
		opts.RunID = "run"
	}
	if opts.SessionID == "" {
		opts.SessionID = opts.RunID
	}
	if opts.TraceID == "" {
		opts.TraceID = opts.RunID
	}
	if opts.CacheNamespace == "" {
		opts.CacheNamespace = promptcache.DefaultNamespace(opts.SessionID, opts.ProviderName, opts.Model)
	}
	toolset, _, err := promptcache.EnsureToolset(ctx, e.Prompt, opts.RunID, opts.SessionID, opts.ProviderName, opts.Model, nil, nil, time.Now())
	if err != nil {
		return provider.Request{}, err
	}
	plan, messages, err := promptcache.BuildPlan(ctx, e.Prompt, promptcache.BuildInput{
		RunID:          opts.RunID,
		SessionID:      opts.SessionID,
		Provider:       opts.ProviderName,
		Model:          opts.Model,
		AdapterVersion: promptcache.Version,
		CacheNamespace: opts.CacheNamespace,
		History:        history,
		Toolset:        toolset,
		Now:            time.Now(),
	})
	if err != nil {
		return provider.Request{}, err
	}
	return provider.Request{RunID: opts.RunID, Step: step, Provider: opts.ProviderName, Model: opts.Model, Messages: messages, RawPlan: plan}, nil
}

func mustRegister(t *testing.T, reg *tools.Registry, tool tools.Tool) {
	t.Helper()
	if err := reg.Register(tool); err != nil {
		t.Fatalf("register %s: %v", tool.Definition.Name, err)
	}
}

type stringArgs struct {
	Value string `json:"value"`
}

func stringTool(name, description string, readOnly bool, permission tools.PermissionSpec, handler func(context.Context, string) (string, error)) tools.Tool {
	return tools.Define[stringArgs](
		tools.Definition{
			Name:        name,
			Description: description,
			InputSchema: tools.StrictObject(map[string]any{
				"value": tools.String("test value"),
			}, []string{"value"}),
			ReadOnly:   readOnly,
			Permission: permission,
		},
		nil,
		nil,
		func(ctx context.Context, inv tools.Invocation[stringArgs]) (tools.Result, error) {
			text, err := handler(ctx, inv.Args.Value)
			if err != nil {
				return tools.Result{}, err
			}
			return tools.Result{Text: text}, nil
		},
	)
}

func assertEventOrder(t *testing.T, events []event.Event, want ...event.Type) {
	t.Helper()
	var got []event.Type
	for _, ev := range events {
		got = append(got, ev.Type)
	}
	pos := 0
	for _, typ := range got {
		if pos < len(want) && typ == want[pos] {
			pos++
		}
	}
	if pos != len(want) {
		t.Fatalf("events = %v, want subsequence %v", got, want)
	}
}

func hasEvent(events []event.Event, typ event.Type) bool {
	return slices.ContainsFunc(events, func(ev event.Event) bool { return ev.Type == typ })
}

func countUserMessages(messages []session.Message, content string) int {
	count := 0
	for _, msg := range messages {
		if msg.Role == session.User && msg.Content == content {
			count++
		}
	}
	return count
}

func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for _, item := range a {
		if !slices.Contains(b, item) {
			return false
		}
	}
	return true
}

func segmentRawsForTest(segments []promptcache.Segment) []string {
	out := make([]string, len(segments))
	for i, segment := range segments {
		out[i] = segment.Raw
	}
	return out
}

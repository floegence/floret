package engine_test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/engine"
	"github.com/floegence/floret/event"
	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/provider/cache"
	"github.com/floegence/floret/session"
	"github.com/floegence/floret/session/artifact"
	"github.com/floegence/floret/session/compaction"
	"github.com/floegence/floret/session/contextpolicy"
	"github.com/floegence/floret/testing/harness"
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

func TestNewRequiresProvider(t *testing.T) {
	if _, err := engine.New(engine.Config{}); err == nil || !strings.Contains(err.Error(), "provider is required") {
		t.Fatalf("New without provider error = %v", err)
	}
}

func TestOptionsReturnsDeepCopyWithoutProviderToolDefinitions(t *testing.T) {
	eng, err := engine.New(engine.Config{
		Provider: harness.NewScriptedProvider(harness.Step(harness.Text("ok"), harness.Done())),
		Options: engine.Options{
			RunID: "run",
			HostedToolDefinitions: []provider.HostedToolDefinition{{
				Name:       "web_search",
				Type:       "web_search",
				Parameters: map[string]any{"nested": map[string]any{"value": "original"}},
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	first := eng.Options()
	first.HostedToolDefinitions[0].Parameters["nested"].(map[string]any)["value"] = "mutated"
	second := eng.Options()
	if got := second.HostedToolDefinitions[0].Parameters["nested"].(map[string]any)["value"]; got != "original" {
		t.Fatalf("Options did not deep copy hosted tool params: %#v", second.HostedToolDefinitions)
	}
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
		harness.Step(harness.Empty()),
		harness.Step(harness.Empty()),
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

func TestRunTurnConcurrentSameEngineIsolatesTurnState(t *testing.T) {
	p := newBarrierProvider(2)
	e := newTestEngine(p, &event.Recorder{})
	e.Options.RunID = "base-run"
	e.Options.SessionID = "base-session"

	var wg sync.WaitGroup
	results := make([]engine.Result, 2)
	inputs := []engine.RunInput{
		{RunID: "turn-a", SessionID: "thread-a", TraceID: "turn-a", History: []session.Message{{Role: session.User, Content: "alpha"}}},
		{RunID: "turn-b", SessionID: "thread-b", TraceID: "turn-b", History: []session.Message{{Role: session.User, Content: "beta"}}},
	}
	for i := range inputs {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = e.RunTurn(context.Background(), inputs[idx])
		}(i)
	}
	wg.Wait()

	for i, result := range results {
		if result.Status != engine.Completed || result.Output == "" {
			t.Fatalf("result %d = %#v", i, result)
		}
		if e.Options.RunID != "base-run" || e.Options.SessionID != "base-session" {
			t.Fatalf("RunTurn mutated receiver options: %#v", e.Options)
		}
	}
	requests := p.Requests()
	if len(requests) != 2 {
		t.Fatalf("requests = %d, want 2", len(requests))
	}
	seen := map[string]provider.Request{}
	for _, req := range requests {
		seen[req.RunID] = req
	}
	for _, input := range inputs {
		req, ok := seen[input.RunID]
		if !ok {
			t.Fatalf("missing request for run %s: %#v", input.RunID, requests)
		}
		if req.RunID != input.RunID {
			t.Fatalf("request scope = %#v, want run %s", req, input.RunID)
		}
		if !slices.ContainsFunc(req.Messages, func(msg session.Message) bool {
			return msg.Role == session.User && msg.Content == input.History[0].Content
		}) {
			t.Fatalf("request %s missing its own history: %#v", input.RunID, req.Messages)
		}
		other := "alpha"
		if input.History[0].Content == "alpha" {
			other = "beta"
		}
		if slices.ContainsFunc(req.Messages, func(msg session.Message) bool { return msg.Content == other }) {
			t.Fatalf("request %s leaked other session history: %#v", input.RunID, req.Messages)
		}
	}
}

func TestLegacyTaskCompleteSignalIsProviderSafeWhenRunContinues(t *testing.T) {
	store := session.NewMemoryStore()
	promptStore := cache.NewMemoryStore()
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
	if slices.ContainsFunc(p2.Requests[0].RawPlan.Segments, func(seg cache.Segment) bool {
		return seg.Kind == cache.SegmentToolCall && seg.Message.ToolName == "task_complete"
	}) {
		t.Fatalf("continued run should not send orphan task_complete tool call: %#v", p2.Requests[0].RawPlan.Segments)
	}
	if !slices.ContainsFunc(p2.Requests[0].RawPlan.Segments, func(seg cache.Segment) bool {
		return seg.Kind == cache.SegmentAssistant && seg.Message.Content == "Agent completed the task: first done"
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
		harness.Step(harness.Tool("read-1", "read", `{"value":"README.md"}`), harness.DoneReason("tool_calls")),
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
	promptStore := cache.NewMemoryStore()
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
	promptStore := cache.NewFileStore(root)
	firstProvider := harness.NewScriptedProvider(harness.Step(harness.Tool("ask", "ask_user", `{"question":"more?"}`), harness.DoneReason("tool_calls")))
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
	second.Prompt = cache.NewFileStore(root)
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
		if _, err := os.Stat(filepath.Join(root, promptCachePathForTest("run"), name)); err != nil {
			t.Fatalf("expected persisted %s: %v", name, err)
		}
	}
	data, err := os.ReadFile(filepath.Join(root, promptCachePathForTest("run"), "responses.jsonl"))
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
	p.cache = cache.CachePolicy{Enabled: true, Namespace: "provider-ns", Retention: cache.RetentionLong}
	promptStore := cache.NewMemoryStore()
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
	if requests[0].CacheRetention != cache.RetentionLong || requests[0].CacheNamespace != "provider-ns" {
		t.Fatalf("cache policy was not normalized before recording: %#v", requests[0])
	}
	if p.Requests[0].RawPlan.PayloadHash != "provider-payload-hash" {
		t.Fatalf("provider request raw plan did not carry payload hash: %#v", p.Requests[0].RawPlan)
	}
}

func TestProviderRequestAndResponseRecordsCarryThreadAndTurnIDs(t *testing.T) {
	p := harness.NewScriptedProvider(harness.Step(
		provider.StreamEvent{Type: provider.UsageEvent, Usage: provider.Usage{InputTokens: 100, WindowInputTokens: 115, OutputTokens: 20, ReasoningTokens: 3, CacheReadTokens: 10, CacheWriteTokens: 5, Source: provider.UsageNative}},
		provider.StreamEvent{Type: provider.Delta, Text: "ok"},
		provider.StreamEvent{Type: provider.Done, ResponseID: "resp-1"},
	))
	promptStore := cache.NewMemoryStore()
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
	if responses[0].InputTokens != 100 || responses[0].WindowInputTokens != 115 || responses[0].OutputTokens != 20 || responses[0].ReasoningTokens != 3 || responses[0].CacheReadTokens != 10 || responses[0].CacheWriteTokens != 5 || responses[0].TotalTokens != 138 || responses[0].UsageSource != string(provider.UsageNative) {
		t.Fatalf("response native usage fields missing: %#v", responses[0])
	}
}

func TestProviderRequestRecordsProviderEstimateMetadata(t *testing.T) {
	rec := &event.Recorder{}
	p := &hashingProvider{
		ScriptedProvider: harness.NewScriptedProvider(harness.Step(harness.Text("ok"), harness.Done())),
		cache:            cache.CachePolicy{Enabled: true, Namespace: "provider-ns", Retention: cache.RetentionLong},
		estimate: provider.TokenEstimate{
			EstimatedInputTokens: 1234,
			Source:               "provider_api",
			Confidence:           provider.EstimateConservative,
		},
	}
	promptStore := cache.NewMemoryStore()
	e := newTestEngine(p, rec)
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
		t.Fatalf("requests = %#v", requests)
	}
	estimate := requests[0].RequestEstimate
	if estimate.EstimatedInputTokens != 1234 || estimate.Source != "provider_api" || estimate.Confidence != contextpolicy.EstimateConfidence(provider.EstimateConservative) {
		t.Fatalf("request estimate did not use provider estimate: %#v", estimate)
	}
	pressure := requests[0].ProjectedPressure
	if pressure.ProjectedInputTokens != 1234 || pressure.Source != contextpolicy.PressureSourceFullRequestEstimate || pressure.Signal != contextpolicy.PressureSignalProjected {
		t.Fatalf("request projected pressure missing provider estimate: %#v", pressure)
	}
	providerRequest := firstEvent(rec.Events, event.ProviderRequest)
	meta, ok := providerRequest.Metadata.(map[string]any)
	if !ok {
		t.Fatalf("provider request metadata = %#v", providerRequest.Metadata)
	}
	if meta["estimated_input_tokens"] != int64(1234) || meta["pressure_source"] != contextpolicy.PressureSourceFullRequestEstimate || meta["pressure_signal"] != contextpolicy.PressureSignalProjected {
		t.Fatalf("provider request metadata missing estimate/pressure details: %#v", meta)
	}
}

func TestProjectionUsesLatestNativeUsageAnchorAcrossTurns(t *testing.T) {
	scripted := harness.NewScriptedProvider(
		harness.Step(
			provider.StreamEvent{Type: provider.UsageEvent, Usage: provider.Usage{InputTokens: 80, WindowInputTokens: 100, Source: provider.UsageNative}},
			harness.Text("first"),
			harness.Done(),
		),
		harness.Step(harness.Text("second"), harness.Done()),
	)
	p := &estimatingProvider{
		Provider: scripted,
		estimates: []provider.TokenEstimate{
			{
				PrefixTokens:         10,
				MessageTokens:        20,
				ToolDefinitionTokens: 5,
				EstimatedInputTokens: 35,
				Source:               "provider_api",
				Confidence:           provider.EstimateApproximate,
			},
			{
				PrefixTokens:         10,
				MessageTokens:        50,
				ToolDefinitionTokens: 5,
				EstimatedInputTokens: 65,
				Source:               "provider_api",
				Confidence:           provider.EstimateApproximate,
			},
		},
	}
	e := newTestEngine(p, &event.Recorder{})
	e.Options.ContextPolicy = contextpolicy.Policy{ContextWindowTokens: 1000, ReservedOutputTokens: 100}

	first := e.Run(context.Background(), "hello")
	if first.Status != engine.Completed {
		t.Fatalf("first result = %#v", first)
	}
	second := e.Run(context.Background(), "again")
	if second.Status != engine.Completed {
		t.Fatalf("second result = %#v", second)
	}
	if len(scripted.Requests) != 2 {
		t.Fatalf("provider requests = %#v", scripted.Requests)
	}
	pressure := scripted.Requests[1].ContextPressure
	if pressure.Signal != contextpolicy.PressureSignalProjected ||
		pressure.Source != contextpolicy.PressureSourceUsageAnchoredDelta ||
		pressure.ProjectedInputTokens != 130 {
		t.Fatalf("second request should use native usage anchor plus rendered delta: %#v", pressure)
	}
	responses, err := e.Prompt.ProviderResponses(context.Background(), "run")
	if err != nil {
		t.Fatal(err)
	}
	if len(responses) == 0 || responses[0].PressureAnchor.WindowInputTokens != 100 || responses[0].PressureAnchor.LastMessageEntryID == "" {
		t.Fatalf("native usage anchor not persisted: %#v", responses)
	}
}

func TestProviderEstimatorErrorBlocksRequest(t *testing.T) {
	p := &estimatingProvider{
		Provider: harness.NewScriptedProvider(harness.Step(harness.Text("should not run"), harness.Done())),
		err:      errors.New("estimate failed"),
	}
	promptStore := cache.NewMemoryStore()
	e := newTestEngine(p, &event.Recorder{})
	e.Prompt = promptStore

	got := e.Run(context.Background(), "hello")

	if got.Status != engine.Failed || got.Err == nil || !strings.Contains(got.Err.Error(), "estimate failed") {
		t.Fatalf("result = %#v", got)
	}
	requests, err := promptStore.ProviderRequests(context.Background(), "run")
	if err != nil {
		t.Fatal(err)
	}
	if len(requests) != 0 {
		t.Fatalf("estimator failure should block request recording, got %#v", requests)
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
			{Type: provider.Done, Reason: "tool_calls"},
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
	p1 := harness.NewScriptedProvider(harness.Step(harness.Tool("ask", "ask_user", `{"question":"Which file?"}`), harness.DoneReason("tool_calls")))
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

func TestToolOutputProjectionAppliesBeforeHistoryAndNextProviderRequest(t *testing.T) {
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
			Permission:  tools.PermissionSpec{Mode: tools.PermissionAllow},
			OutputPolicy: tools.OutputPolicy{
				VisibleMaxBytes: 8,
				Strategy:        tools.OutputTail, PreserveFull: true,
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

func TestToolOutputProjectionFailsWhenPreservingWithoutArtifactStore(t *testing.T) {
	rec := &event.Recorder{}
	p := harness.NewScriptedProvider(
		harness.Step(harness.Tool("read-1", "read", `{"value":"x"}`), harness.DoneReason("tool_calls")),
	)
	reg := tools.NewRegistry()
	mustRegister(t, reg, tools.Define[stringArgs](
		tools.Definition{
			Name:        "read",
			InputSchema: tools.StrictObject(map[string]any{"value": tools.String("value")}, []string{"value"}),
			ReadOnly:    true,
			Permission:  tools.PermissionSpec{Mode: tools.PermissionAllow},
			OutputPolicy: tools.OutputPolicy{
				VisibleMaxBytes: 8,
				Strategy:        tools.OutputTail, PreserveFull: true,
			},
		},
		nil,
		nil,
		func(context.Context, tools.Invocation[stringArgs]) (tools.Result, error) {
			return tools.Result{Text: "0123456789abcdef"}, nil
		},
	))
	e := newTestEngine(p, rec)
	e.Tools = reg
	e.Artifacts = nil

	got := e.Run(context.Background(), "read")

	if got.Status != engine.Failed || got.Err == nil || !strings.Contains(got.Err.Error(), "artifact store") {
		t.Fatalf("result = %#v, want artifact store failure", got)
	}
	if len(p.Requests) != 1 {
		t.Fatalf("provider should not receive a follow-up request after projection failure: %d", len(p.Requests))
	}
	if !slices.ContainsFunc(rec.Events, func(ev event.Event) bool {
		return ev.Type == event.ToolResult && ev.ToolName == "read" && strings.Contains(ev.Err, "artifact store")
	}) {
		t.Fatalf("tool result failure event missing: %#v", rec.Events)
	}
}

func TestErrorToolOutputProjectionPreservesErrorPrefixMetadataAndArtifacts(t *testing.T) {
	rec := &event.Recorder{}
	p := harness.NewScriptedProvider(
		harness.Step(harness.Tool("run-1", "shell", `{"value":"x"}`), harness.DoneReason("tool_calls")),
		harness.Step(harness.Text("recovered"), harness.Done()),
	)
	reg := tools.NewRegistry()
	mustRegister(t, reg, tools.Define[stringArgs](
		tools.Definition{
			Name:         "shell",
			InputSchema:  tools.StrictObject(map[string]any{"value": tools.String("value")}, []string{"value"}),
			Permission:   tools.PermissionSpec{Mode: tools.PermissionAllow},
			OutputPolicy: tools.OutputPolicy{VisibleMaxBytes: 8, Strategy: tools.OutputTail, PreserveFull: true},
		},
		nil,
		nil,
		func(context.Context, tools.Invocation[stringArgs]) (tools.Result, error) {
			return tools.Result{
				Text:     "0123456789abcdef",
				Metadata: map[string]any{"exit_code": 7},
				IsError:  true,
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
		tools.Definition{Name: "local_read", InputSchema: tools.StrictObject(map[string]any{"value": tools.String("value")}, []string{"value"}), ReadOnly: true},
		nil,
		nil,
		func(context.Context, tools.Invocation[stringArgs]) (tools.Result, error) {
			called = true
			return tools.Result{Text: "bad"}, nil
		},
	))
	e := newTestEngine(p, &event.Recorder{})
	e.Tools = reg
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

func TestProviderRequestRejectsLocalHostedToolNameConflict(t *testing.T) {
	p := harness.NewScriptedProvider(harness.Step(harness.Text("unused"), harness.Done()))
	reg := tools.NewRegistry()
	mustRegister(t, reg, tools.Define[stringArgs](
		tools.Definition{Name: "web_search", InputSchema: tools.StrictObject(map[string]any{"query": tools.String("query")}, []string{"query"}), ReadOnly: true},
		nil,
		nil,
		func(context.Context, tools.Invocation[stringArgs]) (tools.Result, error) {
			return tools.Result{Text: "bad"}, nil
		},
	))
	e := newTestEngine(p, &event.Recorder{})
	e.Tools = reg
	e.Options.HostedToolDefinitions = []provider.HostedToolDefinition{{Name: "web_search", Type: "web_search"}}

	got := e.Run(context.Background(), "search")

	if got.Status != engine.Failed || got.Err == nil || !strings.Contains(got.Err.Error(), "both a local tool and a provider-hosted tool") {
		t.Fatalf("result = %#v", got)
	}
	if len(p.Requests) != 0 {
		t.Fatalf("conflicting request should not reach provider: %#v", p.Requests)
	}
}

func TestEngineExposesOnlyRegistryVisibleLocalTools(t *testing.T) {
	p := harness.NewScriptedProvider(
		harness.Step(harness.Tool("hidden-1", "hidden", `{"value":"x"}`), harness.DoneReason("tool_calls")),
		harness.Step(harness.Text("recovered"), harness.Done()),
	)
	reg := tools.NewRegistry()
	hiddenCalled := false
	mustRegister(t, reg, stringTool("visible", "Visible", true, tools.PermissionSpec{}, func(context.Context, string) (string, error) {
		return "visible", nil
	}))
	mustRegister(t, reg, stringTool("needs_approval", "Needs approval", false, tools.PermissionSpec{Mode: tools.PermissionAsk}, func(context.Context, string) (string, error) {
		return "ask", nil
	}))
	mustRegister(t, reg, stringTool("hidden", "Hidden", false, tools.PermissionSpec{Mode: tools.PermissionDeny}, func(context.Context, string) (string, error) {
		hiddenCalled = true
		return "hidden", nil
	}))
	e := newTestEngine(p, &event.Recorder{})
	e.Tools = reg

	got := e.Run(context.Background(), "work")

	if got.Status != engine.Completed || got.Output != "recovered" {
		t.Fatalf("result = %#v", got)
	}
	if hiddenCalled {
		t.Fatalf("deny tool handler should not run")
	}
	if len(p.Requests) < 1 {
		t.Fatalf("provider was not called")
	}
	if !hasProviderTool(p.Requests[0].Tools, "visible") || !hasProviderTool(p.Requests[0].Tools, "needs_approval") || hasProviderTool(p.Requests[0].Tools, "hidden") {
		t.Fatalf("provider-visible toolset = %#v", p.Requests[0].Tools)
	}
}

func TestEngineRejectsInvalidHostedToolDefinitions(t *testing.T) {
	cases := []struct {
		name   string
		hosted []provider.HostedToolDefinition
		want   string
	}{
		{name: "empty name", hosted: []provider.HostedToolDefinition{{Type: "web_search"}}, want: "name is required"},
		{name: "empty type", hosted: []provider.HostedToolDefinition{{Name: "web_search"}}, want: "type is required"},
		{name: "reserved", hosted: []provider.HostedToolDefinition{{Name: "ask_user", Type: "control"}}, want: "reserved"},
		{name: "duplicate", hosted: []provider.HostedToolDefinition{{Name: "web_search", Type: "web_search"}, {Name: "web_search", Type: "other"}}, want: "duplicate hosted tool name"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			p := harness.NewScriptedProvider(harness.Step(harness.Text("unused"), harness.Done()))
			e := newTestEngine(p, &event.Recorder{})
			e.Options.HostedToolDefinitions = tt.hosted
			got := e.Run(context.Background(), "work")
			if got.Status != engine.Failed || got.Err == nil || !strings.Contains(got.Err.Error(), tt.want) {
				t.Fatalf("result = %#v, want %q", got, tt.want)
			}
			if len(p.Requests) != 0 {
				t.Fatalf("invalid hosted tools should not reach provider: %#v", p.Requests)
			}
		})
	}
}

func TestProviderHostedToolEventMustMatchRequestedHostedTool(t *testing.T) {
	p := harness.NewScriptedProvider(harness.Step(
		provider.StreamEvent{Type: provider.HostedToolCall, ToolCall: provider.ToolCall{ID: "hosted-1", Name: "other_search", Args: `{"query":"x"}`}},
		harness.Done(),
	))
	e := newTestEngine(p, &event.Recorder{})
	e.Options.HostedToolDefinitions = []provider.HostedToolDefinition{{Name: "web_search", Type: "web_search"}}
	got := e.Run(context.Background(), "search")
	if got.Status != engine.Failed || got.Err == nil || !strings.Contains(got.Err.Error(), "unrequested hosted tool") {
		t.Fatalf("result = %#v", got)
	}
}

func TestUnknownProviderEventFailsClearly(t *testing.T) {
	p := harness.NewScriptedProvider(harness.Step(provider.StreamEvent{Type: "mystery"}))
	e := newTestEngine(p, &event.Recorder{})
	got := e.Run(context.Background(), "work")
	if got.Status != engine.Failed || got.Err == nil || !strings.Contains(got.Err.Error(), "unknown provider event type") {
		t.Fatalf("result = %#v", got)
	}
}

func TestProviderStreamMissingTerminalFails(t *testing.T) {
	e := newTestEngine(missingTerminalProvider{}, &event.Recorder{})
	got := e.Run(context.Background(), "work")
	if got.Status != engine.Failed || !errors.Is(got.Err, provider.ErrStreamMissingTerminal) {
		t.Fatalf("result = %#v, want missing terminal failure", got)
	}
}

func TestProviderStreamEventAfterTerminalFails(t *testing.T) {
	e := newTestEngine(eventAfterTerminalProvider{}, &event.Recorder{})
	got := e.Run(context.Background(), "work")
	if got.Status != engine.Failed || got.Err == nil || !strings.Contains(got.Err.Error(), "after terminal") {
		t.Fatalf("result = %#v, want post-terminal event failure", got)
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
			harness.DoneReason("tool_calls"),
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
			harness.DoneReason("tool_calls"),
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
	if len(p.Requests) != 2 {
		t.Fatalf("provider requests = %d, want failed request plus compacted retry", len(p.Requests))
	}
	retryMessages := p.Requests[1].Messages
	activeStart := firstNonSystemMessageIndex(retryMessages)
	if activeStart < 0 || len(retryMessages[activeStart:]) < 2 || retryMessages[activeStart].Kind != session.MessageKindCompactionSummary {
		t.Fatalf("retry request should start with checkpoint followed by retained tail: %#v", retryMessages)
	}
	if got := countMessagesByKind(retryMessages, session.MessageKindCompactionSummary); got != 1 {
		t.Fatalf("retry request checkpoint count = %d, want 1: %#v", got, retryMessages)
	}
	checkpoint := retryMessages[activeStart]
	if strings.Count(checkpoint.Content, "<compaction_summary") != 1 {
		t.Fatalf("retry checkpoint should contain one summary envelope: %q", checkpoint.Content)
	}
	if len(retryMessages[activeStart:]) != 2 || retryMessages[activeStart+1].Content != "newer" {
		t.Fatalf("retry active projection should be checkpoint plus retained tail: %#v", retryMessages[activeStart:])
	}
	if got := countMessagesWithExactContent(retryMessages, "newer"); got != 1 {
		t.Fatalf("retained tail user count = %d, want 1: %#v", got, retryMessages)
	}
	segments, err := e.Prompt.Segments(context.Background(), "run", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(segments, func(seg cache.Segment) bool {
		return seg.Kind == cache.SegmentCompaction
	}) {
		t.Fatalf("compaction raw segment missing: %#v", segments)
	}
	for _, want := range []string{"older", "newer"} {
		if !slices.ContainsFunc(segments, func(seg cache.Segment) bool {
			return seg.Kind == cache.SegmentUserMessage && seg.Message.Content == want
		}) {
			t.Fatalf("raw segment %q should remain append-only after compaction: %#v", want, segments)
		}
	}
}

func TestProviderRequestKeepsUnsetMaxOutputTokens(t *testing.T) {
	e := newTestEngine(harness.NewScriptedProvider(), &event.Recorder{})
	e.Options.ContextPolicy = contextpolicy.Policy{
		ContextWindowTokens:  8192,
		MaxOutputTokens:      0,
		ReservedOutputTokens: 1024,
	}
	req, err := buildProviderRequestForTest(context.Background(), e, 0, []session.Message{{Role: session.User, Content: "hello"}})
	if err != nil {
		t.Fatal(err)
	}
	if req.MaxOutputTokens != 0 || req.ContextPolicy.MaxOutputTokens != 0 {
		t.Fatalf("max output should remain unset: max=%d policy=%#v", req.MaxOutputTokens, req.ContextPolicy)
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
		return message.Role == session.User && message.Kind == session.MessageKindCompactionSummary
	}) {
		t.Fatalf("provider request did not use compacted active projection: %#v", p.Requests[0].Messages)
	}
	prepare := firstEvent(rec.Events, event.ContextCompact)
	if prepare.Metrics == nil {
		t.Fatalf("context compact prepare event missing usage metrics: %#v", rec.Events)
	}
	usage, ok := prepare.Metrics.(contextpolicy.Usage)
	if !ok {
		t.Fatalf("context compact prepare metrics = %T, want contextpolicy.Usage", prepare.Metrics)
	}
	if usage.OutputHeadroom == 0 || usage.AutoCompactRatio != contextpolicy.DefaultAutoCompactRatioPercent {
		t.Fatalf("context compact usage missing budget fields: %#v", usage)
	}
}

func TestProviderEstimateTriggersPreRequestCompaction(t *testing.T) {
	rec := &event.Recorder{}
	p := &estimatingProvider{
		Provider: harness.NewScriptedProvider(harness.Step(harness.Text("ok"), harness.Done())),
		estimates: []provider.TokenEstimate{
			{EstimatedInputTokens: 1000, Source: "provider_api", Confidence: provider.EstimateConservative},
			{EstimatedInputTokens: 10, Source: "provider_api", Confidence: provider.EstimateConservative},
		},
	}
	store := &replaceCountingStore{inner: session.NewMemoryStore()}
	if err := store.Append("run",
		session.Message{Role: session.User, Content: "old request", EntryID: "u1"},
		session.Message{Role: session.Assistant, Content: "old answer", EntryID: "a1"},
		session.Message{Role: session.User, Content: "new", EntryID: "u2"},
	); err != nil {
		t.Fatal(err)
	}
	e := newTestEngine(p, rec)
	e.Store = store
	e.Compactor = engine.LocalCompactionManager{Generator: compaction.ExtractiveSummaryGenerator{}}
	e.Options.ContextPolicy = contextpolicy.Policy{ContextWindowTokens: 1000, ReservedOutputTokens: 100, ReservedSummaryTokens: 80, RecentTailTokens: 20, RecentUserTokens: 20}

	got := e.Run(context.Background(), "")

	if got.Status != engine.Completed {
		t.Fatalf("result = %#v", got)
	}
	if got.Metrics.Compactions != 1 || store.replaceCalls != 0 {
		t.Fatalf("provider-estimate compaction not reflected in metrics/store: result=%#v replace=%d", got, store.replaceCalls)
	}
	if len(p.Provider.(*harness.ScriptedProvider).Requests) != 1 {
		t.Fatalf("provider should only receive compacted retry request: %#v", p.Provider.(*harness.ScriptedProvider).Requests)
	}
	prepare := firstEvent(rec.Events, event.ContextCompact)
	meta, ok := prepare.Metadata.(map[string]any)
	if !ok {
		t.Fatalf("context compact metadata = %#v", prepare.Metadata)
	}
	before, ok := meta["before_pressure"].(contextpolicy.ContextPressure)
	if !ok || before.ProjectedInputTokens != 1000 || before.Source != contextpolicy.PressureSourceFullRequestEstimate || before.Signal != contextpolicy.PressureSignalProjected {
		t.Fatalf("provider estimate should drive pre-request pressure: %#v", prepare.Metadata)
	}
}

func TestCompactionPolicyUsesMessageContextPrefixBudget(t *testing.T) {
	p := &estimatingProvider{
		Provider: harness.NewScriptedProvider(harness.Step(harness.Text("ok"), harness.Done())),
		estimate: provider.TokenEstimate{
			PrefixTokens:         100,
			MessageTokens:        900,
			ToolDefinitionTokens: 200,
			EstimatedInputTokens: 1200,
			Source:               "provider_api",
			Confidence:           provider.EstimateConservative,
		},
	}
	store := session.NewMemoryStore()
	if err := store.Append("run", session.Message{Role: session.User, Content: "old", EntryID: "u1"}); err != nil {
		t.Fatal(err)
	}
	compactor := &policyRecordingCompactor{}
	e := newTestEngine(p, &event.Recorder{})
	e.Store = store
	e.Compactor = compactor
	e.Options.ContextPolicy = contextpolicy.Policy{ContextWindowTokens: 1000, ReservedOutputTokens: 100, ReservedSummaryTokens: 80, RecentTailTokens: 20}

	got := e.Run(context.Background(), "")

	messageContext := contextpolicy.EstimateMessageContext(e.SystemPrompt, []session.Message{{Role: session.User, Content: "old", EntryID: "u1"}}, e.Options.ContextPolicy)
	wantWindow := e.Options.ContextPolicy.ContextWindowTokens - messageContext.PrefixTokens
	if got.Status != engine.Failed || compactor.policy.ContextWindowTokens != wantWindow {
		t.Fatalf("compaction policy should stay message-context scoped: result=%#v policy=%#v", got, compactor.policy)
	}
}

func TestGenericRequestEstimateIncludesToolsForProvidersWithoutEstimator(t *testing.T) {
	rec := &event.Recorder{}
	p := harness.NewScriptedProvider(harness.Step(harness.Text("ok"), harness.Done()))
	promptStore := cache.NewMemoryStore()
	e := newTestEngine(p, rec)
	e.Prompt = promptStore
	if err := e.Tools.Register(tools.Define[struct{}](
		tools.Definition{
			Name:        "large_tool",
			Description: strings.Repeat("Large schema tool. ", 20),
			Permission:  tools.PermissionSpec{Mode: tools.PermissionAllow},
			InputSchema: map[string]any{
				"type":                 "object",
				"additionalProperties": false,
				"properties": map[string]any{
					"value": map[string]any{"type": "string", "description": strings.Repeat("Detailed value. ", 20)},
				},
			},
		},
		nil,
		nil,
		func(context.Context, tools.Invocation[struct{}]) (tools.Result, error) {
			return tools.Result{Text: "ok"}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}

	got := e.Run(context.Background(), "hello")

	if got.Status != engine.Completed {
		t.Fatalf("result = %#v", got)
	}
	requests, err := promptStore.ProviderRequests(context.Background(), "run")
	if err != nil {
		t.Fatal(err)
	}
	if len(requests) != 1 {
		t.Fatalf("requests = %#v", requests)
	}
	messageOnly := contextpolicy.EstimateMessageContext("", []session.Message{{Role: session.User, Content: "hello"}}, e.Options.ContextPolicy)
	estimate := requests[0].RequestEstimate
	if estimate.EstimatedInputTokens <= messageOnly.InputTokens || estimate.Source != "generic_request_json" {
		t.Fatalf("generic request estimate should include rendered tools: estimate=%#v messageOnly=%#v", estimate, messageOnly)
	}
}

func TestPreRequestThresholdUsesMaxOutputHeadroom(t *testing.T) {
	rec := &event.Recorder{}
	p := harness.NewScriptedProvider(harness.Step(harness.Text("ok"), harness.Done()))
	store := session.NewMemoryStore()
	if err := store.Append("run",
		session.Message{Role: session.User, Content: strings.Repeat("old ", 620000)},
		session.Message{Role: session.User, Content: "new"},
	); err != nil {
		t.Fatal(err)
	}
	e := newTestEngine(p, rec)
	e.Store = store
	e.Compactor = engine.LocalCompactionManager{Generator: compaction.ExtractiveSummaryGenerator{}}
	e.Options.ContextPolicy = contextpolicy.Policy{
		ContextWindowTokens:   1000000,
		MaxOutputTokens:       384000,
		ReservedOutputTokens:  4096,
		ReservedSummaryTokens: 20000,
		RecentTailTokens:      12000,
	}

	got := e.Run(context.Background(), "")

	if got.Status != engine.Completed {
		t.Fatalf("result = %#v", got)
	}
	if got.Metrics.Compactions != 1 {
		t.Fatalf("compactions = %d, want 1", got.Metrics.Compactions)
	}
	prepare := firstEvent(rec.Events, event.ContextCompact)
	usage, ok := prepare.Metrics.(contextpolicy.Usage)
	if !ok {
		t.Fatalf("context compact metrics = %T, want contextpolicy.Usage", prepare.Metrics)
	}
	if usage.ThresholdTokens != 616000 || usage.OutputHeadroom != 384000 || usage.MaxOutputTokens != 384000 {
		t.Fatalf("compaction should use max output headroom: %#v", usage)
	}
}

func TestPreRequestThresholdRequiresExplicitCompactor(t *testing.T) {
	p := harness.NewScriptedProvider(harness.Step(harness.Text("ok"), harness.Done()))
	store := session.NewMemoryStore()
	if err := store.Append("run",
		session.Message{Role: session.User, Content: strings.Repeat("old ", 1200)},
		session.Message{Role: session.User, Content: "new"},
	); err != nil {
		t.Fatal(err)
	}
	e := newTestEngine(p, &event.Recorder{})
	e.Store = store
	e.Options.ContextPolicy = contextpolicy.Policy{ContextWindowTokens: 900, ReservedOutputTokens: 80, ReservedSummaryTokens: 80, RecentTailTokens: 20}

	got := e.Run(context.Background(), "")

	if got.Status != engine.Failed || got.Err == nil || got.Err.Error() != "compaction manager is required when context exceeds policy" {
		t.Fatalf("result = %#v, want explicit compactor error", got)
	}
	if len(p.Requests) != 0 {
		t.Fatalf("provider should not receive request after missing compactor: %#v", p.Requests)
	}
}

func TestLocalCompactionManagerRequiresExplicitGenerator(t *testing.T) {
	_, _, err := engine.LocalCompactionManager{}.Compact(context.Background(), engine.CompactionRequest{
		History: []session.Message{
			{Role: session.User, Content: "old", EntryID: "u1"},
			{Role: session.User, Content: "new", EntryID: "u2"},
		},
		Policy: contextpolicy.Policy{ContextWindowTokens: 900, ReservedOutputTokens: 80, ReservedSummaryTokens: 80, RecentTailTokens: 20},
	})
	if err == nil || err.Error() != "local compaction manager requires summary generator" {
		t.Fatalf("err = %v, want explicit generator error", err)
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
			[]provider.StreamEvent{
				{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{{ID: "x1", Name: "missing", Args: `{"value":"same"}`}}},
				{Type: provider.Done, Reason: "tool_calls"},
			},
			[]provider.StreamEvent{
				{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{{ID: "x2", Name: "missing", Args: `{"value":"same"}`}}},
				{Type: provider.Done, Reason: "tool_calls"},
			},
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

type missingTerminalProvider struct{}

func (missingTerminalProvider) Stream(context.Context, provider.Request) (<-chan provider.StreamEvent, error) {
	ch := make(chan provider.StreamEvent, 1)
	ch <- provider.StreamEvent{Type: provider.Delta, Text: "partial"}
	close(ch)
	return ch, nil
}

type eventAfterTerminalProvider struct{}

func (eventAfterTerminalProvider) Stream(context.Context, provider.Request) (<-chan provider.StreamEvent, error) {
	ch := make(chan provider.StreamEvent, 2)
	ch <- provider.StreamEvent{Type: provider.Done, Reason: "stop"}
	ch <- provider.StreamEvent{Type: provider.Delta, Text: "late"}
	close(ch)
	return ch, nil
}

type barrierProvider struct {
	mu       sync.Mutex
	want     int
	arrived  int
	released chan struct{}
	requests []provider.Request
}

func newBarrierProvider(want int) *barrierProvider {
	return &barrierProvider{want: want, released: make(chan struct{})}
}

func (p *barrierProvider) Stream(ctx context.Context, req provider.Request) (<-chan provider.StreamEvent, error) {
	p.mu.Lock()
	p.requests = append(p.requests, req)
	p.arrived++
	if p.arrived == p.want {
		close(p.released)
	}
	p.mu.Unlock()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.released:
	}
	ch := make(chan provider.StreamEvent, 2)
	ch <- provider.StreamEvent{Type: provider.Delta, Text: "ok " + req.RunID}
	ch <- provider.StreamEvent{Type: provider.Done, Reason: "stop"}
	close(ch)
	return ch, nil
}

func (p *barrierProvider) Requests() []provider.Request {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]provider.Request(nil), p.requests...)
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
	hash     string
	cache    cache.CachePolicy
	estimate provider.TokenEstimate
}

func (p *hashingProvider) NormalizeCachePolicy(cache.CachePolicy) (cache.CachePolicy, error) {
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

func (p *hashingProvider) EstimateTokens(context.Context, provider.Request) (provider.TokenEstimate, error) {
	if p.estimate.Source != "" || p.estimate.EstimatedInputTokens > 0 {
		return p.estimate, nil
	}
	return provider.TokenEstimate{EstimatedInputTokens: 1, Source: "hashing_provider", Confidence: provider.EstimateExact}, nil
}

type estimatingProvider struct {
	provider.Provider
	estimate  provider.TokenEstimate
	estimates []provider.TokenEstimate
	err       error
}

func (p *estimatingProvider) EstimateTokens(context.Context, provider.Request) (provider.TokenEstimate, error) {
	if p.err != nil {
		return provider.TokenEstimate{}, p.err
	}
	if len(p.estimates) > 0 {
		next := p.estimates[0]
		p.estimates = p.estimates[1:]
		return next, nil
	}
	return p.estimate, nil
}

type policyRecordingCompactor struct {
	policy contextpolicy.Policy
}

func (c *policyRecordingCompactor) Compact(_ context.Context, req engine.CompactionRequest) (compaction.Result, []session.Message, error) {
	c.policy = req.Policy
	return compaction.Result{}, nil, errors.New("stop after recording policy")
}

type testEngine struct {
	Provider     provider.Provider
	Store        session.Store
	Prompt       cache.Store
	Artifacts    artifact.Store
	SystemPrompt string
	Tools        *tools.Registry
	Sink         event.Sink
	Approver     tools.Approver
	StopHook     engine.StopHook
	Compactor    engine.CompactionManager
	Options      engine.Options
}

func newTestEngine(p provider.Provider, rec *event.Recorder) *testEngine {
	return &testEngine{
		Provider:     p,
		Store:        session.NewMemoryStore(),
		Prompt:       cache.NewMemoryStore(),
		Artifacts:    artifact.NewMemoryStore(),
		SystemPrompt: "You are Floret.",
		Tools:        tools.NewRegistry(),
		Sink:         rec,
		Options: engine.Options{
			RunID:                   "run",
			MaxEmptyProviderRetries: 1,
			NoProgressLimit:         2,
			DuplicateToolLimit:      3,
		},
	}
}

func (e *testEngine) build(t *testing.T) *engine.Engine {
	t.Helper()
	eng, err := engine.New(engine.Config{
		Provider:     e.Provider,
		Store:        e.Store,
		Prompt:       e.Prompt,
		Artifacts:    e.Artifacts,
		SystemPrompt: e.SystemPrompt,
		Tools:        e.Tools,
		Sink:         e.Sink,
		Approver:     e.Approver,
		StopHook:     e.StopHook,
		Compactor:    e.Compactor,
		Options:      e.Options,
	})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	return eng
}

func (e *testEngine) tryBuild() (*engine.Engine, error) {
	return engine.New(engine.Config{
		Provider:     e.Provider,
		Store:        e.Store,
		Prompt:       e.Prompt,
		Artifacts:    e.Artifacts,
		SystemPrompt: e.SystemPrompt,
		Tools:        e.Tools,
		Sink:         e.Sink,
		Approver:     e.Approver,
		StopHook:     e.StopHook,
		Compactor:    e.Compactor,
		Options:      e.Options,
	})
}

func (e *testEngine) Run(ctx context.Context, userText string) engine.Result {
	eng, err := e.tryBuild()
	if err != nil {
		return engine.Result{Status: engine.Failed, Err: err}
	}
	return eng.Run(ctx, userText)
}

func (e *testEngine) RunTurn(ctx context.Context, input engine.RunInput) engine.Result {
	eng, err := e.tryBuild()
	if err != nil {
		return engine.Result{Status: engine.Failed, Err: err}
	}
	return eng.RunTurn(ctx, input)
}

func promptCachePathForTest(value string) string {
	return "id_" + base64.RawURLEncoding.EncodeToString([]byte(value))
}

func buildProviderRequestForTest(ctx context.Context, e *testEngine, step int, history []session.Message) (provider.Request, error) {
	if e.Prompt == nil {
		e.Prompt = cache.NewMemoryStore()
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
		opts.CacheNamespace = cache.DefaultNamespace(opts.SessionID, opts.ProviderName, opts.Model)
	}
	toolset, _, err := cache.EnsureToolset(ctx, e.Prompt, opts.RunID, opts.SessionID, opts.ProviderName, opts.Model, nil, nil, time.Now())
	if err != nil {
		return provider.Request{}, err
	}
	plan, messages, err := cache.BuildPlan(ctx, e.Prompt, cache.BuildInput{
		RunID:          opts.RunID,
		SessionID:      opts.SessionID,
		Provider:       opts.ProviderName,
		Model:          opts.Model,
		AdapterVersion: cache.Version,
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
	if permission.Mode == "" {
		permission.Mode = tools.PermissionAllow
	}
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

func firstEvent(events []event.Event, typ event.Type) event.Event {
	for _, ev := range events {
		if ev.Type == typ {
			return ev
		}
	}
	return event.Event{}
}

func hasProviderTool(defs []provider.ToolDefinition, name string) bool {
	return slices.ContainsFunc(defs, func(def provider.ToolDefinition) bool {
		return def.Name == name
	})
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

func countMessagesByKind(messages []session.Message, kind session.MessageKind) int {
	count := 0
	for _, msg := range messages {
		if msg.Kind == kind {
			count++
		}
	}
	return count
}

func countMessagesWithExactContent(messages []session.Message, content string) int {
	count := 0
	for _, msg := range messages {
		if msg.Content == content {
			count++
		}
	}
	return count
}

func firstNonSystemMessageIndex(messages []session.Message) int {
	for i, msg := range messages {
		if msg.Role != session.System {
			return i
		}
	}
	return -1
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

func segmentRawsForTest(segments []cache.Segment) []string {
	out := make([]string, len(segments))
	for i, segment := range segments {
		out[i] = segment.Raw
	}
	return out
}

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

	"github.com/floegence/floret/engine"
	"github.com/floegence/floret/event"
	"github.com/floegence/floret/harness"
	"github.com/floegence/floret/memory"
	"github.com/floegence/floret/promptcache"
	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/session"
	"github.com/floegence/floret/tools"
)

func TestRunDirectAnswerCompletesThroughExplicitSignal(t *testing.T) {
	rec := &event.Recorder{}
	p := harness.NewScriptedProvider(
		[]provider.StreamEvent{
			{Type: provider.Delta, Text: "I checked it. "},
			{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{{ID: "done", Name: "task_complete", Args: "done"}}},
			{Type: provider.Done},
		},
	)
	e := newTestEngine(p, rec)

	got := e.Run(context.Background(), "do the thing")

	if got.Status != engine.Completed {
		t.Fatalf("status = %s, want completed: %v", got.Status, got.Err)
	}
	if got.Output != "done" {
		t.Fatalf("output = %q, want explicit completion payload", got.Output)
	}
	messages, err := e.Store.Messages("run")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(messages, func(msg session.Message) bool {
		return msg.Role == session.Assistant && msg.ToolCallID == "done" && msg.ToolName == "task_complete"
	}) {
		t.Fatalf("task_complete tool call was not persisted before completion: %#v", messages)
	}
	assertEventOrder(t, rec.Events, event.StepStart, event.ProviderRequest, event.ProviderDelta, event.RunEnd)
}

func TestTaskCompleteToolCallAppearsInRawLedgerWhenRunContinues(t *testing.T) {
	store := session.NewMemoryStore()
	promptStore := promptcache.NewMemoryStore()
	p1 := harness.NewScriptedProvider(harness.Step(harness.Tool("done", "task_complete", "first done")))
	e1 := newTestEngine(p1, &event.Recorder{})
	e1.Store = store
	e1.Prompt = promptStore
	got := e1.Run(context.Background(), "finish")
	if got.Status != engine.Completed {
		t.Fatalf("first result = %#v", got)
	}
	p2 := harness.NewScriptedProvider(harness.Step(harness.Tool("done-2", "task_complete", "second done")))
	e2 := newTestEngine(p2, &event.Recorder{})
	e2.Store = store
	e2.Prompt = promptStore
	got = e2.Run(context.Background(), "continue anyway")
	if got.Status != engine.Completed {
		t.Fatalf("second result = %#v", got)
	}
	if !slices.ContainsFunc(p2.Requests[0].RawPlan.Segments, func(seg promptcache.Segment) bool {
		return seg.Kind == promptcache.SegmentToolCall && seg.Message.ToolName == "task_complete" && seg.Message.ToolCallID == "done"
	}) {
		t.Fatalf("continued run missing task_complete raw segment: %#v", p2.Requests[0].RawPlan.Segments)
	}
}

func TestRunToolLoopFeedsResultIntoNextProviderRequest(t *testing.T) {
	rec := &event.Recorder{}
	p := harness.NewScriptedProvider(
		[]provider.StreamEvent{
			{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{{ID: "read-1", Name: "read", Args: "README.md", ReadOnly: true}}},
			{Type: provider.Done},
		},
		[]provider.StreamEvent{
			{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{{ID: "done", Name: "task_complete", Args: "saw file"}}},
			{Type: provider.Done},
		},
	)
	reg := tools.NewRegistry()
	mustRegister(t, reg, tools.Tool{Name: "read", ReadOnly: true, Handler: func(context.Context, string) (string, error) {
		return "file contents", nil
	}})
	e := newTestEngine(p, rec)
	e.Tools = reg

	got := e.Run(context.Background(), "inspect")

	if got.Status != engine.Completed || got.Output != "saw file" {
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

func TestPromptCacheFreezesToolsetWhenRegistryChanges(t *testing.T) {
	p := harness.NewScriptedProvider(
		harness.Step(harness.Tool("read-1", "read", "{}")),
		harness.Step(harness.Tool("done", "task_complete", "ok")),
	)
	reg := tools.NewRegistry()
	mustRegister(t, reg, tools.Tool{Name: "read", Description: "Read original", Handler: func(context.Context, string) (string, error) {
		if err := reg.Register(tools.Tool{Name: "write", Description: "Added later", Handler: func(context.Context, string) (string, error) {
			return "write", nil
		}}); err != nil {
			t.Fatalf("register during run: %v", err)
		}
		return "content", nil
	}})
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
		if len(req.Tools) != 1 || req.Tools[0].Name != "read" || req.Tools[0].Description != "Read original" {
			t.Fatalf("request toolset changed after registry mutation: %#v", req.Tools)
		}
	}
	if p.Requests[0].RawPlan.ToolsetID != p.Requests[1].RawPlan.ToolsetID || p.Requests[1].RawPlan.ToolsetEpoch != 1 {
		t.Fatalf("toolset snapshot was not reused: first=%#v second=%#v", p.Requests[0].RawPlan, p.Requests[1].RawPlan)
	}
}

func TestPromptCacheFileStoreKeepsPrefixStableAcrossEngineRestart(t *testing.T) {
	store := session.NewMemoryStore()
	root := t.TempDir()
	promptStore := promptcache.NewFileStore(root)
	firstProvider := harness.NewScriptedProvider(harness.Step(harness.Tool("ask", "ask_user", "more?")))
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
	secondProvider := harness.NewScriptedProvider(harness.Step(harness.Tool("done", "task_complete", "ok")))
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
	p := &hashingProvider{ScriptedProvider: harness.NewScriptedProvider(harness.Step(harness.Tool("done", "task_complete", "ok")))}
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

func TestAskUserSignalReturnsWaitingWithoutExecutingTool(t *testing.T) {
	rec := &event.Recorder{}
	p := harness.NewScriptedProvider(
		[]provider.StreamEvent{
			{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{{ID: "ask", Name: "ask_user", Args: "Which file?"}}},
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

func TestWaitingCanResumeByAppendingUserAnswerToSameRun(t *testing.T) {
	store := session.NewMemoryStore()
	p1 := harness.NewScriptedProvider(harness.Step(harness.Tool("ask", "ask_user", "Which file?")))
	e1 := newTestEngine(p1, &event.Recorder{})
	e1.Store = store
	got := e1.Run(context.Background(), "continue")
	if got.Status != engine.Waiting {
		t.Fatalf("first result = %#v", got)
	}
	p2 := harness.NewScriptedProvider(harness.Step(harness.Tool("done", "task_complete", "resumed")))
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
	if !slices.ContainsFunc(p2.Requests[0].RawPlan.Segments, func(seg promptcache.Segment) bool {
		return seg.Kind == promptcache.SegmentToolCall && seg.Message.ToolName == "ask_user"
	}) {
		t.Fatalf("resume request missing persisted ask_user raw segment: %#v", p2.Requests[0].RawPlan.Segments)
	}
}

func TestApprovalDeniedReturnsToolErrorAndAllowsModelRecovery(t *testing.T) {
	rec := &event.Recorder{}
	p := harness.NewScriptedProvider(
		[]provider.StreamEvent{
			{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{{ID: "write-1", Name: "write", Args: "danger"}}},
			{Type: provider.Done},
		},
		[]provider.StreamEvent{
			{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{{ID: "done", Name: "task_complete", Args: "not changed"}}},
			{Type: provider.Done},
		},
	)
	reg := tools.NewRegistry()
	called := false
	mustRegister(t, reg, tools.Tool{Name: "write", RequiresApproval: true, Handler: func(context.Context, string) (string, error) {
		called = true
		return "changed", nil
	}})
	e := newTestEngine(p, rec)
	e.Tools = reg
	e.Approver = func(context.Context, tools.ApprovalRequest) (bool, error) { return false, nil }

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

func TestReadOnlyToolsRunInParallelAndMutatingToolsKeepOrder(t *testing.T) {
	reg := tools.NewRegistry()
	order := make(chan string, 4)
	release := make(chan struct{})
	mustRegister(t, reg, tools.Tool{Name: "ro", ReadOnly: true, Handler: func(_ context.Context, arg string) (string, error) {
		order <- "start-" + arg
		<-release
		order <- "end-" + arg
		return arg, nil
	}})
	mustRegister(t, reg, tools.Tool{Name: "mut", Handler: func(_ context.Context, arg string) (string, error) {
		order <- "mut-" + arg
		return arg, nil
	}})
	done := make(chan []tools.Result, 1)
	go func() {
		done <- reg.RunBatch(context.Background(), []provider.ToolCall{
			{ID: "a", Name: "ro", Args: "a", ReadOnly: true},
			{ID: "b", Name: "ro", Args: "b", ReadOnly: true},
			{ID: "c", Name: "mut", Args: "c"},
		}, nil)
	}()
	first := <-order
	second := <-order
	if !sameSet([]string{first, second}, []string{"start-a", "start-b"}) {
		t.Fatalf("read-only tools did not both start before release: %q %q", first, second)
	}
	close(release)
	results := <-done
	if len(results) != 3 || results[2].Call.ID != "c" {
		t.Fatalf("results are not in call order: %#v", results)
	}
}

func TestProviderEmptyOutputRetriesThenCompletes(t *testing.T) {
	rec := &event.Recorder{}
	p := harness.NewScriptedProvider(
		[]provider.StreamEvent{{Type: provider.Empty}},
		[]provider.StreamEvent{
			{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{{ID: "done", Name: "task_complete", Args: "ok"}}},
			{Type: provider.Done},
		},
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
			harness.Tool("done", "task_complete", "ok"),
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
			harness.Tool("done", "task_complete", "ok"),
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
		p := harness.NewScriptedProvider(harness.Step(harness.Usage(provider.Usage{InputTokens: 101}), harness.Tool("done", "task_complete", "ok")))
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
		p := harness.NewScriptedProvider(harness.Step(harness.Usage(provider.Usage{CostUSD: 2, TotalTokens: 1}), harness.Tool("done", "task_complete", "ok")))
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
				{ID: "a", Name: "missing", Args: "a"},
				{ID: "b", Name: "missing", Args: "b"},
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
			{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{{ID: "done", Name: "task_complete", Args: "after compact"}}},
			{Type: provider.Done},
		},
	)
	p.Errs[1] = provider.ErrContextOverflow
	store := session.NewMemoryStore()
	if err := store.Append("run", session.Message{Role: session.User, Content: "older"}, session.Message{Role: session.User, Content: "newer"}); err != nil {
		t.Fatal(err)
	}
	e := newTestEngine(p, rec)
	e.Store = store
	e.Memory = &memory.Manager{MaxMessages: 1}
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
	if e.Memory.Compactions != 1 {
		t.Fatalf("compactions = %d, want 1", e.Memory.Compactions)
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

func TestTruncatedProviderOutputCompactsAndRetries(t *testing.T) {
	rec := &event.Recorder{}
	p := harness.NewScriptedProvider(
		[]provider.StreamEvent{{Type: provider.Truncated}},
		[]provider.StreamEvent{
			{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{{ID: "done", Name: "task_complete", Args: "retried"}}},
			{Type: provider.Done},
		},
	)
	e := newTestEngine(p, rec)

	got := e.Run(context.Background(), "work")

	if got.Status != engine.Completed || got.Output != "retried" {
		t.Fatalf("result = %#v", got)
	}
	if !hasEvent(rec.Events, event.ContextCompact) {
		t.Fatalf("truncation did not compact")
	}
}

func TestLoopGuardsMaxStepsDuplicateToolsAndCancellation(t *testing.T) {
	t.Run("max steps", func(t *testing.T) {
		p := harness.NewScriptedProvider(
			[]provider.StreamEvent{{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{{ID: "x1", Name: "missing", Args: "1"}}}},
			[]provider.StreamEvent{{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{{ID: "x2", Name: "missing", Args: "2"}}}},
		)
		e := newTestEngine(p, &event.Recorder{})
		e.Options.MaxSteps = 1
		got := e.Run(context.Background(), "loop")
		if !errors.Is(got.Err, engine.ErrMaxSteps) {
			t.Fatalf("err = %v, want max steps", got.Err)
		}
	})
	t.Run("duplicate tools", func(t *testing.T) {
		p := harness.NewScriptedProvider(
			[]provider.StreamEvent{{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{{ID: "x1", Name: "missing", Args: "same"}}}},
			[]provider.StreamEvent{{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{{ID: "x2", Name: "missing", Args: "same"}}}},
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
				{ID: "same", Name: "missing", Args: "a"},
				{ID: "same", Name: "missing", Args: "b"},
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

	if got.Status != engine.Failed || !errors.Is(got.Err, context.DeadlineExceeded) {
		t.Fatalf("result = %#v, want deadline failure", got)
	}
}

type blockingProvider struct{}

func (blockingProvider) Stream(context.Context, provider.Request) (<-chan provider.StreamEvent, error) {
	return make(chan provider.StreamEvent), nil
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
		Memory:   &memory.Manager{SystemPrompt: "You are Floret.", MaxMessages: 8},
		Tools:    tools.NewRegistry(),
		Sink:     rec,
		Options: engine.Options{
			RunID:                   "run",
			MaxSteps:                8,
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
	toolset, _, err := promptcache.EnsureToolset(ctx, e.Prompt, opts.RunID, opts.SessionID, opts.ProviderName, opts.Model, nil, time.Now())
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
		t.Fatalf("register %s: %v", tool.Name, err)
	}
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

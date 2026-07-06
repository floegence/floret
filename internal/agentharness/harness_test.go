package agentharness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/floegence/floret/internal/engine"
	"github.com/floegence/floret/internal/event"
	"github.com/floegence/floret/internal/provider"
	"github.com/floegence/floret/internal/provider/cache"
	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/session/compaction"
	"github.com/floegence/floret/internal/sessionlifecycle"
	"github.com/floegence/floret/internal/sessiontree"
	"github.com/floegence/floret/internal/storage/sqlite"
	scriptharness "github.com/floegence/floret/internal/testing/harness"
	"github.com/floegence/floret/observation"
	"github.com/floegence/floret/tools"
)

func TestThreadRunPersistsTurnEntriesAndContext(t *testing.T) {
	ctx := context.Background()
	p := scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("done text"), scriptharness.Done()))
	h := newTestHarness(p, sessiontree.NewMemoryRepo(), cache.NewMemoryStore())
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := thread.Run(ctx, "do it", RunOptions{TurnID: "turn-1"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != engine.Completed || result.Output != "done text" {
		t.Fatalf("result = %#v", result)
	}
	snap, err := thread.Journal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !hasEntry(snap.Entries, sessiontree.EntryTurnMarker, sessiontree.TurnStarted) ||
		!hasEntry(snap.Entries, sessiontree.EntryTurnMarker, sessiontree.TurnCompleted) {
		t.Fatalf("turn markers missing: %#v", snap.Entries)
	}
	completed := firstTurnMarker(snap.Entries, sessiontree.TurnCompleted)
	if completed.Metadata["completion_reason"] != string(engine.CompletionReasonNaturalStop) ||
		completed.Metadata["finish_reason"] != string(provider.FinishStop) ||
		completed.Metadata["raw_finish_reason"] != "stop" ||
		completed.Metadata["finish_inferred"] != "false" {
		t.Fatalf("completed marker metadata = %#v", completed.Metadata)
	}
	if countEntries(snap.Entries, sessiontree.EntryUserMessage) != 1 {
		t.Fatalf("user message should be stored exactly once: %#v", snap.Entries)
	}
	if len(snap.Context) != 2 || snap.Context[0].Content != "do it" || snap.Context[1].Content != "done text" || snap.Context[1].ToolName != "" {
		t.Fatalf("provider-visible context = %#v", snap.Context)
	}
}

func TestThreadRunPassesHostLabelsToLocalTools(t *testing.T) {
	ctx := context.Background()
	p := scriptharness.NewScriptedProvider(
		scriptharness.Step(scriptharness.Tool("target-1", "target_echo", `{"value":"inspect"}`), scriptharness.DoneReason("tool_calls")),
		scriptharness.Step(scriptharness.Text("done text"), scriptharness.Done()),
	)
	h := newTestHarness(p, sessiontree.NewMemoryRepo(), cache.NewMemoryStore())
	mustRegister(h.options.Tools, tools.Define[stringArgs](
		tools.Definition{
			Name:        "target_echo",
			InputSchema: tools.StrictObject(map[string]any{"value": tools.String("test value")}, []string{"value"}),
			Permission:  tools.PermissionSpec{Mode: tools.PermissionAllow},
		},
		nil,
		nil,
		func(_ context.Context, inv tools.Invocation[stringArgs]) (tools.Result, error) {
			if inv.HostContext["target_id"] != "target-123" || inv.Labels["host.target_id"] != "target-123" || inv.Labels["correlation.message_id"] != "message-456" {
				t.Fatalf("tool invocation context = %#v labels=%#v", inv.HostContext, inv.Labels)
			}
			return tools.Result{Text: inv.Args.Value + ":" + inv.HostContext["target_id"]}, nil
		},
	))
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := thread.Run(ctx, "do it", RunOptions{
		TurnID: "turn-1",
		Labels: engine.RunLabels{
			Correlation: map[string]string{"message_id": "message-456"},
			Host:        map[string]string{"target_id": "target-123", "surface": "desktop"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != engine.Completed || result.Output != "done text" {
		t.Fatalf("result = %#v", result)
	}
}

func TestThreadRunGeneratesTitleMetadataAfterSuccessfulTurn(t *testing.T) {
	ctx := context.Background()
	rec := &HarnessRecorder{}
	p := scriptharness.NewScriptedProvider(
		scriptharness.Step(scriptharness.Text("Use a focused test plan for every tool."), scriptharness.Done()),
		scriptharness.Step(scriptharness.Text("Streaming and tool-call validation"), scriptharness.Done()),
	)
	h := New(Options{
		Provider:     p,
		ProviderName: "fake",
		Model:        "fake-model",
		SystemPrompt: "You are a test assistant.",
		Tools:        tools.NewRegistry(),
		Repo:         sessiontree.NewMemoryRepo(),
		PromptStore:  cache.NewMemoryStore(),
		Reasoning: provider.ReasoningCapability{
			Kind:             provider.ReasoningKindEffort,
			DisableSupported: true,
		},
		LoopLimits: LoopLimits{
			MaxEmptyProviderRetries: 1,
			NoProgressLimit:         2,
			DuplicateToolLimit:      3,
		},
	})
	h.options.HarnessSink = rec
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := thread.Run(ctx, "Verify streaming output and tool calls", RunOptions{TurnID: "turn-1"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != engine.Completed {
		t.Fatalf("result = %#v", result)
	}
	snap, err := thread.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snap.TitleStatus != string(sessiontree.ThreadTitleReady) || snap.TitleSource != string(sessiontree.ThreadTitleSourceProvider) {
		t.Fatalf("snapshot title = %#v", snap)
	}
	if snap.Title == "" || utf8.RuneCountInString(snap.Title) > defaultThreadTitleMaxRunes {
		t.Fatalf("snapshot title %q should be non-empty and at most %d runes", snap.Title, defaultThreadTitleMaxRunes)
	}
	if len(p.Requests) != 2 || p.Requests[1].LogicalRequestID != "thread_title" || p.Requests[1].RunID != "turn-1:thread-title" || p.Requests[1].Reasoning.Level != provider.ReasoningLevelOff {
		t.Fatalf("title provider request missing: %#v", p.Requests)
	}
	if len(p.Requests[1].Messages) != 2 || !strings.Contains(p.Requests[1].Messages[1].Content, "Verify streaming output and tool calls") {
		t.Fatalf("title provider prompt = %#v", p.Requests[1].Messages)
	}
	meta, err := h.options.Repo.Thread(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if meta.Title != snap.Title || meta.TitleUpdatedAt.IsZero() || meta.UpdatedAt.Equal(meta.TitleUpdatedAt) {
		t.Fatalf("thread title meta = %#v", meta)
	}
	if !slices.ContainsFunc(rec.Snapshot(), func(ev HarnessEvent) bool {
		return ev.Type == EventTitleUpdated && ev.ThreadID == "thread" && ev.TurnID == "turn-1" && ev.Message == snap.Title
	}) {
		t.Fatalf("title update event missing: %#v", rec.Snapshot())
	}
}

func TestThreadRunRecordsTitleGenerationFailureWithoutFailingTurn(t *testing.T) {
	ctx := context.Background()
	rec := &HarnessRecorder{}
	p := scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("done text"), scriptharness.Done()))
	h := newTestHarness(p, sessiontree.NewMemoryRepo(), cache.NewMemoryStore())
	h.options.HarnessSink = rec
	h.options.TitleGenerator = failingTitleGenerator{err: errors.New("summary provider unavailable")}
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := thread.Run(ctx, "do it", RunOptions{TurnID: "turn-1"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != engine.Completed {
		t.Fatalf("result = %#v", result)
	}
	meta, err := h.options.Repo.Thread(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if meta.Title != "" || meta.TitleStatus != sessiontree.ThreadTitleFailed || !strings.Contains(meta.TitleError, "summary provider unavailable") {
		t.Fatalf("failed title metadata = %#v", meta)
	}
	if !slices.ContainsFunc(rec.Snapshot(), func(ev HarnessEvent) bool {
		return ev.Type == EventTitleFailed && strings.Contains(ev.Message, "summary provider unavailable")
	}) {
		t.Fatalf("title failure event missing: %#v", rec.Snapshot())
	}
}

func TestThreadRunDoesNotOverwriteExistingTitle(t *testing.T) {
	ctx := context.Background()
	p := scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("done text"), scriptharness.Done()))
	h := newTestHarness(p, sessiontree.NewMemoryRepo(), cache.NewMemoryStore())
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	meta, err := h.options.Repo.Thread(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	meta.Title = "Host selected title"
	meta.TitleStatus = sessiontree.ThreadTitleReady
	meta.TitleSource = sessiontree.ThreadTitleSourceHost
	meta.TitleUpdatedAt = meta.CreatedAt.Add(time.Minute)
	if err := h.options.Repo.UpdateThread(ctx, meta); err != nil {
		t.Fatal(err)
	}
	if _, err := thread.Run(ctx, "do it", RunOptions{TurnID: "turn-1"}); err != nil {
		t.Fatal(err)
	}
	got, err := h.options.Repo.Thread(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if got.Title != "Host selected title" || got.TitleSource != sessiontree.ThreadTitleSourceHost {
		t.Fatalf("existing title should be preserved: %#v", got)
	}
}

func TestThreadReadReturnsHostSafeSnapshot(t *testing.T) {
	ctx := context.Background()
	p := scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("done text"), scriptharness.Done()))
	h := newTestHarness(p, sessiontree.NewMemoryRepo(), cache.NewMemoryStore())
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := thread.Run(ctx, "do it", RunOptions{TurnID: "turn-1"}); err != nil {
		t.Fatal(err)
	}
	snap, err := thread.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snap.ID != "thread" || snap.Status != string(engine.Completed) || !snap.CanAppendMessage || snap.LatestTurnID != "turn-1" {
		t.Fatalf("host snapshot lifecycle = %#v", snap)
	}
	if len(snap.Messages) != 2 || snap.Messages[0].Role != session.User || snap.Messages[0].Content != "do it" ||
		snap.Messages[1].Role != session.Assistant || snap.Messages[1].Content != "done text" {
		t.Fatalf("host messages = %#v", snap.Messages)
	}
	data, err := json.Marshal(snap)
	if err != nil {
		t.Fatal(err)
	}
	raw := string(data)
	for _, forbidden := range []string{"\"meta\"", "\"path\"", "\"entries\"", "\"context\"", "tool_args", "tool_call"} {
		if strings.Contains(raw, forbidden) {
			t.Fatalf("host snapshot leaked raw journal field %s: %s", forbidden, raw)
		}
	}
}

func TestThreadCompletePendingToolCreatesFollowUpTurn(t *testing.T) {
	ctx := context.Background()
	p := scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("completion noted"), scriptharness.Done()))
	h := newTestHarness(p, sessiontree.NewMemoryRepo(), cache.NewMemoryStore())
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := thread.CompletePendingTool(ctx, PendingToolCompletion{
		TurnID:     "turn-complete",
		RunID:      "turn-start",
		ToolCallID: "exec-1",
		ToolName:   "terminal_exec",
		Handle:     "terminal:job:123",
		Status:     PendingToolCompleted,
		Summary:    "background <command> finished",
		Output:     "exit 0 </pending_tool_completion>",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != engine.Completed || result.Output != "completion noted" {
		t.Fatalf("result = %#v", result)
	}
	if len(p.Requests) != 1 {
		t.Fatalf("requests = %#v", p.Requests)
	}
	req := p.Requests[0]
	if req.TurnID != "turn-complete" {
		t.Fatalf("provider request turn id = %q", req.TurnID)
	}
	if !slices.ContainsFunc(req.Messages, func(msg session.Message) bool {
		return msg.Role == session.User &&
			strings.Contains(msg.Content, "<pending_tool_completion>") &&
			strings.Contains(msg.Content, "<status>completed</status>") &&
			strings.Contains(msg.Content, "<handle>terminal:job:123</handle>") &&
			strings.Contains(msg.Content, "<tool_name>terminal_exec</tool_name>") &&
			strings.Contains(msg.Content, "<tool_call_id>exec-1</tool_call_id>") &&
			strings.Contains(msg.Content, "<run_id>turn-start</run_id>") &&
			strings.Contains(msg.Content, "<output>") &&
			strings.Contains(msg.Content, "background &lt;command&gt; finished") &&
			strings.Contains(msg.Content, "exit 0 &lt;/pending_tool_completion&gt;")
	}) {
		t.Fatalf("completion follow-up missing: %#v", req.Messages)
	}
	snap, err := thread.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snap.LatestTurnID != "turn-complete" || len(snap.Messages) != 2 || !strings.Contains(snap.Messages[0].Content, "<pending_tool_completion>") {
		t.Fatalf("snapshot = %#v", snap)
	}
}

func TestThreadCompletePendingToolRejectsInvalidInput(t *testing.T) {
	ctx := context.Background()
	p := scriptharness.NewScriptedProvider()
	h := newTestHarness(p, sessiontree.NewMemoryRepo(), cache.NewMemoryStore())
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	cases := []PendingToolCompletion{
		{Status: PendingToolCompleted, Summary: "done"},
		{Handle: "terminal:job:123", Status: PendingToolCompletionStatus("bogus"), Summary: "done"},
		{Handle: "terminal:job:123", Status: PendingToolCompleted},
	}
	for _, completion := range cases {
		if _, err := thread.CompletePendingTool(ctx, completion); err == nil {
			t.Fatalf("completion %#v should fail", completion)
		}
	}
	snap, err := thread.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Messages) != 0 {
		t.Fatalf("invalid completion should not append messages: %#v", snap.Messages)
	}
}

func TestThreadSettlePendingToolAppendsDetailOnlyEvent(t *testing.T) {
	ctx := context.Background()
	p := scriptharness.NewScriptedProvider()
	repo := sessiontree.NewMemoryRepo()
	h := newTestHarness(p, repo, cache.NewMemoryStore())
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	appendPendingToolResultFixture(t, ctx, repo, "thread", "turn-1")
	before, err := thread.Journal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	beforeContext := sessiontree.BuildContext(before.Path, sessiontree.ContextOptions{})

	event, err := thread.SettlePendingTool(ctx, PendingToolSettlement{
		TurnID:     "turn-1",
		RunID:      "run-1",
		ToolCallID: "exec-1",
		ToolName:   "terminal.exec",
		Handle:     "terminal:job:123",
		Status:     PendingToolSettledCompleted,
		Summary:    "command completed",
		Output:     "exit 0",
		Activity:   &observation.ActivityPresentation{Label: "command completed", Payload: map[string]any{"exit_code": 0}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if event.Kind != SubAgentDetailEventToolResult || event.Type != pendingToolSettlementEntryKind {
		t.Fatalf("settlement event = %#v", event)
	}
	if event.ToolResult == nil ||
		event.ToolResult.CallID != "exec-1" ||
		event.ToolResult.ToolName != "terminal.exec" ||
		event.ToolResult.Status != string(observation.ActivityStatusSuccess) ||
		event.ToolResult.Content != "exit 0" {
		t.Fatalf("settlement tool result = %#v", event.ToolResult)
	}
	if event.ActivityTimeline == nil || len(event.ActivityTimeline.Items) != 1 ||
		event.ActivityTimeline.Items[0].Status != observation.ActivityStatusSuccess {
		t.Fatalf("settlement activity = %#v", event.ActivityTimeline)
	}

	after, err := thread.Journal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	afterContext := sessiontree.BuildContext(after.Path, sessiontree.ContextOptions{})
	if !slices.EqualFunc(beforeContext, afterContext, func(left, right session.Message) bool {
		return durableSignature(left) == durableSignature(right)
	}) {
		t.Fatalf("settlement changed provider-visible context:\nbefore=%#v\nafter=%#v", beforeContext, afterContext)
	}
	if slices.ContainsFunc(afterContext, func(msg session.Message) bool { return strings.Contains(msg.Content, "exit 0") }) {
		t.Fatalf("settlement output leaked into provider context: %#v", afterContext)
	}
}

func TestThreadSettlePendingToolIsIdempotentForSameTarget(t *testing.T) {
	ctx := context.Background()
	p := scriptharness.NewScriptedProvider()
	repo := sessiontree.NewMemoryRepo()
	h := newTestHarness(p, repo, cache.NewMemoryStore())
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	appendPendingToolResultFixture(t, ctx, repo, "thread", "turn-1")
	settlement := PendingToolSettlement{
		TurnID:     "turn-1",
		RunID:      "run-1",
		ToolCallID: "exec-1",
		ToolName:   "terminal.exec",
		Handle:     "terminal:job:123",
		Status:     PendingToolSettledCompleted,
		Summary:    "command completed",
		Output:     "exit 0",
	}

	first, err := thread.SettlePendingTool(ctx, settlement)
	if err != nil {
		t.Fatal(err)
	}
	afterFirst, err := thread.Journal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	second, err := thread.SettlePendingTool(ctx, settlement)
	if err != nil {
		t.Fatal(err)
	}
	afterSecond, err := thread.Journal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == "" || second.ID != first.ID {
		t.Fatalf("idempotent settlement should return existing event: first=%#v second=%#v", first, second)
	}
	if countPendingToolSettlementEntries(afterSecond.Path) != countPendingToolSettlementEntries(afterFirst.Path) {
		t.Fatalf("idempotent settlement appended a duplicate entry: before=%#v after=%#v", afterFirst.Path, afterSecond.Path)
	}
	_, err = thread.SettlePendingTool(ctx, PendingToolSettlement{
		TurnID:     "turn-1",
		RunID:      "run-1",
		ToolCallID: "exec-1",
		ToolName:   "terminal.exec",
		Handle:     "terminal:job:123",
		Status:     PendingToolSettledFailed,
		Summary:    "command failed",
	})
	if !errors.Is(err, ErrPendingToolSettlementConflict) {
		t.Fatalf("conflicting settlement err = %v, want conflict", err)
	}
}

func TestThreadSettlePendingToolAcceptsSettlementBeforePendingResult(t *testing.T) {
	ctx := context.Background()
	p := scriptharness.NewScriptedProvider()
	repo := sessiontree.NewMemoryRepo()
	h := newTestHarness(p, repo, cache.NewMemoryStore())
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	appendPendingToolCallFixture(t, ctx, repo, "thread", "turn-1")

	event, err := thread.SettlePendingTool(ctx, PendingToolSettlement{
		TurnID:     "turn-1",
		RunID:      "run-1",
		ToolCallID: "exec-1",
		ToolName:   "terminal.exec",
		Handle:     "terminal:job:123",
		Status:     PendingToolSettledCompleted,
		Summary:    "command completed",
		Output:     "exit 0",
	})
	if err != nil {
		t.Fatal(err)
	}
	if event.Kind != SubAgentDetailEventToolResult || event.Type != pendingToolSettlementEntryKind {
		t.Fatalf("early settlement event = %#v", event)
	}
}

func TestThreadSettlePendingToolRejectsInvalidTarget(t *testing.T) {
	ctx := context.Background()
	p := scriptharness.NewScriptedProvider()
	repo := sessiontree.NewMemoryRepo()
	h := newTestHarness(p, repo, cache.NewMemoryStore())
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	appendPendingToolResultFixture(t, ctx, repo, "thread", "turn-1")

	if _, err := thread.SettlePendingTool(ctx, PendingToolSettlement{}); err == nil || !strings.Contains(err.Error(), "requires turn id") {
		t.Fatalf("empty settlement err = %v", err)
	}
	_, err = thread.SettlePendingTool(ctx, PendingToolSettlement{
		TurnID:     "turn-1",
		RunID:      "other-run",
		ToolCallID: "exec-1",
		ToolName:   "terminal.exec",
		Handle:     "terminal:job:123",
		Status:     PendingToolSettledCompleted,
		Summary:    "done",
	})
	if err == nil || !strings.Contains(err.Error(), "target run was not found") {
		t.Fatalf("wrong run err = %v", err)
	}
	_, err = thread.SettlePendingTool(ctx, PendingToolSettlement{
		TurnID:     "turn-1",
		RunID:      "run-1",
		ToolCallID: "exec-1",
		ToolName:   "terminal.exec",
		Handle:     "terminal:job:missing",
		Status:     PendingToolSettledCompleted,
		Summary:    "done",
	})
	if err == nil || !strings.Contains(err.Error(), "not an active pending tool result") {
		t.Fatalf("wrong handle err = %v", err)
	}
}

func TestThreadReadLifecycleStates(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name      string
		provider  provider.Provider
		prepare   func(context.Context, *AgentHarness, *Thread) error
		run       bool
		want      string
		append    bool
		recover   bool
		phase     string
		latest    string
		waiting   string
		wantError bool
	}{
		{
			name:     "completed",
			provider: scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("done"), scriptharness.Done())),
			run:      true,
			want:     string(engine.Completed),
			append:   true,
			phase:    sessionlifecycle.PhaseIdle,
			latest:   "turn-1",
		},
		{
			name:     "waiting",
			provider: scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Tool("ask", "ask_user", `{"question":"Need file?"}`), scriptharness.DoneReason("tool_calls"))),
			run:      true,
			want:     string(engine.Waiting),
			append:   true,
			phase:    sessionlifecycle.PhaseIdle,
			latest:   "turn-1",
			waiting:  "Need file?",
		},
		{
			name:      "failed",
			provider:  scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("unused"))),
			run:       true,
			want:      string(engine.Failed),
			append:    false,
			phase:     sessionlifecycle.PhaseIdle,
			latest:    "turn-1",
			wantError: true,
		},
		{
			name:     "interrupted",
			provider: scriptharness.NewScriptedProvider(),
			prepare: func(ctx context.Context, _ *AgentHarness, thread *Thread) error {
				if _, err := sessiontree.AppendTurnMarker(ctx, thread.harness.options.Repo, thread.ID(), "turn-1", sessiontree.TurnStarted, nil); err != nil {
					return err
				}
				_, err := sessiontree.AppendMessage(ctx, thread.harness.options.Repo, thread.ID(), "turn-1", session.Message{Role: session.User, Content: "unfinished"})
				return err
			},
			want:    "interrupted",
			append:  false,
			recover: true,
			phase:   sessionlifecycle.PhaseIdle,
			latest:  "turn-1",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			repo := sessiontree.NewMemoryRepo()
			h := newTestHarness(tc.provider, repo, cache.NewMemoryStore())
			thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
			if err != nil {
				t.Fatal(err)
			}
			if tc.prepare != nil {
				if err := tc.prepare(ctx, h, thread); err != nil {
					t.Fatal(err)
				}
				thread, err = h.ResumeThread(ctx, thread.ID(), ResumeOptions{})
				if err != nil {
					t.Fatal(err)
				}
			}
			if tc.run {
				_, err = thread.Run(ctx, "do it", RunOptions{TurnID: "turn-1"})
				if tc.wantError {
					if err == nil {
						t.Fatalf("expected run error")
					}
				} else if err != nil {
					t.Fatal(err)
				}
			}
			snap, err := thread.Read(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if snap.Status != tc.want || snap.CanAppendMessage != tc.append || snap.Recoverable != tc.recover ||
				snap.Phase != tc.phase || snap.LatestTurnID != tc.latest || snap.WaitingPrompt != tc.waiting {
				t.Fatalf("snapshot = %#v", snap)
			}
		})
	}
}

func TestThreadReadLifecycleWhileTurnIsRunning(t *testing.T) {
	ctx := context.Background()
	blocking := newBlockingProvider()
	h := newTestHarness(blocking, sessiontree.NewMemoryRepo(), cache.NewMemoryStore())
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := thread.Run(runCtx, "hang", RunOptions{TurnID: "turn-running"})
		done <- err
	}()
	<-blocking.started

	snap, err := thread.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Status != "running" || snap.Phase != sessionlifecycle.PhaseTurn || snap.LatestTurnID != "turn-running" ||
		snap.CanAppendMessage || snap.Recoverable || snap.WaitingPrompt != "" {
		t.Fatalf("running snapshot = %#v", snap)
	}

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("run err = %v, want context canceled", err)
	}
}

func TestHarnessOwnsEngineIdentityAndToolDefinitions(t *testing.T) {
	ctx := context.Background()
	p := scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("done"), scriptharness.Done()))
	h := New(Options{
		Provider:     p,
		ProviderName: "fake",
		Model:        "fake-model",
		SystemPrompt: "You are a test assistant.",
		Tools:        tools.NewRegistry(),
		Repo:         sessiontree.NewMemoryRepo(),
		PromptStore:  cache.NewMemoryStore(),
		TitleGenerator: fixedTitleGenerator{
			title: "Test thread title",
		},
		LoopLimits: LoopLimits{
			MaxEmptyProviderRetries:  1,
			NoProgressLimit:          2,
			DuplicateToolLimit:       3,
			MaxLengthContinuations:   1,
			MaxStopHookContinuations: 1,
		},
	})
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := thread.Run(ctx, "do it", RunOptions{TurnID: "turn-1"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != engine.Completed {
		t.Fatalf("result = %#v", result)
	}
	if len(p.Requests) != 1 {
		t.Fatalf("requests = %#v", p.Requests)
	}
	req := p.Requests[0]
	if req.RunID == "" || req.RunID == "turn-1" || req.TurnID != "turn-1" || result.RunID != req.RunID || req.RawPlan.Segments[0].ThreadID != "thread" || req.Provider != "fake" || req.Model != "fake-model" {
		t.Fatalf("harness did not own identity/provider/model: %#v", req)
	}
	if req.RawPlan.Segments[0].CreatedByRunID != req.RunID || req.RawPlan.Segments[0].CreatedByTurnID != "turn-1" {
		t.Fatalf("prompt segments did not preserve run/turn identity: %#v", req.RawPlan.Segments[0])
	}
	controlTools := 0
	for _, def := range req.Tools {
		if def.Name == "ask_user" {
			controlTools++
			if def.Annotations["kind"] != "control" {
				t.Fatalf("ask_user should be engine-owned control tool: %#v", def)
			}
		}
		if def.Name == "task_complete" {
			t.Fatalf("host-provided raw task_complete leaked into request: %#v", req.Tools)
		}
	}
	if controlTools != 1 {
		t.Fatalf("expected exactly one engine-owned ask_user tool, got %d in %#v", controlTools, req.Tools)
	}
	if len(req.HostedTools) != 0 {
		t.Fatalf("unexpected hosted tools leaked into request: %#v", req.HostedTools)
	}
}

func TestThreadRunStoresLengthContinuationAsDelta(t *testing.T) {
	ctx := context.Background()
	p := scriptharness.NewScriptedProvider(
		[]provider.StreamEvent{scriptharness.Text("partial "), {Type: provider.Truncated}},
		scriptharness.Step(scriptharness.Text("retried"), scriptharness.Done()),
	)
	h := newTestHarness(p, sessiontree.NewMemoryRepo(), cache.NewMemoryStore())
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}

	result, err := thread.Run(ctx, "do it", RunOptions{TurnID: "turn-1", MaxLengthContinuations: 1})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != engine.Completed || result.Output != "partial retried" {
		t.Fatalf("result = %#v", result)
	}

	snap, err := thread.Journal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var assistant []string
	for _, entry := range snap.Entries {
		if entry.TurnID == "turn-1" && entry.Type == sessiontree.EntryAssistantMessage {
			assistant = append(assistant, entry.Message.Content)
		}
	}
	if !slices.Equal(assistant, []string{"partial ", "retried"}) {
		t.Fatalf("assistant entries = %#v, want length-continuation delta", assistant)
	}
}

func TestThreadRunStopHookContinuationIsPersistedAndMetadataStaysOutOfPrompt(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	promptStore := cache.NewMemoryStore()
	p := scriptharness.NewScriptedProvider(
		scriptharness.Step(scriptharness.Text("draft"), scriptharness.Done()),
		scriptharness.Step(scriptharness.Text("final"), scriptharness.Done()),
	)
	h := newTestHarness(p, repo, promptStore)
	h.options.StopHook = func(_ context.Context, hook engine.StopHookContext) (engine.StopHookResult, error) {
		if hook.Step == 1 {
			return engine.StopHookResult{Continue: true, Prompt: "Please verify before finalizing.", Reason: "verify"}, nil
		}
		return engine.StopHookResult{}, nil
	}
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}

	result, err := thread.Run(ctx, "do it", RunOptions{TurnID: "turn-hook"})
	if err != nil {
		t.Fatal(err)
	}

	if result.Status != engine.Completed || result.Output != "draftfinal" {
		t.Fatalf("result = %#v", result)
	}
	if len(p.Requests) != 2 {
		t.Fatalf("requests = %d, want 2", len(p.Requests))
	}
	if !slices.ContainsFunc(p.Requests[1].Messages, func(msg session.Message) bool {
		return msg.Role == session.User && msg.Content == "Please verify before finalizing."
	}) {
		t.Fatalf("hook continuation prompt missing from provider context: %#v", p.Requests[1].Messages)
	}
	snap, err := thread.Journal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if countUserMessagesInSnapshot(snap, "Please verify before finalizing.") != 1 {
		t.Fatalf("hook continuation prompt should be persisted once: %#v", snap.Entries)
	}
	completed := firstTurnMarker(snap.Entries, sessiontree.TurnCompleted)
	if completed.Metadata["completion_reason"] != string(engine.CompletionReasonNaturalStop) ||
		completed.Metadata["finish_reason"] != string(provider.FinishStop) {
		t.Fatalf("completed marker metadata = %#v", completed.Metadata)
	}
	for _, req := range p.Requests {
		for _, segment := range req.RawPlan.Segments {
			if strings.Contains(segment.Raw, "completion_reason") || strings.Contains(segment.Raw, "finish_reason") {
				t.Fatalf("marker metadata leaked into provider raw prompt: %#v", segment)
			}
		}
	}
}

func TestThreadRunStopHookContinuationBeforeToolCallKeepsSessionTreeOrder(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	promptStore := cache.NewMemoryStore()
	p := scriptharness.NewScriptedProvider(
		scriptharness.Step(scriptharness.Text("draft"), scriptharness.Done()),
		scriptharness.Step(scriptharness.Tool("read-1", "read", `{"value":"README.md"}`), scriptharness.DoneReason("tool_calls")),
		scriptharness.Step(scriptharness.Text("final"), scriptharness.Done()),
	)
	h := newTestHarness(p, repo, promptStore)
	mustRegister(h.options.Tools, stringTool("read", func(context.Context, string) (string, error) {
		return "read result", nil
	}))
	h.options.StopHook = func(_ context.Context, hook engine.StopHookContext) (engine.StopHookResult, error) {
		if hook.Step == 1 {
			return engine.StopHookResult{Continue: true, Prompt: "Please inspect with a tool.", Reason: "tool-check"}, nil
		}
		return engine.StopHookResult{}, nil
	}
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}

	result, err := thread.Run(ctx, "do it", RunOptions{TurnID: "turn-hook-tool"})
	if err != nil {
		t.Fatal(err)
	}

	if result.Status != engine.Completed || result.Output != "draftfinal" {
		t.Fatalf("result = %#v", result)
	}
	snap, err := thread.Journal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := []session.Message{
		{Role: session.User, Content: "do it"},
		{Role: session.Assistant, Content: "draft"},
		{Role: session.User, Content: "Please inspect with a tool."},
		{Role: session.Assistant, Content: "tool_call", ToolCallID: "read-1", ToolName: "read", ToolArgs: `{"value":"README.md"}`},
		{Role: session.Tool, Content: "read result", ToolCallID: "read-1", ToolName: "read"},
		{Role: session.Assistant, Content: "final"},
	}
	if !messagePrefixEqual(snap.Context, want) || len(snap.Context) != len(want) {
		t.Fatalf("session context order = %#v", snap.Context)
	}
	if !slices.ContainsFunc(snap.Entries, func(entry sessiontree.Entry) bool {
		return entry.Type == sessiontree.EntryToolResult &&
			entry.Message.ToolCallID == "read-1" &&
			entry.Message.ToolResult != nil &&
			entry.Message.ToolResult.ContentSHA256 != ""
	}) {
		t.Fatalf("tool result projection metadata missing: %#v", snap.Entries)
	}
	if countUserMessagesInSnapshot(snap, "Please inspect with a tool.") != 1 ||
		countEntriesWithContent(snap.Entries, sessiontree.EntryAssistantMessage, "draft") != 1 ||
		countEntriesWithContent(snap.Entries, sessiontree.EntryAssistantMessage, "final") != 1 ||
		countEntries(snap.Entries, sessiontree.EntryToolCall) != 1 ||
		countEntries(snap.Entries, sessiontree.EntryToolResult) != 1 {
		t.Fatalf("session entries should not duplicate hook/tool suffix: %#v", snap.Entries)
	}
	if !slices.ContainsFunc(snap.Entries, func(entry sessiontree.Entry) bool {
		return entry.Type == sessiontree.EntryTurnMarker &&
			entry.TurnStatus == sessiontree.TurnSavePoint &&
			entry.Metadata["reason"] == "context_continue" &&
			entry.Metadata["continuation_reason"] == string(engine.ContinueHook) &&
			entry.Metadata["hook_reason"] == "tool-check"
	}) {
		t.Fatalf("hook continuation save point missing: %#v", snap.Entries)
	}
	if len(p.Requests) != 3 || !slices.ContainsFunc(p.Requests[1].Messages, func(msg session.Message) bool {
		return msg.Role == session.User && msg.Content == "Please inspect with a tool."
	}) {
		t.Fatalf("hook continuation missing from tool step request: %#v", p.Requests)
	}
}

func TestToolResultViewFromEventBindsOnlyProjectionArtifact(t *testing.T) {
	view := toolResultViewFromEvent(event.Event{
		Metadata: map[string]any{
			"truncated":       true,
			"original_bytes":  128,
			"visible_bytes":   8,
			"strategy":        "tail",
			"content_sha256":  "abc123",
			"artifact_id":     "projection",
			"artifact_label":  "projection.txt#123",
			"artifact_sha256": "full-sha",
		},
		Artifacts: []event.Artifact{
			{ID: "ordinary", SafeLabel: "ordinary.txt#123", URL: "/artifacts/ordinary", Kind: "report"},
			{ID: "projection", SafeLabel: "projection.txt#123", URL: "/artifacts/projection", Kind: "tool_output"},
		},
	})
	if view == nil || view.FullOutput == nil || view.FullOutput.ID != "projection" {
		t.Fatalf("view bound wrong artifact: %#v", view)
	}

	view = toolResultViewFromEvent(event.Event{
		Metadata: map[string]any{
			"truncated":      false,
			"original_bytes": 32,
			"visible_bytes":  32,
			"content_sha256": "abc123",
		},
		Artifacts: []event.Artifact{{ID: "ordinary", SafeLabel: "ordinary.txt#123", URL: "/artifacts/ordinary", Kind: "report"}},
	})
	if view == nil {
		t.Fatalf("view should retain projection metrics")
	}
	if view.FullOutput != nil {
		t.Fatalf("ordinary artifact should not become full output: %#v", view.FullOutput)
	}
}

func TestRetryDoesNotDuplicateUserMessageAndKeepsPrefixStable(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	promptStore := cache.NewMemoryStore()
	failing := scriptharness.NewScriptedProvider(
		scriptharness.Step(scriptharness.Text("partial before failure"), scriptharness.Tool("missing-1", "missing", "{}"), scriptharness.DoneReason("tool_calls")),
		nil,
	)
	failing.Errs[2] = errors.New("provider down")
	h := newTestHarness(failing, repo, promptStore)
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := thread.Run(ctx, "retry me", RunOptions{TurnID: "turn-fail"})
	if err == nil || result.Status != engine.Failed {
		t.Fatalf("failed result = %#v err=%v", result, err)
	}
	threadRequests, err := promptStore.ProviderRequests(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	failedRequests := providerRequestsForRun(threadRequests, result.RunID)
	if len(failedRequests) != 2 {
		t.Fatalf("failed request records = %#v", failedRequests)
	}
	failedSnap, _ := thread.Journal(ctx)
	failedLeaf := failedSnap.Meta.LeafID
	if !slices.ContainsFunc(failedSnap.Entries, func(entry sessiontree.Entry) bool {
		return entry.Type == sessiontree.EntryAssistantMessage && entry.Message.Content == "partial before failure"
	}) {
		t.Fatalf("failed branch should retain partial assistant output: %#v", failedSnap.Entries)
	}
	retryProvider := scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("ok"), scriptharness.Done()))
	h.options.Provider = retryProvider
	result, err = thread.Retry(ctx, RetryOptions{Reason: "provider recovered"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != engine.Completed {
		t.Fatalf("retry result = %#v", result)
	}
	snap, _ := thread.Journal(ctx)
	if countEntries(snap.Entries, sessiontree.EntryUserMessage) != 1 {
		t.Fatalf("retry must not duplicate the original user entry: %#v", snap.Entries)
	}
	failedPath, err := repo.Path(ctx, "thread", failedLeaf)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(failedPath, func(entry sessiontree.Entry) bool {
		return entry.Type == sessiontree.EntryRunFailure && entry.Error == "provider down"
	}) {
		t.Fatalf("failed branch should remain readable by old leaf: %#v", failedPath)
	}
	threadRequests, err = promptStore.ProviderRequests(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	retryRequests := providerRequestsForRun(threadRequests, result.RunID)
	if len(retryRequests) != 1 {
		t.Fatalf("retry request records = %#v", retryRequests)
	}
	if failedRequests[1].PrefixRawHash != retryRequests[0].PrefixRawHash {
		t.Fatalf("retry should resume from last stable save point: failed=%#v retry=%#v", failedRequests[1], retryRequests[0])
	}
	if retryProvider.Requests[0].RawPlan.ReusedSegments == 0 {
		t.Fatalf("retry should reuse immutable raw segments: %#v", retryProvider.Requests[0].RawPlan)
	}
	if !slices.ContainsFunc(retryProvider.Requests[0].RawPlan.Segments, func(seg cache.Segment) bool {
		return seg.Kind == cache.SegmentUserMessage && seg.EntryID != ""
	}) {
		t.Fatalf("retry raw segments should carry source entry ids: %#v", retryProvider.Requests[0].RawPlan.Segments)
	}
	if !slices.ContainsFunc(retryProvider.Requests[0].Messages, func(msg session.Message) bool {
		return msg.Content == "partial before failure"
	}) || !slices.ContainsFunc(retryProvider.Requests[0].Messages, func(msg session.Message) bool {
		return msg.ToolName == "missing" && msg.Role == session.Tool
	}) {
		t.Fatalf("retry request should include stable suffix after tool execution: %#v", retryProvider.Requests[0].Messages)
	}
}

func TestRetryAfterInterruptedTurnUsesRealtimeToolSavePoint(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	promptStore := cache.NewMemoryStore()
	p := scriptharness.NewScriptedProvider(
		scriptharness.Step(scriptharness.Tool("read-1", "read", `{"value":"README.md"}`), scriptharness.DoneReason("tool_calls")),
		scriptharness.Step(scriptharness.Hang()),
	)
	registry := tools.NewRegistry()
	readCalls := 0
	mustRegister(registry, stringTool("read", func(context.Context, string) (string, error) {
		readCalls++
		return "read result", nil
	}))
	h := New(Options{
		Provider:     p,
		ProviderName: "fake",
		Model:        "fake-model",
		SystemPrompt: "You are a test assistant.",
		Tools:        registry,
		Repo:         repo,
		PromptStore:  promptStore,
		TitleGenerator: fixedTitleGenerator{
			title: "Test thread title",
		},
		LoopLimits: LoopLimits{MaxEmptyProviderRetries: 1, NoProgressLimit: 2, DuplicateToolLimit: 3},
	})
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		_, err := thread.Run(runCtx, "read then wait", RunOptions{TurnID: "turn-interrupted"})
		done <- err
	}()
	deadline := time.After(2 * time.Second)
	for {
		snap, _ := thread.Journal(ctx)
		if slices.ContainsFunc(snap.Entries, func(entry sessiontree.Entry) bool {
			return entry.Type == sessiontree.EntryToolResult && entry.Message.Content == "read result"
		}) {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for realtime tool save point")
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	if err := <-done; err == nil {
		t.Fatalf("interrupted turn should return error")
	}
	if readCalls != 1 {
		t.Fatalf("read calls before retry = %d", readCalls)
	}
	retryProvider := scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("ok"), scriptharness.Done()))
	h.options.Provider = retryProvider
	result, err := thread.Retry(ctx, RetryOptions{Reason: "after interruption"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != engine.Completed {
		t.Fatalf("retry result = %#v", result)
	}
	if readCalls != 1 {
		t.Fatalf("retry should not re-execute completed tool, calls=%d", readCalls)
	}
	if !slices.ContainsFunc(retryProvider.Requests[0].Messages, func(msg session.Message) bool {
		return msg.Role == session.Tool && msg.Content == "read result"
	}) {
		t.Fatalf("retry should include saved tool result context: %#v", retryProvider.Requests[0].Messages)
	}
}

func TestForkContinuesWithoutPollutingSourceThread(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	promptStore := cache.NewMemoryStore()
	sourceProvider := scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("source done"), scriptharness.Done()))
	h := newTestHarness(sourceProvider, repo, promptStore)
	source, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "source"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Run(ctx, "first", RunOptions{TurnID: "turn-source"}); err != nil {
		t.Fatal(err)
	}
	sourceSnap, _ := source.Journal(ctx)
	userEntry := firstEntry(sourceSnap.Entries, sessiontree.EntryUserMessage)
	fork, err := h.ForkThread(ctx, ForkOptions{SourceThreadID: "source", EntryID: userEntry.ID, NewThreadID: "fork"})
	if err != nil {
		t.Fatal(err)
	}
	forkProvider := scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("fork done"), scriptharness.Done()))
	h.options.Provider = forkProvider
	if _, err := fork.Run(ctx, "second", RunOptions{TurnID: "turn-fork"}); err != nil {
		t.Fatal(err)
	}
	sourceAfter, _ := source.Journal(ctx)
	forkAfter, _ := fork.Journal(ctx)
	if countEntries(sourceAfter.Entries, sessiontree.EntryUserMessage) != 1 {
		t.Fatalf("source thread should not receive fork user input: %#v", sourceAfter.Entries)
	}
	if countEntries(forkAfter.Entries, sessiontree.EntryUserMessage) != 2 {
		t.Fatalf("fork should contain copied source user and new user: %#v", forkAfter.Entries)
	}
	if slices.ContainsFunc(forkProvider.Requests[0].Messages, func(msg session.Message) bool {
		return msg.Content == "source done"
	}) {
		t.Fatalf("fork from user entry should not include source assistant/tool suffix: %#v", forkProvider.Requests[0].Messages)
	}
	userMessages := userContents(forkProvider.Requests[0].Messages)
	if !slices.Equal(userMessages, []string{"first", "second"}) {
		t.Fatalf("fork provider context = %#v", forkProvider.Requests[0].Messages)
	}
	forkSnap, _ := fork.Journal(ctx)
	if forkSnap.Meta.ForkedFromThreadID != "source" || forkSnap.Meta.ForkedFromEntryID != userEntry.ID {
		t.Fatalf("fork metadata = %#v", forkSnap.Meta)
	}
}

func TestMoveToBranchSummaryEntersActiveContext(t *testing.T) {
	ctx := context.Background()
	h := newTestHarness(scriptharness.NewScriptedProvider(), sessiontree.NewMemoryRepo(), cache.NewMemoryStore())
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	first, err := sessiontree.AppendMessage(ctx, h.options.Repo, "thread", "turn-1", session.Message{Role: session.User, Content: "first"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendMessage(ctx, h.options.Repo, "thread", "turn-2", session.Message{Role: session.Assistant, Content: "branch work"}); err != nil {
		t.Fatal(err)
	}
	if err := thread.MoveTo(ctx, first.ID, MoveOptions{Summary: "Left branch had branch work."}); err != nil {
		t.Fatal(err)
	}
	snap, err := thread.Journal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Context) != 2 || snap.Context[0].Content != "first" || snap.Context[1].Content != "Left branch had branch work." {
		t.Fatalf("branch summary should enter active context: %#v", snap.Context)
	}
}

func TestEngineCompactionIsProjectedAsSessionTreeCompactionEntry(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	p := scriptharness.NewScriptedProvider(nil, scriptharness.Step(scriptharness.Text("ok"), scriptharness.Done()))
	p.Errs[1] = provider.ErrContextOverflow
	h := newTestHarness(p, repo, cache.NewMemoryStore())
	h.options.TurnPolicy.ContextPolicy.ContextWindowTokens = 8000
	h.options.TurnPolicy.ContextPolicy.ReservedOutputTokens = 512
	h.options.TurnPolicy.ContextPolicy.ReservedSummaryTokens = 512
	h.options.TurnPolicy.ContextPolicy.RecentTailTokens = 256
	h.options.CompactionGenerator = compaction.ExtractiveSummaryGenerator{}
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendMessage(ctx, repo, "thread", "seed", session.Message{Role: session.User, Content: "old"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendMessage(ctx, repo, "thread", "seed", session.Message{Role: session.User, Content: "kept"}); err != nil {
		t.Fatal(err)
	}
	result, err := thread.Run(ctx, "new", RunOptions{TurnID: "turn-compact"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != engine.Completed {
		t.Fatalf("result = %#v", result)
	}
	snap, err := thread.Journal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	compaction := firstEntry(snap.Entries, sessiontree.EntryCompaction)
	if compaction.ID == "" || compaction.Summary == "" || compaction.FirstKeptEntryID == "" {
		t.Fatalf("compaction entry missing details: %#v", snap.Entries)
	}
	if !slices.ContainsFunc(snap.Context, func(msg session.Message) bool {
		return msg.Role == session.User && msg.Kind == session.MessageKindCompactionSummary
	}) {
		t.Fatalf("compaction summary should be provider-visible: %#v", snap.Context)
	}
}

func TestFailedEngineCompactionDoesNotAppendSessionTreeCompactionEntry(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	p := &estimatingHarnessProvider{
		Provider: scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("never sent"), scriptharness.Done())),
		estimates: []provider.TokenEstimate{
			{PrefixTokens: 800, MessageTokens: 300, ToolDefinitionTokens: 200, EstimatedInputTokens: 1300, Source: "harness_estimator_test", Method: provider.TokenEstimateProviderRenderedPayload, Confidence: provider.EstimateConservative},
			{PrefixTokens: 800, MessageTokens: 10, ToolDefinitionTokens: 200, EstimatedInputTokens: 1010, Source: "harness_estimator_test", Method: provider.TokenEstimateProviderRenderedPayload, Confidence: provider.EstimateConservative},
		},
	}
	h := newTestHarness(p, repo, cache.NewMemoryStore())
	h.options.TurnPolicy.ContextPolicy.ContextWindowTokens = 1000
	h.options.TurnPolicy.ContextPolicy.ReservedOutputTokens = 100
	h.options.TurnPolicy.ContextPolicy.ReservedSummaryTokens = 80
	h.options.TurnPolicy.ContextPolicy.RecentTailTokens = 20
	h.options.TurnPolicy.ContextPolicy.RecentUserTokens = 20
	h.options.CompactionGenerator = compaction.ExtractiveSummaryGenerator{}
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendMessage(ctx, repo, "thread", "seed", session.Message{Role: session.User, Content: "old", EntryID: "u1"}); err != nil {
		t.Fatal(err)
	}
	_, err = thread.Run(ctx, "new", RunOptions{TurnID: "turn-compact"})
	if !errors.Is(err, engine.ErrFixedContextOverBudget) {
		t.Fatalf("err = %v, want fixed overhead failure", err)
	}
	snap, err := thread.Journal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := countEntries(snap.Entries, sessiontree.EntryCompaction); got != 0 {
		t.Fatalf("failed compaction must not append durable compaction entries, got %d: %#v", got, snap.Entries)
	}
	if got := countMessagesByKind(snap.Context, session.MessageKindCompactionSummary); got != 0 {
		t.Fatalf("failed compaction must not expose active checkpoint, got %d: %#v", got, snap.Context)
	}
	if len(p.Provider.(*scriptharness.ScriptedProvider).Requests) != 0 {
		t.Fatalf("provider should not receive unvalidated request after failed compaction: %#v", p.Provider.(*scriptharness.ScriptedProvider).Requests)
	}
}

func TestEngineCompactionAppendsOnlyValidatedConvergedSessionTreeEntry(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	scripted := scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("ok"), scriptharness.Done()))
	p := &estimatingHarnessProvider{
		Provider: scripted,
		estimates: []provider.TokenEstimate{
			{PrefixTokens: 100, MessageTokens: 900, ToolDefinitionTokens: 100, EstimatedInputTokens: 1100, Source: "harness_estimator_test", Method: provider.TokenEstimateProviderRenderedPayload, Confidence: provider.EstimateConservative},
			{PrefixTokens: 100, MessageTokens: 780, ToolDefinitionTokens: 100, EstimatedInputTokens: 980, Source: "harness_estimator_test", Method: provider.TokenEstimateProviderRenderedPayload, Confidence: provider.EstimateConservative},
			{PrefixTokens: 100, MessageTokens: 200, ToolDefinitionTokens: 100, EstimatedInputTokens: 400, Source: "harness_estimator_test", Method: provider.TokenEstimateProviderRenderedPayload, Confidence: provider.EstimateConservative},
		},
	}
	h := newTestHarness(p, repo, cache.NewMemoryStore())
	h.options.TurnPolicy.ContextPolicy.ContextWindowTokens = 1000
	h.options.TurnPolicy.ContextPolicy.ReservedOutputTokens = 100
	h.options.TurnPolicy.ContextPolicy.ReservedSummaryTokens = 80
	h.options.TurnPolicy.ContextPolicy.RecentTailTokens = 80
	h.options.TurnPolicy.ContextPolicy.RecentUserTokens = 60
	h.options.CompactionGenerator = compaction.ExtractiveSummaryGenerator{}
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendMessage(ctx, repo, "thread", "seed", session.Message{Role: session.User, Content: "older " + strings.Repeat("x ", 300), EntryID: "u1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendMessage(ctx, repo, "thread", "seed", session.Message{Role: session.Assistant, Content: "answer " + strings.Repeat("y ", 100), EntryID: "a1"}); err != nil {
		t.Fatal(err)
	}
	result, err := thread.Run(ctx, "new", RunOptions{TurnID: "turn-compact"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != engine.Completed || result.Output != "ok" {
		t.Fatalf("result = %#v", result)
	}
	snap, err := thread.Journal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := countEntries(snap.Entries, sessiontree.EntryCompaction); got != 1 {
		t.Fatalf("converged compaction should append exactly one durable entry, got %d: %#v", got, snap.Entries)
	}
	entry := latestEntry(snap.Entries, sessiontree.EntryCompaction)
	if entry.CompactionID == "" || entry.CompactionGeneration != 1 || entry.CompactionWindowID == "" {
		t.Fatalf("validated compaction entry missing identity: %#v", entry)
	}
	checkpoint, ok := firstMessageByKind(snap.Context, session.MessageKindCompactionSummary)
	if !ok {
		t.Fatalf("validated compaction should expose active checkpoint: %#v", snap.Context)
	}
	if checkpoint.EntryID != entry.ID || checkpoint.CompactionID != entry.CompactionID || checkpoint.CompactionWindowID != entry.CompactionWindowID {
		t.Fatalf("active checkpoint should be tied to committed entry: checkpoint=%#v entry=%#v", checkpoint, entry)
	}
	if len(scripted.Requests) != 1 {
		t.Fatalf("provider should receive only the validated compacted request: %#v", scripted.Requests)
	}
}

func TestCommittedCompactionAppendErrorContinuesWithDurableEntry(t *testing.T) {
	ctx := context.Background()
	repo := &committedCompactionAppendRepo{MemoryRepo: sessiontree.NewMemoryRepo()}
	p := scriptharness.NewScriptedProvider(nil, scriptharness.Step(scriptharness.Text("ok"), scriptharness.Done()))
	p.Errs[1] = provider.ErrContextOverflow
	h := newTestHarness(p, repo, cache.NewMemoryStore())
	h.options.TurnPolicy.ContextPolicy.ContextWindowTokens = 8000
	h.options.TurnPolicy.ContextPolicy.ReservedOutputTokens = 512
	h.options.TurnPolicy.ContextPolicy.ReservedSummaryTokens = 512
	h.options.TurnPolicy.ContextPolicy.RecentTailTokens = 256
	h.options.CompactionGenerator = compaction.ExtractiveSummaryGenerator{}
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendMessage(ctx, repo, "thread", "seed", session.Message{Role: session.User, Content: "old"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendMessage(ctx, repo, "thread", "seed", session.Message{Role: session.User, Content: "kept"}); err != nil {
		t.Fatal(err)
	}
	result, err := thread.Run(ctx, "new", RunOptions{TurnID: "turn-compact"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != engine.Completed || result.Output != "ok" {
		t.Fatalf("result = %#v", result)
	}
	if repo.compactionCommittedErrors != 1 {
		t.Fatalf("committed compaction append errors = %d, want 1", repo.compactionCommittedErrors)
	}
	snap, err := thread.Journal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := countEntries(snap.Entries, sessiontree.EntryCompaction); got != 1 {
		t.Fatalf("committed append error should still leave one durable compaction, got %d: %#v", got, snap.Entries)
	}
	if got := countMessagesByKind(snap.Context, session.MessageKindCompactionSummary); got != 1 {
		t.Fatalf("committed append error should expose committed checkpoint, got %d: %#v", got, snap.Context)
	}
	if len(p.Requests) != 2 {
		t.Fatalf("provider should receive original overflow request and compacted retry: %#v", p.Requests)
	}
	if got := countMessagesByKind(p.Requests[1].Messages, session.MessageKindCompactionSummary); got != 1 {
		t.Fatalf("compacted retry should carry checkpoint, got %d: %#v", got, p.Requests[1].Messages)
	}
}

func TestDefaultProviderCompactionUsesConfiguredPromptOptions(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	p := scriptharness.NewScriptedProvider(
		nil,
		scriptharness.Step(scriptharness.Text("summary ok"), scriptharness.Done()),
		scriptharness.Step(scriptharness.Text("ok"), scriptharness.Done()),
	)
	p.Errs[1] = provider.ErrContextOverflow
	h := newTestHarness(p, repo, cache.NewMemoryStore())
	h.options.TurnPolicy.ContextPolicy.ContextWindowTokens = 8000
	h.options.TurnPolicy.ContextPolicy.ReservedOutputTokens = 512
	h.options.TurnPolicy.ContextPolicy.ReservedSummaryTokens = 512
	h.options.TurnPolicy.ContextPolicy.RecentTailTokens = 256
	h.options.CompactionPrompt = compaction.PromptOptions{
		WriterSystemPrompt: "You are Acme's context checkpoint writer.",
		SummaryTitle:       "Acme Conversation Checkpoint",
	}
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendMessage(ctx, repo, "thread", "seed", session.Message{Role: session.User, Content: "old"}); err != nil {
		t.Fatal(err)
	}
	result, err := thread.Run(ctx, "new", RunOptions{TurnID: "turn-compact"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != engine.Completed {
		t.Fatalf("result = %#v", result)
	}
	if len(p.Requests) != 3 {
		t.Fatalf("requests = %#v", p.Requests)
	}
	summaryReq := p.Requests[1]
	if len(summaryReq.Messages) < 2 ||
		summaryReq.Messages[0].Content != "You are Acme's context checkpoint writer." ||
		!strings.Contains(summaryReq.Messages[1].Content, "# Acme Conversation Checkpoint") {
		t.Fatalf("custom compaction prompt was not used: %#v", summaryReq.Messages)
	}
}

func TestMultipleCompactionsForkReloadAndContinueUseLatestWindow(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	promptRoot := t.TempDir()
	repo := sessiontree.NewFileRepo(root)
	promptStore := cache.NewFileStore(promptRoot)
	p := scriptharness.NewScriptedProvider(
		nil,
		scriptharness.Step(scriptharness.Text("after first"), scriptharness.Done()),
		nil,
		scriptharness.Step(scriptharness.Text("after second"), scriptharness.Done()),
	)
	p.Errs[1] = provider.ErrContextOverflow
	p.Errs[3] = provider.ErrContextOverflow
	h := newTestHarness(p, repo, promptStore)
	h.options.CompactionGenerator = compaction.ExtractiveSummaryGenerator{}
	h.options.TurnPolicy.ContextPolicy.ContextWindowTokens = 12000
	h.options.TurnPolicy.ContextPolicy.ReservedOutputTokens = 512
	h.options.TurnPolicy.ContextPolicy.ReservedSummaryTokens = 512
	h.options.TurnPolicy.ContextPolicy.RecentTailTokens = 512
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := thread.Run(ctx, "first "+strings.Repeat("alpha ", 120), RunOptions{TurnID: "turn-1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := thread.Run(ctx, "second "+strings.Repeat("beta ", 120), RunOptions{TurnID: "turn-2"}); err != nil {
		t.Fatal(err)
	}
	snap, err := thread.Journal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if countEntries(snap.Entries, sessiontree.EntryCompaction) != 2 {
		t.Fatalf("expected two durable compactions: %#v", snap.Entries)
	}
	if got := countMessagesByKind(snap.Context, session.MessageKindCompactionSummary); got != 1 {
		t.Fatalf("active context should expose only latest compaction summary, got %d: %#v", got, snap.Context)
	}
	checkpoint, ok := firstMessageByKind(snap.Context, session.MessageKindCompactionSummary)
	if !ok || checkpoint.Kind != session.MessageKindCompactionSummary {
		t.Fatalf("active context missing checkpoint: %#v", snap.Context)
	}
	if strings.Count(checkpoint.Content, "<compaction_summary") != 1 || strings.Count(checkpoint.Content, "</compaction_summary>") != 1 {
		t.Fatalf("active checkpoint should contain one summary envelope: %q", checkpoint.Content)
	}
	if strings.Count(checkpoint.Content, "<preserved_user_inputs>") > 1 {
		t.Fatalf("active checkpoint should not duplicate preserved user blocks: %q", checkpoint.Content)
	}
	if strings.Contains(compaction.ExtractCheckpointSummary(checkpoint.Content), "preserved_user_inputs") {
		t.Fatalf("active checkpoint summary should not include an older checkpoint envelope: %q", checkpoint.Content)
	}
	latest := latestEntry(snap.Entries, sessiontree.EntryCompaction)
	if latest.CompactionGeneration != 2 || latest.PreviousCompactionID == "" {
		t.Fatalf("second compaction should link previous generation: %#v", latest)
	}

	reloadedHarness := newTestHarness(scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("fork done"), scriptharness.Done())), sessiontree.NewFileRepo(root), cache.NewFileStore(promptRoot))
	fork, err := reloadedHarness.ForkThread(ctx, ForkOptions{SourceThreadID: "thread", EntryID: latest.ID, NewThreadID: "fork"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := fork.Run(ctx, "continue from fork", RunOptions{TurnID: "turn-fork"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != engine.Completed {
		t.Fatalf("fork result = %#v", result)
	}
	req := reloadedHarness.options.Provider.(*scriptharness.ScriptedProvider).Requests[0]
	if got := countMessagesByKind(req.Messages, session.MessageKindCompactionSummary); got != 1 {
		t.Fatalf("fork request should carry one latest summary, got %d: %#v", got, req.Messages)
	}
	requestCheckpoint, ok := firstMessageByKind(req.Messages, session.MessageKindCompactionSummary)
	if !ok || requestCheckpoint.Kind != session.MessageKindCompactionSummary {
		t.Fatalf("fork request missing checkpoint: %#v", req.Messages)
	}
	if strings.Count(requestCheckpoint.Content, "<compaction_summary") != 1 || strings.Count(requestCheckpoint.Content, "</compaction_summary>") != 1 {
		t.Fatalf("fork request checkpoint should contain one summary envelope: %q", requestCheckpoint.Content)
	}
	if strings.Count(requestCheckpoint.Content, "<preserved_user_inputs>") > 1 {
		t.Fatalf("fork request checkpoint should not duplicate preserved user blocks: %q", requestCheckpoint.Content)
	}
	if strings.Contains(compaction.ExtractCheckpointSummary(requestCheckpoint.Content), "preserved_user_inputs") {
		t.Fatalf("fork request checkpoint summary should not include an older checkpoint envelope: %q", requestCheckpoint.Content)
	}
	if req.RawPlan.CompactionGeneration != 2 || req.RawPlan.CompactionWindowID != latest.CompactionWindowID {
		t.Fatalf("fork request should carry latest compaction window: latest=%#v plan=%#v", latest, req.RawPlan)
	}
	if !slices.ContainsFunc(req.Messages, func(msg session.Message) bool {
		return msg.Role == session.User && msg.Content == "continue from fork"
	}) {
		t.Fatalf("fork continuation missing from provider request: %#v", req.Messages)
	}
}

func TestFileRepoResumeContinuesThreadAndReusesRawSegments(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	promptRoot := t.TempDir()
	repo := sessiontree.NewFileRepo(root)
	promptStore := cache.NewFileStore(promptRoot)
	firstProvider := scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Tool("ask", "ask_user", `{"question":"more?"}`), scriptharness.DoneReason("tool_calls")))
	h := newTestHarness(firstProvider, repo, promptStore)
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := thread.Run(ctx, "hello", RunOptions{TurnID: "turn-1"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != engine.Waiting {
		t.Fatalf("first result = %#v", result)
	}
	firstRequestSegments := append([]string(nil), firstProvider.Requests[0].RawPlan.SegmentIDs...)
	firstRequestRaws := segmentRaws(firstProvider.Requests[0].RawPlan.Segments)
	secondProvider := scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("ok"), scriptharness.Done()))
	resumedHarness := newTestHarness(secondProvider, sessiontree.NewFileRepo(root), cache.NewFileStore(promptRoot))
	resumed, err := resumedHarness.ResumeThread(ctx, "thread", ResumeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	result, err = resumed.Run(ctx, "answer", RunOptions{TurnID: "turn-2"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != engine.Completed {
		t.Fatalf("second result = %#v", result)
	}
	wantMessages := []session.Message{
		{Role: session.System, Content: "You are a test assistant."},
		{Role: session.User, Content: "hello"},
		{Role: session.Assistant, Content: "Agent requested user input: more?", Kind: session.MessageKindControlSignal},
		{Role: session.User, Content: "answer"},
	}
	if !messagePrefixEqual(secondProvider.Requests[0].Messages, wantMessages) {
		t.Fatalf("resumed provider context order = %#v", secondProvider.Requests[0].Messages)
	}
	if secondProvider.Requests[0].RawPlan.ReusedSegments == 0 {
		t.Fatalf("resumed turn should reuse raw ledger from previous process: %#v", secondProvider.Requests[0].RawPlan)
	}
	if !slices.Equal(firstRequestSegments, secondProvider.Requests[0].RawPlan.SegmentIDs[:len(firstRequestSegments)]) {
		t.Fatalf("resumed raw segment prefix changed: first=%#v second=%#v", firstRequestSegments, secondProvider.Requests[0].RawPlan.SegmentIDs)
	}
	if !slices.Equal(firstRequestRaws, segmentRaws(secondProvider.Requests[0].RawPlan.Segments[:len(firstRequestRaws)])) {
		t.Fatalf("resumed raw string prefix changed")
	}
	resumedSnap, _ := resumed.Journal(ctx)
	if !hasEntry(resumedSnap.Entries, sessiontree.EntryTurnMarker, sessiontree.TurnWaiting) ||
		!hasEntry(resumedSnap.Entries, sessiontree.EntryTurnMarker, sessiontree.TurnCompleted) {
		t.Fatalf("resume should preserve waiting and completed markers: %#v", resumedSnap.Entries)
	}
}

func TestFileRepoActiveTurnLeaseBlocksSecondHarnessResume(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	promptRoot := t.TempDir()
	blocking := newBlockingProvider()
	firstHarness := newTestHarness(blocking, sessiontree.NewFileRepo(root), cache.NewFileStore(promptRoot))
	thread, err := firstHarness.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := thread.Run(runCtx, "hang", RunOptions{TurnID: "turn-live"})
		done <- err
	}()
	<-blocking.started

	secondHarness := newTestHarness(scriptharness.NewScriptedProvider(), sessiontree.NewFileRepo(root), cache.NewFileStore(promptRoot))
	if _, err := secondHarness.ResumeThread(ctx, "thread", ResumeOptions{}); !errors.Is(err, ErrActiveTurn) {
		t.Fatalf("resume err = %v, want active turn guard", err)
	}
	entries, err := sessiontree.NewFileRepo(root).Entries(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if slices.ContainsFunc(entries, func(entry sessiontree.Entry) bool {
		return entry.Type == sessiontree.EntryTurnMarker && entry.TurnStatus == sessiontree.TurnAborted ||
			entry.Type == sessiontree.EntryRunFailure && strings.Contains(entry.Error, "interrupted")
	}) {
		t.Fatalf("live turn should not be marked interrupted: %#v", entries)
	}
	cancel()
	<-done
}

func TestFileRepoResumeClearsOnlyExpiredActiveTurnLease(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	promptRoot := t.TempDir()
	now := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	repo := sessiontree.NewFileRepo(root)
	h := newTestHarness(scriptharness.NewScriptedProvider(), repo, cache.NewFileStore(promptRoot))
	h.options.Now = func() time.Time { return now }
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendTurnMarker(ctx, repo, thread.ID(), "turn-stale", sessiontree.TurnStarted, nil); err != nil {
		t.Fatal(err)
	}
	if err := repo.AcquireTurnLease(ctx, sessiontree.TurnLease{ThreadID: thread.ID(), TurnID: "turn-stale", OwnerID: "dead-owner", CreatedAt: now.Add(-25 * time.Hour)}); err != nil {
		t.Fatal(err)
	}
	resumedHarness := newTestHarness(scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("ok"), scriptharness.Done())), sessiontree.NewFileRepo(root), cache.NewFileStore(promptRoot))
	resumedHarness.options.Now = func() time.Time { return now }
	resumed, err := resumedHarness.ResumeThread(ctx, thread.ID(), ResumeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	snap, err := resumed.Journal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(snap.Entries, func(entry sessiontree.Entry) bool {
		return entry.Type == sessiontree.EntryTurnMarker && entry.TurnID == "turn-stale" && entry.TurnStatus == sessiontree.TurnAborted
	}) {
		t.Fatalf("stale started turn should be marked aborted after expired lease recovery: %#v", snap.Entries)
	}
	if _, ok, err := sessiontree.NewFileRepo(root).ActiveTurnLease(ctx, thread.ID()); err != nil || ok {
		t.Fatalf("expired lease should be cleared, ok=%v err=%v", ok, err)
	}
}

func TestMoveToHoldsActiveTurnGuardDuringMutation(t *testing.T) {
	ctx := context.Background()
	repo := &blockingMoveRepo{MemoryRepo: sessiontree.NewMemoryRepo(), entered: make(chan struct{}), release: make(chan struct{})}
	h := newTestHarness(scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("done"), scriptharness.Done())), repo, cache.NewMemoryStore())
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := thread.Run(ctx, "first", RunOptions{TurnID: "turn-1"}); err != nil {
		t.Fatal(err)
	}
	snap, err := thread.Journal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	target := firstEntry(snap.Entries, sessiontree.EntryUserMessage).ID
	done := make(chan error, 1)
	go func() {
		done <- thread.MoveTo(ctx, target, MoveOptions{})
	}()
	<-repo.entered
	if _, err := thread.Run(ctx, "racing", RunOptions{TurnID: "turn-race"}); !errors.Is(err, ErrActiveTurn) {
		t.Fatalf("run err = %v, want active turn during MoveTo", err)
	}
	close(repo.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestManualCompactHoldsActiveTurnGuardDuringMutation(t *testing.T) {
	ctx := context.Background()
	repo := &blockingAppendRepo{MemoryRepo: sessiontree.NewMemoryRepo(), entered: make(chan struct{}), release: make(chan struct{})}
	h := newTestHarness(scriptharness.NewScriptedProvider(
		scriptharness.Step(scriptharness.Text("seeded"), scriptharness.Done()),
		scriptharness.Step(scriptharness.Text("tailed"), scriptharness.Done()),
		scriptharness.Step(scriptharness.Text("done"), scriptharness.Done()),
	), repo, cache.NewMemoryStore())
	h.options.CompactionGenerator = compaction.ExtractiveSummaryGenerator{}
	h.options.TurnPolicy.ContextPolicy.ContextWindowTokens = 256000
	h.options.TurnPolicy.ContextPolicy.ReservedOutputTokens = 64000
	h.options.TurnPolicy.ContextPolicy.ReservedSummaryTokens = 40
	h.options.TurnPolicy.ContextPolicy.RecentTailTokens = 20
	h.options.TurnPolicy.ContextPolicy.RecentUserTokens = 20
	h.options.TurnPolicy.ContextPolicy.CompactedContextTargetTokens = 100
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := thread.Run(ctx, strings.Repeat("old context ", 6000), RunOptions{TurnID: "turn-1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := thread.Run(ctx, "latest tail", RunOptions{TurnID: "turn-2"}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := thread.Compact(ctx, CompactOptions{RequestID: "manual-1"})
		done <- err
	}()
	<-repo.entered
	if _, err := thread.Run(ctx, "racing", RunOptions{TurnID: "turn-race"}); !errors.Is(err, ErrActiveTurn) {
		t.Fatalf("run err = %v, want active turn during Compact", err)
	}
	close(repo.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestDifferentThreadsRunConcurrently(t *testing.T) {
	ctx := context.Background()
	assertDifferentThreadsRunConcurrently(t, ctx, sessiontree.NewMemoryRepo(), cache.NewMemoryStore())
}

func TestSQLiteDifferentThreadsRunConcurrently(t *testing.T) {
	ctx := context.Background()
	repo, err := sqlite.Open(filepath.Join(t.TempDir(), "floret.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = repo.Close() })
	assertDifferentThreadsRunConcurrently(t, ctx, repo, repo)
}

func TestForkAndSourceRunConcurrentlyWithoutPollution(t *testing.T) {
	ctx := context.Background()
	assertForkAndSourceRunConcurrentlyWithoutPollution(t, ctx, sessiontree.NewMemoryRepo(), cache.NewMemoryStore())
}

func TestSQLiteForkAndSourceRunConcurrentlyWithoutPollution(t *testing.T) {
	ctx := context.Background()
	repo, err := sqlite.Open(filepath.Join(t.TempDir(), "floret.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = repo.Close() })
	assertForkAndSourceRunConcurrentlyWithoutPollution(t, ctx, repo, repo)
}

func assertDifferentThreadsRunConcurrently(t *testing.T, ctx context.Context, repo sessiontree.Repo, promptStore cache.Store) {
	t.Helper()
	provider := newConcurrentProvider(2)
	h := newTestHarness(provider, repo, promptStore)
	first, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread-a"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread-b"})
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	var firstResult, secondResult TurnResult
	var firstErr, secondErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		firstResult, firstErr = first.Run(ctx, "alpha", RunOptions{TurnID: "turn-a"})
	}()
	go func() {
		defer wg.Done()
		secondResult, secondErr = second.Run(ctx, "beta", RunOptions{TurnID: "turn-b"})
	}()
	wg.Wait()

	if firstErr != nil || secondErr != nil {
		t.Fatalf("run errors: first=%v second=%v", firstErr, secondErr)
	}
	if firstResult.Status != engine.Completed || secondResult.Status != engine.Completed {
		t.Fatalf("results = %#v %#v", firstResult, secondResult)
	}
	if provider.MaxConcurrent() < 2 {
		t.Fatalf("provider did not observe concurrent runs, max=%d", provider.MaxConcurrent())
	}
	firstSnap, _ := first.Journal(ctx)
	secondSnap, _ := second.Journal(ctx)
	if countEntriesWithContent(firstSnap.Entries, sessiontree.EntryUserMessage, "beta") != 0 ||
		countEntriesWithContent(secondSnap.Entries, sessiontree.EntryUserMessage, "alpha") != 0 {
		t.Fatalf("thread entries polluted: first=%#v second=%#v", firstSnap.Entries, secondSnap.Entries)
	}
}

func assertForkAndSourceRunConcurrentlyWithoutPollution(t *testing.T, ctx context.Context, repo sessiontree.Repo, promptStore cache.Store) {
	t.Helper()
	setup := scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("base"), scriptharness.Done()))
	h := newTestHarness(setup, repo, promptStore)
	source, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "source"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Run(ctx, "seed", RunOptions{TurnID: "turn-seed"}); err != nil {
		t.Fatal(err)
	}
	fork, err := h.ForkThread(ctx, ForkOptions{SourceThreadID: "source", NewThreadID: "fork"})
	if err != nil {
		t.Fatal(err)
	}
	concurrent := newConcurrentProvider(2)
	h.options.Provider = concurrent

	var wg sync.WaitGroup
	var sourceErr, forkErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, sourceErr = source.Run(ctx, "source-only", RunOptions{TurnID: "turn-source"})
	}()
	go func() {
		defer wg.Done()
		_, forkErr = fork.Run(ctx, "fork-only", RunOptions{TurnID: "turn-fork"})
	}()
	wg.Wait()
	if sourceErr != nil || forkErr != nil {
		t.Fatalf("run errors: source=%v fork=%v", sourceErr, forkErr)
	}
	if concurrent.MaxConcurrent() < 2 {
		t.Fatalf("provider did not observe concurrent source/fork runs, max=%d", concurrent.MaxConcurrent())
	}
	sourceSnap, _ := source.Journal(ctx)
	forkSnap, _ := fork.Journal(ctx)
	if countEntriesWithContent(sourceSnap.Entries, sessiontree.EntryUserMessage, "fork-only") != 0 ||
		countEntriesWithContent(forkSnap.Entries, sessiontree.EntryUserMessage, "source-only") != 0 {
		t.Fatalf("fork/source entries polluted: source=%#v fork=%#v", sourceSnap.Entries, forkSnap.Entries)
	}
	if countEntriesWithContent(sourceSnap.Entries, sessiontree.EntryUserMessage, "source-only") != 1 ||
		countEntriesWithContent(forkSnap.Entries, sessiontree.EntryUserMessage, "fork-only") != 1 {
		t.Fatalf("own continuation missing: source=%#v fork=%#v", sourceSnap.Entries, forkSnap.Entries)
	}
}

func TestToolProjectionDoesNotDuplicateMultiToolBatches(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	promptStore := cache.NewMemoryStore()
	p := scriptharness.NewScriptedProvider(
		scriptharness.Step(
			provider.StreamEvent{Type: provider.Reasoning, Text: "first batch"},
			provider.StreamEvent{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{
				{ID: "call-00-a", Name: "read", Args: `{"value":"a"}`, Reasoning: "first batch"},
				{ID: "call-01-a", Name: "read", Args: `{"value":"b"}`, Reasoning: "first batch"},
			}},
			scriptharness.DoneReason("tool_calls"),
		),
		scriptharness.Step(
			provider.StreamEvent{Type: provider.Reasoning, Text: "second batch"},
			provider.StreamEvent{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{
				{ID: "call-00-b", Name: "read", Args: `{"value":"c"}`, Reasoning: "second batch"},
				{ID: "call-01-b", Name: "read", Args: `{"value":"d"}`, Reasoning: "second batch"},
			}},
			scriptharness.DoneReason("tool_calls"),
		),
		scriptharness.Step(scriptharness.Text("done"), scriptharness.Done()),
		scriptharness.Step(scriptharness.Text("follow-up"), scriptharness.Done()),
	)
	registry := tools.NewRegistry()
	mustRegister(registry, stringTool("read", func(_ context.Context, value string) (string, error) {
		return "result " + value, nil
	}))
	h := New(Options{
		Provider:     p,
		ProviderName: "fake",
		Model:        "fake-model",
		SystemPrompt: "You are a test assistant.",
		Tools:        registry,
		Repo:         repo,
		PromptStore:  promptStore,
		TitleGenerator: fixedTitleGenerator{
			title: "Test thread title",
		},
		LoopLimits: LoopLimits{MaxEmptyProviderRetries: 1, NoProgressLimit: 2, DuplicateToolLimit: 3},
	})
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	first, err := thread.Run(ctx, "inspect", RunOptions{TurnID: "turn-1"})
	if err != nil || first.Status != engine.Completed {
		t.Fatalf("first = %#v err=%v", first, err)
	}
	snap, err := thread.Journal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"call-00-a", "call-01-a", "call-00-b", "call-01-b"} {
		if got := countToolEntries(snap.Entries, sessiontree.EntryToolCall, id); got != 1 {
			t.Fatalf("tool call %s count = %d in %#v", id, got, snap.Entries)
		}
		if got := countToolEntries(snap.Entries, sessiontree.EntryToolResult, id); got != 1 {
			t.Fatalf("tool result %s count = %d in %#v", id, got, snap.Entries)
		}
	}
	if got := countEntriesWithContent(snap.Entries, sessiontree.EntryAssistantMessage, "done"); got != 1 {
		t.Fatalf("final assistant message count = %d in %#v", got, snap.Entries)
	}
	for _, marker := range savePointEntries(snap.Entries, "tool_result_batch") {
		path, err := repo.Path(ctx, "thread", marker.ParentID)
		if err != nil {
			t.Fatal(err)
		}
		if err := assertProviderSafeToolHistory(sessiontree.BuildContext(path, sessiontree.ContextOptions{})); err != nil {
			t.Fatalf("save point before %s is not provider-safe: %v", marker.ID, err)
		}
	}
	second, err := thread.Run(ctx, "continue", RunOptions{TurnID: "turn-2"})
	if err != nil || second.Status != engine.Completed {
		t.Fatalf("second = %#v err=%v", second, err)
	}
	snap, err = thread.Journal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := assertProviderSafeToolHistory(snap.Context); err != nil {
		t.Fatalf("follow-up context is not provider-safe: %v\n%#v", err, snap.Context)
	}
	if got := countEntriesWithContent(snap.Entries, sessiontree.EntryAssistantMessage, "done"); got != 1 {
		t.Fatalf("follow-up should not duplicate previous final assistant message: count=%d entries=%#v", got, snap.Entries)
	}
}

func TestTurnProjectionCanceledTurnClosesUnresolvedToolBatch(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	promptStore := cache.NewMemoryStore()
	p := scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("follow-up"), scriptharness.Done()))
	h := newTestHarness(p, repo, promptStore)
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendTurnMarker(ctx, repo, "thread", "turn-1", sessiontree.TurnStarted, map[string]string{"run_id": "run-1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendMessage(ctx, repo, "thread", "turn-1", session.Message{Role: session.User, Content: "inspect"}); err != nil {
		t.Fatal(err)
	}

	projection := &turnProjection{thread: thread, ctx: ctx, turnID: "turn-1", runID: "run-1"}
	projection.Emit(event.Event{Type: event.ToolCall, ToolID: "call-1", ToolName: "read", Args: `{"value":"a"}`, Metadata: map[string]any{"batch_size": 2}})
	projection.Emit(event.Event{Type: event.ToolCall, ToolID: "call-2", ToolName: "read", Args: `{"value":"b"}`, Metadata: map[string]any{"batch_size": 2}})
	projection.Emit(event.Event{Type: event.ToolResult, ToolID: "call-1", ToolName: "read", Result: "result a", Metadata: map[string]any{"batch_size": 2, "tool_result_status": string(observation.ActivityStatusSuccess)}})
	if err := projection.FlushForTurnStatus(engine.Cancelled, context.Canceled); err != nil {
		t.Fatalf("FlushForTurnStatus: %v", err)
	}
	if _, err := sessiontree.AppendTurnMarker(ctx, repo, "thread", "turn-1", sessiontree.TurnAborted, map[string]string{"run_id": "run-1"}); err != nil {
		t.Fatal(err)
	}

	snap, err := thread.Journal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"call-1", "call-2"} {
		if got := countToolEntries(snap.Entries, sessiontree.EntryToolCall, id); got != 1 {
			t.Fatalf("tool call %s count = %d in %#v", id, got, snap.Entries)
		}
		if got := countToolEntries(snap.Entries, sessiontree.EntryToolResult, id); got != 1 {
			t.Fatalf("tool result %s count = %d in %#v", id, got, snap.Entries)
		}
	}
	if status := toolResultStatusForCall(snap.Entries, "call-2"); status != string(observation.ActivityStatusCanceled) {
		t.Fatalf("call-2 status = %q, want canceled in %#v", status, snap.Entries)
	}
	if err := assertProviderSafeToolHistory(snap.Context); err != nil {
		t.Fatalf("provider history after canceled turn is unsafe: %v", err)
	}

	second, err := thread.Run(ctx, "continue", RunOptions{TurnID: "turn-2"})
	if err != nil || second.Status != engine.Completed {
		t.Fatalf("second = %#v err=%v", second, err)
	}
}

func TestTurnProjectionFailedTurnClosesUnresolvedToolBatch(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	promptStore := cache.NewMemoryStore()
	p := scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("follow-up"), scriptharness.Done()))
	h := newTestHarness(p, repo, promptStore)
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendTurnMarker(ctx, repo, "thread", "turn-1", sessiontree.TurnStarted, map[string]string{"run_id": "run-1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendMessage(ctx, repo, "thread", "turn-1", session.Message{Role: session.User, Content: "inspect"}); err != nil {
		t.Fatal(err)
	}

	projection := &turnProjection{thread: thread, ctx: ctx, turnID: "turn-1", runID: "run-1"}
	projection.Emit(event.Event{Type: event.ToolCall, ToolID: "call-1", ToolName: "read", Args: `{"value":"a"}`, Metadata: map[string]any{"batch_size": 2}})
	projection.Emit(event.Event{Type: event.ToolCall, ToolID: "call-2", ToolName: "read", Args: `{"value":"b"}`, Metadata: map[string]any{"batch_size": 2}})
	projection.Emit(event.Event{Type: event.ToolResult, ToolID: "call-1", ToolName: "read", Result: "result a", Metadata: map[string]any{"batch_size": 2, "tool_result_status": string(observation.ActivityStatusSuccess)}})
	cause := errors.New("provider stopped mid-turn")
	if err := projection.FlushForTurnStatus(engine.Failed, cause); err != nil {
		t.Fatalf("FlushForTurnStatus: %v", err)
	}
	if _, err := sessiontree.AppendFailure(ctx, repo, "thread", "turn-1", cause.Error()); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendTurnMarker(ctx, repo, "thread", "turn-1", sessiontree.TurnFailed, map[string]string{"run_id": "run-1"}); err != nil {
		t.Fatal(err)
	}

	snap, err := thread.Journal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"call-1", "call-2"} {
		if got := countToolEntries(snap.Entries, sessiontree.EntryToolCall, id); got != 1 {
			t.Fatalf("tool call %s count = %d in %#v", id, got, snap.Entries)
		}
		if got := countToolEntries(snap.Entries, sessiontree.EntryToolResult, id); got != 1 {
			t.Fatalf("tool result %s count = %d in %#v", id, got, snap.Entries)
		}
	}
	if status := toolResultStatusForCall(snap.Entries, "call-2"); status != string(observation.ActivityStatusError) {
		t.Fatalf("call-2 status = %q, want error in %#v", status, snap.Entries)
	}
	if err := assertProviderSafeToolHistory(snap.Context); err != nil {
		t.Fatalf("provider history after failed turn is unsafe: %v", err)
	}

	second, err := thread.Run(ctx, "continue", RunOptions{TurnID: "turn-2"})
	if err != nil || second.Status != engine.Completed {
		t.Fatalf("second = %#v err=%v", second, err)
	}
}

func TestAppendDeltaSkipsProjectedAssistantFinalButKeepsSeparateTurns(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	promptStore := cache.NewMemoryStore()
	h := newTestHarness(scriptharness.NewScriptedProvider(), repo, promptStore)
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	projected := session.Message{Role: session.Assistant, Content: "final answer", Reasoning: "done reasoning"}
	if err := thread.appendMessage(ctx, "turn-1", "run-1", projected); err != nil {
		t.Fatal(err)
	}
	snap, err := thread.Journal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := thread.appendDelta(ctx, "turn-1", "run-1", nil, []session.Message{projected}, snap.Path); err != nil {
		t.Fatal(err)
	}
	snap, err = thread.Journal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := countEntriesWithContent(snap.Entries, sessiontree.EntryAssistantMessage, "final answer"); got != 1 {
		t.Fatalf("projected assistant final should not be backfilled again: count=%d entries=%#v", got, snap.Entries)
	}
	if err := thread.appendDelta(ctx, "turn-2", "run-2", nil, []session.Message{projected}, snap.Path); err != nil {
		t.Fatal(err)
	}
	snap, err = thread.Journal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := countEntriesWithContent(snap.Entries, sessiontree.EntryAssistantMessage, "final answer"); got != 2 {
		t.Fatalf("same assistant content in another turn should remain valid: count=%d entries=%#v", got, snap.Entries)
	}
}

func TestResumeMarksUnfinishedTurnInterrupted(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	h := newTestHarness(scriptharness.NewScriptedProvider(), repo, cache.NewMemoryStore())
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendTurnMarker(ctx, repo, "thread", "turn-interrupted", sessiontree.TurnStarted, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendMessage(ctx, repo, "thread", "turn-interrupted", session.Message{Role: session.User, Content: "unfinished"}); err != nil {
		t.Fatal(err)
	}
	resumed, err := h.ResumeThread(ctx, thread.ID(), ResumeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	snap, err := resumed.Journal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !hasEntry(snap.Entries, sessiontree.EntryTurnMarker, sessiontree.TurnAborted) {
		t.Fatalf("unfinished turn should be marked aborted: %#v", snap.Entries)
	}
	if !slices.ContainsFunc(snap.Entries, func(entry sessiontree.Entry) bool {
		return entry.Type == sessiontree.EntryRunFailure && entry.TurnID == "turn-interrupted" && entry.Error == "turn interrupted during previous process"
	}) {
		t.Fatalf("unfinished turn failure marker missing: %#v", snap.Entries)
	}
}

func TestResumeInterruptedTurnClosesUnresolvedToolCallAndContinues(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		repo sessiontree.Repo
	}{
		{name: "memory", repo: sessiontree.NewMemoryRepo()},
		{name: "sqlite", repo: openSQLiteRepoForHarnessTest(t)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newTestHarness(scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("continued"), scriptharness.Done())), tc.repo, cache.NewMemoryStore())
			thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
			if err != nil {
				t.Fatal(err)
			}
			appendInterruptedWaitTurn(t, ctx, tc.repo, "thread", "turn-interrupted")

			resumed, err := h.ResumeThread(ctx, thread.ID(), ResumeOptions{})
			if err != nil {
				t.Fatal(err)
			}
			snap, err := resumed.Journal(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if got := countToolEntries(snap.Entries, sessiontree.EntryToolResult, "call-wait"); got != 1 {
				t.Fatalf("tool result count = %d in %#v", got, snap.Entries)
			}
			if status := toolResultStatusForCall(snap.Entries, "call-wait"); status != string(observation.ActivityStatusCanceled) {
				t.Fatalf("call-wait status = %q, want canceled", status)
			}
			if err := assertProviderSafeToolHistory(snap.Context); err != nil {
				t.Fatalf("provider history after resume is unsafe: %v\n%#v", err, snap.Context)
			}
			second, err := resumed.Run(ctx, "continue", RunOptions{TurnID: "turn-continue"})
			if err != nil || second.Status != engine.Completed {
				t.Fatalf("second = %#v err=%v", second, err)
			}
		})
	}
}

func TestResumeRepairsUnsafeInterruptedActiveBranch(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		repo sessiontree.Repo
	}{
		{name: "memory", repo: sessiontree.NewMemoryRepo()},
		{name: "sqlite", repo: openSQLiteRepoForHarnessTest(t)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newTestHarness(scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("continued"), scriptharness.Done())), tc.repo, cache.NewMemoryStore())
			thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
			if err != nil {
				t.Fatal(err)
			}
			appendInterruptedWaitTurn(t, ctx, tc.repo, "thread", "turn-interrupted")
			if _, err := sessiontree.AppendFailure(ctx, tc.repo, "thread", "turn-interrupted", interruptedTurnFailureMessage); err != nil {
				t.Fatal(err)
			}
			if _, err := sessiontree.AppendTurnMarker(ctx, tc.repo, "thread", "turn-interrupted", sessiontree.TurnAborted, map[string]string{"recoverable": "true"}); err != nil {
				t.Fatal(err)
			}
			if _, err := sessiontree.AppendTurnMarker(ctx, tc.repo, "thread", "turn-bad-continue", sessiontree.TurnStarted, nil); err != nil {
				t.Fatal(err)
			}
			if _, err := sessiontree.AppendMessage(ctx, tc.repo, "thread", "turn-bad-continue", session.Message{Role: session.User, Content: "continue"}); err != nil {
				t.Fatal(err)
			}
			badLeaf, err := tc.repo.Append(ctx, sessiontree.Entry{ThreadID: "thread", TurnID: "turn-bad-continue", Type: sessiontree.EntryRunFailure, Error: "provider history unsafe"}, sessiontree.AppendOptions{})
			if err != nil {
				t.Fatal(err)
			}

			resumed, err := h.ResumeThread(ctx, thread.ID(), ResumeOptions{})
			if err != nil {
				t.Fatal(err)
			}
			snap, err := resumed.Journal(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if snap.Meta.LeafID == badLeaf.ID {
				t.Fatalf("active leaf still points at unsafe branch: %q", snap.Meta.LeafID)
			}
			if got := countEntriesWithContent(snap.Path, sessiontree.EntryUserMessage, "continue"); got != 0 {
				t.Fatalf("active repaired path retained bad continue message: count=%d path=%#v", got, snap.Path)
			}
			if got := countToolEntries(snap.Path, sessiontree.EntryToolResult, "call-wait"); got != 1 {
				t.Fatalf("tool result count = %d in %#v", got, snap.Path)
			}
			if err := assertProviderSafeToolHistory(snap.Context); err != nil {
				t.Fatalf("repaired active context is unsafe: %v\n%#v", err, snap.Context)
			}
			oldBranch, err := tc.repo.Path(ctx, "thread", badLeaf.ID)
			if err != nil {
				t.Fatal(err)
			}
			if got := countEntriesWithContent(oldBranch, sessiontree.EntryUserMessage, "continue"); got != 1 {
				t.Fatalf("old unsafe branch should remain readable: count=%d entries=%#v", got, oldBranch)
			}
			second, err := resumed.Run(ctx, "continue", RunOptions{TurnID: "turn-continue"})
			if err != nil || second.Status != engine.Completed {
				t.Fatalf("second = %#v err=%v", second, err)
			}
		})
	}
}

func TestActiveTurnBusyGuard(t *testing.T) {
	ctx := context.Background()
	p := newBlockingProvider()
	repo := sessiontree.NewMemoryRepo()
	h := newTestHarness(p, repo, cache.NewMemoryStore())
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := thread.Run(runCtx, "hang", RunOptions{TurnID: "turn-1"})
		done <- err
	}()
	<-p.started
	_, err = thread.Run(ctx, "second", RunOptions{TurnID: "turn-2"})
	if !errors.Is(err, ErrActiveTurn) {
		t.Fatalf("err = %v, want active turn guard", err)
	}
	snap, err := thread.Journal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if slices.ContainsFunc(snap.Entries, func(entry sessiontree.Entry) bool {
		return entry.TurnID == "turn-2" || (entry.Type == sessiontree.EntryUserMessage && entry.Message.Content == "second")
	}) {
		t.Fatalf("rejected active turn should not append entries: %#v", snap.Entries)
	}
	cancel()
	<-done
}

func TestSQLiteRepoActiveTurnBusyGuard(t *testing.T) {
	ctx := context.Background()
	p := newBlockingProvider()
	repo, err := sqlite.Open(filepath.Join(t.TempDir(), "floret.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = repo.Close() })
	h := newTestHarness(p, repo, cache.NewMemoryStore())
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := thread.Run(runCtx, "hang", RunOptions{TurnID: "turn-live"})
		done <- err
	}()
	<-p.started
	_, err = thread.Run(ctx, "second", RunOptions{TurnID: "turn-second"})
	if !errors.Is(err, ErrActiveTurn) {
		t.Fatalf("err = %v, want active turn guard", err)
	}
	snap, err := thread.Journal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if slices.ContainsFunc(snap.Entries, func(entry sessiontree.Entry) bool {
		return entry.TurnID == "turn-second" || (entry.Type == sessiontree.EntryUserMessage && entry.Message.Content == "second")
	}) {
		t.Fatalf("rejected sqlite active turn should not append entries: %#v", snap.Entries)
	}
	cancel()
	<-done
}

func TestRetryDuringActiveTurnReturnsErrActiveTurnWithoutMovingLeaf(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	promptStore := cache.NewMemoryStore()
	failing := scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("failed"), scriptharness.DoneReason("error")))
	h := newTestHarness(failing, repo, promptStore)
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := thread.Run(ctx, "retryable", RunOptions{TurnID: "turn-failed"}); err == nil {
		t.Fatalf("first run should fail")
	}
	failedSnap, err := thread.Journal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	failedLeaf := failedSnap.Meta.LeafID

	blocking := newBlockingProvider()
	h.options.Provider = blocking
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := thread.Run(runCtx, "active", RunOptions{TurnID: "turn-active"})
		done <- err
	}()
	<-blocking.started

	_, err = thread.Retry(ctx, RetryOptions{Reason: "busy"})
	if !errors.Is(err, ErrActiveTurn) {
		t.Fatalf("err = %v, want active turn guard", err)
	}
	activeSnap, err := thread.Journal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if activeSnap.Meta.LeafID == failedLeaf {
		t.Fatalf("active turn should have advanced leaf before retry check")
	}
	if slices.ContainsFunc(activeSnap.Entries, func(entry sessiontree.Entry) bool {
		return entry.TurnID != "" && strings.HasPrefix(entry.TurnID, "turn-") && entry.TurnID != "turn-failed" && entry.TurnID != "turn-active"
	}) {
		t.Fatalf("busy retry should not append retry entries: %#v", activeSnap.Entries)
	}
	cancel()
	<-done
}

func TestThreadRunPersistsTerminalMarkerAfterDeadline(t *testing.T) {
	ctx := context.Background()
	p := newBlockingProvider()
	repo := sessiontree.NewMemoryRepo()
	h := newTestHarness(p, repo, cache.NewMemoryStore())
	h.options.LoopLimits.WallTime = 10 * time.Millisecond
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}

	result, err := thread.Run(ctx, "hang", RunOptions{TurnID: "turn-deadline"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want deadline exceeded", err)
	}
	if result.Status != engine.Cancelled {
		t.Fatalf("result = %#v", result)
	}
	snap, err := thread.Journal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(snap.Entries, func(entry sessiontree.Entry) bool {
		return entry.Type == sessiontree.EntryRunFailure && entry.TurnID == "turn-deadline" && strings.Contains(entry.Error, context.DeadlineExceeded.Error())
	}) {
		t.Fatalf("deadline failure was not persisted: %#v", snap.Entries)
	}
	if !slices.ContainsFunc(snap.Entries, func(entry sessiontree.Entry) bool {
		return entry.Type == sessiontree.EntryTurnMarker && entry.TurnID == "turn-deadline" && entry.TurnStatus == sessiontree.TurnAborted
	}) {
		t.Fatalf("deadline terminal marker was not persisted: %#v", snap.Entries)
	}
	lifecycle := sessionlifecycle.Derive(snap.Path, sessionlifecycle.PhaseIdle)
	if lifecycle.Status() != string(engine.Cancelled) || lifecycle.CanAppendMessage() {
		t.Fatalf("lifecycle = %#v", lifecycle)
	}
}

func TestThreadRunProjectionCancellationMarksTurnAborted(t *testing.T) {
	ctx := context.Background()
	p := scriptharness.NewScriptedProvider(
		scriptharness.Step(
			scriptharness.Tool("read-1", "read", `{"value":"README.md"}`),
			scriptharness.DoneReason("tool_calls"),
		),
	)
	repo := &blockingToolDispatchRepo{
		MemoryRepo: sessiontree.NewMemoryRepo(),
		entered:    make(chan struct{}),
		release:    make(chan struct{}),
	}
	h := newTestHarness(p, repo, cache.NewMemoryStore())
	mustRegister(h.options.Tools, stringTool("read", func(context.Context, string) (string, error) {
		return "ok", nil
	}))
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct {
		result TurnResult
		err    error
	}, 1)
	go func() {
		result, err := thread.Run(runCtx, "read", RunOptions{TurnID: "turn-canceled"})
		done <- struct {
			result TurnResult
			err    error
		}{result: result, err: err}
	}()

	select {
	case <-repo.entered:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("tool dispatch projection did not start")
	}
	cancel()

	select {
	case out := <-done:
		if !errors.Is(out.err, context.Canceled) {
			t.Fatalf("run err = %v, want context canceled", out.err)
		}
		if out.result.Status != engine.Cancelled {
			t.Fatalf("result status = %s, want cancelled", out.result.Status)
		}
	case <-time.After(time.Second):
		t.Fatal("run did not finish after cancellation")
	}
	snap, err := thread.Journal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(snap.Entries, func(entry sessiontree.Entry) bool {
		return entry.Type == sessiontree.EntryTurnMarker &&
			entry.TurnID == "turn-canceled" &&
			entry.TurnStatus == sessiontree.TurnAborted &&
			entry.Metadata["diagnostic"] == "projection_error"
	}) {
		t.Fatalf("projection cancellation should persist aborted terminal marker: %#v", snap.Entries)
	}
	if slices.ContainsFunc(snap.Entries, func(entry sessiontree.Entry) bool {
		return entry.Type == sessiontree.EntryTurnMarker &&
			entry.TurnID == "turn-canceled" &&
			entry.TurnStatus == sessiontree.TurnFailed
	}) {
		t.Fatalf("projection cancellation persisted failed marker: %#v", snap.Entries)
	}
}

func newTestHarness(p provider.Provider, repo sessiontree.Repo, promptStore cache.Store) *AgentHarness {
	rec := &event.Recorder{}
	registry := tools.NewRegistry()
	return New(Options{
		Provider:       p,
		ProviderName:   "fake",
		Model:          "fake-model",
		SystemPrompt:   "You are a test assistant.",
		Tools:          registry,
		Repo:           repo,
		PromptStore:    promptStore,
		Sink:           rec,
		TitleGenerator: fixedTitleGenerator{title: "Test thread title"},
		LoopLimits: LoopLimits{
			MaxEmptyProviderRetries: 1,
			NoProgressLimit:         2,
			DuplicateToolLimit:      3,
		},
	})
}

func mustRegister(registry *tools.Registry, tool tools.Tool) {
	if err := registry.Register(tool); err != nil {
		panic(err)
	}
}

func appendPendingToolResultFixture(t *testing.T, ctx context.Context, repo sessiontree.Repo, threadID string, turnID string) {
	t.Helper()
	appendPendingToolCallFixture(t, ctx, repo, threadID, turnID)
	if _, err := sessiontree.AppendMessage(ctx, repo, threadID, turnID, session.Message{
		Role:       session.Tool,
		Content:    "<pending_tool_result>\n<summary>Command is running</summary>\n<instruction>wait</instruction>\n<handle>terminal:job:123</handle>\n</pending_tool_result>",
		ToolCallID: "exec-1",
		ToolName:   "terminal.exec",
		ToolResult: &session.ToolResultView{Status: string(observation.ActivityStatusRunning)},
		Activity: &session.ActivityPresentation{
			Label:       "npm test",
			Description: "Command is running",
			Renderer:    string(observation.ActivityRendererTerminal),
			Chips: []session.ActivityChip{
				{Kind: "state", Label: "State", Value: string(observation.ActivityStatusRunning), Tone: "running"},
				{Kind: "handle", Label: "Handle", Value: "terminal:job:123", Tone: "quiet"},
			},
			Payload: map[string]any{"command": "npm test", "pending_handle": "terminal:job:123", "pending_state": string(observation.ActivityStatusRunning)},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendTurnMarker(ctx, repo, threadID, turnID, sessiontree.TurnCompleted, nil); err != nil {
		t.Fatal(err)
	}
}

func appendPendingToolCallFixture(t *testing.T, ctx context.Context, repo sessiontree.Repo, threadID string, turnID string) {
	t.Helper()
	if _, err := sessiontree.AppendTurnMarker(ctx, repo, threadID, turnID, sessiontree.TurnStarted, map[string]string{"run_id": "run-1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendMessage(ctx, repo, threadID, turnID, session.Message{Role: session.User, Content: "run command"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendMessage(ctx, repo, threadID, turnID, session.Message{
		Role:       session.Assistant,
		Content:    "tool_call",
		ToolCallID: "exec-1",
		ToolName:   "terminal.exec",
		ToolArgs:   `{"command":"npm test"}`,
		Activity: &session.ActivityPresentation{
			Label:    "npm test",
			Renderer: string(observation.ActivityRendererTerminal),
			Payload:  map[string]any{"command": "npm test"},
		},
	}); err != nil {
		t.Fatal(err)
	}
}

func countPendingToolSettlementEntries(entries []sessiontree.Entry) int {
	count := 0
	for _, entry := range entries {
		if entry.Type == sessiontree.EntryCustom && entry.Metadata[subAgentDetailKindKey] == pendingToolSettlementEntryKind {
			count++
		}
	}
	return count
}

type stringArgs struct {
	Value string `json:"value"`
}

type failingTitleGenerator struct {
	err error
}

func (g failingTitleGenerator) GenerateTitle(context.Context, TitleRequest) (TitleResult, error) {
	return TitleResult{}, g.err
}

type fixedTitleGenerator struct {
	title string
}

func (g fixedTitleGenerator) GenerateTitle(context.Context, TitleRequest) (TitleResult, error) {
	return TitleResult{Title: g.title, Source: sessiontree.ThreadTitleSourceProvider}, nil
}

type estimatingHarnessProvider struct {
	provider.Provider
	estimates []provider.TokenEstimate
	err       error
}

func (p *estimatingHarnessProvider) EstimateTokens(context.Context, provider.Request) (provider.TokenEstimate, error) {
	if p.err != nil {
		return provider.TokenEstimate{}, p.err
	}
	if len(p.estimates) > 0 {
		next := p.estimates[0]
		p.estimates = p.estimates[1:]
		return next, nil
	}
	return provider.TokenEstimate{}, nil
}

func stringTool(name string, handler func(context.Context, string) (string, error)) tools.Tool {
	return tools.Define[stringArgs](
		tools.Definition{
			Name: name,
			InputSchema: tools.StrictObject(map[string]any{
				"value": tools.String("test value"),
			}, []string{"value"}),
			Permission: tools.PermissionSpec{Mode: tools.PermissionAllow},
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

func hasEntry(entries []sessiontree.Entry, entryType sessiontree.EntryType, status sessiontree.TurnMarkerStatus) bool {
	return slices.ContainsFunc(entries, func(entry sessiontree.Entry) bool {
		return entry.Type == entryType && entry.TurnStatus == status
	})
}

func firstTurnMarker(entries []sessiontree.Entry, status sessiontree.TurnMarkerStatus) sessiontree.Entry {
	for _, entry := range entries {
		if entry.Type == sessiontree.EntryTurnMarker && entry.TurnStatus == status {
			return entry
		}
	}
	return sessiontree.Entry{}
}

func countEntries(entries []sessiontree.Entry, entryType sessiontree.EntryType) int {
	count := 0
	for _, entry := range entries {
		if entry.Type == entryType {
			count++
		}
	}
	return count
}

func countEntriesWithContent(entries []sessiontree.Entry, entryType sessiontree.EntryType, content string) int {
	count := 0
	for _, entry := range entries {
		if entry.Type == entryType && entry.Message.Content == content {
			count++
		}
	}
	return count
}

func countToolEntries(entries []sessiontree.Entry, entryType sessiontree.EntryType, callID string) int {
	count := 0
	for _, entry := range entries {
		if entry.Type == entryType && entry.Message.ToolCallID == callID {
			count++
		}
	}
	return count
}

func toolResultStatusForCall(entries []sessiontree.Entry, callID string) string {
	for _, entry := range entries {
		if entry.Type != sessiontree.EntryToolResult || entry.Message.ToolCallID != callID || entry.Message.ToolResult == nil {
			continue
		}
		return entry.Message.ToolResult.Status
	}
	return ""
}

func openSQLiteRepoForHarnessTest(t *testing.T) sessiontree.Repo {
	t.Helper()
	repo, err := sqlite.Open(filepath.Join(t.TempDir(), "floret.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = repo.Close() })
	return repo
}

func appendInterruptedWaitTurn(t *testing.T, ctx context.Context, repo sessiontree.Repo, threadID string, turnID string) {
	t.Helper()
	if _, err := sessiontree.AppendTurnMarker(ctx, repo, threadID, turnID, sessiontree.TurnStarted, map[string]string{"run_id": "run-interrupted"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendMessage(ctx, repo, threadID, turnID, session.Message{Role: session.User, Content: "start subagents"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendMessage(ctx, repo, threadID, turnID, session.Message{Role: session.Assistant, Content: "waiting for delegated work"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendMessage(ctx, repo, threadID, turnID, session.Message{
		Role:       session.Assistant,
		Content:    "tool_call",
		ToolCallID: "call-wait",
		ToolName:   "subagents",
		ToolArgs:   `{"action":"wait","ids":["child"],"timeout_ms":300000}`,
	}); err != nil {
		t.Fatal(err)
	}
}

func savePointEntries(entries []sessiontree.Entry, reason string) []sessiontree.Entry {
	var out []sessiontree.Entry
	for _, entry := range entries {
		if entry.Type == sessiontree.EntryTurnMarker && entry.TurnStatus == sessiontree.TurnSavePoint && entry.Metadata["reason"] == reason {
			out = append(out, entry)
		}
	}
	return out
}

func assertProviderSafeToolHistory(messages []session.Message) error {
	for i := 0; i < len(messages); i++ {
		msg := messages[i]
		if msg.Role == session.Tool {
			return fmt.Errorf("orphan tool result %q at %d", msg.ToolCallID, i)
		}
		if msg.Role != session.Assistant || msg.ToolCallID == "" || msg.ToolName == "" {
			continue
		}
		var calls []session.Message
		for i < len(messages) && messages[i].Role == session.Assistant && messages[i].ToolCallID != "" && messages[i].ToolName != "" {
			calls = append(calls, messages[i])
			i++
		}
		for _, call := range calls {
			if i >= len(messages) {
				return fmt.Errorf("missing result for %q", call.ToolCallID)
			}
			result := messages[i]
			if result.Role != session.Tool {
				return fmt.Errorf("got %q before result for %q", result.Role, call.ToolCallID)
			}
			if result.ToolCallID != call.ToolCallID {
				return fmt.Errorf("result %q does not match call %q", result.ToolCallID, call.ToolCallID)
			}
			i++
		}
		i--
	}
	return nil
}

func userContents(messages []session.Message) []string {
	var out []string
	for _, msg := range messages {
		if msg.Role == session.User {
			out = append(out, msg.Content)
		}
	}
	return out
}

func countUserMessagesInSnapshot(snap ThreadJournalSnapshot, content string) int {
	count := 0
	for _, entry := range snap.Entries {
		if entry.Type == sessiontree.EntryUserMessage && entry.Message.Content == content {
			count++
		}
	}
	return count
}

func segmentRaws(segments []cache.Segment) []string {
	out := make([]string, len(segments))
	for i, segment := range segments {
		out[i] = segment.Raw
	}
	return out
}

func messagePrefixEqual(got, want []session.Message) bool {
	if len(got) < len(want) {
		return false
	}
	for i, msg := range want {
		candidate := got[i]
		candidate.EntryID = ""
		candidate.ParentEntryID = ""
		candidate.ToolResult = nil
		if candidate != msg {
			return false
		}
	}
	return true
}

func firstEntry(entries []sessiontree.Entry, entryType sessiontree.EntryType) sessiontree.Entry {
	for _, entry := range entries {
		if entry.Type == entryType {
			return entry
		}
	}
	return sessiontree.Entry{}
}

func latestEntry(entries []sessiontree.Entry, entryType sessiontree.EntryType) sessiontree.Entry {
	for i := len(entries) - 1; i >= 0; i-- {
		if entries[i].Type == entryType {
			return entries[i]
		}
	}
	return sessiontree.Entry{}
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

func firstMessageByKind(messages []session.Message, kind session.MessageKind) (session.Message, bool) {
	for _, msg := range messages {
		if msg.Kind == kind {
			return msg, true
		}
	}
	return session.Message{}, false
}

type blockingProvider struct {
	started chan struct{}
	once    sync.Once
}

type blockingMoveRepo struct {
	*sessiontree.MemoryRepo
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (r *blockingMoveRepo) MoveLeaf(ctx context.Context, threadID, entryID string) error {
	r.once.Do(func() { close(r.entered) })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.release:
	}
	return r.MemoryRepo.MoveLeaf(ctx, threadID, entryID)
}

type blockingAppendRepo struct {
	*sessiontree.MemoryRepo
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (r *blockingAppendRepo) Append(ctx context.Context, entry sessiontree.Entry, opts sessiontree.AppendOptions) (sessiontree.Entry, error) {
	if entry.Type == sessiontree.EntryCompaction {
		r.once.Do(func() { close(r.entered) })
		select {
		case <-ctx.Done():
			return sessiontree.Entry{}, ctx.Err()
		case <-r.release:
		}
	}
	return r.MemoryRepo.Append(ctx, entry, opts)
}

type blockingToolDispatchRepo struct {
	*sessiontree.MemoryRepo
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (r *blockingToolDispatchRepo) Append(ctx context.Context, entry sessiontree.Entry, opts sessiontree.AppendOptions) (sessiontree.Entry, error) {
	if entry.Type == sessiontree.EntryCustom &&
		entry.Metadata[subAgentDetailKindKey] == toolDispatchEntryKind {
		r.once.Do(func() { close(r.entered) })
		select {
		case <-ctx.Done():
			return sessiontree.Entry{}, ctx.Err()
		case <-r.release:
		}
	}
	return r.MemoryRepo.Append(ctx, entry, opts)
}

type committedCompactionAppendRepo struct {
	*sessiontree.MemoryRepo
	compactionCommittedErrors int
}

func (r *committedCompactionAppendRepo) Append(ctx context.Context, entry sessiontree.Entry, opts sessiontree.AppendOptions) (sessiontree.Entry, error) {
	committed, err := r.MemoryRepo.Append(ctx, entry, opts)
	if err != nil {
		return committed, err
	}
	if entry.Type != sessiontree.EntryCompaction || r.compactionCommittedErrors > 0 {
		return committed, nil
	}
	r.compactionCommittedErrors++
	return committed, sessiontree.AppendCommittedError{Err: errors.New("thread snapshot save failed after compaction append")}
}

func newBlockingProvider() *blockingProvider {
	return &blockingProvider{started: make(chan struct{})}
}

func (p *blockingProvider) Stream(ctx context.Context, _ provider.Request) (<-chan provider.StreamEvent, error) {
	ch := make(chan provider.StreamEvent)
	p.once.Do(func() { close(p.started) })
	go func() {
		defer close(ch)
		<-ctx.Done()
	}()
	return ch, nil
}

type concurrentProvider struct {
	mu        sync.Mutex
	want      int
	active    int
	maxActive int
	arrived   int
	released  chan struct{}
	requests  []provider.Request
}

func newConcurrentProvider(want int) *concurrentProvider {
	return &concurrentProvider{want: want, released: make(chan struct{})}
}

func (p *concurrentProvider) Stream(ctx context.Context, req provider.Request) (<-chan provider.StreamEvent, error) {
	p.mu.Lock()
	p.requests = append(p.requests, req)
	p.active++
	if p.active > p.maxActive {
		p.maxActive = p.active
	}
	p.arrived++
	if p.arrived == p.want {
		close(p.released)
	}
	p.mu.Unlock()

	select {
	case <-ctx.Done():
		p.finish()
		return nil, ctx.Err()
	case <-p.released:
	}
	ch := make(chan provider.StreamEvent, 2)
	ch <- provider.StreamEvent{Type: provider.Delta, Text: "done " + req.RunID}
	ch <- provider.StreamEvent{Type: provider.Done, Reason: "stop"}
	close(ch)
	p.finish()
	return ch, nil
}

func (p *concurrentProvider) finish() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.active--
}

func (p *concurrentProvider) MaxConcurrent() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.maxActive
}

func providerRequestsForRun(records []cache.ProviderRequestRecord, runID string) []cache.ProviderRequestRecord {
	out := make([]cache.ProviderRequestRecord, 0, len(records))
	for _, record := range records {
		if record.RunID == runID {
			out = append(out, record)
		}
	}
	return out
}

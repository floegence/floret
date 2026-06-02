package agentharness

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/compaction"
	"github.com/floegence/floret/engine"
	"github.com/floegence/floret/event"
	scriptharness "github.com/floegence/floret/harness"
	"github.com/floegence/floret/promptcache"
	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/session"
	"github.com/floegence/floret/sessiontree"
	"github.com/floegence/floret/tools"
)

func TestThreadRunPersistsTurnEntriesAndContext(t *testing.T) {
	ctx := context.Background()
	p := scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("done text"), scriptharness.Done()))
	h := newTestHarness(p, sessiontree.NewMemoryRepo(), promptcache.NewMemoryStore())
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
	snap, err := thread.Read(ctx)
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

func TestThreadRunStopHookContinuationIsPersistedAndMetadataStaysOutOfPrompt(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	promptStore := promptcache.NewMemoryStore()
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
	snap, err := thread.Read(ctx)
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
	promptStore := promptcache.NewMemoryStore()
	p := scriptharness.NewScriptedProvider(
		scriptharness.Step(scriptharness.Text("draft"), scriptharness.Done()),
		scriptharness.Step(scriptharness.Tool("read-1", "read", "{}"), scriptharness.DoneReason("tool_calls")),
		scriptharness.Step(scriptharness.Text("final"), scriptharness.Done()),
	)
	h := newTestHarness(p, repo, promptStore)
	mustRegister(h.options.Tools, tools.Tool{Name: "read", Handler: func(context.Context, string) (string, error) {
		return "read result", nil
	}})
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
	snap, err := thread.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := []session.Message{
		{Role: session.User, Content: "do it"},
		{Role: session.Assistant, Content: "draft"},
		{Role: session.User, Content: "Please inspect with a tool."},
		{Role: session.Assistant, Content: "tool_call", ToolCallID: "read-1", ToolName: "read", ToolArgs: "{}"},
		{Role: session.Tool, Content: "read result", ToolCallID: "read-1", ToolName: "read"},
		{Role: session.Assistant, Content: "final"},
	}
	if !messagePrefixEqual(snap.Context, want) || len(snap.Context) != len(want) {
		t.Fatalf("session context order = %#v", snap.Context)
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

func TestRetryDoesNotDuplicateUserMessageAndKeepsPrefixStable(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	promptStore := promptcache.NewMemoryStore()
	failing := scriptharness.NewScriptedProvider(
		scriptharness.Step(scriptharness.Text("partial before failure"), scriptharness.Tool("missing-1", "missing", "{}")),
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
	failedRequests, err := promptStore.ProviderRequests(ctx, "turn-fail")
	if err != nil {
		t.Fatal(err)
	}
	if len(failedRequests) != 2 {
		t.Fatalf("failed request records = %#v", failedRequests)
	}
	failedSnap, _ := thread.Read(ctx)
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
	snap, _ := thread.Read(ctx)
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
	retryRequests, err := promptStore.ProviderRequests(ctx, result.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(retryRequests) != 1 {
		t.Fatalf("retry request records = %#v", retryRequests)
	}
	if failedRequests[1].PrefixRawHash != retryRequests[0].PrefixRawHash {
		t.Fatalf("retry should resume from last stable save point: failed=%#v retry=%#v", failedRequests[1], retryRequests[0])
	}
	if retryProvider.Requests[0].RawPlan.ReusedSegments == 0 {
		t.Fatalf("retry should reuse immutable raw segments: %#v", retryProvider.Requests[0].RawPlan)
	}
	if !slices.ContainsFunc(retryProvider.Requests[0].RawPlan.Segments, func(seg promptcache.Segment) bool {
		return seg.Kind == promptcache.SegmentUserMessage && seg.EntryID != ""
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
	promptStore := promptcache.NewMemoryStore()
	p := scriptharness.NewScriptedProvider(
		scriptharness.Step(scriptharness.Tool("read-1", "read", "{}")),
		scriptharness.Step(scriptharness.Hang()),
	)
	registry := tools.NewRegistry()
	readCalls := 0
	mustRegister(registry, tools.Tool{Name: "ask_user", Handler: func(context.Context, string) (string, error) { return "", nil }})
	mustRegister(registry, tools.Tool{Name: "read", Handler: func(context.Context, string) (string, error) {
		readCalls++
		return "read result", nil
	}})
	h := New(Options{
		Provider:      p,
		ProviderName:  "fake",
		Model:         "fake-model",
		SystemPrompt:  "You are Floret.",
		Tools:         registry,
		Repo:          repo,
		PromptStore:   promptStore,
		EngineOptions: engine.Options{MaxEmptyProviderRetries: 1, NoProgressLimit: 2, DuplicateToolLimit: 3},
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
		snap, _ := thread.Read(ctx)
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
	promptStore := promptcache.NewMemoryStore()
	sourceProvider := scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("source done"), scriptharness.Done()))
	h := newTestHarness(sourceProvider, repo, promptStore)
	source, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "source"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Run(ctx, "first", RunOptions{TurnID: "turn-source"}); err != nil {
		t.Fatal(err)
	}
	sourceSnap, _ := source.Read(ctx)
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
	sourceAfter, _ := source.Read(ctx)
	forkAfter, _ := fork.Read(ctx)
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
	forkSnap, _ := fork.Read(ctx)
	if forkSnap.Meta.ForkedFromThreadID != "source" || forkSnap.Meta.ForkedFromEntryID != userEntry.ID {
		t.Fatalf("fork metadata = %#v", forkSnap.Meta)
	}
}

func TestMoveToBranchSummaryEntersActiveContext(t *testing.T) {
	ctx := context.Background()
	h := newTestHarness(scriptharness.NewScriptedProvider(), sessiontree.NewMemoryRepo(), promptcache.NewMemoryStore())
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
	snap, err := thread.Read(ctx)
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
	h := newTestHarness(p, repo, promptcache.NewMemoryStore())
	h.options.ContextPolicy.ContextWindowTokens = 8000
	h.options.ContextPolicy.ReservedOutputTokens = 512
	h.options.ContextPolicy.ReservedSummaryTokens = 512
	h.options.ContextPolicy.RecentTailTokens = 256
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
	snap, err := thread.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	compaction := firstEntry(snap.Entries, sessiontree.EntryCompaction)
	if compaction.ID == "" || compaction.Summary == "" || compaction.FirstKeptEntryID == "" {
		t.Fatalf("compaction entry missing details: %#v", snap.Entries)
	}
	if !slices.ContainsFunc(snap.Context, func(msg session.Message) bool {
		return msg.Role == session.Assistant && msg.Kind == session.MessageKindCompactionSummary
	}) {
		t.Fatalf("compaction summary should be provider-visible: %#v", snap.Context)
	}
}

func TestMultipleCompactionsForkReloadAndContinueUseLatestWindow(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	promptRoot := t.TempDir()
	repo := sessiontree.NewFileRepo(root)
	promptStore := promptcache.NewFileStore(promptRoot)
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
	h.options.ContextPolicy.ContextWindowTokens = 12000
	h.options.ContextPolicy.ReservedOutputTokens = 512
	h.options.ContextPolicy.ReservedSummaryTokens = 512
	h.options.ContextPolicy.RecentTailTokens = 512
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
	snap, err := thread.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if countEntries(snap.Entries, sessiontree.EntryCompaction) != 2 {
		t.Fatalf("expected two durable compactions: %#v", snap.Entries)
	}
	if got := countMessagesByKind(snap.Context, session.MessageKindCompactionSummary); got != 1 {
		t.Fatalf("active context should expose only latest compaction summary, got %d: %#v", got, snap.Context)
	}
	latest := latestEntry(snap.Entries, sessiontree.EntryCompaction)
	if latest.CompactionGeneration != 2 || latest.PreviousCompactionID == "" {
		t.Fatalf("second compaction should link previous generation: %#v", latest)
	}

	reloadedHarness := newTestHarness(scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Text("fork done"), scriptharness.Done())), sessiontree.NewFileRepo(root), promptcache.NewFileStore(promptRoot))
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
	promptStore := promptcache.NewFileStore(promptRoot)
	firstProvider := scriptharness.NewScriptedProvider(scriptharness.Step(scriptharness.Tool("ask", "ask_user", "more?")))
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
	resumedHarness := newTestHarness(secondProvider, sessiontree.NewFileRepo(root), promptcache.NewFileStore(promptRoot))
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
		{Role: session.System, Content: "You are Floret."},
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
	resumedSnap, _ := resumed.Read(ctx)
	if !hasEntry(resumedSnap.Entries, sessiontree.EntryTurnMarker, sessiontree.TurnWaiting) ||
		!hasEntry(resumedSnap.Entries, sessiontree.EntryTurnMarker, sessiontree.TurnCompleted) {
		t.Fatalf("resume should preserve waiting and completed markers: %#v", resumedSnap.Entries)
	}
}

func TestResumeMarksUnfinishedTurnInterrupted(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	h := newTestHarness(scriptharness.NewScriptedProvider(), repo, promptcache.NewMemoryStore())
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
	snap, err := resumed.Read(ctx)
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

func TestActiveTurnBusyGuard(t *testing.T) {
	ctx := context.Background()
	p := newBlockingProvider()
	h := newTestHarness(p, sessiontree.NewMemoryRepo(), promptcache.NewMemoryStore())
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
	cancel()
	<-done
}

func newTestHarness(p provider.Provider, repo sessiontree.Repo, promptStore promptcache.Store) *AgentHarness {
	rec := &event.Recorder{}
	registry := tools.NewRegistry()
	mustRegister(registry, tools.Tool{Name: "ask_user", Handler: func(context.Context, string) (string, error) { return "", nil }})
	return New(Options{
		Provider:     p,
		ProviderName: "fake",
		Model:        "fake-model",
		SystemPrompt: "You are Floret.",
		Tools:        registry,
		Repo:         repo,
		PromptStore:  promptStore,
		Sink:         rec,
		EngineOptions: engine.Options{
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

func userContents(messages []session.Message) []string {
	var out []string
	for _, msg := range messages {
		if msg.Role == session.User {
			out = append(out, msg.Content)
		}
	}
	return out
}

func countUserMessagesInSnapshot(snap ThreadSnapshot, content string) int {
	count := 0
	for _, entry := range snap.Entries {
		if entry.Type == sessiontree.EntryUserMessage && entry.Message.Content == content {
			count++
		}
	}
	return count
}

func segmentRaws(segments []promptcache.Segment) []string {
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

type blockingProvider struct {
	started chan struct{}
	once    sync.Once
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

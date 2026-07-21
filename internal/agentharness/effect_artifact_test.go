package agentharness

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/internal/engine"
	"github.com/floegence/floret/internal/event"
	"github.com/floegence/floret/internal/provider/cache"
	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/session/artifact"
	"github.com/floegence/floret/internal/sessiontree"
	"github.com/floegence/floret/internal/storage/sqlite"
	"github.com/floegence/floret/internal/testing/harness"
	"github.com/floegence/floret/observation"
	"github.com/floegence/floret/tools"
)

func TestEffectOutcomeFingerprintIncludesFullOutputSpec(t *testing.T) {
	message := session.Message{
		Role: session.Tool, Content: "visible", ToolCallID: "call-1", ToolName: "shell",
		ToolResult: &session.ToolResultView{Status: string(observation.ActivityStatusSuccess), Truncated: true},
	}
	result := tools.Result{CallID: "call-1", Name: "shell", Text: "full output"}
	base, err := effectOutcomeFingerprint(result, message, nil)
	if err != nil {
		t.Fatal(err)
	}
	full := &artifact.FullOutput{Text: "full output", Kind: artifact.DefaultKind, MIME: artifact.DefaultMIME}
	withFull, err := effectOutcomeFingerprint(result, message, full)
	if err != nil {
		t.Fatal(err)
	}
	changedText, err := effectOutcomeFingerprint(result, message, &artifact.FullOutput{Text: "other", Kind: full.Kind, MIME: full.MIME})
	if err != nil {
		t.Fatal(err)
	}
	changedKind, err := effectOutcomeFingerprint(result, message, &artifact.FullOutput{Text: full.Text, Kind: "report", MIME: full.MIME})
	if err != nil {
		t.Fatal(err)
	}
	if base == withFull || withFull == changedText || withFull == changedKind {
		t.Fatalf("fingerprints did not bind full output: base=%q full=%q text=%q kind=%q", base, withFull, changedText, changedKind)
	}
}

func TestValidateCommittedEffectFinalizationRejectsArtifactMetadataMismatch(t *testing.T) {
	full := &artifact.FullOutput{Text: "0123456789abcdef", Kind: artifact.DefaultKind, MIME: artifact.DefaultMIME}
	message := session.Message{
		Role: session.Tool, Content: "89abcdef", ToolCallID: "call-1", ToolName: "shell",
		ToolResult: &session.ToolResultView{
			Status: string(observation.ActivityStatusSuccess), Truncated: true,
			OriginalBytes: 16, VisibleBytes: 8, Strategy: string(tools.OutputTail), ContentSHA256: artifact.TextSHA256(full.Text),
		},
	}
	req := engine.EffectResultFinalizationRequest{
		RunID: "run-1", ThreadID: "thread", TurnID: "turn-1", ToolCallID: "call-1",
		Message: session.CloneMessage(message), FullOutput: full,
	}
	prepared := sessiontree.EffectAttempt{
		EffectAttemptID: "effect-1",
		Invocation: sessiontree.EffectInvocationIdentity{
			ThreadID: "thread", TurnID: "turn-1", RunID: "run-1", ToolCallID: "call-1", ToolName: "shell",
		},
	}
	ref, err := artifact.RefForEffect(prepared.EffectAttemptID, prepared.Invocation.ToolName, *full)
	if err != nil {
		t.Fatal(err)
	}
	committed := session.CloneMessage(message)
	committed.ToolResult.FullOutput = &ref
	finished := sessiontree.FinishEffectDispatchResult{
		Attempt: sessiontree.EffectAttempt{EffectAttemptID: prepared.EffectAttemptID, ResultEntryID: "entry-1"},
		Result: sessiontree.Entry{
			ID: "entry-1", ThreadID: "thread", TurnID: "turn-1", Type: sessiontree.EntryToolResult,
			Message: committed, Metadata: map[string]string{sessiontree.PendingToolEffectAttemptIDKey: prepared.EffectAttemptID},
		},
		Artifact: &ref,
	}
	if _, err := validateCommittedEffectFinalization(req, prepared, finished); err != nil {
		t.Fatalf("valid committed finalization rejected: %v", err)
	}
	badRef := ref
	badRef.MIME = "application/octet-stream"
	finished.Artifact = &badRef
	if _, err := validateCommittedEffectFinalization(req, prepared, finished); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("metadata mismatch err=%v, want ErrAuthorityCorrupt", err)
	}
}

func TestEffectArtifactPersistenceFailureLeavesNoArtifact(t *testing.T) {
	ctx := context.Background()
	repo := &failingEffectFinishRepo{MemoryRepo: sessiontree.NewMemoryRepo(), err: errors.New("persist failed")}
	p := harness.NewScriptedProvider(
		harness.Step(harness.Tool("call-1", "shell", `{"value":"x"}`), harness.DoneReason("tool_calls")),
	)
	h := newTestHarness(p, repo, cache.NewMemoryStore())
	handlerCalls := 0
	mustRegister(h.options.Tools, tools.Define[stringArgs](
		tools.Definition{
			Name: "shell", InputSchema: tools.StrictObject(map[string]any{"value": tools.String("value")}, []string{"value"}),
			ReadOnly: true, Permission: tools.PermissionSpec{Mode: tools.PermissionAllow},
			OutputPolicy: tools.OutputPolicy{VisibleMaxBytes: 8, Strategy: tools.OutputTail, PreserveFull: true},
		},
		nil,
		nil,
		func(context.Context, tools.Invocation[stringArgs]) (tools.Result, error) {
			handlerCalls++
			return tools.Result{Text: "0123456789abcdef"}, nil
		},
	))
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	_, runErr := thread.Run(ctx, "run", RunOptions{RunID: "run-1", TurnID: "turn-1"})
	var committed *CommittedEffectError
	if !errors.As(runErr, &committed) || !errors.Is(runErr, repo.err) {
		t.Fatalf("run err=%v, want committed persistence error", runErr)
	}
	if handlerCalls != 1 {
		t.Fatalf("handler calls=%d, want 1", handlerCalls)
	}
	ref, err := artifact.RefForEffect("effect-1", "shell", artifact.FullOutput{Text: "0123456789abcdef"})
	if err != nil {
		t.Fatal(err)
	}
	if content, err := repo.ReadArtifact(ctx, sessiontree.ArtifactReadRequest{ThreadID: "thread", ArtifactID: ref.ID}); !errors.Is(err, sessiontree.ErrArtifactNotFound) || content != (sessiontree.ArtifactContent{}) {
		t.Fatalf("artifact content=%#v err=%v, want zero not found", content, err)
	}
	journal, err := thread.Journal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range journal.Entries {
		if entry.Type == sessiontree.EntryToolResult && entry.Message.ToolCallID == "call-1" &&
			(entry.Message.ToolResult == nil || entry.Message.ToolResult.FullOutput != nil || entry.Message.Content == "89abcdef") {
			t.Fatalf("failed atomic admission left admitted full-output result: %#v", entry)
		}
	}
}

func TestReplayEffectResultReturnsCommittedMessageWithoutRedispatch(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	h := newTestHarness(harness.NewScriptedProvider(), repo, cache.NewMemoryStore())
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	entry, err := repo.Append(ctx, sessiontree.Entry{
		ThreadID: "thread", TurnID: "turn-1", Type: sessiontree.EntryToolResult,
		Message: session.Message{
			Role: session.Tool, Content: "visible", ToolCallID: "call-1", ToolName: "shell",
			ToolResult: &session.ToolResultView{Status: string(observation.ActivityStatusSuccess)},
		},
		Metadata: map[string]string{sessiontree.PendingToolEffectAttemptIDKey: "effect-1"},
	}, sessiontree.AppendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	attempt := sessiontree.EffectAttempt{
		EffectAttemptID: "effect-1", State: sessiontree.EffectAttemptCompleted, ResultEntryID: entry.ID,
		Invocation: sessiontree.EffectInvocationIdentity{
			ThreadID: "thread", TurnID: "turn-1", RunID: "run-1", ToolCallID: "call-1", ToolName: "shell",
		},
	}
	result := thread.replayEffectResult(ctx, attempt)
	if result.DispatchErr != nil || result.Text != "visible" {
		t.Fatalf("replay result=%#v", result)
	}
	finalized, err := thread.finalizeEffectResult(ctx, engine.EffectResultFinalizationRequest{
		RunID: "run-1", ThreadID: "thread", TurnID: "turn-1", ToolCallID: "call-1",
		Message: session.Message{Role: session.Tool, Content: "visible", ToolCallID: "call-1", ToolName: "shell", ToolResult: &session.ToolResultView{}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !finalized.Handled || !finalized.Replayed || finalized.Message.Content != "visible" {
		t.Fatalf("finalized=%#v", finalized)
	}
	second, err := thread.finalizeEffectResult(ctx, engine.EffectResultFinalizationRequest{
		RunID: "run-1", ThreadID: "thread", TurnID: "turn-1", ToolCallID: "call-1",
	})
	if !errors.Is(err, ErrEffectDispatchConsumed) || second.Handled {
		t.Fatalf("second finalization=%#v err=%v, want explicit one-shot rejection", second, err)
	}
}

func TestCallerCancellationAfterHandlerStartsStillFinishesEffectDispatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	repo := &countingEffectFinishRepo{MemoryRepo: sessiontree.NewMemoryRepo()}
	p := harness.NewScriptedProvider(
		harness.Step(harness.Tool("call-1", "shell", `{"value":"x"}`), harness.DoneReason("tool_calls")),
	)
	h := newTestHarness(p, repo, cache.NewMemoryStore())
	mustRegister(h.options.Tools, tools.Define[stringArgs](
		tools.Definition{
			Name: "shell", InputSchema: tools.StrictObject(map[string]any{"value": tools.String("value")}, []string{"value"}),
			ReadOnly: true, Permission: tools.PermissionSpec{Mode: tools.PermissionAllow},
		},
		nil,
		nil,
		func(context.Context, tools.Invocation[stringArgs]) (tools.Result, error) {
			cancel()
			return tools.Result{Text: "committed output"}, nil
		},
	))
	thread, err := h.StartThread(context.Background(), StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = thread.Run(ctx, "run", RunOptions{RunID: "run-1", TurnID: "turn-1"})
	if got := repo.finishCalls.Load(); got != 1 {
		t.Fatalf("FinishEffectDispatch calls=%d, want 1", got)
	}
	journal, err := thread.Journal(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range journal.Entries {
		if entry.Type == sessiontree.EntryToolResult && entry.Message.ToolCallID == "call-1" && entry.Message.Content == "committed output" {
			return
		}
	}
	t.Fatalf("cancelled caller bypassed durable effect result: %#v", journal.Entries)
}

func TestCallerCancellationWaitsForStartedHandlerBeyondFinalizationWindow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	repo := &countingEffectFinishRepo{MemoryRepo: sessiontree.NewMemoryRepo()}
	p := harness.NewScriptedProvider(
		harness.Step(harness.Tool("call-1", "shell", `{"value":"x"}`), harness.DoneReason("tool_calls")),
	)
	h := newTestHarness(p, repo, cache.NewMemoryStore())
	const finalizationWindow = 20 * time.Millisecond
	h.effectFinalizationTimeout = finalizationWindow
	started := make(chan struct{})
	release := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	mustRegister(h.options.Tools, tools.Define[stringArgs](
		tools.Definition{
			Name: "shell", InputSchema: tools.StrictObject(map[string]any{"value": tools.String("value")}, []string{"value"}),
			Permission: tools.PermissionSpec{Mode: tools.PermissionAllow},
		},
		nil,
		nil,
		func(context.Context, tools.Invocation[stringArgs]) (tools.Result, error) {
			close(started)
			<-release
			return tools.Result{Text: "late committed output"}, nil
		},
	))
	thread, err := h.StartThread(context.Background(), StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct {
		result TurnResult
		err    error
	}, 1)
	go func() {
		result, err := thread.Run(ctx, "run", RunOptions{RunID: "run-1", TurnID: "turn-1"})
		done <- struct {
			result TurnResult
			err    error
		}{result: result, err: err}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("effect handler did not start")
	}
	cancel()
	select {
	case out := <-done:
		t.Fatalf("run finished before started handler returned: result=%#v err=%v", out.result, out.err)
	case <-time.After(3 * finalizationWindow):
	}
	close(release)
	released = true
	select {
	case out := <-done:
		if out.err == nil || out.result.Status != engine.Cancelled {
			t.Fatalf("run result=%#v err=%v, want caller cancellation after canonical finalization", out.result, out.err)
		}
	case <-time.After(time.Second):
		t.Fatal("run did not finish after handler returned")
	}
	if got := repo.finishCalls.Load(); got != 1 {
		t.Fatalf("FinishEffectDispatch calls=%d, want 1", got)
	}
	journal, err := thread.Journal(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	toolResults := 0
	terminalMarkers := 0
	for _, entry := range journal.Path {
		if entry.Type == sessiontree.EntryToolResult && entry.Message.ToolCallID == "call-1" && entry.Message.Content == "late committed output" {
			toolResults++
		}
		if entry.Type == sessiontree.EntryTurnMarker && entry.TurnID == "turn-1" && isTerminalTurnMarker(entry.TurnStatus) {
			terminalMarkers++
		}
	}
	if toolResults != 1 || terminalMarkers != 1 {
		t.Fatalf("canonical convergence tool_results=%d terminal_markers=%d path=%#v", toolResults, terminalMarkers, journal.Path)
	}
}

func TestCallerCancellationLinearizesWithCommittedEffectDispatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	repo := &beginEffectCommitBarrierRepo{
		MemoryRepo: sessiontree.NewMemoryRepo(),
		committed:  make(chan struct{}),
		release:    make(chan struct{}),
	}
	p := harness.NewScriptedProvider(
		harness.Step(harness.Tool("call-1", "shell", `{"value":"x"}`), harness.DoneReason("tool_calls")),
	)
	h := newTestHarness(p, repo, cache.NewMemoryStore())
	var handlerCalls atomic.Int64
	mustRegister(h.options.Tools, stringTool("shell", func(context.Context, string) (string, error) {
		handlerCalls.Add(1)
		return "committed output", nil
	}))
	thread, err := h.StartThread(context.Background(), StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct {
		result TurnResult
		err    error
	}, 1)
	go func() {
		result, err := thread.Run(ctx, "run", RunOptions{RunID: "run-1", TurnID: "turn-1"})
		done <- struct {
			result TurnResult
			err    error
		}{result: result, err: err}
	}()
	select {
	case <-repo.committed:
	case <-time.After(time.Second):
		close(repo.release)
		t.Fatal("effect dispatch did not reach committed barrier")
	}
	cancel()
	select {
	case out := <-done:
		close(repo.release)
		t.Fatalf("run escaped committed dispatch before handler result: result=%#v err=%v", out.result, out.err)
	case <-time.After(50 * time.Millisecond):
	}
	close(repo.release)
	select {
	case out := <-done:
		if !errors.Is(out.err, context.Canceled) || out.result.Status != engine.Cancelled {
			t.Fatalf("run result=%#v err=%v, want cancelled after canonical effect finalization", out.result, out.err)
		}
	case <-time.After(time.Second):
		t.Fatal("run did not finish after committed dispatch returned")
	}
	if handlerCalls.Load() != 1 || repo.finishCalls.Load() != 1 {
		t.Fatalf("calls handler=%d finish=%d, want 1/1", handlerCalls.Load(), repo.finishCalls.Load())
	}
	journal, err := thread.Journal(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	toolResults := 0
	terminalMarkers := 0
	for _, entry := range journal.Path {
		if entry.Type == sessiontree.EntryToolResult && entry.Message.ToolCallID == "call-1" && entry.Message.Content == "committed output" {
			toolResults++
		}
		if entry.Type == sessiontree.EntryTurnMarker && entry.TurnID == "turn-1" && isTerminalTurnMarker(entry.TurnStatus) {
			terminalMarkers++
		}
	}
	if toolResults != 1 || terminalMarkers != 1 {
		t.Fatalf("canonical convergence tool_results=%d terminal_markers=%d path=%#v", toolResults, terminalMarkers, journal.Path)
	}
}

func TestCallerCancellationKeepsEffectLeaseAliveUntilLateHandlerFinalization(t *testing.T) {
	const ttl = 90 * time.Millisecond
	policy := sessiontree.LeasePolicy{TTL: ttl, RenewInterval: 20 * time.Millisecond, ClockSkewAllowance: 10 * time.Millisecond}
	memory, err := sessiontree.NewMemoryRepoWithLeasePolicy(policy, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	repo := &countingEffectFinishRepo{MemoryRepo: memory}
	ctx, cancel := context.WithCancel(context.Background())
	p := harness.NewScriptedProvider(
		harness.Step(harness.Tool("call-1", "shell", `{"value":"x"}`), harness.DoneReason("tool_calls")),
	)
	h := newTestHarness(p, repo, cache.NewMemoryStore())
	started := make(chan struct{})
	release := make(chan struct{})
	mustRegister(h.options.Tools, stringTool("shell", func(context.Context, string) (string, error) {
		close(started)
		<-release
		return "late output", nil
	}))
	thread, err := h.StartThread(context.Background(), StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct {
		result TurnResult
		err    error
	}, 1)
	go func() {
		result, err := thread.Run(ctx, "run", RunOptions{RunID: "run-1", TurnID: "turn-1"})
		done <- struct {
			result TurnResult
			err    error
		}{result: result, err: err}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("effect handler did not start")
	}
	cancel()
	time.Sleep(2 * ttl)
	select {
	case out := <-done:
		close(release)
		t.Fatalf("run finished before late handler release: result=%#v err=%v", out.result, out.err)
	default:
	}
	close(release)
	select {
	case out := <-done:
		if !errors.Is(out.err, context.Canceled) || out.result.Status != engine.Cancelled {
			t.Fatalf("run result=%#v err=%v, want cancelled after late canonical effect finalization", out.result, out.err)
		}
	case <-time.After(time.Second):
		t.Fatal("run did not finish after late handler release")
	}
	if repo.finishCalls.Load() != 1 {
		t.Fatalf("FinishEffectDispatch calls=%d, want 1", repo.finishCalls.Load())
	}
	journal, err := thread.Journal(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := countTurnMarkersForTurn(journal.Path, "turn-1", sessiontree.TurnAborted); got != 1 {
		t.Fatalf("late effect finalization terminal markers=%d, want 1: %#v", got, journal.Path)
	}
	for _, entry := range journal.Path {
		if entry.Type == sessiontree.EntryToolResult && entry.Message.ToolCallID == "call-1" && entry.Message.Content == "late output" {
			return
		}
	}
	t.Fatalf("late effect result was not committed: %#v", journal.Path)
}

func TestParallelEffectFinalizationWindowStartsAtOrderedFinalization(t *testing.T) {
	ctx := context.Background()
	repo := &countingEffectFinishRepo{MemoryRepo: sessiontree.NewMemoryRepo()}
	p := harness.NewScriptedProvider(
		harness.Step(
			harness.Tool("call-0", "tool_0", `{"value":"zero"}`),
			harness.Tool("call-1", "tool_1", `{"value":"one"}`),
			harness.Tool("call-2", "tool_2", `{"value":"two"}`),
			harness.DoneReason("tool_calls"),
		),
		harness.Step(harness.Text("done"), harness.Done()),
	)
	h := newTestHarness(p, repo, cache.NewMemoryStore())
	const finalizationWindow = 20 * time.Millisecond
	h.effectFinalizationTimeout = finalizationWindow

	releaseSlow := make(chan struct{})
	fastReturned := make(chan struct{})
	allStarted := make(chan struct{})
	var started atomic.Int64
	register := func(name string, wait bool) {
		mustRegister(h.options.Tools, tools.Define[stringArgs](
			tools.Definition{
				Name: name, InputSchema: tools.StrictObject(map[string]any{"value": tools.String("value")}, []string{"value"}),
				Permission: tools.PermissionSpec{Mode: tools.PermissionAllow},
			},
			nil,
			nil,
			func(ctx context.Context, invocation tools.Invocation[stringArgs]) (tools.Result, error) {
				if started.Add(1) == 3 {
					close(allStarted)
				}
				if wait {
					select {
					case <-releaseSlow:
					case <-ctx.Done():
						return tools.Result{}, ctx.Err()
					}
				} else {
					close(fastReturned)
				}
				return tools.Result{Text: invocation.Args.Value}, nil
			},
		))
	}
	register("tool_0", true)
	register("tool_1", false)
	register("tool_2", true)

	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct {
		result TurnResult
		err    error
	}, 1)
	go func() {
		result, err := thread.Run(ctx, "run", RunOptions{RunID: "run-1", TurnID: "turn-1"})
		done <- struct {
			result TurnResult
			err    error
		}{result: result, err: err}
	}()

	select {
	case <-allStarted:
	case <-time.After(time.Second):
		t.Fatal("parallel effect handlers did not all start")
	}
	select {
	case <-fastReturned:
	case <-time.After(time.Second):
		t.Fatal("fast effect handler did not return")
	}
	// The fast handler is deliberately held behind the ordered batch observer.
	// Its persistence budget must not start while the Engine is still waiting for
	// the earlier/later handler results.
	time.Sleep(3 * finalizationWindow)
	close(releaseSlow)

	select {
	case out := <-done:
		if out.err != nil || out.result.Status != engine.Completed {
			t.Fatalf("parallel run result=%#v err=%v", out.result, out.err)
		}
	case <-time.After(time.Second):
		t.Fatal("parallel run did not finish")
	}
	if got := repo.finishCalls.Load(); got != 3 {
		t.Fatalf("FinishEffectDispatch calls=%d, want one per handler", got)
	}
	if got := started.Load(); got != 3 {
		t.Fatalf("handler calls=%d, want 3", got)
	}
	journal, err := thread.Journal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, callID := range []string{"call-0", "call-1", "call-2"} {
		found := false
		for _, entry := range journal.Entries {
			if entry.Type == sessiontree.EntryToolResult && entry.Message.ToolCallID == callID {
				found = true
				if entry.Message.ToolResult == nil || entry.Message.ToolResult.Status != string(observation.ActivityStatusSuccess) {
					t.Fatalf("tool result %s=%#v, want success", callID, entry.Message)
				}
			}
		}
		if !found {
			t.Fatalf("journal missing canonical result for %s: %#v", callID, journal.Entries)
		}
	}
}

func TestFinishEffectFailureMarksUnknownWithFreshContext(t *testing.T) {
	finishErr := errors.New("finish persistence failed")
	repo := &effectFinalizationFailureRepo{
		MemoryRepo:   sessiontree.NewMemoryRepo(),
		finishErr:    finishErr,
		expireFinish: true,
	}
	p := harness.NewScriptedProvider(
		harness.Step(harness.Tool("call-1", "shell", `{"value":"x"}`), harness.DoneReason("tool_calls")),
	)
	h := newTestHarness(p, repo, cache.NewMemoryStore())
	h.effectFinalizationTimeout = 20 * time.Millisecond
	var handlerCalls atomic.Int64
	mustRegister(h.options.Tools, stringTool("shell", func(context.Context, string) (string, error) {
		handlerCalls.Add(1)
		return "known side effect", nil
	}))
	thread, err := h.StartThread(context.Background(), StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	result, runErr := thread.Run(context.Background(), "run", RunOptions{RunID: "run-1", TurnID: "turn-1"})
	var committed *CommittedEffectError
	if !errors.As(runErr, &committed) || !errors.Is(runErr, finishErr) {
		t.Fatalf("run err=%v, want committed finish failure", runErr)
	}
	if handlerCalls.Load() != 1 || repo.finishCalls.Load() != 1 || repo.markCalls.Load() != 1 {
		t.Fatalf("calls handler=%d finish=%d mark=%d, want 1/1/1", handlerCalls.Load(), repo.finishCalls.Load(), repo.markCalls.Load())
	}
	if got := repo.markContextErr.Load(); got != nil {
		t.Fatalf("MarkEffectUnknown inherited exhausted finish context: %v", got)
	}
	if got := repo.markedState.Load(); got != string(sessiontree.EffectAttemptUnknown) {
		t.Fatalf("marked state=%v, want unknown", got)
	}
	journal, err := thread.Journal(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	terminalMarkers := 0
	for _, entry := range journal.Path {
		if entry.Type == sessiontree.EntryTurnMarker && entry.TurnID == "turn-1" && isTerminalTurnMarker(entry.TurnStatus) {
			terminalMarkers++
		}
	}
	if terminalMarkers != 1 {
		t.Fatalf("unknown effect did not permit exactly one terminal marker: %#v", journal.Path)
	}
	if !errors.Is(runErr, sessiontree.ErrEffectOutcomeUnknown) {
		t.Fatalf("run err=%v, want effect outcome unknown classification", runErr)
	}
	if result.Status != engine.Failed || result.FailureCode != sessiontree.TurnFailureEffectOutcomeUnknown {
		t.Fatalf("run result=%#v, want failed effect_outcome_unknown", result)
	}
	page, err := h.ListCanonicalTurnDetailEvents(context.Background(), sessiontree.ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 1}, false)
	if err != nil {
		t.Fatal(err)
	}
	if page.LatestStatus != string(engine.Failed) || len(page.Turns) != 1 {
		t.Fatalf("canonical page=%#v, want one failed turn", page)
	}
	foundFailureCode := false
	for _, detailEvent := range page.Turns[0].Events {
		if detailEvent.TurnMarker != nil && detailEvent.TurnMarker.Metadata[sessiontree.TurnFailureCodeMetadataKey] == sessiontree.TurnFailureEffectOutcomeUnknown {
			foundFailureCode = true
		}
	}
	if !foundFailureCode {
		t.Fatalf("canonical page omitted effect_outcome_unknown: %#v", page.Turns[0].Events)
	}
	replayed, replayErr := thread.Run(context.Background(), "run", RunOptions{RunID: "run-1", TurnID: "turn-1"})
	if replayErr == nil || replayed.Status != engine.Failed || replayed.FailureCode != sessiontree.TurnFailureEffectOutcomeUnknown {
		t.Fatalf("replay result=%#v err=%v, want failed effect_outcome_unknown", replayed, replayErr)
	}
	if handlerCalls.Load() != 1 || repo.finishCalls.Load() != 1 || repo.markCalls.Load() != 1 {
		t.Fatalf("replay calls handler/finish/mark=%d/%d/%d, want unchanged 1/1/1", handlerCalls.Load(), repo.finishCalls.Load(), repo.markCalls.Load())
	}
}

func TestEffectOutcomeFingerprintFailureConvergesUnknownWithoutReplay(t *testing.T) {
	tests := []struct {
		name string
		open func(*testing.T) (sessiontree.Repo, *effectUnknownObserver, func())
	}{
		{
			name: "memory",
			open: func(*testing.T) (sessiontree.Repo, *effectUnknownObserver, func()) {
				observer := &effectUnknownObserver{}
				return &effectOutcomeFingerprintMemoryRepo{MemoryRepo: sessiontree.NewMemoryRepo(), observer: observer}, observer, func() {}
			},
		},
		{
			name: "sqlite",
			open: func(t *testing.T) (sessiontree.Repo, *effectUnknownObserver, func()) {
				store, err := sqlite.Open(filepath.Join(t.TempDir(), "effect-fingerprint.db"))
				if err != nil {
					t.Fatal(err)
				}
				observer := &effectUnknownObserver{}
				return &effectOutcomeFingerprintSQLiteRepo{Store: store, observer: observer}, observer, func() { _ = store.Close() }
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			repo, observer, closeRepo := test.open(t)
			defer closeRepo()
			provider := harness.NewScriptedProvider(
				harness.Step(harness.Tool("call-1", "shell", `{"value":"x"}`), harness.DoneReason("tool_calls")),
			)
			h := newTestHarness(provider, repo, cache.NewMemoryStore())
			fingerprintErr := errors.New("outcome fingerprint failed")
			h.effectOutcomeFingerprinter = func(tools.Result, session.Message, *artifact.FullOutput) (string, error) {
				return "", fingerprintErr
			}
			var handlerCalls atomic.Int64
			mustRegister(h.options.Tools, tools.Define[stringArgs](
				tools.Definition{
					Name: "shell", InputSchema: tools.StrictObject(map[string]any{"value": tools.String("value")}, []string{"value"}),
					Permission: tools.PermissionSpec{Mode: tools.PermissionAllow},
				},
				nil,
				nil,
				func(context.Context, tools.Invocation[stringArgs]) (tools.Result, error) {
					handlerCalls.Add(1)
					return tools.Result{Text: "side effect completed"}, nil
				},
			))
			thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
			if err != nil {
				t.Fatal(err)
			}

			result, runErr := thread.Run(ctx, "run", RunOptions{RunID: "run-1", TurnID: "turn-1"})
			if !errors.Is(runErr, sessiontree.ErrEffectOutcomeUnknown) || !errors.Is(runErr, fingerprintErr) || result.Status != engine.Failed || result.FailureCode != sessiontree.TurnFailureEffectOutcomeUnknown {
				t.Fatalf("run result=%#v err=%v, want failed effect_outcome_unknown", result, runErr)
			}
			if handlerCalls.Load() != 1 || observer.markCalls.Load() != 1 {
				t.Fatalf("calls handler=%d mark=%d, want 1/1", handlerCalls.Load(), observer.markCalls.Load())
			}
			if state, _ := observer.state.Load().(sessiontree.EffectAttemptState); state != sessiontree.EffectAttemptUnknown {
				t.Fatalf("effect state=%q, want unknown", state)
			}

			journal, err := thread.Journal(ctx)
			if err != nil {
				t.Fatal(err)
			}
			terminalMarkers := 0
			failureEntries := 0
			for _, entry := range journal.Path {
				if entry.TurnID != "turn-1" {
					continue
				}
				if entry.Type == sessiontree.EntryTurnMarker && isTerminalTurnMarker(entry.TurnStatus) {
					terminalMarkers++
					if entry.TurnStatus != sessiontree.TurnFailed || entry.Metadata[sessiontree.TurnFailureCodeMetadataKey] != sessiontree.TurnFailureEffectOutcomeUnknown {
						t.Fatalf("terminal=%#v, want failed effect_outcome_unknown", entry)
					}
				}
				if entry.Type == sessiontree.EntryRunFailure {
					failureEntries++
				}
			}
			if terminalMarkers != 1 || failureEntries != 1 {
				t.Fatalf("terminal markers=%d failure entries=%d, want 1/1: %#v", terminalMarkers, failureEntries, journal.Path)
			}
			leaseRepo := repo.(interface {
				ActiveTurnLease(context.Context, string) (sessiontree.TurnLease, bool, error)
			})
			if _, active, err := leaseRepo.ActiveTurnLease(ctx, "thread"); err != nil || active {
				t.Fatalf("active lease=%v err=%v, want released", active, err)
			}

			replayed, replayErr := thread.Run(ctx, "run", RunOptions{RunID: "run-1", TurnID: "turn-1"})
			if replayed.FailureCode != sessiontree.TurnFailureEffectOutcomeUnknown || replayErr == nil {
				t.Fatalf("replay result=%#v err=%v, want canonical terminal replay", replayed, replayErr)
			}
			if handlerCalls.Load() != 1 || observer.markCalls.Load() != 1 {
				t.Fatalf("replay calls handler=%d mark=%d, want unchanged 1/1", handlerCalls.Load(), observer.markCalls.Load())
			}
		})
	}
}

func TestParallelEffectFinalizersContinueAfterFirstPersistenceFailure(t *testing.T) {
	ctx := context.Background()
	repo := &firstEffectFinishFailureRepo{
		MemoryRepo: sessiontree.NewMemoryRepo(),
		finishErr:  errors.New("first effect persistence failed"),
		states:     map[string]sessiontree.EffectAttemptState{},
	}
	provider := harness.NewScriptedProvider(
		harness.Step(
			harness.Tool("call-0", "shell", `{"value":"zero"}`),
			harness.Tool("call-1", "shell", `{"value":"one"}`),
			harness.Tool("call-2", "shell", `{"value":"two"}`),
			harness.DoneReason("tool_calls"),
		),
	)
	h := newTestHarness(provider, repo, cache.NewMemoryStore())
	var handlerCalls atomic.Int64
	mustRegister(h.options.Tools, tools.Define[stringArgs](
		tools.Definition{
			Name: "shell", InputSchema: tools.StrictObject(map[string]any{"value": tools.String("value")}, []string{"value"}),
			Permission: tools.PermissionSpec{Mode: tools.PermissionAllow},
		},
		nil,
		nil,
		func(_ context.Context, invocation tools.Invocation[stringArgs]) (tools.Result, error) {
			handlerCalls.Add(1)
			return tools.Result{Text: invocation.Args.Value}, nil
		},
	))
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}

	result, runErr := thread.Run(ctx, "run", RunOptions{RunID: "run-1", TurnID: "turn-1"})
	if !errors.Is(runErr, sessiontree.ErrEffectOutcomeUnknown) || result.FailureCode != sessiontree.TurnFailureEffectOutcomeUnknown {
		t.Fatalf("run result=%#v err=%v, want effect_outcome_unknown", result, runErr)
	}
	if handlerCalls.Load() != 3 || repo.finishCalls.Load() != 3 || repo.markCalls.Load() != 1 {
		t.Fatalf("calls handler=%d finish=%d mark=%d, want 3/3/1", handlerCalls.Load(), repo.finishCalls.Load(), repo.markCalls.Load())
	}
	wantStates := map[string]sessiontree.EffectAttemptState{
		"call-0": sessiontree.EffectAttemptUnknown,
		"call-1": sessiontree.EffectAttemptCompleted,
		"call-2": sessiontree.EffectAttemptCompleted,
	}
	if got := repo.snapshotStates(); !reflect.DeepEqual(got, wantStates) {
		t.Fatalf("effect states=%v, want %v", got, wantStates)
	}
	journal, err := thread.Journal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, callID := range []string{"call-1", "call-2"} {
		if !slices.ContainsFunc(journal.Entries, func(entry sessiontree.Entry) bool {
			return entry.Type == sessiontree.EntryToolResult && entry.Message.ToolCallID == callID
		}) {
			t.Fatalf("later finalizer %s did not commit: %#v", callID, journal.Entries)
		}
	}
}

func TestFinishAndMarkUnknownFailureAreOneShotAndJoined(t *testing.T) {
	finishErr := errors.New("finish persistence failed")
	markErr := errors.New("mark unknown failed")
	repo := &effectFinalizationFailureRepo{
		MemoryRepo: sessiontree.NewMemoryRepo(),
		finishErr:  finishErr,
		markErr:    markErr,
	}
	p := harness.NewScriptedProvider(
		harness.Step(harness.Tool("call-1", "shell", `{"value":"x"}`), harness.DoneReason("tool_calls")),
	)
	h := newTestHarness(p, repo, cache.NewMemoryStore())
	h.effectFinalizationTimeout = 20 * time.Millisecond
	rec, ok := h.options.Sink.(*event.Recorder)
	if !ok {
		t.Fatalf("test harness sink=%T, want event recorder", h.options.Sink)
	}
	var handlerCalls atomic.Int64
	mustRegister(h.options.Tools, stringTool("shell", func(context.Context, string) (string, error) {
		handlerCalls.Add(1)
		return "known side effect", nil
	}))
	thread, err := h.StartThread(context.Background(), StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	_, runErr := thread.Run(context.Background(), "run", RunOptions{RunID: "run-1", TurnID: "turn-1"})
	if !errors.Is(runErr, sessiontree.ErrEffectOutcomeUnknown) {
		t.Fatalf("run err=%v, want fail-closed unknown outcome", runErr)
	}
	if handlerCalls.Load() != 1 || repo.finishCalls.Load() != 1 || repo.markCalls.Load() != 1 {
		t.Fatalf("calls handler=%d finish=%d mark=%d, want one-shot 1/1/1", handlerCalls.Load(), repo.finishCalls.Load(), repo.markCalls.Load())
	}
	for _, ev := range rec.Snapshot() {
		if ev.Type == event.RunEnd {
			if strings.Contains(ev.Err, finishErr.Error()) && strings.Contains(ev.Err, markErr.Error()) {
				return
			}
		}
	}
	t.Fatalf("missing joined run failure event: %#v", rec.Snapshot())
}

type failingEffectFinishRepo struct {
	*sessiontree.MemoryRepo
	err error
}

type countingEffectFinishRepo struct {
	*sessiontree.MemoryRepo
	finishCalls atomic.Int64
}

type beginEffectCommitBarrierRepo struct {
	*sessiontree.MemoryRepo
	committed   chan struct{}
	release     chan struct{}
	finishCalls atomic.Int64
}

type effectFinalizationFailureRepo struct {
	*sessiontree.MemoryRepo
	finishErr      error
	markErr        error
	expireFinish   bool
	finishCalls    atomic.Int64
	markCalls      atomic.Int64
	markContextErr atomic.Value
	markedState    atomic.Value
}

type effectUnknownObserver struct {
	markCalls atomic.Int64
	state     atomic.Value
}

type effectOutcomeFingerprintMemoryRepo struct {
	*sessiontree.MemoryRepo
	observer *effectUnknownObserver
}

type effectOutcomeFingerprintSQLiteRepo struct {
	*sqlite.Store
	observer *effectUnknownObserver
}

type firstEffectFinishFailureRepo struct {
	*sessiontree.MemoryRepo
	finishErr   error
	finishCalls atomic.Int64
	markCalls   atomic.Int64
	mu          sync.Mutex
	states      map[string]sessiontree.EffectAttemptState
}

func (r *countingEffectFinishRepo) FinishEffectDispatch(ctx context.Context, req sessiontree.FinishEffectDispatchRequest) (sessiontree.FinishEffectDispatchResult, error) {
	r.finishCalls.Add(1)
	return r.MemoryRepo.FinishEffectDispatch(ctx, req)
}

func (r *beginEffectCommitBarrierRepo) BeginEffectDispatch(ctx context.Context, req sessiontree.BeginEffectDispatchRequest) (sessiontree.EffectAttempt, error) {
	attempt, err := r.MemoryRepo.BeginEffectDispatch(ctx, req)
	if err != nil {
		return sessiontree.EffectAttempt{}, err
	}
	close(r.committed)
	<-r.release
	return attempt, nil
}

func (r *beginEffectCommitBarrierRepo) FinishEffectDispatch(ctx context.Context, req sessiontree.FinishEffectDispatchRequest) (sessiontree.FinishEffectDispatchResult, error) {
	r.finishCalls.Add(1)
	return r.MemoryRepo.FinishEffectDispatch(ctx, req)
}

func (r *failingEffectFinishRepo) FinishEffectDispatch(context.Context, sessiontree.FinishEffectDispatchRequest) (sessiontree.FinishEffectDispatchResult, error) {
	return sessiontree.FinishEffectDispatchResult{}, r.err
}

func (r *effectFinalizationFailureRepo) FinishEffectDispatch(ctx context.Context, _ sessiontree.FinishEffectDispatchRequest) (sessiontree.FinishEffectDispatchResult, error) {
	r.finishCalls.Add(1)
	if r.expireFinish {
		<-ctx.Done()
	}
	return sessiontree.FinishEffectDispatchResult{}, r.finishErr
}

func (r *effectFinalizationFailureRepo) MarkEffectUnknown(ctx context.Context, req sessiontree.MarkEffectUnknownRequest) (sessiontree.EffectAttempt, error) {
	r.markCalls.Add(1)
	if err := ctx.Err(); err != nil {
		r.markContextErr.Store(err)
		return sessiontree.EffectAttempt{}, err
	}
	if r.markErr != nil {
		return sessiontree.EffectAttempt{}, r.markErr
	}
	attempt, err := r.MemoryRepo.MarkEffectUnknown(ctx, req)
	if err == nil {
		r.markedState.Store(string(attempt.State))
	}
	return attempt, err
}

func (r *effectOutcomeFingerprintMemoryRepo) MarkEffectUnknown(ctx context.Context, req sessiontree.MarkEffectUnknownRequest) (sessiontree.EffectAttempt, error) {
	r.observer.markCalls.Add(1)
	attempt, err := r.MemoryRepo.MarkEffectUnknown(ctx, req)
	if err == nil {
		r.observer.state.Store(attempt.State)
	}
	return attempt, err
}

func (r *effectOutcomeFingerprintSQLiteRepo) MarkEffectUnknown(ctx context.Context, req sessiontree.MarkEffectUnknownRequest) (sessiontree.EffectAttempt, error) {
	r.observer.markCalls.Add(1)
	attempt, err := r.Store.MarkEffectUnknown(ctx, req)
	if err == nil {
		r.observer.state.Store(attempt.State)
	}
	return attempt, err
}

func (r *firstEffectFinishFailureRepo) FinishEffectDispatch(ctx context.Context, req sessiontree.FinishEffectDispatchRequest) (sessiontree.FinishEffectDispatchResult, error) {
	r.finishCalls.Add(1)
	callID := req.Result.Message.ToolCallID
	if callID == "call-0" {
		return sessiontree.FinishEffectDispatchResult{}, r.finishErr
	}
	result, err := r.MemoryRepo.FinishEffectDispatch(ctx, req)
	if err == nil {
		r.recordState(callID, result.Attempt.State)
	}
	return result, err
}

func (r *firstEffectFinishFailureRepo) MarkEffectUnknown(ctx context.Context, req sessiontree.MarkEffectUnknownRequest) (sessiontree.EffectAttempt, error) {
	r.markCalls.Add(1)
	attempt, err := r.MemoryRepo.MarkEffectUnknown(ctx, req)
	if err == nil {
		r.recordState("call-0", attempt.State)
	}
	return attempt, err
}

func (r *firstEffectFinishFailureRepo) recordState(callID string, state sessiontree.EffectAttemptState) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.states[callID] = state
}

func (r *firstEffectFinishFailureRepo) snapshotStates() map[string]sessiontree.EffectAttemptState {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]sessiontree.EffectAttemptState, len(r.states))
	for callID, state := range r.states {
		out[callID] = state
	}
	return out
}

package agentharness

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/internal/engine"
	"github.com/floegence/floret/internal/provider/cache"
	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/session/artifact"
	"github.com/floegence/floret/internal/sessiontree"
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
	entry, err := sessiontree.AppendMessage(ctx, repo, "thread", "turn-1", session.Message{
		Role: session.Tool, Content: "visible", ToolCallID: "call-1", ToolName: "shell",
		ToolResult: &session.ToolResultView{Status: string(observation.ActivityStatusSuccess)},
	})
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
	_, runErr := thread.Run(context.Background(), "run", RunOptions{RunID: "run-1", TurnID: "turn-1"})
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
	if countTurnMarkersForTurn(journal.Path, "turn-1", sessiontree.TurnAborted) != 1 {
		t.Fatalf("unknown effect did not permit exactly one aborted terminal marker: %#v", journal.Path)
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
	if !errors.Is(runErr, finishErr) || !errors.Is(runErr, markErr) {
		t.Fatalf("run err=%v, want joined finish and unknown failures", runErr)
	}
	if handlerCalls.Load() != 1 || repo.finishCalls.Load() != 1 || repo.markCalls.Load() != 1 {
		t.Fatalf("calls handler=%d finish=%d mark=%d, want one-shot 1/1/1", handlerCalls.Load(), repo.finishCalls.Load(), repo.markCalls.Load())
	}
}

type failingEffectFinishRepo struct {
	*sessiontree.MemoryRepo
	err error
}

type countingEffectFinishRepo struct {
	*sessiontree.MemoryRepo
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

func (r *countingEffectFinishRepo) FinishEffectDispatch(ctx context.Context, req sessiontree.FinishEffectDispatchRequest) (sessiontree.FinishEffectDispatchResult, error) {
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

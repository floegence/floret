package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/v3/config"
	"github.com/floegence/floret/v3/florettest"
	"github.com/floegence/floret/v3/identity"
	"github.com/floegence/floret/v3/observation"
	"github.com/floegence/floret/v3/provider"
	"github.com/floegence/floret/v3/runtime"
	"github.com/floegence/floret/v3/storage"
	"github.com/floegence/floret/v3/tools"
)

func TestV3SubscriptionMessageStrictJSON(t *testing.T) {
	valid := `{"type":"gap","value":{"last_delivered_revision":1,"resync_at_revision":2}}`
	var message runtime.SubscriptionMessage
	if err := json.Unmarshal([]byte(valid), &message); err != nil {
		t.Fatal(err)
	}
	gap, ok := message.Gap()
	if !ok || gap.LastDeliveredRevision != 1 || gap.ResyncAtRevision != 2 {
		t.Fatalf("decoded gap = %#v", message)
	}
	encoded, err := json.Marshal(message)
	if err != nil || string(encoded) != valid {
		t.Fatalf("encoded gap = %s, err = %v", encoded, err)
	}

	for name, payload := range map[string]string{
		"unknown envelope field":   `{"type":"gap","value":{"last_delivered_revision":1,"resync_at_revision":2},"extra":true}`,
		"duplicate envelope field": `{"type":"gap","type":"gap","value":{"last_delivered_revision":1,"resync_at_revision":2}}`,
		"unknown variant field":    `{"type":"gap","value":{"last_delivered_revision":1,"resync_at_revision":2,"extra":true}}`,
		"duplicate variant field":  `{"type":"gap","value":{"last_delivered_revision":1,"last_delivered_revision":1,"resync_at_revision":2}}`,
		"trailing data":            `{"type":"gap","value":{"last_delivered_revision":1,"resync_at_revision":2}} {}`,
		"unknown variant":          `{"type":"other","value":{}}`,
		"missing value":            `{"type":"gap"}`,
		"invalid gap":              `{"type":"gap","value":{"last_delivered_revision":2,"resync_at_revision":2}}`,
	} {
		t.Run(name, func(t *testing.T) {
			if err := json.Unmarshal([]byte(payload), &message); err == nil {
				t.Fatalf("accepted invalid subscription JSON: %s", payload)
			}
		})
	}
}

func TestV3ThreadsListInterruptedTurnRecoveryCandidatesReturnsIdentitiesOnly(t *testing.T) {
	ctx := context.Background()
	ids := &deterministicIDs{threads: []identity.ThreadID{"recovery-thread"}, turns: []identity.TurnID{"recovery-turn"}, runs: []identity.RunID{"recovery-run"}}
	host, err := runtime.Open(ctx, runtime.Options{Storage: storage.Memory(), IDSource: ids})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = host.Shutdown(context.Background()) }()
	created, err := host.Threads().CreateThread(ctx, runtime.CreateThreadCommand{LogicalRequestID: "create-recovery-candidate"})
	if err != nil {
		t.Fatal(err)
	}
	thread, err := host.Thread(ctx, created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	agent, err := runtime.NewAgent(config.AgentConfig{
		Profile: config.AgentProfile{ID: "assistant", Name: "Assistant"}, SystemPrompt: "Be concise.",
		Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}, florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "model", StateCompatibilityKey: "test:model:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
	))
	if err != nil {
		t.Fatal(err)
	}
	turns, err := thread.TurnExecutor(agent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := turns.AdmitTurn(ctx, runtime.StartTurnCommand{LogicalRequestID: "admit-recovery-candidate", UserMessage: runtime.TurnInput{Text: "unfinished"}}); err != nil {
		t.Fatal(err)
	}
	candidates, err := host.Threads().ListInterruptedTurnRecoveryCandidates(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 1 || candidates[0].ThreadID != created.ThreadID || candidates[0].ParentThreadID != "" {
		t.Fatalf("recovery candidates = %#v", candidates)
	}
}

func TestV3ThreadsListInterruptedTurnRecoveryCandidatesHonorsCancellation(t *testing.T) {
	host, err := runtime.Open(context.Background(), runtime.Options{Storage: storage.Memory()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = host.Shutdown(context.Background()) }()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := host.Threads().ListInterruptedTurnRecoveryCandidates(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled candidate discovery err=%v, want context.Canceled", err)
	}
}

func TestExecuteAdmissionAllowsConcurrentApprovalResolution(t *testing.T) {
	for _, test := range []struct {
		name   string
		source func(*testing.T) storage.Source
	}{
		{name: "memory", source: func(*testing.T) storage.Source { return storage.Memory() }},
		{name: "sqlite", source: func(t *testing.T) storage.Source {
			return storage.SQLite(filepath.Join(t.TempDir(), "floret.db"))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			testExecuteAdmissionAllowsConcurrentApprovalResolution(t, test.source(t))
		})
	}
}

func testExecuteAdmissionAllowsConcurrentApprovalResolution(t *testing.T, source storage.Source) {
	t.Helper()
	ctx := context.Background()
	host, err := runtime.Open(ctx, runtime.Options{
		Storage: source,
		IDSource: &deterministicIDs{
			threads: []identity.ThreadID{"thread-approval"},
			turns:   []identity.TurnID{"turn-approval"},
			runs:    []identity.RunID{"run-approval"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = host.Shutdown(context.Background()) }()

	created, err := host.Threads().CreateThread(ctx, runtime.CreateThreadCommand{LogicalRequestID: "create-approval"})
	if err != nil {
		t.Fatal(err)
	}
	thread, err := host.Thread(ctx, created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	reader := mustThreadReader(t, thread)

	gateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "model", StateCompatibilityKey: "test:model:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported, AttachmentPayload: provider.AttachmentDescriptors},
		florettest.Step{Events: []provider.Event{
			{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "call-approval", Name: "write_note", Args: `{}`}}},
			{Type: provider.EventDone, Reason: "tool_calls"},
		}},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "done"}, {Type: provider.EventDone, Reason: "stop"}}},
	)
	var executions atomic.Int32
	tool := tools.Define[struct{}](tools.Definition{
		Name: "write_note", Title: "Write note", Description: "Write a note.",
		InputSchema: tools.StrictObject(map[string]any{}, nil),
		Effects:     []tools.Effect{tools.EffectWrite}, Permission: tools.PermissionSpec{Mode: tools.PermissionAsk},
	}, nil, nil, func(context.Context, tools.Invocation[struct{}]) (tools.Result, error) {
		executions.Add(1)
		return tools.Result{Text: "written"}, nil
	})
	gate := runtime.EffectAuthorizationGateFunc(func(ctx context.Context, request runtime.EffectAuthorizationRequest, effect runtime.AuthorizedEffect) (runtime.EffectDispatchResult, error) {
		return effect(ctx, runtime.EffectAuthorizationProof{
			EffectAttemptID: request.EffectAttemptID, RequestFingerprint: request.RequestFingerprint,
			ThreadID: request.ThreadID, TurnID: request.TurnID, RunID: request.RunID, ToolCallID: request.ToolCallID,
			LeaseOwnerID: request.LeaseOwnerID, LeaseGeneration: request.LeaseGeneration,
			PolicyRevision: "test-policy-v1", AuditReference: "test-audit", AuditHash: "test-audit-hash",
			AuthorizedAt: time.Now(),
		})
	})
	agent, err := runtime.NewAgent(config.AgentConfig{
		Profile: config.AgentProfile{ID: "assistant", Name: "Assistant"}, SystemPrompt: "Be concise.",
		Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}, gateway, runtime.WithAgentTools(tool), runtime.WithAgentEffectAuthorization(gate))
	if err != nil {
		t.Fatal(err)
	}
	turns, err := thread.TurnExecutor(agent)
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := turns.AdmitTurn(ctx, runtime.StartTurnCommand{
		LogicalRequestID: "admit-approval", UserMessage: runtime.TurnInput{Text: "write a note"},
	})
	if err != nil {
		t.Fatal(err)
	}

	executeCtx, cancelExecute := context.WithCancel(ctx)
	defer cancelExecute()
	executed := make(chan struct {
		result runtime.StartTurnResult
		err    error
	}, 1)
	go func() {
		result, executeErr := turns.ExecuteAdmission(executeCtx, admitted.Receipt, runtime.ExecutionContext{})
		executed <- struct {
			result runtime.StartTurnResult
			err    error
		}{result: result, err: executeErr}
	}()

	queue := waitForPublicApprovalQueue(t, reader, 1)
	replayedExecution := make(chan struct {
		result runtime.StartTurnResult
		err    error
	}, 1)
	go func() {
		result, executeErr := turns.ExecuteAdmission(executeCtx, admitted.Receipt, runtime.ExecutionContext{})
		replayedExecution <- struct {
			result runtime.StartTurnResult
			err    error
		}{result: result, err: executeErr}
	}()
	approval := queue.Items[0]
	command := runtime.ResolveApprovalCommand{
		LogicalRequestID: "resolve-approval", DecisionID: "decision-approval",
		ExpectedGeneration: queue.Generation, ExpectedRevision: queue.Revision,
		ExpectedCurrent: runtime.ApprovalIdentity{
			ApprovalID: approval.ApprovalID, ThreadID: approval.ThreadID, TurnID: approval.TurnID,
			RunID: approval.RunID, ToolCallID: approval.ToolCallID, EffectAttemptID: approval.EffectAttemptID,
		},
		ExpectedApprovalRevision: approval.Revision, Decision: runtime.ApprovalDecisionApprove,
	}
	resolved := make(chan struct {
		result runtime.ResolveApprovalMutationResult
		err    error
	}, 1)
	go func() {
		result, resolveErr := turns.ResolveApproval(ctx, command)
		resolved <- struct {
			result runtime.ResolveApprovalMutationResult
			err    error
		}{result: result, err: resolveErr}
	}()

	var resolution runtime.ResolveApprovalMutationResult
	select {
	case outcome := <-resolved:
		if outcome.err != nil {
			t.Fatalf("resolve approval: %v", outcome.err)
		}
		resolution = outcome.result
	case <-time.After(500 * time.Millisecond):
		cancelExecute()
		<-executed
		<-replayedExecution
		<-resolved
		t.Fatal("approval resolution blocked behind active turn execution")
	}
	if resolution.Resolution.Receipt.State != "decision_submitted" || resolution.Receipt.Replayed {
		t.Fatalf("approval resolution = %#v", resolution)
	}
	select {
	case outcome := <-executed:
		if outcome.err != nil {
			finalTurn, _ := reader.ReadTurn(ctx, admitted.TurnID)
			failure := runtime.ThreadTurnFailure{}
			if finalTurn.Failure != nil {
				failure = *finalTurn.Failure
			}
			t.Fatalf("execute admitted turn: %v; failure=%#v", outcome.err, failure)
		}
		if outcome.result.TurnID != admitted.TurnID || outcome.result.RunID != admitted.RunID {
			t.Fatalf("executed turn = %#v", outcome.result)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("turn did not resume after approval")
	}
	select {
	case outcome := <-replayedExecution:
		if outcome.err != nil {
			t.Fatalf("replay admitted execution: %v", outcome.err)
		}
		if !outcome.result.Receipt.Replayed || outcome.result.TurnID != admitted.TurnID || outcome.result.RunID != admitted.RunID {
			t.Fatalf("replayed execution = %#v", outcome.result)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("concurrent execution replay did not converge")
	}
	replayed, err := turns.ResolveApproval(ctx, command)
	if err != nil {
		t.Fatalf("replay approval resolution: %v", err)
	}
	if !replayed.Receipt.Replayed || replayed.Resolution.Receipt.DecisionID != resolution.Resolution.Receipt.DecisionID {
		t.Fatalf("approval replay = %#v", replayed)
	}
	if got := executions.Load(); got != 1 {
		t.Fatalf("tool executions = %d, want 1", got)
	}
	if requests := gateway.Requests(); len(requests) != 2 {
		t.Fatalf("provider requests = %d, want 2", len(requests))
	}
	queue = waitForPublicApprovalQueue(t, reader, 0)
	if queue.CurrentApprovalID != "" {
		t.Fatalf("settled approval queue = %#v", queue)
	}
}

func waitForPublicApprovalQueue(t *testing.T, reader runtime.ThreadReader, count int) runtime.ApprovalQueue {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		queue, err := reader.ReadApprovalQueue(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(queue.Items) == count {
			return queue
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d approvals: %#v", count, queue)
		}
		time.Sleep(time.Millisecond)
	}
}

type deterministicIDs struct {
	threads []identity.ThreadID
	turns   []identity.TurnID
	runs    []identity.RunID
}

type recordingManualCompactions struct {
	polls []runtime.ManualCompactionPollRequest
}

func (source *recordingManualCompactions) PollManualCompaction(_ context.Context, request runtime.ManualCompactionPollRequest) (runtime.ManualCompactionRequest, bool, error) {
	source.polls = append(source.polls, request)
	if len(source.polls) > 1 {
		return runtime.ManualCompactionRequest{}, false, nil
	}
	return runtime.ManualCompactionRequest{RequestID: "manual-request", Source: "host_action"}, true, nil
}

func TestV3AgentManualCompactionCapabilityIsPolledByBoundTurn(t *testing.T) {
	ctx := context.Background()
	host, err := runtime.Open(ctx, runtime.Options{
		Storage: storage.Memory(),
		IDSource: &deterministicIDs{
			threads: []identity.ThreadID{"thread-manual"},
			turns:   []identity.TurnID{"turn-manual"},
			runs:    []identity.RunID{"run-manual"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = host.Shutdown(context.Background()) }()
	created, err := host.Threads().CreateThread(ctx, runtime.CreateThreadCommand{LogicalRequestID: "create-manual"})
	if err != nil {
		t.Fatal(err)
	}
	thread, err := host.Thread(ctx, created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	gateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "model", StateCompatibilityKey: "test:model:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported, AttachmentPayload: provider.AttachmentDescriptors},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "done"}, {Type: provider.EventDone}}},
	)
	source := &recordingManualCompactions{}
	agent, err := runtime.NewAgent(config.AgentConfig{
		Profile: config.AgentProfile{ID: "assistant", Name: "Assistant"}, SystemPrompt: "Be concise.",
		Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}, gateway, runtime.WithAgentManualCompactions(source))
	if err != nil {
		t.Fatal(err)
	}
	turns, err := thread.TurnExecutor(agent)
	if err != nil {
		t.Fatal(err)
	}
	started, err := turns.StartTurn(ctx, runtime.StartTurnCommand{
		LogicalRequestID: "start-manual", UserMessage: runtime.TurnInput{Text: "hello"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(source.polls) == 0 {
		t.Fatal("manual compaction source was not polled")
	}
	poll := source.polls[0]
	if poll.ThreadID != created.ThreadID || poll.TurnID != started.TurnID || poll.RunID != started.RunID || poll.PromptScopeID != identity.PromptScopeID(created.ThreadID) {
		t.Fatalf("manual compaction poll = %#v, started = %#v", poll, started)
	}
}

func TestV3BoundThreadCompactionReturnsCanonicalResult(t *testing.T) {
	ctx := context.Background()
	host, err := runtime.Open(ctx, runtime.Options{
		Storage: storage.Memory(),
		IDSource: &deterministicIDs{
			threads: []identity.ThreadID{"thread-compact"},
			turns:   []identity.TurnID{"turn-compact"},
			runs:    []identity.RunID{"run-turn-compact"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = host.Shutdown(context.Background()) }()
	created, err := host.Threads().CreateThread(ctx, runtime.CreateThreadCommand{LogicalRequestID: "create-compact"})
	if err != nil {
		t.Fatal(err)
	}
	thread, err := host.Thread(ctx, created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	gateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "model", StateCompatibilityKey: "test:model:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported, AttachmentPayload: provider.AttachmentDescriptors},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "short"}, {Type: provider.EventDone}}},
	)
	agent, err := runtime.NewAgent(config.AgentConfig{
		Profile: config.AgentProfile{ID: "assistant", Name: "Assistant"}, SystemPrompt: "Be concise.",
		Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}, gateway)
	if err != nil {
		t.Fatal(err)
	}
	turns, err := thread.TurnExecutor(agent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := turns.StartTurn(ctx, runtime.StartTurnCommand{LogicalRequestID: "start-compact", UserMessage: runtime.TurnInput{Text: "short"}}); err != nil {
		t.Fatal(err)
	}
	compactor := mustThreadCompactor(t, thread, agent)
	result, compactErr := compactor.Compact(ctx, runtime.CompactThreadCommand{
		LogicalRequestID: "compact-request", Source: "idle",
	})
	if compactErr == nil {
		t.Fatal("short standalone compaction unexpectedly succeeded without a noop error")
	}
	if result.ThreadID != created.ThreadID || result.RequestID != "compact-request" || result.Compaction.Status != observation.CompactionStatusNoop {
		t.Fatalf("compact result = %#v, err = %v", result, compactErr)
	}
	if err := result.Validate(); err != nil {
		t.Fatal(err)
	}
}

func (source *deterministicIDs) NewThreadID() (identity.ThreadID, error) {
	value := source.threads[0]
	source.threads = source.threads[1:]
	return value, nil
}

func (source *deterministicIDs) NewTurnID() (identity.TurnID, error) {
	value := source.turns[0]
	source.turns = source.turns[1:]
	return value, nil
}

func (source *deterministicIDs) NewRunID() (identity.RunID, error) {
	value := source.runs[0]
	source.runs = source.runs[1:]
	return value, nil
}

func mustThreadReader(t *testing.T, thread *runtime.Thread) runtime.ThreadReader {
	t.Helper()
	reader, err := thread.Reader()
	if err != nil {
		t.Fatal(err)
	}
	return reader
}

func mustThreadLifecycle(t *testing.T, thread *runtime.Thread) runtime.ThreadLifecycle {
	t.Helper()
	lifecycle, err := thread.Lifecycle()
	if err != nil {
		t.Fatal(err)
	}
	return lifecycle
}

func mustThreadCompactor(t *testing.T, thread *runtime.Thread, agent *runtime.Agent) runtime.ThreadCompactor {
	t.Helper()
	compactor, err := thread.Compactor(agent)
	if err != nil {
		t.Fatal(err)
	}
	return compactor
}

func TestV3TurnAdmissionReceiptSeparatesExecution(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "floret.db")
	ids := &deterministicIDs{
		threads: []identity.ThreadID{"thread-admit"},
		turns:   []identity.TurnID{"turn-admit"},
		runs:    []identity.RunID{"run-admit"},
	}
	host, err := runtime.Open(ctx, runtime.Options{Storage: storage.SQLite(path), IDSource: ids})
	if err != nil {
		t.Fatal(err)
	}
	created, err := host.Threads().CreateThread(ctx, runtime.CreateThreadCommand{LogicalRequestID: "create-admit"})
	if err != nil {
		t.Fatal(err)
	}
	thread, err := host.Thread(ctx, created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	gateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "model", StateCompatibilityKey: "test:model:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported, AttachmentPayload: provider.AttachmentDescriptors},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "executed"}, {Type: provider.EventDone}}},
	)
	agent, err := runtime.NewAgent(config.AgentConfig{
		Profile: config.AgentProfile{ID: "assistant", Name: "Assistant"}, SystemPrompt: "Be concise.",
		Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}, gateway)
	if err != nil {
		t.Fatal(err)
	}
	turns, err := thread.TurnExecutor(agent)
	if err != nil {
		t.Fatal(err)
	}

	startCommand := runtime.StartTurnCommand{
		LogicalRequestID: "admit-turn",
		UserMessage:      runtime.TurnInput{Text: "hello"},
	}
	admitted, err := turns.AdmitTurn(ctx, startCommand)
	if err != nil {
		t.Fatal(err)
	}
	if admitted.ThreadID != created.ThreadID || admitted.TurnID != "turn-admit" || admitted.RunID != "run-admit" ||
		admitted.UserEntryID == "" || admitted.Receipt.Replayed || admitted.Receipt.Revision <= created.Receipt.Revision {
		t.Fatalf("admission = %#v", admitted)
	}
	if requests := gateway.Requests(); len(requests) != 0 {
		t.Fatalf("provider was called during admission: %#v", requests)
	}
	reader := mustThreadReader(t, thread)
	running, err := reader.ReadTurn(ctx, admitted.TurnID)
	if err != nil {
		t.Fatal(err)
	}
	if running.Status != runtime.TurnStatusRunning || running.UserEntryID != admitted.UserEntryID ||
		running.UserMessageOrigin != runtime.ThreadUserMessageOriginUser || running.UserInput != "hello" {
		t.Fatalf("running turn after admission = %#v", running)
	}
	if err := host.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}

	restarted, err := runtime.Open(ctx, runtime.Options{Storage: storage.SQLite(path), IDSource: &deterministicIDs{}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = restarted.Shutdown(context.Background()) }()
	restartedThread, err := restarted.Thread(ctx, created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	restartedGateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "model", StateCompatibilityKey: "test:model:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported, AttachmentPayload: provider.AttachmentDescriptors},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "executed"}, {Type: provider.EventDone}}},
	)
	restartedAgent, err := runtime.NewAgent(config.AgentConfig{
		Profile: config.AgentProfile{ID: "assistant", Name: "Assistant"}, SystemPrompt: "Be concise.",
		Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}, restartedGateway)
	if err != nil {
		t.Fatal(err)
	}
	restartedTurns, err := restartedThread.TurnExecutor(restartedAgent)
	if err != nil {
		t.Fatal(err)
	}
	replayedAdmission, err := restartedTurns.AdmitTurn(ctx, startCommand)
	if err != nil {
		t.Fatal(err)
	}
	if replayedAdmission.ThreadID != admitted.ThreadID || replayedAdmission.TurnID != admitted.TurnID ||
		replayedAdmission.RunID != admitted.RunID || replayedAdmission.UserEntryID != admitted.UserEntryID ||
		!replayedAdmission.Receipt.Replayed {
		t.Fatalf("replayed admission = %#v, want %#v", replayedAdmission, admitted)
	}
	if requests := restartedGateway.Requests(); len(requests) != 0 {
		t.Fatalf("provider was called during admission replay: %#v", requests)
	}
	executed, err := restartedTurns.ExecuteAdmission(ctx, admitted.Receipt, runtime.ExecutionContext{})
	if err != nil {
		t.Fatal(err)
	}
	if executed.AdmissionReceipt == nil {
		t.Fatalf("executed admission receipt is nil")
	}
	if executed.ThreadID != admitted.ThreadID || executed.TurnID != admitted.TurnID || executed.RunID != admitted.RunID ||
		executed.AdmissionReceipt.UserEntryID != admitted.UserEntryID {
		t.Fatalf("executed = %#v", executed)
	}
	if requests := restartedGateway.Requests(); len(requests) != 1 {
		t.Fatalf("provider requests after execute = %#v", requests)
	}
	replayedExecution, err := restartedTurns.ExecuteAdmission(ctx, admitted.Receipt, runtime.ExecutionContext{})
	if err != nil {
		t.Fatal(err)
	}
	if replayedExecution.AdmissionReceipt == nil {
		t.Fatalf("replayed execution admission receipt is nil")
	}
	if !replayedExecution.Receipt.Replayed || replayedExecution.TurnID != admitted.TurnID || !replayedExecution.AdmissionReceipt.Replayed {
		t.Fatalf("replayed execution = %#v", replayedExecution)
	}
	if requests := restartedGateway.Requests(); len(requests) != 1 {
		t.Fatalf("provider was called during execution replay: %#v", requests)
	}
}

func TestV3HostAllocatesAndReplaysCanonicalIdentities(t *testing.T) {
	ctx := context.Background()
	ids := &deterministicIDs{
		threads: []identity.ThreadID{"thread-allocated"},
		turns:   []identity.TurnID{"turn-allocated", "turn-retry"},
		runs:    []identity.RunID{"run-allocated", "run-retry"},
	}
	path := filepath.Join(t.TempDir(), "floret.db")
	host, err := runtime.Open(ctx, runtime.Options{Storage: storage.SQLite(path), IDSource: ids})
	if err != nil {
		t.Fatal(err)
	}

	created, err := host.Threads().CreateThread(ctx, runtime.CreateThreadCommand{
		LogicalRequestID: "create-request",
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.ThreadID != "thread-allocated" || created.Receipt.Replayed {
		t.Fatalf("create result = %#v", created)
	}
	replayed, err := host.Threads().CreateThread(ctx, runtime.CreateThreadCommand{
		LogicalRequestID: "create-request",
	})
	if err != nil {
		t.Fatal(err)
	}
	if replayed.ThreadID != created.ThreadID || !replayed.Receipt.Replayed {
		t.Fatalf("create replay = %#v", replayed)
	}

	thread, err := host.Thread(ctx, created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	reader := mustThreadReader(t, thread)
	lifecycle := mustThreadLifecycle(t, thread)
	snapshot, err := reader.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Revision <= 0 || snapshot.Thread.ThroughOrdinal != 0 || snapshot.Thread.ID != created.ThreadID {
		t.Fatalf("snapshot = %#v", snapshot)
	}

	gateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "model", StateCompatibilityKey: "test:model:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported, AttachmentPayload: provider.AttachmentDescriptors},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "done"}, {Type: provider.EventDone}}},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "retried"}, {Type: provider.EventDone}}},
	)
	agent, err := runtime.NewAgent(config.AgentConfig{
		Profile: config.AgentProfile{ID: "assistant", Name: "Assistant"}, SystemPrompt: "Be concise.",
		Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}, gateway)
	if err != nil {
		t.Fatal(err)
	}
	turns, err := thread.TurnExecutor(agent)
	if err != nil {
		t.Fatal(err)
	}
	started, err := turns.StartTurn(ctx, runtime.StartTurnCommand{
		LogicalRequestID: "turn-request",
		UserMessage:      runtime.TurnInput{Text: "hello"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if started.TurnID != "turn-allocated" || started.RunID != "run-allocated" || started.Receipt.Replayed {
		t.Fatalf("start result = %#v", started)
	}
	overview, err := reader.ReadOverview(ctx)
	if err != nil || overview.LatestTurn == nil || overview.LatestTurn.TurnID != started.TurnID {
		t.Fatalf("thread overview = %#v err=%v", overview, err)
	}
	page, err := reader.ListTurns(ctx, runtime.ThreadTurnsRequest{Tail: 10})
	if err != nil || len(page.Turns) != 1 || page.Turns[0].TurnID != started.TurnID {
		t.Fatalf("thread turns = %#v err=%v", page, err)
	}
	readTurn, err := reader.ReadTurn(ctx, started.TurnID)
	if err != nil || readTurn.RunID != started.RunID {
		t.Fatalf("thread turn = %#v err=%v", readTurn, err)
	}
	if _, err := reader.ReadAgentTodos(ctx); err != nil {
		t.Fatalf("read todos: %v", err)
	}
	if _, err := reader.ReadContext(ctx); err != nil {
		t.Fatalf("read context: %v", err)
	}
	if _, err := reader.ReadApprovalQueue(ctx); err != nil {
		t.Fatalf("read approval queue: %v", err)
	}
	if _, err := reader.ReadAuthoritativeProjection(ctx, started.TurnID, started.RunID); err != nil {
		t.Fatalf("read projection: %v", err)
	}
	if targets, err := reader.ListPendingToolTargets(ctx); err != nil || len(targets) != 0 {
		t.Fatalf("root pending targets = %#v err=%v", targets, err)
	}
	titled, err := lifecycle.SetTitle(ctx, runtime.SetThreadTitleCommand{LogicalRequestID: "title-request", Title: "Canonical title"})
	if err != nil || titled.Thread.Title != "Canonical title" || titled.Receipt.Replayed {
		t.Fatalf("set title = %#v err=%v", titled, err)
	}
	titleReplay, err := lifecycle.SetTitle(ctx, runtime.SetThreadTitleCommand{LogicalRequestID: "title-request", Title: "Canonical title"})
	if err != nil || !titleReplay.Receipt.Replayed || titleReplay.Thread.Title != titled.Thread.Title {
		t.Fatalf("title replay = %#v err=%v", titleReplay, err)
	}
	if _, err := lifecycle.SetTitle(ctx, runtime.SetThreadTitleCommand{LogicalRequestID: "title-request", Title: "Changed title"}); !errors.Is(err, runtime.ErrRequestConflict) {
		t.Fatalf("changed title replay = %v", err)
	}
	if _, err := lifecycle.InterruptedTurnRecovery(ctx); !errors.Is(err, runtime.ErrInterruptedTurnNotFound) {
		t.Fatalf("missing root interrupted turn = %v", err)
	}
	if _, err := lifecycle.PendingToolRecovery(ctx, runtime.PendingToolSettlementTarget{ThreadID: "other-thread"}); !errors.Is(err, runtime.ErrThreadAuthorityInvariant) {
		t.Fatalf("mismatched root pending target = %v", err)
	}
	replayedTurn, err := turns.StartTurn(ctx, runtime.StartTurnCommand{
		LogicalRequestID: "turn-request",
		UserMessage:      runtime.TurnInput{Text: "hello"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if replayedTurn.TurnID != started.TurnID || replayedTurn.RunID != started.RunID || !replayedTurn.Receipt.Replayed {
		t.Fatalf("turn replay = %#v", replayedTurn)
	}
	_, err = turns.StartTurn(ctx, runtime.StartTurnCommand{
		LogicalRequestID: "turn-request",
		UserMessage:      runtime.TurnInput{Text: "changed"},
	})
	var conflict *runtime.RequestConflictError
	if !errors.As(err, &conflict) || !errors.Is(err, runtime.ErrRequestConflict) {
		t.Fatalf("conflicting replay error = %T %v", err, err)
	}
	retried, err := turns.RetryTurn(ctx, runtime.RetryTurnCommand{LogicalRequestID: "retry-request", Reason: "retry"})
	if err != nil {
		t.Fatal(err)
	}
	if retried.TurnID != "turn-retry" || retried.RunID != "run-retry" || retried.Receipt.Replayed {
		t.Fatalf("retry result = %#v", retried)
	}
	replayedRetry, err := turns.RetryTurn(ctx, runtime.RetryTurnCommand{LogicalRequestID: "retry-request", Reason: "retry"})
	if err != nil {
		t.Fatal(err)
	}
	if replayedRetry.TurnID != retried.TurnID || replayedRetry.RunID != retried.RunID || !replayedRetry.Receipt.Replayed {
		t.Fatalf("retry replay = %#v", replayedRetry)
	}

	if err := host.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Snapshot(ctx); !errors.Is(err, runtime.ErrHostClosed) {
		t.Fatalf("snapshot after shutdown = %v", err)
	}

	restarted, err := runtime.Open(ctx, runtime.Options{Storage: storage.SQLite(path), IDSource: &deterministicIDs{}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = restarted.Shutdown(context.Background()) }()
	restartedCreate, err := restarted.Threads().CreateThread(ctx, runtime.CreateThreadCommand{LogicalRequestID: "create-request"})
	if err != nil {
		t.Fatal(err)
	}
	if restartedCreate.ThreadID != created.ThreadID || !restartedCreate.Receipt.Replayed {
		t.Fatalf("restart create replay = %#v", restartedCreate)
	}
	restartedThread, err := restarted.Thread(ctx, created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	restartedGateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "model", StateCompatibilityKey: "test:model:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported, AttachmentPayload: provider.AttachmentDescriptors},
	)
	restartedAgent, err := runtime.NewAgent(config.AgentConfig{
		Profile: config.AgentProfile{ID: "assistant", Name: "Assistant"}, SystemPrompt: "Be concise.",
		Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}, restartedGateway)
	if err != nil {
		t.Fatal(err)
	}
	restartedTurns, err := restartedThread.TurnExecutor(restartedAgent)
	if err != nil {
		t.Fatal(err)
	}
	if result, err := restartedTurns.StartTurn(ctx, runtime.StartTurnCommand{
		LogicalRequestID: "turn-request", UserMessage: runtime.TurnInput{Text: "hello"},
	}); err != nil || result.TurnID != started.TurnID || !result.Receipt.Replayed {
		t.Fatalf("restart turn replay = %#v err=%v", result, err)
	}
	if result, err := restartedTurns.RetryTurn(ctx, runtime.RetryTurnCommand{
		LogicalRequestID: "retry-request", Reason: "retry",
	}); err != nil || result.TurnID != retried.TurnID || !result.Receipt.Replayed {
		t.Fatalf("restart retry replay = %#v err=%v", result, err)
	}
}

func TestV3SubAgentMutationsReplayAcrossRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "floret.db")
	ids := &deterministicIDs{threads: []identity.ThreadID{"parent-thread", "child-thread"}}
	host, err := runtime.Open(ctx, runtime.Options{Storage: storage.SQLite(path), IDSource: ids})
	if err != nil {
		t.Fatal(err)
	}
	created, err := host.Threads().CreateThread(ctx, runtime.CreateThreadCommand{LogicalRequestID: "create-parent"})
	if err != nil {
		t.Fatal(err)
	}
	parent, err := host.Thread(ctx, created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	gateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "model", StateCompatibilityKey: "test:model:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported, AttachmentPayload: provider.AttachmentDescriptors},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "spawned"}, {Type: provider.EventDone}}},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "continued"}, {Type: provider.EventDone}}},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "interrupted"}, {Type: provider.EventDone}}},
	)
	agent, err := runtime.NewAgent(config.AgentConfig{
		Profile: config.AgentProfile{ID: "assistant", Name: "Assistant"}, SystemPrompt: "Be concise.",
		Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}, gateway)
	if err != nil {
		t.Fatal(err)
	}
	subAgents, err := parent.SubAgentManager(ctx, agent)
	if err != nil {
		t.Fatal(err)
	}
	spawnCommand := runtime.SpawnSubAgentCommand{
		LogicalRequestID: "spawn-child", TaskName: "worker", Input: runtime.TurnInput{Text: "start"},
		ForkMode: runtime.SubAgentForkNone,
	}
	spawned, err := subAgents.SpawnSubAgent(ctx, spawnCommand)
	if err != nil {
		t.Fatal(err)
	}
	if spawned.Child.ThreadID != "child-thread" || spawned.Receipt.Replayed || spawned.Receipt.ThreadID != "child-thread" {
		t.Fatalf("spawn result = %#v", spawned)
	}
	waitForV3SubAgentStatus(t, ctx, subAgents, spawned.Child.ThreadID, runtime.SubAgentStatusCompleted)
	reader := mustThreadReader(t, parent)
	child, err := reader.Child(ctx, spawned.Child.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	if child.ID() != spawned.Child.ThreadID {
		t.Fatalf("child id = %q, want %q", child.ID(), spawned.Child.ThreadID)
	}
	children, err := reader.ListSubAgents(ctx)
	if err != nil || len(children) != 1 || children[0].ThreadID != child.ID() {
		t.Fatalf("bound parent children = %#v err=%v", children, err)
	}
	detail, err := child.ReadDetail(ctx, runtime.ThreadDetailRequest{IncludeRaw: true})
	if err != nil || detail.Snapshot.ThreadID != spawned.Child.ThreadID {
		t.Fatalf("child detail = %#v err=%v", detail, err)
	}
	if targets, err := child.ListPendingToolTargets(ctx); err != nil || len(targets) != 0 {
		t.Fatalf("child pending targets = %#v err=%v", targets, err)
	}
	descendant, err := reader.Descendant(ctx, spawned.Child.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	page, err := descendant.ListTurns(ctx, runtime.ThreadTurnsRequest{Tail: 10})
	if err != nil || len(page.Turns) != 1 {
		t.Fatalf("descendant turns = %#v err=%v", page, err)
	}
	turn, err := descendant.ReadTurn(ctx, page.Turns[0].TurnID)
	if err != nil || turn.RunID != page.Turns[0].RunID {
		t.Fatalf("descendant turn = %#v err=%v", turn, err)
	}
	childPage, err := child.ListTurns(ctx, runtime.ThreadTurnsRequest{Tail: 10})
	if err != nil || len(childPage.Turns) != 1 {
		t.Fatalf("child turns = %#v err=%v", childPage, err)
	}
	childTurn, err := child.ReadTurn(ctx, childPage.Turns[0].TurnID)
	if err != nil || childTurn.RunID != childPage.Turns[0].RunID {
		t.Fatalf("child turn = %#v err=%v", childTurn, err)
	}
	if _, err := child.InterruptedTurnRecovery(ctx); !errors.Is(err, runtime.ErrInterruptedTurnNotFound) {
		t.Fatalf("missing child interrupted turn = %v", err)
	}
	if _, err := child.PendingToolRecovery(ctx, runtime.PendingToolSettlementTarget{ThreadID: parent.ID()}); !errors.Is(err, runtime.ErrThreadAuthorityInvariant) {
		t.Fatalf("mismatched child pending target = %v", err)
	}
	if _, err := descendant.ReadTurn(ctx, "missing-turn"); !errors.Is(err, runtime.ErrTurnNotFound) {
		t.Fatalf("missing descendant turn error = %v", err)
	}
	if _, err := descendant.ReadArtifact(ctx, "missing-artifact"); !errors.Is(err, runtime.ErrArtifactNotFound) {
		t.Fatalf("missing descendant artifact error = %v", err)
	}
	if _, err := reader.Child(ctx, parent.ID()); !errors.Is(err, runtime.ErrSubAgentNotFound) {
		t.Fatalf("parent bound as child error = %v", err)
	}
	spawnReplay, err := subAgents.SpawnSubAgent(ctx, spawnCommand)
	if err != nil || !spawnReplay.Receipt.Replayed || spawnReplay.Child.ThreadID != spawned.Child.ThreadID {
		t.Fatalf("spawn replay = %#v err=%v", spawnReplay, err)
	}
	changedSpawn := spawnCommand
	changedSpawn.TaskDescription = "changed"
	if _, err := subAgents.SpawnSubAgent(ctx, changedSpawn); !errors.Is(err, runtime.ErrRequestConflict) {
		t.Fatalf("changed spawn replay = %v", err)
	}

	sendCommand := runtime.SendSubAgentMessageCommand{
		LogicalRequestID: "send-child", ChildThreadID: spawned.Child.ThreadID, Input: runtime.TurnInput{Text: "continue"},
	}
	sent, err := subAgents.SendSubAgentMessage(ctx, sendCommand)
	if err != nil {
		t.Fatal(err)
	}
	waitForV3SubAgentStatus(t, ctx, subAgents, spawned.Child.ThreadID, runtime.SubAgentStatusCompleted)
	sendReplay, err := subAgents.SendSubAgentMessage(ctx, sendCommand)
	if err != nil || !sendReplay.Receipt.Replayed || sendReplay.Child.ThreadID != sent.Child.ThreadID {
		t.Fatalf("send replay = %#v err=%v", sendReplay, err)
	}
	changedSend := sendCommand
	changedSend.Input.Text = "changed"
	if _, err := subAgents.SendSubAgentMessage(ctx, changedSend); !errors.Is(err, runtime.ErrRequestConflict) {
		t.Fatalf("changed send replay = %v", err)
	}

	interruptCommand := runtime.InterruptSubAgentCommand{
		LogicalRequestID: "interrupt-child", ChildThreadID: spawned.Child.ThreadID, Input: runtime.TurnInput{Text: "stop and reconsider"},
	}
	interrupted, err := subAgents.InterruptSubAgent(ctx, interruptCommand)
	if err != nil {
		t.Fatal(err)
	}
	waitForV3SubAgentStatus(t, ctx, subAgents, spawned.Child.ThreadID, runtime.SubAgentStatusCompleted)
	interruptReplay, err := subAgents.InterruptSubAgent(ctx, interruptCommand)
	if err != nil || !interruptReplay.Receipt.Replayed || interruptReplay.Child.ThreadID != interrupted.Child.ThreadID {
		t.Fatalf("interrupt replay = %#v err=%v", interruptReplay, err)
	}
	changedInterrupt := interruptCommand
	changedInterrupt.Input.Text = "changed"
	if _, err := subAgents.InterruptSubAgent(ctx, changedInterrupt); !errors.Is(err, runtime.ErrRequestConflict) {
		t.Fatalf("changed interrupt replay = %v", err)
	}
	waited, err := subAgents.WaitSubAgents(ctx, runtime.WaitSubAgentsCommand{
		ChildThreadIDs: []identity.ThreadID{spawned.Child.ThreadID}, Timeout: time.Second,
	})
	if err != nil || waited.TimedOut || len(waited.Snapshots) != 1 || waited.Snapshots[0].ThreadID != spawned.Child.ThreadID {
		t.Fatalf("wait result = %#v, err = %v", waited, err)
	}
	if err := waited.Validate(); err != nil {
		t.Fatal(err)
	}
	closeCommand := runtime.CloseSubAgentCommand{
		LogicalRequestID: "close-child", ChildThreadID: spawned.Child.ThreadID, Reason: "parent_terminal",
	}
	closed, err := subAgents.CloseSubAgent(ctx, closeCommand)
	if err != nil {
		t.Fatal(err)
	}
	if !closed.Child.Closed || closed.Child.Status != runtime.SubAgentStatusClosed || closed.Receipt.Replayed {
		t.Fatalf("close result = %#v", closed)
	}
	closeReplay, err := subAgents.CloseSubAgent(ctx, closeCommand)
	if err != nil || !closeReplay.Receipt.Replayed || closeReplay.Child.ThreadID != closed.Child.ThreadID {
		t.Fatalf("close replay = %#v, err = %v", closeReplay, err)
	}
	changedClose := closeCommand
	changedClose.Reason = "changed"
	if _, err := subAgents.CloseSubAgent(ctx, changedClose); !errors.Is(err, runtime.ErrRequestConflict) {
		t.Fatalf("changed close replay = %v", err)
	}

	if err := host.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := subAgents.SendSubAgentMessage(ctx, runtime.SendSubAgentMessageCommand{
		LogicalRequestID: "after-shutdown", ChildThreadID: spawned.Child.ThreadID, Input: runtime.TurnInput{Text: "late"},
	}); !errors.Is(err, runtime.ErrHostClosed) {
		t.Fatalf("send after shutdown = %v", err)
	}

	restarted, err := runtime.Open(ctx, runtime.Options{Storage: storage.SQLite(path), IDSource: &deterministicIDs{}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = restarted.Shutdown(context.Background()) }()
	restartedParent, err := restarted.Thread(ctx, created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	restartedGateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "model", StateCompatibilityKey: "test:model:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported, AttachmentPayload: provider.AttachmentDescriptors},
	)
	restartedAgent, err := runtime.NewAgent(config.AgentConfig{
		Profile: config.AgentProfile{ID: "assistant", Name: "Assistant"}, SystemPrompt: "Be concise.",
		Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}, restartedGateway)
	if err != nil {
		t.Fatal(err)
	}
	restartedSubAgents, err := restartedParent.SubAgentManager(ctx, restartedAgent)
	if err != nil {
		t.Fatal(err)
	}
	if result, err := restartedSubAgents.SpawnSubAgent(ctx, spawnCommand); err != nil || !result.Receipt.Replayed || result.Child.ThreadID != spawned.Child.ThreadID {
		t.Fatalf("restart spawn replay = %#v err=%v", result, err)
	}
	if result, err := restartedSubAgents.SendSubAgentMessage(ctx, sendCommand); err != nil || !result.Receipt.Replayed || result.Child.ThreadID != sent.Child.ThreadID {
		t.Fatalf("restart send replay = %#v err=%v", result, err)
	}
	if result, err := restartedSubAgents.InterruptSubAgent(ctx, interruptCommand); err != nil || !result.Receipt.Replayed || result.Child.ThreadID != interrupted.Child.ThreadID {
		t.Fatalf("restart interrupt replay = %#v err=%v", result, err)
	}
	if result, err := restartedSubAgents.CloseSubAgent(ctx, closeCommand); err != nil || !result.Receipt.Replayed || result.Child.ThreadID != closed.Child.ThreadID {
		t.Fatalf("restart close replay = %#v err=%v", result, err)
	}
}

func waitForV3SubAgentStatus(t *testing.T, ctx context.Context, subAgents runtime.SubAgentManager, childThreadID identity.ThreadID, want runtime.SubAgentStatus) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		children, err := subAgents.List(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, child := range children {
			if child.ThreadID == childThreadID && child.Status == want {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("subagent %q did not reach status %q", childThreadID, want)
}

func TestV3SubscriptionSuspendsWithOneGapOnTransientOverflow(t *testing.T) {
	ctx := context.Background()
	host, err := runtime.Open(ctx, runtime.Options{
		Storage: storage.SQLite(filepath.Join(t.TempDir(), "floret.db")),
		IDSource: &deterministicIDs{
			threads: []identity.ThreadID{"thread-gap"},
			turns:   []identity.TurnID{"turn-gap"},
			runs:    []identity.RunID{"run-gap"},
		},
		SubscriptionBuffer: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = host.Shutdown(context.Background()) }()
	created, err := host.Threads().CreateThread(ctx, runtime.CreateThreadCommand{LogicalRequestID: "create-gap"})
	if err != nil {
		t.Fatal(err)
	}
	thread, err := host.Thread(ctx, created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	reader := mustThreadReader(t, thread)
	view, err := reader.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	subscription, err := reader.Subscribe(ctx, runtime.SubscribeOptions{AfterRevision: view.Revision})
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	gateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "model", StateCompatibilityKey: "test:model:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported, AttachmentPayload: provider.AttachmentDescriptors},
		florettest.Step{Events: []provider.Event{
			{Type: provider.EventDelta, Text: "one"},
			{Type: provider.EventDelta, Text: "two"},
			{Type: provider.EventDelta, Text: "three"},
			{Type: provider.EventDone},
		}},
	)
	agent, err := runtime.NewAgent(config.AgentConfig{
		Profile: config.AgentProfile{ID: "assistant", Name: "Assistant"}, SystemPrompt: "Be concise.",
		Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}, gateway)
	if err != nil {
		t.Fatal(err)
	}
	turns, err := thread.TurnExecutor(agent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := turns.StartTurn(ctx, runtime.StartTurnCommand{
		LogicalRequestID: "turn-gap", UserMessage: runtime.TurnInput{Text: "hello"},
	}); err != nil {
		t.Fatal(err)
	}
	message, err := subscription.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	gap, ok := message.Gap()
	if !ok || message.Kind() != runtime.SubscriptionMessageGap ||
		gap.LastDeliveredRevision != view.Revision || gap.ResyncAtRevision <= view.Revision {
		t.Fatalf("overflow message = %#v", message)
	}
	if _, err := subscription.Next(ctx); !errors.Is(err, runtime.ErrSubscriptionStale) {
		t.Fatalf("next after gap = %v", err)
	}
}

func TestV3SubscriptionGapFreezesResyncRevisionAtOverflow(t *testing.T) {
	ctx := context.Background()
	host, err := runtime.Open(ctx, runtime.Options{
		Storage: storage.Memory(),
		IDSource: &deterministicIDs{
			threads: []identity.ThreadID{"thread-gap-freeze"},
			turns:   []identity.TurnID{"turn-gap-first"},
			runs:    []identity.RunID{"run-gap-first"},
		},
		SubscriptionBuffer: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = host.Shutdown(context.Background()) }()
	created, err := host.Threads().CreateThread(ctx, runtime.CreateThreadCommand{LogicalRequestID: "create-gap-freeze"})
	if err != nil {
		t.Fatal(err)
	}
	thread, err := host.Thread(ctx, created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	reader := mustThreadReader(t, thread)
	lifecycle := mustThreadLifecycle(t, thread)
	initial, err := reader.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	subscription, err := reader.Subscribe(ctx, runtime.SubscribeOptions{AfterRevision: initial.Revision})
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	gateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "model", StateCompatibilityKey: "test:model:v1"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{
			{Type: provider.EventDelta, Text: "one"},
			{Type: provider.EventDelta, Text: "two"},
			{Type: provider.EventDone},
		}},
	)
	agent, err := runtime.NewAgent(config.AgentConfig{
		Profile: config.AgentProfile{ID: "assistant", Name: "Assistant"}, SystemPrompt: "Be concise.",
		Context: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}, gateway)
	if err != nil {
		t.Fatal(err)
	}
	turns, err := thread.TurnExecutor(agent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := turns.StartTurn(ctx, runtime.StartTurnCommand{
		LogicalRequestID: "gap-first", UserMessage: runtime.TurnInput{Text: "first"},
	}); err != nil {
		t.Fatal(err)
	}
	atTurnEnd, err := reader.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	deleted, err := lifecycle.Delete(ctx, runtime.DeleteThreadCommand{LogicalRequestID: "gap-delete"})
	if err != nil {
		t.Fatal(err)
	}
	if deleted.Receipt.Revision <= atTurnEnd.Revision {
		t.Fatalf("delete did not advance revision: turn_end=%d delete=%d", atTurnEnd.Revision, deleted.Receipt.Revision)
	}
	message, err := subscription.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	gap, ok := message.Gap()
	if !ok || gap.ResyncAtRevision <= initial.Revision || gap.ResyncAtRevision > atTurnEnd.Revision ||
		gap.ResyncAtRevision >= deleted.Receipt.Revision {
		t.Fatalf("gap = %#v, want overflow revision after %d and no later than turn end %d, before delete %d",
			gap, initial.Revision, atTurnEnd.Revision, deleted.Receipt.Revision)
	}
	if _, err := subscription.Next(ctx); !errors.Is(err, runtime.ErrSubscriptionStale) {
		t.Fatalf("next after frozen gap = %v", err)
	}
}

func TestV3ForkAndDeleteReplayUseFloretOwnedTombstones(t *testing.T) {
	ctx := context.Background()
	host, err := runtime.Open(ctx, runtime.Options{
		Storage:  storage.SQLite(filepath.Join(t.TempDir(), "floret.db")),
		IDSource: &deterministicIDs{threads: []identity.ThreadID{"thread-source", "thread-fork"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = host.Shutdown(context.Background()) }()
	created, err := host.Threads().CreateThread(ctx, runtime.CreateThreadCommand{LogicalRequestID: "create-source"})
	if err != nil {
		t.Fatal(err)
	}
	thread, err := host.Thread(ctx, created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	lifecycle := mustThreadLifecycle(t, thread)
	forked, err := lifecycle.Fork(ctx, runtime.ForkThreadCommand{LogicalRequestID: "fork-request"})
	if err != nil {
		t.Fatal(err)
	}
	if forked.ThreadID != "thread-fork" || forked.Receipt.Replayed {
		t.Fatalf("fork result = %#v", forked)
	}
	forkReplay, err := lifecycle.Fork(ctx, runtime.ForkThreadCommand{LogicalRequestID: "fork-request"})
	if err != nil || forkReplay.ThreadID != forked.ThreadID || !forkReplay.Receipt.Replayed {
		t.Fatalf("fork replay = %#v err=%v", forkReplay, err)
	}

	destination, err := host.Thread(ctx, forked.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	destinationReader := mustThreadReader(t, destination)
	destinationLifecycle := mustThreadLifecycle(t, destination)
	view, err := destinationReader.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	subscription, err := destinationReader.Subscribe(ctx, runtime.SubscribeOptions{AfterRevision: view.Revision})
	if err != nil {
		t.Fatal(err)
	}
	deleted, err := destinationLifecycle.Delete(ctx, runtime.DeleteThreadCommand{LogicalRequestID: "delete-request"})
	if err != nil {
		t.Fatal(err)
	}
	if deleted.Receipt.Revision <= view.Revision || deleted.Receipt.Replayed {
		t.Fatalf("delete result = %#v", deleted)
	}
	message, err := subscription.Next(ctx)
	durable, durableOK := message.Durable()
	deletedEvent, deletedOK := durable.Deleted()
	_, changeOK := durable.Change()
	if err != nil || !durableOK || !deletedOK || changeOK ||
		durable.Revision() != deleted.Receipt.Revision || deletedEvent.ThreadID != forked.ThreadID {
		t.Fatalf("deleted subscription message = %#v err=%v", message, err)
	}
	if _, err := subscription.Next(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("subscription after Deleted = %v", err)
	}

	tombstoned, err := host.Thread(ctx, forked.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	tombstonedReader := mustThreadReader(t, tombstoned)
	tombstonedLifecycle := mustThreadLifecycle(t, tombstoned)
	if _, err := tombstonedReader.Snapshot(ctx); !errors.Is(err, runtime.ErrThreadDeleted) {
		t.Fatalf("tombstone snapshot = %v", err)
	}
	if _, err := tombstonedReader.Bootstrap(ctx, runtime.ThreadBootstrapRequest{TurnLimit: 20}); !errors.Is(err, runtime.ErrThreadDeleted) {
		t.Fatalf("tombstone bootstrap = %v", err)
	}
	replayedDelete, err := tombstonedLifecycle.Delete(ctx, runtime.DeleteThreadCommand{LogicalRequestID: "delete-request"})
	if err != nil || !replayedDelete.Receipt.Replayed || replayedDelete.Receipt.Revision != deleted.Receipt.Revision {
		t.Fatalf("delete replay = %#v err=%v", replayedDelete, err)
	}
	replaySubscription, err := tombstonedReader.Subscribe(ctx, runtime.SubscribeOptions{AfterRevision: view.Revision})
	if err != nil {
		t.Fatal(err)
	}
	replayedMessage, err := replaySubscription.Next(ctx)
	replayedDurable, durableOK := replayedMessage.Durable()
	_, deletedOK = replayedDurable.Deleted()
	if err != nil || !durableOK || !deletedOK {
		t.Fatalf("new subscription Deleted replay = %#v err=%v", replayedMessage, err)
	}
	eofSubscription, err := tombstonedReader.Subscribe(ctx, runtime.SubscribeOptions{AfterRevision: deleted.Receipt.Revision})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := eofSubscription.Next(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("new subscription after final revision = %v", err)
	}
}

package runtime

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/v3/observation"
	"github.com/floegence/floret/v3/tools"
)

func TestRuntimeLiveProjectionRecorderDropsRequestedEntryAfterTerminalDecision(t *testing.T) {
	t.Parallel()
	const (
		threadID = "thread-approval-monotonic"
		turnID   = "turn-approval-monotonic"
		runID    = "run-approval-monotonic"
		toolID   = "call-approval-monotonic"
	)
	startedAt := time.UnixMilli(1_786_449_218_000).UTC()
	recorder := &runtimeLiveProjectionRecorder{}
	project := func(ordinal int64, state string, eventType observation.EventType) (*ThreadTurnProjection, *ThreadTurnProjectionDelta) {
		detail := ThreadDetailEvent{
			ID: "approval-" + state, Ordinal: ordinal, ThreadID: threadID, TurnID: turnID, RunID: runID,
			Kind: ThreadDetailEventApproval, Type: string(eventType), CreatedAt: startedAt.Add(time.Duration(ordinal) * time.Millisecond),
			Approval: &ThreadDetailApproval{State: state, ToolID: toolID, ToolName: "terminal.exec"},
		}
		return recorder.projectWithDelta(Event{
			Type: observation.EventTypeThreadEntryCommitted, ThreadID: threadID, TurnID: turnID, RunID: runID,
			Committed: &detail, Timestamp: detail.CreatedAt,
		})
	}

	requested, _ := project(10, "requested", observation.EventTypeToolApprovalRequested)
	if item := runtimeProjectionToolItem(*requested, toolID); item.ApprovalState != "requested" {
		t.Fatalf("initial approval item = %#v", item)
	}
	rejected, _ := project(12, "rejected", observation.EventTypeToolApprovalRejected)
	if item := runtimeProjectionToolItem(*rejected, toolID); item.Status != observation.ActivityStatusDeclined || item.ApprovalState != "rejected" {
		t.Fatalf("terminal approval item = %#v", item)
	}

	staleProjection, staleDelta := project(11, "requested", observation.EventTypeToolApprovalRequested)
	if staleProjection != nil || staleDelta != nil {
		t.Fatalf("stale requested entry produced live output: projection=%#v delta=%#v", staleProjection, staleDelta)
	}
	state := recorder.statesByTurn[runtimeLiveProjectionTurnKey(threadID, turnID, runID)]
	if state == nil || state.lastProjection == nil || state.throughOrdinal != 12 {
		t.Fatalf("recorder state after stale entry = %#v", state)
	}
	if item := runtimeProjectionToolItem(*state.lastProjection, toolID); item.Status != observation.ActivityStatusDeclined || item.ApprovalState != "rejected" || item.RequiresApproval {
		t.Fatalf("stale requested entry regressed terminal approval = %#v", item)
	}
	sameOrdinalProjection, sameOrdinalDelta := project(12, "requested", observation.EventTypeToolApprovalRequested)
	if sameOrdinalProjection != nil || sameOrdinalDelta != nil {
		t.Fatalf("conflicting same-ordinal entry produced live output: projection=%#v delta=%#v", sameOrdinalProjection, sameOrdinalDelta)
	}
	if item := runtimeProjectionToolItem(*state.lastProjection, toolID); item.Status != observation.ActivityStatusDeclined || item.ApprovalState != "rejected" {
		t.Fatalf("same-ordinal entry replaced terminal approval = %#v", item)
	}
}

func TestProjectThreadTurnUsesCanonicalControlDisposition(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name        string
		disposition string
	}{
		{name: "ask_user", disposition: "waiting"},
		{name: "task_complete", disposition: "terminal"},
		{name: "continue", disposition: "continue"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			projection := ProjectThreadTurn(ProjectThreadTurnRequest{
				ThreadID: "thread-control", TurnID: "turn-control", RunID: "run-control", TraceID: "trace-control",
				Events: []ThreadDetailEvent{{
					ID: testCase.name, Ordinal: 1, ThreadID: "thread-control", TurnID: "turn-control", RunID: "run-control",
					Kind: ThreadDetailEventToolCall, CreatedAt: time.UnixMilli(1_786_449_220_000).UTC(),
					Message: &ThreadDetailMessage{Kind: "control_signal", Content: "signal"},
					ToolCall: &ThreadDetailToolCall{
						ID: testCase.name + "-call", Name: testCase.name,
						ControlSignal: &ThreadDetailControlSignal{Name: testCase.name, CallID: testCase.name + "-call", Disposition: testCase.disposition, Text: "signal"},
					},
				}},
			})
			var signalDisposition string
			var activityDisposition any
			for _, segment := range projection.Segments {
				if segment.Signal != nil {
					signalDisposition = segment.Signal.Disposition
				}
				if segment.ActivityTimeline != nil && len(segment.ActivityTimeline.Items) == 1 {
					activityDisposition = segment.ActivityTimeline.Items[0].Metadata["control_disposition"]
				}
			}
			if signalDisposition != testCase.disposition || activityDisposition != testCase.disposition {
				t.Fatalf("%s dispositions signal/activity=%q/%#v; projection=%#v", testCase.name, signalDisposition, activityDisposition, projection)
			}
		})
	}
}

type blockingRejectedProjectionSink struct {
	entered     chan struct{}
	release     chan struct{}
	enteredOnce sync.Once
}

func (sink *blockingRejectedProjectionSink) EmitEvent(event Event) {
	if event.Committed == nil || event.Committed.Kind != ThreadDetailEventApproval || event.Committed.Approval == nil || event.Committed.Approval.State != "rejected" {
		return
	}
	sink.enteredOnce.Do(func() { close(sink.entered) })
	<-sink.release
}

func TestResolveApprovalReceiptDoesNotWaitForRejectedProjectionSink(t *testing.T) {
	ctx := context.Background()
	registry := tools.NewRegistry()
	if err := registry.Register(tools.Define[runtimeEchoArgs](
		tools.Definition{
			Name: "write_note", InputSchema: runtimeEchoSchema(), Effects: []tools.Effect{tools.EffectWrite},
			Permission: tools.PermissionSpec{Mode: tools.PermissionAsk},
		}, nil, nil, func(context.Context, tools.Invocation[runtimeEchoArgs]) (tools.Result, error) {
			t.Fatal("rejected tool handler ran")
			return tools.Result{}, nil
		})); err != nil {
		t.Fatal(err)
	}
	gateway := runtimeModelGateway(func(_ context.Context, req modelRequest) (<-chan modelEvent, error) {
		events := make(chan modelEvent, 2)
		if req.Step == 1 {
			events <- modelEvent{Type: modelEventToolCalls, ToolCalls: []tools.ToolCall{{ID: "call-1", Name: "write_note", Args: `{"text":"notes.md"}`}}}
			events <- modelEvent{Type: modelEventDone, Reason: "tool_calls"}
		} else {
			events <- modelEvent{Type: modelEventDelta, Text: "continued"}
			events <- modelEvent{Type: modelEventDone, Reason: "stop"}
		}
		close(events)
		return events, nil
	})
	sink := &blockingRejectedProjectionSink{entered: make(chan struct{}), release: make(chan struct{})}
	defer close(sink.release)
	host, err := newTestHost(t, providerHostOptions{
		Config: runtimeGatewayConfig("test"), modelGateway: gateway,
		modelGatewayIdentity: runtimeGatewayIdentity("fake-model"), store: newMemoryStore(), Tools: registry,
		Sink: sink, IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, createThreadRequest{ThreadID: "thread-fast-reject"}); err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() {
		_, runErr := host.RunTurn(ctx, runTurnRequest{
			RunID: "run-fast-reject", ThreadID: "thread-fast-reject", TurnID: "turn-fast-reject",
			Input: TurnInput{Text: "write a note"},
		})
		runDone <- runErr
	}()
	queue := waitRuntimeApprovalQueue(t, ctx, host, "thread-fast-reject", 1)
	request := runtimeApprovalDecisionRequest(queue, queue.Items[0], "decision-fast-reject", ApprovalDecisionReject)
	type resolveOutcome struct {
		result ResolveApprovalResult
		err    error
	}
	resolved := make(chan resolveOutcome, 1)
	go func() {
		result, resolveErr := host.ResolveApproval(ctx, request)
		resolved <- resolveOutcome{result: result, err: resolveErr}
	}()
	select {
	case <-sink.entered:
	case <-time.After(time.Second):
		t.Fatal("rejected projection did not reach the barrier")
	}
	select {
	case outcome := <-resolved:
		if outcome.err != nil || outcome.result.Receipt.State != "rejected" {
			t.Fatalf("approval resolution = %#v err=%v", outcome.result, outcome.err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("approval receipt waited for the rejected projection sink")
	}
}

func TestResolveApprovalReceiptDoesNotWaitForProviderContinuation(t *testing.T) {
	ctx := context.Background()
	registry := tools.NewRegistry()
	if err := registry.Register(tools.Define[runtimeEchoArgs](
		tools.Definition{
			Name: "write_note", InputSchema: runtimeEchoSchema(), Effects: []tools.Effect{tools.EffectWrite},
			Permission: tools.PermissionSpec{Mode: tools.PermissionAsk},
		}, nil, nil, func(context.Context, tools.Invocation[runtimeEchoArgs]) (tools.Result, error) {
			t.Fatal("rejected tool handler ran")
			return tools.Result{}, nil
		})); err != nil {
		t.Fatal(err)
	}
	continuationEntered := make(chan struct{})
	releaseContinuation := make(chan struct{})
	defer close(releaseContinuation)
	var continuationOnce sync.Once
	gateway := runtimeModelGateway(func(_ context.Context, req modelRequest) (<-chan modelEvent, error) {
		if req.Step == 1 {
			events := make(chan modelEvent, 2)
			events <- modelEvent{Type: modelEventToolCalls, ToolCalls: []tools.ToolCall{{ID: "call-1", Name: "write_note", Args: `{"text":"notes.md"}`}}}
			events <- modelEvent{Type: modelEventDone, Reason: "tool_calls"}
			close(events)
			return events, nil
		}
		continuationOnce.Do(func() { close(continuationEntered) })
		<-releaseContinuation
		events := make(chan modelEvent, 1)
		events <- modelEvent{Type: modelEventDone, Reason: "stop"}
		close(events)
		return events, nil
	})
	host, err := newTestHost(t, providerHostOptions{
		Config: runtimeGatewayConfig("test"), modelGateway: gateway,
		modelGatewayIdentity: runtimeGatewayIdentity("fake-model"), store: newMemoryStore(), Tools: registry,
		IDGenerator: deterministicIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, createThreadRequest{ThreadID: "thread-fast-continuation"}); err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() {
		_, runErr := host.RunTurn(ctx, runTurnRequest{
			RunID: "run-fast-continuation", ThreadID: "thread-fast-continuation", TurnID: "turn-fast-continuation",
			Input: TurnInput{Text: "write a note"},
		})
		runDone <- runErr
	}()
	queue := waitRuntimeApprovalQueue(t, ctx, host, "thread-fast-continuation", 1)
	resolved := make(chan error, 1)
	go func() {
		_, resolveErr := host.ResolveApproval(ctx, runtimeApprovalDecisionRequest(queue, queue.Items[0], "decision-fast-continuation", ApprovalDecisionReject))
		resolved <- resolveErr
	}()
	select {
	case <-continuationEntered:
	case <-time.After(time.Second):
		t.Fatal("provider continuation did not reach the barrier")
	}
	select {
	case resolveErr := <-resolved:
		if resolveErr != nil {
			t.Fatalf("resolve approval: %v", resolveErr)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("approval receipt waited for provider continuation")
	}
}

package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
	"github.com/floegence/floret/v7/internal/sessiontree"
	internalstorage "github.com/floegence/floret/v7/internal/storage"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/tools"
)

func parentEventDone() florettest.Step {
	return florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "done"}, {Type: provider.EventDone, Reason: "stop"}}}
}

func parentEventHost(t *testing.T, enabled bool, source storage.Source) (*Host, ThreadService, *florettest.ScriptedGateway) {
	t.Helper()
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "events", StateCompatibilityKey: "test:events:v1"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, parentEventDone(), parentEventDone(), parentEventDone(), parentEventDone())
	agent, err := testAgent(gateway)
	if err != nil {
		t.Fatal(err)
	}
	host, err := Open(t.Context(), Options{Storage: source, EnableParentEvents: enabled})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
	if err != nil {
		t.Fatal(err)
	}
	return host, service, gateway
}

func parentEventChild(t *testing.T, host *Host, parent identity.ThreadID, name string) identity.ThreadID {
	t.Helper()
	// Controllable canonical child facts; no child Agent is started.
	err := host.store.repo.(*internalstorage.BackendKernel).UpdateDomain(t.Context(), func(memory *sessiontree.MemoryRepo, _ spi.WriteTx) error {
		_, err := memory.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: name, ParentThreadID: parent.String(), ParentTurnID: "delegating-turn", TaskName: name, TaskDescription: "actual objective", AgentPath: "/" + name, HostProfileRef: "test"})
		if err != nil {
			return err
		}
		_, err = memory.AcceptTurn(t.Context(), sessiontree.AcceptTurnRequest{ThreadID: name, TurnID: name + "-turn", RunID: name + "-run", LogicalRequestID: name + "-send", RequestFingerprint: name, InputRequestFingerprint: name, Input: session.Message{Role: session.User, Content: "child mission"}})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return identity.ThreadID(name)
}

func parentEventFinish(t *testing.T, host *Host, child identity.ThreadID, status sessiontree.TurnMarkerStatus) {
	t.Helper()
	_, err := host.store.repo.(*internalstorage.BackendKernel).FinishTurn(t.Context(), sessiontree.FinishTurnRequest{ThreadID: child.String(), TurnID: child.String() + "-turn", RunID: child.String() + "-run", TerminalEntryID: child.String() + "-terminal", Status: status, OutcomeFingerprint: "terminal", FailureMessage: map[bool]string{true: "RAW_ERROR_MUST_NOT_BE_IN_NOTIFICATION"}[status == sessiontree.TurnFailed], Metadata: map[string]string{sessiontree.TurnFailureCodeMetadataKey: map[bool]string{true: sessiontree.TurnFailureProvider}[status == sessiontree.TurnFailed]}, Now: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
}

func parentEventCount(request provider.Request) (int, string) {
	count := 0
	var text string
	for _, message := range request.Messages {
		if strings.Contains(message.Text, "Subagent events (runtime facts") {
			count++
			text += message.Text
		}
	}
	return count, text
}

func TestParentEventsNaturalRequestAdmissionAndLegacyOptOut(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "enabled"}[enabled], func(t *testing.T) {
			host, service, gateway := parentEventHost(t, enabled, storage.Memory())
			parent, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "parent"})
			if err != nil {
				t.Fatal(err)
			}
			child := parentEventChild(t, host, parent.ThreadID, "child")
			if _, err := host.store.repo.(*internalstorage.BackendKernel).AppendRuntimeFacts(t.Context(), child.String(), []sessiontree.Entry{{ID: "progress", ThreadID: child.String(), TurnID: "child-turn", RunID: "child-run", Type: sessiontree.EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "changing progress preview"}}}); err != nil {
				t.Fatal(err)
			}
			parentEventFinish(t, host, child, sessiontree.TurnCompleted)
			if len(gateway.Requests()) != 0 {
				t.Fatal("receiving event woke idle parent")
			}
			if _, err := service.Send(t.Context(), SendInput{ThreadID: parent.ThreadID, RequestKey: "send", Input: UserInput{Text: "review work"}}); err != nil {
				t.Fatal(err)
			}
			view := waitThreadView(t, service, parent.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
			if view.Failure != nil {
				t.Fatal(view.Failure)
			}
			requests := gateway.Requests()
			if len(requests) != 1 {
				t.Fatalf("requests=%d", len(requests))
			}
			count, text := parentEventCount(requests[0])
			if enabled && count != 1 || !enabled && count != 0 {
				t.Fatalf("count=%d text=%s", count, text)
			}
			if strings.Contains(text, "changing progress") || strings.Contains(text, "actual objective") {
				t.Fatal("dynamic overview was injected")
			}
			if enabled {
				page, err := service.(ThreadParentEventService).ListParentEvents(t.Context(), ListParentEventsInput{ThreadID: parent.ThreadID})
				if err != nil || len(page.Events) != 1 || !page.Events[0].Admitted || page.Events[0].Kind != "completed" {
					t.Fatalf("page=%+v err=%v", page, err)
				}
			}
			entries, _ := host.store.repo.Entries(t.Context(), parent.ThreadID.String())
			admissions := 0
			for _, entry := range entries {
				if entry.Type == sessiontree.EntryParentEventsAdmitted {
					admissions++
				}
			}
			if enabled && admissions != 1 {
				t.Fatalf("admissions=%d", admissions)
			}
			bytes, _ := json.Marshal(requests[0])
			t.Logf("events=%d estimated_incremental_tokens=%d serialized_request_bytes=%d estimator=contextpolicy.EstimateTextTokens+8", count, contextpolicy.EstimateTextTokens(text)+int64(count*8), len(bytes))
		})
	}
}

func TestParentEventsFailureReasonDoesNotExposeRawError(t *testing.T) {
	host, service, gateway := parentEventHost(t, true, storage.Memory())
	parent, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "parent"})
	if err != nil {
		t.Fatal(err)
	}
	child := parentEventChild(t, host, parent.ThreadID, "child")
	parentEventFinish(t, host, child, sessiontree.TurnFailed)
	if _, err := service.Send(t.Context(), SendInput{ThreadID: parent.ThreadID, RequestKey: "send", Input: UserInput{Text: "review"}}); err != nil {
		t.Fatal(err)
	}
	waitThreadView(t, service, parent.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	_, text := parentEventCount(gateway.Requests()[0])
	if !strings.Contains(text, "provider") || strings.Contains(text, "RAW_ERROR") {
		t.Fatal(text)
	}
}

func parentEventReport(t *testing.T, host *Host, child identity.ThreadID, callID, message string) ReportToParentInput {
	t.Helper()
	kernel := host.store.repo.(*internalstorage.BackendKernel)
	args, _ := json.Marshal(map[string]string{"message": message})
	inv := sessiontree.EffectInvocationIdentity{ThreadID: child.String(), TurnID: child.String() + "-turn", RunID: child.String() + "-run", ToolCallID: callID, ToolName: "report_to_parent", ArgumentHash: sessiontree.StableHash(string(args))}
	if _, err := kernel.AppendRuntimeFacts(t.Context(), child.String(), []sessiontree.Entry{{ID: "call:" + callID, ThreadID: child.String(), TurnID: inv.TurnID, RunID: inv.RunID, Type: sessiontree.EntryToolCall, Message: session.Message{Role: session.Assistant, Content: "tool_call", ToolName: inv.ToolName, ToolCallID: inv.ToolCallID, ToolArgs: string(args)}}}); err != nil {
		t.Fatal(err)
	}
	prepared, err := kernel.PrepareEffectAttempt(t.Context(), sessiontree.PrepareEffectAttemptRequest{Invocation: inv, RequestFingerprint: "report:" + callID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kernel.BeginEffectDispatch(t.Context(), sessiontree.BeginEffectDispatchRequest{EffectAttemptID: prepared.Attempt.EffectAttemptID, RequestFingerprint: "report:" + callID, AuthorizationProofHash: "authorized"}); err != nil {
		t.Fatal(err)
	}
	return ReportToParentInput{ThreadID: child, TurnID: identity.TurnID(inv.TurnID), RunID: identity.RunID(inv.RunID), ToolCallID: callID, ToolName: inv.ToolName, ArgumentHash: inv.ArgumentHash, RequestKey: RequestKey(callID), Message: message}
}

func TestParentEventsReportAuthorizationReadPaginationAndOverflow(t *testing.T) {
	host, service, gateway := parentEventHost(t, true, storage.Memory())
	parent, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "parent"})
	if err != nil {
		t.Fatal(err)
	}
	events := service.(ThreadParentEventService)
	child := parentEventChild(t, host, parent.ThreadID, "child")
	report := parentEventReport(t, host, child, "first", strings.Repeat("界", 2000))
	accepted, err := events.ReportToParent(t.Context(), report)
	if err != nil || !accepted.Accepted {
		t.Fatalf("receipt=%+v err=%v", accepted, err)
	}
	replay, err := events.ReportToParent(t.Context(), report)
	if err != nil || replay != accepted {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	report.Message = "conflicting body"
	if _, err := events.ReportToParent(t.Context(), report); err == nil {
		t.Fatal("conflicting content accepted")
	}
	report.RequestKey = "fresh"
	report.ArgumentHash = "forged"
	if _, err := events.ReportToParent(t.Context(), report); err == nil {
		t.Fatal("forged invocation accepted")
	}
	for _, message := range []string{"", "   ", string([]byte{255}), strings.Repeat("x", 8193)} {
		report.Message = message
		if _, err := events.ReportToParent(t.Context(), report); err == nil {
			t.Fatal("invalid body accepted")
		}
	}
	for i := 0; i < 25; i++ {
		id := fmt.Sprintf("report-%02d", i)
		in := parentEventReport(t, host, child, id, strings.Repeat("z", 512))
		if _, err := events.ReportToParent(t.Context(), in); err != nil {
			t.Fatal(err)
		}
	}
	if len(gateway.Requests()) != 0 {
		t.Fatal("report woke parent")
	}
	page, err := events.ListParentEvents(t.Context(), ListParentEventsInput{ThreadID: parent.ThreadID, Limit: 3})
	if err != nil || len(page.Events) != 3 || page.NextAfterEventID == "" {
		t.Fatalf("page=%+v err=%v", page, err)
	}
	full, err := events.ReadParentEvents(t.Context(), ReadParentEventsInput{ThreadID: parent.ThreadID, EventID: accepted.EventID})
	if err != nil || len(full.Events) != 1 || full.Events[0].Message != strings.Repeat("界", 2000) {
		t.Fatalf("full report lost: %v", err)
	}
	count := 0
	after := ""
	for {
		p, err := events.ReadParentEvents(t.Context(), ReadParentEventsInput{ThreadID: parent.ThreadID, FirstEventID: page.FirstEventID, LastEventID: page.LastEventID, AfterEventID: after, Limit: 3})
		if err != nil {
			t.Fatal(err)
		}
		count += len(p.Events)
		if p.NextAfterEventID == "" {
			break
		}
		after = p.NextAfterEventID
	}
	if count != 26 {
		t.Fatalf("range count=%d", count)
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: parent.ThreadID, RequestKey: "send", Input: UserInput{Text: "review"}}); err != nil {
		t.Fatal(err)
	}
	view := waitThreadView(t, service, parent.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if view.Failure != nil {
		t.Fatal(view.Failure)
	}
	count, text := parentEventCount(gateway.Requests()[0])
	if count != 1 || len(text) > 8192 || contextpolicy.EstimateTextTokens(text)+8 > 2048 || !strings.Contains(text, "Fixed event range:") {
		t.Fatalf("invalid batch count=%d bytes=%d tokens=%d text=%s", count, len(text), contextpolicy.EstimateTextTokens(text), text)
	}
	pending, err := events.ListParentEvents(t.Context(), ListParentEventsInput{ThreadID: parent.ThreadID, PendingOnly: true})
	if err != nil || len(pending.Events) != 0 {
		t.Fatalf("admitted backlog remains pending: %+v %v", pending, err)
	}
	if view.ParentEvents == nil || len(view.ParentEvents.Events) == 0 {
		t.Fatal("receipt view missing")
	}
	view.ParentEvents.Events[0].Admitted = false
	detached, _ := service.View(t.Context(), parent.ThreadID)
	if !detached.ParentEvents.Events[0].Admitted {
		t.Fatal("caller mutated admitted presentation")
	}

}

func TestParentEventsSQLiteRestartForkAndNoRepeatedAdmission(t *testing.T) {
	path := t.TempDir() + "/events.sqlite"
	host, service, _ := parentEventHost(t, true, storage.SQLite(path))
	parent, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "parent"})
	if err != nil {
		t.Fatal(err)
	}
	child := parentEventChild(t, host, parent.ThreadID, "child")
	parentEventFinish(t, host, child, sessiontree.TurnCompleted)
	if err := host.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	host, service, gateway := parentEventHost(t, true, storage.SQLite(path))
	events := service.(ThreadParentEventService)
	fork, err := service.Fork(t.Context(), ForkThreadInput{SourceThreadID: parent.ThreadID, RequestKey: "fork"})
	if err != nil {
		t.Fatal(err)
	}
	other := parentEventChild(t, host, parent.ThreadID, "later")
	parentEventFinish(t, host, other, sessiontree.TurnCompleted)
	inherited, err := events.ListParentEvents(t.Context(), ListParentEventsInput{ThreadID: fork.ThreadID})
	if err != nil || len(inherited.Events) != 1 {
		t.Fatalf("fork=%+v %v", inherited, err)
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: parent.ThreadID, RequestKey: "send", Input: UserInput{Text: "review"}}); err != nil {
		t.Fatal(err)
	}
	waitThreadView(t, service, parent.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	waitThreadExecutionDone(t, service.(*threadRuntimeService), parent.ThreadID)
	if _, err := service.Send(t.Context(), SendInput{ThreadID: parent.ThreadID, RequestKey: "next", Input: UserInput{Text: "continue"}}); err != nil {
		t.Fatal(err)
	}
	waitThreadView(t, service, parent.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if len(gateway.Requests()) != 2 {
		t.Fatal("unexpected provider sampling")
	}
	entries, _ := host.store.repo.Entries(t.Context(), parent.ThreadID.String())
	admissions := 0
	for _, entry := range entries {
		if entry.Type == sessiontree.EntryParentEventsAdmitted {
			admissions++
		}
	}
	if admissions != 1 {
		t.Fatalf("admissions=%d", admissions)
	}
}

func parentEventHostWithGateway(t *testing.T, gateway provider.Gateway, configured ...AgentOption) (*Host, ThreadService) {
	t.Helper()
	agent, err := testAgent(gateway, configured...)
	if err != nil {
		t.Fatal(err)
	}
	host, err := Open(t.Context(), Options{Storage: storage.Memory(), EnableParentEvents: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
	if err != nil {
		t.Fatal(err)
	}
	return host, service
}

func TestParentEventsDuringProviderWaitRemainPassiveAtNaturalEnd(t *testing.T) {
	release := make(chan struct{})
	step := parentEventDone()
	step.BlockUntil = release
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "events", StateCompatibilityKey: "test:events"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, step, parentEventDone())
	host, service := parentEventHostWithGateway(t, gateway)
	parent, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	child := parentEventChild(t, host, parent.ThreadID, "child")
	started, err := service.Send(t.Context(), SendInput{ThreadID: parent.ThreadID, RequestKey: "first", Input: UserInput{Text: "work"}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := gateway.WaitForRequests(ctx, 1); err != nil {
		t.Fatal(err)
	}
	subscription, err := service.Subscribe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	parentEventFinish(t, host, child, sessiontree.TurnCompleted)
	for {
		view, err := subscription.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if view.ThreadID == parent.ThreadID && view.ParentEvents != nil && len(view.ParentEvents.Events) == 1 {
			if view.Activity != ThreadActivityActive || view.RunID != started.RunID || view.ParentEvents.Events[0].Admitted {
				t.Fatalf("event altered execution: %+v", view)
			}
			break
		}
	}
	if len(gateway.Requests()) != 1 {
		t.Fatal("receiving event resampled provider")
	}
	close(release)
	waitThreadView(t, service, parent.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	waitThreadExecutionDone(t, service.(*threadRuntimeService), parent.ThreadID)
	pending, err := service.(ThreadParentEventService).ListParentEvents(ctx, ListParentEventsInput{ThreadID: parent.ThreadID, PendingOnly: true})
	if err != nil || len(pending.Events) != 1 {
		t.Fatalf("pending=%+v %v", pending, err)
	}
	if n, _ := parentEventCount(gateway.Requests()[0]); n != 0 {
		t.Fatal("frozen request was mutated")
	}
	if _, err := service.Send(ctx, SendInput{ThreadID: parent.ThreadID, RequestKey: "next", Input: UserInput{Text: "review"}}); err != nil {
		t.Fatal(err)
	}
	waitThreadView(t, service, parent.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if len(gateway.Requests()) != 2 {
		t.Fatal("unexpected extra request")
	}
	if n, _ := parentEventCount(gateway.Requests()[1]); n != 1 {
		t.Fatal("next natural request missed event")
	}
}

func TestParentEventsDuringToolDoNotInterruptAndFollowCompletePair(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	work := tools.Define[map[string]any](tools.Definition{Name: "work", InputSchema: tools.StrictObject(nil, nil), Effects: []tools.Effect{tools.EffectRead}, Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, nil, nil, func(ctx context.Context, _ tools.Invocation[map[string]any]) (tools.Result, error) {
		close(started)
		select {
		case <-ctx.Done():
			return tools.Result{}, ctx.Err()
		case <-release:
			return tools.Result{Text: "COMPLETE_TOOL_RESULT"}, nil
		}
	})
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "events", StateCompatibilityKey: "test:events"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "work-call", Name: "work", Args: "{}"}}}, {Type: provider.EventDone, Reason: "tool_calls"}}}, parentEventDone())
	host, service := parentEventHostWithGateway(t, gateway, WithAgentTools(work), WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, req EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
		return effect(ctx, EffectAuthorizationProof{EffectAttemptID: req.EffectAttemptID, RequestFingerprint: req.RequestFingerprint, ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, ToolCallID: req.ToolCallID, PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now()})
	})))
	parent, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	child := parentEventChild(t, host, parent.ThreadID, "child")
	if _, err := service.Send(t.Context(), SendInput{ThreadID: parent.ThreadID, RequestKey: "send", Input: UserInput{Text: "work"}}); err != nil {
		t.Fatal(err)
	}
	waitClosed(t, started, "tool never dispatched")
	parentEventFinish(t, host, child, sessiontree.TurnFailed)
	if len(gateway.Requests()) != 1 {
		t.Fatal("event interrupted tool")
	}
	close(release)
	view := waitThreadView(t, service, parent.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if view.Failure != nil {
		t.Fatal(view.Failure)
	}
	requests := gateway.Requests()
	if len(requests) != 2 {
		t.Fatalf("requests=%d", len(requests))
	}
	_, text := parentEventCount(requests[1])
	if !strings.Contains(text, "failed") {
		t.Fatal(text)
	}
	resultIndex, eventIndex := -1, -1
	for i, m := range requests[1].Messages {
		if m.ToolResult != nil && strings.Contains(m.ToolResult.Text, "COMPLETE_TOOL_RESULT") {
			resultIndex = i
		}
		if strings.Contains(m.Text, "Subagent events (") {
			eventIndex = i
		}
	}
	if resultIndex < 0 || eventIndex <= resultIndex {
		t.Fatalf("event must follow paired result: result=%d event=%d", resultIndex, eventIndex)
	}
}

func parentEventAsk(t *testing.T, host *Host, child identity.ThreadID, id string, kind ThreadInteractionKind) {
	t.Helper()
	interaction := ThreadInteraction{ID: id, TurnID: identity.TurnID(child.String() + "-turn"), RunID: identity.RunID(child.String() + "-run"), Kind: kind}
	payload, _ := json.Marshal(interaction)
	_, err := host.store.repo.(*internalstorage.BackendKernel).AppendRuntimeFacts(t.Context(), child.String(), []sessiontree.Entry{{ID: "interaction-requested:" + id, ThreadID: child.String(), TurnID: interaction.TurnID.String(), RunID: interaction.RunID.String(), Type: sessiontree.EntryInteractionAsked, Payload: payload}})
	if err != nil {
		t.Fatal(err)
	}
}

func TestParentEventsInteractionDedupResolutionAndNoApprovalLeak(t *testing.T) {
	host, service, gateway := parentEventHost(t, true, storage.Memory())
	parent, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	child := parentEventChild(t, host, parent.ThreadID, "child")
	parentEventAsk(t, host, child, "approval", ThreadInteractionApproval)
	parentEventAsk(t, host, child, "input", ThreadInteractionInput)
	// Stable repeated source publication must not create another receipt.
	parentEventAsk(t, host, child, "approval", ThreadInteractionApproval)
	kernel := host.store.repo.(*internalstorage.BackendKernel)
	_, err = kernel.AppendRuntimeFacts(t.Context(), child.String(), []sessiontree.Entry{{ID: "interaction-resolved:approval", ThreadID: child.String(), TurnID: "child-turn", RunID: "child-run", Type: sessiontree.EntryInteractionDone, Payload: json.RawMessage("{\"accepted\":true,\"approved\":true}")}})
	if err != nil {
		t.Fatal(err)
	}
	page, err := service.(ThreadParentEventService).ListParentEvents(t.Context(), ListParentEventsInput{ThreadID: parent.ThreadID})
	if err != nil || len(page.Events) != 2 {
		t.Fatalf("page=%+v %v", page, err)
	}
	if !page.Events[0].Resolved || page.Events[1].Resolved {
		t.Fatal("interaction readiness changed")
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: parent.ThreadID, RequestKey: "send", Input: UserInput{Text: "review"}}); err != nil {
		t.Fatal(err)
	}
	waitThreadView(t, service, parent.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	_, text := parentEventCount(gateway.Requests()[0])
	if !strings.Contains(text, "input_required") || strings.Contains(text, "approval_required") || !strings.Contains(text, "interaction_resolved") {
		t.Fatal(text)
	}
	entries, _ := kernel.Entries(t.Context(), child.String())
	pending := 0
	for _, e := range entries {
		if e.Type == sessiontree.EntryInteractionDone {
			pending++
		}
	}
	if pending != 1 {
		t.Fatal("notification answered child input")
	}
}

func TestParentEventsAdmissionSurvivesPreparationFailureAndRetry(t *testing.T) {
	host, service, gateway := parentEventHost(t, true, storage.Memory())
	parent, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	child := parentEventChild(t, host, parent.ThreadID, "child")
	parentEventFinish(t, host, child, sessiontree.TurnCompleted)
	// Freeze canonical admission, then simulate failure before provider preparation.
	kernel := host.store.repo.(*internalstorage.BackendKernel)
	accepted, err := kernel.AcceptTurn(t.Context(), sessiontree.AcceptTurnRequest{ThreadID: parent.ThreadID.String(), TurnID: "failed-turn", RunID: "failed-run", LogicalRequestID: "failed-send", RequestFingerprint: "failed", InputRequestFingerprint: "failed", Input: session.Message{Role: session.User, Content: "review work"}})
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := kernel.AdmitParentEvents(t.Context(), parent.ThreadID.String(), "failed-turn", "failed-run")
	if err != nil || admitted.ID == "" {
		t.Fatalf("admit=%+v %v", admitted, err)
	}
	_, err = kernel.FinishTurn(t.Context(), sessiontree.FinishTurnRequest{ThreadID: parent.ThreadID.String(), TurnID: "failed-turn", RunID: "failed-run", TerminalEntryID: "failed-terminal", OutcomeFingerprint: "failed", Status: sessiontree.TurnFailed, FailureMessage: "preparation unavailable", Metadata: map[string]string{sessiontree.TurnFailureCodeMetadataKey: sessiontree.TurnFailureContextFixedOverhead}})
	if err != nil {
		t.Fatal(err)
	}
	if len(gateway.Requests()) != 0 {
		t.Fatal("fixture called provider")
	}
	if _, err := service.Retry(t.Context(), RetryInput{ThreadID: parent.ThreadID, SourceTurnID: identity.TurnID(accepted.TurnStarted.TurnID), RequestKey: "retry"}); err != nil {
		t.Fatal(err)
	}
	final := waitThreadView(t, service, parent.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if final.Failure != nil {
		t.Fatal(final.Failure)
	}
	if len(gateway.Requests()) != 1 {
		t.Fatal("unexpected provider count")
	}
	if n, text := parentEventCount(gateway.Requests()[0]); n != 1 {
		t.Fatalf("retry lost admission: %d %s", n, text)
	}
	page, err := service.(ThreadParentEventService).ListParentEvents(t.Context(), ListParentEventsInput{ThreadID: parent.ThreadID})
	if err != nil || len(page.Events) != 1 || !page.Events[0].Admitted {
		t.Fatalf("page=%+v %v", page, err)
	}
}

func TestParentEventsStopDoesNotAdmitOrWake(t *testing.T) {
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "events", StateCompatibilityKey: "test:events"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, florettest.Step{WaitForCancellation: true}, parentEventDone())
	host, service := parentEventHostWithGateway(t, gateway)
	parent, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	child := parentEventChild(t, host, parent.ThreadID, "child")
	if _, err := service.Send(t.Context(), SendInput{ThreadID: parent.ThreadID, RequestKey: "first", Input: UserInput{Text: "work"}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := gateway.WaitForRequests(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Cancel(ctx, CancelInput{ThreadID: parent.ThreadID, RequestKey: "stop"}); err != nil {
		t.Fatal(err)
	}
	waitThreadExecutionDone(t, service.(*threadRuntimeService), parent.ThreadID)
	parentEventFinish(t, host, child, sessiontree.TurnCompleted)
	page, err := service.(ThreadParentEventService).ListParentEvents(ctx, ListParentEventsInput{ThreadID: parent.ThreadID, PendingOnly: true})
	if err != nil || len(page.Events) != 1 {
		t.Fatalf("page=%+v %v", page, err)
	}
	if len(gateway.Requests()) != 1 {
		t.Fatal("stopped parent was restarted")
	}
}

func TestParentEventsCompactionKeepsCanonicalReceiptAndNoReadmission(t *testing.T) {
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "compact", StateCompatibilityKey: "test:compact"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, compactionTestStep("ack"), compactionTestStep("ack"), compactionTestStep("ack"), compactionTestStep("The subagent result remains available through its canonical event reference."), compactionTestStep("reviewed"))
	agent := compactionTestAgent(t, gateway)
	host, err := Open(t.Context(), Options{Storage: storage.SQLite(t.TempDir() + "/events.db"), EnableParentEvents: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
	if err != nil {
		t.Fatal(err)
	}
	parent, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	child := parentEventChild(t, host, parent.ThreadID, "child")
	parentEventFinish(t, host, child, sessiontree.TurnCompleted)
	seedCompactionHistory(t, service, parent.ThreadID)
	waitThreadExecutionDone(t, service.(*threadRuntimeService), parent.ThreadID)
	page, err := service.(ThreadParentEventService).ListParentEvents(t.Context(), ListParentEventsInput{ThreadID: parent.ThreadID})
	if err != nil || len(page.Events) != 1 || !page.Events[0].Admitted {
		t.Fatalf("page=%+v %v", page, err)
	}
	admitted, err := service.(ThreadContextCompactor).CompactContext(t.Context(), CompactContextInput{ThreadID: parent.ThreadID, RequestKey: "compact"})
	if err != nil {
		t.Fatal(err)
	}
	final := waitThreadView(t, service, parent.ThreadID, func(v ThreadView) bool {
		return v.TurnID == admitted.TurnID && v.Activity == ThreadActivityIdle && v.LastOutcome != nil
	})
	if final.Failure != nil {
		t.Fatal(final.Failure)
	}
	waitThreadExecutionDone(t, service.(*threadRuntimeService), parent.ThreadID)
	read, err := service.(ThreadParentEventService).ReadParentEvents(t.Context(), ReadParentEventsInput{ThreadID: parent.ThreadID, EventID: page.Events[0].ID})
	if err != nil || len(read.Events) != 1 || !read.Events[0].Admitted {
		t.Fatal("compaction lost canonical receipt")
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: parent.ThreadID, RequestKey: "after", Input: UserInput{Text: "review"}}); err != nil {
		t.Fatal(err)
	}
	waitThreadView(t, service, parent.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	entries, _ := host.store.repo.Entries(t.Context(), parent.ThreadID.String())
	n := 0
	for _, e := range entries {
		if e.Type == sessiontree.EntryParentEventsAdmitted {
			n++
		}
	}
	if n != 1 || len(gateway.Requests()) != 5 {
		t.Fatalf("admissions=%d requests=%d", n, len(gateway.Requests()))
	}
}

func TestParentEventsHumanWaitRetainsEventUntilExplicitResponse(t *testing.T) {
	args := "{\"reason_code\":\"missing_external_input\",\"required_from_user\":[\"scope\"],\"evidence_refs\":[],\"questions\":[{\"id\":\"scope\",\"header\":\"Scope\",\"question\":\"Which checks?\",\"is_secret\":false,\"response_mode\":\"write\"}]}"
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "events", StateCompatibilityKey: "test:events"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "ask", Name: "ask_user", Args: args}}}, {Type: provider.EventDone, Reason: "tool_calls"}}}, parentEventDone())
	host, service := parentEventHostWithGateway(t, gateway)
	parent, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	child := parentEventChild(t, host, parent.ThreadID, "child")
	if _, err := service.Send(t.Context(), SendInput{ThreadID: parent.ThreadID, RequestKey: "first", Input: UserInput{Text: "work"}}); err != nil {
		t.Fatal(err)
	}
	waitThreadView(t, service, parent.ThreadID, func(v ThreadView) bool { return len(v.Interactions) == 1 })
	waitThreadExecutionDone(t, service.(*threadRuntimeService), parent.ThreadID)
	parentEventFinish(t, host, child, sessiontree.TurnCompleted)
	page, err := service.(ThreadParentEventService).ListParentEvents(t.Context(), ListParentEventsInput{ThreadID: parent.ThreadID, PendingOnly: true})
	if err != nil || len(page.Events) != 1 {
		t.Fatal("event lost while parent waiting")
	}
	view, err := service.View(t.Context(), parent.ThreadID)
	if err != nil || len(view.Interactions) != 1 || view.Interactions[0].Resolved || len(gateway.Requests()) != 1 {
		t.Fatalf("event changed human wait: %+v %v", view, err)
	}
	if _, err := service.Respond(t.Context(), RespondInput{ThreadID: parent.ThreadID, InteractionID: "ask", Answers: []InteractionAnswer{{Input: map[string]string{"scope": "all"}}}, RequestKey: "respond"}); err != nil {
		t.Fatal(err)
	}
	waitThreadView(t, service, parent.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if len(gateway.Requests()) != 2 {
		t.Fatal("unexpected sampling")
	}
	if n, text := parentEventCount(gateway.Requests()[1]); n != 1 {
		t.Fatalf("event lost after response: %d %s", n, text)
	}
}

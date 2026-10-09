package runtime

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
)

func TestIndependentThreadCompactionEmptyHistoryNeverCallsProvider(t *testing.T) {
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "compact", StateCompatibilityKey: "test:compact"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported})
	agent, err := testAgent(gateway)
	if err != nil {
		t.Fatal(err)
	}
	_, service := testThreadServiceWithAgent(t, agent)
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	compactor := service.(ThreadContextCompactor)
	accepted, err := compactor.CompactContext(t.Context(), CompactContextInput{ThreadID: created.ThreadID, RequestKey: "compact"})
	if err != nil {
		t.Fatal(err)
	}
	if accepted.TurnKind != TurnKindContextCompaction || accepted.Activity != ThreadActivityActive || len(accepted.Items) != 0 {
		t.Fatalf("admission=%+v", accepted)
	}
	terminal := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if *terminal.LastOutcome != TurnOutcomeCompleted || terminal.Failure != nil || len(terminal.Items) != 0 {
		t.Fatalf("compaction=%+v", terminal)
	}
	if got := len(gateway.Requests()); got != 0 {
		t.Fatalf("provider requests=%d, want 0", got)
	}
	snapshot, err := service.(ThreadContextReader).Context(t.Context(), created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Compactions) != 1 || snapshot.Compactions[0].Status != "noop" || snapshot.Compactions[0].AfterItemID != "" {
		t.Fatalf("snapshot=%+v", snapshot)
	}
	replay, err := compactor.CompactContext(context.Background(), CompactContextInput{ThreadID: created.ThreadID, RequestKey: "compact"})
	if err != nil || replay.TurnID != terminal.TurnID {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "compact", Input: UserInput{Text: "hello"}}); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("cross-kind replay=%v", err)
	}
	if _, err := service.Retry(t.Context(), RetryInput{ThreadID: created.ThreadID, SourceTurnID: terminal.TurnID, RequestKey: "retry"}); !errors.Is(err, ErrNoRetryTarget) {
		t.Fatalf("retry=%v", err)
	}
}

func compactionTestStep(text string) florettest.Step {
	return florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: text}, {Type: provider.EventDone, ResponseState: &provider.State{Kind: "test", ID: "continuation"}, Usage: provider.Usage{InputTokens: 9000, OutputTokens: 10, Available: true}}}}
}

func compactionTestAgent(t *testing.T, gateway provider.Gateway) *Agent {
	t.Helper()
	agent, err := NewAgent(config.AgentConfig{Profile: config.AgentProfile{ID: "compact", Name: "Compact"}, SystemPrompt: "Follow the task.", Context: config.ContextPolicy{
		ContextWindowTokens: 256000, CompactedContextTargetTokens: 2000, RecentTailTokens: 100, RecentUserTokens: 100, ReservedSummaryTokens: 1000,
	}}, gateway)
	if err != nil {
		t.Fatal(err)
	}
	return agent
}

func seedCompactionHistory(t *testing.T, service ThreadService, threadID identity.ThreadID) ThreadView {
	t.Helper()
	var view ThreadView
	for i := 0; i < 3; i++ {
		text := strings.Repeat("historical context filler ", 500)
		if i == 0 {
			text = "Earliest fact: launch code is CERULEAN-739. " + text
		}
		admission, err := service.Send(t.Context(), SendInput{ThreadID: threadID, RequestKey: RequestKey(fmt.Sprintf("seed-%d", i)), Input: UserInput{Text: text, Context: []MessageContextItem{{Kind: "runtime", Title: "Working directory", Text: "/work/first"}}}})
		if err != nil {
			t.Fatal(err)
		}
		view = waitThreadView(t, service, threadID, func(v ThreadView) bool {
			return v.TurnID == admission.TurnID && v.Activity == ThreadActivityIdle && v.LastOutcome != nil
		})
		if view.Failure != nil {
			t.Fatal(view.Failure)
		}
	}
	return view
}

func TestIndependentThreadCompactionInstallsOnceAndInvalidatesContinuation(t *testing.T) {
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "compact", StateCompatibilityKey: "test:compact"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		compactionTestStep("Seed acknowledged."), compactionTestStep("Seed acknowledged."), compactionTestStep("Seed acknowledged."), compactionTestStep("The launch code is CERULEAN-739. Work in /work/first. All previous filler was acknowledged."), compactionTestStep("CERULEAN-739"))
	path := filepath.Join(t.TempDir(), "threads.sqlite")
	open := func() (*Host, ThreadService) {
		host, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
		if err != nil {
			t.Fatal(err)
		}
		service, err := host.ThreadService(AgentFactoryFunc(func(_ context.Context, req AgentRequest) (*Agent, error) {
			if req.TurnKind == TurnKindContextCompaction && (req.Input.Text != "" || len(req.Input.Context) != 0) {
				t.Error("compaction factory received fabricated input")
			}
			return compactionTestAgent(t, gateway), nil
		}))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
		return host, service
	}
	host, service := open()
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	before := seedCompactionHistory(t, service, created.ThreadID)
	oldState, err := host.store.repo.(sessiontree.ProviderStateReader).ProviderState(t.Context(), created.ThreadID.String())
	if err != nil || oldState.State.ID == "" {
		t.Fatalf("missing initial continuation: %v", err)
	}
	admitted, err := service.(ThreadContextCompactor).CompactContext(t.Context(), CompactContextInput{ThreadID: created.ThreadID, RequestKey: "compact"})
	if err != nil {
		t.Fatal(err)
	}
	after := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool {
		return v.TurnID == admitted.TurnID && v.Activity == ThreadActivityIdle && v.LastOutcome != nil
	})
	if after.Failure != nil || *after.LastOutcome != TurnOutcomeCompleted || len(after.Items) != len(before.Items) {
		t.Fatalf("manual result=%+v", after)
	}
	if len(gateway.Requests()) != 4 {
		t.Fatalf("manual requested ordinary continuation: %d requests", len(gateway.Requests()))
	}
	if len(gateway.Requests()[3].Tools) != 0 {
		t.Fatal("summary request exposed executable tools")
	}
	if _, err := host.store.repo.(sessiontree.ProviderStateReader).ProviderState(t.Context(), created.ThreadID.String()); err == nil {
		t.Fatal("installed summary retained old continuation")
	}
	ctxSnapshot, err := service.(ThreadContextReader).Context(t.Context(), created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	if len(ctxSnapshot.Compactions) != 1 || ctxSnapshot.Compactions[0].Status != "compacted" || ctxSnapshot.Compactions[0].AfterItemID != before.Items[len(before.Items)-1].ID {
		t.Fatalf("compactions=%+v", ctxSnapshot.Compactions)
	}
	if ctxSnapshot.ContextUsage == nil || ctxSnapshot.ContextUsage.Confirmed != nil || ctxSnapshot.ContextUsage.Estimate == nil {
		t.Fatalf("usage=%+v", ctxSnapshot.ContextUsage)
	}
	entries, err := host.store.repo.Path(t.Context(), created.ThreadID.String(), "")
	if err != nil {
		t.Fatal(err)
	}
	messages, err := sessiontree.BuildContextChecked(entries, sessiontree.ContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if messages[0].Kind != session.MessageKindCompactionSummary || !strings.Contains(messages[0].Content, "CERULEAN-739") {
		t.Fatal("earliest fact missing from active summary")
	}
	for _, message := range messages[1:] {
		if strings.Contains(message.Content, "CERULEAN-739") {
			t.Fatal("earliest source remains in retained tail")
		}
	}
	if err := host.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, service = open()
	replay, err := service.(ThreadContextCompactor).CompactContext(t.Context(), CompactContextInput{ThreadID: created.ThreadID, RequestKey: "compact"})
	if err != nil || replay.TurnID != admitted.TurnID || len(gateway.Requests()) != 4 {
		t.Fatalf("restart replay=%+v err=%v", replay, err)
	}
	follow, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "follow", Input: UserInput{Text: "What was the launch code?"}})
	if err != nil {
		t.Fatal(err)
	}
	latest := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool {
		return v.TurnID == follow.TurnID && v.Activity == ThreadActivityIdle && v.LastOutcome != nil
	})
	if latest.Failure != nil {
		t.Fatal(latest.Failure)
	}
	request := gateway.Requests()[4]
	if request.PreviousState != nil {
		t.Fatal("next provider request reused pre-compaction state")
	}
	replay, err = service.(ThreadContextCompactor).CompactContext(t.Context(), CompactContextInput{ThreadID: created.ThreadID, RequestKey: "compact"})
	if err != nil || replay.TurnID != latest.TurnID || len(gateway.Requests()) != 5 {
		t.Fatal("old request replay rolled view back or executed")
	}
}

func TestIndependentThreadCompactionCancellationPreservesContext(t *testing.T) {
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "compact", StateCompatibilityKey: "test:compact"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, compactionTestStep("ack"), compactionTestStep("ack"), compactionTestStep("ack"), florettest.Step{WaitForCancellation: true})
	host, service := testThreadServiceWithAgent(t, compactionTestAgent(t, gateway))
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	before := seedCompactionHistory(t, service, created.ThreadID)
	admission, err := service.(ThreadContextCompactor).CompactContext(t.Context(), CompactContextInput{ThreadID: created.ThreadID, RequestKey: "compact"})
	if err != nil {
		t.Fatal(err)
	}
	deadline, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := gateway.WaitForRequests(deadline, 4); err != nil {
		t.Fatal(err)
	}
	if _, err := service.(ThreadContextCompactor).CompactContext(t.Context(), CompactContextInput{ThreadID: created.ThreadID, RequestKey: "busy"}); !errors.Is(err, ErrThreadBusy) {
		t.Fatalf("busy=%v", err)
	}
	if _, err := service.Cancel(t.Context(), CancelInput{ThreadID: created.ThreadID, RequestKey: "stop"}); err != nil {
		t.Fatal(err)
	}
	after := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if after.TurnID != admission.TurnID || *after.LastOutcome != TurnOutcomeCancelled || len(after.Items) != len(before.Items) {
		t.Fatalf("cancel=%+v", after)
	}
	snapshot, err := service.(ThreadContextReader).Context(t.Context(), created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Compactions) != 1 || snapshot.Compactions[0].Status != "cancelled" {
		t.Fatalf("cancel lifecycle=%+v", snapshot.Compactions)
	}
	state, err := host.store.repo.(sessiontree.ProviderStateReader).ProviderState(t.Context(), created.ThreadID.String())
	if err != nil || state.State.ID != "continuation" {
		t.Fatalf("cancel changed continuation: %v", err)
	}
	entries, err := host.store.repo.Entries(t.Context(), created.ThreadID.String())
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Type == sessiontree.EntryCompaction {
			t.Fatal("cancel installed summary")
		}
	}
}

func TestIndependentThreadCompactionFailuresKeepOriginalContextAndBoundRetries(t *testing.T) {
	for _, scenario := range []struct {
		name         string
		steps        []florettest.Step
		wantRequests int
	}{
		{name: "provider_error", steps: []florettest.Step{{ReturnError: errors.New("summary unavailable")}}, wantRequests: 4},
		{name: "stream_error", steps: []florettest.Step{{Events: []provider.Event{{Type: provider.EventError, Err: errors.New("summary stream failed")}}}}, wantRequests: 4},
		{name: "empty_summary", steps: []florettest.Step{{Events: []provider.Event{{Type: provider.EventEmpty}}}}, wantRequests: 4},
		{name: "repeated_truncation", steps: []florettest.Step{{Events: []provider.Event{{Type: provider.EventDelta, Text: "partial"}, {Type: provider.EventTruncated}}}, {Events: []provider.Event{{Type: provider.EventDelta, Text: "partial again"}, {Type: provider.EventTruncated}}}}, wantRequests: 5},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			steps := []florettest.Step{compactionTestStep("ack"), compactionTestStep("ack"), compactionTestStep("ack")}
			steps = append(steps, scenario.steps...)
			steps = append(steps, compactionTestStep("Launch code is CERULEAN-739."))
			gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "compact", StateCompatibilityKey: "test:compact"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, steps...)
			host, service := testThreadServiceWithAgent(t, compactionTestAgent(t, gateway))
			created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
			if err != nil {
				t.Fatal(err)
			}
			before := seedCompactionHistory(t, service, created.ThreadID)
			admitted, err := service.(ThreadContextCompactor).CompactContext(t.Context(), CompactContextInput{ThreadID: created.ThreadID, RequestKey: "compact"})
			if err != nil {
				t.Fatal(err)
			}
			after := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool {
				return v.TurnID == admitted.TurnID && v.Activity == ThreadActivityIdle && v.LastOutcome != nil
			})
			if *after.LastOutcome != TurnOutcomeFailed || after.Failure == nil || len(after.Items) != len(before.Items) {
				t.Fatalf("failure=%+v", after)
			}
			if len(gateway.Requests()) != scenario.wantRequests {
				t.Fatalf("requests=%d", len(gateway.Requests()))
			}
			if _, err := service.Retry(t.Context(), RetryInput{ThreadID: created.ThreadID, SourceTurnID: admitted.TurnID, RequestKey: "wrong-retry"}); !errors.Is(err, ErrNoRetryTarget) {
				t.Fatalf("generic retry=%v", err)
			}
			state, err := host.store.repo.(sessiontree.ProviderStateReader).ProviderState(t.Context(), created.ThreadID.String())
			if err != nil || state.State.ID != "continuation" {
				t.Fatalf("lost continuation=%v", err)
			}
			path, err := host.store.repo.Path(t.Context(), created.ThreadID.String(), "")
			if err != nil {
				t.Fatal(err)
			}
			messages, err := sessiontree.BuildContextChecked(path, sessiontree.ContextOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if len(messages) != 6 || !strings.Contains(messages[0].Content, "CERULEAN-739") {
				t.Fatal("failed compaction changed effective history")
			}
			retry, err := service.(ThreadContextCompactor).CompactContext(t.Context(), CompactContextInput{ThreadID: created.ThreadID, RequestKey: "retry-compact"})
			if err != nil {
				t.Fatal(err)
			}
			completed := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool {
				return v.TurnID == retry.TurnID && v.Activity == ThreadActivityIdle && v.LastOutcome != nil
			})
			if completed.Failure != nil || *completed.LastOutcome != TurnOutcomeCompleted {
				t.Fatalf("retry=%+v", completed)
			}
		})
	}
}

func TestIndependentThreadCompactionAdmissionDoesNotWaitForFactory(t *testing.T) {
	host, err := Open(t.Context(), Options{Storage: storage.Memory()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	started, release := make(chan struct{}), make(chan struct{})
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "compact", StateCompatibilityKey: "test:compact"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported})
	agent := compactionTestAgent(t, gateway)
	service, err := host.ThreadService(AgentFactoryFunc(func(ctx context.Context, req AgentRequest) (*Agent, error) {
		close(started)
		select {
		case <-release:
			return agent, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	requestCtx, cancel := context.WithTimeout(t.Context(), time.Second)
	admitted, err := service.(ThreadContextCompactor).CompactContext(requestCtx, CompactContextInput{ThreadID: created.ThreadID, RequestKey: "compact"})
	cancel() // A disconnected caller does not cancel durable admission.
	if err != nil || admitted.Activity != ThreadActivityActive {
		t.Fatalf("admission=%+v err=%v", admitted, err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("factory never started")
	}
	if _, err := service.(ThreadContextCompactor).CompactContext(t.Context(), CompactContextInput{ThreadID: created.ThreadID, RequestKey: "compact"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Cancel(t.Context(), CancelInput{ThreadID: created.ThreadID, RequestKey: "cancel"}); err != nil {
		t.Fatal(err)
	}
	close(release)
	view := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if *view.LastOutcome != TurnOutcomeCancelled || len(gateway.Requests()) != 0 {
		t.Fatalf("preparation cancel=%+v", view)
	}
}

func TestIndependentThreadCompactionSmallHistoryPreservesUsageAndContinuation(t *testing.T) {
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "compact", StateCompatibilityKey: "test:compact"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, compactionTestStep("ack"))
	agent, err := testAgent(gateway)
	if err != nil {
		t.Fatal(err)
	}
	host, service := testThreadServiceWithAgent(t, agent)
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	sent, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "send", Input: UserInput{Text: "Remember a small fact."}})
	if err != nil {
		t.Fatal(err)
	}
	before := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool {
		return v.TurnID == sent.TurnID && v.Activity == ThreadActivityIdle && v.LastOutcome != nil
	})
	beforeContext, err := service.(ThreadContextReader).Context(t.Context(), created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	beforeState, err := host.store.repo.(sessiontree.ProviderStateReader).ProviderState(t.Context(), created.ThreadID.String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.(ThreadContextCompactor).CompactContext(t.Context(), CompactContextInput{ThreadID: created.ThreadID, RequestKey: "send"}); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("ordinary key conflict=%v", err)
	}
	admitted, err := service.(ThreadContextCompactor).CompactContext(t.Context(), CompactContextInput{ThreadID: created.ThreadID, RequestKey: "compact"})
	if err != nil {
		t.Fatal(err)
	}
	after := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool {
		return v.TurnID == admitted.TurnID && v.Activity == ThreadActivityIdle && v.LastOutcome != nil
	})
	afterContext, err := service.(ThreadContextReader).Context(t.Context(), created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	afterState, err := host.store.repo.(sessiontree.ProviderStateReader).ProviderState(t.Context(), created.ThreadID.String())
	if err != nil {
		t.Fatal(err)
	}
	if len(gateway.Requests()) != 1 || len(after.Items) != len(before.Items) || after.Failure != nil {
		t.Fatalf("small history=%+v", after)
	}
	if len(afterContext.Compactions) != 1 || afterContext.Compactions[0].Reason != "context_too_small" {
		t.Fatalf("noop=%+v", afterContext.Compactions)
	}
	if !reflect.DeepEqual(beforeState.State, afterState.State) || !reflect.DeepEqual(beforeContext.ContextUsage, afterContext.ContextUsage) {
		t.Fatal("noop changed continuation or measured usage")
	}
}

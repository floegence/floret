package agentharness

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v6/internal/engine"
	"github.com/floegence/floret/v6/internal/event"
	"github.com/floegence/floret/v6/internal/provider"
	"github.com/floegence/floret/v6/internal/session"
	"github.com/floegence/floret/v6/internal/session/contextpolicy"
	"github.com/floegence/floret/v6/internal/sessiontree"
)

func TestTurnProjectionDropsSupersededProviderAttemptFromLiveAndCanonicalOutput(t *testing.T) {
	repo := sessiontree.NewMemoryRepo()
	if _, err := repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendTurnMarker(t.Context(), repo, "thread", "turn", sessiontree.TurnStarted, map[string]string{"run_id": "run"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendMessage(t.Context(), repo, "thread", "turn", session.Message{Role: session.User, Content: "work"}); err != nil {
		t.Fatal(err)
	}
	recorder := &event.Recorder{}
	projection := &turnProjection{
		ctx: t.Context(), turnID: "turn", runID: "run", downstream: recorder,
		thread: &Thread{id: "thread", harness: &AgentHarness{options: Options{Repo: repo}}},
	}

	projection.Emit(providerAttemptEvent(event.ProviderRequest, "logical", "attempt-1", 1, ""))
	projection.Emit(providerAttemptEvent(event.ProviderDelta, "logical", "attempt-1", 1, "superseded draft"))
	projection.Emit(providerAttemptEvent(event.ProviderRequest, "logical", "attempt-2", 2, ""))
	projection.Emit(providerAttemptEvent(event.ProviderDelta, "logical", "attempt-1", 1, "late stale delta"))
	projection.Emit(providerAttemptEvent(event.ProviderDelta, "logical", "attempt-2", 2, "canonical"))
	if err := projection.Flush(); err != nil {
		t.Fatal(err)
	}

	for _, observed := range recorder.Events {
		if observed.Type == event.ProviderDelta && observed.Message == "late stale delta" {
			t.Fatalf("stale provider delta reached live sink: %#v", recorder.Events)
		}
	}
	entries, err := repo.Entries(t.Context(), "thread")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Type == sessiontree.EntryAssistantMessage && strings.Contains(entry.Message.Content, "superseded") {
			t.Fatalf("superseded draft entered canonical timeline: %#v", entries)
		}
	}
	if got := countAssistantEntriesWithContent(entries, "canonical"); got != 1 {
		t.Fatalf("canonical assistant count=%d, want 1", got)
	}
}

func TestTurnProjectionFailsClosedForConflictingProviderAttemptIdentity(t *testing.T) {
	projection := &turnProjection{turnID: "turn", runID: "run"}
	projection.Emit(providerAttemptEvent(event.ProviderRequest, "logical", "attempt-a", 1, ""))
	projection.Emit(providerAttemptEvent(event.ProviderRequest, "logical", "attempt-b", 1, ""))
	if projection.err == nil || !strings.Contains(projection.err.Error(), "provider attempt identity conflict") {
		t.Fatalf("projection error=%v, want provider attempt identity conflict", projection.err)
	}
	if origin := turnProjectionFailureOrigin(projection.err); origin != "contract" {
		t.Fatalf("projection failure origin=%q, want contract", origin)
	}
}

func TestTurnProjectionFailsClosedWhenNextAttemptCrossesIncompletePublicToolBatch(t *testing.T) {
	projection := &turnProjection{turnID: "turn", runID: "run"}
	projection.Emit(providerAttemptEvent(event.ProviderRequest, "logical", "attempt-1", 1, ""))
	projection.Emit(event.Event{
		Type: event.ToolCall, ToolID: "tool-1", ToolName: "read", Args: `{}`,
		Metadata: map[string]any{"batch_index": 0, "batch_size": 2},
	})
	projection.Emit(providerAttemptEvent(event.ProviderRequest, "logical", "attempt-2", 2, ""))
	if projection.err == nil || !strings.Contains(projection.err.Error(), "pending canonical tool batch") {
		t.Fatalf("projection error=%v, want pending canonical tool batch", projection.err)
	}
	if origin := turnProjectionFailureOrigin(projection.err); origin != "contract" {
		t.Fatalf("projection failure origin=%q, want contract", origin)
	}
}

func TestTurnProjectionPublishesCanonicalThreadUsageTotals(t *testing.T) {
	repo, thread := newUsageProjectionThread(t)
	recorder := &event.Recorder{}
	projection := &turnProjection{
		ctx: t.Context(), turnID: "turn", runID: "run", downstream: recorder, thread: thread,
	}

	projection.Emit(providerAttemptEvent(event.ProviderRequest, "logical", "attempt-1", 1, ""))
	projection.Emit(finalProviderUsageEvent("logical", "attempt-1", 1, 1, provider.Usage{
		InputTokens: 80, OutputTokens: 20, CacheReadTokens: 15, CacheWriteTokens: 5,
		WindowInputTokens: 100, Available: true, Source: provider.UsageNative,
	}))
	projection.Emit(providerAttemptEvent(event.ProviderRequest, "logical", "attempt-2", 2, ""))
	projection.Emit(finalProviderUsageEvent("logical", "attempt-2", 2, 2, provider.Usage{
		InputTokens: 40, OutputTokens: 10, CacheReadTokens: 30,
		WindowInputTokens: 70, Available: true, Source: provider.UsageNative,
	}))
	if projection.err != nil {
		t.Fatal(projection.err)
	}

	var usageEvents []event.Event
	for _, observed := range recorder.Snapshot() {
		if observed.Type == event.ProviderUsage && observed.ThreadUsageTotals != nil {
			usageEvents = append(usageEvents, observed)
		}
	}
	if len(usageEvents) != 2 {
		t.Fatalf("committed usage events=%#v", usageEvents)
	}
	if got := *usageEvents[0].ThreadUsageTotals; got != (event.ThreadUsageTotals{
		InputTokens: 80, OutputTokens: 20, CacheReadTokens: 15, CacheWriteTokens: 5,
	}) {
		t.Fatalf("first totals=%#v", got)
	}
	if got := *usageEvents[1].ThreadUsageTotals; got != (event.ThreadUsageTotals{
		InputTokens: 120, OutputTokens: 30, CacheReadTokens: 45, CacheWriteTokens: 5,
	}) {
		t.Fatalf("second totals=%#v", got)
	}

	entries, err := repo.Entries(t.Context(), "thread")
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := thread.harness.threadDetailContext(entries, 1, threadDetailActivityContext{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.UsageTotals == nil || *snapshot.UsageTotals != (ThreadTokenUsageTotals{
		InputTokens: 120, OutputTokens: 30, CacheReadTokens: 45, CacheWriteTokens: 5,
	}) {
		t.Fatalf("canonical totals=%#v", snapshot.UsageTotals)
	}
}

func TestTurnProjectionRejectsSupersededAndUncommittedUsageTotals(t *testing.T) {
	_, thread := newUsageProjectionThread(t)
	recorder := &event.Recorder{}
	projection := &turnProjection{
		ctx: t.Context(), turnID: "turn", runID: "run", downstream: recorder, thread: thread,
	}

	projection.Emit(providerAttemptEvent(event.ProviderRequest, "logical", "attempt-1", 1, ""))
	projection.Emit(providerAttemptEvent(event.ProviderRequest, "logical", "attempt-2", 2, ""))
	projection.Emit(finalProviderUsageEvent("logical", "attempt-1", 1, 1, provider.Usage{
		InputTokens: 999, WindowInputTokens: 999, Available: true, Source: provider.UsageNative,
	}))
	projection.Emit(event.Event{
		Type: event.ProviderUsage, RunID: "run", ThreadID: "thread", TurnID: "turn", Step: 2,
		Provider: "test", Model: "scripted", Metrics: provider.Usage{InputTokens: 777},
		Metadata: map[string]any{
			"phase": "stream_usage", "logical_request_id": "logical", "attempt_id": "attempt-2", "attempt_epoch": 2,
		},
		Timestamp: time.Unix(1_723_800_001, 0).UTC(),
	})
	projection.Emit(finalProviderUsageEvent("logical", "attempt-2", 2, 2, provider.Usage{
		InputTokens: 40, CacheReadTokens: 30, WindowInputTokens: 70, Available: true, Source: provider.UsageNative,
	}))
	if projection.err != nil {
		t.Fatal(projection.err)
	}

	var providerUsageEvents []event.Event
	for _, observed := range recorder.Snapshot() {
		if observed.Type == event.ProviderUsage {
			providerUsageEvents = append(providerUsageEvents, observed)
		}
	}
	if len(providerUsageEvents) != 2 {
		t.Fatalf("provider usage events=%#v", providerUsageEvents)
	}
	if providerUsageEvents[0].ThreadUsageTotals != nil {
		t.Fatalf("stream usage totals=%#v, want nil", providerUsageEvents[0].ThreadUsageTotals)
	}
	if got := providerUsageEvents[1].ThreadUsageTotals; got == nil || *got != (event.ThreadUsageTotals{InputTokens: 40, CacheReadTokens: 30}) {
		t.Fatalf("accepted totals=%#v", got)
	}
}

func TestTurnProjectionDoesNotPublishUsageWhenCanonicalAppendFails(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "write failure", err: errors.New("injected append failure")},
		{name: "cancelled", err: context.Canceled},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repo, thread := newUsageProjectionThread(t)
			recorder := &event.Recorder{}
			projection := &turnProjection{
				ctx: t.Context(), turnID: "turn", runID: "run", downstream: recorder, thread: thread,
			}
			projection.Emit(providerAttemptEvent(event.ProviderRequest, "logical", "attempt-1", 1, ""))
			thread.harness.options.Repo = failingContextStatusRepo{JournalRepo: repo, err: test.err}
			projection.Emit(finalProviderUsageEvent("logical", "attempt-1", 1, 1, provider.Usage{
				InputTokens: 80, CacheReadTokens: 15, WindowInputTokens: 95, Available: true, Source: provider.UsageNative,
			}))
			if !errors.Is(projection.err, test.err) {
				t.Fatalf("projection error=%v, want %v", projection.err, test.err)
			}
			for _, observed := range recorder.Snapshot() {
				if observed.Type == event.ProviderUsage {
					t.Fatalf("uncommitted provider usage reached live sink: %#v", observed)
				}
			}
		})
	}
}

type failingContextStatusRepo struct {
	sessiontree.JournalRepo
	err error
}

func (repo failingContextStatusRepo) Append(ctx context.Context, entry sessiontree.Entry, opts sessiontree.AppendOptions) (sessiontree.Entry, error) {
	if entry.Metadata[threadDetailKindKey] == subAgentContextStatusEntryKind {
		return sessiontree.Entry{}, repo.err
	}
	return repo.JournalRepo.Append(ctx, entry, opts)
}

func newUsageProjectionThread(t *testing.T) (*sessiontree.MemoryRepo, *Thread) {
	t.Helper()
	repo := sessiontree.NewMemoryRepo()
	if _, err := repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendTurnMarker(t.Context(), repo, "thread", "turn", sessiontree.TurnStarted, map[string]string{"run_id": "run"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendMessage(t.Context(), repo, "thread", "turn", session.Message{Role: session.User, Content: "work"}); err != nil {
		t.Fatal(err)
	}
	thread := &Thread{id: "thread", harness: &AgentHarness{options: Options{Repo: repo}}}
	if err := thread.appendContextPolicyEvent(t.Context(), "turn", "run", "test", "scripted", contextpolicy.Policy{ContextWindowTokens: 1_000}); err != nil {
		t.Fatal(err)
	}
	return repo, thread
}

func finalProviderUsageEvent(logicalRequestID, attemptID string, attemptEpoch, step int, usage provider.Usage) event.Event {
	usage = usage.Normalized()
	pressure := contextpolicy.ContextPressure{
		WindowInputTokens: usage.WindowInputTokens, ContextWindowTokens: 1_000,
		Signal: contextpolicy.PressureSignalNativeUsage, Source: contextpolicy.PressureSourceProviderUsage,
	}
	details := engine.ProviderUsageContextStatus{
		Phase: engine.ProviderUsagePhaseFinalContextStatus, RequestID: fmt.Sprintf("run:req:%d", step),
		LogicalRequestID: logicalRequestID, Attempt: attemptEpoch, Usage: usage,
		ContextPressure: pressure, Status: engine.ContextStatusStable,
	}
	return event.Event{
		Type: event.ProviderUsage, RunID: "run", ThreadID: "thread", TurnID: "turn", Step: step,
		Provider: "test", Model: "scripted", Metrics: usage,
		Metadata: map[string]any{
			"details": details, "logical_request_id": logicalRequestID, "attempt_id": attemptID,
			"attempt_epoch": attemptEpoch, "attempt": attemptEpoch,
		},
		Timestamp: time.Unix(1_723_800_000+int64(step), 0).UTC(),
	}
}

func providerAttemptEvent(eventType event.Type, logicalRequestID, attemptID string, attemptEpoch int, message string) event.Event {
	return event.Event{
		Type: eventType, RunID: "run", ThreadID: "thread", TurnID: "turn", Message: message,
		Metadata: map[string]any{
			"logical_request_id": logicalRequestID,
			"attempt_id":         attemptID,
			"attempt_epoch":      attemptEpoch,
		},
	}
}

func countAssistantEntriesWithContent(entries []sessiontree.Entry, content string) int {
	count := 0
	for _, entry := range entries {
		if entry.Type == sessiontree.EntryAssistantMessage && entry.Message.Content == content {
			count++
		}
	}
	return count
}

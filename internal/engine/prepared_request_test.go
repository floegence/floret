package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"

	"github.com/floegence/floret/internal/engine"
	"github.com/floegence/floret/internal/event"
	"github.com/floegence/floret/internal/provider"
	"github.com/floegence/floret/internal/provider/cache"
	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/session/compaction"
	"github.com/floegence/floret/internal/session/contextpolicy"
)

func TestPreparedProviderRequestLifecycle(t *testing.T) {
	t.Run("normal consume", func(t *testing.T) {
		p := newPreparedLifecycleProvider(preparedBehavior{events: preparedDone("ok")})
		e := newTestEngine(p, &event.Recorder{})
		got := e.Run(context.Background(), "work")
		if got.Status != engine.Completed || got.Output != "ok" {
			t.Fatalf("result = %#v", got)
		}
		assertPreparedHandlesTerminated(t, p, []int{1})
		if p.fallbackStreams != 0 {
			t.Fatalf("fallback provider stream calls = %d", p.fallbackStreams)
		}
	})

	t.Run("request gate failure discards", func(t *testing.T) {
		p := newPreparedLifecycleProvider(preparedBehavior{events: preparedDone("unused")})
		e := newTestEngine(p, &event.Recorder{})
		e.Options.ProviderRequestGate = func(context.Context) (func(), error) {
			return nil, errors.New("gate unavailable")
		}
		got := e.Run(context.Background(), "work")
		if got.Status != engine.Failed || got.Err == nil {
			t.Fatalf("result = %#v", got)
		}
		assertPreparedHandlesTerminated(t, p, []int{0})
	})

	t.Run("request ledger failure discards", func(t *testing.T) {
		p := newPreparedLifecycleProvider(preparedBehavior{events: preparedDone("unused")})
		e := newTestEngine(p, &event.Recorder{})
		e.Prompt = &failingProviderRequestStore{Store: cache.NewMemoryStore(), err: errors.New("append request failed")}
		got := e.Run(context.Background(), "work")
		if got.Status != engine.Failed || got.Err == nil {
			t.Fatalf("result = %#v", got)
		}
		assertPreparedHandlesTerminated(t, p, []int{0})
	})

	t.Run("input limit discards before stream", func(t *testing.T) {
		p := newPreparedLifecycleProvider(preparedBehavior{
			estimate: preparedEstimate(500), events: preparedDone("unused"), enforceInputLimit: true,
		})
		e := newTestEngine(p, &event.Recorder{})
		e.Options.MaxInputTokens = 100
		got := e.Run(context.Background(), "work")
		if got.Status != engine.Failed || !errors.Is(got.Err, engine.ErrInputTokenBudgetExceeded) {
			t.Fatalf("result = %#v", got)
		}
		assertPreparedHandlesTerminated(t, p, []int{0})
	})

	t.Run("stream startup failure terminates", func(t *testing.T) {
		p := newPreparedLifecycleProvider(preparedBehavior{streamErr: errors.New("stream failed")})
		e := newTestEngine(p, &event.Recorder{})
		got := e.Run(context.Background(), "work")
		if got.Status != engine.Failed || got.Err == nil {
			t.Fatalf("result = %#v", got)
		}
		assertPreparedHandlesTerminated(t, p, []int{1})
	})

	t.Run("token component overflow discards", func(t *testing.T) {
		p := newPreparedLifecycleProvider(preparedBehavior{estimate: provider.TokenEstimate{
			PrefixTokens: math.MaxInt64, MessageTokens: 1, EstimatedInputTokens: math.MaxInt64,
			Source: "prepared_test", Method: provider.TokenEstimateProviderRenderedPayload,
			Confidence: provider.EstimateConservative, Coverage: provider.TokenEstimateCoverageComplete,
		}})
		e := newTestEngine(p, &event.Recorder{})
		got := e.Run(context.Background(), "work")
		if got.Status != engine.Failed || !errors.Is(got.Err, engine.ErrInvalidTokenEstimate) {
			t.Fatalf("result = %#v", got)
		}
		assertPreparedHandlesTerminated(t, p, []int{0})
	})

	t.Run("cancellation during consume terminates", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		p := newPreparedLifecycleProvider(preparedBehavior{stream: func() <-chan provider.StreamEvent {
			ch := make(chan provider.StreamEvent)
			cancel()
			return ch
		}})
		e := newTestEngine(p, &event.Recorder{})
		got := e.Run(ctx, "work")
		if got.Status != engine.Cancelled || !errors.Is(got.Err, context.Canceled) {
			t.Fatalf("result = %#v", got)
		}
		assertPreparedHandlesTerminated(t, p, []int{1})
	})
}

func TestPreparedProviderRequestDoesNotEnterRequestJSONOrLedgerShape(t *testing.T) {
	p := newPreparedLifecycleProvider(preparedBehavior{events: preparedDone("ok")})
	e := newTestEngine(p, &event.Recorder{})
	got := e.Run(context.Background(), "work")
	if got.Status != engine.Completed {
		t.Fatalf("result = %#v", got)
	}
	handles := p.snapshotHandles()
	if len(handles) != 1 {
		t.Fatalf("handles = %d", len(handles))
	}
	rawRequest, err := json.Marshal(provider.Request{Prepared: handles[0]})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(rawRequest), "Prepared") || strings.Contains(string(rawRequest), "streamCalls") {
		t.Fatalf("prepared handle leaked into request JSON: %s", rawRequest)
	}
	records, err := e.Prompt.ProviderRequests(context.Background(), "run")
	if err != nil || len(records) != 1 {
		t.Fatalf("provider request records=%#v err=%v", records, err)
	}
	rawLedger, err := json.Marshal(records)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(rawLedger), "Prepared") || strings.Contains(string(rawLedger), "streamCalls") {
		t.Fatalf("prepared handle leaked into provider request ledger: %s", rawLedger)
	}
}

func TestPreparedProviderRequestDiscardedAndRepreparedForCompaction(t *testing.T) {
	p := newPreparedLifecycleProvider(
		preparedBehavior{estimate: preparedEstimate(1000), events: preparedDone("unused")},
		preparedBehavior{estimate: preparedEstimate(10), events: preparedDone("ok")},
	)
	store := session.NewMemoryStore()
	if err := store.AppendTranscript("run",
		session.Message{Role: session.User, Content: "older", EntryID: "u1"},
		session.Message{Role: session.Assistant, Content: "answer", EntryID: "a1"},
		session.Message{Role: session.User, Content: "new", EntryID: "u2"},
	); err != nil {
		t.Fatal(err)
	}
	e := newTestEngine(p, &event.Recorder{})
	e.Store = store
	e.Compactor = engine.LocalCompactionManager{Generator: compaction.ExtractiveSummaryGenerator{}}
	e.Options.ContextPolicy = contextpolicy.Policy{ContextWindowTokens: 1000, ReservedOutputTokens: 100, ReservedSummaryTokens: 80, RecentTailTokens: 20, RecentUserTokens: 20}

	got := e.Run(context.Background(), "")
	if got.Status != engine.Completed || got.Output != "ok" || got.Metrics.Compactions != 1 {
		t.Fatalf("result = %#v", got)
	}
	assertPreparedHandlesTerminated(t, p, []int{0, 1})
}

func TestPreparedProviderRequestOverflowReplacementTerminatesBothHandles(t *testing.T) {
	p := newPreparedLifecycleProvider(
		preparedBehavior{streamErr: provider.ErrContextOverflow},
		preparedBehavior{events: preparedDone("after compact")},
	)
	store := session.NewMemoryStore()
	if err := store.AppendTranscript("run",
		session.Message{Role: session.User, Content: "older", EntryID: "u1"},
		session.Message{Role: session.User, Content: "newer", EntryID: "u2"},
	); err != nil {
		t.Fatal(err)
	}
	e := newTestEngine(p, &event.Recorder{})
	e.Store = store
	e.Compactor = engine.LocalCompactionManager{Generator: compaction.ExtractiveSummaryGenerator{}}

	got := e.Run(context.Background(), "")
	if got.Status != engine.Completed || got.Output != "after compact" || got.Metrics.Compactions != 1 {
		t.Fatalf("result = %#v", got)
	}
	assertPreparedHandlesTerminated(t, p, []int{1, 1})
}

func TestPreparedProviderRequestManualCompactContextDiscardsUnstreamedHandle(t *testing.T) {
	tests := []struct {
		name       string
		closeErr   error
		wantStatus engine.Status
	}{
		{name: "success", wantStatus: engine.Completed},
		{name: "close failure preserves committed compaction", closeErr: errors.New("close failed"), wantStatus: engine.Failed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newPreparedLifecycleProvider(preparedBehavior{closeErr: tt.closeErr})
			e := newTestEngine(p, &event.Recorder{})
			e.Compactor = engine.LocalCompactionManager{Generator: compaction.ExtractiveSummaryGenerator{}}
			e.Options.ContextPolicy = contextpolicy.Policy{
				ContextWindowTokens:          256000,
				ReservedOutputTokens:         64000,
				ReservedSummaryTokens:        2400,
				RecentTailTokens:             20,
				RecentUserTokens:             20,
				CompactedContextTargetTokens: 100,
			}
			history := []session.Message{
				{Role: session.User, Content: "first request " + strings.Repeat("alpha ", 300), EntryID: "u1"},
				{Role: session.Assistant, Content: "first answer " + strings.Repeat("bravo ", 300), EntryID: "a1"},
				{Role: session.User, Content: "second request " + strings.Repeat("charlie ", 300), EntryID: "u2"},
				{Role: session.Assistant, Content: "second answer " + strings.Repeat("delta ", 300), EntryID: "a2"},
			}

			got := e.build(t).CompactContext(context.Background(), engine.RunInput{
				RunID:   "manual-prepared-request",
				History: history,
			}, engine.ManualCompactionRequest{RequestID: "manual-prepared-request", Source: "unit_test"})

			if got.Status != tt.wantStatus || got.Metrics.Compactions != 1 || got.Compaction.CompactionID == "" {
				t.Fatalf("manual compact result = %#v", got)
			}
			if tt.closeErr == nil && got.Err != nil {
				t.Fatalf("manual compact error = %v", got.Err)
			}
			if tt.closeErr != nil && !errors.Is(got.Err, tt.closeErr) {
				t.Fatalf("manual compact error = %v, want %v", got.Err, tt.closeErr)
			}
			if countMessagesByKind(got.Messages, session.MessageKindCompactionSummary) != 1 {
				t.Fatalf("manual compact messages = %#v", got.Messages)
			}
			assertPreparedHandlesTerminated(t, p, []int{0})
		})
	}
}

type preparedBehavior struct {
	estimate          provider.TokenEstimate
	events            []provider.StreamEvent
	stream            func() <-chan provider.StreamEvent
	streamErr         error
	closeErr          error
	enforceInputLimit bool
}

type failingProviderRequestStore struct {
	cache.Store
	err error
}

func (s *failingProviderRequestStore) AppendProviderRequest(context.Context, cache.ProviderRequestRecord) error {
	return s.err
}

type preparedLifecycleProvider struct {
	mu              sync.Mutex
	behaviors       []preparedBehavior
	handles         []*preparedLifecycleHandle
	fallbackStreams int
}

func newPreparedLifecycleProvider(behaviors ...preparedBehavior) *preparedLifecycleProvider {
	return &preparedLifecycleProvider{behaviors: behaviors}
}

func (p *preparedLifecycleProvider) PrepareRequest(_ context.Context, _ provider.Request) (provider.PreparedRequest, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	index := len(p.handles)
	behavior := preparedBehavior{events: preparedDone("ok")}
	if index < len(p.behaviors) {
		behavior = p.behaviors[index]
	}
	if behavior.estimate.Source == "" {
		behavior.estimate = preparedEstimate(10)
	}
	handle := &preparedLifecycleHandle{
		behavior:    behavior,
		fingerprint: fmt.Sprintf("prepared-payload-%d", index+1),
	}
	p.handles = append(p.handles, handle)
	return handle, nil
}

func (p *preparedLifecycleProvider) Stream(context.Context, provider.Request) (<-chan provider.StreamEvent, error) {
	p.mu.Lock()
	p.fallbackStreams++
	p.mu.Unlock()
	return nil, errors.New("prepared provider fallback stream must not be called")
}

func (p *preparedLifecycleProvider) snapshotHandles() []*preparedLifecycleHandle {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]*preparedLifecycleHandle(nil), p.handles...)
}

type preparedLifecycleHandle struct {
	mu          sync.Mutex
	behavior    preparedBehavior
	fingerprint string
	streamCalls int
	closeCalls  int
	closed      bool
}

func (h *preparedLifecycleHandle) Stream(context.Context) (<-chan provider.StreamEvent, error) {
	h.mu.Lock()
	h.streamCalls++
	behavior := h.behavior
	h.mu.Unlock()
	if behavior.streamErr != nil {
		return nil, behavior.streamErr
	}
	if behavior.stream != nil {
		return behavior.stream(), nil
	}
	ch := make(chan provider.StreamEvent, len(behavior.events))
	for _, event := range behavior.events {
		ch <- event
	}
	close(ch)
	return ch, nil
}

func (h *preparedLifecycleHandle) TokenEstimate() provider.TokenEstimate {
	return h.behavior.estimate
}

func (h *preparedLifecycleHandle) PayloadFingerprint() string {
	return h.fingerprint
}

func (h *preparedLifecycleHandle) EnforceInputTokenLimit() bool {
	return h.behavior.enforceInputLimit
}

func (h *preparedLifecycleHandle) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return h.behavior.closeErr
	}
	h.closed = true
	h.closeCalls++
	return h.behavior.closeErr
}

func preparedEstimate(tokens int64) provider.TokenEstimate {
	return provider.TokenEstimate{
		EstimatedInputTokens: tokens,
		Source:               "prepared_test",
		Method:               provider.TokenEstimateProviderRenderedPayload,
		Confidence:           provider.EstimateConservative,
		Coverage:             provider.TokenEstimateCoverageComplete,
	}
}

func preparedDone(text string) []provider.StreamEvent {
	return []provider.StreamEvent{{Type: provider.Delta, Text: text}, {Type: provider.Done, Reason: "stop"}}
}

func assertPreparedHandlesTerminated(t *testing.T, p *preparedLifecycleProvider, streamCalls []int) {
	t.Helper()
	handles := p.snapshotHandles()
	if len(handles) != len(streamCalls) {
		t.Fatalf("prepared handles = %d, want %d", len(handles), len(streamCalls))
	}
	for index, handle := range handles {
		handle.mu.Lock()
		gotStreamCalls := handle.streamCalls
		gotCloseCalls := handle.closeCalls
		closed := handle.closed
		handle.mu.Unlock()
		if gotStreamCalls != streamCalls[index] || gotCloseCalls != 1 || !closed {
			t.Fatalf("handle %d lifecycle: stream=%d close=%d closed=%t, want stream=%d close=1 closed", index, gotStreamCalls, gotCloseCalls, closed, streamCalls[index])
		}
	}
}

var _ provider.Provider = (*preparedLifecycleProvider)(nil)
var _ provider.RequestPreparer = (*preparedLifecycleProvider)(nil)
var _ provider.PreparedRequest = (*preparedLifecycleHandle)(nil)
var _ provider.PreparedInputTokenLimit = (*preparedLifecycleHandle)(nil)

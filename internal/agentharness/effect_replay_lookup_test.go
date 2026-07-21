package agentharness

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/floegence/floret/internal/engine"
	"github.com/floegence/floret/internal/provider/cache"
	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/sessiontree"
	"github.com/floegence/floret/internal/testing/harness"
	"github.com/floegence/floret/observation"
)

func TestReplayEffectResultUsesExactEntryInLargeJournal(t *testing.T) {
	thread, attempt, message, repo := newReplayEffectFixture(t, 1000)
	repo.resetCounts()

	result := thread.replayEffectResult(context.Background(), attempt)
	if result.DispatchErr != nil || result.Text != message.Content {
		t.Fatalf("replay result = %#v", result)
	}
	if repo.entryCalls.Load() != 1 || repo.entriesCalls.Load() != 0 {
		t.Fatalf("lookup calls = Entry:%d Entries:%d, want Entry:1 Entries:0", repo.entryCalls.Load(), repo.entriesCalls.Load())
	}
	finalized, err := thread.finalizeEffectResult(context.Background(), replayFinalizationRequest(attempt, message))
	if err != nil {
		t.Fatal(err)
	}
	if !finalized.Handled || !finalized.Replayed || finalized.CanonicalEntryID != attempt.ResultEntryID {
		t.Fatalf("finalized result = %#v", finalized)
	}
}

func TestReplayEffectResultFailsClosedOnInvalidCanonicalEntry(t *testing.T) {
	tests := []struct {
		name         string
		attemptState sessiontree.EffectAttemptState
		transform    func(sessiontree.Entry) sessiontree.Entry
	}{
		{
			name: "nil tool result with valid integrity",
			transform: func(entry sessiontree.Entry) sessiontree.Entry {
				entry.Message.ToolResult = nil
				return sessiontree.PrepareEntry(entry)
			},
		},
		{
			name: "completed attempt with error result",
			transform: func(entry sessiontree.Entry) sessiontree.Entry {
				entry.Message.ToolResult.Status = string(observation.ActivityStatusError)
				return sessiontree.PrepareEntry(entry)
			},
		},
		{
			name:         "failed attempt with success result",
			attemptState: sessiontree.EffectAttemptFailed,
			transform: func(entry sessiontree.Entry) sessiontree.Entry {
				return sessiontree.PrepareEntry(entry)
			},
		},
		{
			name: "wrong thread",
			transform: func(entry sessiontree.Entry) sessiontree.Entry {
				entry.ThreadID = "other-thread"
				return entry
			},
		},
		{
			name: "broken integrity",
			transform: func(entry sessiontree.Entry) sessiontree.Entry {
				entry.RawHash = "broken"
				return entry
			},
		},
		{
			name: "wrong effect identity",
			transform: func(entry sessiontree.Entry) sessiontree.Entry {
				entry.Metadata[sessiontree.PendingToolEffectAttemptIDKey] = "other-effect"
				entry = sessiontree.PrepareEntry(entry)
				return entry
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			thread, attempt, message, repo := newReplayEffectFixture(t, 1)
			if test.attemptState != "" {
				attempt.State = test.attemptState
			}
			repo.transform = test.transform
			repo.resetCounts()

			result := thread.replayEffectResult(context.Background(), attempt)
			if !errors.Is(result.DispatchErr, sessiontree.ErrAuthorityCorrupt) {
				t.Fatalf("replay error = %v, want committed ErrAuthorityCorrupt", result.DispatchErr)
			}
			var committed *CommittedEffectError
			if !errors.As(result.DispatchErr, &committed) || committed.EffectAttemptID != attempt.EffectAttemptID {
				t.Fatalf("replay error = %#v, want committed effect failure", result.DispatchErr)
			}
			if repo.entryCalls.Load() != 1 || repo.entriesCalls.Load() != 0 {
				t.Fatalf("lookup calls = Entry:%d Entries:%d, want Entry:1 Entries:0", repo.entryCalls.Load(), repo.entriesCalls.Load())
			}
			finalized, err := thread.finalizeEffectResult(context.Background(), replayFinalizationRequest(attempt, message))
			if !errors.Is(err, ErrEffectDispatchConsumed) || finalized.Handled {
				t.Fatalf("invalid replay registered finalizer: result=%#v err=%v", finalized, err)
			}
		})
	}
}

func TestReplayEffectResultFailsClosedWhenCanonicalEntryIsMissing(t *testing.T) {
	thread, attempt, _, repo := newReplayEffectFixture(t, 1)
	attempt.ResultEntryID = "missing"
	repo.resetCounts()

	result := thread.replayEffectResult(context.Background(), attempt)
	if !errors.Is(result.DispatchErr, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("replay error = %v, want committed ErrAuthorityCorrupt", result.DispatchErr)
	}
	var committed *CommittedEffectError
	if !errors.As(result.DispatchErr, &committed) {
		t.Fatalf("replay error = %#v, want committed effect failure", result.DispatchErr)
	}
	if repo.entryCalls.Load() != 1 || repo.entriesCalls.Load() != 0 {
		t.Fatalf("lookup calls = Entry:%d Entries:%d, want Entry:1 Entries:0", repo.entryCalls.Load(), repo.entriesCalls.Load())
	}
}

func BenchmarkReplayEffectResultExactLookup(b *testing.B) {
	for _, journalSize := range []int{1, 1000} {
		b.Run(fmt.Sprintf("journal/%d", journalSize), func(b *testing.B) {
			thread, attempt, message, repo := newReplayEffectFixture(b, journalSize)
			request := replayFinalizationRequest(attempt, message)
			repo.resetCounts()
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				result := thread.replayEffectResult(context.Background(), attempt)
				if result.DispatchErr != nil {
					b.Fatal(result.DispatchErr)
				}
				if _, err := thread.finalizeEffectResult(context.Background(), request); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if repo.entryCalls.Load() != int64(b.N) || repo.entriesCalls.Load() != 0 {
				b.Fatalf("lookup calls = Entry:%d Entries:%d, want Entry:%d Entries:0", repo.entryCalls.Load(), repo.entriesCalls.Load(), b.N)
			}
		})
	}
}

type replayEntryCountingRepo struct {
	*sessiontree.MemoryRepo
	entryCalls   atomic.Int64
	entriesCalls atomic.Int64
	transform    func(sessiontree.Entry) sessiontree.Entry
}

func (r *replayEntryCountingRepo) Entry(ctx context.Context, threadID, entryID string) (sessiontree.Entry, error) {
	r.entryCalls.Add(1)
	entry, err := r.MemoryRepo.Entry(ctx, threadID, entryID)
	if err != nil || r.transform == nil {
		return entry, err
	}
	return r.transform(entry), nil
}

func (r *replayEntryCountingRepo) Entries(ctx context.Context, threadID string) ([]sessiontree.Entry, error) {
	r.entriesCalls.Add(1)
	return r.MemoryRepo.Entries(ctx, threadID)
}

func (r *replayEntryCountingRepo) resetCounts() {
	r.entryCalls.Store(0)
	r.entriesCalls.Store(0)
}

func newReplayEffectFixture(tb testing.TB, journalSize int) (*Thread, sessiontree.EffectAttempt, session.Message, *replayEntryCountingRepo) {
	tb.Helper()
	ctx := context.Background()
	repo := &replayEntryCountingRepo{MemoryRepo: sessiontree.NewMemoryRepo()}
	h := newTestHarness(harness.NewScriptedProvider(), repo, cache.NewMemoryStore())
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		tb.Fatal(err)
	}
	for index := 1; index < journalSize; index++ {
		if _, err := repo.Append(ctx, sessiontree.Entry{
			ThreadID: "thread", Type: sessiontree.EntryCustom, Metadata: map[string]string{"index": fmt.Sprint(index)},
		}, sessiontree.AppendOptions{}); err != nil {
			tb.Fatal(err)
		}
	}
	message := session.Message{
		Role: session.Tool, Content: "visible", ToolCallID: "call-1", ToolName: "shell",
		ToolResult: &session.ToolResultView{Status: string(observation.ActivityStatusSuccess)},
	}
	entry, err := repo.Append(ctx, sessiontree.Entry{
		ThreadID: "thread", TurnID: "turn-1", Type: sessiontree.EntryToolResult, Message: message,
		Metadata: map[string]string{sessiontree.PendingToolEffectAttemptIDKey: "effect-1"},
	}, sessiontree.AppendOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	attempt := sessiontree.EffectAttempt{
		EffectAttemptID: "effect-1", State: sessiontree.EffectAttemptCompleted, ResultEntryID: entry.ID,
		Invocation: sessiontree.EffectInvocationIdentity{
			ThreadID: "thread", TurnID: "turn-1", RunID: "run-1", ToolCallID: "call-1", ToolName: "shell",
		},
	}
	return thread, attempt, message, repo
}

func replayFinalizationRequest(attempt sessiontree.EffectAttempt, message session.Message) engine.EffectResultFinalizationRequest {
	return engine.EffectResultFinalizationRequest{
		RunID: attempt.Invocation.RunID, ThreadID: attempt.Invocation.ThreadID, TurnID: attempt.Invocation.TurnID,
		ToolCallID: attempt.Invocation.ToolCallID, Message: session.CloneMessage(message),
	}
}

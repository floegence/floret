package runtime

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func textGrowthActor(history int) *threadRuntimeState {
	actor := &threadRuntimeState{state: threadRuntimeData{turnID: "turn", runID: "run"}}
	for i := range history {
		actor.state.view.Items = appendThreadItem(actor.state.view.Items, ThreadItem{ID: fmt.Sprintf("history:%d", i), Kind: ThreadItemAssistant, Text: strings.Repeat("history ", 64), CanonicalOrdinal: uint64(i + 1)})
	}
	return actor
}

func BenchmarkThreadNativeAppendDelivery(b *testing.B) {
	for _, history := range []int{200, 2000} {
		b.Run(fmt.Sprintf("history_%d", history), func(b *testing.B) {
			actor := textGrowthActor(history)
			actor.state.view.ThreadID = "thread"
			actor.state.view.TurnID, actor.state.view.RunID = "turn", "run"
			actor.state.view.ViewVersion = 1
			actor.appendLiveTextSegment(ThreadItemAssistant, "start", time.Time{}, "source")
			subscription := &ThreadUpdateSubscription{updates: make(chan queuedThreadUpdate, 64), done: make(chan struct{})}
			service := &threadRuntimeService{published: make(map[string]uint64), updateSubscribers: map[*ThreadUpdateSubscription]struct{}{subscription: {}}}
			actor.service = service
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_ = actor.apply(context.Background(), func() error {
					actor.appendLiveTextSegment(ThreadItemAssistant, "x", time.Time{}, "source")
					actor.state.view.ViewVersion++
					return nil
				})
				actor.mu.Lock()
				service.flushUpdateLocked(actor)
				actor.mu.Unlock()
				if _, err := subscription.Next(context.Background()); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			actor.stopUpdateTimer()
		})
	}
}

func TestThreadTextGrowthRetainsImmutableSnapshots(t *testing.T) {
	actor := textGrowthActor(200)
	actor.appendLiveTextSegment(ThreadItemAssistant, "Hello 世界", time.Now(), "source")
	first := cloneThreadRuntimeView(actor.state.view)
	for range 100 {
		actor.appendLiveTextSegment(ThreadItemAssistant, "🌸", time.Now(), "source")
	}
	if got := first.Items[200].Text; got != "Hello 世界" {
		t.Fatalf("snapshot changed after append: %q", got)
	}
	actor.finishLiveTextSegment()
	if got := actor.state.view.Items[200].Text; got != "Hello 世界"+strings.Repeat("🌸", 100) {
		t.Fatalf("final text differs: %q", got)
	}
	actor.appendLiveTextSegment(ThreadItemThinking, "Reason", time.Now(), "reason")
	actor.appendLiveTextSegment(ThreadItemAssistant, "!", time.Now(), "source")
	if got := actor.state.view.Items[200].Text; got != "Hello 世界"+strings.Repeat("🌸", 100)+"!" {
		t.Fatalf("resumed segment lost text: %q", got)
	}
}

func TestThreadTextGrowthDoesNotCopyAccumulatedText(t *testing.T) {
	actor := textGrowthActor(2000)
	actor.appendLiveTextSegment(ThreadItemAssistant, strings.Repeat("x", 1<<20), time.Now(), "source")
	// Warm the append buffer before measuring steady growth.
	actor.appendLiveTextSegment(ThreadItemAssistant, "x", time.Now(), "source")
	result := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			actor.appendLiveTextSegment(ThreadItemAssistant, "x", time.Time{}, "source")
		}
	})
	if result.AllocedBytesPerOp() > 4096 {
		t.Fatalf("append copies accumulated text: %d bytes/op", result.AllocedBytesPerOp())
	}
}

func BenchmarkThreadTextGrowth(b *testing.B) {
	for _, history := range []int{200, 2000} {
		b.Run(fmt.Sprintf("history_%d", history), func(b *testing.B) {
			actor := textGrowthActor(history)
			actor.appendLiveTextSegment(ThreadItemAssistant, "start", time.Now(), "source")
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				actor.appendLiveTextSegment(ThreadItemAssistant, "x", time.Time{}, "source")
			}
		})
	}
}

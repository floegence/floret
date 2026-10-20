package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/observation"
)

func nativeUpdatesFixture(t *testing.T) (*threadRuntimeService, *threadRuntimeState, threadRuntimeEventSink, *ThreadUpdateSubscription) {
	t.Helper()
	service, actor, sink, legacy := liveActivityFixture(t)
	legacy.Close()
	delete(service.subscribers, legacy)
	service.host = &Host{}
	sub, err := service.SubscribeUpdates(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sub.Close)
	return service, actor, sink, sub
}

func emitText(sink threadRuntimeEventSink, source, text string) {
	sink.EmitEvent(Event{Type: observation.EventTypeProviderDelta, ThreadID: "thread-live", TurnID: "turn-live", RunID: "run-live", outputSourceID: source, Stream: &StreamObservation{Type: StreamObservationAssistantDelta, Text: text}})
}

func nextUpdate(t *testing.T, sub *ThreadUpdateSubscription) ThreadUpdate {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	update, err := sub.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return update
}

func TestThreadUpdatesAppendUsesByteOffsetsAndContinuousVersions(t *testing.T) {
	_, _, sink, sub := nativeUpdatesFixture(t)
	emitText(sink, "source", "世界🌸")
	first := nextUpdate(t, sub)
	if first.BaseVersion != 1 || first.ViewVersion != 2 || len(first.Items) != 1 || first.Items[0].Item == nil || first.Items[0].Item.Text != "世界🌸" {
		t.Fatalf("first output must publish an immediate insertion: %+v", first)
	}
	emitText(sink, "source", "a")
	emitText(sink, "source", "é")
	update := nextUpdate(t, sub)
	if update.BaseVersion != first.ViewVersion || update.ViewVersion != 4 || len(update.Items) != 1 {
		t.Fatalf("noncontinuous append: %+v", update)
	}
	change := update.Items[0]
	if change.Append == nil || change.Append.OffsetBytes != len("世界🌸") || change.Append.Text != "aé" || change.Item != nil {
		t.Fatalf("append must contain only new UTF-8 bytes: %+v", change)
	}
}

func TestThreadUpdatesBoundaryFlushesGrowthAndIsolatesConsumers(t *testing.T) {
	service, actor, sink, sub := nativeUpdatesFixture(t)
	other, err := service.SubscribeUpdates(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	emitText(sink, "first", "first")
	first := nextUpdate(t, sub)
	first.Items[0].Item.Text = "mutated"
	if got := nextUpdate(t, other).Items[0].Item.Text; got != "first" {
		t.Fatalf("subscriber leaked mutation: %q", got)
	}
	emitText(sink, "first", " tail")
	emitText(sink, "second", "second")
	growth, boundary := nextUpdate(t, sub), nextUpdate(t, sub)
	if growth.ViewVersion != boundary.BaseVersion || growth.Items[0].Append.Text != " tail" {
		t.Fatalf("boundary overtook growth: %+v %+v", growth, boundary)
	}
	if !strings.Contains(service.currentView(actor).Items[0].Text, "tail") {
		t.Fatal("actor lost growth")
	}
}

func TestThreadUpdatesOverflowRequiresNewBaseline(t *testing.T) {
	_, _, sink, sub := nativeUpdatesFixture(t)
	for i := range 100 {
		emitText(sink, strings.Repeat("x", i+1), "text")
	}
	_, err := sub.Next(t.Context())
	if !errors.Is(err, ErrThreadUpdateOverflow) {
		t.Fatalf("overflow must fail explicitly: %v", err)
	}
}

func TestThreadUpdatesSnapshotFlushEstablishesExactBaseline(t *testing.T) {
	service, actor, sink, sub := nativeUpdatesFixture(t)
	emitText(sink, "source", "first")
	emitText(sink, "source", " pending")
	baseline := service.currentView(actor)
	if actor.pendingUpdate != nil {
		t.Fatal("snapshot left an overlapping pending batch")
	}
	emitText(sink, "source", " later")
	final := service.currentView(actor)
	assertObservedSnapshot(t, sub, &baseline, final)
}

func TestThreadUpdatesWithoutObserversDoNotMaterializeOrSchedule(t *testing.T) {
	service, actor, sink, sub := nativeUpdatesFixture(t)
	sub.Close()
	for range 100 {
		emitText(sink, "source", "text")
	}
	actor.mu.Lock()
	defer actor.mu.Unlock()
	if actor.pendingUpdate != nil || actor.updateTimer != nil {
		t.Fatal("unobserved growth scheduled presentation work")
	}
	if len(service.updateSubscribers) != 0 {
		t.Fatal("closed observer retained")
	}
	if actor.state.view.Items[0].Text != strings.Repeat("text", 100) {
		t.Fatal("execution lost unobserved output")
	}
}

func TestThreadUpdatesCanonicalFailureInvalidatesAndCanResubscribe(t *testing.T) {
	service, actor, sink, sub := nativeUpdatesFixture(t)
	emitText(sink, "source", "first")
	if err := actor.apply(t.Context(), func() error { actor.state.contextErr = errors.New("unavailable canonical snapshot"); return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Next(t.Context()); !errors.Is(err, ErrThreadUpdateInvalidated) {
		t.Fatalf("observer silently retained invalid baseline: %v", err)
	}
	if err := actor.apply(t.Context(), func() error { actor.state.contextErr = nil; return nil }); err != nil {
		t.Fatal(err)
	}
	recovered, err := service.SubscribeUpdates(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	baseline := service.currentView(actor)
	emitText(sink, "source", " recovered")
	assertObservedSnapshot(t, recovered, &baseline, service.currentView(actor))
}

func TestThreadUpdatesByteOverflowReleasesQueuedBodies(t *testing.T) {
	service, actor, _, sub := nativeUpdatesFixture(t)
	update := ThreadUpdate{ThreadID: actor.state.view.ThreadID, BaseVersion: 1, ViewVersion: 2, Items: []ThreadItemChange{{Kind: ThreadItemAppended, ItemID: "large", Append: &ThreadTextAppend{Text: strings.Repeat("x", threadUpdateQueueBytes/2)}}}}
	service.deliverUpdateLocked(actor, update)
	if sub.queuedBytes == 0 || sub.queuedBytes > threadUpdateQueueBytes || len(sub.updates) != 1 {
		t.Fatal("first batch was not budgeted")
	}
	update.BaseVersion, update.ViewVersion = 2, 3
	service.deliverUpdateLocked(actor, update)
	if _, err := sub.Next(t.Context()); !errors.Is(err, ErrThreadUpdateOverflow) {
		t.Fatalf("overflow=%v", err)
	}
	if sub.queuedBytes != 0 || len(sub.updates) != 0 {
		t.Fatal("overflow retained queued bodies")
	}
	if _, exists := service.updateSubscribers[sub]; exists {
		t.Fatal("overflow retained subscriber")
	}
}

func TestThreadUpdatesReadCloseAndInvalidationReleaseBudget(t *testing.T) {
	for _, mode := range []string{"read", "close", "invalidate", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			service, actor, _, first := nativeUpdatesFixture(t)
			first.Close()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			sub, err := service.SubscribeUpdates(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer sub.Close()
			update := ThreadUpdate{ThreadID: actor.state.view.ThreadID, BaseVersion: 1, ViewVersion: 2, Items: []ThreadItemChange{{Kind: ThreadItemAppended, ItemID: "body", Append: &ThreadTextAppend{Text: strings.Repeat("x", 400<<10)}}}}
			service.deliverUpdateLocked(actor, update)
			switch mode {
			case "read":
				if _, err := sub.Next(t.Context()); err != nil {
					t.Fatal(err)
				}
			case "close":
				sub.Close()
			case "invalidate":
				service.invalidateUpdates()
			case "cancel":
				cancel()
				select {
				case <-sub.done:
				case <-time.After(time.Second):
					t.Fatal("cancellation did not release subscription")
				}
			}
			sub.mu.Lock()
			defer sub.mu.Unlock()
			if sub.queuedBytes != 0 || len(sub.updates) != 0 {
				t.Fatal("settled subscription retained bodies")
			}
		})
	}
}

func TestThreadUpdatesOversizedBatchDoesNotBlockCanonicalProgress(t *testing.T) {
	service, actor, sink, sub := nativeUpdatesFixture(t)
	emitText(sink, "large", strings.Repeat("x", threadUpdateQueueBytes+1))
	if _, err := sub.Next(t.Context()); !errors.Is(err, ErrThreadUpdateOverflow) {
		t.Fatalf("oversized batch=%v", err)
	}
	if len(actor.state.view.Items) != 1 || len(actor.state.view.Items[0].Text) != threadUpdateQueueBytes+1 {
		t.Fatal("observer budget truncated canonical presentation")
	}
	replacement, err := service.SubscribeUpdates(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	baseline := service.currentView(actor)
	emitText(sink, "large", "still running")
	update := nextUpdate(t, replacement)
	if update.BaseVersion != baseline.ViewVersion || update.Items[0].Append == nil || update.Items[0].Append.Text != "still running" {
		t.Fatal("resubscription did not recover")
	}
}

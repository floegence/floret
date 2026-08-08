package runtime

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/v3/identity"
	"github.com/floegence/floret/v3/observation"
	"github.com/floegence/floret/v3/storage"
)

func TestThreadActorSerializesOneThreadWithoutBlockingAnother(t *testing.T) {
	registry := newThreadActorRegistry()
	t.Cleanup(registry.close)

	actorA := registry.actor("thread-a")
	actorB := registry.actor("thread-b")
	aEntered := make(chan struct{})
	aRelease := make(chan struct{})
	aSecondEntered := make(chan struct{})
	bDone := make(chan error, 1)

	go func() {
		_ = actorA.apply(context.Background(), func() error {
			close(aEntered)
			<-aRelease
			return nil
		})
	}()
	select {
	case <-aEntered:
	case <-time.After(time.Second):
		t.Fatal("thread A actor did not start its first message")
	}

	go func() {
		_ = actorA.apply(context.Background(), func() error {
			close(aSecondEntered)
			return nil
		})
	}()
	go func() {
		bDone <- actorB.apply(context.Background(), func() error { return nil })
	}()

	select {
	case err := <-bDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("thread B actor waited for thread A mailbox")
	}
	select {
	case <-aSecondEntered:
		t.Fatal("second thread A message overtook the first")
	default:
	}

	close(aRelease)
	select {
	case <-aSecondEntered:
	case <-time.After(time.Second):
		t.Fatal("second thread A message did not run after the first completed")
	}
}

func TestThreadActorRegistryCloseDrainsSubmittedMailboxMutations(t *testing.T) {
	registry := newThreadActorRegistry()
	actor := registry.actor("thread-shutdown")
	firstEntered := make(chan struct{})
	firstRelease := make(chan struct{})
	secondDone := make(chan error, 1)
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- actor.apply(context.Background(), func() error {
			close(firstEntered)
			<-firstRelease
			return nil
		})
	}()
	select {
	case <-firstEntered:
	case <-time.After(time.Second):
		t.Fatal("first mutation did not enter actor")
	}
	go func() {
		secondDone <- actor.apply(context.Background(), func() error { return nil })
	}()
	deadline := time.Now().Add(time.Second)
	for {
		actor.submitMu.Lock()
		inFlight := actor.inFlight
		actor.submitMu.Unlock()
		if inFlight == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("second mutation was not accepted by actor before close")
		}
		time.Sleep(time.Millisecond)
	}

	closed := make(chan struct{})
	go func() {
		registry.close()
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatal("registry close returned while actor mutation was still running")
	case <-time.After(25 * time.Millisecond):
	}
	close(firstRelease)
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatalf("submitted mutation returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("submitted mailbox mutation was lost during close")
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("registry close did not complete after draining mailbox")
	}
}

func TestThreadActorFencesStaleAttemptBeforeLiveProjection(t *testing.T) {
	registry := newThreadActorRegistry()
	t.Cleanup(registry.close)
	actor := registry.actor("thread-attempt")
	current := Event{
		Type: observation.EventTypeProviderDelta, ThreadID: "thread-attempt", TurnID: "turn-attempt", RunID: "run-attempt",
		Stream: &StreamObservation{Type: StreamObservationAssistantDelta, Text: "current", LogicalRequestID: "logical-attempt", AttemptID: "attempt-2", AttemptEpoch: 2},
		Projection: &ThreadTurnProjection{
			ThreadID: "thread-attempt", TurnID: "turn-attempt", RunID: "run-attempt", Status: TurnStatusRunning,
			ThroughOrdinal: 2,
		},
	}
	if !actor.acceptLiveEvent(current) {
		t.Fatal("current attempt was rejected")
	}
	stale := current
	stale.Stream = &StreamObservation{Type: StreamObservationAssistantDelta, Text: "stale", LogicalRequestID: "logical-attempt", AttemptID: "attempt-1", AttemptEpoch: 1}
	stale.Projection = &ThreadTurnProjection{
		ThreadID: "thread-attempt", TurnID: "turn-attempt", RunID: "run-attempt", Status: TurnStatusRunning,
		ThroughOrdinal: 99,
	}
	if actor.acceptLiveEvent(stale) {
		t.Fatal("stale attempt entered the live actor projection")
	}
	if actor.state.attemptID != "attempt-2" || actor.state.attemptEpoch != 2 || actor.state.assistantDraft != "current" || actor.state.liveProjection == nil || actor.state.liveProjection.ThroughOrdinal != 2 {
		t.Fatalf("actor state = %#v", actor.state)
	}
	conflict := current
	conflict.Stream = &StreamObservation{LogicalRequestID: identity.LogicalRequestID("logical-attempt"), AttemptID: "attempt-conflict", AttemptEpoch: 2}
	if actor.acceptLiveEvent(conflict) {
		t.Fatal("conflicting attempt identity entered the live actor projection")
	}
}

func TestThreadActorFencesStaleAttemptBeforeSubscriptionBuffer(t *testing.T) {
	ctx := context.Background()
	host, err := Open(ctx, Options{Storage: storage.Memory()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })

	created, err := host.Threads().CreateThread(ctx, CreateThreadCommand{LogicalRequestID: "create-subscription-fence"})
	if err != nil {
		t.Fatal(err)
	}
	thread, err := host.Thread(ctx, created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := thread.Reader()
	if err != nil {
		t.Fatal(err)
	}
	view, err := reader.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	subscription, err := reader.Subscribe(ctx, SubscribeOptions{AfterRevision: view.Revision})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = subscription.Close() })

	current := Event{
		Type: observation.EventTypeProviderDelta, ThreadID: created.ThreadID, TurnID: "turn-subscription-fence", RunID: "run-subscription-fence",
		Stream: &StreamObservation{
			Type: StreamObservationAssistantDelta, Text: "current", LogicalRequestID: "logical-subscription-fence",
			AttemptID: "attempt-current", AttemptEpoch: 2,
		},
	}
	host.publishSubscriptionEvent(current)
	stale := current
	stale.Stream = &StreamObservation{
		Type: StreamObservationAssistantDelta, Text: "stale", LogicalRequestID: "logical-subscription-fence",
		AttemptID: "attempt-stale", AttemptEpoch: 1,
	}
	host.publishSubscriptionEvent(stale)

	message, err := subscription.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	event, ok := message.Transient()
	if !ok || event.Stream == nil || event.Stream.Text != "current" || event.Stream.AttemptID != "attempt-current" {
		t.Fatalf("subscription message = %#v, want only the current attempt", message)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if _, err := subscription.Next(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stale attempt entered subscription buffer: %v", err)
	}
}

func TestThreadActorMailboxPreservesConcurrentSubmissionOrder(t *testing.T) {
	registry := newThreadActorRegistry()
	t.Cleanup(registry.close)
	actor := registry.actor("thread-order")

	const count = 32
	start := make(chan struct{})
	var submitted sync.WaitGroup
	submitted.Add(count)
	results := make(chan int, count)
	for index := 0; index < count; index++ {
		index := index
		go func() {
			defer submitted.Done()
			<-start
			if err := actor.apply(context.Background(), func() error {
				results <- index
				return nil
			}); err != nil {
				t.Errorf("apply %d: %v", index, err)
			}
		}()
	}
	close(start)
	submitted.Wait()
	close(results)

	seen := make(map[int]bool, count)
	for index := range results {
		if seen[index] {
			t.Fatalf("actor executed message %d twice", index)
		}
		seen[index] = true
	}
	if len(seen) != count {
		t.Fatalf("actor executed %d messages, want %d", len(seen), count)
	}
}

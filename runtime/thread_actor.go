package runtime

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/floegence/floret/v3/identity"
	"github.com/floegence/floret/v3/observation"
)

// threadActor owns the ordered, short-lived runtime mutations for one thread.
// Provider I/O and approval waits must never run inside the mailbox.
type threadActor struct {
	threadID string
	mailbox  chan threadActorMessage
	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
	submitMu sync.Mutex
	closing  bool
	inFlight int
	submitWG sync.WaitGroup
	state    threadActorState
}

type threadActorState struct {
	turnID           identity.TurnID
	runID            identity.RunID
	logicalRequestID identity.LogicalRequestID
	attemptID        string
	attemptEpoch     int
	assistantDraft   string
	thinkingDraft    string
	liveProjection   *ThreadTurnProjection
}

type threadActorMessage struct {
	ctx    context.Context
	mutate func() error
	done   chan error
}

type threadActorRegistry struct {
	mu     sync.Mutex
	actors map[string]*threadActor
	closed bool
}

func newThreadActorRegistry() *threadActorRegistry {
	return &threadActorRegistry{actors: make(map[string]*threadActor)}
}

func (registry *threadActorRegistry) actor(threadID string) *threadActor {
	threadID = strings.TrimSpace(threadID)
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if actor := registry.actors[threadID]; actor != nil {
		return actor
	}
	actor := &threadActor{
		threadID: threadID,
		mailbox:  make(chan threadActorMessage),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	if registry.closed {
		close(actor.stop)
		close(actor.done)
		return actor
	}
	registry.actors[threadID] = actor
	go actor.run()
	return actor
}

func (registry *threadActorRegistry) close() {
	if registry == nil {
		return
	}
	registry.mu.Lock()
	if registry.closed {
		registry.mu.Unlock()
		return
	}
	registry.closed = true
	actors := make([]*threadActor, 0, len(registry.actors))
	for _, actor := range registry.actors {
		actors = append(actors, actor)
	}
	registry.mu.Unlock()
	for _, actor := range actors {
		actor.beginClose()
	}
	for _, actor := range actors {
		actor.submitWG.Wait()
		actor.stopOnce.Do(func() { close(actor.stop) })
	}
	for _, actor := range actors {
		<-actor.done
	}
}

func (actor *threadActor) apply(ctx context.Context, mutate func() error) error {
	if actor == nil || mutate == nil {
		return errors.New("thread actor and mutation are required")
	}
	if ctx == nil {
		return errors.New("thread actor context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	actor.submitMu.Lock()
	if actor.closing {
		actor.submitMu.Unlock()
		return ErrHostClosed
	}
	actor.inFlight++
	actor.submitWG.Add(1)
	actor.submitMu.Unlock()
	defer func() {
		actor.submitMu.Lock()
		actor.inFlight--
		actor.submitMu.Unlock()
		actor.submitWG.Done()
	}()
	message := threadActorMessage{ctx: ctx, mutate: mutate, done: make(chan error, 1)}
	select {
	case <-actor.stop:
		return ErrHostClosed
	case <-ctx.Done():
		return ctx.Err()
	case actor.mailbox <- message:
	}
	return <-message.done
}

func (actor *threadActor) beginClose() {
	if actor == nil {
		return
	}
	actor.submitMu.Lock()
	actor.closing = true
	actor.submitMu.Unlock()
}

func (actor *threadActor) run() {
	defer close(actor.done)
	for {
		select {
		case <-actor.stop:
			return
		case message := <-actor.mailbox:
			if err := message.ctx.Err(); err != nil {
				message.done <- err
				continue
			}
			message.done <- message.mutate()
		}
	}
}

// acceptLiveEvent applies the current provider-attempt fence and records the
// latest memory-only projection. It is called only from the actor mailbox.
func (actor *threadActor) acceptLiveEvent(event Event) bool {
	if actor == nil {
		return false
	}
	if event.TurnID != "" && actor.state.turnID != "" && event.TurnID != actor.state.turnID {
		actor.state = threadActorState{}
	}
	if event.TurnID != "" {
		actor.state.turnID = event.TurnID
	}
	if event.RunID != "" {
		actor.state.runID = event.RunID
	}
	logicalRequestID, attemptID, attemptEpoch, hasAttempt := liveEventAttemptIdentity(event)
	if hasAttempt {
		switch {
		case attemptEpoch < actor.state.attemptEpoch:
			return false
		case attemptEpoch == actor.state.attemptEpoch && actor.state.attemptID != "" && attemptID != actor.state.attemptID:
			return false
		case attemptEpoch > actor.state.attemptEpoch:
			actor.state.assistantDraft = ""
			actor.state.thinkingDraft = ""
			actor.state.liveProjection = nil
		}
		actor.state.logicalRequestID = logicalRequestID
		actor.state.attemptID = attemptID
		actor.state.attemptEpoch = attemptEpoch
	}
	if event.Stream != nil {
		switch event.Stream.Type {
		case StreamObservationAssistantDelta:
			actor.state.assistantDraft += event.Stream.Text
		case StreamObservationReasoningDelta:
			actor.state.thinkingDraft += event.Stream.Text
		}
	}
	if event.Projection != nil {
		actor.state.liveProjection = cloneThreadTurnProjectionPtr(event.Projection)
	}
	return true
}

func liveEventAttemptIdentity(event Event) (identity.LogicalRequestID, string, int, bool) {
	if event.Stream != nil && event.Stream.AttemptEpoch > 0 && strings.TrimSpace(event.Stream.AttemptID) != "" {
		return event.Stream.LogicalRequestID, strings.TrimSpace(event.Stream.AttemptID), event.Stream.AttemptEpoch, true
	}
	if event.Type != observation.EventTypeProviderRequest {
		return "", "", 0, false
	}
	metadata := event.Metadata
	if metadata == nil {
		return "", "", 0, false
	}
	logicalRequestID := identity.LogicalRequestID(stringFromMetadata(metadata, "logical_request_id"))
	attemptID := strings.TrimSpace(stringFromMetadata(metadata, "attempt_id"))
	attemptEpoch := intFromMetadata(metadata, "attempt_epoch")
	return logicalRequestID, attemptID, attemptEpoch, attemptEpoch > 0 && attemptID != ""
}

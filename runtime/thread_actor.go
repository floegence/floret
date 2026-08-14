package runtime

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/floegence/floret/v4/identity"
	"github.com/floegence/floret/v4/observation"
)

// threadRuntimeState is the only in-memory lifecycle owner for one thread.
// Provider and tool I/O must never run while its mutex is held.
type threadRuntimeState struct {
	threadID string
	mu       sync.Mutex
	closed   bool
	state    threadRuntimeData
}

type threadRuntimeData struct {
	turnID              identity.TurnID
	runID               identity.RunID
	logicalRequestID    identity.LogicalRequestID
	attemptID           string
	attemptEpoch        int
	assistantDraft      string
	thinkingDraft       string
	view                ThreadView
	cancel              context.CancelFunc
	cancelOwner         string
	executionDone       <-chan struct{}
	requestKeys         map[string]threadRuntimeRequest
	agent               *Agent
	pendingInteractions map[string]*pendingThreadInteraction
	hydrationStarted    bool
}

type pendingThreadInteraction struct {
	resolution chan InteractionResolution
}

func (runtime *threadRuntimeState) apply(ctx context.Context, mutate func() error) error {
	if runtime == nil || mutate == nil {
		return errors.New("thread runtime and mutation are required")
	}
	if ctx == nil {
		return errors.New("thread actor context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.closed {
		return ErrHostClosed
	}
	return mutate()
}

// acceptLiveEvent applies the current provider-attempt fence and records the
// latest memory-only projection. It is called only from the actor mailbox.
func (runtime *threadRuntimeState) acceptLiveEvent(event Event) bool {
	if runtime == nil {
		return false
	}
	if event.TurnID != "" && runtime.state.turnID != "" && event.TurnID != runtime.state.turnID {
		return false
	}
	if event.TurnID != "" {
		runtime.state.turnID = event.TurnID
	}
	if event.RunID != "" {
		runtime.state.runID = event.RunID
	}
	logicalRequestID, attemptID, attemptEpoch, hasAttempt := liveEventAttemptIdentity(event)
	if hasAttempt {
		switch {
		case attemptEpoch < runtime.state.attemptEpoch:
			return false
		case attemptEpoch == runtime.state.attemptEpoch && runtime.state.attemptID != "" && attemptID != runtime.state.attemptID:
			return false
		case attemptEpoch > runtime.state.attemptEpoch:
			runtime.state.assistantDraft = ""
			runtime.state.thinkingDraft = ""
		}
		runtime.state.logicalRequestID = logicalRequestID
		runtime.state.attemptID = attemptID
		runtime.state.attemptEpoch = attemptEpoch
	}
	if event.Stream != nil {
		switch event.Stream.Type {
		case StreamObservationAssistantDelta:
			runtime.state.assistantDraft += event.Stream.Text
		case StreamObservationReasoningDelta:
			runtime.state.thinkingDraft += event.Stream.Text
		}
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

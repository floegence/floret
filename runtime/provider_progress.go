package runtime

import (
	"reflect"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/observation"
	"github.com/floegence/floret/v7/tools"
)

// ProviderRequestProgress is memory-only feedback for the current request attempt.
// EstimatedOutputTokens measures received text, reasoning and local arguments;
// it is not provider Usage, billable tokens or context-window consumption.
type ProviderRequestProgress struct {
	LogicalRequestID      identity.LogicalRequestID `json:"logical_request_id"`
	AttemptID             string                    `json:"attempt_id"`
	AttemptEpoch          int                       `json:"attempt_epoch"`
	StartedAt             time.Time                 `json:"started_at"`
	FirstOutputAt         *time.Time                `json:"first_output_at,omitempty"`
	EstimatedOutputTokens int64                     `json:"estimated_output_tokens"`
}

// ThreadToolCallPreview describes unvalidated arguments, never a running tool.
// It has no canonical item ordinal or durable representation. Matching admitted
// calls may use its counts until their authoritative result arrives.
type ThreadToolCallPreview struct {
	ThreadID          identity.ThreadID         `json:"thread_id"`
	TurnID            identity.TurnID           `json:"turn_id"`
	RunID             identity.RunID            `json:"run_id"`
	LogicalRequestID  identity.LogicalRequestID `json:"logical_request_id"`
	AttemptID         string                    `json:"attempt_id"`
	AttemptEpoch      int                       `json:"attempt_epoch"`
	CallID            string                    `json:"call_id"`
	Name              string                    `json:"name"`
	ArgumentsComplete bool                      `json:"arguments_complete"`
	Preview           *tools.ArgumentPreview    `json:"preview"`
}

func mergeRequestProgress(previous, next *ThreadRunProgress, event Event) *ThreadRunProgress {
	if previous != nil {
		next.Request = cloneThreadRunProgress(previous).Request
	}
	switch event.Type {
	case observation.EventTypeStepStart, observation.EventTypeProviderRetry:
		next.Request = nil
	case observation.EventTypeProviderRequest:
		logical, attempt, epoch, ok := liveEventAttemptIdentity(event)
		if ok {
			next.Request = &ProviderRequestProgress{LogicalRequestID: logical, AttemptID: attempt, AttemptEpoch: epoch, StartedAt: event.Timestamp}
		} else {
			next.Request = nil
		}
	default:
		if next.Request != nil && event.Stream != nil {
			stream := event.Stream
			meaningful := stream.Text != "" || stream.ToolCallStream != nil
			if meaningful && next.Request.FirstOutputAt == nil {
				at := event.Timestamp
				next.Request.FirstOutputAt = &at
			}
			if stream.EstimatedOutputTokens > next.Request.EstimatedOutputTokens {
				next.Request.EstimatedOutputTokens = stream.EstimatedOutputTokens
			}
		}
	}
	return next
}

func cloneToolCallPreviews(in []ThreadToolCallPreview) []ThreadToolCallPreview {
	if in == nil {
		return nil
	}
	out := append([]ThreadToolCallPreview{}, in...)
	for i := range out {
		out[i].Preview = out[i].Preview.Clone()
	}
	return out
}

func (actor *threadRuntimeState) applyArgumentPreview(event Event) bool {
	before := cloneToolCallPreviews(actor.state.view.ToolCallPreviews)
	if event.Type == observation.EventTypeStepStart || event.Type == observation.EventTypeProviderRequest || event.Type == observation.EventTypeProviderRetry || event.Type == observation.EventTypeRunEnd {
		actor.state.view.ToolCallPreviews = nil
	}
	if event.Type == observation.EventTypeToolResult {
		actor.removeArgumentPreview(event.ToolID)
	}
	if stream := event.Stream; stream != nil && stream.ArgumentPreviewUpdated && stream.ToolCallStream != nil {
		call := stream.ToolCallStream
		if stream.ArgumentPreview == nil {
			actor.removeArgumentPreview(call.ID)
		} else {
			item := ThreadToolCallPreview{ThreadID: event.ThreadID, TurnID: event.TurnID, RunID: event.RunID, LogicalRequestID: stream.LogicalRequestID, AttemptID: stream.AttemptID, AttemptEpoch: stream.AttemptEpoch, CallID: call.ID, Name: call.Name, ArgumentsComplete: stream.Type == StreamObservationToolCallEnd, Preview: stream.ArgumentPreview.Clone()}
			found := false
			for i, old := range actor.state.view.ToolCallPreviews {
				if old.CallID == call.ID {
					actor.state.view.ToolCallPreviews[i] = item
					found = true
					break
				}
			}
			if !found {
				actor.state.view.ToolCallPreviews = append(actor.state.view.ToolCallPreviews, item)
			}
		}
	}
	if stream := event.Stream; stream != nil && stream.Type == StreamObservationToolCallEnd && stream.ToolCallStream != nil {
		for i := range actor.state.view.ToolCallPreviews {
			if actor.state.view.ToolCallPreviews[i].CallID == stream.ToolCallStream.ID {
				actor.state.view.ToolCallPreviews[i].ArgumentsComplete = true
			}
		}
	}
	actor.pruneArgumentPreviews()
	return !reflect.DeepEqual(before, actor.state.view.ToolCallPreviews)
}

func (actor *threadRuntimeState) removeArgumentPreview(callID string) {
	for i, item := range actor.state.view.ToolCallPreviews {
		if item.CallID == callID {
			actor.state.view.ToolCallPreviews = append(actor.state.view.ToolCallPreviews[:i], actor.state.view.ToolCallPreviews[i+1:]...)
			if len(actor.state.view.ToolCallPreviews) == 0 {
				actor.state.view.ToolCallPreviews = nil
			}
			return
		}
	}
}

func (actor *threadRuntimeState) pruneArgumentPreviews() {
	if actor.state.view.Activity == ThreadActivityIdle && actor.state.view.LastOutcome != nil {
		actor.state.view.ToolCallPreviews = nil
		return
	}
	for _, item := range actor.state.view.Items {
		if item.Activity == nil {
			continue
		}
		switch item.Activity.Status {
		case observation.ActivityStatusSuccess, observation.ActivityStatusError, observation.ActivityStatusDeclined, observation.ActivityStatusCanceled:
			actor.removeArgumentPreview(item.Activity.ToolID)
		}
	}
}

package runtime

import (
	"context"
	"errors"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
)

// ThreadParentEventService is an optional Host-owned passive delivery capability.
// Hosts authorize each read. Reporting additionally verifies the canonical active
// invocation and derives the recipient from the direct parent relationship.
type ThreadParentEventService interface {
	ReportToParent(context.Context, ReportToParentInput) (ParentEventReceipt, error)
	ListParentEvents(context.Context, ListParentEventsInput) (ParentEventPage, error)
	ReadParentEvents(context.Context, ReadParentEventsInput) (ParentEventPage, error)
}

// ReportToParentInput contains host-supplied invocation identity, never model routing.
type ReportToParentInput struct {
	ThreadID     identity.ThreadID
	TurnID       identity.TurnID
	RunID        identity.RunID
	ToolCallID   string
	ToolName     string
	ArgumentHash string
	RequestKey   RequestKey
	Message      string
}

type ParentEventReceipt struct {
	Accepted       bool              `json:"accepted"`
	EventID        string            `json:"event_id"`
	ParentThreadID identity.ThreadID `json:"parent_thread_id"`
}

// ParentEventKind classifies a canonical child report or critical source fact.
type ParentEventKind string

const (
	ParentEventReport           ParentEventKind = "report"
	ParentEventCompleted        ParentEventKind = "completed"
	ParentEventFailed           ParentEventKind = "failed"
	ParentEventCancelled        ParentEventKind = "cancelled"
	ParentEventInterrupted      ParentEventKind = "interrupted"
	ParentEventTimedOut         ParentEventKind = "timed_out"
	ParentEventInputRequired    ParentEventKind = "input_required"
	ParentEventApprovalRequired ParentEventKind = "approval_required"
)

// ParentEvent is detached; reads include complete report text. Admitted means
// canonical context placement, not proof that a model read the event.
type ParentEvent struct {
	ID             string            `json:"id"`
	ParentThreadID identity.ThreadID `json:"parent_thread_id"`
	SourceThreadID identity.ThreadID `json:"source_thread_id"`
	SourceTurnID   identity.TurnID   `json:"source_turn_id"`
	SourceRunID    identity.RunID    `json:"source_run_id"`
	SourceEntryID  string            `json:"source_entry_id"`
	Kind           ParentEventKind   `json:"kind"`
	ReasonCode     string            `json:"reason_code,omitempty"`
	InteractionID  string            `json:"interaction_id,omitempty"`
	Message        string            `json:"message,omitempty"`
	ReceivedAt     string            `json:"received_at"`
	Admitted       bool              `json:"admitted"`
	Resolved       bool              `json:"resolved,omitempty"`
}

// ParentEventPage identifies the queried canonical thread path. Inherited events
// retain their original ParentThreadID, which may differ from ThreadID.
type ParentEventPage struct {
	ThreadID         identity.ThreadID `json:"thread_id"`
	Events           []ParentEvent     `json:"events"`
	FirstEventID     string            `json:"first_event_id,omitempty"`
	LastEventID      string            `json:"last_event_id,omitempty"`
	NextAfterEventID string            `json:"next_after_event_id,omitempty"`
}

type ListParentEventsInput struct {
	ThreadID        identity.ThreadID
	SourceThreadIDs []identity.ThreadID
	AfterEventID    string
	Limit           int
	PendingOnly     bool
}

// ReadParentEventsInput selects one event or a closed immutable event interval.
// Continue with the returned fixed boundaries and explicit NextAfterEventID.
type ReadParentEventsInput struct {
	ThreadID     identity.ThreadID
	EventID      string
	FirstEventID string
	LastEventID  string
	AfterEventID string
	Limit        int
}

var _ ThreadParentEventService = (*threadRuntimeService)(nil)

func (service *threadRuntimeService) parentEventsRepo() (sessiontree.ParentEventRepo, error) {
	if service == nil || service.host == nil || !service.host.enableParentEvents {
		return nil, ErrUnsupportedStoreCapability
	}
	if err := service.host.available(); err != nil {
		return nil, err
	}
	repo, ok := service.host.store.repo.(sessiontree.ParentEventRepo)
	if !ok {
		return nil, ErrUnsupportedStoreCapability
	}
	return repo, nil
}

func (service *threadRuntimeService) ReportToParent(ctx context.Context, in ReportToParentInput) (ParentEventReceipt, error) {
	repo, err := service.parentEventsRepo()
	if err != nil {
		return ParentEventReceipt{}, err
	}
	if err := service.host.requireExecution(); err != nil {
		return ParentEventReceipt{}, err
	}
	event, err := repo.ReportToParent(ctx, sessiontree.ParentReportRequest{Invocation: sessiontree.EffectInvocationIdentity{ThreadID: in.ThreadID.String(), TurnID: in.TurnID.String(), RunID: in.RunID.String(), ToolCallID: in.ToolCallID, ToolName: in.ToolName, ArgumentHash: in.ArgumentHash}, RequestKey: string(in.RequestKey), Message: in.Message})
	if err != nil {
		return ParentEventReceipt{}, runtimeHostError(err)
	}
	return ParentEventReceipt{Accepted: true, EventID: event.ID, ParentThreadID: identity.ThreadID(event.ParentThreadID)}, nil
}

func (service *threadRuntimeService) ListParentEvents(ctx context.Context, in ListParentEventsInput) (ParentEventPage, error) {
	repo, err := service.parentEventsRepo()
	if err != nil {
		return ParentEventPage{}, err
	}
	if _, err := service.ensureThread(ctx, in.ThreadID); err != nil {
		return ParentEventPage{}, err
	}
	sources := make([]string, len(in.SourceThreadIDs))
	for i, id := range in.SourceThreadIDs {
		sources[i] = id.String()
	}
	page, err := repo.ParentEvents(ctx, sessiontree.ParentEventQuery{ThreadID: in.ThreadID.String(), SourceThreadIDs: sources, AfterEventID: in.AfterEventID, Limit: in.Limit, PendingOnly: in.PendingOnly})
	if err != nil {
		return ParentEventPage{}, runtimeHostError(err)
	}
	return publicParentEventPage(in.ThreadID, page, false), nil
}

func (service *threadRuntimeService) ReadParentEvents(ctx context.Context, in ReadParentEventsInput) (ParentEventPage, error) {
	repo, err := service.parentEventsRepo()
	if err != nil {
		return ParentEventPage{}, err
	}
	if in.EventID == "" && (in.FirstEventID == "" || in.LastEventID == "") {
		return ParentEventPage{}, errors.New("read requires an event ID or fixed first and last event IDs")
	}
	if in.EventID != "" && (in.FirstEventID != "" || in.LastEventID != "") {
		return ParentEventPage{}, ErrRequestConflict
	}
	if _, err := service.ensureThread(ctx, in.ThreadID); err != nil {
		return ParentEventPage{}, err
	}
	page, err := repo.ParentEvents(ctx, sessiontree.ParentEventQuery{ThreadID: in.ThreadID.String(), EventID: in.EventID, FirstEventID: in.FirstEventID, LastEventID: in.LastEventID, AfterEventID: in.AfterEventID, Limit: in.Limit})
	if err != nil {
		return ParentEventPage{}, runtimeHostError(err)
	}
	return publicParentEventPage(in.ThreadID, page, true), nil
}

func publicParentEventPage(threadID identity.ThreadID, page sessiontree.ParentEventPage, full bool) ParentEventPage {
	result := ParentEventPage{ThreadID: threadID, Events: make([]ParentEvent, 0, len(page.Events)), FirstEventID: page.FirstEventID, LastEventID: page.LastEventID, NextAfterEventID: page.NextAfterEventID}
	for _, event := range page.Events {
		message := ""
		if full {
			message = event.Message
		}
		result.Events = append(result.Events, ParentEvent{ID: event.ID, ParentThreadID: identity.ThreadID(event.ParentThreadID), SourceThreadID: identity.ThreadID(event.SourceThreadID), SourceTurnID: identity.TurnID(event.SourceTurnID), SourceRunID: identity.RunID(event.SourceRunID), SourceEntryID: event.SourceEntryID, Kind: ParentEventKind(event.Kind), ReasonCode: event.ReasonCode, InteractionID: event.InteractionID, Message: message, ReceivedAt: event.ReceivedAt.UTC().Format("2006-01-02T15:04:05.999999999Z"), Admitted: event.Admitted, Resolved: event.Resolved})
	}
	return result
}

func (service *threadRuntimeService) parentEventView(ctx context.Context, threadID identity.ThreadID) (*ParentEventPage, error) {
	if !service.host.enableParentEvents {
		return nil, nil
	}
	repo, err := service.parentEventsRepo()
	if err != nil {
		return nil, err
	}
	page, err := repo.ParentEvents(ctx, sessiontree.ParentEventQuery{ThreadID: threadID.String(), Limit: 100})
	if err != nil {
		return nil, runtimeHostError(err)
	}
	result := publicParentEventPage(threadID, page, false)
	return &result, nil
}

// A committed receipt/admission only refreshes observation. No call to View,
// Send, Steer or recovery is permitted here, including for an unhydrated parent.
func (host *Host) observeParentEvents(threadIDs []string) {
	host.threadRuntimeMu.Lock()
	service := host.threadRuntime
	host.threadRuntimeMu.Unlock()
	if service == nil {
		return
	}
	for _, id := range threadIDs {
		threadID := identity.ThreadID(id)
		actor := service.runtime(threadID)
		var view ThreadView
		err := actor.apply(context.Background(), func() error {
			if actor.state.view.ViewVersion == 0 {
				return nil
			}
			page, err := service.parentEventView(context.Background(), threadID)
			if err != nil {
				return err
			}
			actor.state.view.ParentEvents = page
			actor.state.view.ViewVersion++
			view = cloneThreadRuntimeView(actor.state.view)
			return nil
		})
		if err == nil && view.ViewVersion > 0 {
			service.publish(view)
		}
	}
}

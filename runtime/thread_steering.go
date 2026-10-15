package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/agentharness"
	"github.com/floegence/floret/v7/internal/engine"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
)

// ThreadSteeringController is an optional capability of Host.ThreadService.
// Steering interrupts the current execution fragment, preserving the task and
// confirmed tool results. Accepted input is durable before this method returns.
type ThreadSteeringController interface {
	Steer(context.Context, SteerInput) (ThreadView, error)
	SteerQueued(context.Context, SteerQueuedInput) (ThreadView, error)
}

type SteerInput struct {
	ThreadID   identity.ThreadID `json:"thread_id"`
	Input      UserInput         `json:"input"`
	RequestKey RequestKey        `json:"request_key"`
}

// SteerQueuedInput atomically consumes the exact pending input. An edited,
// removed, or already consumed item returns ErrRequestConflict.
type SteerQueuedInput struct {
	ThreadID      identity.ThreadID `json:"thread_id"`
	QueueItemID   string            `json:"queue_item_id"`
	ExpectedInput UserInput         `json:"expected_input"`
	RequestKey    RequestKey        `json:"request_key"`
}

type ThreadSteeringStatus string

const (
	ThreadSteeringPending   ThreadSteeringStatus = "pending"
	ThreadSteeringApplied   ThreadSteeringStatus = "applied"
	ThreadSteeringCancelled ThreadSteeringStatus = "cancelled"
)

type ThreadSteering struct {
	Status ThreadSteeringStatus `json:"status"`
	// Reason classifies cancelled input: stopped, effect_outcome_unknown, task_ended, or fork.
	Reason string `json:"reason,omitempty"`
}

var _ ThreadSteeringController = (*threadRuntimeService)(nil)

func (service *threadRuntimeService) Steer(ctx context.Context, in SteerInput) (ThreadView, error) {
	return service.steer(ctx, in, nil)
}

func (service *threadRuntimeService) SteerQueued(ctx context.Context, in SteerQueuedInput) (ThreadView, error) {
	if strings.TrimSpace(in.QueueItemID) == "" {
		return ThreadView{}, ErrRequestConflict
	}
	return service.steer(ctx, SteerInput{ThreadID: in.ThreadID, Input: in.ExpectedInput, RequestKey: in.RequestKey}, &in)
}

func (service *threadRuntimeService) steer(ctx context.Context, in SteerInput, queued *SteerQueuedInput) (ThreadView, error) {
	if err := service.host.requireExecution(); err != nil {
		return ThreadView{}, err
	}
	key, err := cleanRequestKey(in.RequestKey)
	if err != nil {
		return ThreadView{}, err
	}
	input, err := normalizeTurnInput(in.Input)
	if err != nil {
		return ThreadView{}, err
	}
	if _, err := service.View(ctx, in.ThreadID); err != nil {
		return ThreadView{}, err
	}
	fingerprint, err := stableFingerprint(input)
	if queued != nil {
		fingerprint, err = stableFingerprint(*queued)
	}
	if err != nil {
		return ThreadView{}, err
	}
	actor := service.runtime(in.ThreadID)
	for {
		var idle, replay, suspended bool
		var wait <-chan struct{}
		var current ThreadView
		err = actor.apply(ctx, func() error {
			entry, readErr := service.host.store.repo.Entry(ctx, in.ThreadID.String(), "steer:"+key)
			if readErr == nil {
				if entry.Type != sessiontree.EntrySteeringRequested || entry.RequestFingerprint != fingerprint {
					return ErrRequestConflict
				}
				replay = true
				current = cloneThreadRuntimeView(actor.state.view)
				return nil
			}
			if !errors.Is(readErr, sessiontree.ErrEntryNotFound) {
				return runtimeHostError(readErr)
			}
			if queued != nil {
				promotion, readErr := service.host.store.repo.Entry(ctx, in.ThreadID.String(), "queue-promoted:"+queued.QueueItemID)
				if readErr == nil {
					if promotion.RequestKey != key || promotion.RequestFingerprint != fingerprint {
						return ErrRequestConflict
					}
					replay = true
					current = cloneThreadRuntimeView(actor.state.view)
					return nil
				}
				if !errors.Is(readErr, sessiontree.ErrEntryNotFound) {
					return readErr
				}
			}
			if existing, ok, readErr := service.lookupSendEntry(ctx, in.ThreadID, key); readErr != nil {
				return readErr
			} else if ok && queued == nil {
				if existing.RequestFingerprint != fingerprint {
					return ErrRequestConflict
				}
				replay = true
				current = cloneThreadRuntimeView(actor.state.view)
				return nil
			}
			if actor.state.view.Cancellation != nil && actor.state.view.Activity == ThreadActivityActive {
				return ErrThreadBusy
			}
			if actor.state.view.TurnKind == TurnKindContextCompaction && actor.state.view.Activity == ThreadActivityActive {
				return ErrThreadBusy
			}
			if actor.state.executionSealed && actor.state.view.Activity == ThreadActivityActive {
				wait = actor.state.executionDone
				return nil
			}
			idle = actor.state.view.Activity != ThreadActivityActive
			if idle {
				return nil
			}
			entries := []sessiontree.Entry{}
			queueIndex := -1
			if queued != nil {
				for i, item := range actor.state.view.Queue {
					if item.ID == queued.QueueItemID {
						expected, _ := stableFingerprint(queued.ExpectedInput)
						actual, _ := stableFingerprint(item.Input)
						if expected != actual || len(item.SupplementalContext) != 0 {
							return ErrRequestConflict
						}
						queueIndex = i
						break
					}
				}
				if queueIndex < 0 {
					return ErrRequestConflict
				}
				payload, _ := json.Marshal(queued.QueueItemID)
				entries = append(entries, sessiontree.Entry{ID: "steer-queue:" + key, Type: sessiontree.EntryQueueDeleted, ThreadID: in.ThreadID.String(), RequestKey: key, RequestFingerprint: fingerprint, Payload: payload})
			}
			now := time.Now().UTC()
			request := sessiontree.Entry{ID: "steer:" + key, Type: sessiontree.EntrySteeringRequested, ThreadID: in.ThreadID.String(), TurnID: actor.state.turnID.String(), RunID: actor.state.runID.String(), RequestKey: key, RequestFingerprint: fingerprint, CreatedAt: now,
				Message: session.Message{Role: session.User, Content: input.Text, Attachments: sessionMessageAttachments(input.Attachments), References: sessionMessageReferences(input.References), Context: sessionMessageContext(input.Context), ModelContextSnapshot: sessionModelContext(input.ModelContextSnapshot)}}
			entries = append(entries, request)
			if err := service.appendRuntimeFactsError(ctx, in.ThreadID, entries); err != nil {
				return err
			}
			if queueIndex >= 0 {
				actor.state.view.Queue = slices.Delete(actor.state.view.Queue, queueIndex, queueIndex+1)
			}
			actor.state.steering = append(actor.state.steering, request.ID)
			actor.state.view.Items = appendThreadItem(actor.state.view.Items, ThreadItem{ID: request.ID, TurnID: actor.state.turnID, RunID: actor.state.runID, Kind: ThreadItemSteering, Text: input.Text, CreatedAt: now, Attachments: cloneMessageAttachments(input.Attachments), References: slices.Clone(input.References), Context: slices.Clone(input.Context), Steering: &ThreadSteering{Status: ThreadSteeringPending}})
			actor.state.view.ViewVersion++
			if actor.state.fragmentCancel != nil {
				actor.state.fragmentCancel(&agentharness.GracefulCancellation{Deadline: time.Now().Add(gracefulCancellationLimit)})
			}
			suspended = actor.state.cancel == nil
			current = cloneThreadRuntimeView(actor.state.view)
			return nil
		})
		if err != nil {
			return ThreadView{}, err
		}
		if replay {
			return current, nil
		}
		if wait != nil {
			select {
			case <-ctx.Done():
				return ThreadView{}, ctx.Err()
			case <-wait:
			}
			latest := service.currentView(actor)
			if latest.Cancellation != nil {
				return ThreadView{}, ErrRequestConflict
			}
			continue
		}
		if idle {
			if queued != nil {
				current, err = service.startAccepted(ctx, actor, in.ThreadID, input, nil, key, queued.QueueItemID, key, queued)
			} else {
				current, err = service.startAccepted(ctx, actor, in.ThreadID, input, nil, key, "", "")
			}
			if errors.Is(err, ErrThreadBusy) {
				continue
			}
			if err != nil {
				return ThreadView{}, err
			}
		}
		service.publish(current)
		if suspended {
			go service.continueCanonicalInput(actor, in.ThreadID, ThreadInteraction{ID: "steer:" + key, TurnID: current.TurnID, RunID: current.RunID}, InteractionResolution{}, nil, true)
		}
		return current, nil
	}
}

func (service *threadRuntimeService) steeringControl(actor *threadRuntimeState) *engine.SteeringControl {
	var turnID identity.TurnID
	var runID identity.RunID
	_ = actor.apply(context.Background(), func() error { turnID, runID = actor.state.turnID, actor.state.runID; return nil })
	return &engine.SteeringControl{
		CanContinue: func(err error) bool {
			return !errors.Is(err, ErrEffectOutcomeUnknown) && !errors.Is(err, sessiontree.ErrEffectOutcomeUnknown)
		},
		Begin: func(ctx context.Context, prepare func() error) (context.Context, []session.Message, error) {
			var fragment context.Context
			var history []session.Message
			pending := false
			valid := func() error {
				if actor.state.turnID != turnID || actor.state.runID != runID || actor.state.view.Activity != ThreadActivityActive || actor.state.view.Cancellation != nil {
					return context.Canceled
				}
				return nil
			}
			install := func() {
				if actor.state.fragmentCancel != nil {
					actor.state.fragmentCancel(context.Canceled)
				}
				fragment, actor.state.fragmentCancel = context.WithCancelCause(ctx)
			}
			err := actor.apply(ctx, func() error {
				if err := valid(); err != nil {
					return err
				}
				pending = len(actor.state.steering) > 0
				if !pending {
					install()
				}
				return nil
			})
			if err != nil || !pending {
				return fragment, nil, err
			}
			// Projection settlement emits observations, so it must run outside the actor lock.
			if prepare != nil {
				if err := prepare(); err != nil {
					return nil, nil, err
				}
			}
			err = actor.apply(ctx, func() error {
				if err := valid(); err != nil {
					return err
				}
				if err := service.applySteering(ctx, actor, runID); err != nil {
					return err
				}
				meta, err := service.host.store.repo.Thread(ctx, actor.threadID)
				if err != nil {
					return err
				}
				path, err := service.host.store.repo.Path(ctx, actor.threadID, meta.LeafID)
				if err != nil {
					return err
				}
				history, err = sessiontree.BuildContextChecked(path, sessiontree.ContextOptions{})
				if err != nil {
					return err
				}
				install()
				return nil
			})
			if err == nil {
				service.publish(service.currentView(actor))
			}
			return fragment, history, err
		},
		Finish: func(ctx context.Context) (bool, error) {
			sealed := false
			err := actor.apply(ctx, func() error {
				if actor.state.runID != runID {
					// An answered input may claim its continuation before this
					// waiting execution drains. Its old result must still settle.
					sealed = true
					return nil
				}
				if actor.state.view.Cancellation != nil {
					return context.Canceled
				}
				if len(actor.state.steering) > 0 {
					return nil
				}
				actor.state.executionSealed = true
				sealed = true
				return nil
			})
			return sealed, err
		},
	}
}

func (service *threadRuntimeService) resolveSteeringInteractions(ctx context.Context, actor *threadRuntimeState) error {
	var facts []sessiontree.Entry
	resolution := InteractionResolution{Accepted: false, Outcome: "cancelled"}
	payload, _ := json.Marshal(resolution)
	for _, interaction := range actor.state.view.Interactions {
		if interaction.Resolved || interaction.TurnID != actor.state.turnID {
			continue
		}
		facts = append(facts, sessiontree.Entry{ID: "interaction-resolved:" + interaction.ID, Type: sessiontree.EntryInteractionDone, ThreadID: actor.threadID, TurnID: interaction.TurnID.String(), RunID: interaction.RunID.String(), Payload: payload})
	}
	if len(facts) > 0 {
		if err := service.appendRuntimeFactsError(ctx, identity.ThreadID(actor.threadID), facts); err != nil {
			return err
		}
	}
	for _, interaction := range actor.state.view.Interactions {
		if !interaction.Resolved && interaction.TurnID == actor.state.turnID {
			resolveThreadInteractionCanonical(&actor.state.view, interaction.ID, resolution)
			delete(actor.state.pendingInteractions, interaction.ID)
		}
	}
	return nil
}

func (service *threadRuntimeService) applySteering(ctx context.Context, actor *threadRuntimeState, runID identity.RunID) error {
	if err := service.resolveSteeringInteractions(ctx, actor); err != nil {
		return err
	}
	var facts []sessiontree.Entry
	for _, id := range actor.state.steering {
		payload, _ := json.Marshal(sessiontree.SteeringResolution{SteeringID: id})
		facts = append(facts, sessiontree.Entry{ID: "steer-resolved:" + id, Type: sessiontree.EntrySteeringApplied, ThreadID: actor.threadID, TurnID: actor.state.turnID.String(), RunID: runID.String(), Payload: payload})
	}
	if err := service.appendRuntimeFactsError(ctx, identity.ThreadID(actor.threadID), facts); err != nil {
		return err
	}
	for i := range actor.state.view.Items {
		item := &actor.state.view.Items[i]
		if slices.Contains(actor.state.steering, item.ID) {
			item.Steering = &ThreadSteering{Status: ThreadSteeringApplied}
		}
	}
	actor.state.steering = nil
	actor.state.view.ViewVersion++
	return nil
}

func steeringProjection(entries []sessiontree.Entry) (map[string]ThreadSteering, error) {
	states := make(map[string]ThreadSteering)
	terminal := make(map[string]bool)
	for _, entry := range entries {
		switch entry.Type {
		case sessiontree.EntrySteeringRequested:
			states[entry.ID] = ThreadSteering{Status: ThreadSteeringPending}
		case sessiontree.EntrySteeringApplied, sessiontree.EntrySteeringCancelled:
			resolution, err := sessiontree.DecodeSteeringResolution(entry)
			if err != nil {
				return nil, err
			}
			status := ThreadSteeringApplied
			if entry.Type == sessiontree.EntrySteeringCancelled {
				status = ThreadSteeringCancelled
			}
			states[resolution.SteeringID] = ThreadSteering{Status: status, Reason: resolution.Reason}
		case sessiontree.EntryTurnMarker:
			if entry.TurnStatus == sessiontree.TurnCompleted || entry.TurnStatus == sessiontree.TurnFailed || entry.TurnStatus == sessiontree.TurnAborted {
				terminal[entry.TurnID] = true
			}
		}
	}
	for _, entry := range entries {
		if entry.Type == sessiontree.EntrySteeringRequested && states[entry.ID].Status == ThreadSteeringPending && terminal[entry.TurnID] {
			states[entry.ID] = ThreadSteering{Status: ThreadSteeringCancelled, Reason: "task_ended"}
		}
	}
	return states, nil
}

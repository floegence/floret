package runtime

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
)

// ThreadMessageEditor is optional. A revision admits a new Turn in the same
// Thread; historical records and external effects are never undone.
type ThreadMessageEditor interface {
	LatestEditableMessage(context.Context, identity.ThreadID) (EditableMessage, error)
	EditLastUserMessage(context.Context, EditLastUserMessageInput) (ThreadCommandAck, error)
	LookupMessageEdit(context.Context, LookupMessageEditInput) (MessageEdit, bool, error)
	ReadMessageEditHistory(context.Context, LookupMessageEditInput) ([]ThreadItem, error)
}
type EditableMessage struct {
	RevisionRequestKey RequestKey      `json:"revision_request_key,omitempty"`
	ItemID             string          `json:"item_id"`
	TurnID             identity.TurnID `json:"turn_id"`
	Input              UserInput       `json:"input"`
}
type EditLastUserMessageInput struct {
	ThreadID      identity.ThreadID `json:"thread_id"`
	ItemID        string            `json:"item_id"`
	ExpectedInput UserInput         `json:"expected_input"`
	Input         UserInput         `json:"input"`
	RequestKey    RequestKey        `json:"request_key"`
	StopActive    bool              `json:"stop_active"`
}
type LookupMessageEditInput struct {
	ThreadID   identity.ThreadID `json:"thread_id"`
	RequestKey RequestKey        `json:"request_key"`
}
type MessageEdit struct {
	EditableMessage
	Replacement UserInput        `json:"replacement"`
	Ack         ThreadCommandAck `json:"ack"`
}

var _ ThreadMessageEditor = (*threadRuntimeService)(nil)

func canonicalEditInput(input UserInput) session.Message {
	return session.Message{Role: session.User, Content: input.Text, Attachments: sessionMessageAttachments(input.Attachments), References: sessionMessageReferences(input.References), Context: sessionMessageContext(input.Context), ModelContextSnapshot: sessionModelContext(input.ModelContextSnapshot)}
}
func (s *threadRuntimeService) LatestEditableMessage(ctx context.Context, id identity.ThreadID) (EditableMessage, error) {
	if _, err := s.ensureThread(ctx, id); err != nil {
		return EditableMessage{}, err
	}
	e, discardedTurns, revisionKey, err := s.latestEditableInput(ctx, id)
	if err != nil {
		return EditableMessage{}, runtimeHostError(err)
	}
	children, err := sessiontree.ListThreads(ctx, s.host.store.repo, sessiontree.ListThreadsOptions{IncludeArchived: true})
	if err != nil {
		return EditableMessage{}, runtimeHostError(err)
	}
	for _, child := range children {
		if child.ParentThreadID != id.String() || child.Lifecycle == sessiontree.ThreadLifecycleClosed {
			continue
		}
		_, discarded := discardedTurns[child.ParentTurnID]
		if child.ParentTurnID == "" || discarded {
			return EditableMessage{}, ErrThreadBusy
		}
	}
	return EditableMessage{ItemID: e.ID, TurnID: identity.TurnID(e.TurnID), Input: turnInputFromSessionMessage(e.Message), RevisionRequestKey: revisionKey}, nil
}

// Read one frozen path backwards in bounded, body-free transactions. Only the
// selected input is materialized. Retain distinct discarded turn identities for
// child admission, not the intervening tool/assistant history. Revision ancestry
// uses the same frozen boundary and never authorizes a superseded branch.
func (s *threadRuntimeService) latestEditableInput(ctx context.Context, id identity.ThreadID) (sessiontree.Entry, map[string]struct{}, RequestKey, error) {
	reader, paged := s.host.store.repo.(sessiontree.PathWindowReader)
	complete, exact := s.host.store.repo.(sessiontree.PathEntriesWindowReader)
	if !paged || !exact {
		// Released custom JournalRepo adapters retain their complete-read contract.
		path, err := s.host.store.repo.Path(ctx, id.String(), "")
		if err != nil {
			return sessiontree.Entry{}, nil, "", err
		}
		entry, err := sessiontree.LatestEditableInput(path)
		if err != nil {
			return sessiontree.Entry{}, nil, "", err
		}
		var key RequestKey
		for i, parent := range path {
			if parent.ID == entry.ParentID && i > 0 && path[i-1].Type == sessiontree.EntryMessageRevision {
				key = RequestKey(path[i-1].RequestKey)
			}
		}
		discarded := make(map[string]struct{})
		found := false
		for _, source := range path {
			found = found || source.ID == entry.ID
			if found && source.TurnID != "" {
				discarded[source.TurnID] = struct{}{}
			}
		}
		return entry, discarded, key, nil
	}
	options := sessiontree.PathWindowOptions{ThreadID: id.String(), Direction: sessiontree.PathWindowBefore, Limit: 128}
	turns := map[string]struct{}{}
	for {
		page, err := reader.PathMetadataWindow(ctx, options)
		if err != nil {
			return sessiontree.Entry{}, nil, "", err
		}
		options.LeafID = page.LeafID
		for i := len(page.Entries) - 1; i >= 0; i-- {
			entry := page.Entries[i].Entry
			candidate, err := sessiontree.LatestEditableInput([]sessiontree.Entry{entry})
			if err != nil && !errors.Is(err, sessiontree.ErrEntryNotFound) {
				return sessiontree.Entry{}, nil, "", err
			}
			if entry.TurnID != "" {
				turns[entry.TurnID] = struct{}{}
			}
			if err != nil {
				continue
			}
			parents, err := reader.PathMetadataWindow(ctx, sessiontree.PathWindowOptions{ThreadID: id.String(), LeafID: page.LeafID, AnchorID: candidate.ID, Direction: sessiontree.PathWindowBefore, Limit: 2})
			if err != nil {
				return sessiontree.Entry{}, nil, "", err
			}
			var key RequestKey
			if len(parents.Entries) == 2 && parents.Entries[1].Entry.ID == candidate.ParentID && parents.Entries[0].Entry.Type == sessiontree.EntryMessageRevision {
				key = RequestKey(parents.Entries[0].Entry.RequestKey)
			}
			selected, err := complete.PathEntriesWindow(ctx, sessiontree.PathWindowOptions{ThreadID: id.String(), LeafID: page.LeafID, AnchorID: candidate.ID, Direction: sessiontree.PathWindowAround, Limit: 1})
			if err != nil {
				return sessiontree.Entry{}, nil, "", err
			}
			if len(selected.Entries) != 1 || selected.Entries[0].ID != candidate.ID {
				return sessiontree.Entry{}, nil, "", ErrAuthorityCorrupt
			}
			return selected.Entries[0], turns, key, nil
		}
		if !page.HasBefore {
			return sessiontree.Entry{}, nil, "", sessiontree.ErrEntryNotFound
		}
		options.AnchorID = page.FirstEntryID
	}
}

func (s *threadRuntimeService) LookupMessageEdit(ctx context.Context, in LookupMessageEditInput) (MessageEdit, bool, error) {
	key, err := cleanRequestKey(in.RequestKey)
	if err != nil {
		return MessageEdit{}, false, err
	}
	if _, err = s.ensureThread(ctx, in.ThreadID); err != nil {
		return MessageEdit{}, false, err
	}
	entry, err := s.host.store.repo.Entry(ctx, in.ThreadID.String(), "message-edit:"+key)
	if errors.Is(err, sessiontree.ErrEntryNotFound) {
		return MessageEdit{}, false, nil
	}
	if err != nil {
		return MessageEdit{}, false, runtimeHostError(err)
	}
	revision, err := sessiontree.DecodeMessageRevision(entry)
	if err != nil {
		return MessageEdit{}, false, runtimeHostError(err)
	}
	if revision.SourceThreadID != in.ThreadID.String() {
		return MessageEdit{}, false, ErrRequestConflict
	}
	sourcePath, err := s.host.store.repo.Path(ctx, revision.SourceThreadID, revision.PreviousLeafID)
	if err != nil {
		return MessageEdit{}, false, runtimeHostError(err)
	}
	if err := sessiontree.ValidateMessageRevisionSource(entry, sourcePath); err != nil {
		return MessageEdit{}, false, runtimeHostError(err)
	}
	source, err := s.host.store.repo.Entry(ctx, revision.SourceThreadID, revision.SourceEntryID)
	if err != nil {
		return MessageEdit{}, false, runtimeHostError(err)
	}
	replacement, err := s.host.store.repo.Entry(ctx, in.ThreadID.String(), "user:"+key)
	if err != nil {
		return MessageEdit{}, false, runtimeHostError(err)
	}
	return MessageEdit{EditableMessage: EditableMessage{ItemID: source.ID, TurnID: identity.TurnID(source.TurnID), Input: turnInputFromSessionMessage(source.Message)}, Replacement: turnInputFromSessionMessage(replacement.Message), Ack: ThreadCommandAck{ThreadID: in.ThreadID, RequestKey: in.RequestKey, TurnID: identity.TurnID(replacement.TurnID), RunID: identity.RunID(replacement.RunID)}}, true, nil
}
func (s *threadRuntimeService) ReadMessageEditHistory(ctx context.Context, in LookupMessageEditInput) ([]ThreadItem, error) {
	if _, found, err := s.LookupMessageEdit(ctx, in); err != nil {
		return nil, err
	} else if !found {
		return nil, ErrRequestConflict
	}
	entry, err := s.host.store.repo.Entry(ctx, in.ThreadID.String(), "message-edit:"+string(in.RequestKey))
	if err != nil {
		return nil, runtimeHostError(err)
	}
	revision, err := sessiontree.DecodeMessageRevision(entry)
	if err != nil {
		return nil, runtimeHostError(err)
	}
	path, err := s.host.store.repo.Path(ctx, revision.SourceThreadID, revision.PreviousLeafID)
	if err != nil {
		return nil, runtimeHostError(err)
	}
	projection := newThreadCanonicalProjection()
	if err := projection.append(path); err != nil {
		return nil, err
	}
	view, err := projection.snapshot(ctx, s, in.ThreadID, path)
	return view.Items, err
}
func (s *threadRuntimeService) EditLastUserMessage(ctx context.Context, in EditLastUserMessageInput) (ThreadCommandAck, error) {
	if err := s.host.requireExecution(); err != nil {
		return ThreadCommandAck{}, err
	}
	key, err := cleanRequestKey(in.RequestKey)
	if err != nil {
		return ThreadCommandAck{}, err
	}
	in.RequestKey = RequestKey(key)
	in.ItemID = strings.TrimSpace(in.ItemID)
	in.ExpectedInput = turnInputFromSessionMessage(session.CloneMessage(canonicalEditInput(in.ExpectedInput)))
	in.Input, err = normalizeTurnInput(in.Input)
	if err != nil {
		return ThreadCommandAck{}, err
	}
	if previous, found, err := s.LookupMessageEdit(ctx, LookupMessageEditInput{in.ThreadID, in.RequestKey}); err != nil {
		return ThreadCommandAck{}, err
	} else if found {
		if previous.ItemID != in.ItemID || !reflect.DeepEqual(canonicalEditInput(previous.Input), canonicalEditInput(in.ExpectedInput)) || !reflect.DeepEqual(canonicalEditInput(previous.Replacement), canonicalEditInput(in.Input)) {
			return ThreadCommandAck{}, ErrRequestConflict
		}
		return previous.Ack, nil
	}
	if _, found, err := s.lookupSendEntry(ctx, in.ThreadID, key); err != nil {
		return ThreadCommandAck{}, err
	} else if found {
		return ThreadCommandAck{}, ErrRequestConflict
	}
	if _, err = s.viewResult(ctx, in.ThreadID, threadAckResult); err != nil {
		return ThreadCommandAck{}, err
	}
	actor := s.runtime(in.ThreadID)
	var active bool
	var turn identity.TurnID
	err = actor.apply(ctx, func() error {
		if actor.state.messageEditKey != "" {
			return ErrThreadBusy
		}
		target, err := s.LatestEditableMessage(ctx, in.ThreadID)
		if err != nil {
			return err
		}
		if target.ItemID != in.ItemID || !reflect.DeepEqual(canonicalEditInput(target.Input), canonicalEditInput(in.ExpectedInput)) {
			return ErrRequestConflict
		}
		active = actor.state.view.Activity == ThreadActivityActive
		turn = actor.state.turnID
		if active && !in.StopActive {
			return ErrThreadBusy
		}
		actor.state.messageEditKey = key
		return nil
	})
	if err != nil {
		return ThreadCommandAck{}, err
	}
	// The caller may disconnect after Stop. One bounded in-process owner finishes
	// this explicit transition; no pending edit is persisted or resumed on startup.
	type result struct {
		ack ThreadCommandAck
		err error
	}
	done := make(chan result, 1)
	go func() {
		transition, cancel := context.WithTimeout(context.Background(), 2*gracefulCancellationLimit+5*time.Second)
		defer cancel()
		defer func() {
			_ = actor.apply(context.Background(), func() error {
				if actor.state.messageEditKey == key {
					actor.state.messageEditKey = ""
				}
				return nil
			})
		}()
		if active {
			subscription, err := s.subscribe(transition, true)
			if err != nil {
				done <- result{err: err}
				return
			}
			defer subscription.Close()
			if _, err = s.requestGracefulCancellationResult(transition, actor, in.ThreadID, turn, "message-edit-stop:"+key, threadAckResult, nil); err != nil {
				done <- result{err: err}
				return
			}
			for {
				running := false
				err = actor.apply(transition, func() error { running = actor.state.view.Activity == ThreadActivityActive; return nil })
				if err != nil {
					done <- result{err: err}
					return
				}
				if !running {
					break
				}
				select {
				case <-transition.Done():
					done <- result{err: transition.Err()}
					return
				case <-subscription.done:
					done <- result{err: ErrHostClosed}
					return
				case <-subscription.changed:
				}
			}
		}
		ack, err := s.admitMessageEdit(transition, actor, in)
		done <- result{ack, err}
	}()
	select {
	case out := <-done:
		return out.ack, out.err
	case <-ctx.Done():
		return ThreadCommandAck{}, ctx.Err()
	}
}
func (s *threadRuntimeService) admitMessageEdit(ctx context.Context, actor *threadRuntimeState, in EditLastUserMessageInput) (ThreadCommandAck, error) {
	// Stop may outlive the caller and race Host shutdown. Publish canonical
	// admission and its execution owner under the existing shutdown boundary.
	s.host.mutationMu.Lock()
	defer s.host.mutationMu.Unlock()
	if err := s.host.requireExecution(); err != nil {
		return ThreadCommandAck{}, err
	}
	turn, run, err := s.host.nextTurnRunIDs()
	if err != nil {
		return ThreadCommandAck{}, err
	}
	fingerprint, err := stableFingerprint(in.Input)
	if err != nil {
		return ThreadCommandAck{}, err
	}
	request := turnExecutionRequest{LogicalRequestID: identity.LogicalRequestID(in.RequestKey), TurnID: turn, RunID: run, Input: in.Input, InputFingerprint: fingerprint, EditSourceEntryID: in.ItemID, EditExpectedInput: in.ExpectedInput}
	execution, cancel := context.WithCancelCause(context.Background())
	done := make(chan struct{})
	var previous <-chan struct{}
	var accepted acceptedTurn
	err = actor.apply(ctx, func() error {
		if actor.state.view.Activity == ThreadActivityActive {
			return ErrThreadBusy
		}
		var err error
		accepted, err = s.acceptCanonicalTurn(ctx, in.ThreadID, request)
		if err != nil {
			return err
		}
		canonical, projectionErr := s.loadCanonicalThreadView(context.WithoutCancel(ctx), in.ThreadID)
		actor.sourceWindow = nil
		actor.replaceItems(cloneThreadItems(canonical.Items))
		actor.replaceInteractions(nil)
		previous, err = actor.beginRun(beginThreadRunInput{turnID: turn, runID: run, logicalRequestID: identity.LogicalRequestID(in.RequestKey), cancel: cancel, executionDone: done, clearOutcome: true})
		if err != nil {
			return err
		}
		if projectionErr == nil {
			projectionErr = actor.installCanonicalView(canonical)
		}
		actor.state.contextErr = projectionErr
		actor.state.messageEditKey = ""
		return nil
	})
	if err != nil {
		cancel(context.Canceled)
		return ThreadCommandAck{}, err
	}
	prepared := s.prepareExecution(execution, actor, AgentRequest{ThreadID: in.ThreadID, TurnID: turn, RequestKey: string(in.RequestKey), Input: in.Input, CanonicalTurnInput: in.Input})
	go s.executePreparedSend(execution, actor, prepared, accepted, request, previous, done)
	return ThreadCommandAck{ThreadID: in.ThreadID, RequestKey: in.RequestKey, TurnID: turn, RunID: run}, nil
}

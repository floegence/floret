package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
)

// ThreadQueueController is an optional extension implemented by Host.ThreadService.
// It edits pending input or gracefully stops a response to send a chosen input.
// Existing ThreadService implementations remain source-compatible.
type ThreadQueueController interface {
	EditQueued(context.Context, EditQueuedInput) (ThreadView, error)
	SendQueuedNow(context.Context, PromoteQueuedInput) (ThreadView, error)
}

// EditQueuedInput uses ExpectedText to reject edits made against stale content.
// RequestKey identifies one edit, independently of the original send request.
// Text edits preserve attachments, references and context. To replace complete
// content, supply both ExpectedInput and Input, leaving ExpectedText and Text
// empty. The complete expected input rejects concurrent content changes even
// when text is unchanged. Both modes preserve identity, supplemental context and
// queue position. Already admitted turns cannot be edited through this boundary.
type EditQueuedInput struct {
	ThreadID      identity.ThreadID `json:"thread_id"`
	QueueItemID   string            `json:"queue_item_id"`
	ExpectedText  string            `json:"expected_text"`
	Text          string            `json:"text"`
	RequestKey    RequestKey        `json:"request_key"`
	ExpectedInput *UserInput        `json:"expected_input,omitempty"`
	Input         *UserInput        `json:"input,omitempty"`
}

var _ ThreadQueueController = (*threadRuntimeService)(nil)

func (service *threadRuntimeService) EditQueued(ctx context.Context, in EditQueuedInput) (ThreadView, error) {
	view, err := service.editQueuedCommand(ctx, in, threadFullResult)
	return service.completeViewResult(ctx, view, err)
}

func (service *threadRuntimeService) editQueuedCommand(ctx context.Context, in EditQueuedInput, mode threadResultMode) (ThreadView, error) {
	key, err := cleanRequestKey(in.RequestKey)
	if err != nil {
		return ThreadView{}, err
	}
	if _, err := service.viewResult(ctx, in.ThreadID, mode); err != nil {
		return ThreadView{}, err
	}
	queueID := strings.TrimSpace(in.QueueItemID)
	fingerprint, err := stableFingerprint(struct{ ID, Expected, Text string }{queueID, in.ExpectedText, in.Text})
	var expectedFingerprint string
	var replacement UserInput
	fullInput := in.ExpectedInput != nil || in.Input != nil
	if fullInput {
		if in.ExpectedInput == nil || in.Input == nil || in.ExpectedText != "" || in.Text != "" {
			return ThreadView{}, errors.New("complete queue edits require expected_input and input without text edit fields")
		}
		expected, normalizeErr := normalizeTurnInput(*in.ExpectedInput)
		if normalizeErr != nil {
			return ThreadView{}, normalizeErr
		}
		next, normalizeErr := normalizeTurnInput(*in.Input)
		if normalizeErr != nil {
			return ThreadView{}, normalizeErr
		}
		in.ExpectedInput, in.Input = &expected, &next
		expectedFingerprint, err = stableFingerprint(*in.ExpectedInput)
		if err != nil {
			return ThreadView{}, err
		}
		replacement = *in.Input
		replacement.Attachments = cloneMessageAttachments(replacement.Attachments)
		replacement.References = append([]MessageReference(nil), replacement.References...)
		replacement.Context = append([]MessageContextItem(nil), replacement.Context...)
		replacement.ModelContextSnapshot = runtimeModelContext(sessionModelContext(replacement.ModelContextSnapshot))
		fingerprint, err = stableFingerprint(struct {
			ID              string
			Expected, Input UserInput
		}{queueID, *in.ExpectedInput, replacement})
	}
	if err != nil {
		return ThreadView{}, err
	}
	actor := service.runtime(in.ThreadID)
	err = actor.apply(ctx, func() error {
		if previous, ok := actor.state.requestKeys[key]; ok {
			if previous.fingerprint != fingerprint {
				return ErrRequestConflict
			}
			return nil
		}
		index := -1
		for i, item := range actor.state.view.Queue {
			if item.ID == queueID {
				index = i
				break
			}
		}
		if index < 0 {
			return ErrRequestConflict
		}
		item := actor.state.view.Queue[index]
		if fullInput {
			currentFingerprint, err := stableFingerprint(item.Input)
			if err != nil {
				return err
			}
			if currentFingerprint != expectedFingerprint {
				return ErrRequestConflict
			}
			item.Input = replacement
		} else {
			if item.Input.Text != in.ExpectedText {
				return ErrRequestConflict
			}
			item.Input.Text = in.Text
		}
		if err := item.Input.Validate(); err != nil {
			return err
		}
		order := make([]string, len(actor.state.view.Queue))
		for i, queued := range actor.state.view.Queue {
			order[i] = queued.ID
		}
		// A single journal transaction reuses existing queue facts. There is no
		// externally visible deletion or schema change.
		payloads := []any{queueID, item, order}
		kinds := []sessiontree.EntryType{sessiontree.EntryQueueDeleted, sessiontree.EntryQueueAdded, sessiontree.EntryQueueReordered}
		suffixes := []string{"remove", "replace", "order"}
		entries := make([]sessiontree.Entry, len(kinds))
		for i, kind := range kinds {
			raw, marshalErr := json.Marshal(payloads[i])
			if marshalErr != nil {
				return marshalErr
			}
			entries[i] = sessiontree.Entry{ID: "queue-edit:" + key + ":" + suffixes[i], ThreadID: in.ThreadID.String(), Type: kind, RequestKey: key, RequestFingerprint: fingerprint, Payload: raw}
		}
		writer, ok := service.host.store.repo.(sessiontree.RuntimeJournalRepo)
		if !ok {
			return ErrUnsupportedStoreCapability
		}
		if _, err := writer.AppendRuntimeFacts(ctx, in.ThreadID.String(), entries); err != nil {
			return runtimeHostError(err)
		}
		actor.state.view.Queue[index] = item
		actor.markComponents(threadQueueChanged)
		actor.state.view.ViewVersion++
		actor.state.requestKeys[key] = threadRuntimeRequest{fingerprint: fingerprint}
		return nil
	})
	if err != nil {
		return ThreadView{}, err
	}
	view := service.currentResult(actor, mode)
	return view, nil
}

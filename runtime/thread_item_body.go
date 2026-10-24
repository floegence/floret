package runtime

import (
	"context"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/threadquery"
)

// ThreadItemBodyInput locates the current text source of one presentation item.
// The item identity is not authorization and must belong to the active path.
type ThreadItemBodyInput struct {
	ThreadID identity.ThreadID `json:"thread_id"`
	ItemID   string            `json:"item_id"`
}

// ThreadItemBodyReader resolves a source independently of preview size, without
// reading source bytes or creating a window subscription. ReadBody, BodyManifest
// and ReadLiveBody still require fresh authorization for every subsequent read.
// Unsupported or unavailable item text returns threadquery.ErrRecordUnavailable.
// PreviewBytes is zero because no preview is read.
type ThreadItemBodyReader interface {
	ItemBody(context.Context, threadquery.Scope, ThreadItemBodyInput) (ThreadItemBody, error)
}

var _ ThreadItemBodyReader = (*threadRuntimeService)(nil)

func (service *threadRuntimeService) ItemBody(ctx context.Context, scope threadquery.Scope, input ThreadItemBodyInput) (ThreadItemBody, error) {
	item, err := service.detailSourceItem(ctx, scope, input)
	if err != nil {
		return ThreadItemBody{}, err
	}
	var body ThreadItemBody

	switch item.Kind {
	case ThreadItemAssistant, ThreadItemThinking, ThreadItemUser:
	default:
		return ThreadItemBody{}, threadquery.ErrRecordUnavailable
	}
	body = ThreadItemBody{Field: ThreadBodyContent, Bytes: len(item.Text)}
	if item.Kind == ThreadItemThinking {
		body.Field = ThreadBodyReasoning
	}
	if item.textOmitted {
		body.Bytes = item.textBytes
	}
	if item.controlTextBytes > 0 {
		body.Bytes = item.controlTextBytes
	}
	if item.CanonicalOrdinal != 0 {
		if item.Kind == ThreadItemUser {
			body.EntryID = item.ID
		} else {
			body.SourceID = item.ID
		}
	} else if item.liveBody != nil {
		body.SourceID, body.RunID = item.ID, item.RunID
	} else {
		return ThreadItemBody{}, threadquery.ErrRecordUnavailable
	}
	return body, nil
}

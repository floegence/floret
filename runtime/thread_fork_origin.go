package runtime

import (
	"context"

	"github.com/floegence/floret/v7/identity"
)

// ThreadForkOrigin identifies the immutable source selected when a Fork was
// admitted. An empty SourceEntryID denotes an empty source path. RequestKey is
// the host's opaque Fork key, not an authorization grant or model input.
type ThreadForkOrigin struct {
	ReferenceHistory bool              `json:"reference_history,omitempty"`
	SourceThreadID   identity.ThreadID `json:"source_thread_id"`
	SourceEntryID    string            `json:"source_entry_id,omitempty"`
	RequestKey       RequestKey        `json:"request_key,omitempty"`
}

// ThreadForkOriginReader lets a host distinguish a fork it has not yet
// materialized from a new conversation. The host authorizes the destination;
// the returned origin does not authorize access to its source.
type ThreadForkOriginReader interface {
	ReadForkOrigin(context.Context, identity.ThreadID) (ThreadForkOrigin, bool, error)
}

var _ ThreadForkOriginReader = (*threadRuntimeService)(nil)

func (service *threadRuntimeService) ReadForkOrigin(ctx context.Context, destination identity.ThreadID) (ThreadForkOrigin, bool, error) {
	if _, err := identity.ParseThreadID(destination.String()); err != nil {
		return ThreadForkOrigin{}, false, err
	}
	meta, err := service.host.store.repo.Thread(ctx, destination.String())
	if err != nil {
		return ThreadForkOrigin{}, false, runtimeHostError(err)
	}
	if meta.ForkedFromThreadID == "" {
		return ThreadForkOrigin{}, false, nil
	}
	return ThreadForkOrigin{SourceThreadID: identity.ThreadID(meta.ForkedFromThreadID), SourceEntryID: meta.ForkedFromEntryID, RequestKey: RequestKey(meta.OriginRequestKey), ReferenceHistory: meta.ReferenceHistory}, true, nil
}

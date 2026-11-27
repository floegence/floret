package runtime

import (
	"context"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
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

// ThreadReferenceAttachmentReader returns the opaque host-owned descriptors
// in a destination's archived prefix, in canonical order. These facts are
// neither a direct user turn nor permission to load a resource. Authorize the
// destination before reading, and separately validate each resource grant.
type ThreadReferenceAttachmentReader interface {
	ReadReferenceAttachments(context.Context, identity.ThreadID) ([]MessageAttachment, error)
}

var _ ThreadReferenceAttachmentReader = (*threadRuntimeService)(nil)

func (service *threadRuntimeService) ReadReferenceAttachments(ctx context.Context, destination identity.ThreadID) ([]MessageAttachment, error) {
	if _, err := identity.ParseThreadID(destination.String()); err != nil {
		return nil, err
	}
	reader, ok := service.host.store.repo.(sessiontree.PathEntriesWindowReader)
	if !ok {
		return nil, ErrUnsupportedStoreCapability
	}
	options := sessiontree.PathWindowOptions{ThreadID: destination.String(), Direction: sessiontree.PathWindowAfter, Limit: 32}
	var out []MessageAttachment
	for {
		page, err := reader.PathEntriesWindow(ctx, options)
		if err != nil {
			return nil, runtimeHostError(err)
		}
		for _, entry := range page.Entries {
			if entry.Type != sessiontree.EntryReferenceHistory {
				return out, nil
			}
			out = append(out, runtimeMessageAttachments(entry.Message.Attachments)...)
		}
		if !page.HasAfter {
			return out, nil
		}
		if len(page.Entries) == 0 || page.Entries[len(page.Entries)-1].ID == options.AnchorID {
			return nil, ErrAuthorityCorrupt
		}
		options.LeafID, options.AnchorID = page.LeafID, page.Entries[len(page.Entries)-1].ID
	}
}

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

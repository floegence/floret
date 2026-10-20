package runtime

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/threadquery"
)

// ThreadBodyManifestInput locates one committed assistant or reasoning source.
// SourceID is the corresponding ThreadItem.ID, not an authorization token.
// LeafID freezes the manifest; every continuation must retain the returned leaf.
// AfterEntryID is scan progress and can denote a skipped inactive occurrence.
type ThreadBodyManifestInput struct {
	ThreadID     identity.ThreadID `json:"thread_id"`
	LeafID       string            `json:"leaf_id,omitempty"`
	SourceID     string            `json:"source_id"`
	Field        ThreadBodyField   `json:"field"`
	AfterEntryID string            `json:"after_entry_id,omitempty"`
	Limit        int               `json:"limit,omitempty"`
}

// ThreadBodyPart identifies one exact immutable field for ReadBody. Concatenate
// content parts in manifest order; repeated reasoning has a single part.
// Control output parts preserve raw canonical bytes, including whitespace. The
// existing assistant presentation trims them and supplies its empty-text label.
type ThreadBodyPart struct {
	EntryID string          `json:"entry_id"`
	Field   ThreadBodyField `json:"field"`
	SHA256  string          `json:"sha256"`
	Bytes   int             `json:"bytes"`
}

// ThreadBodyManifestPage contains at most 150 locators and 64 KiB encoded data.
// Empty pages can have HasMore when inactive branches consume the scan budget.
// A leaf remains valid on ordinary append, but fails after an incompatible path
// change. The manifest is a read boundary, never another presentation version.
type ThreadBodyManifestPage struct {
	ThreadID    identity.ThreadID `json:"thread_id"`
	LeafID      string            `json:"leaf_id"`
	SourceID    string            `json:"source_id"`
	Parts       []ThreadBodyPart  `json:"parts"`
	NextEntryID string            `json:"next_entry_id,omitempty"`
	HasMore     bool              `json:"has_more"`
}

// ThreadBodyManifestReader optionally resolves canonical output sources without
// loading their text or complete ThreadView. Every page requires a fresh scope
// and host private-fact policy, like ReadBody and SearchBody.
type ThreadBodyManifestReader interface {
	BodyManifest(context.Context, threadquery.Scope, ThreadBodyManifestInput) (ThreadBodyManifestPage, error)
}

var _ ThreadBodyManifestReader = (*threadRuntimeService)(nil)

func (service *threadRuntimeService) BodyManifest(ctx context.Context, scope threadquery.Scope, input ThreadBodyManifestInput) (ThreadBodyManifestPage, error) {
	if err := service.authorizeBodyRead(ctx, scope, input.ThreadID); err != nil {
		return ThreadBodyManifestPage{}, err
	}
	reader, ok := service.host.store.repo.(sessiontree.SourceBodyPageReader)
	if !ok {
		return ThreadBodyManifestPage{}, ErrUnsupportedStoreCapability
	}
	limit := input.Limit
	if limit == 0 {
		limit = 150
	}
	page, err := reader.SourceBodyPage(ctx, sessiontree.SourceBodyPageOptions{ThreadID: input.ThreadID.String(), LeafID: input.LeafID, SourceID: input.SourceID, Field: string(input.Field), AfterEntryID: input.AfterEntryID, Limit: limit})
	if errors.Is(err, spi.ErrInvalidArgument) {
		return ThreadBodyManifestPage{}, threadquery.ErrInvalidQuery
	}
	if err != nil {
		return ThreadBodyManifestPage{}, runtimeHostError(err)
	}
	result := ThreadBodyManifestPage{ThreadID: input.ThreadID, LeafID: page.LeafID, SourceID: input.SourceID, Parts: []ThreadBodyPart{}, NextEntryID: page.NextEntryID, HasMore: page.HasMore}
	for _, body := range page.Bodies {
		result.Parts = append(result.Parts, ThreadBodyPart{EntryID: body.EntryID, Field: ThreadBodyField(body.Field), SHA256: body.SHA256, Bytes: body.Bytes})
	}
	for {
		raw, err := json.Marshal(result)
		if err != nil {
			return ThreadBodyManifestPage{}, err
		}
		if len(raw) <= 64<<10 {
			return result, nil
		}
		if len(result.Parts) <= 1 {
			return ThreadBodyManifestPage{}, threadquery.ErrInvalidQuery
		}
		result.Parts = result.Parts[:len(result.Parts)-1]
		result.NextEntryID = result.Parts[len(result.Parts)-1].EntryID
		result.HasMore = true
	}
}

func (service *threadRuntimeService) authorizeBodyRead(ctx context.Context, scope threadquery.Scope, threadID identity.ThreadID) error {
	if err := service.host.available(); err != nil {
		return err
	}
	if ctx == nil {
		return threadquery.ErrInvalidQuery
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := identity.ParseThreadID(threadID.String()); err != nil {
		return threadquery.ErrInvalidQuery
	}
	authorized, err := sessiontree.ValidateQueryScope(scope)
	if err != nil {
		return err
	}
	if !authorized[threadID.String()] {
		return threadquery.ErrRecordUnavailable
	}
	return nil
}

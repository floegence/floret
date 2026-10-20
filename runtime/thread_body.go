package runtime

import (
	"context"
	"errors"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/threadquery"
)

// ThreadBodyField identifies an exact canonical field. Tool arguments and
// reasoning require the same private-fact authorization as ThreadQueryReader.
type ThreadBodyField string

const (
	ThreadBodyContent       ThreadBodyField = "content"
	ThreadBodyReasoning     ThreadBodyField = "reasoning"
	ThreadBodyArguments     ThreadBodyField = "arguments"
	ThreadBodySummary       ThreadBodyField = "summary"
	ThreadBodyError         ThreadBodyField = "error"
	ThreadBodyControlOutput ThreadBodyField = "control_output"
)

// ThreadBodyInput locates immutable source bytes, never grants access, and may
// be resumed across restarts. The host must freshly authorize Scope each time.
type ThreadBodyInput struct {
	ThreadID    identity.ThreadID `json:"thread_id"`
	EntryID     string            `json:"entry_id"`
	Field       ThreadBodyField   `json:"field"`
	OffsetBytes int               `json:"offset_bytes"`
	LimitBytes  int               `json:"limit_bytes"`
	// SHA256, when supplied, rejects a locator that no longer denotes these bytes.
	SHA256 string `json:"sha256"`
}

// ThreadBodyChunk preserves exact source bytes at a complete UTF-8 boundary.
// Empty text at Bytes is EOF. SHA256 covers the complete field, not this chunk.
type ThreadBodyChunk struct {
	ThreadID    identity.ThreadID `json:"thread_id"`
	EntryID     string            `json:"entry_id"`
	Field       ThreadBodyField   `json:"field"`
	SHA256      string            `json:"sha256"`
	Bytes       int               `json:"bytes"`
	OffsetBytes int               `json:"offset_bytes"`
	Text        string            `json:"text"`
}

// ThreadBodyReader is optional and does not expand ThreadService. Each read is
// at most 16 KiB source and 64 KiB encoded. It never materializes a full entry.
type ThreadBodyReader interface {
	ReadBody(context.Context, threadquery.Scope, ThreadBodyInput) (ThreadBodyChunk, error)
}

var _ ThreadBodyReader = (*threadRuntimeService)(nil)

func (service *threadRuntimeService) ReadBody(ctx context.Context, scope threadquery.Scope, input ThreadBodyInput) (ThreadBodyChunk, error) {
	if err := service.authorizeBodyRead(ctx, scope, input.ThreadID); err != nil {
		return ThreadBodyChunk{}, err
	}
	reader, ok := service.host.store.repo.(sessiontree.CanonicalBodyReader)
	if !ok {
		return ThreadBodyChunk{}, ErrUnsupportedStoreCapability
	}
	limit := input.LimitBytes
	if limit == 0 {
		limit = 16 << 10
	}
	part, err := reader.ReadCanonicalBody(ctx, input.ThreadID.String(), input.EntryID, string(input.Field), input.OffsetBytes, limit)
	if errors.Is(err, spi.ErrInvalidArgument) {
		return ThreadBodyChunk{}, threadquery.ErrInvalidQuery
	}
	if err != nil {
		return ThreadBodyChunk{}, runtimeHostError(err)
	}
	if input.SHA256 != "" && input.SHA256 != part.SHA256 {
		return ThreadBodyChunk{}, threadquery.ErrRecordUnavailable
	}
	return ThreadBodyChunk{ThreadID: input.ThreadID, EntryID: part.EntryID, Field: ThreadBodyField(part.Field), SHA256: part.SHA256, Bytes: part.Bytes, OffsetBytes: part.Offset, Text: part.Text}, nil
}

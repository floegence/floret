package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/outputbody"
	"github.com/floegence/floret/v7/threadquery"
	"os"
)

// ThreadLiveBodyInput reads a frozen prefix of an uncommitted output source.
// RunID and SourceID are associations, never grants. ThroughBytes is taken
// from the accepted item's Body.Bytes; later appends do not extend the read.
// After canonical installation, retry through the item's canonical locator.
type ThreadLiveBodyInput struct {
	ThreadID     identity.ThreadID `json:"thread_id"`
	RunID        identity.RunID    `json:"run_id"`
	SourceID     string            `json:"source_id"`
	ThroughBytes int               `json:"through_bytes"`
	OffsetBytes  int               `json:"offset_bytes"`
	LimitBytes   int               `json:"limit_bytes"`
}

// ThreadLiveBodyChunk preserves exact displayed UTF-8 bytes. Unlike canonical
// fields, live prefixes have no durable hash and cannot resume after restart.
type ThreadLiveBodyChunk struct {
	ThreadID     identity.ThreadID `json:"thread_id"`
	RunID        identity.RunID    `json:"run_id"`
	SourceID     string            `json:"source_id"`
	ThroughBytes int               `json:"through_bytes"`
	OffsetBytes  int               `json:"offset_bytes"`
	Text         string            `json:"text"`
}

// ThreadLiveBodyReader is optional and never widens ThreadService. Each read
// freshly authorizes private facts, returns at most 16 KiB source, and releases
// the actor lock before file I/O. Canonical reads remain a separate contract.
type ThreadLiveBodyReader interface {
	ReadLiveBody(context.Context, threadquery.Scope, ThreadLiveBodyInput) (ThreadLiveBodyChunk, error)
}

var _ ThreadLiveBodyReader = (*threadRuntimeService)(nil)

func (service *threadRuntimeService) ReadLiveBody(ctx context.Context, scope threadquery.Scope, input ThreadLiveBodyInput) (ThreadLiveBodyChunk, error) {
	if err := service.authorizeBodyRead(ctx, scope, input.ThreadID); err != nil {
		return ThreadLiveBodyChunk{}, err
	}
	limit := input.LimitBytes
	if limit == 0 {
		limit = 16 << 10
	}
	if input.RunID == "" || input.SourceID == "" || input.ThroughBytes < 0 || input.OffsetBytes < 0 || input.OffsetBytes > input.ThroughBytes || limit < 4 || limit > 16<<10 {
		return ThreadLiveBodyChunk{}, threadquery.ErrInvalidQuery
	}
	service.runtimesMu.Lock()
	actor := service.runtimes[input.ThreadID.String()]
	service.runtimesMu.Unlock()
	if actor == nil {
		return ThreadLiveBodyChunk{}, threadquery.ErrRecordUnavailable
	}
	var reader outputbody.Reader
	err := actor.apply(ctx, func() error {
		if actor.state.contextErr != nil {
			return actor.state.contextErr
		}
		index := actor.itemIndexByID(input.SourceID)
		if index < 0 {
			return threadquery.ErrRecordUnavailable
		}
		item := actor.state.view.Items[index]
		if item.RunID != input.RunID || item.CanonicalOrdinal != 0 || item.liveBody == nil || input.ThroughBytes > item.textBytes {
			return threadquery.ErrRecordUnavailable
		}
		reader = item.liveBody
		return nil
	})
	if err != nil {
		return ThreadLiveBodyChunk{}, err
	}
	text, err := reader.Read(ctx, input.OffsetBytes, limit, input.ThroughBytes)
	if errors.Is(err, os.ErrClosed) {
		return ThreadLiveBodyChunk{}, threadquery.ErrRecordUnavailable
	}
	if errors.Is(err, os.ErrInvalid) {
		return ThreadLiveBodyChunk{}, threadquery.ErrInvalidQuery
	}
	if err != nil {
		return ThreadLiveBodyChunk{}, err
	}
	result := ThreadLiveBodyChunk{ThreadID: input.ThreadID, RunID: input.RunID, SourceID: input.SourceID, ThroughBytes: input.ThroughBytes, OffsetBytes: input.OffsetBytes, Text: text}
	for {
		raw, err := json.Marshal(result)
		if err != nil {
			return ThreadLiveBodyChunk{}, err
		}
		if len(raw) <= 64<<10 {
			return result, nil
		}
		if result.Text == "" {
			return ThreadLiveBodyChunk{}, threadquery.ErrInvalidQuery
		}
		result.Text, _ = boundedThreadText(result.Text, len(result.Text)/2)
	}
}

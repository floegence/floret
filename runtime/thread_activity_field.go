package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"unicode/utf8"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/threadquery"
)

// ThreadActivityFieldInput reads one decoded string in an immutable typed
// ActivityPresentation. Path is a JSON pointer, including array ordinals. Sources
// are resolved against ItemID on each read, never grant access themselves,
// and must still belong to the authorized thread's active path on every read.
// For a resident field, use SourceID and RunID returned by ActivityStructure,
// leaving EntryID and hashes empty. SourceID freezes the selected string, even
// across growth, replacement and canonical completion. It expires on restart or
// bounded cache eviction; resolve that field again when it becomes unavailable.
type ThreadActivityFieldInput struct {
	// AlignStart adjusts an offset inside a UTF-8 rune back by at most three
	// bytes. The response reports the actual offset; false keeps strict reads.
	AlignStart   bool              `json:"align_start,omitempty"`
	SourceID     string            `json:"source_id,omitempty"`
	RunID        identity.RunID    `json:"run_id,omitempty"`
	ItemID       string            `json:"item_id"`
	ThreadID     identity.ThreadID `json:"thread_id"`
	EntryID      string            `json:"entry_id"`
	Path         string            `json:"path"`
	SourceSHA256 string            `json:"source_sha256"`
	SHA256       string            `json:"sha256"`
	OffsetBytes  int               `json:"offset_bytes"`
	LimitBytes   int               `json:"limit_bytes"`
}

// ThreadActivityFieldChunk contains complete UTF-8 text. SourceSHA256 identifies
// the complete canonical Activity JSON; SHA256 identifies the decoded field.
// OffsetBytes and Bytes refer to decoded UTF-8, not JSON escape bytes.
type ThreadActivityFieldChunk struct {
	SourceID     string            `json:"source_id,omitempty"`
	RunID        identity.RunID    `json:"run_id,omitempty"`
	ThreadID     identity.ThreadID `json:"thread_id"`
	EntryID      string            `json:"entry_id"`
	Path         string            `json:"path"`
	SourceSHA256 string            `json:"source_sha256"`
	SHA256       string            `json:"sha256"`
	Bytes        int               `json:"bytes"`
	OffsetBytes  int               `json:"offset_bytes"`
	Text         string            `json:"text"`
}

// ThreadActivityFieldReader is an optional read-only capability. Every call
// requires fresh authorized scope. Reads return at most 16 KiB of decoded text,
// use only the requested field after source preparation, and preserve a frozen
// source across ordinary appends. Responses are at most 64 KiB encoded. Missing
// legacy metadata requires one owner-controlled complete source preparation.
// Existing ThreadBodyActivityJSON semantics remain unchanged.
type ThreadActivityFieldReader interface {
	ReadActivityField(context.Context, threadquery.Scope, ThreadActivityFieldInput) (ThreadActivityFieldChunk, error)
}

var _ ThreadActivityFieldReader = (*threadRuntimeService)(nil)

func (service *threadRuntimeService) ReadActivityField(ctx context.Context, scope threadquery.Scope, input ThreadActivityFieldInput) (ThreadActivityFieldChunk, error) {
	if err := service.authorizeBodyRead(ctx, scope, input.ThreadID); err != nil {
		return ThreadActivityFieldChunk{}, err
	}
	if input.SourceID == "" {
		if reader, ok := service.host.store.repo.(sessiontree.ItemActivityReader); ok {
			limit := input.LimitBytes
			if limit == 0 {
				limit = 16 << 10
			}
			part, err := reader.ReadItemActivityField(ctx, input.ItemID, sessiontree.ActivityFieldInput{AlignStart: input.AlignStart, ThreadID: input.ThreadID.String(), EntryID: input.EntryID, Path: input.Path, SourceSHA256: input.SourceSHA256, Offset: input.OffsetBytes, Limit: limit})
			if !errors.Is(err, sessiontree.ErrUnsupportedStoreCapability) {
				if err != nil {
					return ThreadActivityFieldChunk{}, activityReadError(err)
				}
				if input.SHA256 != "" && part.SHA256 != input.SHA256 {
					return ThreadActivityFieldChunk{}, threadquery.ErrRecordUnavailable
				}
				return boundActivityFieldChunk(ThreadActivityFieldChunk{ThreadID: input.ThreadID, EntryID: part.EntryID, Path: part.Path, SourceSHA256: part.SourceSHA256, SHA256: part.SHA256, Bytes: part.Bytes, OffsetBytes: part.Offset, Text: part.Text})
			}
		}
	}
	item, err := service.activitySourceItem(ctx, scope, ThreadActivityStructureInput{ThreadID: input.ThreadID, ItemID: input.ItemID})
	if err != nil {
		return ThreadActivityFieldChunk{}, err
	}
	if input.SourceID != "" {
		return service.readResidentActivityField(ctx, item, input)
	}
	if !slices.Contains(item.activityEntryIDs, input.EntryID) {
		return ThreadActivityFieldChunk{}, threadquery.ErrRecordUnavailable
	}
	reader, ok := service.host.store.repo.(sessiontree.ActivityFieldReader)
	if !ok {
		return ThreadActivityFieldChunk{}, ErrUnsupportedStoreCapability
	}
	limit := input.LimitBytes
	if limit == 0 {
		limit = 16 << 10
	}
	part, err := reader.ReadActivityField(ctx, sessiontree.ActivityFieldInput{AlignStart: input.AlignStart, ThreadID: input.ThreadID.String(), EntryID: input.EntryID, Path: input.Path, SourceSHA256: input.SourceSHA256, Offset: input.OffsetBytes, Limit: limit})
	if errors.Is(err, spi.ErrInvalidArgument) {
		return ThreadActivityFieldChunk{}, threadquery.ErrInvalidQuery
	}
	if errors.Is(err, sessiontree.ErrEntryNotFound) {
		return ThreadActivityFieldChunk{}, threadquery.ErrRecordUnavailable
	}
	if err != nil {
		return ThreadActivityFieldChunk{}, runtimeHostError(err)
	}
	if input.SHA256 != "" && part.SHA256 != input.SHA256 {
		return ThreadActivityFieldChunk{}, threadquery.ErrRecordUnavailable
	}
	result := ThreadActivityFieldChunk{ThreadID: input.ThreadID, EntryID: part.EntryID, Path: part.Path, SourceSHA256: part.SourceSHA256, SHA256: part.SHA256, Bytes: part.Bytes, OffsetBytes: part.Offset, Text: part.Text}
	return boundActivityFieldChunk(result)
}

func boundActivityFieldChunk(result ThreadActivityFieldChunk) (ThreadActivityFieldChunk, error) {
	for {
		wire, err := json.Marshal(result)
		if err != nil {
			return ThreadActivityFieldChunk{}, err
		}
		if len(wire) <= 64<<10 {
			return result, nil
		}
		end := len(result.Text) / 2
		for end > 0 && !utf8.RuneStart(result.Text[end]) {
			end--
		}
		if end == 0 {
			return ThreadActivityFieldChunk{}, threadquery.ErrInvalidQuery
		}
		result.Text = result.Text[:end]
	}
}

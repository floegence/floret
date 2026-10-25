package runtime

import (
	"context"
	"encoding/json"
	"unicode/utf8"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/threadquery"
)

// ThreadItemTextInput reads a bounded range of one item presentation source.
// Source locators must match ItemBody. OffsetBytes counts rendered UTF-8 bytes,
// including the existing control-output whitespace normalization. ThroughBytes
// freezes a live extent; zero requests the current extent. Callers authorize
// every read, and must discard chunks if the source identity changes.
type ThreadItemTextInput struct {
	ThreadID     identity.ThreadID `json:"thread_id"`
	ItemID       string            `json:"item_id"`
	RunID        identity.RunID    `json:"run_id,omitempty"`
	SourceID     string            `json:"source_id,omitempty"`
	EntryID      string            `json:"entry_id,omitempty"`
	Field        ThreadBodyField   `json:"field"`
	ThroughBytes int               `json:"through_bytes,omitempty"`
	OffsetBytes  int               `json:"offset_bytes"`
	LimitBytes   int               `json:"limit_bytes,omitempty"`
}

// ThreadItemTextChunk contains at most 16 KiB exact presentation text and 64 KiB
// encoded JSON. It never
// transfers activity JSON or changes model history. Bytes is the complete
// presentation extent, which can differ from raw control-output body bytes.
type ThreadItemTextChunk struct {
	ThreadID     identity.ThreadID `json:"thread_id"`
	ItemID       string            `json:"item_id"`
	RunID        identity.RunID    `json:"run_id,omitempty"`
	SourceID     string            `json:"source_id,omitempty"`
	EntryID      string            `json:"entry_id,omitempty"`
	Field        ThreadBodyField   `json:"field"`
	Bytes        int               `json:"bytes"`
	ThroughBytes int               `json:"through_bytes"`
	OffsetBytes  int               `json:"offset_bytes"`
	Text         string            `json:"text"`
}

// ThreadItemTextReader optionally exposes the same text projection used by
// windows, with bounded transport and no full View or complete body assembly.
type ThreadItemTextReader interface {
	ReadItemText(context.Context, threadquery.Scope, ThreadItemTextInput) (ThreadItemTextChunk, error)
}

var _ ThreadItemTextReader = (*threadRuntimeService)(nil)

func (service *threadRuntimeService) ReadItemText(ctx context.Context, scope threadquery.Scope, input ThreadItemTextInput) (ThreadItemTextChunk, error) {
	var empty ThreadItemTextChunk
	limit := input.LimitBytes
	if limit == 0 {
		limit = 16 << 10
	}
	if input.OffsetBytes < 0 || input.ThroughBytes < 0 || limit < utf8.UTFMax || limit > 16<<10 {
		return empty, threadquery.ErrInvalidQuery
	}
	item, err := service.detailSourceItem(ctx, scope, ThreadItemBodyInput{ThreadID: input.ThreadID, ItemID: input.ItemID})
	if err != nil {
		return empty, err
	}
	body, err := itemBodyLocator(item)
	if err != nil {
		return empty, err
	}
	if body.RunID != input.RunID || body.SourceID != input.SourceID || body.EntryID != input.EntryID || body.Field != input.Field {
		return empty, threadquery.ErrRecordUnavailable
	}
	extent := len(item.Text)
	if item.textOmitted {
		extent = item.textBytes
	}
	through := input.ThroughBytes
	if through == 0 {
		through = extent
	}
	// ItemBody reports the raw control source extent. Presentation reads use
	// the canonical reducer's trimmed extent, exactly like ordinary windows.
	if item.controlTextBytes > 0 && through == item.controlTextBytes {
		through = extent
	}
	if through > extent || input.OffsetBytes > through {
		return empty, threadquery.ErrRecordUnavailable
	}
	text := ""
	if input.OffsetBytes < through {
		text, err = service.readItemTextRange(ctx, input.ThreadID.String(), item, input.OffsetBytes, min(limit, through-input.OffsetBytes))
		if err != nil {
			return empty, err
		}
		if text == "" || !utf8.ValidString(text) {
			return empty, threadquery.ErrInvalidQuery
		}
	}
	result := ThreadItemTextChunk{ThreadID: input.ThreadID, ItemID: input.ItemID, RunID: body.RunID, SourceID: body.SourceID, EntryID: body.EntryID, Field: body.Field, Bytes: extent, ThroughBytes: through, OffsetBytes: input.OffsetBytes, Text: text}
	for {
		raw, err := json.Marshal(result)
		if err != nil {
			return empty, err
		}
		if len(raw) <= 64<<10 {
			return result, nil
		}
		if result.Text == "" {
			return empty, threadquery.ErrInvalidQuery
		}
		result.Text, _ = boundedThreadText(result.Text, len(result.Text)/2)
	}
}

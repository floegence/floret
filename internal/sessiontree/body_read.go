package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/floegence/floret/v7/storage/spi"
)

// CanonicalBodyChunk is a bounded read of one immutable canonical field. Its
// locator is an identity, never authorization. Reads verify current path access.
type CanonicalBodyChunk struct {
	ThreadID string `json:"thread_id"`
	EntryID  string `json:"entry_id"`
	Field    string `json:"field"`
	SHA256   string `json:"sha256"`
	Bytes    int    `json:"bytes"`
	Offset   int    `json:"offset"`
	Text     string `json:"text"`
}

type CanonicalBodyReader interface {
	ReadCanonicalBody(context.Context, string, string, string, int, int) (CanonicalBodyChunk, error)
}

func (r *MemoryRepo) ReadCanonicalBody(ctx context.Context, threadID, entryID, field string, offset, limit int) (CanonicalBodyChunk, error) {
	if ctx == nil {
		return CanonicalBodyChunk{}, spi.ErrInvalidArgument
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return CanonicalBodyChunk{}, err
	}
	if offset < 0 || limit < utf8.UTFMax || limit > canonicalBodyBlockBytes {
		return CanonicalBodyChunk{}, spi.ErrInvalidArgument
	}
	entry, err := r.activeBodyEntryLocked(threadID, entryID)
	if err != nil {
		return CanonicalBodyChunk{}, err
	}
	fields := canonicalEntryBodyFields(&entry)
	value, valid := fields[field]
	if !valid {
		return CanonicalBodyChunk{}, spi.ErrInvalidArgument
	}
	result := CanonicalBodyChunk{ThreadID: threadID, EntryID: entryID, Field: field, Offset: offset}
	ref, stored := entry.bodyFields[field]
	if stored {
		result.SHA256, result.Bytes = ref.SHA256, ref.Bytes
		if offset > ref.Bytes {
			return CanonicalBodyChunk{}, spi.ErrInvalidArgument
		}
		read := func(tx spi.ReadTx) error {
			var err error
			result.Text, err = readCanonicalBodyText(ctx, tx, ref, offset, limit)
			return err
		}
		var err error
		if r.bodyTx != nil {
			err = read(r.bodyTx)
		} else if r.bodyBackend != nil {
			err = r.bodyBackend.View(ctx, read)
		} else {
			err = errors.New("canonical body storage is unavailable")
		}
		if err != nil {
			return CanonicalBodyChunk{}, err
		}
	} else {
		result.Bytes, result.SHA256 = len(*value), stableHash(*value)
		if offset > len(*value) || offset < len(*value) && !utf8.RuneStart((*value)[offset]) {
			return CanonicalBodyChunk{}, spi.ErrInvalidArgument
		}
		end := min(len(*value), offset+limit)
		for end < len(*value) && end > offset && !utf8.RuneStart((*value)[end]) {
			end--
		}
		// Copy the small result so an ephemeral repository cannot retain an entire
		// giant source through a tiny chunk's backing string.
		result.Text = strings.Clone((*value)[offset:end])
	}
	// JSON control-character escaping can expand sixfold. Budget the actual wire
	// representation even for hostile input; never silently skip source bytes.
	for {
		raw, err := json.Marshal(result)
		if err != nil {
			return CanonicalBodyChunk{}, err
		}
		if len(raw) <= canonicalBodyReadBytes-512 {
			break
		}
		end := len(result.Text) / 2
		for end > 0 && !utf8.RuneStart(result.Text[end]) {
			end--
		}
		if end == 0 {
			return CanonicalBodyChunk{}, spi.ErrInvalidArgument
		}
		result.Text = result.Text[:end]
	}
	return result, nil
}

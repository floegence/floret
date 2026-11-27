package storage

import (
	"bytes"
	"context"

	"github.com/floegence/floret/v7/internal/provider/cache"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/storage/spi"
)

type executionSegmentReader struct {
	kernel   *BackendKernel
	scope    string
	budget   int
	deletion uint64
	bytes    int
	prefix   []cache.Segment
}

func (kernel *BackendKernel) NewExecutionSegmentReader(scope string) cache.SegmentReader {
	return &executionSegmentReader{kernel: kernel, scope: scope, budget: 16 << 20}
}

func (reader *executionSegmentReader) Segments(ctx context.Context, scope, provider, model string) ([]cache.Segment, error) {
	kernel := reader.kernel
	if reader.scope != scope {
		return kernel.Segments(ctx, scope, provider, model)
	}
	kernel.promptMu.Lock()
	defer kernel.promptMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if reader.deletion != kernel.promptDeletion {
		reader.prefix, reader.bytes, reader.deletion = nil, 0, kernel.promptDeletion
	}
	var result []cache.Segment
	include := func(segment cache.Segment) {
		if (provider == "" || segment.Provider == provider) && (model == "" || segment.Model == model) {
			if result == nil && len(reader.prefix) > 0 {
				result = make([]cache.Segment, 0, len(reader.prefix))
			}
			segment.Message.Attachments = session.CloneMessageAttachments(segment.Message.Attachments)
			result = append(result, segment)
		}
	}
	for _, segment := range reader.prefix {
		include(segment)
	}
	prefix, size := reader.prefix, reader.bytes
	ordinal := uint64(len(prefix))
	err := kernel.viewPrompt(ctx, scope, func(tx spi.ReadTx) error {
		namespacePrefix := promptRecordPrefix(scope, promptSegment)
		start := namespacePrefix
		if ordinal != 0 {
			start = promptRecordKey(promptRecord{ScopeID: scope, Category: promptSegment, Ordinal: ordinal + 1})
		}
		return scanPromptRecords(tx, start, prefixEnd(namespacePrefix), func(row spi.Record) error {
			ordinal++
			var record promptRecord
			if err := decodePromptValue(row.Value, "prompt_record", &record); err != nil {
				return err
			}
			if record.Ordinal != ordinal || record.ScopeID != scope || record.Category != promptSegment || !bytes.Equal(promptRecordKey(record), row.Key) {
				return sessiontree.ErrAuthorityCorrupt
			}
			var segment cache.Segment
			if err := decodePromptJSON(record.Value, &segment); err != nil {
				return err
			}
			if segment.PromptScopeID != scope {
				return sessiontree.ErrAuthorityCorrupt
			}
			if uint64(len(prefix))+1 == ordinal && len(prefix) < 8192 && size+len(row.Value) <= reader.budget {
				prefix = append(prefix, segment)
				size += len(row.Value)
			}
			include(segment)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	for _, record := range kernel.pending[scope] {
		if record.Category != promptSegment {
			continue
		}
		var segment cache.Segment
		if err := decodePromptJSON(record.Value, &segment); err != nil {
			return nil, err
		}
		include(segment)
	}
	reader.prefix, reader.bytes = prefix, size
	return result, nil
}

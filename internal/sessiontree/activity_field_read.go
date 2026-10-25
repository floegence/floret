package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/storage/spi"
)

type ActivityFieldInput struct {
	AlignStart                            bool
	ThreadID, EntryID, Path, SourceSHA256 string
	Offset, Limit                         int
}
type ActivityFieldChunk struct {
	EntryID, Path, SourceSHA256, SHA256 string
	Bytes, Offset                       int
	Text                                string
}
type ActivityFieldReader interface {
	ReadActivityField(context.Context, ActivityFieldInput) (ActivityFieldChunk, error)
}

func (repo *BackendRepo) ReadActivityField(ctx context.Context, input ActivityFieldInput) (ActivityFieldChunk, error) {
	if ctx == nil || input.Offset < 0 || input.Limit < utf8.UTFMax || input.Limit > canonicalBodyBlockBytes || len(input.Path) > 4096 || !strings.HasPrefix(input.Path, "/") {
		return ActivityFieldChunk{}, spi.ErrInvalidArgument
	}
	if err := repo.prepareActivityFields(ctx, input.ThreadID, input.EntryID); err != nil {
		return ActivityFieldChunk{}, err
	}
	if delegate := repo.delegate.Load(); delegate != nil {
		return delegate.ReadActivityField(ctx, input)
	}
	var result ActivityFieldChunk
	err := repo.canonicalThreadView(ctx, input.ThreadID, func(tx spi.ReadTx, meta ThreadMeta) error {
		source, err := activeLocatedEntry(ctx, tx, meta, input.EntryID)
		if err != nil {
			return err
		}
		ref, ok := source.bodyFields["activity_json"]
		if !ok || input.SourceSHA256 != "" && input.SourceSHA256 != ref.SHA256 {
			return ErrEntryNotFound
		}
		node, err := readActivityFieldNode(tx, ref, input.Path)
		if err != nil {
			return err
		}
		text, start, err := readRequestedActivityTextRange(ctx, tx, ref, node, input)
		if err != nil {
			return err
		}
		result = ActivityFieldChunk{EntryID: input.EntryID, Path: input.Path, SourceSHA256: ref.SHA256, SHA256: node.SHA256, Bytes: node.Bytes, Offset: start, Text: text}
		return nil
	}, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.ReadActivityField(ctx, input)
		return err
	})
	return result, err
}
func (repo *MemoryRepo) ReadActivityField(ctx context.Context, input ActivityFieldInput) (ActivityFieldChunk, error) {
	if ctx == nil || input.Offset < 0 || input.Limit < utf8.UTFMax || input.Limit > canonicalBodyBlockBytes || len(input.Path) > 4096 || !strings.HasPrefix(input.Path, "/") {
		return ActivityFieldChunk{}, spi.ErrInvalidArgument
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return ActivityFieldChunk{}, err
	}
	entry, err := repo.activeBodyEntryLocked(input.ThreadID, input.EntryID)
	if err != nil {
		return ActivityFieldChunk{}, err
	}
	if ref, stored := entry.bodyFields["activity_json"]; stored {
		if input.SourceSHA256 != "" && input.SourceSHA256 != ref.SHA256 {
			return ActivityFieldChunk{}, ErrEntryNotFound
		}
		var result ActivityFieldChunk
		read := func(tx spi.ReadTx) error {
			node, err := readActivityFieldNode(tx, ref, input.Path)
			if err != nil {
				return err
			}
			text, start, err := readRequestedActivityTextRange(ctx, tx, ref, node, input)
			if err != nil {
				return err
			}
			result = ActivityFieldChunk{EntryID: input.EntryID, Path: input.Path, SourceSHA256: ref.SHA256, SHA256: node.SHA256, Bytes: node.Bytes, Offset: start, Text: text}
			return nil
		}
		if repo.bodyTx != nil {
			err = read(repo.bodyTx)
		} else if repo.bodyBackend != nil {
			err = repo.bodyBackend.View(ctx, read)
		} else {
			err = ErrAuthorityCorrupt
		}
		return result, err
	}
	raw, err := canonicalFieldValue(entry, "activity_json")
	if err != nil {
		return ActivityFieldChunk{}, err
	}
	if raw == "" {
		return ActivityFieldChunk{}, ErrEntryNotFound
	}
	hash := stableHash(raw)
	if input.SourceSHA256 != "" && input.SourceSHA256 != hash {
		return ActivityFieldChunk{}, ErrEntryNotFound
	}
	nodes, err := indexActivityJSON(ctx, raw)
	if err != nil {
		return ActivityFieldChunk{}, err
	}
	node, ok := nodes[input.Path]
	if !ok {
		return ActivityFieldChunk{}, ErrEntryNotFound
	}
	if node.Kind != "string" {
		return ActivityFieldChunk{}, spi.ErrInvalidArgument
	}
	var text string
	if json.Unmarshal([]byte(raw[node.Offset:node.Offset+node.EncodedBytes]), &text) != nil {
		return ActivityFieldChunk{}, ErrAuthorityCorrupt
	}
	if input.Offset > len(text) {
		return ActivityFieldChunk{}, spi.ErrInvalidArgument
	}
	if input.AlignStart {
		for input.Offset > 0 && input.Offset < len(text) && !utf8.RuneStart(text[input.Offset]) {
			input.Offset--
		}
	} else if input.Offset < len(text) && !utf8.RuneStart(text[input.Offset]) {
		return ActivityFieldChunk{}, spi.ErrInvalidArgument
	}
	end := min(len(text), input.Offset+input.Limit)
	for end < len(text) && end > input.Offset && !utf8.RuneStart(text[end]) {
		end--
	}
	return ActivityFieldChunk{EntryID: input.EntryID, Path: input.Path, SourceSHA256: hash, SHA256: node.SHA256, Bytes: len(text), Offset: input.Offset, Text: strings.Clone(text[input.Offset:end])}, nil
}
func (repo *BackendRepo) prepareActivityFields(ctx context.Context, thread, entryID string) error {
	ready := false
	var ref canonicalBodyRef
	if err := repo.canonicalThreadView(ctx, thread, func(tx spi.ReadTx, meta ThreadMeta) error {
		entry, err := activeLocatedEntry(ctx, tx, meta, entryID)
		if err != nil {
			return err
		}
		var found bool
		ref, found = entry.bodyFields["activity_json"]
		if !found {
			return ErrEntryNotFound
		}
		_, err = readActivityFieldNode(tx, ref, "")
		if errors.Is(err, spi.ErrNotFound) {
			if summaryErr := readActivitySummaryReady(tx, ref); summaryErr != nil && (!errors.Is(summaryErr, spi.ErrNotFound) || errors.Is(summaryErr, ErrAuthorityCorrupt)) {
				return summaryErr
			}
		}
		ready = err == nil
		if ready {
			_, err = readActivityMergeMetadata(tx, ref)
			if err != nil {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
		}
		if errors.Is(err, spi.ErrNotFound) {
			return nil
		}
		return err
	}, func(*MemoryRepo) error { ready = true; return nil }); err != nil || ready {
		return err
	}
	work, release, err := repo.migrationGate.acquire(ctx, true)
	if err != nil {
		return err
	}
	defer release()
	return repo.prepareActivityFieldsOwned(work, thread, entryID, true)
}

// The caller owns migrationGate. Historical field preparation uses private
// bounded staging and publishes readiness only after every source check.
func (repo *BackendRepo) prepareActivityFieldsOwned(work context.Context, thread, entryID string, activeOnly bool) error {
	reader := &canonicalSnapshotReader{migrationSourceReader: migrationSourceReader{ctx: work, backend: repo.backend}, format: backendDomainV32Format, metadata: map[string]migrationMetadataRead{}}
	meta, err := readPublishedThreadMeta(reader, backendDomainV32Format, thread)
	if err != nil {
		return err
	}
	source, err := readCanonicalEntryMetadata(reader, thread, entryID)
	if err == nil && activeOnly {
		source, err = activeLocatedEntry(work, reader, meta, entryID)
	}
	if err != nil {
		return reader.validationError(err)
	}
	ref := source.bodyFields["activity_json"]
	text, err := materializeCanonicalBody(work, reader, ref)
	if err != nil {
		return reader.validationError(err)
	}
	nodes, err := indexActivityJSON(work, text)
	if err != nil {
		return err
	}
	// No writer is held while reading or indexing a released large activity.
	records, err := activityFieldRecords(ref, nodes)
	if err != nil {
		return err
	}
	update := func(write func(spi.WriteTx) error) error {
		repo.mu.Lock()
		defer repo.mu.Unlock()
		return repo.backend.Update(work, func(tx spi.WriteTx) error {
			// Immutable source identity survives normal appends. Validate current
			// ancestry and the same body instead of requiring an unchanged tail.
			meta, err := readPublishedThreadMeta(tx, backendDomainV32Format, thread)
			if err != nil {
				return err
			}
			current, err := readCanonicalEntryMetadata(tx, thread, entryID)
			if err == nil && activeOnly {
				current, err = activeLocatedEntry(work, tx, meta, entryID)
			}
			if err != nil {
				return err
			}
			if current.RawHash != source.RawHash || current.bodyFields["activity_json"] != ref {
				return ErrAuthorityCorrupt
			}
			return write(tx)
		})
	}
	for first := 0; first < len(records); first += 128 {
		last := min(first+128, len(records))
		if err := update(func(tx spi.WriteTx) error {
			if err := work.Err(); err != nil {
				return err
			}
			return storagebridge.WriteRecords(tx, activityFieldsNamespace, records[first:last])
		}); err != nil {
			return err
		}
	}
	return update(func(tx spi.WriteTx) error {
		if err := writeActivityMergeMetadata(tx, ref, text, nodes); err != nil {
			return err
		}
		raw, err := json.Marshal(ref)
		if err != nil {
			return err
		}
		return tx.Put(activityFieldsNamespace, activityFieldReadyKey(ref.SHA256), raw)
	})
}

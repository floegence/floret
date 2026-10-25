package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/storage/spi"
)

func activitySummaryReadyKey(hash string) []byte { return backendDomainKey(hash, "summary-ready") }

// Summary readiness covers only merge discriminators and the root and immediate payload field shapes plus bounded
// label/description text fields. It never certifies hidden structure or text parts. Full detail
// readiness remains the authority for every other field read.
func readActivitySummaryReady(tx spi.ReadTx, ref canonicalBodyRef) error {
	raw, err := tx.Get(activityFieldsNamespace, activitySummaryReadyKey(ref.SHA256))
	if err != nil {
		return err
	}
	var ready canonicalBodyRef
	if json.Unmarshal(raw, &ready) != nil || ready != ref {
		return ErrAuthorityCorrupt
	}
	_, err = readActivityMergeMetadata(tx, ref)
	if err != nil {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	return nil
}

// The existing preparation owner batches immutable, source-bound summaries.
// No field bodies are persisted, and no historical detail tree is made ready.
// Each batch validates the frozen ancestor before publishing complete sources.
func (repo *BackendRepo) prepareActivitySummariesOwned(ctx context.Context, reader *canonicalSnapshotReader, meta ThreadMeta, entries []Entry) error {
	var batch []spi.Record
	bytes := 0
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		repo.mu.Lock()
		err := func() error {
			defer repo.mu.Unlock()
			return repo.backend.Update(ctx, func(tx spi.WriteTx) error {
				if _, err := verifyWindowPreparationOwner(ctx, tx, meta); err != nil {
					return err
				}
				for _, record := range batch {
					if err := ctx.Err(); err != nil {
						return err
					}
					if err := tx.Put(activityFieldsNamespace, record.Key, record.Value); err != nil {
						return err
					}
				}
				return nil
			})
		}()
		batch, bytes = nil, 0
		return err
	}
	seen := map[string]bool{}
	for _, entry := range entries {
		ref, ok := entry.bodyFields["activity_json"]
		if !ok || entry.Type != EntryToolCall && entry.Type != EntryToolResult || entry.Message.ToolName == "goal" || seen[ref.SHA256] {
			continue
		}
		seen[ref.SHA256] = true
		if _, err := readActivityFieldNode(reader, ref, ""); err == nil {
			if _, err := readActivityMergeMetadata(reader, ref); err != nil {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			continue
		} else if !errors.Is(err, spi.ErrNotFound) {
			return err
		}
		if err := readActivitySummaryReady(reader, ref); err == nil {
			continue
		} else if !errors.Is(err, spi.ErrNotFound) || errors.Is(err, ErrAuthorityCorrupt) {
			return err
		}
		source, err := materializeCanonicalBody(ctx, reader, ref)
		if err != nil {
			return reader.validationError(err)
		}
		nodes, err := indexActivitySummaryJSON(ctx, source)
		if err != nil {
			return err
		}
		facts, err := activityMergeMetadata(source, nodes)
		if err != nil {
			return err
		}
		selected := map[string]activityFieldNode{}
		for path, node := range nodes {
			if activitySummaryPath(path) {
				if node.ShapeOnly && node.Kind == "object" {
					node.Children = nil
				}
				selected[path] = node
			}
		}
		records, err := activityFieldRecords(ref, selected)
		if err != nil {
			return err
		}
		encoded, err := encodeActivityMetadata("activity-merge-v1", facts)
		if err != nil {
			return err
		}
		ready, err := json.Marshal(ref)
		if err != nil {
			return err
		}
		records = append(records, spi.Record{Key: activityMergeKey(ref.SHA256), Value: encoded}, spi.Record{Key: activitySummaryReadyKey(ref.SHA256), Value: ready})
		size := 0
		for _, record := range records {
			size += len(record.Key) + len(record.Value)
		}
		if len(records) > 128 || size > backendThreadMigrationBatchBytes {
			return ErrAuthorityCorrupt
		}
		if len(batch)+len(records) > 128 || bytes+size > backendThreadMigrationBatchBytes {
			if err := flush(); err != nil {
				return err
			}
		}
		batch, bytes = append(batch, records...), bytes+size
	}
	return flush()
}

// Immediate shapes contain counts, never hidden members or string bodies.
func activitySummaryPath(path string) bool {
	return path != "" && (strings.Count(path, "/") == 1 || strings.HasPrefix(path, "/payload/") && strings.Count(path, "/") == 2)
}

// Large activity admission writes only bounded browsing metadata. Full detail
// locators are prepared by the existing source owner on explicit field access.
// The published summary/full readiness markers keep one canonical source and
// preserve the exact format understood by released readers.
func writeActivitySummaryIndex(ctx context.Context, tx spi.WriteTx, ref canonicalBodyRef, source string) error {
	if ref.SHA256 != stableHash(source) || ref.Bytes != len(source) {
		return ErrAuthorityCorrupt
	}
	if _, err := readActivityFieldNode(tx, ref, ""); err == nil {
		_, err = readActivityMergeMetadata(tx, ref)
		if err != nil {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		return nil
	} else if !errors.Is(err, spi.ErrNotFound) {
		return err
	}
	if err := readActivitySummaryReady(tx, ref); err == nil {
		return nil
	} else if !errors.Is(err, spi.ErrNotFound) || errors.Is(err, ErrAuthorityCorrupt) {
		return err
	}
	nodes, err := indexActivitySummaryJSON(ctx, source)
	if err != nil {
		return err
	}
	selected := map[string]activityFieldNode{}
	for path, node := range nodes {
		if activitySummaryPath(path) {
			if node.ShapeOnly && node.Kind == "object" {
				node.Children = nil
			}
			selected[path] = node
		}
	}
	records, err := activityFieldRecords(ref, selected)
	if err != nil {
		return err
	}
	for first := 0; first < len(records); first += storagebridge.MaxRecordBatch {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := storagebridge.WriteRecords(tx, activityFieldsNamespace, records[first:min(len(records), first+storagebridge.MaxRecordBatch)]); err != nil {
			return err
		}
	}
	if err := writeActivityMergeMetadata(tx, ref, source, nodes); err != nil {
		return err
	}
	ready, err := json.Marshal(ref)
	if err != nil {
		return err
	}
	return tx.Put(activityFieldsNamespace, activitySummaryReadyKey(ref.SHA256), ready)
}

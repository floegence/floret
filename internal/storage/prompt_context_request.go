package storage

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/floegence/floret/v7/internal/provider/cache"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/internal/storagecodec"
	"github.com/floegence/floret/v7/storage/spi"
)

// This disposable index contains source ordinals only, never another request.
// Its coverage is checked against the canonical append counter on every read.
const promptRequestLocatorNamespace = "floret.prompt.request-locators.v1"

func contextRequestPrefix(scope string) []byte {
	return storagecodec.Tuple(storagecodec.TupleString(scope))
}
func contextRequestKey(scope, thread, entry string) []byte {
	return storagecodec.Tuple(storagecodec.TupleString(scope), storagecodec.TupleString("context"), storagecodec.TupleString(thread), storagecodec.TupleString(entry))
}
func contextRequestCoverageKey(scope string) []byte {
	return storagecodec.Tuple(storagecodec.TupleString(scope), storagecodec.TupleString("coverage"))
}

type requestLocatorCoverage struct {
	First   uint64 `json:"first"`
	Through uint64 `json:"through"`
}

func contextRequestCoverage(tx spi.ReadTx, scope string) (requestLocatorCoverage, bool, error) {
	raw, err := tx.Get(promptRequestLocatorNamespace, contextRequestCoverageKey(scope))
	if errors.Is(err, spi.ErrNotFound) {
		return requestLocatorCoverage{}, false, nil
	}
	if err != nil {
		return requestLocatorCoverage{}, false, err
	}
	var coverage requestLocatorCoverage
	if err := decodePromptJSON(raw, &coverage); err != nil || coverage.First == 0 || coverage.First > coverage.Through {
		return coverage, false, errors.Join(sessiontree.ErrAuthorityCorrupt, err)
	}
	return coverage, true, nil
}
func writeContextRequestCoverage(tx spi.WriteTx, scope string, coverage requestLocatorCoverage) error {
	raw, err := json.Marshal(coverage)
	if err != nil {
		return err
	}
	return tx.Put(promptRequestLocatorNamespace, contextRequestCoverageKey(scope), raw)
}
func putContextRequestLocator(tx spi.WriteTx, record promptRecord) error {
	var request cache.ProviderRequestRecord
	if err := decodePromptJSON(record.Value, &request); err != nil {
		return err
	}
	if request.PromptScopeID != record.ScopeID {
		return sessiontree.ErrAuthorityCorrupt
	}
	raw, _ := json.Marshal(record.Ordinal)
	return tx.Put(promptRequestLocatorNamespace, contextRequestKey(record.ScopeID, request.ThreadID, request.ModelContextEntryID), raw)
}
func readRequestOrdinal(tx spi.ReadTx, scope string, ordinal uint64) (cache.ProviderRequestRecord, error) {
	raw, err := tx.Get(promptRecordsNamespace, promptRecordKey(promptRecord{ScopeID: scope, Category: promptRequest, Ordinal: ordinal}))
	if err != nil {
		return cache.ProviderRequestRecord{}, errors.Join(sessiontree.ErrAuthorityCorrupt, err)
	}
	return decodeRequestOrdinal(raw, scope, ordinal)
}

func decodeRequestOrdinal(raw []byte, scope string, ordinal uint64) (cache.ProviderRequestRecord, error) {
	var record struct {
		ScopeID  string                                              `json:"scope_id"`
		Category int                                                 `json:"category"`
		Ordinal  uint64                                              `json:"ordinal"`
		Value    storagecodec.JSONValue[cache.ProviderRequestRecord] `json:"value"`
	}
	if err := storagecodec.DecodeEnvelopeInto(raw, "prompt_record", &record); err != nil {
		return cache.ProviderRequestRecord{}, err
	}
	if record.ScopeID != scope || record.Category != promptRequest || record.Ordinal != ordinal || !record.Value.Present {
		return cache.ProviderRequestRecord{}, sessiontree.ErrAuthorityCorrupt
	}
	result := record.Value.Value
	if result.PromptScopeID != scope {
		return result, sessiontree.ErrAuthorityCorrupt
	}
	return result, nil
}

func (kernel *BackendKernel) LatestModelContextRequest(ctx context.Context, scope, thread, entry string) (cache.ProviderRequestRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return cache.ProviderRequestRecord{}, false, err
	}
	kernel.promptMu.Lock()
	defer kernel.promptMu.Unlock()
	if err := ctx.Err(); err != nil {
		return cache.ProviderRequestRecord{}, false, err
	}
	for i := len(kernel.pending[scope]) - 1; i >= 0; i-- {
		pending := kernel.pending[scope][i]
		if pending.Category != promptRequest {
			continue
		}
		var request cache.ProviderRequestRecord
		if err := decodePromptJSON(pending.Value, &request); err != nil {
			return request, false, err
		}
		if request.PromptScopeID != scope {
			return request, false, sessiontree.ErrAuthorityCorrupt
		}
		if request.ThreadID == thread && request.ModelContextEntryID == entry {
			return request, true, nil
		}
	}
	for {
		var result cache.ProviderRequestRecord
		var found, complete bool
		err := kernel.promptBackend.View(ctx, func(tx spi.ReadTx) error {
			counters, err := readPromptCounters(tx, scope)
			if err != nil {
				return err
			}
			count := counters.Counts[promptRequest]
			if count == 0 {
				complete = true
				return nil
			}
			coverage, indexed, err := contextRequestCoverage(tx, scope)
			if err != nil {
				return err
			}
			if indexed && coverage.Through > count {
				return sessiontree.ErrAuthorityCorrupt
			}
			if !indexed || coverage.Through != count {
				return nil
			}
			complete = coverage.First == 1
			raw, err := tx.Get(promptRequestLocatorNamespace, contextRequestKey(scope, thread, entry))
			if errors.Is(err, spi.ErrNotFound) {
				return nil
			}
			if err != nil {
				return err
			}
			var ordinal uint64
			if err := decodePromptJSON(raw, &ordinal); err != nil || ordinal == 0 || ordinal > count {
				return errors.Join(sessiontree.ErrAuthorityCorrupt, err)
			}
			if ordinal < coverage.First {
				return nil
			}
			complete = true
			result, err = readRequestOrdinal(tx, scope, ordinal)
			if err != nil {
				return err
			}
			if result.ThreadID != thread || result.ModelContextEntryID != entry {
				return sessiontree.ErrAuthorityCorrupt
			}
			found = true
			return nil
		})
		if err != nil || complete {
			return result, found, err
		}
		if err := kernel.prepareContextRequestPage(ctx, scope); err != nil {
			return cache.ProviderRequestRecord{}, false, err
		}
	}
}

// Each transaction extends a proven suffix by at most sixteen requests. A
// canceled or failed transaction cannot publish incomplete coverage. A request
// appended by an older writer starts a new suffix; old locators are ignored
// until their ordinals are covered again.
func (kernel *BackendKernel) prepareContextRequestPage(ctx context.Context, scope string) error {
	return kernel.promptBackend.Update(ctx, func(tx spi.WriteTx) error {
		counters, err := readPromptCounters(tx, scope)
		if err != nil {
			return err
		}
		count := counters.Counts[promptRequest]
		coverage, indexed, err := contextRequestCoverage(tx, scope)
		if err != nil {
			return err
		}
		if indexed && coverage.Through > count {
			return sessiontree.ErrAuthorityCorrupt
		}
		if count == 0 {
			return nil
		}
		if count == ^uint64(0) {
			return sessiontree.ErrAuthorityCorrupt
		}
		if !indexed || coverage.Through != count {
			coverage = requestLocatorCoverage{First: count + 1, Through: count}
		}
		end := coverage.First
		start := uint64(1)
		if end > 16 {
			start = end - 16
		}
		keys := make([][]byte, 0, end-start)
		for ordinal := start; ordinal < end; ordinal++ {
			keys = append(keys, promptRecordKey(promptRecord{ScopeID: scope, Category: promptRequest, Ordinal: ordinal}))
		}
		if len(keys) == 0 {
			return nil
		}
		records, err := storagebridge.ReadRecords(tx, storagebridge.RecordBatch{Namespace: promptRecordsNamespace, Keys: keys})
		if err != nil {
			return errors.Join(sessiontree.ErrAuthorityCorrupt, err)
		}
		for ordinal := end - 1; ordinal >= start; ordinal-- {
			if err := ctx.Err(); err != nil {
				return err
			}
			request, err := decodeRequestOrdinal(records[ordinal-start], scope, ordinal)
			if err != nil {
				return err
			}
			key := contextRequestKey(scope, request.ThreadID, request.ModelContextEntryID)
			raw, err := tx.Get(promptRequestLocatorNamespace, key)
			var previous uint64
			if err != nil && !errors.Is(err, spi.ErrNotFound) {
				return err
			}
			if err == nil {
				if err := decodePromptJSON(raw, &previous); err != nil || previous == 0 || previous > count {
					return errors.Join(sessiontree.ErrAuthorityCorrupt, err)
				}
			}
			if previous < ordinal || previous < coverage.First {
				raw, _ = json.Marshal(ordinal)
				if err := tx.Put(promptRequestLocatorNamespace, key, raw); err != nil {
					return err
				}
			}
			coverage.First = ordinal
		}
		return writeContextRequestCoverage(tx, scope, coverage)
	})
}

func deleteContextRequestLocators(tx spi.WriteTx, scope string) error {
	prefix := contextRequestPrefix(scope)
	for {
		page, err := tx.Scan(spi.ScanRequest{Namespace: promptRequestLocatorNamespace, Start: prefix, End: prefixEnd(prefix), Limit: 128})
		if err != nil {
			return err
		}
		for _, row := range page.Records {
			if err := tx.Delete(promptRequestLocatorNamespace, row.Key); err != nil {
				return err
			}
		}
		if !page.HasMore {
			return nil
		}
		if len(page.Records) == 0 {
			return sessiontree.ErrAuthorityCorrupt
		}
	}
}

package storage

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/floegence/floret/v7/internal/provider/cache"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/internal/storagecodec"
	"github.com/floegence/floret/v7/storage/spi"
)

// Disposable locators contain source ordinals, never another configuration or
// measurement authority. Coverage advances in the canonical source transaction.
const promptLookupNamespace = "floret.prompt.lookups.v1"
const promptLookupPageSize = 16
const promptLookupPageBytes = 256 << 10

type turnSourceOrdinals struct{ Surface, Model, Conflict uint64 }

func promptLookupKey(scope, kind string, parts ...string) []byte {
	values := [][]byte{storagecodec.TupleString(scope), storagecodec.TupleString(kind)}
	for _, part := range parts {
		values = append(values, storagecodec.TupleString(part))
	}
	return storagecodec.Tuple(values...)
}
func promptLookupCoverageKey(scope string, category int) []byte {
	return append(promptLookupKey(scope, "coverage"), storagecodec.TupleOrdinal(uint64(category))...)
}
func readLookup[T any](tx spi.ReadTx, key []byte) (T, bool, error) {
	var value T
	raw, err := tx.Get(promptLookupNamespace, key)
	if errors.Is(err, spi.ErrNotFound) {
		return value, false, nil
	}
	if err != nil {
		return value, false, err
	}
	if err := decodePromptJSON(raw, &value); err != nil {
		return value, false, errors.Join(sessiontree.ErrAuthorityCorrupt, err)
	}
	return value, true, nil
}
func putLookup(tx spi.WriteTx, key []byte, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return tx.Put(promptLookupNamespace, key, raw)
}
func readPromptOrdinal[T any](tx spi.ReadTx, scope string, category int, ordinal uint64) (T, error) {
	var zero T
	raw, err := tx.Get(promptRecordsNamespace, promptRecordKey(promptRecord{ScopeID: scope, Category: category, Ordinal: ordinal}))
	if err != nil {
		return zero, errors.Join(sessiontree.ErrAuthorityCorrupt, err)
	}
	var record struct {
		ScopeID  string                    `json:"scope_id"`
		Category int                       `json:"category"`
		Ordinal  uint64                    `json:"ordinal"`
		Value    storagecodec.JSONValue[T] `json:"value"`
	}
	if err := storagecodec.DecodeEnvelopeInto(raw, "prompt_record", &record); err != nil {
		return zero, err
	}
	if record.ScopeID != scope || record.Category != category || record.Ordinal != ordinal || !record.Value.Present {
		return zero, sessiontree.ErrAuthorityCorrupt
	}
	return record.Value.Value, nil
}
func lookupSource(tx spi.ReadTx, key []byte, count uint64) (uint64, bool, error) {
	ordinal, found, err := readLookup[uint64](tx, key)
	if err == nil && found && (ordinal == 0 || ordinal > count) {
		err = sessiontree.ErrAuthorityCorrupt
	}
	return ordinal, found, err
}
func promptSurfaceTurn(request cache.ProviderRequestRecord) string {
	turn := strings.TrimSpace(request.TurnID)
	if turn == "" {
		turn = strings.TrimSpace(request.RunID)
	}
	return turn
}
func indexPromptRecord(tx spi.WriteTx, record promptRecord) error {
	scope, ordinal := record.ScopeID, record.Ordinal
	switch record.Category {
	case promptRequest:
		var request cache.ProviderRequestRecord
		if err := decodePromptJSON(record.Value, &request); err != nil {
			return err
		}
		if request.PromptScopeID != scope {
			return sessiontree.ErrAuthorityCorrupt
		}
		surfaceTurn := promptSurfaceTurn(cache.ProviderRequestRecord{TurnID: request.TurnID, RunID: request.RunID})
		turns := []string{surfaceTurn}
		if request.TurnID != turns[0] {
			turns = append(turns, request.TurnID)
		}
		for _, turn := range turns {
			key := promptLookupKey(scope, "turn", turn)
			refs, _, err := readLookup[turnSourceOrdinals](tx, key)
			if err != nil {
				return err
			}
			if refs.Surface >= ordinal || refs.Model >= ordinal || refs.Conflict >= ordinal {
				return sessiontree.ErrAuthorityCorrupt
			}
			if refs.Surface == 0 && surfaceTurn == turn {
				refs.Surface = ordinal
			}
			if request.TurnID == turn {
				if refs.Model == 0 {
					refs.Model = ordinal
				} else if refs.Conflict == 0 {
					first, err := readPromptOrdinal[cache.ProviderRequestRecord](tx, scope, promptRequest, refs.Model)
					if err != nil {
						return err
					}
					if first.PromptScopeID != scope || first.TurnID != turn {
						return sessiontree.ErrAuthorityCorrupt
					}
					if first.Provider != request.Provider || first.Model != request.Model {
						refs.Conflict = ordinal
					}
				}
			}
			if err := putLookup(tx, key, refs); err != nil {
				return err
			}
		}
		for _, key := range [][]byte{promptLookupKey(scope, "request", request.ID), promptLookupKey(scope, "attempt", request.ID, request.AttemptID)} {
			if err := putLookup(tx, key, ordinal); err != nil {
				return err
			}
		}
	case promptToolset:
		var value cache.ToolsetSnapshot
		if err := decodePromptJSON(record.Value, &value); err != nil {
			return err
		}
		if value.PromptScopeID != scope {
			return sessiontree.ErrAuthorityCorrupt
		}
		return putLookup(tx, promptLookupKey(scope, "toolset", value.Provider, value.Model), ordinal)
	case promptResponse:
		var value cache.ProviderResponseRecord
		if err := decodePromptJSON(record.Value, &value); err != nil {
			return err
		}
		if value.PromptScopeID != scope {
			return sessiontree.ErrAuthorityCorrupt
		}
		anchor := value.PressureAnchor
		if anchor.WindowInputTokens > 0 || anchor.WindowInputTokens == 0 && anchor.InputMeasurementQualified {
			if scope != "" && anchor.PromptScopeID != scope {
				return nil
			}
			keys := map[string][]byte{}
			for _, provider := range []string{"", anchor.Provider} {
				for _, model := range []string{"", anchor.Model} {
					key := promptLookupKey(scope, "pressure", provider, model)
					keys[string(key)] = key
				}
			}
			for _, key := range keys {
				if err := putLookup(tx, key, ordinal); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
func updatePromptLookup(tx spi.WriteTx, record promptRecord) error {
	if record.Category == promptSegment {
		return nil
	}
	key := promptLookupCoverageKey(record.ScopeID, record.Category)
	through, _, err := readLookup[uint64](tx, key)
	if err != nil {
		return err
	}
	if through >= record.Ordinal {
		return sessiontree.ErrAuthorityCorrupt
	}
	// Missing historical prefixes are prepared on demand, never on append.
	if through+1 != record.Ordinal {
		return nil
	}
	if err := indexPromptRecord(tx, record); err != nil {
		return err
	}
	return putLookup(tx, key, record.Ordinal)
}
func (kernel *BackendKernel) preparePromptLookupPage(ctx context.Context, scope string, category int) error {
	kernel.promptMu.Lock()
	defer kernel.promptMu.Unlock()
	return kernel.promptBackend.Update(ctx, func(tx spi.WriteTx) error {
		counter, err := readPromptCounters(tx, scope)
		if err != nil {
			return err
		}
		key := promptLookupCoverageKey(scope, category)
		through, _, err := readLookup[uint64](tx, key)
		if err != nil {
			return err
		}
		count := counter.Counts[category]
		if through > count {
			return sessiontree.ErrAuthorityCorrupt
		}
		end := through + min(uint64(promptLookupPageSize), count-through)
		processed, bytes := through, 0
		for ordinal := through + 1; ordinal <= end && ordinal > 0; ordinal++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			raw, err := tx.Get(promptRecordsNamespace, promptRecordKey(promptRecord{ScopeID: scope, Category: category, Ordinal: ordinal}))
			if err != nil {
				return errors.Join(sessiontree.ErrAuthorityCorrupt, err)
			}
			var record promptRecord
			if err := decodePromptValue(raw, "prompt_record", &record); err != nil {
				return err
			}
			if record.ScopeID != scope || record.Category != category || record.Ordinal != ordinal {
				return sessiontree.ErrAuthorityCorrupt
			}
			if err := indexPromptRecord(tx, record); err != nil {
				return err
			}
			processed = ordinal
			bytes += len(raw)
			if bytes >= promptLookupPageBytes {
				break
			}
		}
		if processed == through {
			return nil
		}
		return putLookup(tx, key, processed)
	})
}

// Release the mutex between preparation pages. Cancellation never publishes
// incomplete coverage, and unrelated operations can progress between pages.
func (kernel *BackendKernel) withPromptLookup(ctx context.Context, scope string, categories []int, read func(spi.ReadTx, promptScopeCounters) error) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		missing := -1
		kernel.promptMu.Lock()
		err := func() error {
			defer kernel.promptMu.Unlock()
			return kernel.promptBackend.View(ctx, func(tx spi.ReadTx) error {
				counters, err := readPromptCounters(tx, scope)
				if err != nil {
					return err
				}
				for _, category := range categories {
					through, _, err := readLookup[uint64](tx, promptLookupCoverageKey(scope, category))
					if err != nil {
						return err
					}
					if through > counters.Counts[category] {
						return sessiontree.ErrAuthorityCorrupt
					}
					if through < counters.Counts[category] {
						missing = category
						return nil
					}
				}
				return read(tx, counters)
			})
		}()
		if err != nil || missing < 0 {
			return err
		}
		if err := kernel.preparePromptLookupPage(ctx, scope, missing); err != nil {
			return err
		}
	}
}
func (kernel *BackendKernel) ReadTurnRequestFacts(ctx context.Context, scope, turn string) (cache.TurnRequestFacts, error) {
	var facts cache.TurnRequestFacts
	err := kernel.withPromptLookup(ctx, scope, []int{promptRequest}, func(tx spi.ReadTx, counters promptScopeCounters) error {
		refs, _, err := readLookup[turnSourceOrdinals](tx, promptLookupKey(scope, "turn", turn))
		if err != nil {
			return err
		}
		count := counters.Counts[promptRequest]
		decoded := map[uint64]cache.ProviderRequestRecord{}
		get := func(ordinal uint64) (cache.ProviderRequestRecord, error) {
			if ordinal == 0 || ordinal > count {
				return cache.ProviderRequestRecord{}, sessiontree.ErrAuthorityCorrupt
			}
			if request, ok := decoded[ordinal]; ok {
				return request, nil
			}
			request, err := readPromptOrdinal[cache.ProviderRequestRecord](tx, scope, promptRequest, ordinal)
			if err == nil && request.PromptScopeID != scope {
				err = sessiontree.ErrAuthorityCorrupt
			}
			if err == nil {
				decoded[ordinal] = request
			}
			return request, err
		}
		if refs.Surface > 0 {
			request, err := get(refs.Surface)
			if err != nil {
				return err
			}
			if promptSurfaceTurn(request) != turn {
				return sessiontree.ErrAuthorityCorrupt
			}
			facts.Surface, facts.SurfaceFound = request.TurnSurface, true
		}
		if refs.Model > 0 {
			request, err := get(refs.Model)
			if err != nil {
				return err
			}
			if request.TurnID != turn {
				return sessiontree.ErrAuthorityCorrupt
			}
			facts.Provider, facts.Model, facts.ModelFound = request.Provider, request.Model, true
		}
		if refs.Conflict > 0 {
			request, err := get(refs.Conflict)
			if err != nil {
				return err
			}
			if !facts.ModelFound || request.TurnID != turn || request.Provider == facts.Provider && request.Model == facts.Model {
				return sessiontree.ErrAuthorityCorrupt
			}
			facts.ModelDrift = true
		}
		for _, record := range kernel.pending[scope] {
			if record.Category == promptRequest {
				var request cache.ProviderRequestRecord
				if err := decodePromptJSON(record.Value, &request); err != nil {
					return err
				}
				if request.PromptScopeID != scope {
					return sessiontree.ErrAuthorityCorrupt
				}
				if !facts.SurfaceFound && promptSurfaceTurn(request) == turn {
					facts.Surface, facts.SurfaceFound = request.TurnSurface, true
				}
				if request.TurnID == turn {
					if facts.ModelFound && (facts.Provider != request.Provider || facts.Model != request.Model) {
						facts.ModelDrift = true
					}
					if !facts.ModelFound {
						facts.Provider, facts.Model, facts.ModelFound = request.Provider, request.Model, true
					}
				}
			}
		}
		return nil
	})
	return facts, err
}
func (kernel *BackendKernel) lookupActiveToolset(ctx context.Context, scope, provider, model string) (cache.ToolsetSnapshot, bool, error) {
	var result cache.ToolsetSnapshot
	found := false
	err := kernel.withPromptLookup(ctx, scope, []int{promptToolset}, func(tx spi.ReadTx, counters promptScopeCounters) error {
		pending := kernel.pending[scope]
		for i := len(pending) - 1; i >= 0; i-- {
			if pending[i].Category == promptToolset {
				var value cache.ToolsetSnapshot
				if err := decodePromptJSON(pending[i].Value, &value); err != nil {
					return err
				}
				if value.PromptScopeID != scope {
					return sessiontree.ErrAuthorityCorrupt
				}
				if value.Provider == provider && value.Model == model {
					result, found = value, true
					return nil
				}
			}
		}
		ordinal, ok, err := lookupSource(tx, promptLookupKey(scope, "toolset", provider, model), counters.Counts[promptToolset])
		if err != nil || !ok {
			return err
		}
		result, err = readPromptOrdinal[cache.ToolsetSnapshot](tx, scope, promptToolset, ordinal)
		if err != nil {
			return err
		}
		if result.PromptScopeID != scope || result.Provider != provider || result.Model != model {
			return sessiontree.ErrAuthorityCorrupt
		}
		found = true
		return nil
	})
	return result, found, err
}
func matchingPressure(anchor cache.PressureAnchorState, scope, provider, model string) bool {
	return (anchor.WindowInputTokens > 0 || anchor.WindowInputTokens == 0 && anchor.InputMeasurementQualified) && (scope == "" || anchor.PromptScopeID == scope) && (provider == "" || anchor.Provider == provider) && (model == "" || anchor.Model == model)
}
func (kernel *BackendKernel) lookupPressureResponse(tx spi.ReadTx, counters promptScopeCounters, scope, provider, model string) (cache.ProviderResponseRecord, bool, error) {
	pending := kernel.pending[scope]
	for i := len(pending) - 1; i >= 0; i-- {
		if pending[i].Category == promptResponse {
			var response cache.ProviderResponseRecord
			if err := decodePromptJSON(pending[i].Value, &response); err != nil {
				return response, false, err
			}
			if response.PromptScopeID != scope {
				return response, false, sessiontree.ErrAuthorityCorrupt
			}
			if matchingPressure(response.PressureAnchor, scope, provider, model) {
				return response, true, nil
			}
		}
	}
	ordinal, found, err := lookupSource(tx, promptLookupKey(scope, "pressure", provider, model), counters.Counts[promptResponse])
	if err != nil || !found {
		return cache.ProviderResponseRecord{}, false, err
	}
	response, err := readPromptOrdinal[cache.ProviderResponseRecord](tx, scope, promptResponse, ordinal)
	if err == nil && (response.PromptScopeID != scope || !matchingPressure(response.PressureAnchor, scope, provider, model)) {
		err = sessiontree.ErrAuthorityCorrupt
	}
	return response, err == nil, err
}
func (kernel *BackendKernel) lookupPressureAnchor(ctx context.Context, scope, provider, model string) (cache.PressureAnchorState, bool, error) {
	var result cache.PressureAnchorState
	found := false
	err := kernel.withPromptLookup(ctx, scope, []int{promptResponse}, func(tx spi.ReadTx, counters promptScopeCounters) error {
		response, ok, err := kernel.lookupPressureResponse(tx, counters, scope, provider, model)
		result, found = response.PressureAnchor, ok
		return err
	})
	return result, found, err
}
func (kernel *BackendKernel) ReadPressureEvidence(ctx context.Context, scope, provider, model string) (cache.PressureEvidence, bool, error) {
	var result cache.PressureEvidence
	found := false
	err := kernel.withPromptLookup(ctx, scope, []int{promptResponse, promptRequest}, func(tx spi.ReadTx, counters promptScopeCounters) error {
		response, ok, err := kernel.lookupPressureResponse(tx, counters, scope, provider, model)
		if err != nil || !ok {
			return err
		}
		anchor := response.PressureAnchor
		measured := anchor.MeasurementPrefix != (cache.PressureAnchorState{}).MeasurementPrefix
		match := func(request cache.ProviderRequestRecord) bool {
			return request.ID == anchor.RequestID && (!measured || request.AttemptID == response.AttemptID)
		}
		var request cache.ProviderRequestRecord
		requestFound := false
		pending := kernel.pending[scope]
		for i := len(pending) - 1; i >= 0; i-- {
			if pending[i].Category == promptRequest {
				var candidate cache.ProviderRequestRecord
				if err := decodePromptJSON(pending[i].Value, &candidate); err != nil {
					return err
				}
				if candidate.PromptScopeID != scope {
					return sessiontree.ErrAuthorityCorrupt
				}
				if match(candidate) {
					request, requestFound = candidate, true
					break
				}
			}
		}
		if !requestFound {
			key := promptLookupKey(scope, "request", anchor.RequestID)
			if measured {
				key = promptLookupKey(scope, "attempt", anchor.RequestID, response.AttemptID)
			}
			ordinal, ok, err := lookupSource(tx, key, counters.Counts[promptRequest])
			if err != nil || !ok {
				return err
			}
			request, err = readPromptOrdinal[cache.ProviderRequestRecord](tx, scope, promptRequest, ordinal)
			if err != nil {
				return err
			}
			if request.PromptScopeID != scope || !match(request) {
				return sessiontree.ErrAuthorityCorrupt
			}
		}
		result = cache.PressureEvidence{Anchor: anchor, Request: request}
		if measured {
			result.Response = &response
		}
		found = true
		return nil
	})
	return result, found, err
}
func deletePromptLookups(tx spi.WriteTx, scope string) error {
	prefix := storagecodec.Tuple(storagecodec.TupleString(scope))
	for {
		page, err := tx.Scan(spi.ScanRequest{Namespace: promptLookupNamespace, Start: prefix, End: prefixEnd(prefix), Limit: 128})
		if err != nil {
			return err
		}
		for _, row := range page.Records {
			if err := tx.Delete(promptLookupNamespace, row.Key); err != nil {
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

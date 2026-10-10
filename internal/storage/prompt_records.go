package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/floegence/floret/v7/internal/provider/cache"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/internal/storagecodec"
	"github.com/floegence/floret/v7/storage/spi"
)

const promptRecordsNamespace = "floret.domain.prompt.v2"
const promptCategoryCount = 4
const (
	promptSegment = iota
	promptToolset
	promptRequest
	promptResponse
)

var promptSchemaKey = storagecodec.Tuple(storagecodec.TupleString("schema"))
var promptStateKey = storagecodec.Tuple(storagecodec.TupleString("prompt"), storagecodec.TupleString("state"))

type promptScopeCounters struct {
	ScopeID string                      `json:"scope_id"`
	Counts  [promptCategoryCount]uint64 `json:"counts"`
}

type promptRecord struct {
	ScopeID  string          `json:"scope_id"`
	Category int             `json:"category"`
	Ordinal  uint64          `json:"ordinal"`
	Value    json.RawMessage `json:"value"`
}

type promptBatch []promptRecord

func promptScopeKey(scope string) []byte {
	return storagecodec.Tuple(storagecodec.TupleString("scope"), storagecodec.TupleString(scope))
}
func promptRecordPrefix(scope string, category int) []byte {
	return storagecodec.Tuple(storagecodec.TupleString("record"), storagecodec.TupleString(scope), storagecodec.TupleOrdinal(uint64(category)))
}
func promptRecordKey(record promptRecord) []byte {
	return append(promptRecordPrefix(record.ScopeID, record.Category), storagecodec.TupleOrdinal(record.Ordinal)...)
}
func prefixEnd(prefix []byte) []byte {
	end := bytes.Clone(prefix)
	for index := len(end) - 1; index >= 0; index-- {
		if end[index] != 255 {
			end[index]++
			return end[:index+1]
		}
	}
	return nil
}

func decodePromptValue(encoded []byte, kind string, value any) error {
	payload, err := storagecodec.DecodeEnvelope(encoded, kind)
	if err != nil {
		return err
	}
	return decodePromptJSON(payload, value)
}

func decodePromptJSON(payload []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("prompt value contains trailing data")
	}
	return nil
}

func putPromptValue(tx spi.WriteTx, key []byte, kind string, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	encoded, err := storagecodec.EncodeEnvelope(kind, payload)
	if err != nil {
		return err
	}
	return tx.Put(promptRecordsNamespace, key, encoded)
}

func makePromptRecord(scope string, category int, value any) (promptRecord, error) {
	if strings.TrimSpace(scope) == "" {
		return promptRecord{}, errors.New("prompt scope id is required")
	}
	payload, err := json.Marshal(value)
	return promptRecord{ScopeID: scope, Category: category, Value: payload}, err
}

func readPromptCounters(tx spi.ReadTx, scope string) (promptScopeCounters, error) {
	value := promptScopeCounters{ScopeID: scope}
	encoded, err := tx.Get(promptRecordsNamespace, promptScopeKey(scope))
	if errors.Is(err, spi.ErrNotFound) {
		return value, nil
	}
	if err != nil {
		return value, err
	}
	if err := decodePromptValue(encoded, "prompt_scope", &value); err != nil {
		return value, err
	}
	if value.ScopeID != scope {
		return value, errors.New("prompt scope metadata identity mismatch")
	}
	return value, nil
}

func writePromptBatch(tx spi.WriteTx, batch promptBatch) error {
	counters := map[string]promptScopeCounters{}
	for _, record := range batch {
		counter, ok := counters[record.ScopeID]
		if !ok {
			var err error
			counter, err = readPromptCounters(tx, record.ScopeID)
			if err != nil {
				return err
			}
		}
		if counter.Counts[record.Category] == ^uint64(0) {
			return errors.New("prompt ordinal overflow")
		}
		counter.Counts[record.Category]++
		record.Ordinal = counter.Counts[record.Category]
		if err := putPromptValue(tx, promptRecordKey(record), "prompt_record", record); err != nil {
			return err
		}
		counters[record.ScopeID] = counter
	}
	for scope, counter := range counters {
		if err := putPromptValue(tx, promptScopeKey(scope), "prompt_scope", counter); err != nil {
			return err
		}
	}
	return nil
}

func scanPromptRecords(tx spi.ReadTx, start, end []byte, visit func(spi.Record) error) error {
	request := spi.ScanRequest{Namespace: promptRecordsNamespace, Start: start, End: end, Limit: 256}
	for {
		page, err := tx.Scan(request)
		if err != nil {
			return err
		}
		for _, record := range page.Records {
			if err := visit(record); err != nil {
				return err
			}
		}
		if !page.HasMore {
			return nil
		}
		request.After = page.Next
	}
}

func validatePromptRecordValue(record promptRecord) error {
	var scope string
	switch record.Category {
	case promptSegment:
		var value cache.Segment
		if err := decodePromptJSON(record.Value, &value); err != nil {
			return err
		}
		scope = value.PromptScopeID
	case promptToolset:
		var value cache.ToolsetSnapshot
		if err := decodePromptJSON(record.Value, &value); err != nil {
			return err
		}
		scope = value.PromptScopeID
	case promptRequest:
		var value cache.ProviderRequestRecord
		if err := decodePromptJSON(record.Value, &value); err != nil {
			return err
		}
		scope = value.PromptScopeID
	case promptResponse:
		var value cache.ProviderResponseRecord
		if err := decodePromptJSON(record.Value, &value); err != nil {
			return err
		}
		scope = value.PromptScopeID
	default:
		return errors.New("unknown prompt record category")
	}
	if strings.TrimSpace(scope) == "" || scope != record.ScopeID {
		return errors.New("prompt record scope identity mismatch")
	}
	return nil
}

func verifyPromptRecords(tx spi.ReadTx) error {
	counts := map[string][promptCategoryCount]uint64{}
	metadata := map[string][promptCategoryCount]uint64{}
	schemaFound := false
	err := scanPromptRecords(tx, nil, nil, func(row spi.Record) error {
		switch {
		case bytes.Equal(row.Key, promptSchemaKey):
			var version int
			if err := decodePromptValue(row.Value, "prompt_schema", &version); err != nil {
				return err
			}
			if version != 2 {
				return errors.New("unsupported prompt schema")
			}
			schemaFound = true
		case bytes.HasPrefix(row.Key, storagecodec.Tuple(storagecodec.TupleString("scope"))):
			var counter promptScopeCounters
			if err := decodePromptValue(row.Value, "prompt_scope", &counter); err != nil {
				return err
			}
			if strings.TrimSpace(counter.ScopeID) == "" || !bytes.Equal(row.Key, promptScopeKey(counter.ScopeID)) {
				return errors.New("prompt scope metadata key mismatch")
			}
			metadata[counter.ScopeID] = counter.Counts
		default:
			var record promptRecord
			if err := decodePromptValue(row.Value, "prompt_record", &record); err != nil {
				return err
			}
			if err := validatePromptRecordValue(record); err != nil {
				return err
			}
			counter := counts[record.ScopeID]
			if record.Ordinal != counter[record.Category]+1 || !bytes.Equal(row.Key, promptRecordKey(record)) {
				return errors.New("prompt append sequence or key mismatch")
			}
			counter[record.Category]++
			counts[record.ScopeID] = counter
		}
		return nil
	})
	if err != nil {
		return errors.Join(sessiontree.ErrAuthorityCorrupt, err)
	}
	if !schemaFound || len(counts) != len(metadata) {
		return errors.Join(sessiontree.ErrAuthorityCorrupt, errors.New("prompt schema or scope inventory missing"))
	}
	for scope, counter := range counts {
		if metadata[scope] != counter {
			return errors.Join(sessiontree.ErrAuthorityCorrupt, errors.New("prompt scope counters mismatch"))
		}
	}
	return nil
}

func (kernel *BackendKernel) readPromptCategory(ctx context.Context, scope string, category int) (promptBatch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var result promptBatch
	err := kernel.promptBackend.View(ctx, func(tx spi.ReadTx) error {
		prefix := promptRecordPrefix(scope, category)
		return scanPromptRecords(tx, prefix, prefixEnd(prefix), func(row spi.Record) error {
			var record promptRecord
			if err := decodePromptValue(row.Value, "prompt_record", &record); err != nil {
				return err
			}
			result = append(result, record)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	for _, record := range kernel.pending[scope] {
		if record.Category == category {
			result = append(result, record)
		}
	}
	return result, nil
}

func (kernel *BackendKernel) flushPrompt(tx spi.WriteTx, scopes ...string) error {
	for _, scope := range scopes {
		if err := writePromptBatch(tx, kernel.pending[scope]); err != nil {
			return err
		}
	}
	return nil
}

func (kernel *BackendKernel) clearPending(scopes ...string) {
	for _, scope := range scopes {
		delete(kernel.pending, scope)
	}
}
func (kernel *BackendKernel) pendingScopes() []string {
	var scopes []string
	for scope := range kernel.pending {
		scopes = append(scopes, scope)
	}
	return scopes
}

func deletePromptScope(tx spi.WriteTx, scope string) error {
	prefix := storagecodec.Tuple(storagecodec.TupleString("record"), storagecodec.TupleString(scope))
	var keys [][]byte
	if err := scanPromptRecords(tx, prefix, prefixEnd(prefix), func(row spi.Record) error { keys = append(keys, row.Key); return nil }); err != nil {
		return err
	}
	for _, key := range keys {
		if err := tx.Delete(promptRecordsNamespace, key); err != nil {
			return err
		}
	}
	return tx.Delete(promptRecordsNamespace, promptScopeKey(scope))
}

func decodePromptBatch[T any](batch promptBatch) ([]T, error) {
	result := make([]T, 0, len(batch))
	for _, record := range batch {
		var value T
		if err := decodePromptJSON(record.Value, &value); err != nil {
			return nil, fmt.Errorf("decode prompt record: %w", err)
		}
		result = append(result, value)
	}
	return result, nil
}

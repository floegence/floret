package sessiontree

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/floegence/floret/v3/storage/spi"
)

const (
	backendDomainJournalNamespace = "floret.domain.sessiontree.journal.v1"
	backendDomainJournalVersion   = 1
	backendDomainJournalScanLimit = 256
)

type backendDomainJournalPatch struct {
	Set     map[string]map[string]json.RawMessage `json:"set,omitempty"`
	Delete  map[string][]string                   `json:"delete,omitempty"`
	Replace map[string]json.RawMessage            `json:"replace,omitempty"`
}

type backendDomainJournalFrame struct {
	Version  int                       `json:"version"`
	Sequence uint64                    `json:"sequence"`
	Patch    backendDomainJournalPatch `json:"patch"`
	Checksum string                    `json:"checksum"`
}

func backendJournalKey(sequence uint64) []byte {
	return []byte(fmt.Sprintf("%020d", sequence))
}

func parseBackendJournalKey(key []byte) (uint64, error) {
	if len(key) != 20 {
		return 0, errors.New("journal key must contain a 20-digit sequence")
	}
	sequence, err := strconv.ParseUint(string(key), 10, 64)
	if err != nil || sequence == 0 {
		return 0, errors.New("journal key sequence is invalid")
	}
	return sequence, nil
}

func buildBackendDomainJournalFrame(sequence uint64, before, after []byte) (backendDomainJournalFrame, bool, error) {
	if sequence == 0 {
		return backendDomainJournalFrame{}, false, errors.New("journal sequence is required")
	}
	patch, changed, err := buildBackendDomainJournalPatch(before, after)
	if err != nil || !changed {
		return backendDomainJournalFrame{}, changed, err
	}
	checksum, err := backendDomainJournalChecksum(sequence, patch)
	if err != nil {
		return backendDomainJournalFrame{}, false, err
	}
	return backendDomainJournalFrame{
		Version: backendDomainJournalVersion, Sequence: sequence, Patch: patch, Checksum: checksum,
	}, true, nil
}

func buildBackendDomainJournalPatch(before, after []byte) (backendDomainJournalPatch, bool, error) {
	var beforeState map[string]json.RawMessage
	var afterState map[string]json.RawMessage
	if err := decodeJSONObject(before, &beforeState); err != nil {
		return backendDomainJournalPatch{}, false, fmt.Errorf("decode journal base: %w", err)
	}
	if err := decodeJSONObject(after, &afterState); err != nil {
		return backendDomainJournalPatch{}, false, fmt.Errorf("decode journal result: %w", err)
	}
	patch := backendDomainJournalPatch{}
	for field, afterValue := range afterState {
		beforeValue, found := beforeState[field]
		if found && bytes.Equal(beforeValue, afterValue) {
			continue
		}
		beforeObject, beforeIsObject := rawJSONObject(beforeValue)
		afterObject, afterIsObject := rawJSONObject(afterValue)
		if found && beforeIsObject && afterIsObject {
			for key, value := range afterObject {
				if current, ok := beforeObject[key]; ok && bytes.Equal(current, value) {
					continue
				}
				if patch.Set == nil {
					patch.Set = make(map[string]map[string]json.RawMessage)
				}
				if patch.Set[field] == nil {
					patch.Set[field] = make(map[string]json.RawMessage)
				}
				patch.Set[field][key] = bytes.Clone(value)
			}
			for key := range beforeObject {
				if _, ok := afterObject[key]; ok {
					continue
				}
				if patch.Delete == nil {
					patch.Delete = make(map[string][]string)
				}
				patch.Delete[field] = append(patch.Delete[field], key)
			}
			continue
		}
		if patch.Replace == nil {
			patch.Replace = make(map[string]json.RawMessage)
		}
		patch.Replace[field] = bytes.Clone(afterValue)
	}
	for field := range beforeState {
		if _, found := afterState[field]; found {
			continue
		}
		if patch.Replace == nil {
			patch.Replace = make(map[string]json.RawMessage)
		}
		patch.Replace[field] = json.RawMessage("null")
	}
	changed := len(patch.Set) > 0 || len(patch.Delete) > 0 || len(patch.Replace) > 0
	return patch, changed, nil
}

func applyBackendDomainJournalPatch(base []byte, patch backendDomainJournalPatch) ([]byte, error) {
	var state map[string]json.RawMessage
	if err := decodeJSONObject(base, &state); err != nil {
		return nil, err
	}
	for field, replacement := range patch.Replace {
		if bytes.Equal(bytes.TrimSpace(replacement), []byte("null")) {
			delete(state, field)
			continue
		}
		state[field] = bytes.Clone(replacement)
	}
	for field, values := range patch.Set {
		object, ok := rawJSONObject(state[field])
		if !ok {
			return nil, fmt.Errorf("journal field %q is not an object", field)
		}
		for key, value := range values {
			object[key] = bytes.Clone(value)
		}
		encoded, err := json.Marshal(object)
		if err != nil {
			return nil, err
		}
		state[field] = encoded
	}
	for field, keys := range patch.Delete {
		object, ok := rawJSONObject(state[field])
		if !ok {
			return nil, fmt.Errorf("journal field %q is not an object", field)
		}
		for _, key := range keys {
			delete(object, key)
		}
		encoded, err := json.Marshal(object)
		if err != nil {
			return nil, err
		}
		state[field] = encoded
	}
	return json.Marshal(state)
}

func encodeBackendDomainJournalFrame(frame backendDomainJournalFrame) ([]byte, error) {
	return json.Marshal(frame)
}

func decodeBackendDomainJournalFrame(raw []byte) (backendDomainJournalFrame, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var frame backendDomainJournalFrame
	if err := decoder.Decode(&frame); err != nil {
		return backendDomainJournalFrame{}, err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return backendDomainJournalFrame{}, errors.New("journal frame contains trailing data")
	}
	if frame.Version != backendDomainJournalVersion || frame.Sequence == 0 || frame.Checksum == "" {
		return backendDomainJournalFrame{}, errors.New("journal frame header is invalid")
	}
	want, err := backendDomainJournalChecksum(frame.Sequence, frame.Patch)
	if err != nil {
		return backendDomainJournalFrame{}, err
	}
	if frame.Checksum != want {
		return backendDomainJournalFrame{}, errors.New("journal frame checksum does not match")
	}
	return frame, nil
}

func backendDomainJournalChecksum(sequence uint64, patch backendDomainJournalPatch) (string, error) {
	payload, err := json.Marshal(struct {
		Version  int                       `json:"version"`
		Sequence uint64                    `json:"sequence"`
		Patch    backendDomainJournalPatch `json:"patch"`
	}{Version: backendDomainJournalVersion, Sequence: sequence, Patch: patch})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func replayBackendDomainJournal(ctx context.Context, tx spi.WriteTx, checkpoint *MemoryRepo, now func() time.Time) (*MemoryRepo, uint64, error) {
	if checkpoint == nil {
		return nil, 0, errors.New("journal checkpoint is required")
	}
	payload, err := checkpoint.EncodeMemoryState()
	if err != nil {
		return nil, 0, err
	}
	records, err := scanBackendDomainJournal(ctx, tx)
	if err != nil {
		return nil, 0, err
	}
	current := checkpoint
	var sequence uint64
	for index, record := range records {
		keySequence, keyErr := parseBackendJournalKey(record.Key)
		if keyErr != nil || keySequence != sequence+1 {
			return nil, 0, errors.Join(ErrAuthorityCorrupt, keyErr, errors.New("journal sequence is not contiguous"))
		}
		frame, frameErr := decodeBackendDomainJournalFrame(record.Value)
		if frameErr != nil {
			if index != len(records)-1 {
				return nil, 0, errors.Join(ErrAuthorityCorrupt, frameErr)
			}
			if deleteErr := tx.Delete(backendDomainJournalNamespace, record.Key); deleteErr != nil {
				return nil, 0, deleteErr
			}
			break
		}
		if frame.Sequence != keySequence {
			return nil, 0, errors.Join(ErrAuthorityCorrupt, errors.New("journal key and frame sequence differ"))
		}
		payload, err = applyBackendDomainJournalPatch(payload, frame.Patch)
		if err != nil {
			return nil, 0, errors.Join(ErrAuthorityCorrupt, err)
		}
		current, err = DecodeMemoryState(payload, now)
		if err != nil {
			return nil, 0, errors.Join(ErrAuthorityCorrupt, err)
		}
		sequence = frame.Sequence
	}
	return current, sequence, nil
}

func scanBackendDomainJournal(ctx context.Context, tx spi.ReadTx) ([]spi.Record, error) {
	var records []spi.Record
	var after []byte
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		page, err := tx.Scan(spi.ScanRequest{
			Namespace: backendDomainJournalNamespace, After: after, Limit: backendDomainJournalScanLimit,
		})
		if err != nil {
			return nil, err
		}
		records = append(records, page.Records...)
		if !page.HasMore {
			return records, nil
		}
		if len(page.Next) == 0 {
			return nil, errors.Join(ErrAuthorityCorrupt, errors.New("journal scan cursor is missing"))
		}
		after = page.Next
	}
}

func clearBackendDomainJournal(ctx context.Context, tx spi.WriteTx) error {
	records, err := scanBackendDomainJournal(ctx, tx)
	if err != nil {
		return err
	}
	for _, record := range records {
		if err := tx.Delete(backendDomainJournalNamespace, record.Key); err != nil {
			return err
		}
	}
	return nil
}

func decodeJSONObject(raw []byte, target *map[string]json.RawMessage) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if *target == nil {
		return errors.New("JSON object is null")
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return errors.New("JSON object contains trailing data")
	}
	return nil
}

func rawJSONObject(raw []byte) (map[string]json.RawMessage, bool) {
	if len(bytes.TrimSpace(raw)) == 0 || bytes.TrimSpace(raw)[0] != '{' {
		return nil, false
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil || object == nil {
		return nil, false
	}
	return object, true
}

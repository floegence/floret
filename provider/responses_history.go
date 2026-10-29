package provider

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

const responsesHistoryMaxBytes = 8 << 20

// ResponsesHistoryOptions identifies an adapter's native Responses state.
// Kind and ID must match its persisted state contract. FunctionNamespace is
// empty for ordinary Responses and explicit for namespaced function calls.
// The Gateway identity remains responsible for model/account/endpoint isolation.
type ResponsesHistoryOptions struct {
	Kind              string
	ID                string
	FunctionNamespace string
}

// ResponsesHistory is an immutable preparation of native output bound to the
// exact rendered canonical input. It owns no transport and executes no tools.
// Use Input for complete replay and estimation; Continuation is optional only
// when the endpoint and request reuse policy allow server-side continuation.
type ResponsesHistory struct {
	options           ResponsesHistoryOptions
	canonical         []json.RawMessage
	records           []responsesHistoryRecord
	input             []json.RawMessage
	responseID        string
	continuationStart int
}

// This envelope also reads the published Redeven ChatGPT native_output format.
// Keep its field names and canonical fingerprints stable throughout v7.
type responsesHistoryRecord struct {
	Start   int               `json:"start"`
	Prefix  string            `json:"prefix"`
	Visible []json.RawMessage `json:"visible"`
	Output  []json.RawMessage `json:"output"`
}

// PrepareResponsesHistory validates previous opaque state and replaces exactly
// matching canonical output segments with their original provider items. A
// changed prefix (compaction or fork) drops that suffix, including its response
// ID; a changed visible segment under an unchanged prefix fails explicitly.
// Nil previous state starts a new history. Callers must enforce ReusePolicy
// before supplying or retaining state, especially for supplemental secrets.
func PrepareResponsesHistory(options ResponsesHistoryOptions, canonical []json.RawMessage, previous *State) (*ResponsesHistory, error) {
	if strings.TrimSpace(options.Kind) == "" || strings.TrimSpace(options.ID) == "" {
		return nil, errors.New("missing Responses history identity")
	}
	normalized, err := normalizeResponsesItems(canonical)
	if err != nil {
		return nil, err
	}
	h := &ResponsesHistory{options: options, canonical: normalized, records: []responsesHistoryRecord{}, continuationStart: -1}
	records := []responsesHistoryRecord{}
	if previous != nil {
		if previous.Kind != options.Kind || previous.ID != options.ID {
			return nil, errors.New("incompatible Responses history identity")
		}
		if previous.ReusePolicy != StateReuseExactSurface && previous.ReusePolicy != StateReuseCanonicalHistory {
			return nil, errors.New("unsupported Responses history reuse policy")
		}
		for key := range previous.Attributes {
			if key != "native_output" && key != "response_id" {
				return nil, errors.New("unsupported Responses history attribute")
			}
		}
		raw := previous.Attributes["native_output"]
		if len(raw) > responsesHistoryMaxBytes {
			return nil, errors.New("Responses history exceeds state limit")
		}
		decoder := json.NewDecoder(strings.NewReader(raw))
		decoder.DisallowUnknownFields()
		if err = decoder.Decode(&records); err != nil || records == nil {
			return nil, errors.New("invalid Responses history records")
		}
		if decoder.Decode(new(any)) != io.EOF {
			return nil, errors.New("trailing Responses history data")
		}
		if id, ok := previous.Attributes["response_id"]; ok {
			if !validResponsesResponseID(id) {
				return nil, errors.New("invalid Responses response identity")
			}
			h.responseID = id
		}
	}
	cursor, previousEnd := 0, 0
	pruned := false
	for _, record := range records {
		if record.Start < previousEnd || record.Start < 0 || len(record.Prefix) != 64 || len(record.Output) == 0 {
			return nil, errors.New("invalid Responses history record")
		}
		// Bound arithmetic independently of untrusted integer values.
		if record.Start > int(^uint(0)>>1)-len(record.Visible) {
			return nil, errors.New("invalid Responses history boundary")
		}
		previousEnd = record.Start + len(record.Visible)
		if err = validateResponsesOutput(record.Output, options.FunctionNamespace); err != nil {
			return nil, err
		}
		visible, normalizeErr := normalizeResponsesItems(record.Visible)
		if normalizeErr != nil {
			return nil, normalizeErr
		}
		record.Visible = visible
		if pruned || record.Start > len(normalized) || responsesFingerprint(normalized[:record.Start]) != record.Prefix {
			pruned = true
			continue
		}
		end := record.Start + len(visible)
		if end > len(normalized) || responsesFingerprint(normalized[record.Start:end]) != responsesFingerprint(visible) {
			return nil, errors.New("Responses native output does not match canonical history")
		}
		h.input = append(h.input, normalized[cursor:record.Start]...)
		h.input = append(h.input, record.Output...)
		h.records = append(h.records, record)
		cursor = end
	}
	h.input = append(h.input, normalized[cursor:]...)
	if pruned || len(records) == 0 {
		h.responseID = ""
	} else if h.responseID != "" {
		h.continuationStart = cursor
	}
	return h, nil
}

// Input returns detached complete native history, including hosted search and
// reasoning output. It must remain available even when continuation is selected.
func (h *ResponsesHistory) Input() []json.RawMessage { return cloneResponsesItems(h.input) }

// Continuation returns the response ID and only the canonical input after that
// response. An empty ID means no valid server continuation is available. This
// method never probes an endpoint or retries a failed request.
func (h *ResponsesHistory) Continuation() (string, []json.RawMessage) {
	if h.responseID == "" || h.continuationStart < 0 {
		return "", nil
	}
	return h.responseID, cloneResponsesItems(h.canonical[h.continuationStart:])
}

// Commit binds a confirmed terminal response's output to its canonical visible
// projection. The returned opaque State must be committed with that projection.
// An empty responseID permits stateless replay on endpoints without response IDs.
// A terminal incomplete response may retain its exact snapshot for length
// continuation. An interrupted stream or failed response must not be committed.
// The adapter validates terminal evidence; the Engine owns finish disposition
// and canonical publication. Neither h nor inputs mutate.
func (h *ResponsesHistory) Commit(responseID string, visible, output []json.RawMessage) (*State, error) {
	if responseID != "" && !validResponsesResponseID(responseID) {
		return nil, errors.New("invalid Responses response identity")
	}
	if len(output) == 0 {
		return nil, errors.New("missing native Responses output")
	}
	if err := validateResponsesOutput(output, h.options.FunctionNamespace); err != nil {
		return nil, err
	}
	normalized, err := normalizeResponsesItems(visible)
	if err != nil {
		return nil, err
	}
	records := append([]responsesHistoryRecord(nil), h.records...)
	records = append(records, responsesHistoryRecord{Start: len(h.canonical), Prefix: responsesFingerprint(h.canonical), Visible: normalized, Output: cloneResponsesItems(output)})
	raw, err := json.Marshal(records)
	if err != nil {
		return nil, err
	}
	if len(raw) > responsesHistoryMaxBytes {
		return nil, errors.New("Responses history exceeds state limit")
	}
	attrs := map[string]string{"native_output": string(raw)}
	if responseID != "" {
		attrs["response_id"] = responseID
	}
	return &State{Kind: h.options.Kind, ID: h.options.ID, ReusePolicy: StateReuseCanonicalHistory, Attributes: attrs}, nil
}

func validResponsesResponseID(id string) bool {
	return strings.TrimSpace(id) != "" && len(id) <= 512 && !strings.ContainsAny(id, "\r\n")
}
func responsesFingerprint(items []json.RawMessage) string {
	raw, _ := json.Marshal(items)
	return fmt.Sprintf("%x", sha256.Sum256(raw))
}
func cloneResponsesItems(items []json.RawMessage) []json.RawMessage {
	out := make([]json.RawMessage, len(items))
	for i, item := range items {
		out[i] = bytes.Clone(item)
	}
	return out
}
func normalizeResponsesItems(items []json.RawMessage) ([]json.RawMessage, error) {
	out := make([]json.RawMessage, 0, len(items))
	for _, raw := range items {
		var item map[string]json.RawMessage
		if json.Unmarshal(raw, &item) != nil || item == nil {
			return nil, errors.New("invalid Responses item")
		}
		encoded, err := json.Marshal(item)
		if err != nil {
			return nil, err
		}
		out = append(out, encoded)
	}
	return out, nil
}
func validateResponsesOutput(items []json.RawMessage, namespace string) error {
	for _, raw := range items {
		var item struct {
			Type      string `json:"type"`
			Namespace string `json:"namespace"`
		}
		if json.Unmarshal(raw, &item) != nil {
			return errors.New("invalid native Responses output")
		}
		switch item.Type {
		case "reasoning", "message", "web_search_call":
		case "function_call":
			if item.Namespace != namespace {
				return errors.New("unexpected Responses function namespace")
			}
		default:
			return fmt.Errorf("unsupported native Responses output %q", item.Type)
		}
	}
	return nil
}

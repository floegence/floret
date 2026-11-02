package provider

import (
	"bytes"
	"encoding/json"
	"reflect"
	"sort"
	"strings"
)

// ResponsesOutput collects native response.output_item.done items for one
// request. Its zero value is ready to use; it is not safe for concurrent use.
// Add validates duplicate events strictly. Complete reconciles only a confirmed
// terminal snapshot, which owns the final opaque reasoning bytes. The adapter
// remains responsible for terminal success, usage, namespace and tool validation.
type ResponsesOutput struct {
	items map[int64]json.RawMessage
	bytes int
}

// ResponsesOutputError reports bounded structural facts, never provider values.
// Code is response_output_limit_exceeded, response_output_invalid,
// response_output_conflict, response_output_missing,
// response_snapshot_incomplete, or response_output_index_missing. OutputIndex
// identifies a conflicting item. Fields contains sorted known top-level field
// names; unknown names collapse to "other". Callers may use errors.As.
type ResponsesOutputError struct {
	Code        string
	OutputIndex *int64
	Fields      []string
}

func (e *ResponsesOutputError) Error() string { return e.Code }

// Add retains a defensive copy of an item at a nonnegative output index.
// Repeated events must be JSON-equivalent, including their opaque content.
// The aggregate retained output and each item are bounded to 8 MiB.
func (c *ResponsesOutput) Add(index int64, raw json.RawMessage) error {
	value, err := decodeResponsesOutput(raw)
	if err != nil {
		return err
	}
	if index < 0 {
		return &ResponsesOutputError{Code: "response_output_invalid"}
	}
	if previous, ok := c.items[index]; ok {
		old, _ := decodeResponsesOutput(previous)
		if !reflect.DeepEqual(old, value) {
			return responsesOutputConflict(index, old, value)
		}
		return nil
	}
	if len(raw) > responsesHistoryMaxBytes-c.bytes {
		return &ResponsesOutputError{Code: "response_output_limit_exceeded"}
	}
	if c.items == nil {
		c.items = make(map[int64]json.RawMessage)
	}
	c.items[index] = bytes.Clone(raw)
	c.bytes += len(raw)
	return nil
}

// Complete returns defensive copies in output order without mutating the
// collected events, including when validation fails. An omitted/empty snapshot
// uses completed events. A nonempty snapshot must cover every collected index.
// Its exact bytes are authoritative when items agree, or when matching reasoning
// items differ only in nonempty encrypted_content strings. This is opaque value
// reconciliation, not a claim that their encrypted plaintext is equivalent.
func (c *ResponsesOutput) Complete(snapshot []json.RawMessage) ([]json.RawMessage, error) {
	if len(snapshot) == 0 {
		if len(c.items) == 0 {
			return nil, &ResponsesOutputError{Code: "response_output_missing"}
		}
		out := make([]json.RawMessage, len(c.items))
		for index := range out {
			raw, ok := c.items[int64(index)]
			if !ok {
				return nil, &ResponsesOutputError{Code: "response_output_index_missing"}
			}
			out[index] = bytes.Clone(raw)
		}
		return out, nil
	}
	for index := range c.items {
		if index >= int64(len(snapshot)) {
			return nil, &ResponsesOutputError{Code: "response_snapshot_incomplete"}
		}
	}
	total := 0
	for _, raw := range snapshot {
		if len(raw) > responsesHistoryMaxBytes-total {
			return nil, &ResponsesOutputError{Code: "response_output_limit_exceeded"}
		}
		total += len(raw)
	}
	out := make([]json.RawMessage, len(snapshot))
	for index, raw := range snapshot {
		value, err := decodeResponsesOutput(raw)
		if err != nil {
			return nil, err
		}
		if previous, ok := c.items[int64(index)]; ok {
			old, _ := decodeResponsesOutput(previous)
			if !reflect.DeepEqual(old, value) && !compatibleResponsesReasoning(old, value) {
				return nil, responsesOutputConflict(int64(index), old, value)
			}
		}
		out[index] = bytes.Clone(raw)
	}
	return out, nil
}

func decodeResponsesOutput(raw json.RawMessage) (map[string]any, error) {
	if len(raw) > responsesHistoryMaxBytes {
		return nil, &ResponsesOutputError{Code: "response_output_limit_exceeded"}
	}
	var value map[string]any
	if json.Unmarshal(raw, &value) != nil || value == nil {
		return nil, &ResponsesOutputError{Code: "response_output_invalid"}
	}
	return value, nil
}

func compatibleResponsesReasoning(old, value map[string]any) bool {
	if old["type"] != "reasoning" || value["type"] != "reasoning" || len(old) != len(value) {
		return false
	}
	id, ok := old["id"].(string)
	if !ok || strings.TrimSpace(id) == "" || value["id"] != id {
		return false
	}
	before, beforeOK := old["encrypted_content"].(string)
	after, afterOK := value["encrypted_content"].(string)
	if !beforeOK || !afterOK || strings.TrimSpace(before) == "" || strings.TrimSpace(after) == "" {
		return false
	}
	for key, before := range old {
		if key == "encrypted_content" {
			continue
		}
		after, exists := value[key]
		if !exists || !reflect.DeepEqual(before, after) {
			return false
		}
	}
	return true
}

func responsesOutputConflict(index int64, old, value map[string]any) *ResponsesOutputError {
	fields := map[string]bool{}
	for key, before := range old {
		if after, ok := value[key]; !ok || !reflect.DeepEqual(before, after) {
			fields[responsesOutputField(key)] = true
		}
	}
	for key := range value {
		if _, ok := old[key]; !ok {
			fields[responsesOutputField(key)] = true
		}
	}
	names := make([]string, 0, len(fields))
	for key := range fields {
		names = append(names, key)
	}
	sort.Strings(names)
	return &ResponsesOutputError{Code: "response_output_conflict", OutputIndex: &index, Fields: names}
}

func responsesOutputField(field string) string {
	switch field {
	case "id", "type", "status", "role", "content", "summary", "encrypted_content", "call_id", "name", "namespace", "arguments", "action", "phase":
		return field
	default:
		return "other"
	}
}

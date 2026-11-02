package provider

import (
	"bytes"
	"encoding/json"
	"net/url"
	"reflect"
	"sort"
	"strings"
)

// ResponsesOutput collects native response.output_item.done items for one
// request. Its zero value is ready to use; it is not safe for concurrent use.
// Add validates duplicate events strictly. Complete reconciles only a confirmed
// terminal snapshot, preserving completed reasoning and hosted-search sources.
// The adapter remains responsible for terminal success, usage, namespace and
// tool validation.
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
	// Detail contains bounded structural facts, never provider field values.
	Detail *ResponsesOutputDiagnostic
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
			return responsesOutputConflict(index, old, value, "response.output_item.done")
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
// A completed reasoning item with nonempty encrypted_content keeps its original
// bytes. If that ciphertext is absent, null or empty, the terminal item can
// supply it. Matching identity and all non-opaque fields must agree; malformed
// ciphertext is never repaired. This selects an opaque source, not plaintext
// equivalence. Matching completed searches retain done bytes when only valid
// action.sources differ. Other fields stay strict. Terminal-only bytes are retained.
func (c *ResponsesOutput) Complete(snapshot []json.RawMessage) ([]json.RawMessage, error) {
	output, _, err := c.CompleteWithDiagnostics(snapshot)
	return output, err
}

// ResponsesOutputDiagnostic contains allowlisted structural facts only. Source
// counts are nil if absent or not arrays. Selected identifies accepted authority.
type ResponsesOutputDiagnostic struct {
	OutputIndex   int64
	ItemType      string
	PreviousEvent string
	CurrentEvent  string
	Paths         []string
	Difference    string
	SourcesBefore *int
	SourcesAfter  *int
	Selected      string
}

// CompleteWithDiagnostics applies Complete's rules and reports accepted search
// source variations. Diagnostics do not imply equivalent sources or retry safety.
func (c *ResponsesOutput) CompleteWithDiagnostics(snapshot []json.RawMessage) ([]json.RawMessage, []ResponsesOutputDiagnostic, error) {
	var details []ResponsesOutputDiagnostic
	output, err := c.complete(snapshot, &details)
	if err != nil {
		return nil, nil, err
	}
	return output, details, nil
}

func (c *ResponsesOutput) complete(snapshot []json.RawMessage, details *[]ResponsesOutputDiagnostic) ([]json.RawMessage, error) {
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
	selectedBytes := 0
	for index, raw := range snapshot {
		value, err := decodeResponsesOutput(raw)
		if err != nil {
			return nil, err
		}
		if previous, ok := c.items[int64(index)]; ok {
			old, _ := decodeResponsesOutput(previous)
			compatible, keepCompleted := compatibleResponsesReasoning(old, value)
			if reflect.DeepEqual(old, value) && old["type"] == "web_search_call" {
				keepCompleted = true
			}
			if !reflect.DeepEqual(old, value) {
				if compatibleResponsesSearch(old, value) {
					compatible, keepCompleted = true, true
					detail := responsesOutputDetail(int64(index), old, value, "terminal_snapshot")
					detail.Selected = "response.output_item.done"
					*details = append(*details, *detail)
				}
				if !compatible {
					return nil, responsesOutputConflict(int64(index), old, value, "terminal_snapshot")
				}
			}
			if keepCompleted {
				raw = previous
			}
		}
		if len(raw) > responsesHistoryMaxBytes-selectedBytes {
			return nil, &ResponsesOutputError{Code: "response_output_limit_exceeded"}
		}
		selectedBytes += len(raw)
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

func compatibleResponsesReasoning(old, value map[string]any) (compatible, keepCompleted bool) {
	if old["type"] != "reasoning" || value["type"] != "reasoning" {
		return false, false
	}
	id, ok := old["id"].(string)
	if !ok || strings.TrimSpace(id) == "" || value["id"] != id {
		return false, false
	}
	before, beforeOK := responsesReasoningCipher(old["encrypted_content"])
	after, afterOK := responsesReasoningCipher(value["encrypted_content"])
	if !beforeOK || !afterOK || (before == "" && after == "") {
		return false, false
	}
	for key, before := range old {
		if key == "encrypted_content" {
			continue
		}
		after, exists := value[key]
		if !exists || !reflect.DeepEqual(before, after) {
			return false, false
		}
	}
	for key := range value {
		if key == "encrypted_content" {
			continue
		}
		if _, exists := old[key]; !exists {
			return false, false
		}
	}
	return true, before != ""
}

func responsesReasoningCipher(value any) (string, bool) {
	if value == nil {
		return "", true
	}
	text, ok := value.(string)
	if !ok || (text != "" && strings.TrimSpace(text) == "") {
		return "", false
	}
	return text, true
}

func responsesOutputConflict(index int64, old, value map[string]any, event string) *ResponsesOutputError {
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
	return &ResponsesOutputError{Code: "response_output_conflict", OutputIndex: &index, Fields: names, Detail: responsesOutputDetail(index, old, value, event)}
}

func responsesOutputField(field string) string {
	switch field {
	case "id", "type", "status", "role", "content", "summary", "encrypted_content", "call_id", "name", "namespace", "arguments", "action", "phase":
		return field
	default:
		return "other"
	}
}

// A hosted search has executed at done. Its terminal source list is observed
// result metadata, not a replacement operation or a second tool result.
func compatibleResponsesSearch(old, value map[string]any) bool {
	if old["type"] != "web_search_call" || value["type"] != "web_search_call" || old["status"] != "completed" || value["status"] != "completed" {
		return false
	}
	id, ok := old["id"].(string)
	if !ok || strings.TrimSpace(id) == "" || value["id"] != id {
		return false
	}
	before, ok := old["action"].(map[string]any)
	if !ok {
		return false
	}
	after, ok := value["action"].(map[string]any)
	if !ok {
		return false
	}
	if before["type"] != "search" || after["type"] != "search" {
		return false
	}
	if query, exists := before["query"]; exists {
		if _, ok := query.(string); !ok {
			return false
		}
	}
	if queries, exists := before["queries"]; exists {
		list, ok := queries.([]any)
		if !ok {
			return false
		}
		for _, query := range list {
			if _, ok := query.(string); !ok {
				return false
			}
		}
	}
	if !equalResponsesExcept(old, value, "action") || !equalResponsesExcept(before, after, "sources") {
		return false
	}
	return validResponsesSources(before) && validResponsesSources(after)
}

func equalResponsesExcept(a, b map[string]any, except string) bool {
	for key, value := range a {
		if key != except {
			other, exists := b[key]
			if !exists || !reflect.DeepEqual(value, other) {
				return false
			}
		}
	}
	for key := range b {
		if key != except {
			if _, exists := a[key]; !exists {
				return false
			}
		}
	}
	return true
}

// Validate the compatibility exception only; equal native records retain their
// existing admission contract. Absent/empty sources are valid; null is not an array.
func validResponsesSources(action map[string]any) bool {
	raw, exists := action["sources"]
	if !exists {
		return true
	}
	sources, ok := raw.([]any)
	if !ok {
		return false
	}
	for _, raw := range sources {
		source, ok := raw.(map[string]any)
		if !ok {
			return false
		}
		if source["type"] != "url" {
			return false
		}
		address, ok := source["url"].(string)
		if !ok || strings.TrimSpace(address) != address || address == "" {
			return false
		}
		parsed, err := url.Parse(address)
		if err != nil || parsed.Scheme == "" {
			return false
		}
		for key, value := range source {
			switch key {
			case "type", "url":
			case "title":
				if _, ok := value.(string); !ok {
					return false
				}
			default:
				return false
			}
		}
	}
	return true
}

func responsesOutputDetail(index int64, old, value map[string]any, event string) *ResponsesOutputDiagnostic {
	d := &ResponsesOutputDiagnostic{OutputIndex: index, PreviousEvent: "response.output_item.done", CurrentEvent: event, Difference: "value_changed"}
	switch old["type"] {
	case "reasoning", "message", "web_search_call", "function_call":
		d.ItemType, _ = old["type"].(string)
	default:
		d.ItemType = "other"
	}
	paths := map[string]bool{}
	for key, before := range old {
		if after, exists := value[key]; !exists || !reflect.DeepEqual(before, after) {
			paths[responsesOutputField(key)] = true
		}
	}
	for key := range value {
		if _, exists := old[key]; !exists {
			paths[responsesOutputField(key)] = true
		}
	}
	if paths["action"] {
		before, aOK := old["action"].(map[string]any)
		after, bOK := value["action"].(map[string]any)
		if aOK && bOK {
			delete(paths, "action")
			add := func(key string) {
				switch key {
				case "type", "query", "queries", "url", "pattern", "sources":
					paths["action."+key] = true
				default:
					paths["action.other"] = true
				}
			}
			for key, a := range before {
				if b, exists := after[key]; !exists || !reflect.DeepEqual(a, b) {
					add(key)
				}
			}
			for key := range after {
				if _, exists := before[key]; !exists {
					add(key)
				}
			}
			a, aArray := before["sources"].([]any)
			b, bArray := after["sources"].([]any)
			if aArray {
				count := len(a)
				d.SourcesBefore = &count
			}
			if bArray {
				count := len(b)
				d.SourcesAfter = &count
			}
			if len(paths) == 1 && paths["action.sources"] {
				d.Difference = "sources_changed"
				if !validResponsesSources(before) || !validResponsesSources(after) {
					d.Difference = "sources_invalid"
				} else if aArray && bArray && len(b) > len(a) && reflect.DeepEqual(a, b[:len(a)]) {
					d.Difference = "sources_appended"
				}
			}
		}
	}
	for path := range paths {
		d.Paths = append(d.Paths, path)
	}
	sort.Strings(d.Paths)
	return d
}

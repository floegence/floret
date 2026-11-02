package provider

import (
	"bytes"
	"encoding/json"
	"sort"
	"strings"
)

// ResponsesHistoryError describes a rejected native history binding without
// conversation text or opaque provider values. Code is one of the reasons
// documented by Error. RecordIndex and CanonicalIndex are zero-based when known;
// ExpectedItems counts the saved visible projection and AvailableItems counts
// canonical items remaining at that boundary. Fields contains known JSON names
// only. Callers may use errors.As; this error does not authorize retry or repair.
type ResponsesHistoryError struct {
	Code           string
	RecordIndex    *int
	CanonicalIndex *int
	ExpectedItems  *int
	AvailableItems *int
	Fields         []string
}

func (e *ResponsesHistoryError) Error() string {
	switch e.Code {
	case "missing_identity":
		return "missing Responses history identity"
	case "incompatible_identity":
		return "incompatible Responses history identity"
	case "unsupported_policy":
		return "unsupported Responses history reuse policy"
	case "unsupported_attribute":
		return "unsupported Responses history attribute"
	case "state_limit":
		return "Responses history exceeds state limit"
	case "invalid_records":
		return "invalid Responses history records"
	case "trailing_data":
		return "trailing Responses history data"
	case "invalid_response_id":
		return "invalid Responses response identity"
	case "invalid_record":
		return "invalid Responses history record"
	case "invalid_boundary":
		return "invalid Responses history boundary"
	case "visible_mismatch":
		return "Responses native output does not match canonical history"
	case "missing_output":
		return "missing native Responses output"
	case "invalid_item":
		return "invalid Responses item"
	case "invalid_output":
		return "invalid native Responses output"
	case "unexpected_namespace":
		return "unexpected Responses function namespace"
	case "unsupported_output":
		return "unsupported native Responses output"
	default:
		return "invalid Responses history"
	}
}

func matchResponsesVisible(canonical, visible, output []json.RawMessage) ([]json.RawMessage, bool) {
	if len(canonical) >= len(visible) && responsesFingerprint(canonical[:len(visible)]) == responsesFingerprint(visible) {
		return cloneResponsesItems(canonical[:len(visible)]), true
	}
	// v7 Redeven Responses writers saved commentary/search/answer as separate
	// plain assistant messages while the runtime projects one assistant message.
	// Keep this published binding readable throughout v7; remove only with a
	// future-major migration of every affected native-output record.
	count := 0
	texts := []string{}
	var shape map[string]json.RawMessage
	for _, raw := range visible {
		text, fields, ok := responsesPlainAssistant(raw)
		if !ok {
			break
		}
		if shape != nil && !equalResponsesObject(shape, fields) {
			return nil, false
		}
		shape = fields
		texts = append(texts, text)
		count++
	}
	if count < 2 || len(canonical) < 1+len(visible)-count {
		return nil, false
	}
	actual, actualShape, ok := responsesPlainAssistant(canonical[0])
	if !ok || !equalResponsesObject(shape, actualShape) {
		return nil, false
	}
	expected := strings.Join(texts, "")
	// Earlier adapters trimmed each fragment. Prefer native bytes when they
	// prove the exact original spacing; deleting those spaces is real drift.
	fragments, whole, nativeMatches := responsesNativeAssistantText(output)
	nativeMatches = nativeMatches && len(fragments) == count
	if nativeMatches {
		for i := range texts {
			if texts[i] != fragments[i] {
				nativeMatches = false
				break
			}
		}
	}
	if nativeMatches {
		expected = whole
	}
	if actual != expected {
		return nil, false
	}
	end := 1 + len(visible) - count
	if responsesFingerprint(canonical[1:end]) != responsesFingerprint(visible[count:]) {
		return nil, false
	}
	return cloneResponsesItems(canonical[:end]), true
}

// Only the published plain text projection can regroup. IDs, phases, content
// arrays, annotations, unknown fields and different role/type shapes stay exact.
func responsesPlainAssistant(raw json.RawMessage) (string, map[string]json.RawMessage, bool) {
	var value map[string]json.RawMessage
	if json.Unmarshal(raw, &value) != nil {
		return "", nil, false
	}
	if string(value["role"]) != "\"assistant\"" {
		return "", nil, false
	}
	if typ, ok := value["type"]; ok && string(typ) != "\"message\"" {
		return "", nil, false
	}
	for key := range value {
		if key != "role" && key != "type" && key != "content" {
			return "", nil, false
		}
	}
	var text string
	if len(value["content"]) == 0 || string(value["content"]) == "null" || json.Unmarshal(value["content"], &text) != nil || text == "" {
		return "", nil, false
	}
	delete(value, "content")
	return text, value, true
}

func equalResponsesObject(a, b map[string]json.RawMessage) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return bytes.Equal(x, y)
}

func responsesNativeAssistantText(output []json.RawMessage) ([]string, string, bool) {
	var fragments []string
	var text, all strings.Builder
	flush := func() {
		if value := strings.TrimSpace(text.String()); value != "" {
			fragments = append(fragments, value)
		}
		text.Reset()
	}
	for _, raw := range output {
		var item struct {
			Type    string
			Role    string
			Content []struct {
				Type string
				Text string
			}
		}
		if json.Unmarshal(raw, &item) != nil {
			return nil, "", false
		}
		switch item.Type {
		case "web_search_call":
			flush()
		case "message":
			if item.Role != "assistant" {
				return nil, "", false
			}
			for _, part := range item.Content {
				if part.Type != "output_text" {
					return nil, "", false
				}
				text.WriteString(part.Text)
				all.WriteString(part.Text)
			}
		case "reasoning", "function_call":
		default:
			return nil, "", false
		}
	}
	flush()
	return fragments, strings.TrimSpace(all.String()), true
}

func responsesVisibleConflictFields(expected, actual []json.RawMessage) []string {
	fields := map[string]bool{}
	if len(actual) < len(expected) {
		fields["items"] = true
	}
	for i := 0; i < len(expected) && i < len(actual); i++ {
		var a, b map[string]json.RawMessage
		_ = json.Unmarshal(expected[i], &a)
		_ = json.Unmarshal(actual[i], &b)
		for key, value := range a {
			if !bytes.Equal(value, b[key]) {
				fields[responsesHistoryField(key)] = true
			}
		}
		for key := range b {
			if _, ok := a[key]; !ok {
				fields[responsesHistoryField(key)] = true
			}
		}
		if len(fields) > 0 {
			break
		}
	}
	out := make([]string, 0, len(fields))
	for field := range fields {
		out = append(out, field)
	}
	sort.Strings(out)
	return out
}
func responsesHistoryField(field string) string {
	switch field {
	case "role", "type", "content", "call_id", "name", "namespace", "arguments":
		return field
	default:
		return "other"
	}
}

// CommitAssistant binds the complete emitted assistant text and ordered local
// function calls using the runtime's canonical assistant projection. Text is
// concatenated in emission order by the adapter, including commentary around
// hosted tools; presentation fragments are not separate model messages. Calls
// must already have their canonical wire names, arguments and namespace. Native
// output, including message phases, searches and encrypted reasoning, stays
// byte-for-byte unchanged. This is the binding path for runtime Responses
// adapters; Commit remains available for other exact visible projections.
func (h *ResponsesHistory) CommitAssistant(responseID, text string, calls, output []json.RawMessage) (*State, error) {
	visible := make([]json.RawMessage, 0, len(calls)+1)
	if text = strings.TrimSpace(text); text != "" {
		raw, _ := json.Marshal(map[string]string{"role": "assistant", "content": text})
		visible = append(visible, raw)
	}
	for _, raw := range calls {
		var call struct {
			Type      string
			Namespace string
		}
		if json.Unmarshal(raw, &call) != nil || call.Type != "function_call" {
			return nil, &ResponsesHistoryError{Code: "invalid_item"}
		}
		if call.Namespace != h.options.FunctionNamespace {
			return nil, &ResponsesHistoryError{Code: "unexpected_namespace"}
		}
		visible = append(visible, raw)
	}
	return h.Commit(responseID, visible, output)
}

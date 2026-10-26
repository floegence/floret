package activityfields

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
)

// MutationSummaryBytes bounds canonical reads independently of diff size.
const MutationSummaryBytes = 16 << 10

// MutationSummaryValues reads only complete title strings from the beginning of
// an already validated single-mutation array. The supplied prefix may end inside
// a deferred field. Missing values never assert canonical absence.
func MutationSummaryValues(prefix []byte) map[string]json.RawMessage {
	out := map[string]json.RawMessage{}
	decoder := json.NewDecoder(bytes.NewReader(prefix[:min(len(prefix), MutationSummaryBytes)]))
	if token, err := decoder.Token(); err != nil || token != json.Delim('[') {
		return out
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		return out
	}
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			break
		}
		key, ok := token.(string)
		if !ok {
			break
		}
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			break
		}
		if key != "display_name" && key != "change_type" {
			continue
		}
		var value string
		if json.Unmarshal(raw, &value) == nil && len(value) <= 2048 {
			out["/payload/mutations/0/"+key], _ = json.Marshal(strings.TrimSpace(value))
		}
		if len(out) == 2 {
			break
		}
	}
	return out
}

// SummaryText identifies small, product-neutral activity attributes. Collection
// members, command bodies, result bodies and errors retain their field sources.
func SummaryText(path string) bool {
	switch path {
	case "/label", "/description", "/payload/status", "/payload/operation", "/payload/display_name",
		"/payload/action", "/payload/change_type", "/payload/url", "/payload/final_url", "/payload/query", "/payload/pattern",
		"/payload/process_id", "/payload/execution_location", "/payload/content_type", "/payload/format":
		return true
	}
	return false
}

// SummaryValues returns only explicitly selected attributes. Oversized strings
// remain field sources; absence of a value never asserts canonical absence.
func SummaryValues(root any) map[string]json.RawMessage {
	out := map[string]json.RawMessage{}
	if mutations, err := resolve(root, "/payload/mutations"); err == nil {
		mutations = indirect(mutations)
		if (mutations.Kind() == reflect.Slice || mutations.Kind() == reflect.Array) && mutations.Len() == 1 {
			for _, key := range []string{"display_name", "change_type"} {
				path := "/payload/mutations/0/" + key
				if value, err := resolve(root, path); err == nil {
					value = indirect(value)
					if value.Kind() == reflect.String && len(value.String()) <= 2048 {
						out[path], _ = json.Marshal(strings.TrimSpace(value.String()))
					}
				}
			}
		}
	}
	for _, path := range []string{"/chips", "/target_refs"} {
		value, err := resolve(root, path)
		if err != nil || !value.IsValid() {
			continue
		}
		raw, err := json.Marshal(value.Interface())
		if err == nil && len(raw) <= 16<<10 {
			out[path] = raw
		}
	}
	for _, parent := range []string{"", "/payload"} {
		value, err := resolve(root, parent)
		if err != nil {
			continue
		}
		for _, field := range children(value) {
			path := parent + "/" + escape(field.name)
			if !SummaryText(path) || path == "/label" || path == "/description" {
				continue
			}
			v := indirect(field.value)
			if v.Kind() != reflect.String || len(v.String()) > 2048 {
				continue
			}
			out[path], _ = json.Marshal(strings.TrimSpace(v.String()))
		}
	}
	return out
}

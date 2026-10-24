package activityfields

import (
	"encoding/json"
	"reflect"
	"strings"
)

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

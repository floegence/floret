package requestmeasure

import (
	"encoding/json"
	"testing"
)

func TestFrozenPrefixIncludesSurfaceAndOrderedContent(t *testing.T) {
	payload := []byte(`{"model":"m","instructions":"system","tools":[{"name":"read"}],"input":[{"role":"user","content":"hello"}],"max_output_tokens":100}`)
	prefix, err := RenderedPrefix(payload, "openai_responses", "account:connection", "input")
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"tail", "output_refit", "same", "surface", "edited", "truncated", "identity", "format"} {
		t.Run(kind, func(t *testing.T) {
			var body map[string]any
			json.Unmarshal(payload, &body)
			identity, format := "account:connection", "openai_responses"
			matched := true
			switch kind {
			case "tail":
				body["input"] = append(body["input"].([]any), map[string]any{"type": "function_call_output", "output": "result"})
			case "output_refit":
				body["max_output_tokens"] = 1
			case "surface":
				body["instructions"] = "changed"
				matched = false
			case "edited":
				body["input"].([]any)[0].(map[string]any)["content"] = "changed"
				matched = false
			case "truncated":
				body["input"] = []any{}
				matched = false
			case "identity":
				identity = "new connection"
				matched = false
			case "format":
				format = "openai_chat"
				matched = false
			}
			raw, _ := json.Marshal(body)
			suffix, ok := RenderedSuffix(raw, format, identity, prefix)
			if ok != matched {
				t.Fatalf("matched=%v want=%v", ok, matched)
			}
			if ok && kind != "tail" {
				if len(suffix) != 0 {
					t.Fatalf("empty suffix=%s", suffix)
				}
				return
			}
			if ok {
				var tail map[string][]any
				if json.Unmarshal(suffix, &tail) != nil || len(tail) != 1 || len(tail["input"]) != map[bool]int{true: 1, false: 0}[kind == "tail"] {
					t.Fatalf("suffix=%s", suffix)
				}
			}
		})
	}
}

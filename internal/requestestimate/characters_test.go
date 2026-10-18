package requestestimate

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestPreparedCharactersUnicodeAndBudgetIndependence(t *testing.T) {
	body := map[string]any{"instructions": " 中😀é\n", "input": []any{map[string]any{"role": "user", "content": "a\nb"}, map[string]any{"type": "function_call_output", "output": strings.Repeat("中😀", 40000)}, map[string]any{"type": "reasoning", "encrypted_content": "abc", "summary": []any{map[string]any{"type": "summary_text", "text": "想"}}}}, "max_output_tokens": 1}
	payload, _ := json.Marshal(body)
	first, err := Count("openai", "unknown-model", "openai_responses", payload)
	if err != nil {
		t.Fatal(err)
	}
	if first.Characters == nil || first.Characters.TotalCharacters != 80010 {
		t.Fatalf("unexpected code point count: %+v", first.Characters)
	}
	if err := first.Characters.Validate(); err != nil {
		t.Fatal(err)
	}
	body["max_output_tokens"] = 10000
	body["input"].([]any)[2].(map[string]any)["encrypted_content"] = strings.Repeat("x", 10000)
	payload, _ = json.Marshal(body)
	second, err := Count("openai", "gpt-4o", "openai_responses", payload)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first.Characters, second.Characters) {
		t.Fatalf("budget or opaque data changed characters: %+v / %+v", first.Characters, second.Characters)
	}
}

func TestPreparedCharactersAttributionAndDeterministicSchema(t *testing.T) {
	payload := []byte(`{"instructions":"A中😀","input":[],"tools":[{"type":"function","name":"read","description":"中😀","parameters":{"type":"object"}}]}`)
	got, err := CountAttributed("openai", "gpt-4o", "openai_responses", payload, []Attribution{{Path: "/instructions", Category: "system", Parts: []Part{{Start: 0, End: 1}, {Start: 1, End: 8, Category: "model_context", Key: "facts"}}}, {Path: "/tools/0", Category: "tool_definition", Key: "read", Source: "local"}})
	if err != nil {
		t.Fatal(err)
	}
	// Deterministic decoded schema JSON has 86 code points; text has three.
	const schema = `{"description":"中😀","name":"read","parameters":{"type":"object"},"type":"function"}`
	want := int64(len([]rune(schema)) + 3)
	if got.Characters == nil || got.Characters.TotalCharacters != want {
		t.Fatalf("characters: %+v; want %d", got.Characters, want)
	}
	if err := got.Characters.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, c := range got.Characters.Categories {
		if c.Category == "model_context" && (c.Characters != 2 || len(c.Details) != 1 || c.Details[0].Key != "facts") {
			t.Fatalf("span attribution: %+v", c)
		}
	}
}

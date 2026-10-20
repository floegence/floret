package provider

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
)

func TestOrderToolSchemas(t *testing.T) {
	schema := `{"type":"object","properties":{"content":{"type":"string"},"file_path":{"type":"string"},"options":{"properties":{"z":{},"a":{}},"type":"object"}},"required":["file_path","content"]}`
	for _, tc := range []struct {
		format RequestFormat
		tool   string
	}{
		{RequestFormatOpenAIResponses, `{"type":"function","name":"write","parameters":` + schema + `}`},
		{RequestFormatOpenAIResponses, `{"type":"namespace","name":"files","tools":[{"type":"function","name":"write","parameters":` + schema + `}]}`},
		{RequestFormatOpenAIChat, `{"type":"function","function":{"name":"write","parameters":` + schema + `}}`},
		{RequestFormatAnthropic, `{"name":"write","input_schema":` + schema + `}`},
		{RequestFormatJSON, `{"name":"write","input_schema":` + schema + `}`},
	} {
		t.Run(string(tc.format)+tc.tool[:15], func(t *testing.T) {
			input := []byte(`{"tools":[` + tc.tool + `],"input":[{"arguments":"unchanged"}],"metadata":{"properties":{"z":1,"a":2}}}`)
			got, err := OrderToolSchemas(tc.format, input)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Contains(got, []byte(`"properties":{"file_path":`)) {
				t.Fatalf("wrong root order: %s", got)
			}
			if !bytes.Contains(got, []byte(`"properties":{"z":{},"a":{}}`)) {
				t.Fatalf("nested schema changed: %s", got)
			}
			var before, after any
			json.Unmarshal(input, &before)
			json.Unmarshal(got, &after)
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("changed semantics: %s", got)
			}
			again, err := OrderToolSchemas(tc.format, got)
			if err != nil || !bytes.Equal(got, again) {
				t.Fatalf("not idempotent: %s %v", again, err)
			}
		})
	}
}

func TestOrderToolSchemasHostedMalformedAndDeterministic(t *testing.T) {
	for _, format := range []RequestFormat{RequestFormatOpenAIChat, RequestFormatOpenAIResponses, RequestFormatAnthropic, RequestFormatJSON} {
		raw := []byte(`{"tools":[{"type":"web_search","input_schema":{"properties":{"z":{},"a":{}}},"parameters":{"properties":{"z":{},"a":{}}}}]}`)
		got, err := OrderToolSchemas(format, raw)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(got, []byte(`"properties":{"z":{},"a":{}}`)) {
			t.Fatalf("hosted tool changed: %s", got)
		}
	}
	for _, raw := range []string{`{`, `null`, `[]`, `{"tools":{}}`, `{"tools":[{"type":"function","parameters":{"properties":true}}]}`} {
		if _, err := OrderToolSchemas(RequestFormatOpenAIResponses, []byte(raw)); err == nil {
			t.Fatalf("accepted malformed payload %s", raw)
		}
	}
	if _, err := OrderToolSchemas("unknown", []byte(`{}`)); err == nil {
		t.Fatal("accepted unsupported format")
	}
	var first []byte
	for i := 0; i < 100; i++ {
		properties := map[string]any{}
		names := []string{"content", "file_path", "z", "a"}
		for j := range names {
			properties[names[(j+i)%len(names)]] = map[string]any{"type": []string{"string", "null"}}
		}
		schema := map[string]any{"properties": properties, "required": []string{"file_path", "content"}}
		raw, err := json.Marshal(map[string]any{"tools": []any{map[string]any{"type": "function", "parameters": schema}}, "input": []any{map[string]any{"arguments": `{"content":"first","file_path":"last"}`}}})
		if err != nil {
			t.Fatal(err)
		}
		got, err := OrderToolSchemas(RequestFormatOpenAIResponses, raw)
		if err != nil {
			t.Fatal(err)
		}
		if first == nil {
			first = got
		} else if !bytes.Equal(first, got) {
			t.Fatalf("unstable request: %s", got)
		}
		var envelope struct{ Input []struct{ Arguments string } }
		if err := json.Unmarshal(got, &envelope); err != nil {
			t.Fatal(err)
		}
		if envelope.Input[0].Arguments != `{"content":"first","file_path":"last"}` {
			t.Fatal("rewrote historical argument string")
		}
	}
}

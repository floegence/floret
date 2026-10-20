package tools

import (
	"errors"
	"reflect"
	"testing"
)

func TestInputPropertyOrder(t *testing.T) {
	for _, required := range []any{[]string{"file_path", "content"}, []any{"file_path", "content"}} {
		schema := map[string]any{"properties": map[string]any{"content": String("body"), "file_path": String("path"), "z": Boolean(""), "a": Boolean("")}, "required": required}
		got, err := InputPropertyOrder(schema)
		if err != nil || !reflect.DeepEqual(got, []string{"file_path", "content", "a", "z"}) {
			t.Fatalf("order=%v, err=%v", got, err)
		}
		if !reflect.DeepEqual(schema["required"], required) {
			t.Fatal("mutated schema")
		}
	}
	for _, required := range []any{nil, []any{}, []string{}} {
		got, err := InputPropertyOrder(map[string]any{"properties": map[string]any{"z": nil, "a": nil}, "required": required})
		if err != nil || !reflect.DeepEqual(got, []string{"a", "z"}) {
			t.Fatalf("empty required: %v %v", got, err)
		}
	}
	for _, schema := range []map[string]any{{"properties": true}, {"required": "path"}, {"required": []any{1}}, {"required": []string{"a", "a"}}} {
		if _, err := InputPropertyOrder(schema); !errors.Is(err, ErrSchema) {
			t.Fatalf("schema=%v err=%v", schema, err)
		}
	}
	got, err := InputPropertyOrder(map[string]any{"properties": map[string]any{"a": nil}, "required": []string{"other", "a"}})
	if err != nil || !reflect.DeepEqual(got, []string{"a"}) {
		t.Fatalf("undeclared required: %v %v", got, err)
	}
}

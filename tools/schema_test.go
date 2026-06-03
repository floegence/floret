package tools

import (
	"errors"
	"strings"
	"testing"
)

func TestStrictObjectRequiresNullableFieldsAndForbidsExtraFields(t *testing.T) {
	schema := StrictObject(map[string]any{
		"path":  String("path"),
		"limit": Nullable(Integer("limit")),
	}, nil)

	if _, err := Validate(schema, []byte(`{"path":"README.md","limit":null}`)); err != nil {
		t.Fatalf("nullable required field should validate: %v", err)
	}
	if _, err := Validate(schema, []byte(`{"path":"README.md"}`)); err == nil || !strings.Contains(err.Error(), "$.limit is required") {
		t.Fatalf("missing nullable field err = %v", err)
	}
	if _, err := Validate(schema, []byte(`{"path":"README.md","limit":1,"extra":true}`)); err == nil || !strings.Contains(err.Error(), "$.extra is not allowed") {
		t.Fatalf("extra field err = %v", err)
	}
}

func TestValidateRejectsInvalidJSONTrailingTokensWrongTypeMissingRequired(t *testing.T) {
	schema := StrictObject(map[string]any{
		"name": String("name"),
		"age":  Integer("age"),
	}, []string{"age", "name"})

	cases := []struct {
		name string
		raw  string
		want string
	}{
		{name: "invalid json", raw: `{"name":`, want: "invalid JSON"},
		{name: "trailing token", raw: `{"name":"Ada","age":1} {"name":"Grace","age":2}`, want: "expected exactly one JSON value"},
		{name: "wrong root", raw: `[]`, want: "$ must be an object"},
		{name: "wrong type", raw: `{"name":"Ada","age":"1"}`, want: "$.age must be an integer"},
		{name: "missing required", raw: `{"name":"Ada"}`, want: "$.age is required"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Validate(schema, []byte(tc.raw))
			if err == nil || !errors.Is(err, ErrSchema) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestNormalizeInputSchemaRejectsNonObjectAndOpenObject(t *testing.T) {
	if _, err := NormalizeInputSchema(map[string]any{"type": "string"}); err == nil || !strings.Contains(err.Error(), "must be an object") {
		t.Fatalf("non-object schema err = %v", err)
	}
	if _, err := NormalizeInputSchema(map[string]any{"type": "object", "additionalProperties": true}); err == nil || !strings.Contains(err.Error(), "additionalProperties=false") {
		t.Fatalf("open object schema err = %v", err)
	}
	if got, err := NormalizeInputSchema(map[string]any{"type": "object", "additionalProperties": false}); err != nil || got["properties"] == nil {
		t.Fatalf("closed object without properties should normalize: got=%#v err=%v", got, err)
	}
}

func TestEnumArrayNestedObjectValidation(t *testing.T) {
	schema := StrictObject(map[string]any{
		"mode": Enum("fast", "safe"),
		"items": Array(StrictObject(map[string]any{
			"id":     Integer("id"),
			"labels": Array(String("label"), "labels"),
		}, []string{"id", "labels"}), "items"),
	}, []string{"items", "mode"})

	if _, err := Validate(schema, []byte(`{"mode":"safe","items":[{"id":1,"labels":["a","b"]}]}`)); err != nil {
		t.Fatalf("valid nested schema err = %v", err)
	}
	if _, err := Validate(schema, []byte(`{"mode":"slow","items":[]}`)); err == nil || !strings.Contains(err.Error(), "$.mode must be one of") {
		t.Fatalf("enum err = %v", err)
	}
	if _, err := Validate(schema, []byte(`{"mode":"safe","items":[{"id":1,"labels":[2]}]}`)); err == nil || !strings.Contains(err.Error(), "$.items[0].labels[0] must be a string") {
		t.Fatalf("nested array err = %v", err)
	}
}

package tools

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strings"
)

var ErrSchema = errors.New("schema validation failed")

func StrictObject(properties map[string]any, required []string) map[string]any {
	if properties == nil {
		properties = map[string]any{}
	}
	if required == nil {
		required = make([]string, 0, len(properties))
		for name := range properties {
			required = append(required, name)
		}
	}
	required = append([]string{}, required...)
	slices.Sort(required)
	return map[string]any{
		"type":                 "object",
		"properties":           properties,
		"required":             required,
		"additionalProperties": false,
	}
}

func String(description string) map[string]any {
	return scalar("string", description)
}

func Boolean(description string) map[string]any {
	return scalar("boolean", description)
}

func Integer(description string) map[string]any {
	return scalar("integer", description)
}

func Number(description string) map[string]any {
	return scalar("number", description)
}

func Array(items map[string]any, description string) map[string]any {
	s := map[string]any{"type": "array", "items": items}
	if description != "" {
		s["description"] = description
	}
	return s
}

func Enum(values ...string) map[string]any {
	enum := make([]any, 0, len(values))
	for _, value := range values {
		enum = append(enum, value)
	}
	return map[string]any{"type": "string", "enum": enum}
}

func Nullable(schema map[string]any) map[string]any {
	out := cloneSchema(schema)
	switch typ := out["type"].(type) {
	case string:
		out["type"] = []any{typ, "null"}
	case []any:
		if !slices.ContainsFunc(typ, func(v any) bool { return v == "null" }) {
			out["type"] = append(append([]any(nil), typ...), "null")
		}
	default:
		out["type"] = []any{"null"}
	}
	return out
}

func Validate(schema map[string]any, raw []byte) (map[string]any, error) {
	var value any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&value); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %v", ErrSchema, err)
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return nil, fmt.Errorf("%w: expected exactly one JSON value", ErrSchema)
	}
	if err := validateValue("$", schema, value); err != nil {
		return nil, err
	}
	obj, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: $ must be an object", ErrSchema)
	}
	return obj, nil
}

func ValidateStructured(schema map[string]any, value any) error {
	if schema == nil {
		return nil
	}
	return validateValue("$", schema, value)
}

func NormalizeInputSchema(schema map[string]any) (map[string]any, error) {
	if schema == nil {
		schema = StrictObject(nil, nil)
	}
	typ, _ := schema["type"].(string)
	if typ != "object" {
		return nil, fmt.Errorf("%w: input schema must be an object", ErrSchema)
	}
	if schema["additionalProperties"] != false {
		return nil, fmt.Errorf("%w: input schema must set additionalProperties=false", ErrSchema)
	}
	if _, ok := schema["properties"].(map[string]any); !ok {
		schema = cloneSchema(schema)
		schema["properties"] = map[string]any{}
	}
	return cloneSchema(schema), nil
}

func InvalidArgumentsText(name string, err error) string {
	reason := strings.TrimPrefix(err.Error(), ErrSchema.Error()+": ")
	return fmt.Sprintf("Tool %s was called with invalid arguments: %s. Rewrite the input to match the schema.", name, reason)
}

func scalar(kind, description string) map[string]any {
	s := map[string]any{"type": kind}
	if description != "" {
		s["description"] = description
	}
	return s
}

func cloneSchema(schema map[string]any) map[string]any {
	out := make(map[string]any, len(schema))
	for k, v := range schema {
		out[k] = cloneAny(v)
	}
	return out
}

func cloneAny(value any) any {
	switch v := value.(type) {
	case map[string]any:
		return cloneSchema(v)
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = cloneAny(item)
		}
		return out
	case []string:
		out := []any{}
		if len(v) > 0 {
			out = make([]any, len(v))
		}
		for i, item := range v {
			out[i] = item
		}
		return out
	default:
		return value
	}
}

func validateValue(path string, schema map[string]any, value any) error {
	if allowsNull(schema) && value == nil {
		return nil
	}
	kind := primaryType(schema)
	switch kind {
	case "object":
		obj, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("%w: %s must be an object", ErrSchema, path)
		}
		props, _ := schema["properties"].(map[string]any)
		for _, name := range requiredFields(schema) {
			if _, ok := obj[name]; !ok {
				return fmt.Errorf("%w: %s.%s is required", ErrSchema, path, name)
			}
		}
		if schema["additionalProperties"] == false {
			for name := range obj {
				if _, ok := props[name]; !ok {
					return fmt.Errorf("%w: %s.%s is not allowed", ErrSchema, path, name)
				}
			}
		}
		for name, prop := range props {
			if child, ok := obj[name]; ok {
				childSchema, ok := prop.(map[string]any)
				if !ok {
					return fmt.Errorf("%w: %s.%s has invalid schema", ErrSchema, path, name)
				}
				if err := validateValue(path+"."+name, childSchema, child); err != nil {
					return err
				}
			}
		}
	case "string":
		if _, ok := value.(string); !ok {
			return fmt.Errorf("%w: %s must be a string", ErrSchema, path)
		}
	case "boolean":
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("%w: %s must be a boolean", ErrSchema, path)
		}
	case "integer":
		if !isInteger(value) {
			return fmt.Errorf("%w: %s must be an integer", ErrSchema, path)
		}
		if err := validateNumericRange(path, schema, numericValue(value)); err != nil {
			return err
		}
	case "number":
		if !isNumber(value) {
			return fmt.Errorf("%w: %s must be a number", ErrSchema, path)
		}
		if err := validateNumericRange(path, schema, numericValue(value)); err != nil {
			return err
		}
	case "array":
		items, ok := value.([]any)
		if !ok {
			return fmt.Errorf("%w: %s must be an array", ErrSchema, path)
		}
		itemSchema, _ := schema["items"].(map[string]any)
		if itemSchema != nil {
			for i, item := range items {
				if err := validateValue(fmt.Sprintf("%s[%d]", path, i), itemSchema, item); err != nil {
					return err
				}
			}
		}
	default:
		return fmt.Errorf("%w: %s has unsupported schema type %q", ErrSchema, path, kind)
	}
	if enum, ok := schema["enum"].([]any); ok && len(enum) > 0 && !enumContains(enum, value) {
		return fmt.Errorf("%w: %s must be one of %s", ErrSchema, path, enumList(enum))
	}
	return nil
}

func primaryType(schema map[string]any) string {
	switch typ := schema["type"].(type) {
	case string:
		return typ
	case []any:
		for _, item := range typ {
			if s, ok := item.(string); ok && s != "null" {
				return s
			}
		}
	}
	return ""
}

func allowsNull(schema map[string]any) bool {
	switch typ := schema["type"].(type) {
	case string:
		return typ == "null"
	case []any:
		return slices.ContainsFunc(typ, func(v any) bool { return v == "null" })
	}
	return false
}

func requiredFields(schema map[string]any) []string {
	raw, ok := schema["required"].([]any)
	if !ok {
		if values, ok := schema["required"].([]string); ok {
			return values
		}
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func isInteger(value any) bool {
	switch v := value.(type) {
	case json.Number:
		if _, err := v.Int64(); err == nil {
			return true
		}
		f, err := v.Float64()
		return err == nil && math.Trunc(f) == f
	case float64:
		return math.Trunc(v) == v
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return true
	default:
		return false
	}
}

func isNumber(value any) bool {
	switch v := value.(type) {
	case json.Number:
		_, err := v.Float64()
		return err == nil
	case float64, float32, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return true
	default:
		return false
	}
}

func numericValue(value any) float64 {
	switch v := value.(type) {
	case json.Number:
		f, _ := v.Float64()
		return f
	case float64:
		return v
	case float32:
		return float64(v)
	case int:
		return float64(v)
	case int8:
		return float64(v)
	case int16:
		return float64(v)
	case int32:
		return float64(v)
	case int64:
		return float64(v)
	case uint:
		return float64(v)
	case uint8:
		return float64(v)
	case uint16:
		return float64(v)
	case uint32:
		return float64(v)
	case uint64:
		return float64(v)
	default:
		return math.NaN()
	}
}

func validateNumericRange(path string, schema map[string]any, value float64) error {
	if min, ok := schemaNumber(schema["minimum"]); ok && value < min {
		return fmt.Errorf("%w: %s must be >= %s", ErrSchema, path, trimNumber(min))
	}
	if max, ok := schemaNumber(schema["maximum"]); ok && value > max {
		return fmt.Errorf("%w: %s must be <= %s", ErrSchema, path, trimNumber(max))
	}
	return nil
}

func schemaNumber(value any) (float64, bool) {
	switch v := value.(type) {
	case json.Number:
		f, err := v.Float64()
		return f, err == nil
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	default:
		return 0, false
	}
}

func trimNumber(value float64) string {
	if math.Trunc(value) == value {
		return fmt.Sprintf("%.0f", value)
	}
	return fmt.Sprintf("%g", value)
}

func enumContains(enum []any, value any) bool {
	for _, item := range enum {
		if fmt.Sprint(item) == fmt.Sprint(value) {
			return true
		}
	}
	return false
}

func enumList(enum []any) string {
	parts := make([]string, 0, len(enum))
	for _, item := range enum {
		parts = append(parts, fmt.Sprintf("%q", item))
	}
	return strings.Join(parts, ", ")
}

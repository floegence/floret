package tools

import (
	"fmt"
	"slices"
)

// InputPropertyOrder returns root property names in rendering order: declared
// required names first, then remaining names alphabetically. It does not mutate
// schema or impose any ordering requirement on tool arguments. Required names
// absent from properties are ignored, preserving JSON Schema semantics.
// Malformed properties or required declarations return an ErrSchema error.
func InputPropertyOrder(schema map[string]any) ([]string, error) {
	properties := map[string]any{}
	if value, exists := schema["properties"]; exists {
		var ok bool
		properties, ok = value.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%w: properties must be an object", ErrSchema)
		}
	}
	var required []string
	switch value := schema["required"].(type) {
	case nil:
	case []string:
		required = value
	case []any:
		for _, item := range value {
			name, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("%w: required must contain strings", ErrSchema)
			}
			required = append(required, name)
		}
	default:
		return nil, fmt.Errorf("%w: required must be an array", ErrSchema)
	}
	order := make([]string, 0, len(properties))
	seen := make(map[string]bool, len(required))
	for _, name := range required {
		if seen[name] {
			return nil, fmt.Errorf("%w: duplicate required property %q", ErrSchema, name)
		}
		seen[name] = true
		if _, ok := properties[name]; ok {
			order = append(order, name)
		}
	}
	remaining := make([]string, 0, len(properties))
	for name := range properties {
		if !seen[name] {
			remaining = append(remaining, name)
		}
	}
	slices.Sort(remaining)
	return append(order, remaining...), nil
}

// Package toolrender owns the rendering of local tool schema property order.
package toolrender

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/floegence/floret/v7/tools"
)

// Revision participates in rendered request lineage, not canonical identity.
const Revision = "tool-schema.v1"

// OrderSchemas changes only local input schema root property member order.
func OrderSchemas(format string, payload []byte) ([]byte, error) {
	switch format {
	case "openai_chat", "openai_responses", "anthropic_messages", "input_json":
	default:
		return nil, fmt.Errorf("unsupported tool schema request format %q", format)
	}
	envelope, err := object(payload)
	if err != nil {
		return nil, err
	}
	raw, exists := envelope["tools"]
	if !exists {
		return bytes.Clone(payload), nil
	}
	var definitions []json.RawMessage
	if err := json.Unmarshal(raw, &definitions); err != nil {
		return nil, fmt.Errorf("tools: %w", err)
	}
	for i := range definitions {
		definitions[i], err = OrderTool(format, definitions[i])
		if err != nil {
			return nil, fmt.Errorf("tools[%d]: %w", i, err)
		}
	}
	envelope["tools"], err = json.Marshal(definitions)
	if err != nil {
		return nil, err
	}
	return json.Marshal(envelope)
}

// OrderTool applies the same rule to one protocol-native raw tool segment.
func OrderTool(format string, raw []byte) ([]byte, error) {
	tool, err := object(raw)
	if err != nil {
		return nil, err
	}
	var kind string
	if value, ok := tool["type"]; ok {
		if err := json.Unmarshal(value, &kind); err != nil {
			return nil, err
		}
	}
	key := "input_schema"
	switch format {
	case "openai_responses":
		if kind == "namespace" {
			// A namespace contains function declarations, not business objects.
			return OrderSchemas(format, raw)
		}
		if kind != "function" {
			return bytes.Clone(raw), nil
		}
		key = "parameters"
	case "openai_chat":
		if kind != "function" {
			return bytes.Clone(raw), nil
		}
		function, err := object(tool["function"])
		if err != nil {
			return nil, err
		}
		if schema, ok := function["parameters"]; ok {
			function["parameters"], err = orderSchema(schema)
			if err != nil {
				return nil, err
			}
		}
		tool["function"], err = json.Marshal(function)
		if err != nil {
			return nil, err
		}
		return json.Marshal(tool)
	case "anthropic_messages", "input_json":
		// Provider-native hosted tools have a non-custom type.
		if kind != "" && kind != "custom" {
			return bytes.Clone(raw), nil
		}
	default:
		return nil, fmt.Errorf("unsupported tool schema request format %q", format)
	}
	if schema, ok := tool[key]; ok {
		tool[key], err = orderSchema(schema)
		if err != nil {
			return nil, err
		}
	}
	return json.Marshal(tool)
}

func orderSchema(raw []byte) ([]byte, error) {
	schema, err := object(raw)
	if err != nil {
		return nil, err
	}
	declaration := map[string]any{}
	properties := map[string]json.RawMessage{}
	if value, ok := schema["properties"]; ok {
		properties, err = object(value)
		if err != nil {
			return nil, fmt.Errorf("properties: %w", err)
		}
		names := make(map[string]any, len(properties))
		for name := range properties {
			names[name] = nil
		}
		declaration["properties"] = names
	}
	if value, ok := schema["required"]; ok {
		var required any
		if err := json.Unmarshal(value, &required); err != nil {
			return nil, err
		}
		declaration["required"] = required
	}
	order, err := tools.InputPropertyOrder(declaration)
	if err != nil {
		return nil, err
	}
	if _, ok := schema["properties"]; !ok {
		return bytes.Clone(raw), nil
	}
	var ordered bytes.Buffer
	ordered.WriteByte('{')
	for i, name := range order {
		if i > 0 {
			ordered.WriteByte(',')
		}
		key, _ := json.Marshal(name)
		ordered.Write(key)
		ordered.WriteByte(':')
		ordered.Write(properties[name])
	}
	ordered.WriteByte('}')
	schema["properties"] = ordered.Bytes()
	return json.Marshal(schema)
}

func object(raw []byte) (map[string]json.RawMessage, error) {
	var value map[string]json.RawMessage
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	if value == nil {
		return nil, fmt.Errorf("expected JSON object")
	}
	return value, nil
}

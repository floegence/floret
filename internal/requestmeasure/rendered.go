package requestmeasure

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
)

func split(payload []byte, format string) (map[string]json.RawMessage, []json.RawMessage, string, error) {
	field := "messages"
	if format == "openai_responses" {
		field = "input"
	} else if format != "openai_chat" && format != "anthropic_messages" {
		return nil, nil, "", fmt.Errorf("unsupported measurement format")
	}
	var body map[string]json.RawMessage
	if err := json.Unmarshal(payload, &body); err != nil {
		return nil, nil, "", err
	}
	var items []json.RawMessage
	if err := json.Unmarshal(body[field], &items); err != nil || items == nil {
		return nil, nil, "", fmt.Errorf("measurement requires complete ordered input")
	}
	delete(body, field)
	for _, key := range []string{"max_output_tokens", "max_tokens", "max_completion_tokens", "stream"} {
		delete(body, key)
	}
	return body, items, field, nil
}

func fingerprint(body map[string]json.RawMessage, items []json.RawMessage) string {
	// Normalize object key ordering while preserving list order and numeric text.
	raw, _ := json.Marshal(struct {
		Surface map[string]json.RawMessage
		Items   []json.RawMessage
	}{body, items})
	var value any
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if d.Decode(&value) != nil {
		return ""
	}
	raw, _ = json.Marshal(value)
	return fmt.Sprintf("sha256:%x", sha256.Sum256(raw))
}

func RenderedPrefix(payload []byte, format, executionIdentity, boundary string) (Prefix, error) {
	body, items, _, err := split(payload, format)
	if err != nil {
		return Prefix{}, err
	}
	p := Prefix{ExecutionIdentity: executionIdentity, Format: format, Fingerprint: fingerprint(body, items), ItemCount: len(items), Boundary: boundary}
	if !p.Valid() {
		return Prefix{}, fmt.Errorf("invalid measurement identity")
	}
	return p, nil
}

func RenderedSuffix(payload []byte, format, executionIdentity string, p Prefix) ([]byte, bool) {
	if !p.Valid() || p.Format != format || p.ExecutionIdentity != executionIdentity {
		return nil, false
	}
	body, items, field, err := split(payload, format)
	if err != nil || p.ItemCount > len(items) || fingerprint(body, items[:p.ItemCount]) != p.Fingerprint {
		return nil, false
	}
	// No fixed input is re-estimated. The estimator applies its ordinary framing,
	// media and conservative margin only to these complete frozen suffix items.
	if p.ItemCount == len(items) {
		return nil, true
	}
	suffix, err := json.Marshal(map[string]any{field: items[p.ItemCount:]})
	return suffix, err == nil
}

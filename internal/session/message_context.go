package session

import (
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"
)

// MessageContextItem is an admitted host snapshot, effective from its user
// message onward. It is model context, never resource authorization evidence.
type MessageContextItem struct {
	Kind  string `json:"kind"`
	Title string `json:"title"`
	Text  string `json:"text"`
}

func (item MessageContextItem) Validate() error {
	for _, field := range []struct {
		name, value string
		limit       int
	}{
		{"kind", item.Kind, 128}, {"title", item.Title, 256}, {"text", item.Text, 64_000},
	} {
		if !utf8.ValidString(field.value) || strings.TrimSpace(field.value) == "" || utf8.RuneCountInString(field.value) > field.limit {
			return fmt.Errorf("message context %s requires non-empty UTF-8 text of at most %d characters", field.name, field.limit)
		}
		if field.name != "text" && (field.value != strings.TrimSpace(field.value) || containsReferenceLineBreak(field.value)) {
			return fmt.Errorf("message context %s must be trim-stable and single-line", field.name)
		}
	}
	return nil
}

func ValidateMessageContext(items []MessageContextItem) error {
	if len(items) > 128 {
		return fmt.Errorf("message context contains %d items, maximum is 128", len(items))
	}
	for index, item := range items {
		if err := item.Validate(); err != nil {
			return fmt.Errorf("message context item %d: %w", index, err)
		}
	}
	raw, err := json.Marshal(items)
	if err != nil {
		return err
	}
	if len(raw) > 256*1024 {
		return fmt.Errorf("message context exceeds 262144 bytes")
	}
	return nil
}

// ProviderContent renders submitted facts in order without opaque locators.
// JSON strings delimit host/user data without granting it instruction authority.
func ProviderContent(message Message) string {
	var out strings.Builder
	out.WriteString(message.Content)
	if len(message.References) > 0 {
		out.WriteString("\n\nSubmitted references (data supplied with this message):\n")
		for _, ref := range message.References {
			raw, _ := json.Marshal(struct {
				Kind      MessageReferenceKind `json:"kind"`
				Label     string               `json:"label"`
				Text      string               `json:"text"`
				Truncated bool                 `json:"truncated"`
			}{ref.Kind, ref.Label, ref.Text, ref.Truncated})
			out.Write(raw)
			out.WriteByte('\n')
		}
	}
	if len(message.Context) > 0 {
		out.WriteString("\n\nRuntime context snapshots (effective from this message; later snapshots may supersede them):\n")
		for _, item := range message.Context {
			raw, _ := json.Marshal(item)
			out.Write(raw)
			out.WriteByte('\n')
		}
	}
	return out.String()
}

package contextpolicy

import (
	"errors"
	"math"
)

// ContextCharacterBreakdown describes visible frozen input, independently of tokens.
// Characters are Unicode code points in decoded text and deterministic schema JSON.
type ContextCharacterBreakdown struct {
	Source          string                     `json:"source"`
	Unit            string                     `json:"unit"`
	TotalCharacters int64                      `json:"total_characters"`
	Categories      []ContextCharacterCategory `json:"categories"`
	Limitations     []string                   `json:"limitations,omitempty"`
}

type ContextCharacterCategory struct {
	Category   string                   `json:"category"`
	Characters int64                    `json:"characters"`
	Details    []ContextCharacterDetail `json:"details,omitempty"`
}

type ContextCharacterDetail struct {
	Key        string `json:"key"`
	Source     string `json:"source,omitempty"`
	Namespace  string `json:"namespace,omitempty"`
	Characters int64  `json:"characters"`
}

// Clone returns an independent copy, including all optional attribution.
func (b *ContextCharacterBreakdown) Clone() *ContextCharacterBreakdown {
	if b == nil {
		return nil
	}
	out := *b
	out.Categories = append([]ContextCharacterCategory(nil), b.Categories...)
	for i := range out.Categories {
		out.Categories[i].Details = append([]ContextCharacterDetail(nil), b.Categories[i].Details...)
	}
	out.Limitations = append([]string(nil), b.Limitations...)
	return &out
}

// Validate rejects inconsistent units, negative counts, overflow and duplicate attribution.
func (b *ContextCharacterBreakdown) Validate() error {
	if b == nil {
		return nil
	}
	invalid := errors.New("invalid prepared character attribution")
	if b.Source != "prepared_characters_v1" || b.Unit != "unicode_code_points" || b.TotalCharacters < 0 {
		return invalid
	}
	total := int64(0)
	seen := map[string]bool{}
	for _, c := range b.Categories {
		switch c.Category {
		case "system", "user", "assistant", "tool_definition", "tool_call", "tool_result", "model_context", "compaction", "reasoning", "hosted_context", "unlabelled":
		default:
			return invalid
		}
		if seen[c.Category] || c.Characters < 0 || c.Characters > math.MaxInt64-total {
			return invalid
		}
		seen[c.Category] = true
		total += c.Characters
		details := int64(0)
		keys := make(map[[3]string]bool, len(c.Details))
		for _, d := range c.Details {
			key := [3]string{d.Key, d.Source, d.Namespace}
			if keys[key] || d.Characters < 0 || d.Characters > math.MaxInt64-details {
				return invalid
			}
			keys[key] = true
			details += d.Characters
		}
		if len(c.Details) > 0 && details != c.Characters {
			return invalid
		}
	}
	if total != b.TotalCharacters {
		return invalid
	}
	limits := map[string]bool{}
	for _, l := range b.Limitations {
		switch l {
		case "media_input", "opaque_reasoning", "unknown_content", "hosted_context", "opaque_transport":
		default:
			return invalid
		}
		if limits[l] {
			return invalid
		}
		limits[l] = true
	}
	return nil
}

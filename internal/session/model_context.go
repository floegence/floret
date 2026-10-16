package session

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"unicode/utf8"
)

// ModelContextSection is one complete, replaceable runtime fact partition.
type ModelContextSection struct {
	Key   string `json:"key"`
	Title string `json:"title"`
	Text  string `json:"text"`
}

// ModelContextSnapshot is a complete replacement. Nil means no supplied update.
type ModelContextSnapshot struct {
	Sections []ModelContextSection `json:"sections"`
}

func NormalizeModelContext(in *ModelContextSnapshot) (*ModelContextSnapshot, error) {
	if in == nil {
		return nil, nil
	}
	if len(in.Sections) > 128 {
		return nil, fmt.Errorf("model context contains more than 128 sections")
	}
	out := &ModelContextSnapshot{Sections: slices.Clone(in.Sections)}
	if out.Sections == nil {
		out.Sections = []ModelContextSection{}
	}
	for i, section := range out.Sections {
		if err := (MessageContextItem{Kind: section.Key, Title: section.Title, Text: section.Text}).Validate(); err != nil {
			return nil, fmt.Errorf("model context section %d: %w", i, err)
		}
		if !utf8.ValidString(section.Key) || strings.ContainsAny(section.Key, "\x00") {
			return nil, fmt.Errorf("invalid model context key")
		}
	}
	slices.SortFunc(out.Sections, func(a, b ModelContextSection) int { return strings.Compare(a.Key, b.Key) })
	for i := 1; i < len(out.Sections); i++ {
		if out.Sections[i-1].Key == out.Sections[i].Key {
			return nil, fmt.Errorf("duplicate model context key %q", out.Sections[i].Key)
		}
	}
	raw, err := json.Marshal(out)
	if err != nil {
		return nil, err
	}
	if len(raw) > 256*1024 {
		return nil, fmt.Errorf("model context exceeds 262144 bytes")
	}
	return out, nil
}

func CloneModelContext(in *ModelContextSnapshot) *ModelContextSnapshot {
	if in == nil {
		return nil
	}
	return &ModelContextSnapshot{Sections: slices.Clone(in.Sections)}
}

func EqualModelContext(a, b *ModelContextSnapshot) bool {
	return reflect.DeepEqual(a, b)
}

// CurrentModelContext reduces canonical facts in chronological order.
func CurrentModelContext(history []Message) *ModelContextSnapshot {
	var current *ModelContextSnapshot
	for _, message := range history {
		if message.ModelContextSnapshot != nil {
			current = CloneModelContext(message.ModelContextSnapshot)
		}
	}
	return current
}

func ModelContextChanges(previous, next *ModelContextSnapshot) ([]string, []string) {
	old := map[string]ModelContextSection{}
	if previous != nil {
		for _, section := range previous.Sections {
			old[section.Key] = section
		}
	}
	changed, removed := []string{}, []string{}
	if next != nil {
		for _, section := range next.Sections {
			if prior, exists := old[section.Key]; !exists || prior != section {
				changed = append(changed, section.Key)
			}
			delete(old, section.Key)
		}
	}
	for key := range old {
		removed = append(removed, key)
	}
	slices.Sort(removed)
	return changed, removed
}

// RenderModelContext derives a deterministic change message, never a second fact store.
func RenderModelContext(previous, next *ModelContextSnapshot) string {
	text, _ := renderModelContextParts(previous, next)
	return text
}

func renderModelContextParts(previous, next *ModelContextSnapshot) (string, []ModelContextPart) {
	if next == nil || EqualModelContext(previous, next) {
		return "", nil
	}
	old := map[string]ModelContextSection{}
	if previous != nil {
		for _, section := range previous.Sections {
			old[section.Key] = section
		}
	}
	changed := []ModelContextSection{}
	for _, section := range next.Sections {
		if prior, exists := old[section.Key]; !exists || prior != section {
			changed = append(changed, section)
		}
		delete(old, section.Key)
	}
	removed := make([]string, 0, len(old))
	for key := range old {
		removed = append(removed, key)
	}
	slices.Sort(removed)
	// Preserve canonical bytes while exposing fragment boundaries in memory.
	// Estimation never parses historical prompt text.
	parts := []ModelContextPart{{Text: "Model context update (runtime facts, not a user request; sections replace earlier values by key; removed keys are no longer current):\n"}}
	baseline, _ := json.Marshal(previous == nil)
	parts[0].Text += `{"baseline":` + string(baseline) + `,"sections":[`
	for i, section := range changed {
		if i > 0 {
			parts = append(parts, ModelContextPart{Text: ","})
		}
		raw, _ := json.Marshal(section)
		parts = append(parts, ModelContextPart{Key: section.Key, Text: string(raw)})
	}
	parts = append(parts, ModelContextPart{Text: `],"removed":[`})
	for i, key := range removed {
		if i > 0 {
			parts = append(parts, ModelContextPart{Text: ","})
		}
		raw, _ := json.Marshal(key)
		parts = append(parts, ModelContextPart{Key: key, Text: string(raw)})
	}
	parts = append(parts, ModelContextPart{Text: `]}`})
	var text strings.Builder
	for _, part := range parts {
		text.WriteString(part.Text)
	}
	return text.String(), parts
}

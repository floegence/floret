package engine

import (
	"fmt"
	"strings"
	"testing"
)

func TestTurnSupplementalContextFieldAndCollectionBoundaries(t *testing.T) {
	metadata := make(map[string]string, MaxTurnSupplementalMetadataPairs)
	for index := 0; index < MaxTurnSupplementalMetadataPairs; index++ {
		prefix := fmt.Sprintf("%02d", index)
		metadata[prefix+strings.Repeat("k", MaxTurnSupplementalMetadataKeyBytes-len(prefix))] = strings.Repeat("v", MaxTurnSupplementalMetadataValueRunes)
	}
	valid := TurnSupplementalContextItem{
		Kind:     strings.Repeat("类", MaxTurnSupplementalContextKindRunes),
		Title:    strings.Repeat("题", MaxTurnSupplementalContextTitleRunes),
		Text:     strings.Repeat("文", MaxTurnSupplementalContextTextRunes),
		Metadata: metadata,
	}
	if _, err := NormalizeAndValidateTurnSupplementalContext([]TurnSupplementalContextItem{valid}); err != nil {
		t.Fatalf("boundary supplemental item rejected: %v", err)
	}
	items := make([]TurnSupplementalContextItem, MaxTurnSupplementalContextItems)
	for index := range items {
		items[index] = TurnSupplementalContextItem{Kind: "context", Text: "value"}
	}
	if _, err := NormalizeAndValidateTurnSupplementalContext(items); err != nil {
		t.Fatalf("boundary supplemental item count rejected: %v", err)
	}

	cases := []struct {
		name  string
		items []TurnSupplementalContextItem
	}{
		{name: "overlong kind", items: []TurnSupplementalContextItem{{Kind: strings.Repeat("类", MaxTurnSupplementalContextKindRunes+1)}}},
		{name: "overlong title", items: []TurnSupplementalContextItem{{Title: strings.Repeat("题", MaxTurnSupplementalContextTitleRunes+1)}}},
		{name: "overlong text", items: []TurnSupplementalContextItem{{Text: strings.Repeat("文", MaxTurnSupplementalContextTextRunes+1)}}},
		{name: "invalid kind utf8", items: []TurnSupplementalContextItem{{Kind: string([]byte{0xff})}}},
		{name: "invalid title utf8", items: []TurnSupplementalContextItem{{Title: string([]byte{0xff})}}},
		{name: "invalid text utf8", items: []TurnSupplementalContextItem{{Text: string([]byte{0xff})}}},
		{name: "too many metadata pairs", items: []TurnSupplementalContextItem{{Metadata: supplementalMetadataPairs(MaxTurnSupplementalMetadataPairs + 1)}}},
		{name: "overlong metadata key", items: []TurnSupplementalContextItem{{Metadata: map[string]string{strings.Repeat("k", MaxTurnSupplementalMetadataKeyBytes+1): "value"}}}},
		{name: "overlong metadata value", items: []TurnSupplementalContextItem{{Metadata: map[string]string{"key": strings.Repeat("值", MaxTurnSupplementalMetadataValueRunes+1)}}}},
		{name: "invalid metadata key utf8", items: []TurnSupplementalContextItem{{Metadata: map[string]string{string([]byte{0xff}): "value"}}}},
		{name: "invalid metadata value utf8", items: []TurnSupplementalContextItem{{Metadata: map[string]string{"key": string([]byte{0xff})}}}},
		{name: "too many items", items: append(items, TurnSupplementalContextItem{Kind: "context", Text: "value"})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NormalizeAndValidateTurnSupplementalContext(tc.items); err == nil {
				t.Fatalf("invalid supplemental context succeeded: %#v", tc.items)
			}
		})
	}
}

func TestTurnSupplementalContextRenderedPayloadBoundary(t *testing.T) {
	atLimit := supplementalContextAtRenderedPayloadLimit(t)
	rendered := renderTurnSupplementalContext(atLimit)
	if len([]byte(rendered)) != MaxTurnSupplementalPayloadBytes {
		t.Fatalf("rendered payload = %d bytes, want %d", len([]byte(rendered)), MaxTurnSupplementalPayloadBytes)
	}
	if _, err := NormalizeAndValidateTurnSupplementalContext(atLimit); err != nil {
		t.Fatalf("boundary rendered payload rejected: %v", err)
	}
	overLimit := cloneTurnSupplementalContext(atLimit)
	overLimit[len(overLimit)-1].Text += "x"
	if _, err := NormalizeAndValidateTurnSupplementalContext(overLimit); err == nil {
		t.Fatal("oversized rendered supplemental payload succeeded")
	}
}

func supplementalMetadataPairs(count int) map[string]string {
	metadata := make(map[string]string, count)
	for index := 0; index < count; index++ {
		metadata[fmt.Sprintf("key-%03d", index)] = "value"
	}
	return metadata
}

func supplementalContextAtRenderedPayloadLimit(t *testing.T) []TurnSupplementalContextItem {
	t.Helper()
	items := make([]TurnSupplementalContextItem, 0, MaxTurnSupplementalContextItems)
	for index := 0; index < MaxTurnSupplementalContextItems; index++ {
		candidate := append(cloneTurnSupplementalContext(items), TurnSupplementalContextItem{
			Kind: "context",
			Text: strings.Repeat("x", MaxTurnSupplementalContextTextRunes),
		})
		if len([]byte(renderTurnSupplementalContext(candidate))) > MaxTurnSupplementalPayloadBytes {
			break
		}
		items = candidate
	}
	baseBytes := len([]byte(renderTurnSupplementalContext(items)))
	if len(items) == 0 || baseBytes >= MaxTurnSupplementalPayloadBytes {
		t.Fatalf("unexpected supplemental payload base size %d", baseBytes)
	}
	oneByteCandidate := append(cloneTurnSupplementalContext(items), TurnSupplementalContextItem{Kind: "context", Text: "x"})
	overhead := len([]byte(renderTurnSupplementalContext(oneByteCandidate))) - baseBytes - 1
	textBytes := MaxTurnSupplementalPayloadBytes - baseBytes - overhead
	if textBytes < 1 || textBytes > MaxTurnSupplementalContextTextRunes {
		t.Fatalf("supplemental payload boundary requires %d text bytes", textBytes)
	}
	return append(items, TurnSupplementalContextItem{Kind: "context", Text: strings.Repeat("x", textBytes)})
}

package session

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestModelContextValidationAndOwnership(t *testing.T) {
	in := &ModelContextSnapshot{Sections: []ModelContextSection{{Key: "z", Title: "Z", Text: "last"}, {Key: "a", Title: "A", Text: "first"}}}
	normalized, err := NormalizeModelContext(in)
	if err != nil || normalized.Sections[0].Key != "a" {
		t.Fatalf("normalized=%+v err=%v", normalized, err)
	}
	in.Sections[1].Text = "caller mutation"
	if normalized.Sections[0].Text != "first" {
		t.Fatal("snapshot aliases caller")
	}
	for _, sections := range [][]ModelContextSection{
		{{Key: "a", Title: "A", Text: "one"}, {Key: "a", Title: "A", Text: "two"}},
		{{Key: " a", Title: "A", Text: "one"}},
		{{Key: "a", Title: "A", Text: string([]byte{255})}},
		{{Key: "a", Title: "A", Text: strings.Repeat("x", 64001)}},
	} {
		if _, err := NormalizeModelContext(&ModelContextSnapshot{Sections: sections}); err == nil {
			t.Fatal("invalid snapshot accepted")
		}
	}
	sections := make([]ModelContextSection, 129)
	if _, err := NormalizeModelContext(&ModelContextSnapshot{Sections: sections}); err == nil {
		t.Fatal("oversized collection accepted")
	}
	for i := 0; i < 5; i++ {
		sections[i] = ModelContextSection{Key: string(rune('a' + i)), Title: "A", Text: strings.Repeat("x", 64000)}
	}
	if _, err := NormalizeModelContext(&ModelContextSnapshot{Sections: sections[:5]}); err == nil {
		t.Fatal("oversized payload accepted")
	}
}

func TestModelContextProjectionUsesOneChronologicalReducer(t *testing.T) {
	a, _ := NormalizeModelContext(&ModelContextSnapshot{Sections: []ModelContextSection{{Key: "a", Title: "A", Text: "old"}, {Key: "b", Title: "B", Text: "stable"}}})
	b, _ := NormalizeModelContext(&ModelContextSnapshot{Sections: []ModelContextSection{{Key: "a", Title: "A", Text: "new"}, {Key: "b", Title: "B", Text: "stable"}}})
	c := &ModelContextSnapshot{Sections: []ModelContextSection{}}
	history := []Message{{Role: User, Content: "work", EntryID: "u", ModelContextSnapshot: a}, {Role: Assistant, Content: "working"}, {Role: User, Kind: MessageKindModelContext, ModelContextSnapshot: a}, {Role: User, Kind: MessageKindModelContext, ModelContextSnapshot: b}, {Role: User, Kind: MessageKindModelContext, ModelContextSnapshot: c}}
	projected, _, err := ProjectProviderHistory(history, "")
	if err != nil || len(projected) != 5 {
		t.Fatalf("projection=%+v err=%v", projected, err)
	}
	if !strings.Contains(projected[1].Content, `"baseline":true`) || strings.Contains(projected[3].Content, "stable") || !strings.Contains(projected[4].Content, `"removed":["a","b"]`) {
		t.Fatalf("projection=%+v", projected)
	}
	again, _, _ := ProjectProviderHistory(projected, "")
	if !reflect.DeepEqual(projected, again) {
		t.Fatal("wire projection is not idempotent")
	}
	if HasRetryEligibleDurableInput(history[3]) {
		t.Fatal("snapshot became a retry input")
	}
	if !EqualModelContext(CurrentModelContext(history), c) {
		t.Fatal("clear was lost")
	}
}

func TestModelContextFragmentsPreserveRenderingAndOwnership(t *testing.T) {
	next := &ModelContextSnapshot{Sections: []ModelContextSection{{Key: "skills", Title: "Skills", Text: "Unicode 中文 <tool>"}}}
	for _, pair := range [][2]*ModelContextSnapshot{{nil, next}, {next, &ModelContextSnapshot{Sections: []ModelContextSection{}}}} {
		text, parts := renderModelContextParts(pair[0], pair[1])
		changed := []ModelContextSection{}
		removed := []string{}
		if pair[0] == nil {
			changed = append(changed, next.Sections...)
		} else {
			removed = append(removed, "skills")
		}
		raw, _ := json.Marshal(struct {
			Baseline bool                  `json:"baseline"`
			Sections []ModelContextSection `json:"sections"`
			Removed  []string              `json:"removed"`
		}{pair[0] == nil, changed, removed})
		expected := "Model context update (runtime facts, not a user request; sections replace earlier values by key; removed keys are no longer current):\n" + string(raw)
		if text != expected {
			t.Fatalf("published rendering changed: %q != %q", text, expected)
		}
		var joined strings.Builder
		for _, part := range parts {
			joined.WriteString(part.Text)
		}
		if joined.String() != text || text != RenderModelContext(pair[0], pair[1]) {
			t.Fatal("fragment rendering differs")
		}
		message := Message{Role: User, Content: text, ModelContextParts: parts}
		cloned := CloneMessage(message)
		cloned.ModelContextParts[0].Text = "mutated"
		if message.ModelContextParts[0].Text == "mutated" {
			t.Fatal("fragment alias")
		}
	}
}

package contextpolicy

import (
	"math"
	"math/rand/v2"
	"testing"
)

func TestCharacterBreakdownExactIntegerInvariants(t *testing.T) {
	for range 1000 {
		a, b := rand.Int64N(1<<50), rand.Int64N(1<<50)
		value := &ContextCharacterBreakdown{Source: "prepared_characters_v1", Unit: "unicode_code_points", TotalCharacters: a + b, Categories: []ContextCharacterCategory{{Category: "system", Characters: a}, {Category: "tool_definition", Characters: b, Details: []ContextCharacterDetail{{Key: "a", Characters: b / 2}, {Key: "b", Characters: b - b/2}}}}}
		if err := value.Validate(); err != nil {
			t.Fatal(err)
		}
		clone := value.Clone()
		clone.Categories[1].Details[0].Characters++
		if clone.Validate() == nil || value.Validate() != nil {
			t.Fatal("broken child total or aliased clone accepted")
		}
	}
	for _, value := range []*ContextCharacterBreakdown{
		{Source: "prepared_characters_v1", Unit: "bytes"},
		{Source: "prepared_characters_v1", Unit: "unicode_code_points", TotalCharacters: math.MaxInt64, Categories: []ContextCharacterCategory{{Category: "user", Characters: math.MaxInt64}, {Category: "system", Characters: 1}}},
		{Source: "prepared_characters_v1", Unit: "unicode_code_points", Categories: []ContextCharacterCategory{{Category: "user", Characters: -1}}},
		{Source: "prepared_characters_v1", Unit: "unicode_code_points", Categories: []ContextCharacterCategory{{Category: "user"}, {Category: "user"}}},
	} {
		if value.Validate() == nil {
			t.Fatalf("invalid characters accepted: %+v", value)
		}
	}
}

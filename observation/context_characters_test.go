package observation

import (
	"github.com/floegence/floret/v7/config"
	"testing"
)

func TestReconcileCharactersIndependentOfTokenDifference(t *testing.T) {
	for _, tokens := range []int64{0, 1, 68074, 126467} {
		s := accountingSample(tokens, 68074)
		s.Usage.InputTokensReported = true
		s.Usage.InputTokenScope = "provider_execution"
		s.RequestEstimate.Characters = &config.ContextCharacterBreakdown{Source: "prepared_characters_v1", Unit: "unicode_code_points", TotalCharacters: 169903, Categories: []config.ContextCharacterCategory{{Category: "tool_result", Characters: 169903}}}
		got, err := ReconcileContextStatus(s)
		if err != nil || !got.CharactersAvailable || !got.Measured || got.Characters.TotalCharacters != 169903 || got.InputTokens != tokens {
			t.Fatalf("same-request accounting: %+v %v", got, err)
		}
		got.Characters.Categories[0].Characters = 0
		if s.RequestEstimate.Characters.Categories[0].Characters != 169903 {
			t.Fatal("reconciliation leaked mutable data")
		}
	}
}

func TestReconcileCharactersUnavailableAndInvalid(t *testing.T) {
	s := accountingSample(100, 50)
	got, err := ReconcileContextStatus(s)
	if err != nil || !got.Measured || got.CharactersAvailable || got.CharacterLimitation != "characters_unavailable" {
		t.Fatalf("historical sample: %+v %v", got, err)
	}
	s.RequestEstimate.Characters = &config.ContextCharacterBreakdown{Source: "prepared_characters_v1", Unit: "unicode_code_points", TotalCharacters: 1}
	got, err = ReconcileContextStatus(s)
	if err == nil || !got.Measured || got.InputTokens != 100 || got.CharactersAvailable {
		t.Fatalf("invalid optional attribution: %+v %v", got, err)
	}
	s = accountingSample(0, 0)
	s.Usage.InputTokenScope = "provider_execution"
	s.Usage.OutputTokens = 20
	got, _ = ReconcileContextStatus(s)
	if got.Measured {
		t.Fatal("output-only usage became input measurement")
	}
	s.Usage.InputTokensReported = true
	got, _ = ReconcileContextStatus(s)
	if !got.Measured || got.InputTokens != 0 {
		t.Fatal("explicit native input zero lost")
	}
}

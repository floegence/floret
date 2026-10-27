package engine

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/tools"
)

func TestUncertainWarningPreservesPayloadBudgetAndArtifactIdentity(t *testing.T) {
	for _, strategy := range []tools.OutputStrategy{tools.OutputHead, tools.OutputTail} {
		t.Run(string(strategy), func(t *testing.T) {
			input := tools.Result{Text: "first\nlast", IsError: true, EffectOutcome: tools.EffectOutcomeUncertain}
			policy := tools.OutputPolicy{VisibleMaxBytes: 5, VisibleMaxLines: 1, Strategy: strategy, PreserveFull: true}
			result, projection := projectToolResultOutput(input, policy)
			expected := "first"
			if strategy == tools.OutputTail {
				expected = "last"
			}
			if !strings.HasPrefix(projection.VisibleText, "External effect unconfirmed.") || !strings.HasSuffix(projection.VisibleText, "\n"+expected) || !projection.Truncated {
				t.Fatalf("warning consumed output budget: %+v", projection)
			}
			if projection.FullOutputPlan == nil || projection.FullOutputPlan.Text != result.Text || projection.OriginalBytes != len(result.Text) || projection.OriginalLines != 3 || projection.VisibleLines != 2 || projection.ContentSHA256 != fmt.Sprintf("%x", sha256.Sum256([]byte(result.Text))) {
				t.Fatalf("inconsistent complete-output metadata: %+v", projection)
			}
		})
	}
}

func TestUncertainWarningKeepsCompletePayloadWithoutArchivingOrMutatingPolicy(t *testing.T) {
	for _, text := range []string{"", "世界", "one\ntwo"} {
		policy := tools.OutputPolicy{VisibleMaxBytes: len(text) + 1, VisibleMaxLines: strings.Count(text, "\n") + 1, PreserveFull: true}
		result, projection := projectToolResultOutput(tools.Result{Text: text, IsError: true, EffectOutcome: tools.EffectOutcomeUncertain, OutputPolicy: &policy}, policy)
		if projection.Truncated || projection.FullOutputPlan != nil || projection.VisibleText != result.Text || !strings.HasSuffix(result.Text, "\n"+text) || policy.VisibleMaxBytes != len(text)+1 {
			t.Fatalf("complete payload changed: %+v", projection)
		}
	}
}

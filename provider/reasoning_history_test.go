package provider_test

import (
	"github.com/floegence/floret/v7/provider"
	"testing"
)

func TestReasoningHistoryPolicyValidation(t *testing.T) {
	for _, policy := range []provider.ReasoningHistoryPolicy{"", provider.ReasoningHistoryAll, provider.ReasoningHistoryCurrentUser} {
		if err := (provider.Capabilities{Reasoning: provider.ReasoningUnsupported, ReasoningHistory: policy}).Validate(); err != nil {
			t.Fatal(err)
		}
	}
	if err := (provider.Capabilities{Reasoning: provider.ReasoningUnsupported, ReasoningHistory: "invented"}).Validate(); err == nil {
		t.Fatal("unknown reasoning history policy accepted")
	}
}

package agentharness

import (
	"context"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session"
)

type titleProviderFunc func(context.Context, provider.Request) (<-chan provider.StreamEvent, error)

func (f titleProviderFunc) Stream(ctx context.Context, request provider.Request) (<-chan provider.StreamEvent, error) {
	return f(ctx, request)
}

func TestProviderTitleAllowsReasoningBeforeVisibleTitle(t *testing.T) {
	for _, tc := range []struct {
		name  string
		cap   provider.ReasoningCapability
		level provider.ReasoningLevel
	}{
		{name: "unknown"},
		{name: "dynamic", cap: provider.ReasoningCapability{Kind: provider.ReasoningKindDynamic}},
		{name: "always on", cap: provider.ReasoningCapability{Kind: provider.ReasoningKindAlwaysOn}},
		{name: "lowest declared effort", cap: provider.ReasoningCapability{Kind: provider.ReasoningKindEffort, SupportedLevels: []provider.ReasoningLevel{provider.ReasoningLevelLow, provider.ReasoningLevelHigh}}, level: provider.ReasoningLevelLow},
		{name: "can disable", cap: provider.ReasoningCapability{Kind: provider.ReasoningKindToggle, DisableSupported: true}, level: provider.ReasoningLevelOff},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			generator := ProviderTitleGenerator{Reasoning: tc.cap, Provider: titleProviderFunc(func(_ context.Context, request provider.Request) (<-chan provider.StreamEvent, error) {
				calls++
				if request.Reasoning.Level != tc.level {
					t.Fatalf("reasoning=%+v, want %s", request.Reasoning, tc.level)
				}
				stream := make(chan provider.StreamEvent, 3)
				if request.Reasoning.Level != provider.ReasoningLevelOff {
					stream <- provider.StreamEvent{Type: provider.Reasoning, Text: "Private planning must not become the title."}
					// A short visible title can follow more than 64 reasoning tokens.
					if request.MaxOutputTokens < 160 {
						stream <- provider.StreamEvent{Type: provider.Truncated}
						close(stream)
						return stream, nil
					}
				}
				stream <- provider.StreamEvent{Type: provider.Delta, Text: "Recent agent research"}
				stream <- provider.StreamEvent{Type: provider.Done}
				close(stream)
				return stream, nil
			})}
			result, err := generator.GenerateTitle(t.Context(), TitleRequest{Messages: []session.Message{{Role: session.User, Content: "Research recent agent developments."}}})
			if err != nil || result.Title != "Recent agent research" || calls != 1 {
				t.Fatalf("result=%+v err=%v calls=%d", result, err, calls)
			}
		})
	}
}

func TestProviderTitlePreservesExplicitBudgetAndTruncationFailure(t *testing.T) {
	for _, truncated := range []bool{false, true} {
		generator := ProviderTitleGenerator{MaxOutputTokens: 32, Provider: titleProviderFunc(func(_ context.Context, request provider.Request) (<-chan provider.StreamEvent, error) {
			if request.MaxOutputTokens != 32 {
				t.Fatalf("explicit budget changed: %d", request.MaxOutputTokens)
			}
			stream := make(chan provider.StreamEvent, 2)
			stream <- provider.StreamEvent{Type: provider.Delta, Text: strings.Repeat("Topic ", 20)}
			terminal := provider.Done
			if truncated {
				terminal = provider.Truncated
			}
			stream <- provider.StreamEvent{Type: terminal}
			close(stream)
			return stream, nil
		})}
		result, err := generator.GenerateTitle(t.Context(), TitleRequest{Messages: []session.Message{{Role: session.User, Content: "Research recent agent developments."}}})
		if truncated {
			if err == nil || result.Title != "" {
				t.Fatalf("accepted partial title: %+v, %v", result, err)
			}
		} else if err != nil || result.Title == "" || utf8.RuneCountInString(result.Title) > 48 {
			t.Fatalf("title length contract changed: %+v, %v", result, err)
		}
	}
}

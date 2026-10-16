package agentharness

import (
	"context"
	"errors"
	"github.com/floegence/floret/v7/internal/providerattempt"
	"github.com/floegence/floret/v7/internal/providerretry"
	"strings"
	"testing"
	"testing/synctest"
	"time"
	"unicode/utf8"

	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
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

func TestProviderTitleRecoveryKeepsItsOwnIdentityAndDeadline(t *testing.T) {
	retry := providerretry.DefaultPolicy()
	retry.BaseDelay = time.Microsecond
	retry.MaxDelay = time.Microsecond
	retry.Jitter = 0
	var requests []provider.Request
	p := titleProviderFunc(func(_ context.Context, req provider.Request) (<-chan provider.StreamEvent, error) {
		requests = append(requests, req)
		if len(requests) == 1 {
			return nil, providerretry.WithAdvice(errors.New("overloaded"), providerretry.Advice{Category: providerretry.Transient})
		}
		events := make(chan provider.StreamEvent, 2)
		events <- provider.StreamEvent{Type: provider.Delta, Text: "Recovered title"}
		events <- provider.StreamEvent{Type: provider.Done}
		close(events)
		return events, nil
	})
	got, err := (ProviderTitleGenerator{Provider: p, RetryPolicy: retry}).GenerateTitle(t.Context(), TitleRequest{ThreadID: "thread", TurnID: "turn", Messages: []session.Message{{Role: session.User, Content: "title"}}})
	if err != nil || got.Title != "Recovered title" || len(requests) != 2 || requests[0].RunID != requests[1].RunID || requests[1].LogicalRequestID != ThreadTitleLogicalRequestID || requests[1].AttemptEpoch != 2 {
		t.Fatalf("title=%+v err=%v requests=%+v", got, err, requests)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	before := len(requests)
	_, err = (ProviderTitleGenerator{Provider: p, RetryPolicy: retry}).GenerateTitle(ctx, TitleRequest{Messages: []session.Message{{Role: session.User, Content: "title"}}})
	if !errors.Is(err, context.Canceled) || len(requests) != before {
		t.Fatal("cancelled title sent request")
	}
}

func TestTitlePhysicalDeadlineCoversSynchronousOpen(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := ProviderTitleGenerator{RetryPolicy: providerretry.Policy{Disabled: true}, TimeoutPolicy: providerattempt.RequestTimeoutPolicy{FirstOutputTimeout: time.Minute}, Provider: titleProviderFunc(func(ctx context.Context, _ provider.Request) (<-chan provider.StreamEvent, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		})}
		_, err := g.GenerateTitle(t.Context(), TitleRequest{Messages: []session.Message{{Role: session.User, Content: "title me"}}})
		var timeout *provider.RequestTimeoutError
		if !errors.As(err, &timeout) || timeout.Details.Stage != "first_output" {
			t.Fatalf("error=%v", err)
		}
	})
}

func TestProviderTitleRespectsModelOutputCapability(t *testing.T) {
	generator := ProviderTitleGenerator{Policy: contextpolicy.Policy{ModelMaxOutputTokens: 8}, Provider: titleProviderFunc(func(_ context.Context, req provider.Request) (<-chan provider.StreamEvent, error) {
		if req.MaxOutputTokens != 8 {
			t.Fatalf("title output = %d, want model capability 8", req.MaxOutputTokens)
		}
		stream := make(chan provider.StreamEvent, 2)
		stream <- provider.StreamEvent{Type: provider.Delta, Text: "Topic"}
		stream <- provider.StreamEvent{Type: provider.Done}
		close(stream)
		return stream, nil
	})}
	_, err := generator.GenerateTitle(t.Context(), TitleRequest{Messages: []session.Message{{Role: session.User, Content: "Describe the topic"}}})
	if err != nil {
		t.Fatal(err)
	}
}

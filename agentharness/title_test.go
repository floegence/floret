package agentharness

import (
	"context"
	"testing"
	"unicode/utf8"

	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/session"
)

type recordingTitleProvider struct {
	requests []provider.Request
}

func (p *recordingTitleProvider) Stream(_ context.Context, req provider.Request) (<-chan provider.StreamEvent, error) {
	p.requests = append(p.requests, req)
	out := make(chan provider.StreamEvent, 2)
	out <- provider.StreamEvent{Type: provider.Delta, Text: "一个特别特别特别长的中文自动标题应该被截断"}
	out <- provider.StreamEvent{Type: provider.Done, Reason: "stop"}
	close(out)
	return out, nil
}

func TestProviderTitleGeneratorUsesShortNonReasoningRequest(t *testing.T) {
	recorder := &recordingTitleProvider{}
	generator := ProviderTitleGenerator{
		Provider:     recorder,
		ProviderName: "fake",
		Model:        "fake-model",
	}

	result, err := generator.GenerateTitle(context.Background(), TitleRequest{
		ThreadID: "thread",
		TurnID:   "turn-1",
		Messages: []session.Message{{Role: session.User, Content: "Summarize this conversation."}},
	})
	if err != nil {
		t.Fatalf("GenerateTitle: %v", err)
	}
	if got := utf8.RuneCountInString(result.Title); got > defaultThreadTitleMaxRunes {
		t.Fatalf("title length=%d title=%q, want at most %d runes", got, result.Title, defaultThreadTitleMaxRunes)
	}
	if len(recorder.requests) != 1 {
		t.Fatalf("requests=%d, want 1", len(recorder.requests))
	}
	req := recorder.requests[0]
	if got := req.MaxOutputTokens; got != defaultThreadTitleMaxOutputTokens {
		t.Fatalf("MaxOutputTokens=%d, want %d", got, defaultThreadTitleMaxOutputTokens)
	}
	if req.MaxOutputTokens > 96 {
		t.Fatalf("MaxOutputTokens=%d, want compact title request budget", req.MaxOutputTokens)
	}
	if !req.DisableReasoning {
		t.Fatalf("DisableReasoning=false, want true")
	}
	if got := req.LogicalRequestID; got != ThreadTitleLogicalRequestID {
		t.Fatalf("LogicalRequestID=%q, want %q", got, ThreadTitleLogicalRequestID)
	}
}

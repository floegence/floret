package agentharness

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/floegence/floret/internal/provider"
	"github.com/floegence/floret/internal/session"
)

type recordingTitleProvider struct {
	requests []provider.Request
}

type neverClosingTitleProvider struct {
	started chan struct{}
	once    sync.Once
}

func (p *recordingTitleProvider) Stream(_ context.Context, req provider.Request) (<-chan provider.StreamEvent, error) {
	p.requests = append(p.requests, req)
	out := make(chan provider.StreamEvent, 2)
	out <- provider.StreamEvent{Type: provider.Delta, Text: "一个特别特别特别长的中文自动标题应该被截断"}
	out <- provider.StreamEvent{Type: provider.Done, Reason: "stop"}
	close(out)
	return out, nil
}

func (p *neverClosingTitleProvider) Stream(context.Context, provider.Request) (<-chan provider.StreamEvent, error) {
	p.once.Do(func() { close(p.started) })
	return make(chan provider.StreamEvent), nil
}

func TestProviderTitleGeneratorCancelsNeverClosingStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	provider := &neverClosingTitleProvider{started: make(chan struct{})}
	generator := ProviderTitleGenerator{Provider: provider, ProviderName: "fake", Model: "fake-model"}
	done := make(chan error, 1)
	go func() {
		_, err := generator.GenerateTitle(ctx, TitleRequest{
			ThreadID: "thread", TurnID: "turn-1",
			Messages: []session.Message{{Role: session.User, Content: "Summarize this conversation."}},
		})
		done <- err
	}()
	select {
	case <-provider.started:
	case <-time.After(time.Second):
		t.Fatal("title provider did not start")
	}
	cancel()
	select {
	case err := <-done:
		if err != context.Canceled || !errors.Is(err, context.Canceled) {
			t.Fatalf("GenerateTitle err = %v, want exact context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("GenerateTitle did not stop after context cancellation")
	}
}

func TestProviderTitleGeneratorUsesShortNonReasoningRequest(t *testing.T) {
	recorder := &recordingTitleProvider{}
	generator := ProviderTitleGenerator{
		Provider:     recorder,
		ProviderName: "fake",
		Model:        "fake-model",
		Reasoning:    provider.ReasoningCapability{Kind: provider.ReasoningKindEffort, SupportedLevels: []provider.ReasoningLevel{provider.ReasoningLevelOff, provider.ReasoningLevelLow}},
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
	if req.Reasoning.Level != provider.ReasoningLevelOff {
		t.Fatalf("Reasoning=%#v, want off", req.Reasoning)
	}
	if got := req.LogicalRequestID; got != ThreadTitleLogicalRequestID {
		t.Fatalf("LogicalRequestID=%q, want %q", got, ThreadTitleLogicalRequestID)
	}
}

func TestProviderTitleGeneratorOmitsReasoningForModelsWithoutShortSelection(t *testing.T) {
	recorder := &recordingTitleProvider{}
	generator := ProviderTitleGenerator{
		Provider:     recorder,
		ProviderName: "fake",
		Model:        "fake-model",
	}

	if _, err := generator.GenerateTitle(context.Background(), TitleRequest{
		ThreadID: "thread",
		TurnID:   "turn-1",
		Messages: []session.Message{{Role: session.User, Content: "Summarize this conversation."}},
	}); err != nil {
		t.Fatalf("GenerateTitle: %v", err)
	}
	if len(recorder.requests) != 1 {
		t.Fatalf("requests=%d, want 1", len(recorder.requests))
	}
	if got := recorder.requests[0].Reasoning; !got.IsZero() {
		t.Fatalf("Reasoning=%#v, want omitted", got)
	}
}

func TestThreadTitlePromptUsesAttachmentMetadataWithoutResourceReference(t *testing.T) {
	messages, err := threadTitlePromptMessages([]session.Message{{
		Role: session.User,
		Attachments: []session.MessageAttachment{{
			ResourceRef: "upload:secret-resource-id",
			Name:        "architecture.png",
			MIMEType:    "image/png",
		}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 2 || !strings.Contains(messages[1].Content, "architecture.png (image/png)") {
		t.Fatalf("title messages = %#v", messages)
	}
	if strings.Contains(messages[1].Content, "secret-resource-id") {
		t.Fatalf("title prompt leaked resource reference: %q", messages[1].Content)
	}
}

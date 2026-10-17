package compaction

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/providerretry"
	"github.com/floegence/floret/v7/internal/session"
	sessioncompaction "github.com/floegence/floret/v7/internal/session/compaction"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
)

func TestProviderSummaryReceivesHistoricalUserAndToolImages(t *testing.T) {
	images := []session.MessageAttachment{
		{ResourceRef: "private-upload", Name: "same.png", MIMEType: "image/png", SizeBytes: 100},
		{ResourceRef: "private-tool-media", Name: "same.png", MIMEType: "image/png", SizeBytes: 101},
	}
	var received []session.MessageAttachment
	p := summaryProviderFunc(func(_ context.Context, req provider.Request) (<-chan provider.StreamEvent, error) {
		if len(req.Tools) != 0 || req.PreviousState != nil || !req.DisableProviderState {
			t.Fatal("summary gained tools or continuation")
		}
		for _, m := range req.Messages {
			if m.Role == session.Tool || m.ToolCallID != "" {
				t.Fatal("fabricated tool exchange")
			}
			for _, a := range m.Attachments {
				if !strings.Contains(m.Content, "media:v1:") || !strings.Contains(m.Content, a.Name) {
					t.Fatal("image lost adjacent source label")
				}
				if strings.Contains(m.Content, a.ResourceRef) {
					t.Fatal("source label leaked resource authority")
				}
				received = append(received, a)
			}
		}
		return summaryEvents("visual facts"), nil
	})
	_, _, err := (ProviderSummaryGenerator{Provider: p, Policy: contextpolicy.Policy{ContextWindowTokens: 20000, ReservedSummaryTokens: 100}}).GenerateSummaryWithDetails(t.Context(), sessioncompaction.Preparation{CompactedHead: []session.Message{
		{Role: session.User, Attachments: images[:1]},
		{Role: session.Tool, ToolName: "image.read", ToolCallID: "read-1", ToolResult: &session.ToolResultView{Attachments: images[1:]}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(received) != 2 || received[0].ResourceRef != images[0].ResourceRef || received[1].ResourceRef != images[1].ResourceRef {
		t.Fatalf("summary lost or reordered native images: %+v", received)
	}
}

type frozenImageSummaryProvider struct {
	prepared, replays, refits, streams int
	handles                            []*frozenImageSummary
	requests                           []provider.Request
	oversized                          bool
}
type frozenImageSummary struct {
	owner   *frozenImageSummaryProvider
	request provider.Request
	closed  bool
}

func (p *frozenImageSummaryProvider) Stream(context.Context, provider.Request) (<-chan provider.StreamEvent, error) {
	panic("must stream frozen images")
}
func (p *frozenImageSummaryProvider) PrepareRequest(_ context.Context, req provider.Request) (provider.PreparedRequest, error) {
	p.prepared++
	return p.handle(req), nil
}
func (p *frozenImageSummaryProvider) handle(req provider.Request) *frozenImageSummary {
	h := &frozenImageSummary{owner: p, request: req}
	p.handles = append(p.handles, h)
	return h
}
func (h *frozenImageSummary) TokenEstimate() provider.TokenEstimate {
	e, _ := provider.GenericRequestEstimate(h.request)
	for _, m := range h.request.Messages {
		if len(m.Attachments) > 0 {
			if h.owner.oversized {
				e.EstimatedInputTokens += 100000
			} else {
				e.EstimatedInputTokens += 500
			}
		}
	}
	return e
}
func (h *frozenImageSummary) PayloadFingerprint() string {
	return fmt.Sprintf("frozen:%d:%v", h.request.MaxOutputTokens, h.request.Messages)
}
func (h *frozenImageSummary) Close() error { h.closed = true; return nil }
func (h *frozenImageSummary) PrepareRetry(_ context.Context, req provider.Request) (provider.PreparedRequest, error) {
	h.owner.replays++
	// Preserve frozen content, updating only observable transport identity.
	req.Messages = session.CloneMessages(h.request.Messages)
	return h.owner.handle(req), nil
}
func (h *frozenImageSummary) WithMaxOutputTokens(_ context.Context, cap int64) (provider.PreparedRequest, error) {
	if h.closed {
		return nil, errors.New("refit consumed frozen source")
	}
	h.owner.refits++
	req := h.request
	req.MaxOutputTokens = cap
	return h.owner.handle(req), nil
}
func (h *frozenImageSummary) Stream(context.Context) (<-chan provider.StreamEvent, error) {
	p := h.owner
	p.streams++
	p.requests = append(p.requests, h.request)
	if p.streams == 1 {
		return nil, providerretry.WithAdvice(errors.New("transient"), providerretry.Advice{Category: providerretry.Transient})
	}
	if p.streams == 2 {
		ch := make(chan provider.StreamEvent, 2)
		ch <- provider.StreamEvent{Type: provider.Delta, Text: "partial"}
		ch <- provider.StreamEvent{Type: provider.Truncated}
		close(ch)
		return ch, nil
	}
	return summaryEvents("facts"), nil
}
func TestImageSummaryTransportRetryAndRegenerationKeepFrozenInput(t *testing.T) {
	p := &frozenImageSummaryProvider{}
	retry := providerretry.DefaultPolicy()
	retry.BaseDelay = time.Microsecond
	retry.MaxDelay = time.Microsecond
	retry.Jitter = 0
	prep := sessioncompaction.Preparation{Request: sessioncompaction.Request{CompactionID: "compact"}, CompactedHead: []session.Message{{Role: session.User, Attachments: []session.MessageAttachment{{ResourceRef: "immutable", Name: "receipt.png", MIMEType: "image/png", SizeBytes: 10}}}}}
	summary, details, err := (ProviderSummaryGenerator{Provider: p, RetryPolicy: retry, Policy: contextpolicy.Policy{ContextWindowTokens: 10000, ReservedSummaryTokens: 100}}).GenerateSummaryWithDetails(t.Context(), prep)
	if err != nil || summary != "facts" || p.prepared != 1 || p.replays != 3 || p.refits != 1 || details.Attempts != 3 {
		t.Fatalf("%q %+v provider=%+v err=%v", summary, details, p, err)
	}
	if p.requests[0].LogicalRequestID != p.requests[1].LogicalRequestID || !strings.Contains(p.requests[1].AttemptID, "generation:0") || !strings.Contains(p.requests[2].AttemptID, "generation:1") {
		t.Fatal("retry and regeneration identity conflated")
	}
	for _, h := range p.handles {
		if !h.closed {
			t.Fatal("frozen image handle leaked")
		}
	}
	for _, req := range p.requests {
		if req.Messages[1].Content != p.requests[0].Messages[1].Content {
			t.Fatal("frozen image prompt changed")
		}
	}
}
func TestSingleImageCannotSilentlyFallBackToTextSummary(t *testing.T) {
	p := &frozenImageSummaryProvider{oversized: true}
	summary, _, err := (ProviderSummaryGenerator{Provider: p, Policy: contextpolicy.Policy{ContextWindowTokens: 3000, ReservedSummaryTokens: 100}}).GenerateSummaryWithDetails(t.Context(), sessioncompaction.Preparation{CompactedHead: []session.Message{{Role: session.User, Attachments: []session.MessageAttachment{{ResourceRef: "immutable", Name: "large.png", MIMEType: "image/png", SizeBytes: 10}}}}})
	if err == nil || summary != "" || !strings.Contains(err.Error(), "indivisible image") {
		t.Fatalf("summary=%q err=%v", summary, err)
	}
	for _, h := range p.handles {
		if !h.closed {
			t.Fatal("overflow image handle leaked")
		}
	}
}

func TestImageOnlyPreservedInputRetainsDiscoverableLocator(t *testing.T) {
	image := session.MessageAttachment{ResourceRef: "private-upload", Name: "receipt.png", MIMEType: "image/png", SizeBytes: 100}
	checkpoint := sessioncompaction.BuildCheckpointMessage("visual facts", []session.Message{{EntryID: "user-1", Role: session.User, Attachments: []session.MessageAttachment{image}}}, nil)
	if !strings.Contains(checkpoint.Content, "media:v1:") || !strings.Contains(checkpoint.Content, "receipt.png") {
		t.Fatal("image-only preserved input lost attachment label")
	}
	if strings.Contains(checkpoint.Content, image.ResourceRef) {
		t.Fatal("checkpoint leaked resource authority")
	}
	if !strings.Contains(checkpoint.Content, "reread") {
		t.Fatal("checkpoint must explain lossy visual memory")
	}
}

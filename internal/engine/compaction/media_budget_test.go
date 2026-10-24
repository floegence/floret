package compaction

import (
	"context"
	"errors"
	"fmt"
	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session"
	sessioncompaction "github.com/floegence/floret/v7/internal/session/compaction"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
	"testing"
)

type mediaBudgetProvider struct {
	accepted []string
	rejected int
	all      bool
}

func (p *mediaBudgetProvider) Stream(_ context.Context, req provider.Request) (<-chan provider.StreamEvent, error) {
	var refs []string
	for _, m := range req.Messages {
		for _, a := range m.Attachments {
			refs = append(refs, a.ResourceRef)
		}
	}
	if p.all || len(refs) > 1 {
		p.rejected++
		return nil, provider.ErrContextOverflow
	}
	p.accepted = append(p.accepted, refs...)
	return summaryEvents("all reviewed facts"), nil
}
func TestSummaryMediaBudgetCoversAllImagesInOrder(t *testing.T) {
	p := &mediaBudgetProvider{}
	m := session.Message{Role: session.User, Content: "Review every frame"}
	for i := range 6 {
		m.Attachments = append(m.Attachments, session.MessageAttachment{ResourceRef: fmt.Sprintf("image:%d", i), Name: "frame", MIMEType: "image/png", SizeBytes: 6 << 20})
	}
	_, details, err := (ProviderSummaryGenerator{Provider: p, Policy: contextpolicy.Policy{ContextWindowTokens: 950000, ReservedSummaryTokens: 100}}).GenerateSummaryWithDetails(t.Context(), sessioncompaction.Preparation{CompactedHead: []session.Message{m}})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.accepted) != 6 || details.Batches < 6 {
		t.Fatalf("accepted=%v details=%+v", p.accepted, details)
	}
	for i, ref := range p.accepted {
		if ref != fmt.Sprintf("image:%d", i) {
			t.Fatal(p.accepted)
		}
	}
}
func TestSummaryAtomicImageOverflowTerminatesWithCause(t *testing.T) {
	p := &mediaBudgetProvider{all: true}
	m := session.Message{Role: session.User, Attachments: []session.MessageAttachment{{ResourceRef: "image:one", Name: "frame", MIMEType: "image/png", SizeBytes: 6 << 20}}}
	_, _, err := (ProviderSummaryGenerator{Provider: p, Policy: contextpolicy.Policy{ContextWindowTokens: 950000, ReservedSummaryTokens: 100}}).GenerateSummaryWithDetails(t.Context(), sessioncompaction.Preparation{CompactedHead: []session.Message{m}})
	if !errors.Is(err, provider.ErrContextOverflow) || p.rejected > 10 {
		t.Fatalf("rejected=%d error=%v", p.rejected, err)
	}
}

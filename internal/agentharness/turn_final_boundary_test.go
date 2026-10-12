package agentharness

import (
	"github.com/floegence/floret/v7/internal/engine"
	"github.com/floegence/floret/v7/internal/event"
	"reflect"
	"testing"
)

func TestAcceptedFinalBoundaryDiscardsHookDraftAndPreservesLengthThroughEmptyRetry(t *testing.T) {
	repo, thread := newUsageProjectionThread(t)
	p := &turnProjection{ctx: t.Context(), thread: thread, turnID: "turn", runID: "run"}
	p.Emit(event.Event{Type: event.ProviderDelta, Message: "draft"})
	p.Emit(event.Event{Type: event.StepEnd, ContinuationReason: string(engine.ContinueProviderTruncated)})
	p.Emit(event.Event{Type: event.ContextContinue, Message: "continue", ContinuationReason: string(engine.ContinueHook)})
	p.Emit(event.Event{Type: event.StepEnd, ContinuationReason: string(engine.ContinueHook)})
	p.Emit(event.Event{Type: event.ProviderDelta, Message: "final part one"})
	p.Emit(event.Event{Type: event.StepEnd, ContinuationReason: string(engine.ContinueProviderTruncated)})
	p.Emit(event.Event{Type: event.StepEnd, ContinuationReason: string(engine.ContinueRetryEmpty)})
	p.Emit(event.Event{Type: event.ProviderDelta, Message: "final part two"})
	if p.acceptedFinal {
		t.Fatal("a continuation cannot accept the final response")
	}
	p.Emit(event.Event{Type: event.StepEnd, CompletionReason: string(engine.CompletionReasonNaturalStop)})
	if err := p.Flush(); err != nil {
		t.Fatal(err)
	}
	if !p.acceptedFinal || len(p.finalCandidate) != 2 {
		t.Fatalf("accepted=%v refs=%v", p.acceptedFinal, p.finalCandidate)
	}
	var texts []string
	for _, id := range p.finalCandidate {
		entry, err := repo.Entry(t.Context(), "thread", id)
		if err != nil {
			t.Fatal(err)
		}
		texts = append(texts, entry.Message.Content)
	}
	if !reflect.DeepEqual(texts, []string{"final part one", "final part two"}) {
		t.Fatalf("final texts=%v", texts)
	}
}

package agentharness

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/floegence/floret/v7/internal/engine"
	"github.com/floegence/floret/v7/internal/event"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/internal/testing/harness"
)

type finalizationHistoryGuard struct {
	*sessiontree.MemoryRepo
	finalizing atomic.Bool
}

func (r *finalizationHistoryGuard) Emit(e event.Event) {
	if e.Type == event.RunEnd {
		r.finalizing.Store(true)
	}
}

func (r *finalizationHistoryGuard) Path(ctx context.Context, threadID, leafID string) ([]sessiontree.Entry, error) {
	if r.finalizing.Load() {
		return nil, errors.New("finalization must not reread unrelated historical entries")
	}
	return r.MemoryRepo.Path(ctx, threadID, leafID)
}

func TestAcceptedFinalBoundaryReadsOnlyCurrentTurnAcrossRuns(t *testing.T) {
	for _, resumed := range []bool{false, true} {
		t.Run(map[bool]string{false: "original", true: "resumed"}[resumed], func(t *testing.T) {
			repo := &finalizationHistoryGuard{MemoryRepo: sessiontree.NewMemoryRepo()}
			if _, err := repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: "thread"}); err != nil {
				t.Fatal(err)
			}
			if _, err := repo.AcceptTurn(t.Context(), sessiontree.AcceptTurnRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", RequestFingerprint: "request", InputRequestFingerprint: "input", Input: session.Message{Role: session.User, Content: "work"}}); err != nil {
				t.Fatal(err)
			}
			runID := "run"
			if resumed {
				if _, err := sessiontree.AppendRunMessage(t.Context(), repo, "thread", "turn", runID, session.Message{Role: session.Assistant, Content: "earlier output"}); err != nil {
					t.Fatal(err)
				}
				if _, err := sessiontree.AppendTurnMarker(t.Context(), repo, "thread", "turn", runID, sessiontree.TurnWaiting, nil); err != nil {
					t.Fatal(err)
				}
				runID = "resumed-run"
				if _, err := sessiontree.AppendTurnMarker(t.Context(), repo, "thread", "turn", runID, sessiontree.TurnSavePoint, nil); err != nil {
					t.Fatal(err)
				}
			}
			h := New(Options{Repo: repo, Provider: harness.NewScriptedProvider(harness.Step(harness.Text("finished"), harness.Done())), ProviderName: "test", Model: "scripted", Sink: repo, TurnPolicy: TurnPolicy{ContextPolicy: contextpolicy.Policy{ContextWindowTokens: 10000, ReservedOutputTokens: 1000}}})
			thread := &Thread{id: "thread", harness: h}
			result, err := thread.runAccepted(t.Context(), "work", RunOptions{TurnID: "turn", RunID: runID, AdmissionCommitted: true}, nil)
			if err != nil || result.Status != engine.Completed || result.Output != "finished" {
				t.Fatalf("completion=%+v err=%v", result, err)
			}
			if !repo.finalizing.Load() {
				t.Fatal("finalization guard was not activated")
			}
			entries, err := repo.Entries(t.Context(), "thread")
			if err != nil {
				t.Fatal(err)
			}
			terminal := entries[len(entries)-1]
			var ids []string
			if err := json.Unmarshal([]byte(terminal.Metadata[sessiontree.FinalResponseEntriesMetadataKey]), &ids); err != nil || len(ids) != 1 {
				t.Fatalf("final response ids=%v err=%v terminal=%+v", ids, err, terminal)
			}
			final, err := repo.Entry(t.Context(), "thread", ids[0])
			if err != nil || final.RunID != runID || final.Message.Content != "finished" {
				t.Fatalf("final entry=%+v err=%v", final, err)
			}
		})
	}
}

func TestAcceptedFinalBoundaryDiscardsHookDraftAndPreservesLengthThroughEmptyRetry(t *testing.T) {
	repo, thread := newUsageProjectionThread(t)
	p := &turnProjection{ctx: t.Context(), thread: thread, turnID: "turn", runID: "run"}
	commitText(t, p, "draft")
	p.Emit(event.Event{Type: event.StepEnd, ContinuationReason: string(engine.ContinueProviderTruncated)})
	p.Emit(event.Event{Type: event.ContextContinue, Message: "continue", ContinuationReason: string(engine.ContinueHook)})
	p.Emit(event.Event{Type: event.StepEnd, ContinuationReason: string(engine.ContinueHook)})
	commitText(t, p, "final part one")
	p.Emit(event.Event{Type: event.StepEnd, ContinuationReason: string(engine.ContinueProviderTruncated)})
	p.Emit(event.Event{Type: event.StepEnd, ContinuationReason: string(engine.ContinueRetryEmpty)})
	commitText(t, p, "final part two")
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

func commitText(t *testing.T, p *turnProjection, text string) {
	t.Helper()
	if _, err := p.commitProviderContext(t.Context(), nil, engine.ResponseOutput{Items: []engine.ResponseOutputItem{{Message: session.Message{Role: session.Assistant, Content: text, ContentSourceID: "output:" + text}}}}, nil, false); err != nil {
		t.Fatal(err)
	}
}

type rejectingOutputSink struct{ err error }

func (s rejectingOutputSink) Emit(event.Event) {}
func (s rejectingOutputSink) CommitProviderOutput(context.Context, ProviderOutputCommitted) error {
	return s.err
}

func TestOutputTakeoverFailureDoesNotRecommitAndClosesDurableCalls(t *testing.T) {
	repo, thread := newUsageProjectionThread(t)
	cause := errors.New("view installation failed")
	p := &turnProjection{ctx: t.Context(), thread: thread, turnID: "turn", runID: "run", downstream: rejectingOutputSink{cause}}
	output := engine.ResponseOutput{Items: []engine.ResponseOutputItem{{Message: session.Message{Role: session.Assistant, Content: "once", ContentSourceID: "output:once"}}}}
	call := session.Message{Role: session.Assistant, Content: "tool_call", ToolCallID: "inspect", ToolName: "inspect", ToolArgs: "{}"}
	if committed, err := p.commitProviderContext(t.Context(), nil, output, []session.Message{call}, false); err != nil || !errors.Is(committed.NotificationErr, cause) {
		t.Fatalf("takeover=%v", err)
	}
	if !errors.Is(p.err, cause) {
		t.Fatal("takeover failure did not fence further commits")
	}
	if _, err := thread.finalizeFailedTurn(t.Context(), "turn", "run", engine.Failed, cause, "projection_error", engine.FailureOriginContract); err == nil {
		t.Fatal("missing failure")
	}
	entries, err := repo.Entries(t.Context(), "thread")
	if err != nil {
		t.Fatal(err)
	}
	texts, calls, results := 0, 0, 0
	for _, entry := range entries {
		if entry.Message.Content == "once" {
			texts++
		}
		if entry.Type == sessiontree.EntryToolCall {
			calls++
		}
		if entry.Type == sessiontree.EntryToolResult {
			results++
		}
	}
	if texts != 1 || calls != 1 || results != 1 {
		t.Fatalf("texts=%d calls=%d results=%d", texts, calls, results)
	}
}

type panickingOutputSink struct{}

func (panickingOutputSink) Emit(event.Event) {}
func (panickingOutputSink) CommitProviderOutput(context.Context, ProviderOutputCommitted) error {
	panic("subscriber failed")
}

func TestCommittedOutputNotificationPanicRetainsCanonicalMessages(t *testing.T) {
	repo, thread := newUsageProjectionThread(t)
	p := &turnProjection{ctx: t.Context(), thread: thread, turnID: "turn", runID: "run", downstream: panickingOutputSink{}}
	output := engine.ResponseOutput{Items: []engine.ResponseOutputItem{{Message: session.Message{Role: session.Assistant, Content: "committed once", ContentSourceID: "output:once"}}}}
	committed, err := p.commitProviderContext(t.Context(), nil, output, nil, false)
	if err != nil || committed.NotificationErr == nil || len(committed.Messages) != 1 || committed.Messages[0].EntryID == "" {
		t.Fatalf("committed=%+v err=%v", committed, err)
	}
	before, err := repo.Entries(t.Context(), "thread")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.commitProviderContext(t.Context(), nil, output, nil, false); err == nil {
		t.Fatal("notification failure did not fence another commit")
	}
	after, err := repo.Entries(t.Context(), "thread")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("notification panic duplicated committed output")
	}
}

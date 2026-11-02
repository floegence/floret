package sessiontree

import (
	"context"
	"errors"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/session/artifact"
	"github.com/floegence/floret/v7/tools"
	"reflect"
	"testing"
	"time"
)

func TestHostWorkTransactionalSettlementAndFork(t *testing.T) {
	backend, repo, attempt := canonicalSettlementFixture(t, true)
	w := HostWork{ID: "work", ExecutorID: "executor", Invocation: attempt.Invocation, State: "bound"}
	if _, err := repo.BindHostWork(t.Context(), w); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.BindHostWork(t.Context(), w); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("duplicate binding: %v", err)
	}
	request := HostWorkSettlement{Activity: &tools.ActivityPresentation{Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Status: "success", Summary: "completed"}}, ThreadID: "thread", ID: "work", ExecutorID: "executor", State: "completed", Notice: "work completed", Notify: true, Output: &artifact.FullOutput{Text: "retained output 中文"}}
	before := cloneMigrationRecords(backend.records)
	rollback := errors.New("commit failed")
	repo.backend = &domainCommitBoundaryBackend{Backend: backend, afterCallback: func() error { return rollback }}
	if _, err := repo.SettleHostWork(t.Context(), request); !errors.Is(err, rollback) {
		t.Fatalf("rollback=%v", err)
	}
	if !reflect.DeepEqual(before, backend.records) {
		t.Fatal("partial work or output committed")
	}
	repo.backend = backend
	final, err := repo.SettleHostWork(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.SettleHostWork(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	changed := request
	changed.Notice = "conflict"
	if _, err := repo.SettleHostWork(t.Context(), changed); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("conflict=%v", err)
	}
	if _, err := repo.FinishEffectDispatch(t.Context(), canonicalEffectRequest(attempt)); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CancelTurn(t.Context(), testCancelTurnRequest(time.Now())); err != nil {
		t.Fatal(err)
	}
	if _, eligible, err := repo.HostWorkContinuationKey(t.Context(), "thread"); err != nil || eligible {
		t.Fatalf("cancel wake=%t %v", eligible, err)
	}
	fork, err := repo.Fork(t.Context(), ForkOptions{SourceThreadID: "thread", NewThreadID: "fork", Now: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	works, err := repo.HostWorks(t.Context(), fork.ID)
	if err != nil || len(works) != 1 || !works[0].Inherited {
		t.Fatalf("fork work=%+v %v", works, err)
	}
	if _, eligible, err := repo.HostWorkContinuationKey(t.Context(), fork.ID); err != nil || eligible {
		t.Fatalf("fork woke=%t %v", eligible, err)
	}
	out, err := repo.ReadArtifact(t.Context(), ArtifactReadRequest{ThreadID: fork.ID, ArtifactID: final.Output.ID})
	if err != nil || out.Text != request.Output.Text {
		t.Fatalf("fork output=%+v %v", out, err)
	}
	changed = request
	changed.ThreadID = fork.ID
	if _, err := repo.SettleHostWork(t.Context(), changed); !errors.Is(err, ErrInvalidThreadAuthority) {
		t.Fatalf("fork took execution ownership: %v", err)
	}
}

func TestHostWorkRequiresDispatchedEffectAndValidNotice(t *testing.T) {
	_, repo, attempt := canonicalSettlementFixture(t, false)
	w := HostWork{ID: "work", ExecutorID: "executor", Invocation: attempt.Invocation, State: "bound"}
	if _, err := repo.BindHostWork(t.Context(), w); err == nil {
		t.Fatal("undispatched effect admitted")
	}
	if _, err := repo.BeginEffectDispatch(t.Context(), BeginEffectDispatchRequest{EffectAttemptID: attempt.EffectAttemptID, RequestFingerprint: "effect", AuthorizationProofHash: "proof"}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.BindHostWork(t.Context(), w); err != nil {
		t.Fatal(err)
	}
	req := HostWorkSettlement{ThreadID: "thread", ID: "work", ExecutorID: "executor", State: "unknown", Notice: "unknown", Notify: true, OutputUnavailable: true}
	if _, err := repo.SettleHostWork(t.Context(), req); err == nil {
		t.Fatal("unknown work granted notification")
	}
	req.Notify = false
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := repo.SettleHostWork(ctx, req); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel=%v", err)
	}
	works, err := repo.HostWorks(t.Context(), "thread")
	if err != nil || len(works) != 1 || works[0].State != "bound" {
		t.Fatalf("cancel changed facts=%+v %v", works, err)
	}
}

func TestHostWorkRejectsUnboundObservedResultWithoutMutation(t *testing.T) {
	backend, repo, attempt := canonicalSettlementFixture(t, true)
	before := cloneMigrationRecords(backend.records)
	_, err := repo.Append(t.Context(), Entry{ThreadID: "thread", TurnID: attempt.Invocation.TurnID, RunID: attempt.Invocation.RunID, Type: EntryToolResult, Message: session.Message{Role: session.Tool, ToolCallID: "read", ToolName: "read", ToolResult: &session.ToolResultView{Status: "success", ObservedHostWork: "absent"}}}, AppendOptions{})
	if err == nil || !reflect.DeepEqual(before, backend.records) {
		t.Fatalf("unbound observation published: %v", err)
	}
}

package sessiontree

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

func canonicalSettlementFixture(t *testing.T, dispatched bool) (*migrationTestBackend, *BackendRepo, EffectAttempt) {
	t.Helper()
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if _, err := memory.AcceptTurn(t.Context(), AcceptTurnRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", LogicalRequestID: "send", RequestFingerprint: "send", InputRequestFingerprint: "input", Input: session.Message{Role: session.User, Content: "run"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := memory.AppendRuntimeFacts(t.Context(), "thread", []Entry{
		{ID: "tool-call", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, ToolCallID: "call", ToolName: "shell", ToolArgs: "{}"}},
		{ID: "interaction-requested:approval", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryInteractionAsked, RequestKey: "approval", RequestFingerprint: "approval", Payload: json.RawMessage("{}")},
	}); err != nil {
		t.Fatal(err)
	}
	prepared, err := memory.PrepareEffectAttempt(t.Context(), PrepareEffectAttemptRequest{Invocation: EffectInvocationIdentity{ThreadID: "thread", TurnID: "turn", RunID: "run", ToolCallID: "call", ToolName: "shell", ArgumentHash: StableHash("{}")}, RequestFingerprint: "effect"})
	if err != nil {
		t.Fatal(err)
	}
	if dispatched {
		if _, err := memory.BeginEffectDispatch(t.Context(), BeginEffectDispatchRequest{EffectAttemptID: prepared.Attempt.EffectAttemptID, RequestFingerprint: "effect", AuthorizationProofHash: "proof"}); err != nil {
			t.Fatal(err)
		}
	}
	backend, repo := openV31ReviewFixture(t, memory)
	if err := repo.PrepareQueryThread(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	return backend, repo, prepared.Attempt
}

func TestV32CancelSettlementRollbackAndRestart(t *testing.T) {
	for _, dispatched := range []bool{false, true} {
		t.Run(map[bool]string{false: "prepared", true: "dispatched"}[dispatched], func(t *testing.T) {
			backend, repo, attempt := canonicalSettlementFixture(t, dispatched)
			request := testCancelTurnRequest(time.Now())
			before := cloneMigrationRecords(backend.records)
			rollback := errors.New("settlement commit failure")
			repo.backend = &domainCommitBoundaryBackend{Backend: backend, afterCallback: func() error { return rollback }}
			if _, err := repo.CancelTurn(t.Context(), request); !errors.Is(err, rollback) {
				t.Fatalf("rollback=%v", err)
			}
			if !reflect.DeepEqual(before, backend.records) {
				t.Fatal("partial cancellation survived rollback")
			}
			repo.backend = backend
			result, err := repo.CancelTurn(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			wantStatus, wantAttempt, wantTool := TurnAborted, EffectAttemptCancelled, "canceled"
			if dispatched {
				wantStatus, wantAttempt, wantTool = TurnFailed, EffectAttemptUnknown, "error"
			}
			if result.Terminal.TurnStatus != wantStatus || len(result.InteractionResolutions) != 1 || len(result.ToolResults) != 1 || result.ToolResults[0].Message.ToolResult.Status != wantTool {
				t.Fatalf("settlement=%+v", result)
			}
			if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
				entry, err := readCanonicalEntryMetadata(tx, "thread", effectAttemptEntryID(attempt.EffectAttemptID, wantAttempt))
				if err != nil {
					return err
				}
				stored, err := decodeEffectAttempt(entry)
				if err != nil || stored.State != wantAttempt {
					t.Fatalf("attempt=%+v, %v", stored, err)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if repo.threadMemory != nil {
				t.Fatal("cancellation hydrated the journal")
			}
			if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
				var err error
				repo, _, err = NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, false)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			replayed, err := repo.CancelTurn(t.Context(), request)
			if err != nil || !replayed.Replayed || replayed.Terminal.ID != result.Terminal.ID {
				t.Fatalf("replay=%+v, %v", replayed, err)
			}
			if _, err := repo.AcceptTurn(t.Context(), AcceptTurnRequest{ThreadID: "thread", TurnID: "next", RunID: "next-run", LogicalRequestID: "next", RequestFingerprint: "next", InputRequestFingerprint: "next-input", Input: session.Message{Role: session.User, Content: "continue"}}); err != nil {
				t.Fatal(err)
			}
			if repo.threadMemory != nil {
				t.Fatal("restart replay hydrated the journal")
			}
		})
	}
}

func TestV32FinishRejectsWrongRunAndRetiresPreparedEffects(t *testing.T) {
	backend, repo, attempt := canonicalSettlementFixture(t, false)
	if _, err := repo.AppendRuntimeFacts(t.Context(), "thread", []Entry{
		{ID: "interaction-resolved:approval", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryInteractionDone, Payload: json.RawMessage("{}")},
		{ID: "tool-result", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolResult, Message: session.Message{Role: session.Tool, ToolCallID: "call", ToolName: "shell", ToolResult: &session.ToolResultView{Status: "canceled"}}},
	}); err != nil {
		t.Fatal(err)
	}
	req := FinishTurnRequest{ThreadID: "thread", TurnID: "turn", RunID: "wrong-run", TerminalEntryID: "terminal", OutcomeFingerprint: "finish", Status: TurnCompleted}
	before := cloneMigrationRecords(backend.records)
	if _, err := repo.FinishTurn(t.Context(), req); !errors.Is(err, ErrStaleAuthority) {
		t.Fatalf("wrong run=%v", err)
	}
	if !reflect.DeepEqual(before, backend.records) {
		t.Fatal("wrong run mutated canonical history")
	}
	req.RunID = "run"
	rollback := errors.New("finish commit failure")
	repo.backend = &domainCommitBoundaryBackend{Backend: backend, afterCallback: func() error { return rollback }}
	if _, err := repo.FinishTurn(t.Context(), req); !errors.Is(err, rollback) {
		t.Fatalf("rollback=%v", err)
	}
	if !reflect.DeepEqual(before, backend.records) {
		t.Fatal("partial finish survived rollback")
	}
	repo.backend = backend
	finished, err := repo.FinishTurn(t.Context(), req)
	if err != nil || finished.Terminal.TurnStatus != TurnCompleted {
		t.Fatalf("finish=%+v, %v", finished, err)
	}
	retired, err := repo.Entry(t.Context(), "thread", effectAttemptEntryID(attempt.EffectAttemptID, EffectAttemptCancelled))
	if err != nil || retired.ParentID != finished.Terminal.ID {
		t.Fatalf("retirement=%+v, %v", retired, err)
	}
	replayed, err := repo.FinishTurn(t.Context(), req)
	if err != nil || !replayed.Replayed || repo.threadMemory != nil {
		t.Fatalf("replay=%+v, %v", replayed, err)
	}
}

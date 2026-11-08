package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

func TestRecoveredPreparedToolsAtomicSettlement(t *testing.T) {
	for _, failure := range []string{"error", "panic", "cancel"} {
		t.Run(failure, func(t *testing.T) {
			backend, repo, attempt := canonicalSettlementFixture(t, false)
			before := cloneMigrationRecords(backend.records)
			if err := repo.CancelPreparedToolCalls(t.Context(), "thread", "turn", "run"); !errors.Is(err, ErrRequestConflict) {
				t.Fatalf("unresolved approval: %v", err)
			}
			if !reflect.DeepEqual(before, backend.records) {
				t.Fatal("pending approval mutated")
			}
			_, err := repo.AppendRuntimeFacts(t.Context(), "thread", []Entry{{ID: "interaction-resolved:approval", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryInteractionDone, Payload: json.RawMessage(`{"accepted":true,"approved":true}`)}})
			if err != nil {
				t.Fatal(err)
			}
			before = cloneMigrationRecords(backend.records)
			injected := errors.New("commit failed")
			repo.backend = &domainCommitBoundaryBackend{Backend: backend, afterCallback: func() error {
				if failure == "panic" {
					panic(injected)
				}
				return injected
			}}
			func() {
				if failure == "panic" {
					defer func() {
						if recover() != injected {
							t.Error("missing panic")
						}
					}()
				}
				ctx := t.Context()
				if failure == "cancel" {
					var cancel func()
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				}
				if err := repo.CancelPreparedToolCalls(ctx, "thread", "turn", "run"); err == nil {
					t.Fatal("missing failure")
				}
			}()
			if !reflect.DeepEqual(before, backend.records) {
				t.Fatal("partial recovery survived failure")
			}
			repo.backend = backend
			if err := repo.CancelPreparedToolCalls(t.Context(), "thread", "turn", "run"); err != nil {
				t.Fatal(err)
			}
			stored, err := repo.EffectAttempt(t.Context(), "thread", attempt.EffectAttemptID)
			if err != nil || stored.State != EffectAttemptCancelled || stored.Invocation != attempt.Invocation {
				t.Fatalf("attempt=%+v %v", stored, err)
			}
			entry, err := repo.Entry(t.Context(), "thread", "tool-restarted:turn:call")
			if err != nil || entry.Message.ToolCallID != "call" || entry.Message.ToolName != "shell" || entry.Message.ToolResult.Status != "canceled" {
				t.Fatalf("result=%+v %v", entry, err)
			}
			before = cloneMigrationRecords(backend.records)
			if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
				var err error
				repo, _, err = NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, false)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if err := repo.CancelPreparedToolCalls(t.Context(), "thread", "turn", "run"); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, backend.records) {
				t.Fatal("restart rewrote settled records")
			}
		})
	}
}

func TestRecoveredToolsNeverCancelDispatchedEffects(t *testing.T) {
	backend, repo, _ := canonicalSettlementFixture(t, true)
	if _, err := repo.AppendRuntimeFacts(t.Context(), "thread", []Entry{{ID: "interaction-resolved:approval", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryInteractionDone, Payload: json.RawMessage(`{"accepted":true,"approved":true}`)}}); err != nil {
		t.Fatal(err)
	}
	before := cloneMigrationRecords(backend.records)
	if err := repo.CancelPreparedToolCalls(t.Context(), "thread", "turn", "run"); !errors.Is(err, ErrEffectOutcomeUnknown) {
		t.Fatalf("unknown outcome: %v", err)
	}
	if !reflect.DeepEqual(before, backend.records) {
		t.Fatal("unknown effect changed")
	}
}

package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/storagebridge"
	publicstorage "github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
)

type lateHostWorkRepo interface {
	Repo
	RuntimeTurnRepo
	EffectAttemptRepo
	HostWorkRepo
	CanonicalTurnReadRepo
	CanonicalTurnRepo
}

func TestCanonicalTurnReadsAcrossEarlierHostWorkSettlement(t *testing.T) {
	for _, store := range []string{"memory", "sqlite"} {
		t.Run(store, func(t *testing.T) {
			var repo lateHostWorkRepo = NewMemoryRepo()
			reopen := func() {}
			if store == "sqlite" {
				backend, err := storagebridge.Open(t.Context(), storagebridge.Source(publicstorage.SQLite(t.TempDir()+"/state.sqlite")))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = backend.Close() })
				if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
					r, err := NewFreshCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, false)
					repo = r
					return err
				}); err != nil {
					t.Fatal(err)
				}
				reopen = func() { repo = reopenCurrentStartupRepo(t, backend, false) }
				reopen()
			}
			if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
				t.Fatal(err)
			}
			accept := func(turn string) {
				t.Helper()
				_, err := repo.AcceptTurn(t.Context(), AcceptTurnRequest{ThreadID: "thread", TurnID: turn, RunID: turn + "-run", LogicalRequestID: turn, RequestFingerprint: turn, InputRequestFingerprint: turn, Input: session.Message{Role: session.User, Content: turn}})
				if err != nil {
					t.Fatal(err)
				}
			}
			finish := func(turn string) {
				t.Helper()
				_, err := repo.FinishTurn(t.Context(), FinishTurnRequest{ThreadID: "thread", TurnID: turn, RunID: turn + "-run", TerminalEntryID: turn + "-done", OutcomeFingerprint: turn, Status: TurnCompleted})
				if err != nil {
					t.Fatal(err)
				}
			}
			accept("first")
			call := Entry{ThreadID: "thread", TurnID: "first", RunID: "first-run", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, ToolCallID: "call", ToolName: "shell", ToolArgs: "{}"}}
			if _, err := repo.Append(t.Context(), call, AppendOptions{}); err != nil {
				t.Fatal(err)
			}
			invocation := EffectInvocationIdentity{ThreadID: "thread", TurnID: "first", RunID: "first-run", ToolCallID: "call", ToolName: "shell", ArgumentHash: StableHash("{}")}
			prepared, err := repo.PrepareEffectAttempt(t.Context(), PrepareEffectAttemptRequest{Invocation: invocation, RequestFingerprint: "effect"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := repo.BeginEffectDispatch(t.Context(), BeginEffectDispatchRequest{EffectAttemptID: prepared.Attempt.EffectAttemptID, RequestFingerprint: "effect", AuthorizationProofHash: "proof"}); err != nil {
				t.Fatal(err)
			}
			if _, err := repo.BindHostWork(t.Context(), HostWork{ID: "background", ExecutorID: "executor", Invocation: invocation, State: "bound"}); err != nil {
				t.Fatal(err)
			}
			result := Entry{ThreadID: "thread", TurnID: "first", RunID: "first-run", Type: EntryToolResult, Message: session.Message{Role: session.Tool, ToolCallID: "call", ToolName: "shell", Content: "running", ToolResult: &session.ToolResultView{Status: "success"}}}
			if _, err := repo.FinishEffectDispatch(t.Context(), FinishEffectDispatchRequest{EffectAttemptID: prepared.Attempt.EffectAttemptID, RequestFingerprint: "effect", OutcomeFingerprint: "done", Result: result}); err != nil {
				t.Fatal(err)
			}
			finish("first")
			accept("second")
			if _, err := repo.SettleHostWork(t.Context(), HostWorkSettlement{ThreadID: "thread", ID: "background", ExecutorID: "executor", State: "completed", Notice: "completed during second turn", Notify: true}); err != nil {
				t.Fatal(err)
			}
			if _, err := AppendRunMessage(t.Context(), repo, "thread", "second", "second-run", session.Message{Role: session.Assistant, Content: "second result"}); err != nil {
				t.Fatal(err)
			}
			verify := func(phase string) {
				t.Helper()
				for _, turn := range []string{"first", "second"} {
					read, err := repo.ReadCanonicalTurn(context.Background(), "thread", turn)
					if err != nil {
						t.Fatalf("%s read %s: %v", phase, turn, err)
					}
					if read.LatestTurn.TurnID != "second" {
						t.Fatalf("latest=%s", read.LatestTurn.TurnID)
					}
					count := 0
					for _, item := range read.Turn.Entries {
						if item.Entry.ID == hostWorkEntryID("background", true) {
							count++
						}
					}
					want := 0
					if turn == "first" {
						want = 1
					}
					if count != want {
						t.Fatalf("%s settlement count=%d want=%d", turn, count, want)
					}
					if _, found, err := repo.CanonicalTurnEntries(t.Context(), "thread", turn, turn+"-run"); err != nil || !found {
						t.Fatalf("%s entries %s: found=%t err=%v", phase, turn, found, err)
					}
				}
			}
			verify("before final boundary")
			finish("second")
			for i := 0; i < 2; i++ {
				reopen()
				verify(fmt.Sprintf("restart %d", i))
			}
		})
	}
}

func TestCanonicalTurnSettlementGapRejectsInvalidAuthority(t *testing.T) {
	work := HostWork{ID: "late", ExecutorID: "executor", Invocation: EffectInvocationIdentity{ThreadID: "thread", TurnID: "old", RunID: "old-run", ToolCallID: "call", ToolName: "shell", ArgumentHash: StableHash("{}")}, State: "completed", Notice: "done"}
	payload, _ := json.Marshal(work)
	valid := Entry{ID: hostWorkEntryID("late", true), ThreadID: "thread", TurnID: "old", RunID: "old-run", ParentID: "before", PathDepth: 3, Type: EntryHostWork, Payload: payload}
	entries := []Entry{{ID: "before", ThreadID: "thread", TurnID: "current", PathDepth: 2}, {ID: "after", ThreadID: "thread", TurnID: "current", ParentID: valid.ID, PathDepth: 4}}
	for _, name := range []string{"valid", "ordinary foreign entry", "same-turn omission", "wrong thread", "invalid payload", "non-decreasing depth", "missing ancestor", "canceled"} {
		t.Run(name, func(t *testing.T) {
			parent := valid
			ctx := t.Context()
			switch name {
			case "ordinary foreign entry":
				parent.Type = EntryAssistantMessage
			case "same-turn omission":
				parent.TurnID = "current"
			case "wrong thread":
				parent.ThreadID = "other"
			case "invalid payload":
				parent.Payload = json.RawMessage(`{}`)
			case "non-decreasing depth":
				parent.PathDepth = 4
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			err := validateCanonicalTurnLinksWithReader(ctx, entries, func(id string) (Entry, error) {
				if name == "missing ancestor" {
					return Entry{}, ErrEntryNotFound
				}
				return parent, nil
			})
			if name == "valid" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			want := ErrAuthorityCorrupt
			if name == "canceled" {
				want = context.Canceled
			}
			if !errors.Is(err, want) {
				t.Fatalf("got %v want %v", err, want)
			}
		})
	}
}

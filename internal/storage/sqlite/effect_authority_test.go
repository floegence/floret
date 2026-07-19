package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/session/artifact"
	"github.com/floegence/floret/internal/sessiontree"
)

type effectAuthorityTestRepo interface {
	CreateThread(context.Context, sessiontree.ThreadMeta) (sessiontree.ThreadMeta, error)
	AdmitTurn(context.Context, sessiontree.AdmitTurnRequest) (sessiontree.AdmitTurnResult, error)
	RenewTurnLease(context.Context, sessiontree.TurnLease) (sessiontree.TurnLease, error)
	PrepareEffectAttempt(context.Context, sessiontree.PrepareEffectAttemptRequest) (sessiontree.PrepareEffectAttemptResult, error)
	RejectEffectAttempt(context.Context, sessiontree.RejectEffectAttemptRequest) (sessiontree.EffectAttempt, error)
	BeginEffectDispatch(context.Context, sessiontree.BeginEffectDispatchRequest) (sessiontree.EffectAttempt, error)
	FinishEffectDispatch(context.Context, sessiontree.FinishEffectDispatchRequest) (sessiontree.FinishEffectDispatchResult, error)
	MarkEffectUnknown(context.Context, sessiontree.MarkEffectUnknownRequest) (sessiontree.EffectAttempt, error)
	FinishTurn(context.Context, sessiontree.FinishTurnRequest) (sessiontree.FinishTurnResult, error)
	Entries(context.Context, string) ([]sessiontree.Entry, error)
}

func TestEffectAuthorityMatchesMemoryAndSQLite(t *testing.T) {
	now := time.Date(2026, 7, 19, 9, 0, 0, 0, time.UTC)
	policy := sessiontree.LeasePolicy{TTL: time.Hour, RenewInterval: 20 * time.Minute, ClockSkewAllowance: time.Second}
	memory, err := sessiontree.NewMemoryRepoWithLeasePolicy(policy, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	sqliteStore, err := Open(filepath.Join(t.TempDir(), "effect.db"), WithLeasePolicy(policy), WithAuthorityClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqliteStore.Close() })
	for name, repo := range map[string]effectAuthorityTestRepo{"memory": memory, "sqlite": sqliteStore} {
		t.Run(name, func(t *testing.T) {
			lease := seedEffectTurn(t, repo, now, "thread", "turn", "run")
			prepare := effectPrepareRequest(lease, "run", "call", "hash-a", "request-a", now)
			prepared, err := repo.PrepareEffectAttempt(context.Background(), prepare)
			if err != nil || prepared.Replayed || prepared.Attempt.State != sessiontree.EffectAttemptPrepared || prepared.Attempt.EffectAttemptID == "" {
				t.Fatalf("prepare = %#v err=%v", prepared, err)
			}
			replayed, err := repo.PrepareEffectAttempt(context.Background(), prepare)
			if err != nil || !replayed.Replayed || replayed.Attempt.EffectAttemptID != prepared.Attempt.EffectAttemptID {
				t.Fatalf("prepare replay = %#v err=%v", replayed, err)
			}
			conflict := prepare
			conflict.RequestFingerprint = "request-b"
			if _, err := repo.PrepareEffectAttempt(context.Background(), conflict); !errors.Is(err, sessiontree.ErrRequestConflict) {
				t.Fatalf("prepare conflict err=%v", err)
			}
			renewed, err := repo.RenewTurnLease(context.Background(), lease)
			if err != nil {
				t.Fatal(err)
			}
			dispatching, err := repo.BeginEffectDispatch(context.Background(), sessiontree.BeginEffectDispatchRequest{
				Lease: renewed, EffectAttemptID: prepared.Attempt.EffectAttemptID, RequestFingerprint: "request-a",
				ObservedHeartbeat: lease.Heartbeat, AuthorizationProofHash: "authorization-proof", Now: now,
			})
			if err != nil || dispatching.State != sessiontree.EffectAttemptDispatching {
				t.Fatalf("begin = %#v err=%v", dispatching, err)
			}
			finishRequest := sessiontree.FinishEffectDispatchRequest{
				Lease: renewed, EffectAttemptID: prepared.Attempt.EffectAttemptID, RequestFingerprint: "request-a",
				OutcomeFingerprint: "outcome-a", Result: effectResultEntry("thread", "turn", "call", "tool", "ok"),
				FullOutput: &artifact.FullOutput{Text: "complete output", Kind: "report", MIME: "text/markdown"}, Now: now,
			}
			finished, err := repo.FinishEffectDispatch(context.Background(), finishRequest)
			if err != nil || finished.Attempt.State != sessiontree.EffectAttemptCompleted || finished.Attempt.ResultEntryID == "" || finished.Artifact == nil {
				t.Fatalf("finish = %#v err=%v", finished, err)
			}
			replayedFinish, err := repo.FinishEffectDispatch(context.Background(), finishRequest)
			if err != nil || !replayedFinish.Replayed || replayedFinish.Result.ID != finished.Result.ID || replayedFinish.Artifact == nil || *replayedFinish.Artifact != *finished.Artifact {
				t.Fatalf("finish replay = %#v err=%v", replayedFinish, err)
			}
			for _, mutate := range []func(*artifact.FullOutput){
				func(full *artifact.FullOutput) { full.Text = "changed" },
				func(full *artifact.FullOutput) { full.Kind = "changed" },
				func(full *artifact.FullOutput) { full.MIME = "application/json" },
			} {
				conflict := finishRequest
				full := *finishRequest.FullOutput
				mutate(&full)
				conflict.FullOutput = &full
				if result, err := repo.FinishEffectDispatch(context.Background(), conflict); !errors.Is(err, sessiontree.ErrRequestConflict) || result.Result.ID != "" || result.Artifact != nil {
					t.Fatalf("full-output conflict result=%#v err=%v", result, err)
				}
			}
			entries, err := repo.Entries(context.Background(), "thread")
			if err != nil || countEffectResults(entries, "call") != 1 {
				t.Fatalf("effect result entries = %#v err=%v", entries, err)
			}
			for _, entry := range entries {
				if entry.ID == finished.Attempt.ResultEntryID && entry.Metadata[sessiontree.PendingToolEffectAttemptIDKey] != prepared.Attempt.EffectAttemptID {
					t.Fatalf("effect result attempt identity = %#v, want %q", entry.Metadata, prepared.Attempt.EffectAttemptID)
				}
			}
		})
	}
}

func TestFinishTurnCancelsPreparedAndRejectsDispatchingEffect(t *testing.T) {
	now := time.Date(2026, 7, 19, 10, 0, 0, 0, time.UTC)
	for name, makeRepo := range map[string]func(t *testing.T) effectAuthorityTestRepo{
		"memory": func(t *testing.T) effectAuthorityTestRepo { return sessiontree.NewMemoryRepo() },
		"sqlite": func(t *testing.T) effectAuthorityTestRepo {
			store, err := Open(filepath.Join(t.TempDir(), "effect-finish.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			return store
		},
	} {
		t.Run(name, func(t *testing.T) {
			repo := makeRepo(t)
			lease := seedEffectTurn(t, repo, now, "thread", "turn", "run")
			prepared, err := repo.PrepareEffectAttempt(context.Background(), effectPrepareRequest(lease, "run", "prepared", "hash-p", "request-p", now))
			if err != nil {
				t.Fatal(err)
			}
			dispatching, err := repo.PrepareEffectAttempt(context.Background(), effectPrepareRequest(lease, "run", "dispatching", "hash-d", "request-d", now))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := repo.BeginEffectDispatch(context.Background(), sessiontree.BeginEffectDispatchRequest{
				Lease: lease, EffectAttemptID: dispatching.Attempt.EffectAttemptID, RequestFingerprint: "request-d",
				ObservedHeartbeat: lease.Heartbeat, AuthorizationProofHash: "proof", Now: now,
			}); err != nil {
				t.Fatal(err)
			}
			finish := sessiontree.FinishTurnRequest{
				Lease: lease, RunID: "run", TerminalEntryID: "terminal", Status: sessiontree.TurnFailed,
				OutcomeFingerprint: "turn-outcome", Now: now,
			}
			if _, err := repo.FinishTurn(context.Background(), finish); !errors.Is(err, sessiontree.ErrEffectOutcomeUnknown) {
				t.Fatalf("FinishTurn err=%v, want ErrEffectOutcomeUnknown", err)
			}
			if _, err := repo.MarkEffectUnknown(context.Background(), sessiontree.MarkEffectUnknownRequest{
				Lease: lease, EffectAttemptID: dispatching.Attempt.EffectAttemptID, RequestFingerprint: "request-d",
				OutcomeFingerprint: "unknown-d", Now: now,
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := repo.FinishTurn(context.Background(), finish); err != nil {
				t.Fatal(err)
			}
			if _, err := repo.RejectEffectAttempt(context.Background(), sessiontree.RejectEffectAttemptRequest{
				Lease: lease, EffectAttemptID: prepared.Attempt.EffectAttemptID, RequestFingerprint: "request-p",
				RejectionCode: "late", RejectionFingerprint: "late", Now: now,
			}); !errors.Is(err, sessiontree.ErrStaleAuthority) {
				t.Fatalf("prepared attempt retained dispatch authority after turn finish: %v", err)
			}
		})
	}
}

func seedEffectTurn(t *testing.T, repo effectAuthorityTestRepo, now time.Time, threadID, turnID, runID string) sessiontree.TurnLease {
	t.Helper()
	ctx := context.Background()
	if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: threadID, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	admitted, err := repo.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
		ThreadID: threadID, TurnID: turnID, RunID: runID, OwnerID: "owner",
		Input: session.Message{Role: session.User, Content: "work"}, RequestFingerprint: "admit-" + turnID, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	return admitted.Lease
}

func effectPrepareRequest(lease sessiontree.TurnLease, runID, callID, argumentHash, fingerprint string, now time.Time) sessiontree.PrepareEffectAttemptRequest {
	return sessiontree.PrepareEffectAttemptRequest{
		Lease: lease, RequestFingerprint: fingerprint, Now: now,
		Invocation: sessiontree.EffectInvocationIdentity{
			ThreadID: lease.ThreadID, TurnID: lease.TurnID, RunID: runID, ToolCallID: callID, ToolName: "tool", ArgumentHash: argumentHash,
		},
	}
}

func effectResultEntry(threadID, turnID, callID, toolName, text string) sessiontree.Entry {
	return sessiontree.Entry{
		ThreadID: threadID, TurnID: turnID, Type: sessiontree.EntryToolResult,
		Message: session.Message{Role: session.Tool, ToolCallID: callID, ToolName: toolName, Content: text, ToolResult: &session.ToolResultView{Status: "completed"}},
	}
}

func countEffectResults(entries []sessiontree.Entry, callID string) int {
	count := 0
	for _, entry := range entries {
		if entry.Type == sessiontree.EntryToolResult && entry.Message.ToolCallID == callID {
			count++
		}
	}
	return count
}

package sessiontree

import (
	"context"
	"errors"
	"testing"

	"github.com/floegence/floret/internal/session"
)

func TestMemoryReadCanonicalTurnUsesActivePathAdmissionAuthority(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}

	original, err := repo.AdmitTurn(ctx, AdmitTurnRequest{
		ThreadID: "thread", TurnID: "turn-original", RunID: "run-original", OwnerID: "owner-original",
		RequestFingerprint: "request-original", Input: session.Message{Role: session.User, Content: "original question"},
	})
	if err != nil {
		t.Fatal(err)
	}
	originalFinish, err := repo.FinishTurn(ctx, FinishTurnRequest{
		Lease: original.Lease, RunID: "run-original", TerminalEntryID: "original-terminal", Status: TurnCompleted,
		OutcomeFingerprint: "outcome-original",
	})
	if err != nil {
		t.Fatal(err)
	}
	assertCanonicalTurnRead(t, repo, "turn-original", "run-original", nil)

	retry, err := repo.AdmitTurn(ctx, AdmitTurnRequest{
		ThreadID: "thread", TurnID: "turn-retry", RunID: "run-retry", OwnerID: "owner-retry",
		RequestFingerprint: "request-retry", RetrySourceTurnID: "turn-original", RetrySourceEntryID: original.UserMessage.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.FinishTurn(ctx, FinishTurnRequest{
		Lease: retry.Lease, RunID: "run-retry", TerminalEntryID: "retry-terminal", Status: TurnCompleted,
		OutcomeFingerprint: "outcome-retry",
	}); err != nil {
		t.Fatal(err)
	}
	assertCanonicalTurnRead(t, repo, "turn-retry", "run-retry", &CanonicalTurnRetrySource{TurnID: "turn-original", EntryID: original.UserMessage.ID})
	retryKey := turnAdmissionKey("thread", "turn-retry")
	retryAdmission := repo.turnAdmissions[retryKey]
	delete(repo.turnAdmissions, retryKey)
	if _, err := repo.ReadCanonicalTurn(ctx, "thread", "turn-retry"); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("retry without admission err=%v, want ErrAuthorityCorrupt", err)
	}
	repo.turnAdmissions[retryKey] = retryAdmission

	if err := repo.MoveLeaf(ctx, "thread", originalFinish.Terminal.ID); err != nil {
		t.Fatal(err)
	}
	branch, err := repo.AdmitTurn(ctx, AdmitTurnRequest{
		ThreadID: "thread", TurnID: "turn-branch", RunID: "run-branch", OwnerID: "owner-branch",
		RequestFingerprint: "request-branch", Input: session.Message{Role: session.User, Content: "branch question"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.FinishTurn(ctx, FinishTurnRequest{
		Lease: branch.Lease, RunID: "run-branch", TerminalEntryID: "branch-terminal", Status: TurnCompleted,
		OutcomeFingerprint: "outcome-branch",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.ReadCanonicalTurn(ctx, "thread", "turn-retry"); !errors.Is(err, ErrCanonicalTurnNotFound) {
		t.Fatalf("abandoned retry err=%v, want ErrCanonicalTurnNotFound", err)
	}
	assertCanonicalTurnRead(t, repo, "turn-branch", "run-branch", nil)
	branchKey := turnAdmissionKey("thread", "turn-branch")
	branchAdmission := repo.turnAdmissions[branchKey]
	branchAdmission.RunID = "run-drift"
	repo.turnAdmissions[branchKey] = branchAdmission
	if _, err := repo.ReadCanonicalTurn(ctx, "thread", "turn-branch"); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("mismatched normal execution admission err=%v, want ErrAuthorityCorrupt", err)
	}
}

func TestMemoryReadCanonicalTurnDistinguishesNotFoundFromCorruption(t *testing.T) {
	ctx := context.Background()

	t.Run("unknown", func(t *testing.T) {
		repo := NewMemoryRepo()
		if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
			t.Fatal(err)
		}
		if _, err := repo.ReadCanonicalTurn(ctx, "thread", "missing"); !errors.Is(err, ErrCanonicalTurnNotFound) {
			t.Fatalf("err=%v, want ErrCanonicalTurnNotFound", err)
		}
	})

	t.Run("marker only", func(t *testing.T) {
		repo := NewMemoryRepo()
		if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
			t.Fatal(err)
		}
		if _, err := AppendTurnMarker(ctx, repo, "thread", "turn", TurnStarted, map[string]string{"run_id": "run"}); err != nil {
			t.Fatal(err)
		}
		if _, err := repo.ReadCanonicalTurn(ctx, "thread", "turn"); !errors.Is(err, ErrCanonicalTurnNotFound) {
			t.Fatalf("err=%v, want ErrCanonicalTurnNotFound", err)
		}
	})

	t.Run("journal without started marker", func(t *testing.T) {
		repo := NewMemoryRepo()
		if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
			t.Fatal(err)
		}
		if _, err := AppendMessage(ctx, repo, "thread", "turn", session.Message{Role: session.User, Content: "input"}); err != nil {
			t.Fatal(err)
		}
		if _, err := repo.ReadCanonicalTurn(ctx, "thread", "turn"); !errors.Is(err, ErrAuthorityCorrupt) {
			t.Fatalf("err=%v, want ErrAuthorityCorrupt", err)
		}
	})

	t.Run("normal journal with duplicate terminal", func(t *testing.T) {
		repo := NewMemoryRepo()
		if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
			t.Fatal(err)
		}
		if _, err := AppendTurnMarker(ctx, repo, "thread", "turn", TurnStarted, map[string]string{"run_id": "run"}); err != nil {
			t.Fatal(err)
		}
		if _, err := AppendMessage(ctx, repo, "thread", "turn", session.Message{Role: session.User, Content: "input"}); err != nil {
			t.Fatal(err)
		}
		for range 2 {
			if _, err := AppendTurnMarker(ctx, repo, "thread", "turn", TurnCompleted, map[string]string{"run_id": "run"}); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := repo.ReadCanonicalTurn(ctx, "thread", "turn"); !errors.Is(err, ErrAuthorityCorrupt) {
			t.Fatalf("exact err=%v, want ErrAuthorityCorrupt", err)
		}
		if _, err := repo.ListCanonicalTurns(ctx, ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 1}); !errors.Is(err, ErrAuthorityCorrupt) {
			t.Fatalf("list err=%v, want ErrAuthorityCorrupt", err)
		}
	})

	t.Run("admission without journal", func(t *testing.T) {
		repo := NewMemoryRepo()
		if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
			t.Fatal(err)
		}
		repo.turnAdmissions[turnAdmissionKey("thread", "turn")] = turnAdmissionLedger{ThreadID: "thread", TurnID: "turn", RunID: "run"}
		if _, err := repo.ReadCanonicalTurn(ctx, "thread", "turn"); !errors.Is(err, ErrAuthorityCorrupt) {
			t.Fatalf("err=%v, want ErrAuthorityCorrupt", err)
		}
	})

	t.Run("normal journal is admitted without execution ledger", func(t *testing.T) {
		repo := NewMemoryRepo()
		if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
			t.Fatal(err)
		}
		if _, err := repo.AdmitTurn(ctx, AdmitTurnRequest{
			ThreadID: "thread", TurnID: "turn", RunID: "run", OwnerID: "owner", RequestFingerprint: "request",
			Input: session.Message{Role: session.User, Content: "input"},
		}); err != nil {
			t.Fatal(err)
		}
		delete(repo.turnAdmissions, turnAdmissionKey("thread", "turn"))
		if read, err := repo.ReadCanonicalTurn(ctx, "thread", "turn"); err != nil || read.Turn.TurnID != "turn" {
			t.Fatalf("read=%#v err=%v", read, err)
		}
	})
}

func TestMemoryReadCanonicalTurnRejectsNonLatestRetrySourceOutsideAncestorPath(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	original, err := repo.AdmitTurn(ctx, AdmitTurnRequest{
		ThreadID: "thread", TurnID: "original", RunID: "original-run", OwnerID: "original-owner",
		RequestFingerprint: "original-request", Input: session.Message{Role: session.User, Content: "original"},
	})
	if err != nil {
		t.Fatal(err)
	}
	originalFinish, err := repo.FinishTurn(ctx, FinishTurnRequest{
		Lease: original.Lease, RunID: "original-run", TerminalEntryID: "original-terminal",
		Status: TurnCompleted, OutcomeFingerprint: "original-outcome",
	})
	if err != nil {
		t.Fatal(err)
	}
	offPath, err := repo.AdmitTurn(ctx, AdmitTurnRequest{
		ThreadID: "thread", TurnID: "off-path", RunID: "off-path-run", OwnerID: "off-path-owner",
		RequestFingerprint: "off-path-request", Input: session.Message{Role: session.User, Content: "off path"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.FinishTurn(ctx, FinishTurnRequest{
		Lease: offPath.Lease, RunID: "off-path-run", TerminalEntryID: "off-path-terminal",
		Status: TurnCompleted, OutcomeFingerprint: "off-path-outcome",
	}); err != nil {
		t.Fatal(err)
	}
	if err := repo.MoveLeaf(ctx, "thread", originalFinish.Terminal.ID); err != nil {
		t.Fatal(err)
	}
	retry, err := repo.AdmitTurn(ctx, AdmitTurnRequest{
		ThreadID: "thread", TurnID: "retry", RunID: "retry-run", OwnerID: "retry-owner",
		RequestFingerprint: "retry-request", RetrySourceTurnID: "original", RetrySourceEntryID: original.UserMessage.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.FinishTurn(ctx, FinishTurnRequest{
		Lease: retry.Lease, RunID: "retry-run", TerminalEntryID: "retry-terminal",
		Status: TurnCompleted, OutcomeFingerprint: "retry-outcome",
	}); err != nil {
		t.Fatal(err)
	}
	newer, err := repo.AdmitTurn(ctx, AdmitTurnRequest{
		ThreadID: "thread", TurnID: "newer", RunID: "newer-run", OwnerID: "newer-owner",
		RequestFingerprint: "newer-request", Input: session.Message{Role: session.User, Content: "newer"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.FinishTurn(ctx, FinishTurnRequest{
		Lease: newer.Lease, RunID: "newer-run", TerminalEntryID: "newer-terminal",
		Status: TurnCompleted, OutcomeFingerprint: "newer-outcome",
	}); err != nil {
		t.Fatal(err)
	}

	admission := repo.turnAdmissions[turnAdmissionKey("thread", "retry")]
	admission.BaseLeafID = offPath.UserMessage.ID
	repo.turnAdmissions[turnAdmissionKey("thread", "retry")] = admission
	for index := range repo.entries["thread"] {
		entry := &repo.entries["thread"][index]
		if entry.ID != retry.TurnStarted.ID {
			continue
		}
		entry.Metadata[RetrySourceTurnIDMetadataKey] = "off-path"
		entry.Metadata[RetrySourceEntryIDMetadataKey] = offPath.UserMessage.ID
		entry.Raw = RawForEntry(*entry)
		entry.RawHash = StableHash(entry.Raw)
	}
	if _, err := repo.ReadCanonicalTurn(ctx, "thread", "retry"); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("non-latest retry with non-ancestor source err=%v, want ErrAuthorityCorrupt", err)
	}
}

func TestMemoryReadCanonicalTurnRejectsCorruptActiveLeafPath(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name   string
		mutate func(*MemoryRepo, Entry)
	}{
		{name: "missing leaf", mutate: func(repo *MemoryRepo, _ Entry) {
			meta := repo.threads["thread"]
			meta.LeafID = "missing-leaf"
			repo.threads["thread"] = meta
		}},
		{name: "broken parent chain", mutate: func(repo *MemoryRepo, terminal Entry) {
			for index := range repo.entries["thread"] {
				if repo.entries["thread"][index].ID == terminal.ID {
					repo.entries["thread"][index].ParentID = "missing-parent"
					break
				}
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			repo := NewMemoryRepo()
			if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
				t.Fatal(err)
			}
			admitted, err := repo.AdmitTurn(ctx, AdmitTurnRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", OwnerID: "owner", RequestFingerprint: "request", Input: session.Message{Role: session.User, Content: "input"}})
			if err != nil {
				t.Fatal(err)
			}
			finished, err := repo.FinishTurn(ctx, FinishTurnRequest{Lease: admitted.Lease, RunID: "run", TerminalEntryID: "terminal", Status: TurnCompleted, OutcomeFingerprint: "outcome"})
			if err != nil {
				t.Fatal(err)
			}
			test.mutate(repo, finished.Terminal)
			_, err = repo.ReadCanonicalTurn(ctx, "thread", "turn")
			if !errors.Is(err, ErrAuthorityCorrupt) || errors.Is(err, ErrCanonicalTurnNotFound) {
				t.Fatalf("corrupt active leaf path err=%v, want ErrAuthorityCorrupt only", err)
			}
		})
	}
}

func assertCanonicalTurnRead(t *testing.T, repo CanonicalTurnReadRepo, turnID, runID string, retrySource *CanonicalTurnRetrySource) CanonicalTurnRead {
	t.Helper()
	read, err := repo.ReadCanonicalTurn(context.Background(), "thread", turnID)
	if err != nil {
		t.Fatal(err)
	}
	if read.Turn.TurnID != turnID || read.Turn.RunID != runID || read.ThroughOrdinal < read.Turn.StartedOrdinal || read.LatestTurn.TurnID == "" {
		t.Fatalf("canonical read=%#v", read)
	}
	if !sameCanonicalTurnRetrySource(read.Turn.RetrySource, retrySource) {
		t.Fatalf("retry source=%#v, want %#v", read.Turn.RetrySource, retrySource)
	}
	return read
}

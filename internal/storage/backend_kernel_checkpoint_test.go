package storage

import (
	"context"
	"testing"
	"time"

	"github.com/floegence/floret/v5/internal/session"
	"github.com/floegence/floret/v5/internal/sessiontree"
	"github.com/floegence/floret/v5/internal/storagebridge"
	publicstorage "github.com/floegence/floret/v5/storage"
	"github.com/floegence/floret/v5/storage/spi"
)

func TestFinishTurnCommitsCanonicalCheckpointAndClearsRecoveryJournal(t *testing.T) {
	ctx := context.Background()
	backend, err := storagebridge.Open(ctx, storagebridge.Source(publicstorage.Memory()))
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	now := time.Date(2026, 8, 25, 2, 0, 0, 0, time.UTC)
	kernel, err := NewBackendKernel(ctx, backend, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kernel.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := kernel.AcceptTurn(ctx, sessiontree.AcceptTurnRequest{
		ThreadID: "thread", TurnID: "turn", RunID: "run", LogicalRequestID: "request",
		RequestFingerprint: "turn-fingerprint", InputRequestFingerprint: "input-fingerprint",
		Input: session.Message{Role: session.User, Content: "hello"}, Now: now,
	}); err != nil {
		t.Fatal(err)
	}
	if count := recoveryJournalRecordCount(t, ctx, backend); count == 0 {
		t.Fatal("active turn did not leave recovery journal frames")
	}
	if _, err := kernel.FinishTurn(ctx, sessiontree.FinishTurnRequest{
		ThreadID: "thread", TurnID: "turn", RunID: "run", TerminalEntryID: "terminal",
		Status: sessiontree.TurnCompleted, OutcomeFingerprint: "completed-fingerprint", Now: now.Add(time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	if count := recoveryJournalRecordCount(t, ctx, backend); count != 0 {
		t.Fatalf("terminal checkpoint left %d recovery journal frames", count)
	}
	reopened, err := NewBackendKernel(ctx, backend, func() time.Time { return now.Add(2 * time.Second) })
	if err != nil {
		t.Fatal(err)
	}
	meta, err := reopened.Thread(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if meta.LeafID != "terminal" {
		t.Fatalf("reopened leaf=%q, want terminal", meta.LeafID)
	}
}

func recoveryJournalRecordCount(t *testing.T, ctx context.Context, backend spi.Backend) int {
	t.Helper()
	count := 0
	if err := backend.View(ctx, func(tx spi.ReadTx) error {
		var after []byte
		for {
			page, err := tx.Scan(spi.ScanRequest{Namespace: "floret.domain.sessiontree.journal.v1", After: after, Limit: 256})
			if err != nil {
				return err
			}
			count += len(page.Records)
			if !page.HasMore {
				return nil
			}
			after = page.Next
		}
	}); err != nil {
		t.Fatal(err)
	}
	return count
}

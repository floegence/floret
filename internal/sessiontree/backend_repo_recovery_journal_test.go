package sessiontree_test

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/v3/internal/session"
	"github.com/floegence/floret/v3/internal/sessiontree"
	"github.com/floegence/floret/v3/internal/storagebridge"
	"github.com/floegence/floret/v3/internal/storagecodec"
	publicstorage "github.com/floegence/floret/v3/storage"
	"github.com/floegence/floret/v3/storage/spi"
)

const (
	testBackendDomainNamespace        = "floret.domain"
	testBackendDomainJournalNamespace = "floret.domain.sessiontree.journal.v1"
)

var testBackendStateKey = storagecodec.Tuple(storagecodec.TupleString("sessiontree"), storagecodec.TupleString("state"))

type viewCountingBackend struct {
	spi.Backend
	views atomic.Int32
}

func (backend *viewCountingBackend) View(ctx context.Context, read func(spi.ReadTx) error) error {
	backend.views.Add(1)
	return backend.Backend.View(ctx, read)
}

func TestBackendRepoSemanticMutationAppendsJournalWithoutRewritingCheckpoint(t *testing.T) {
	ctx := context.Background()
	backend, err := storagebridge.Open(ctx, storagebridge.Source(publicstorage.SQLite(filepath.Join(t.TempDir(), "journal.db"))))
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	repo, err := sessiontree.NewBackendRepo(ctx, backend, sessiontree.DefaultLeasePolicy, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	checkpointBefore := readBackendRecord(t, backend, testBackendDomainNamespace, testBackendStateKey)

	if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread-journal"}); err != nil {
		t.Fatal(err)
	}
	checkpointAfter := readBackendRecord(t, backend, testBackendDomainNamespace, testBackendStateKey)
	if !bytes.Equal(checkpointAfter, checkpointBefore) {
		t.Fatal("semantic mutation rewrote the complete session-tree checkpoint")
	}
	page := scanBackendNamespace(t, backend, testBackendDomainJournalNamespace)
	if len(page.Records) != 1 {
		t.Fatalf("journal records = %d, want one semantic frame", len(page.Records))
	}
}

func TestBackendRepoJournalFrameScalesWithSemanticChangeNotCheckpoint(t *testing.T) {
	ctx := context.Background()
	backend, err := storagebridge.Open(ctx, storagebridge.Source(publicstorage.SQLite(filepath.Join(t.TempDir(), "bounded-journal.db"))))
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	repo, err := sessiontree.NewBackendRepo(ctx, backend, sessiontree.DefaultLeasePolicy, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 100; index++ {
		if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: fmt.Sprintf("thread-%03d", index)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := repo.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}
	checkpoint := readBackendRecord(t, backend, testBackendDomainNamespace, testBackendStateKey)
	if _, err := repo.SetThreadTitle(ctx, sessiontree.SetThreadTitleRequest{
		ThreadID: "thread-050", Title: "bounded semantic frame", Now: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	page := scanBackendNamespace(t, backend, testBackendDomainJournalNamespace)
	if len(page.Records) != 1 {
		t.Fatalf("journal records = %d, want one", len(page.Records))
	}
	if frameSize, checkpointSize := len(page.Records[0].Value), len(checkpoint); frameSize >= checkpointSize/4 {
		t.Fatalf("journal frame size=%d, checkpoint size=%d; frame scales with full state", frameSize, checkpointSize)
	}
}

func TestBackendRepoCheckpointStartsFreshReplaySequence(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "checkpoint-sequence.db")
	backend, err := storagebridge.Open(ctx, storagebridge.Source(publicstorage.SQLite(path)))
	if err != nil {
		t.Fatal(err)
	}
	repo, err := sessiontree.NewBackendRepo(ctx, backend, sessiontree.DefaultLeasePolicy, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread-checkpoint-sequence"}); err != nil {
		t.Fatal(err)
	}
	if err := repo.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}
	if page := scanBackendNamespace(t, backend, testBackendDomainJournalNamespace); len(page.Records) != 0 {
		t.Fatalf("journal records after checkpoint = %d, want zero", len(page.Records))
	}
	if _, err := repo.SetThreadTitle(ctx, sessiontree.SetThreadTitleRequest{
		ThreadID: "thread-checkpoint-sequence", Title: "after checkpoint", Now: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	page := scanBackendNamespace(t, backend, testBackendDomainJournalNamespace)
	if len(page.Records) != 1 || !bytes.Equal(page.Records[0].Key, testBackendJournalKey(1)) {
		t.Fatalf("journal after checkpoint = %#v, want a fresh sequence one", page.Records)
	}
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}

	restartedBackend, err := storagebridge.Open(ctx, storagebridge.Source(publicstorage.SQLite(path)))
	if err != nil {
		t.Fatal(err)
	}
	defer restartedBackend.Close()
	restarted, err := sessiontree.NewBackendRepo(ctx, restartedBackend, sessiontree.DefaultLeasePolicy, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	thread, err := restarted.Thread(ctx, "thread-checkpoint-sequence")
	if err != nil {
		t.Fatal(err)
	}
	if thread.Title != "after checkpoint" {
		t.Fatalf("replayed title = %q, want %q", thread.Title, "after checkpoint")
	}
}

func TestBackendRepoLiveRevisionReadDoesNotEnterBackend(t *testing.T) {
	ctx := context.Background()
	inner, err := storagebridge.Open(ctx, storagebridge.Source(publicstorage.Memory()))
	if err != nil {
		t.Fatal(err)
	}
	backend := &viewCountingBackend{Backend: inner}
	defer backend.Close()
	repo, err := sessiontree.NewBackendRepo(ctx, backend, sessiontree.DefaultLeasePolicy, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread-live-revision"}); err != nil {
		t.Fatal(err)
	}
	backend.views.Store(0)
	revision, err := repo.CurrentThreadRevision(ctx, "thread-live-revision")
	if err != nil || revision <= 0 {
		t.Fatalf("live revision=%d err=%v", revision, err)
	}
	if views := backend.views.Load(); views != 0 {
		t.Fatalf("live revision opened %d backend views, want memory-only hot path", views)
	}
}

func TestBackendRepoRebuildsFromCheckpointAndJournalAfterCrash(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "restart.db")
	backend, err := storagebridge.Open(ctx, storagebridge.Source(publicstorage.SQLite(path)))
	if err != nil {
		t.Fatal(err)
	}
	repo, err := sessiontree.NewBackendRepo(ctx, backend, sessiontree.DefaultLeasePolicy, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	created, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread-restart"})
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}

	restartedBackend, err := storagebridge.Open(ctx, storagebridge.Source(publicstorage.SQLite(path)))
	if err != nil {
		t.Fatal(err)
	}
	defer restartedBackend.Close()
	restarted, err := sessiontree.NewBackendRepo(ctx, restartedBackend, sessiontree.DefaultLeasePolicy, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	got, err := restarted.Thread(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != created.ID {
		t.Fatalf("restarted thread = %#v, want stable identity %q", got, created.ID)
	}
}

func TestBackendRepoIgnoresOnlyTornJournalTail(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "torn-tail.db")
	backend, err := storagebridge.Open(ctx, storagebridge.Source(publicstorage.SQLite(path)))
	if err != nil {
		t.Fatal(err)
	}
	repo, err := sessiontree.NewBackendRepo(ctx, backend, sessiontree.DefaultLeasePolicy, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread-before-torn-tail"}); err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(ctx, func(tx spi.WriteTx) error {
		return tx.Put(testBackendDomainJournalNamespace, testBackendJournalKey(2), []byte(`{"version":1,"sequence":2`))
	}); err != nil {
		t.Fatal(err)
	}
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}

	restartedBackend, err := storagebridge.Open(ctx, storagebridge.Source(publicstorage.SQLite(path)))
	if err != nil {
		t.Fatal(err)
	}
	defer restartedBackend.Close()
	restarted, err := sessiontree.NewBackendRepo(ctx, restartedBackend, sessiontree.DefaultLeasePolicy, time.Now)
	if err != nil {
		t.Fatalf("restart with torn final frame: %v", err)
	}
	if _, err := restarted.Thread(ctx, "thread-before-torn-tail"); err != nil {
		t.Fatalf("canonical state before torn tail was lost: %v", err)
	}
}

func TestBackendRepoRejectsCorruptJournalBeforeTail(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "corrupt-middle.db")
	backend, err := storagebridge.Open(ctx, storagebridge.Source(publicstorage.SQLite(path)))
	if err != nil {
		t.Fatal(err)
	}
	repo, err := sessiontree.NewBackendRepo(ctx, backend, sessiontree.DefaultLeasePolicy, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread-first"}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread-second"}); err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(ctx, func(tx spi.WriteTx) error {
		return tx.Put(testBackendDomainJournalNamespace, testBackendJournalKey(1), []byte(`{"version":1,"sequence":1`))
	}); err != nil {
		t.Fatal(err)
	}
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}

	restartedBackend, err := storagebridge.Open(ctx, storagebridge.Source(publicstorage.SQLite(path)))
	if err != nil {
		t.Fatal(err)
	}
	defer restartedBackend.Close()
	if _, err := sessiontree.NewBackendRepo(ctx, restartedBackend, sessiontree.DefaultLeasePolicy, time.Now); err == nil {
		t.Fatal("restart accepted a corrupt journal frame before a later committed frame")
	}
}

func TestBackendRepoRecoveryJournalKeepsEffectIntentExactlyOnce(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "effect-intent.db")
	backend, err := storagebridge.Open(ctx, storagebridge.Source(publicstorage.SQLite(path)))
	if err != nil {
		t.Fatal(err)
	}
	repo, err := sessiontree.NewBackendRepo(ctx, backend, sessiontree.DefaultLeasePolicy, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread-effect"}); err != nil {
		t.Fatal(err)
	}
	if err := repo.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}
	admitted, err := repo.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
		ThreadID: "thread-effect", TurnID: "turn-effect", RunID: "run-effect", OwnerID: "owner-effect",
		Input: session.Message{Role: session.User, Content: "write once"}, RequestFingerprint: "admit-effect",
	})
	if err != nil {
		t.Fatal(err)
	}
	request := sessiontree.PrepareEffectAttemptRequest{
		Lease: admitted.Lease, RequestFingerprint: "effect-request",
		Invocation: sessiontree.EffectInvocationIdentity{
			ThreadID: "thread-effect", TurnID: "turn-effect", RunID: "run-effect",
			ToolCallID: "call-effect", ToolName: "write_file", ArgumentHash: "args-effect",
		},
	}
	prepared, err := repo.PrepareEffectAttempt(ctx, request)
	if err != nil || prepared.Replayed {
		t.Fatalf("prepare effect = %#v err=%v", prepared, err)
	}
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}

	restartedBackend, err := storagebridge.Open(ctx, storagebridge.Source(publicstorage.SQLite(path)))
	if err != nil {
		t.Fatal(err)
	}
	defer restartedBackend.Close()
	restarted, err := sessiontree.NewBackendRepo(ctx, restartedBackend, sessiontree.DefaultLeasePolicy, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	recoveredAdmission, found, err := restarted.ReadTurnAdmission(ctx, "thread-effect", "turn-effect", "run-effect")
	if err != nil || !found || recoveredAdmission.UserMessage.Message.Content != "write once" {
		t.Fatalf("recovered admission = %#v found=%v err=%v", recoveredAdmission, found, err)
	}
	request.Lease = recoveredAdmission.Lease
	replayed, err := restarted.PrepareEffectAttempt(ctx, request)
	if err != nil || !replayed.Replayed || replayed.Attempt.EffectAttemptID != prepared.Attempt.EffectAttemptID {
		t.Fatalf("replayed effect = %#v err=%v, want stable attempt %q", replayed, err, prepared.Attempt.EffectAttemptID)
	}
}

func TestBackendRepoRecoveryJournalKeepsApprovalRejectionExactlyOnce(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "approval-rejection.db")
	now := time.Date(2026, 8, 9, 10, 0, 0, 0, time.UTC)
	backend, err := storagebridge.Open(ctx, storagebridge.Source(publicstorage.SQLite(path)))
	if err != nil {
		t.Fatal(err)
	}
	repo, err := sessiontree.NewBackendRepo(ctx, backend, sessiontree.DefaultLeasePolicy, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread-reject"}); err != nil {
		t.Fatal(err)
	}
	if err := repo.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}
	admitted, err := repo.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
		ThreadID: "thread-reject", TurnID: "turn-reject", RunID: "run-reject", OwnerID: "owner-reject",
		Input: session.Message{Role: session.User, Content: "do not write"}, RequestFingerprint: "admit-reject", Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	invocation := sessiontree.EffectInvocationIdentity{
		ThreadID: "thread-reject", TurnID: "turn-reject", RunID: "run-reject",
		ToolCallID: "call-reject", ToolName: "write_file", ArgumentHash: "args-reject",
	}
	effectAttemptID := sessiontree.ApprovalEffectAttemptID(invocation)
	prepared, err := repo.PrepareApprovalBatch(ctx, sessiontree.PrepareApprovalBatchRequest{
		Lease: admitted.Lease,
		Items: []sessiontree.ApprovalPreflightItem{{
			EffectAttemptID: effectAttemptID, EffectRequestFingerprint: "effect-reject", ApprovalRequestFingerprint: "approval-reject",
			Invocation: invocation,
			RequestedEntry: sessiontree.Entry{
				ID: sessiontree.ApprovalRequestedEntryID(effectAttemptID), ThreadID: "thread-reject", TurnID: "turn-reject",
				Type: sessiontree.EntryCustom, Metadata: map[string]string{"approval_state": "requested"},
			},
			ToolKind: "local", Step: 1, BatchSize: 1,
			Resources: []sessiontree.ApprovalResource{{Kind: "file", Value: "notes.md"}}, Effects: []string{"write"}, Destructive: true,
		}},
		Now: now,
	})
	if err != nil || len(prepared.Approvals) != 1 {
		t.Fatalf("prepare approval = %#v err=%v", prepared, err)
	}
	record := prepared.Approvals[0]
	request := sessiontree.ResolveApprovalRequest{
		DecisionID: "decision-reject", ExpectedRootThreadID: "thread-reject", ExpectedGeneration: prepared.Queue.Generation,
		ExpectedRevision: prepared.Queue.Revision, ExpectedCurrent: record.Identity(), ExpectedApprovalRevision: record.Revision,
		Decision: sessiontree.ApprovalDecisionReject,
		RejectedEntry: sessiontree.Entry{
			ID: sessiontree.ApprovalRejectedEntryID("decision-reject", record.ApprovalID), ThreadID: "thread-reject", TurnID: "turn-reject",
			Type: sessiontree.EntryCustom, Metadata: map[string]string{"approval_state": "rejected"},
		},
		Now: now.Add(time.Second),
	}
	resolved, err := repo.ResolveApproval(ctx, request)
	if err != nil || resolved.Replayed || resolved.Approval.State != sessiontree.ApprovalRejected || resolved.Effect.State != sessiontree.EffectAttemptRejected {
		t.Fatalf("resolve rejection = %#v err=%v", resolved, err)
	}
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}

	restartedBackend, err := storagebridge.Open(ctx, storagebridge.Source(publicstorage.SQLite(path)))
	if err != nil {
		t.Fatal(err)
	}
	defer restartedBackend.Close()
	restarted, err := sessiontree.NewBackendRepo(ctx, restartedBackend, sessiontree.DefaultLeasePolicy, func() time.Time { return now.Add(2 * time.Second) })
	if err != nil {
		t.Fatal(err)
	}
	waited, err := restarted.WaitApprovalDecision(ctx, record.ApprovalID)
	if err != nil || waited.Approval.State != sessiontree.ApprovalRejected || waited.Receipt.DecisionID != "decision-reject" {
		t.Fatalf("recovered rejection = %#v err=%v", waited, err)
	}
	replayed, err := restarted.ResolveApproval(ctx, request)
	if err != nil || !replayed.Replayed || replayed.Approval.Revision != resolved.Approval.Revision || replayed.Effect.EffectAttemptID != effectAttemptID {
		t.Fatalf("replayed rejection = %#v err=%v", replayed, err)
	}
	entries, err := restarted.Entries(ctx, "thread-reject")
	if err != nil {
		t.Fatal(err)
	}
	rejectedEntries := 0
	for _, entry := range entries {
		if entry.ID == request.RejectedEntry.ID {
			rejectedEntries++
		}
	}
	if rejectedEntries != 1 {
		t.Fatalf("rejected entries = %d, want one canonical outcome", rejectedEntries)
	}
}

func testBackendJournalKey(sequence uint64) []byte {
	return []byte(fmt.Sprintf("%020d", sequence))
}

func readBackendRecord(t *testing.T, backend spi.Backend, namespace string, key []byte) []byte {
	t.Helper()
	var value []byte
	if err := backend.View(context.Background(), func(tx spi.ReadTx) error {
		var err error
		value, err = tx.Get(namespace, key)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return value
}

func scanBackendNamespace(t *testing.T, backend spi.Backend, namespace string) spi.ScanPage {
	t.Helper()
	var page spi.ScanPage
	if err := backend.View(context.Background(), func(tx spi.ReadTx) error {
		var err error
		page, err = tx.Scan(spi.ScanRequest{Namespace: namespace, Limit: 100})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return page
}

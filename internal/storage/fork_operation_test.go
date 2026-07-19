package storage

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/session/artifact"
	"github.com/floegence/floret/internal/sessiontree"
)

func TestMemoryForkOperationCommitIsAllNodeAtomic(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	leaves := map[string]string{}
	if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "source-a"}); err != nil {
		t.Fatal(err)
	}
	rootEntry, err := sessiontree.AppendMessage(ctx, repo, "source-a", "seed", session.Message{Role: session.User, Content: "source-a"})
	if err != nil {
		t.Fatal(err)
	}
	leaves["source-a"] = rootEntry.ID
	childMeta, childEntry, err := repo.CreateThreadWithInitialEntry(ctx, sessiontree.ThreadMeta{
		ID: "source-b", ParentThreadID: "source-a", ParentTurnID: "parent-turn", TaskName: "child", AgentPath: "child",
	}, sessiontree.Entry{Type: sessiontree.EntryCustom, Message: session.Message{Role: session.User, Content: "source-b"}})
	if err != nil {
		t.Fatal(err)
	}
	leaves["source-b"] = childEntry.ID
	childMeta.Lifecycle = sessiontree.ThreadLifecycleClosed
	if err := repo.UpdateThread(ctx, childMeta); err != nil {
		t.Fatal(err)
	}
	operations := NewMemoryForkOperationStore(repo)
	now := time.Date(2026, 7, 19, 10, 0, 0, 0, time.UTC)
	planRecord := ForkOperationPlan{
		Version: ForkOperationPlanVersion, OperationID: "fork-operation", RequestFingerprint: "fork-fingerprint", PreparedAt: now,
		Root: ForkOperationPlanNode{
			NodeID: "root", SourceThreadID: "source-a", SourceEntryID: leaves["source-a"], SourceLeafEntryID: leaves["source-a"], DestinationThreadID: "destination-a",
			ArtifactClosure: emptyForkArtifactClosure(t, "source-a", "destination-a"),
		},
		TerminalChildren: []ForkOperationPlanNode{{
			NodeID: "terminal-child-1", SourceThreadID: "source-b", SourceEntryID: leaves["source-b"], SourceLeafEntryID: leaves["source-b"], DestinationThreadID: "destination-b",
			ArtifactClosure: emptyForkArtifactClosure(t, "source-b", "destination-b"),
			DestinationMeta: &sessiontree.ForkDestinationMeta{ParentThreadID: "destination-a", ParentTurnID: "parent-turn", TaskName: "child", AgentPath: "child", Lifecycle: sessiontree.ThreadLifecycleClosed},
		}},
	}
	plan := mustMarshalForkOperationPlan(t, planRecord)
	if _, created, err := operations.PrepareForkOperation(ctx, ForkOperationRecord{
		OperationID: "fork-operation", RequestFingerprint: "fork-fingerprint",
		SourceThreadIDs:    []string{"source-a", "source-b"},
		AuthorityThreadIDs: []string{"source-a", "destination-a", "source-b", "destination-b"},
		State:              ForkOperationPrepared, Plan: plan, CreatedAt: now, UpdatedAt: now,
	}); err != nil || !created {
		t.Fatalf("PrepareForkOperation created=%v err=%v", created, err)
	}
	nodes := ForkOperationPlanNodes(planRecord)
	injected := errors.New("injected second-node rewrite failure")
	failing := append([]sessiontree.ForkOptions(nil), nodes...)
	failing[1].RewriteEntry = func(sessiontree.Entry, sessiontree.ForkEntryIdentity) (sessiontree.Entry, error) {
		return sessiontree.Entry{}, injected
	}
	request := ForkOperationCommitRequest{
		OperationID: "fork-operation", RequestFingerprint: "fork-fingerprint", Plan: plan,
		Nodes: failing, Result: []byte(`{"operation_id":"fork-operation","thread_id":"destination-a"}`), FinishedAt: now.Add(time.Minute),
	}
	if _, _, err := operations.CommitForkOperation(ctx, request); !errors.Is(err, injected) {
		t.Fatalf("CommitForkOperation error = %v, want injected failure", err)
	}
	for _, threadID := range []string{"destination-a", "destination-b"} {
		if _, err := repo.Thread(ctx, threadID); !errors.Is(err, sessiontree.ErrThreadNotFound) {
			t.Fatalf("destination %q visible after failed batch: %v", threadID, err)
		}
	}
	if _, err := sessiontree.AppendMessage(ctx, repo, "source-a", "blocked", session.Message{Role: session.User, Content: "blocked"}); !errors.Is(err, sessiontree.ErrThreadAuthorityBusy) {
		t.Fatalf("prepared claim was released after transient failure: %v", err)
	}

	request.Nodes = nodes
	committed, replayed, err := operations.CommitForkOperation(ctx, request)
	if err != nil || replayed || committed.State != ForkOperationCompleted {
		t.Fatalf("CommitForkOperation record=%#v replayed=%v err=%v", committed, replayed, err)
	}
	for _, threadID := range []string{"destination-a", "destination-b"} {
		if _, err := repo.Thread(ctx, threadID); err != nil {
			t.Fatalf("destination %q missing after commit: %v", threadID, err)
		}
	}
	if _, replayed, err := operations.CommitForkOperation(ctx, request); err != nil || !replayed {
		t.Fatalf("completed replay replayed=%v err=%v", replayed, err)
	}
}

func TestMemoryForkOperationFailureReleasesOnlyUnpublishedClaims(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "source"}); err != nil {
		t.Fatal(err)
	}
	operations := NewMemoryForkOperationStore(repo)
	now := time.Date(2026, 7, 19, 11, 0, 0, 0, time.UTC)
	plan := mustMarshalForkOperationPlan(t, ForkOperationPlan{
		Version: ForkOperationPlanVersion, OperationID: "failed-operation", RequestFingerprint: "fingerprint", PreparedAt: now,
		Root: ForkOperationPlanNode{NodeID: "root", SourceThreadID: "source", DestinationThreadID: "released-destination", ArtifactClosure: emptyForkArtifactClosure(t, "source", "released-destination")},
	})
	if _, created, err := operations.PrepareForkOperation(ctx, ForkOperationRecord{
		OperationID: "failed-operation", RequestFingerprint: "fingerprint",
		SourceThreadIDs: []string{"source"}, AuthorityThreadIDs: []string{"source", "released-destination"},
		State: ForkOperationPrepared, Plan: plan, CreatedAt: now, UpdatedAt: now,
	}); err != nil || !created {
		t.Fatalf("PrepareForkOperation created=%v err=%v", created, err)
	}
	failed, replayed, err := operations.FailForkOperation(ctx, ForkOperationFailureRequest{
		OperationID: "failed-operation", RequestFingerprint: "fingerprint",
		ErrorCode: "destination_conflict", ErrorMessage: "deterministic conflict", FinishedAt: now.Add(time.Minute),
	})
	if err != nil || replayed || failed.State != ForkOperationFailed {
		t.Fatalf("FailForkOperation record=%#v replayed=%v err=%v", failed, replayed, err)
	}
	if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "released-destination"}); err != nil {
		t.Fatalf("failed operation did not release unpublished destination: %v", err)
	}
	if _, replayed, err := operations.FailForkOperation(ctx, ForkOperationFailureRequest{
		OperationID: "failed-operation", RequestFingerprint: "fingerprint",
		ErrorCode: "destination_conflict", ErrorMessage: "deterministic conflict", FinishedAt: now.Add(2 * time.Minute),
	}); err != nil || !replayed {
		t.Fatalf("failed replay replayed=%v err=%v", replayed, err)
	}
}

func TestMemoryPrepareForkOperationRejectsSourceLeafDriftWithoutPublishingClaims(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "source"}); err != nil {
		t.Fatal(err)
	}
	pinned, err := sessiontree.AppendMessage(ctx, repo, "source", "turn-1", session.Message{Role: session.User, Content: "pinned"})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 19, 11, 30, 0, 0, time.UTC)
	plan := mustMarshalForkOperationPlan(t, ForkOperationPlan{
		Version: ForkOperationPlanVersion, OperationID: "stale-operation", RequestFingerprint: "stale-fingerprint", PreparedAt: now,
		Root: ForkOperationPlanNode{
			NodeID: "root", SourceThreadID: "source", SourceEntryID: pinned.ID, SourceLeafEntryID: pinned.ID, DestinationThreadID: "destination",
			ArtifactClosure: emptyForkArtifactClosure(t, "source", "destination"),
		},
	})
	if _, err := sessiontree.AppendMessage(ctx, repo, "source", "turn-2", session.Message{Role: session.User, Content: "new leaf"}); err != nil {
		t.Fatal(err)
	}
	operations := NewMemoryForkOperationStore(repo)
	_, created, err := operations.PrepareForkOperation(ctx, ForkOperationRecord{
		OperationID: "stale-operation", RequestFingerprint: "stale-fingerprint",
		SourceThreadIDs: []string{"source"}, AuthorityThreadIDs: []string{"source", "destination"},
		State: ForkOperationPrepared, Plan: plan, CreatedAt: now, UpdatedAt: now,
	})
	if !errors.Is(err, sessiontree.ErrStaleAuthority) || created {
		t.Fatalf("PrepareForkOperation created=%v err=%v, want ErrStaleAuthority", created, err)
	}
	if _, err := operations.ForkOperation(ctx, "stale-operation"); !errors.Is(err, ErrForkOperationNotFound) {
		t.Fatalf("rejected prepare stored operation: %v", err)
	}
	if _, err := repo.Thread(ctx, "destination"); !errors.Is(err, sessiontree.ErrThreadNotFound) {
		t.Fatalf("rejected prepare published destination: %v", err)
	}
	if _, err := sessiontree.AppendMessage(ctx, repo, "source", "turn-3", session.Message{Role: session.User, Content: "unclaimed"}); err != nil {
		t.Fatalf("rejected prepare retained source claim: %v", err)
	}
}

func emptyForkArtifactClosure(t *testing.T, sourceThreadID, destinationThreadID string) artifact.Closure {
	t.Helper()
	items := []artifact.ManifestItem{}
	fingerprint, err := artifact.ClosureFingerprint(sourceThreadID, destinationThreadID, items)
	if err != nil {
		t.Fatal(err)
	}
	return artifact.Closure{SourceThreadID: sourceThreadID, DestinationThreadID: destinationThreadID, Items: items, Fingerprint: fingerprint}
}

func mustMarshalForkOperationPlan(t *testing.T, plan ForkOperationPlan) []byte {
	t.Helper()
	data, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

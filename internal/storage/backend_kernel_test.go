package storage_test

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/floegence/floret/v3/internal/provider/cache"
	"github.com/floegence/floret/v3/internal/session"
	"github.com/floegence/floret/v3/internal/session/artifact"
	"github.com/floegence/floret/v3/internal/sessiontree"
	. "github.com/floegence/floret/v3/internal/storage"
	"github.com/floegence/floret/v3/internal/storagebridge"
	publicstorage "github.com/floegence/floret/v3/storage"
	"github.com/floegence/floret/v3/storage/spi"
)

func TestBackendKernelDeletesRootAndPromptScopeAtomically(t *testing.T) {
	for _, test := range backendKernelSources(t) {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			backend, kernel := openBackendKernel(t, test.source)
			defer backend.Close()
			if _, err := kernel.CreateRoot(ctx, sessiontree.CreateRootRequest{
				ThreadID: "root", CreateIntentID: "create-root", ContractVersion: "2",
				Meta: sessiontree.ThreadMeta{ID: "root"},
			}); err != nil {
				t.Fatal(err)
			}
			if err := kernel.AppendSegment(ctx, cache.Segment{ID: "segment", PromptScopeID: "root", Provider: "gateway", Model: "model"}); err != nil {
				t.Fatal(err)
			}
			if _, err := kernel.DeleteRootTree(ctx, "root"); err != nil {
				t.Fatal(err)
			}
			segments, err := kernel.Segments(ctx, "root", "gateway", "model")
			if err != nil {
				t.Fatal(err)
			}
			if len(segments) != 0 {
				t.Fatalf("deleted prompt scope retained segments: %#v", segments)
			}
			if _, err := kernel.Thread(ctx, "root"); !errors.Is(err, sessiontree.ErrThreadNotFound) {
				t.Fatalf("deleted root read error = %v", err)
			}
		})
	}
}

func TestBackendKernelApprovalWaitObservesCommittedDecision(t *testing.T) {
	for _, test := range backendKernelSources(t) {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			now := time.Date(2026, 7, 29, 14, 0, 0, 0, time.UTC)
			backend, kernel := openBackendKernel(t, test.source)
			defer backend.Close()
			if _, err := kernel.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread"}); err != nil {
				t.Fatal(err)
			}
			admitted, err := kernel.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
				ThreadID: "thread", TurnID: "turn", RunID: "run", OwnerID: "owner",
				Input: session.Message{Role: session.User, Content: "work"}, RequestFingerprint: "admit", Now: now,
			})
			if err != nil {
				t.Fatal(err)
			}
			prepared, err := kernel.PrepareApprovalBatch(ctx, backendApprovalPrepare(admitted.Lease, now))
			if err != nil {
				t.Fatal(err)
			}
			record := prepared.Approvals[0]
			type waitResult struct {
				result sessiontree.WaitApprovalDecisionResult
				err    error
			}
			done := make(chan waitResult, 1)
			go func() {
				result, err := kernel.WaitApprovalDecision(ctx, record.ApprovalID)
				done <- waitResult{result: result, err: err}
			}()
			resolved, err := kernel.ResolveApproval(ctx, sessiontree.ResolveApprovalRequest{
				DecisionID: "decision", ExpectedRootThreadID: "thread", ExpectedGeneration: prepared.Queue.Generation,
				ExpectedRevision: prepared.Queue.Revision, ExpectedCurrent: record.Identity(),
				ExpectedApprovalRevision: record.Revision, Decision: sessiontree.ApprovalDecisionApprove, Now: now.Add(time.Second),
			})
			if err != nil {
				t.Fatal(err)
			}
			select {
			case waited := <-done:
				if waited.err != nil || waited.result.Approval.DecisionID != resolved.Approval.DecisionID {
					t.Fatalf("waited=%#v err=%v", waited.result, waited.err)
				}
			case <-time.After(time.Second):
				t.Fatal("approval waiter was not notified")
			}
		})
	}
}

func TestBackendKernelForkPrepareReplaysExactRequestAndRejectsConflict(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 29, 15, 0, 0, 0, time.UTC)
	backend, kernel := openBackendKernel(t, publicstorage.Memory())
	defer backend.Close()
	if _, err := kernel.CreateThread(ctx, sessiontree.ThreadMeta{ID: "source"}); err != nil {
		t.Fatal(err)
	}
	record := backendForkRecord(t, "fingerprint", now)
	if _, replayed, err := kernel.PrepareForkOperation(ctx, record); err != nil || replayed {
		t.Fatalf("first prepare replayed=%v err=%v", replayed, err)
	}
	if _, replayed, err := kernel.PrepareForkOperation(ctx, record); err != nil || !replayed {
		t.Fatalf("exact replay replayed=%v err=%v", replayed, err)
	}
	conflict := backendForkRecord(t, "other-fingerprint", now)
	if _, replayed, err := kernel.PrepareForkOperation(ctx, conflict); !errors.Is(err, ErrForkOperationConflict) || replayed {
		t.Fatalf("conflicting prepare replayed=%v err=%v, want ErrForkOperationConflict", replayed, err)
	}
}

func backendKernelSources(t *testing.T) []struct {
	name   string
	source publicstorage.Source
} {
	t.Helper()
	return []struct {
		name   string
		source publicstorage.Source
	}{
		{name: "memory", source: publicstorage.Memory()},
		{name: "sqlite", source: publicstorage.SQLite(filepath.Join(t.TempDir(), "kernel.db"))},
	}
}

func openBackendKernel(t *testing.T, source publicstorage.Source) (spi.Backend, *BackendKernel) {
	t.Helper()
	backend, err := storagebridge.Open(context.Background(), storagebridge.Source(source))
	if err != nil {
		t.Fatal(err)
	}
	kernel, err := NewBackendKernel(context.Background(), backend, sessiontree.DefaultLeasePolicy, time.Now)
	if err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	return backend, kernel
}

func backendApprovalPrepare(lease sessiontree.TurnLease, now time.Time) sessiontree.PrepareApprovalBatchRequest {
	item := sessiontree.ApprovalPreflightItem{
		EffectRequestFingerprint: "effect-call", ApprovalRequestFingerprint: "approval-call",
		Invocation: sessiontree.EffectInvocationIdentity{
			ThreadID: lease.ThreadID, TurnID: lease.TurnID, RunID: "run", ToolCallID: "call", ToolName: "write_file", ArgumentHash: "args",
		},
		ToolKind: "local", Step: 1, BatchSize: 1,
		Resources: []sessiontree.ApprovalResource{{Kind: "file", Value: "notes.md"}}, Effects: []string{"write"}, Destructive: true,
	}
	item.EffectAttemptID = sessiontree.ApprovalEffectAttemptID(item.Invocation)
	item.RequestedEntry = sessiontree.Entry{
		ID: sessiontree.ApprovalRequestedEntryID(item.EffectAttemptID), ThreadID: lease.ThreadID, TurnID: lease.TurnID,
		Type: sessiontree.EntryCustom, Metadata: map[string]string{"approval_state": "requested"},
	}
	return sessiontree.PrepareApprovalBatchRequest{Lease: lease, Now: now, Items: []sessiontree.ApprovalPreflightItem{item}}
}

func backendForkRecord(t *testing.T, fingerprint string, now time.Time) ForkOperationRecord {
	t.Helper()
	plan := ForkOperationPlan{
		Version: ForkOperationPlanVersion, OperationID: "fork-operation", RequestFingerprint: fingerprint, PreparedAt: now,
		Root: ForkOperationPlanNode{
			NodeID: "root", SourceThreadID: "source", DestinationThreadID: "destination",
			ArtifactClosure: emptyForkArtifactClosure(t, "source", "destination"),
		},
	}
	encoded, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	return ForkOperationRecord{
		OperationID: "fork-operation", RequestFingerprint: fingerprint,
		SourceThreadIDs: []string{"source"}, AuthorityThreadIDs: []string{"source", "destination"},
		State: ForkOperationPrepared, Plan: encoded, CreatedAt: now, UpdatedAt: now,
	}
}

func emptyForkArtifactClosure(t *testing.T, sourceThreadID, destinationThreadID string) artifact.Closure {
	t.Helper()
	items := []artifact.ManifestItem{}
	fingerprint, err := artifact.ClosureFingerprint(sourceThreadID, destinationThreadID, items)
	if err != nil {
		t.Fatal(err)
	}
	return artifact.Closure{
		SourceThreadID: sourceThreadID, DestinationThreadID: destinationThreadID,
		Items: items, Fingerprint: fingerprint,
	}
}

package storage_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	internalprovider "github.com/floegence/floret/v3/internal/provider"
	"github.com/floegence/floret/v3/internal/provider/cache"
	"github.com/floegence/floret/v3/internal/session"
	"github.com/floegence/floret/v3/internal/session/artifact"
	"github.com/floegence/floret/v3/internal/sessiontree"
	internalstorage "github.com/floegence/floret/v3/internal/storage"
	legacy "github.com/floegence/floret/v3/internal/storage/sqlite"
	"github.com/floegence/floret/v3/internal/storagebridge"
	"github.com/floegence/floret/v3/internal/storagecodec"
	floretruntime "github.com/floegence/floret/v3/runtime"
	"github.com/floegence/floret/v3/storage"
)

func legacyThread(id string, createdAt time.Time) sessiontree.ThreadMeta {
	return sessiontree.ThreadMeta{ID: id, CreatedAt: createdAt, UpdatedAt: createdAt}
}

func mutateLegacyMetadata(t *testing.T, path, key, value string) {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.Exec(`UPDATE schema_meta SET value = ? WHERE key = ?`, value, key); err != nil {
		t.Fatal(err)
	}
}

func legacyMetadata(t *testing.T, path, key string) string {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var value string
	if err := database.QueryRow(`SELECT value FROM schema_meta WHERE key = ?`, key).Scan(&value); err != nil {
		t.Fatal(err)
	}
	return value
}

func preflightV2Migration(ctx context.Context, path, operationID string) (storage.V2MigrationPlan, error) {
	return storage.PreflightV2Migration(ctx, storage.V2MigrationPreflightRequest{
		Path: path, OperationID: operationID, CoordinatorCommitment: "sha256:test-coordinator",
	})
}

func applyV2Migration(ctx context.Context, path, operationID string) (storage.V2MigrationReceipt, error) {
	plan, err := preflightV2Migration(ctx, path, operationID)
	if err != nil {
		return storage.V2MigrationReceipt{}, err
	}
	return storage.ApplyV2Migration(ctx, storage.V2MigrationApplyRequest{Path: path, Plan: plan})
}

func TestMigrateV2ConvertsExactSchemaV16AndReplaysOperation(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "floret.db")
	legacyStore, err := legacy.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	createdAt := time.Date(2026, time.July, 29, 8, 0, 0, 0, time.UTC)
	if _, err := legacyStore.CreateThread(ctx, legacyThread("thread-1", createdAt)); err != nil {
		t.Fatal(err)
	}
	if err := legacyStore.Close(); err != nil {
		t.Fatal(err)
	}

	plan, err := preflightV2Migration(ctx, path, "migration-1")
	if err != nil {
		t.Fatal(err)
	}
	conflictingPlan, err := preflightV2Migration(ctx, path, "migration-2")
	if err != nil {
		t.Fatal(err)
	}
	result, err := storage.ApplyV2Migration(ctx, storage.V2MigrationApplyRequest{Path: path, Plan: plan})
	if err != nil {
		t.Fatal(err)
	}
	if result.OperationID != "migration-1" || result.Replayed {
		t.Fatalf("migration result = %#v", result)
	}
	host, err := floretruntime.Open(ctx, floretruntime.Options{Storage: storage.SQLite(path)})
	if err != nil {
		t.Fatal(err)
	}
	handle, err := host.Thread(ctx, "thread-1")
	if err != nil {
		t.Fatal(err)
	}
	reader, err := handle.Reader()
	if err != nil {
		t.Fatal(err)
	}
	view, err := reader.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	thread := view.Thread
	if thread.ID != "thread-1" || !thread.CreatedAt.Equal(createdAt) {
		t.Fatalf("migrated thread = %#v", thread)
	}
	if err := host.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}

	replay, err := storage.ApplyV2Migration(ctx, storage.V2MigrationApplyRequest{Path: path, Plan: plan})
	if err != nil || !replay.Replayed {
		t.Fatalf("replay = %#v, err = %v", replay, err)
	}
	_, err = storage.ApplyV2Migration(ctx, storage.V2MigrationApplyRequest{Path: path, Plan: conflictingPlan})
	if !errors.Is(err, storage.ErrMigrationConflict) {
		t.Fatalf("different operation error = %v", err)
	}
}

func TestMigrateV2RejectsNonExactV16Fingerprint(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "floret.db")
	legacyStore, err := legacy.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := legacyStore.Close(); err != nil {
		t.Fatal(err)
	}
	mutateLegacyMetadata(t, path, "schema_fingerprint", "not-v16")

	_, err = preflightV2Migration(ctx, path, "migration-1")
	var schemaError *storage.MigrationSchemaError
	if !errors.As(err, &schemaError) || schemaError.Version != "16" || schemaError.Fingerprint != "not-v16" {
		t.Fatalf("schema error = %#v (%v)", schemaError, err)
	}
	if got := legacyMetadata(t, path, "schema_fingerprint"); got != "not-v16" {
		t.Fatalf("failed migration changed source fingerprint to %q", got)
	}
}

func TestMigrateV2RejectsHostOwnedLegacyMetadataWithoutMutation(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "floret.db")
	legacyStore, err := legacy.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := legacyStore.Close(); err != nil {
		t.Fatal(err)
	}
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`INSERT INTO metadata_records(namespace, id, created_at, updated_at, data_json)
		VALUES ('redeven', 'product-record', '2026-07-29T00:00:00Z', '2026-07-29T00:00:00Z', '{"owned_by":"host"}')`); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	_, err = preflightV2Migration(ctx, path, "migration-metadata")
	if err == nil || !strings.Contains(err.Error(), "host-owned records") {
		t.Fatalf("metadata migration error = %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("rejected metadata migration changed the v16 database bytes")
	}
}

func TestMigrateV2ReplayRejectsCorruptLogicalContent(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name       string
		mutate     string
		mutateArgs []any
	}{
		{name: "logical schema", mutate: `UPDATE floret_backend_records SET value = '{"version":"2","fingerprint":"wrong"}' WHERE namespace = 'floret.system'`},
		{
			name: "session tree state", mutate: `UPDATE floret_backend_records SET value = X'00' WHERE namespace = ? AND key = ?`,
			mutateArgs: []any{"floret.domain", storagecodec.Tuple(storagecodec.TupleString("sessiontree"), storagecodec.TupleString("state"))},
		},
		{
			name: "root thread inventory", mutate: `UPDATE floret_backend_records SET value = X'00' WHERE namespace = ? AND key = ?`,
			mutateArgs: []any{"floret.domain", storagecodec.Tuple(storagecodec.TupleString("sessiontree"), storagecodec.TupleString("root_thread_inventory"))},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "floret.db")
			legacyStore, err := legacy.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := legacyStore.Close(); err != nil {
				t.Fatal(err)
			}
			plan, err := preflightV2Migration(ctx, path, "migration-replay")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := storage.ApplyV2Migration(ctx, storage.V2MigrationApplyRequest{Path: path, Plan: plan}); err != nil {
				t.Fatal(err)
			}
			database, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := database.Exec(test.mutate, test.mutateArgs...); err != nil {
				database.Close()
				t.Fatal(err)
			}
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}

			result, err := storage.ApplyV2Migration(ctx, storage.V2MigrationApplyRequest{Path: path, Plan: plan})
			if err == nil || result.Replayed {
				t.Fatalf("corrupt replay result = %#v, err = %v", result, err)
			}
			var schemaError *storage.MigrationSchemaError
			if !errors.As(err, &schemaError) {
				t.Fatalf("corrupt replay error = %T %v", err, err)
			}
		})
	}
}

func TestMigrateV2PreservesDurableAuthorityProviderPromptAndForkState(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "floret.db")
	legacyStore, err := legacy.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	for _, threadID := range []string{"active", "fork-source"} {
		if _, err := legacyStore.CreateThread(ctx, legacyThread(threadID, now)); err != nil {
			t.Fatal(err)
		}
	}
	entry, err := legacyStore.Append(ctx, sessiontree.Entry{
		ID: "entry", ThreadID: "active", TurnID: "turn", Type: sessiontree.EntryUserMessage,
		Message: session.Message{Role: session.User, Content: "preserve me"}, CreatedAt: now,
	}, sessiontree.AppendOptions{ID: "entry", Now: now})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := legacyStore.AcquireTurnLease(ctx, sessiontree.TurnLease{
		ThreadID: "active", TurnID: "turn", OwnerID: "owner", Purpose: sessiontree.TurnLeasePurposeTurn,
	})
	if err != nil {
		t.Fatal(err)
	}
	leaseCtx := sessiontree.ContextWithTurnLease(ctx, lease)
	if _, err := legacyStore.CompareAndSwapAgentTodoState(leaseCtx, sessiontree.AgentTodoState{
		ThreadID: "active", Items: []sessiontree.AgentTodoItem{{ID: "todo", Content: "keep", Status: sessiontree.AgentTodoPending}},
		UpdatedAt: now, UpdatedByTurnID: "turn", UpdatedByRunID: "run", UpdatedByToolCall: "call",
	}, 0); err != nil {
		t.Fatal(err)
	}
	providerState := sessiontree.ProviderStateRecord{
		ThreadID: "active", LeafEntryID: entry.ID, CompatibilityKey: "gateway/model",
		State:          internalprovider.State{Kind: "opaque", ID: "state", Attributes: map[string]string{"cursor": "next"}},
		CreatedByRunID: "run", CreatedByTurnID: "turn", UpdatedAt: now,
	}
	if err := legacyStore.PutProviderState(leaseCtx, providerState); err != nil {
		t.Fatal(err)
	}
	segment := cache.Segment{ID: "segment", PromptScopeID: "active", ThreadID: "active", Provider: "gateway", Model: "model", Sequence: 1, CreatedAt: now}
	if err := legacyStore.AppendSegment(ctx, segment); err != nil {
		t.Fatal(err)
	}
	closure := emptyMigrationArtifactClosure(t, "fork-source", "fork-destination")
	plan := internalstorage.ForkOperationPlan{
		Version: internalstorage.ForkOperationPlanVersion, OperationID: "fork-operation",
		RequestFingerprint: "fork-request", PreparedAt: now,
		Root: internalstorage.ForkOperationPlanNode{
			NodeID: "root", SourceThreadID: "fork-source", DestinationThreadID: "fork-destination", ArtifactClosure: closure,
		},
	}
	planJSON, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	forkRecord := internalstorage.ForkOperationRecord{
		OperationID: "fork-operation", RequestFingerprint: "fork-request",
		SourceThreadIDs: []string{"fork-source"}, AuthorityThreadIDs: []string{"fork-source", "fork-destination"},
		State: internalstorage.ForkOperationPrepared, Plan: planJSON, CreatedAt: now, UpdatedAt: now,
	}
	if _, created, err := legacyStore.PrepareForkOperation(ctx, forkRecord); err != nil || !created {
		t.Fatalf("prepare fork created=%v err=%v", created, err)
	}
	if err := legacyStore.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := applyV2Migration(ctx, path, "migration-authority"); err != nil {
		t.Fatal(err)
	}

	backend, err := storagebridge.Open(ctx, storagebridge.Source(storage.SQLite(path)))
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	kernel, err := internalstorage.NewBackendKernel(ctx, backend, sessiontree.DefaultLeasePolicy, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	gotLease, found, err := kernel.ActiveTurnLease(ctx, "active")
	if err != nil || !found || !sessiontree.SameTurnLease(gotLease, lease) {
		t.Fatalf("active lease = %#v found=%v err=%v", gotLease, found, err)
	}
	gotTodo, err := kernel.ReadAgentTodoState(ctx, "active")
	if err != nil || len(gotTodo.Items) != 1 || gotTodo.Items[0].Content != "keep" {
		t.Fatalf("todo = %#v err=%v", gotTodo, err)
	}
	gotProviderState, err := kernel.ProviderState(ctx, "active")
	if err != nil || gotProviderState.State.ID != providerState.State.ID || gotProviderState.State.Attributes["cursor"] != "next" {
		t.Fatalf("provider state = %#v err=%v", gotProviderState, err)
	}
	segments, err := kernel.Segments(ctx, "active", "gateway", "model")
	if err != nil || len(segments) != 1 || segments[0].ID != segment.ID {
		t.Fatalf("segments = %#v err=%v", segments, err)
	}
	gotFork, err := kernel.ForkOperation(ctx, "fork-operation")
	if err != nil || gotFork.RequestFingerprint != forkRecord.RequestFingerprint {
		t.Fatalf("fork operation = %#v err=%v", gotFork, err)
	}
	if _, err := kernel.Append(ctx, sessiontree.Entry{
		ID: "blocked", ThreadID: "fork-source", Type: sessiontree.EntryCustom, CreatedAt: now,
	}, sessiontree.AppendOptions{ID: "blocked", Now: now}); !errors.Is(err, sessiontree.ErrThreadAuthorityBusy) {
		t.Fatalf("migrated fork authority claim error = %v", err)
	}
}

func emptyMigrationArtifactClosure(t *testing.T, sourceThreadID, destinationThreadID string) artifact.Closure {
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

func TestMigrateV2PreservesTurnAndApprovalAuthorityLedgers(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "floret.db")
	now := time.Now().UTC().Truncate(time.Millisecond)
	legacyStore, err := legacy.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, threadID := range []string{"approval", "finished"} {
		if _, err := legacyStore.CreateThread(ctx, legacyThread(threadID, now)); err != nil {
			t.Fatal(err)
		}
	}
	approvalAdmission, err := legacyStore.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
		ThreadID: "approval", TurnID: "approval-turn", RunID: "approval-run", OwnerID: "approval-owner",
		RequestFingerprint: "approval-admission", Input: session.Message{Role: session.User, Content: "approve"}, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	approvalRequest := migrationApprovalPrepare(approvalAdmission.Lease, now)
	prepared, err := legacyStore.PrepareApprovalBatch(ctx, approvalRequest)
	if err != nil {
		t.Fatal(err)
	}
	finishedAdmission, err := legacyStore.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
		ThreadID: "finished", TurnID: "finished-turn", RunID: "finished-run", OwnerID: "finished-owner",
		RequestFingerprint: "finished-admission", Input: session.Message{Role: session.User, Content: "finish"}, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	finished, err := legacyStore.FinishTurn(ctx, sessiontree.FinishTurnRequest{
		Lease: finishedAdmission.Lease, RunID: "finished-run", TerminalEntryID: "finished-terminal",
		Status: sessiontree.TurnCompleted, OutcomeFingerprint: "finished-outcome", Now: now.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := legacyStore.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := applyV2Migration(ctx, path, "migration-turns"); err != nil {
		t.Fatal(err)
	}
	backend, err := storagebridge.Open(ctx, storagebridge.Source(storage.SQLite(path)))
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	kernel, err := internalstorage.NewBackendKernel(ctx, backend, sessiontree.DefaultLeasePolicy, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	gotApprovalAdmission, found, err := kernel.ReadTurnAdmission(ctx, "approval", "approval-turn", "approval-run")
	if err != nil || !found || gotApprovalAdmission.Terminal != nil || gotApprovalAdmission.TurnStarted.ID != approvalAdmission.TurnStarted.ID {
		t.Fatalf("approval admission = %#v found=%v err=%v", gotApprovalAdmission, found, err)
	}
	queue, err := kernel.ReadApprovalQueue(ctx, "approval")
	if err != nil || queue.CurrentApprovalID != prepared.Approvals[0].ApprovalID || len(queue.Items) != 1 {
		t.Fatalf("approval queue = %#v err=%v", queue, err)
	}
	approval, err := kernel.Approval(ctx, prepared.Approvals[0].ApprovalID)
	if err != nil || approval.RequestFingerprint != prepared.Approvals[0].RequestFingerprint {
		t.Fatalf("approval = %#v err=%v", approval, err)
	}
	replayedApproval, err := kernel.PrepareApprovalBatch(ctx, approvalRequest)
	if err != nil || !replayedApproval.Replayed || len(replayedApproval.Effects) != 1 {
		t.Fatalf("approval replay = %#v err=%v", replayedApproval, err)
	}
	gotFinished, found, err := kernel.ReadTurnAdmission(ctx, "finished", "finished-turn", "finished-run")
	if err != nil || !found || gotFinished.Terminal == nil || gotFinished.Terminal.Terminal.ID != finished.Terminal.ID {
		t.Fatalf("finished admission = %#v found=%v err=%v", gotFinished, found, err)
	}
}

func TestMigrateV2PreservesRootSubAgentCompactionAndArtifactReplay(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "floret.db")
	now := time.Now().UTC().Truncate(time.Millisecond)
	legacyStore, err := legacy.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	rootRequest := sessiontree.CreateRootRequest{
		ThreadID: "root", CreateIntentID: "create-root", ContractVersion: "1",
		Meta: legacyThread("root", now),
	}
	if created, err := legacyStore.CreateRoot(ctx, rootRequest); err != nil || created.Replayed {
		t.Fatalf("root create = %#v, err = %v", created, err)
	}
	publication := sessiontree.PublishSubAgentRequest{
		PublicationID: "publish-child", RequestFingerprint: "publish-child-fingerprint", ParentThreadID: "root",
		ChildMeta: sessiontree.ThreadMeta{
			ID: "child", ParentThreadID: "root", TaskName: "child", AgentPath: "/root/child",
			CreatedAt: now, UpdatedAt: now,
		},
		Message: session.Message{Role: session.User, Content: "delegated work"}, Now: now,
	}
	if published, err := legacyStore.PublishSubAgent(ctx, publication); err != nil || published.Replayed {
		t.Fatalf("subagent publication = %#v, err = %v", published, err)
	}
	if _, err := legacyStore.CreateThread(ctx, legacyThread("compact", now)); err != nil {
		t.Fatal(err)
	}
	entry, err := legacyStore.Append(ctx, sessiontree.Entry{
		ID: "compact-entry", ThreadID: "compact", Type: sessiontree.EntryUserMessage,
		Message: session.Message{Role: session.User, Content: "compact me"}, CreatedAt: now,
	}, sessiontree.AppendOptions{ID: "compact-entry", Now: now})
	if err != nil {
		t.Fatal(err)
	}
	compactionRequest := sessiontree.BeginCompactionRequest{
		ThreadID: "compact", RequestID: "compact-request", RequestFingerprint: "compact-fingerprint",
		Source: "manual", SourceLeafID: entry.ID, ActivePathHash: sessiontree.ActivePathHash([]sessiontree.Entry{entry}),
		SummarySchemaVersion: "v1", PromptIdentity: "prompt", RequestPayloadHash: "payload", OwnerID: "owner", Now: now,
	}
	startedCompaction, err := legacyStore.BeginCompaction(ctx, compactionRequest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := legacyStore.CreateThread(ctx, legacyThread("artifact", now)); err != nil {
		t.Fatal(err)
	}
	artifactRef := seedMigrationArtifact(t, ctx, legacyStore, "artifact", "complete artifact output", now)
	if err := legacyStore.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := applyV2Migration(ctx, path, "migration-lifecycle"); err != nil {
		t.Fatal(err)
	}

	backend, err := storagebridge.Open(ctx, storagebridge.Source(storage.SQLite(path)))
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	kernel, err := internalstorage.NewBackendKernel(ctx, backend, sessiontree.DefaultLeasePolicy, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if replayed, err := kernel.CreateRoot(ctx, rootRequest); err != nil || !replayed.Replayed {
		t.Fatalf("migrated root replay = %#v, err = %v", replayed, err)
	}
	if replayed, err := kernel.PublishSubAgent(ctx, publication); err != nil || !replayed.Replayed || replayed.Thread.ID != "child" {
		t.Fatalf("migrated subagent replay = %#v, err = %v", replayed, err)
	}
	compaction, found, err := kernel.ReadCompaction(ctx, "compact", "compact-request")
	if err != nil || !found || compaction.Lease.Generation != startedCompaction.Operation.Lease.Generation || compaction.RequestFingerprint != compactionRequest.RequestFingerprint {
		t.Fatalf("migrated compaction = %#v, found = %v, err = %v", compaction, found, err)
	}
	content, err := kernel.ReadArtifact(ctx, sessiontree.ArtifactReadRequest{ThreadID: "artifact", ArtifactID: artifactRef.ID})
	if err != nil || content.Ref != artifactRef || content.Text != "complete artifact output" {
		t.Fatalf("migrated artifact = %#v, err = %v", content, err)
	}
}

func seedMigrationArtifact(t *testing.T, ctx context.Context, store *legacy.Store, threadID, text string, now time.Time) artifact.Ref {
	t.Helper()
	turnID, runID, callID := "artifact-turn", "artifact-run", "artifact-call"
	admitted, err := store.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
		ThreadID: threadID, TurnID: turnID, RunID: runID, OwnerID: "artifact-owner",
		RequestFingerprint: "artifact-admission", Input: session.Message{Role: session.User, Content: "preserve output"}, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := store.PrepareEffectAttempt(ctx, sessiontree.PrepareEffectAttemptRequest{
		Lease: admitted.Lease, RequestFingerprint: "artifact-effect", Now: now,
		Invocation: sessiontree.EffectInvocationIdentity{
			ThreadID: threadID, TurnID: turnID, RunID: runID, ToolCallID: callID, ToolName: "read", ArgumentHash: "artifact-arguments",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.BeginEffectDispatch(ctx, sessiontree.BeginEffectDispatchRequest{
		Lease: admitted.Lease, EffectAttemptID: prepared.Attempt.EffectAttemptID, RequestFingerprint: "artifact-effect",
		ObservedHeartbeat: admitted.Lease.Heartbeat, AuthorizationProofHash: "artifact-proof", Now: now,
	}); err != nil {
		t.Fatal(err)
	}
	finished, err := store.FinishEffectDispatch(ctx, sessiontree.FinishEffectDispatchRequest{
		Lease: admitted.Lease, EffectAttemptID: prepared.Attempt.EffectAttemptID, RequestFingerprint: "artifact-effect",
		OutcomeFingerprint: "artifact-outcome", FullOutput: &artifact.FullOutput{Text: text}, Now: now,
		Result: sessiontree.Entry{ThreadID: threadID, TurnID: turnID, Type: sessiontree.EntryToolResult,
			Message: session.Message{Role: session.Tool, ToolCallID: callID, ToolName: "read", Content: "visible", ToolResult: &session.ToolResultView{Status: "success", Truncated: true}}},
	})
	if err != nil || finished.Artifact == nil {
		t.Fatalf("finish artifact effect = %#v, err = %v", finished, err)
	}
	if _, err := store.FinishTurn(ctx, sessiontree.FinishTurnRequest{
		Lease: admitted.Lease, RunID: runID, TerminalEntryID: "artifact-terminal",
		Status: sessiontree.TurnCompleted, OutcomeFingerprint: "artifact-turn-outcome", Now: now,
	}); err != nil {
		t.Fatal(err)
	}
	return *finished.Artifact
}

func migrationApprovalPrepare(lease sessiontree.TurnLease, now time.Time) sessiontree.PrepareApprovalBatchRequest {
	item := sessiontree.ApprovalPreflightItem{
		EffectRequestFingerprint: "effect-request", ApprovalRequestFingerprint: "approval-request",
		Invocation: sessiontree.EffectInvocationIdentity{
			ThreadID: lease.ThreadID, TurnID: lease.TurnID, RunID: "approval-run",
			ToolCallID: "tool-call", ToolName: "write_file", ArgumentHash: "args",
		},
		ToolKind: "local", Step: 1, BatchSize: 1,
		Resources: []sessiontree.ApprovalResource{{Kind: "file", Value: "notes.md"}},
		Effects:   []string{"write"}, Destructive: true,
	}
	item.EffectAttemptID = sessiontree.ApprovalEffectAttemptID(item.Invocation)
	item.RequestedEntry = sessiontree.Entry{
		ID: sessiontree.ApprovalRequestedEntryID(item.EffectAttemptID), ThreadID: lease.ThreadID,
		TurnID: lease.TurnID, Type: sessiontree.EntryCustom,
		Metadata: map[string]string{"approval_state": "requested"},
	}
	return sessiontree.PrepareApprovalBatchRequest{Lease: lease, Now: now, Items: []sessiontree.ApprovalPreflightItem{item}}
}

package sessiontree

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/session/artifact"
)

func TestMemoryArtifactAdmissionReplayAndConflictHaveAtomicState(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	repo, err := NewMemoryRepoWithLeasePolicy(DefaultLeasePolicy, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	admitted, prepared := prepareMemoryArtifactEffect(t, repo, "thread", "turn", "run", "call", now)
	request := FinishEffectDispatchRequest{
		Lease: admitted.Lease, EffectAttemptID: prepared.Attempt.EffectAttemptID, RequestFingerprint: "effect-thread",
		OutcomeFingerprint: "outcome", FullOutput: &artifact.FullOutput{Text: "complete output", Kind: "report", MIME: "text/markdown"}, Now: now,
		Result: Entry{ThreadID: "thread", TurnID: "turn", Type: EntryToolResult,
			Message: session.Message{Role: session.Tool, ToolCallID: "call", ToolName: "read", ToolResult: &session.ToolResultView{Status: "success"}}},
	}
	finished, err := repo.FinishEffectDispatch(ctx, request)
	if err != nil || finished.Replayed || finished.Artifact == nil {
		t.Fatalf("first finish = %#v err=%v", finished, err)
	}
	entriesBefore := len(repo.entries["thread"])
	artifactsBefore := len(repo.artifacts)
	replayed, err := repo.FinishEffectDispatch(ctx, request)
	if err != nil || !replayed.Replayed || replayed.Result.ID != finished.Result.ID || replayed.Artifact == nil || *replayed.Artifact != *finished.Artifact {
		t.Fatalf("finish replay = %#v err=%v", replayed, err)
	}
	for _, mutate := range []func(*FinishEffectDispatchRequest){
		func(req *FinishEffectDispatchRequest) { req.FullOutput.Text = "changed" },
		func(req *FinishEffectDispatchRequest) { req.FullOutput.Kind = "changed" },
		func(req *FinishEffectDispatchRequest) { req.FullOutput.MIME = "application/json" },
	} {
		conflict := request
		full := *request.FullOutput
		conflict.FullOutput = &full
		mutate(&conflict)
		got, err := repo.FinishEffectDispatch(ctx, conflict)
		if !errors.Is(err, ErrRequestConflict) || got.Result.ID != "" || got.Artifact != nil {
			t.Fatalf("changed output result=%#v err=%v", got, err)
		}
	}
	if len(repo.entries["thread"]) != entriesBefore || len(repo.artifacts) != artifactsBefore {
		t.Fatalf("replay conflicts changed state: entries=%d artifacts=%d", len(repo.entries["thread"]), len(repo.artifacts))
	}

	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "orphan-check", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	orphanAdmission, orphanPrepared := prepareMemoryArtifactEffect(t, repo, "orphan-check", "turn", "run", "call", now)
	orphanFull := artifact.FullOutput{Text: "must not persist"}
	orphanRef, err := artifact.RefForEffect(orphanPrepared.Attempt.EffectAttemptID, "read", orphanFull)
	if err != nil {
		t.Fatal(err)
	}
	_, err = repo.FinishEffectDispatch(ctx, FinishEffectDispatchRequest{
		Lease: orphanAdmission.Lease, EffectAttemptID: orphanPrepared.Attempt.EffectAttemptID, RequestFingerprint: "effect-orphan-check",
		OutcomeFingerprint: "invalid", FullOutput: &orphanFull, Now: now,
		Result: Entry{ThreadID: "wrong-thread", TurnID: "turn", Type: EntryToolResult,
			Message: session.Message{Role: session.Tool, ToolCallID: "call", ToolName: "read", ToolResult: &session.ToolResultView{Status: "success"}}},
	})
	if !errors.Is(err, ErrInvalidThreadAuthority) {
		t.Fatalf("invalid artifact finish err=%v", err)
	}
	if _, ok := repo.artifacts[artifactRecordKey("orphan-check", orphanRef.ID)]; ok {
		t.Fatal("failed effect finish persisted an orphan artifact")
	}
}

func TestMemoryArtifactReadAuthorityClosureAndDeleteRace(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 19, 13, 0, 0, 0, time.UTC)
	repo, err := NewMemoryRepoWithLeasePolicy(DefaultLeasePolicy, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	for _, meta := range []ThreadMeta{
		{ID: "root"},
		{ID: "child", ParentThreadID: "root", TaskName: "child", AgentPath: "child"},
		{ID: "deep", ParentThreadID: "child", TaskName: "deep", AgentPath: "child/deep"},
		{ID: "sibling", ParentThreadID: "root", TaskName: "sibling", AgentPath: "sibling"},
		{ID: "foreign"},
	} {
		meta.CreatedAt, meta.UpdatedAt = now, now
		if _, err := repo.CreateThread(ctx, meta); err != nil {
			t.Fatalf("create %q: %v", meta.ID, err)
		}
	}
	rootRef, _ := seedMemoryArtifact(t, repo, "root", "root", "root output", now)
	deepRef, _ := seedMemoryArtifact(t, repo, "deep", "deep", "deep output", now)

	for _, request := range []ArtifactReadRequest{
		{ThreadID: "root", ArtifactID: rootRef.ID},
		{ParentThreadID: "root", ThreadID: "deep", ArtifactID: deepRef.ID},
		{ParentThreadID: "child", ThreadID: "deep", ArtifactID: deepRef.ID},
	} {
		content, err := repo.ReadArtifact(ctx, request)
		if err != nil || content.Ref.ID != request.ArtifactID || content.Text == "" {
			t.Fatalf("authorized read %#v content=%#v err=%v", request, content, err)
		}
	}
	for _, test := range []struct {
		request ArtifactReadRequest
		want    error
	}{
		{ArtifactReadRequest{ThreadID: "deep", ArtifactID: deepRef.ID}, ErrSubAgentParentRequired},
		{ArtifactReadRequest{ParentThreadID: "root", ThreadID: "root", ArtifactID: rootRef.ID}, ErrSubAgentNotFound},
		{ArtifactReadRequest{ParentThreadID: "deep", ThreadID: "child", ArtifactID: deepRef.ID}, ErrSubAgentNotFound},
		{ArtifactReadRequest{ParentThreadID: "sibling", ThreadID: "deep", ArtifactID: deepRef.ID}, ErrSubAgentNotFound},
		{ArtifactReadRequest{ParentThreadID: "root", ThreadID: "foreign", ArtifactID: deepRef.ID}, ErrSubAgentNotFound},
		{ArtifactReadRequest{ThreadID: "root", ArtifactID: deepRef.ID}, ErrArtifactNotFound},
	} {
		content, err := repo.ReadArtifact(ctx, test.request)
		if !errors.Is(err, test.want) || content != (ArtifactContent{}) {
			t.Fatalf("unauthorized read %#v content=%#v err=%v want=%v", test.request, content, err, test.want)
		}
	}

	path, err := repo.Path(ctx, "root", "")
	if err != nil {
		t.Fatal(err)
	}
	closure, err := repo.ArtifactClosure(ctx, ArtifactClosureRequest{SourceThreadID: "root", DestinationThreadID: "fork", EntryIDs: artifactEntryIDs(path)})
	if err != nil || len(closure.Items) != 1 || closure.Items[0].ArtifactID != rootRef.ID {
		t.Fatalf("closure=%#v err=%v", closure, err)
	}
	forked, err := repo.Fork(ctx, ForkOptions{SourceThreadID: "root", NewThreadID: "fork", ArtifactClosure: closure, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	content, err := repo.ReadArtifact(ctx, ArtifactReadRequest{ThreadID: forked.ID, ArtifactID: rootRef.ID})
	if err != nil || content.Text != "root output" || content.Ref != rootRef {
		t.Fatalf("forked content=%#v err=%v", content, err)
	}

	raceRepo, err := NewMemoryRepoWithLeasePolicy(DefaultLeasePolicy, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raceRepo.CreateThread(ctx, ThreadMeta{ID: "race", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	raceRef, _ := seedMemoryArtifact(t, raceRepo, "race", "race", "race output", now)
	start := make(chan struct{})
	var wg sync.WaitGroup
	var readContent ArtifactContent
	var readErr, deleteErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		readContent, readErr = raceRepo.ReadArtifact(ctx, ArtifactReadRequest{ThreadID: "race", ArtifactID: raceRef.ID})
	}()
	go func() {
		defer wg.Done()
		<-start
		_, deleteErr = raceRepo.DeleteRootTree(ctx, "race")
	}()
	close(start)
	wg.Wait()
	if deleteErr != nil {
		t.Fatalf("delete race: %v", deleteErr)
	}
	if readErr == nil {
		if readContent.Text != "race output" || readContent.Ref != raceRef {
			t.Fatalf("partial successful race read: %#v", readContent)
		}
	} else if !errors.Is(readErr, ErrThreadDeleted) || readContent != (ArtifactContent{}) {
		t.Fatalf("race read content=%#v err=%v", readContent, readErr)
	}
	if content, err := raceRepo.ReadArtifact(ctx, ArtifactReadRequest{ThreadID: "race", ArtifactID: raceRef.ID}); !errors.Is(err, ErrThreadDeleted) || content != (ArtifactContent{}) {
		t.Fatalf("post-delete read content=%#v err=%v", content, err)
	}
}

func TestMemoryArtifactReadRejectsCorruptAuthorityAndCompositeRecord(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 19, 13, 30, 0, 0, time.UTC)
	newRepo := func(t *testing.T) *MemoryRepo {
		t.Helper()
		repo, err := NewMemoryRepoWithLeasePolicy(DefaultLeasePolicy, func() time.Time { return now })
		if err != nil {
			t.Fatal(err)
		}
		for _, meta := range []ThreadMeta{
			{ID: "root", CreatedAt: now, UpdatedAt: now},
			{ID: "child", ParentThreadID: "root", TaskName: "child", AgentPath: "child", CreatedAt: now, UpdatedAt: now},
		} {
			if _, err := repo.CreateThread(ctx, meta); err != nil {
				t.Fatal(err)
			}
		}
		return repo
	}
	assertCorrupt := func(t *testing.T, repo *MemoryRepo, req ArtifactReadRequest) {
		t.Helper()
		content, err := repo.ReadArtifact(ctx, req)
		if !errors.Is(err, ErrAuthorityCorrupt) || content != (ArtifactContent{}) {
			t.Fatalf("corrupt read %#v content=%#v err=%v", req, content, err)
		}
	}

	t.Run("bound parent shape", func(t *testing.T) {
		repo := newRepo(t)
		parent := repo.threads["root"]
		parent.TaskName = "not-root-metadata"
		repo.threads["root"] = parent
		assertCorrupt(t, repo, ArtifactReadRequest{ParentThreadID: "root", ThreadID: "child", ArtifactID: "missing"})
	})

	t.Run("target shape", func(t *testing.T) {
		repo := newRepo(t)
		child := repo.threads["child"]
		child.TaskName = ""
		repo.threads["child"] = child
		assertCorrupt(t, repo, ArtifactReadRequest{ParentThreadID: "root", ThreadID: "child", ArtifactID: "missing"})
	})

	t.Run("tombstone ancestry", func(t *testing.T) {
		repo := newRepo(t)
		parent := repo.threads["root"]
		if _, err := repo.DeleteRootTree(ctx, "root"); err != nil {
			t.Fatal(err)
		}
		repo.threads["root"] = parent
		delete(repo.tombstones, "root")
		if content, err := repo.ReadArtifact(ctx, ArtifactReadRequest{ParentThreadID: "root", ThreadID: "child", ArtifactID: "missing"}); !errors.Is(err, ErrThreadDeleted) || content != (ArtifactContent{}) {
			t.Fatalf("valid deleted descendant content=%#v err=%v", content, err)
		}
		childTombstone := repo.tombstones["child"]
		childTombstone.ParentThreadID = "child"
		repo.tombstones["child"] = childTombstone
		assertCorrupt(t, repo, ArtifactReadRequest{ParentThreadID: "root", ThreadID: "child", ArtifactID: "missing"})
		childTombstone.ParentThreadID = "missing"
		repo.tombstones["child"] = childTombstone
		assertCorrupt(t, repo, ArtifactReadRequest{ParentThreadID: "root", ThreadID: "child", ArtifactID: "missing"})
	})

	t.Run("tombstone root authority", func(t *testing.T) {
		repo := newRepo(t)
		parent := repo.threads["root"]
		if _, err := repo.DeleteRootTree(ctx, "root"); err != nil {
			t.Fatal(err)
		}
		repo.threads["root"] = parent
		delete(repo.tombstones, "root")
		childTombstone := repo.tombstones["child"]
		childTombstone.RootThreadID = "foreign-root"
		repo.tombstones["child"] = childTombstone
		assertCorrupt(t, repo, ArtifactReadRequest{ParentThreadID: "root", ThreadID: "child", ArtifactID: "missing"})
	})

	t.Run("composite record key", func(t *testing.T) {
		repo := newRepo(t)
		ref, _ := seedMemoryArtifact(t, repo, "root", "root", "root output", now)
		key := artifactRecordKey("root", ref.ID)
		record := repo.artifacts[key]
		record.ThreadID = "other"
		repo.artifacts[key] = record
		assertCorrupt(t, repo, ArtifactReadRequest{ThreadID: "root", ArtifactID: ref.ID})

		record.ThreadID = "root"
		record.Ref.ID = "other-artifact"
		repo.artifacts[key] = record
		assertCorrupt(t, repo, ArtifactReadRequest{ThreadID: "root", ArtifactID: ref.ID})
	})
}

func TestMemoryArtifactClosureRejectsOffCanonicalDuplicateDriftAndCollision(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 19, 14, 0, 0, 0, time.UTC)
	repo, err := NewMemoryRepoWithLeasePolicy(DefaultLeasePolicy, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "source", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	ref, _ := seedMemoryArtifact(t, repo, "source", "source", "source output", now)
	path, err := repo.Path(ctx, "source", "")
	if err != nil {
		t.Fatal(err)
	}
	pinnedLeafID := path[len(path)-1].ID
	closure, err := repo.ArtifactClosure(ctx, ArtifactClosureRequest{SourceThreadID: "source", DestinationThreadID: "collision", EntryIDs: artifactEntryIDs(path)})
	if err != nil {
		t.Fatal(err)
	}
	duplicateEntry, err := AppendMessage(ctx, repo, "source", "duplicate-turn", session.Message{
		Role: session.Tool, ToolCallID: "duplicate-call", ToolName: "read",
		ToolResult: &session.ToolResultView{Status: "success", FullOutput: &ref},
	})
	if err != nil {
		t.Fatal(err)
	}
	if closure, err := repo.ArtifactClosure(ctx, ArtifactClosureRequest{
		SourceThreadID: "source", DestinationThreadID: "off-path", EntryIDs: []string{duplicateEntry.ID},
	}); !errors.Is(err, ErrAuthorityCorrupt) || !artifact.IsZeroClosure(closure) {
		t.Fatalf("off-canonical closure=%#v err=%v", closure, err)
	}

	duplicate := artifact.CloneClosure(closure)
	duplicate.DestinationThreadID = "duplicate"
	duplicate.Items = append(duplicate.Items, duplicate.Items[0])
	duplicate.Fingerprint, err = artifact.ClosureFingerprint(duplicate.SourceThreadID, duplicate.DestinationThreadID, duplicate.Items)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Fork(ctx, ForkOptions{SourceThreadID: "source", EntryID: pinnedLeafID, EntryIDPinned: true, NewThreadID: "duplicate", ArtifactClosure: duplicate, Now: now}); !errors.Is(err, ErrStaleAuthority) {
		t.Fatalf("duplicate closure fork err=%v", err)
	}
	if _, err := repo.Thread(ctx, "duplicate"); !errors.Is(err, ErrThreadNotFound) {
		t.Fatalf("duplicate closure published destination: %v", err)
	}

	item := closure.Items[0]
	sourceRecord := repo.artifacts[artifactRecordKey("source", item.ArtifactID)]
	repo.artifacts[artifactRecordKey("collision", item.ArtifactID)] = artifact.Record{
		ThreadID: "collision", Ref: item.Ref, Text: "orphan collision", CanonicalEntryID: "missing", CreatedAt: now,
	}
	if _, err := repo.Fork(ctx, ForkOptions{SourceThreadID: "source", EntryID: pinnedLeafID, EntryIDPinned: true, NewThreadID: "collision", ArtifactClosure: closure, Now: now}); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("artifact collision fork err=%v", err)
	}
	if _, err := repo.Thread(ctx, "collision"); !errors.Is(err, ErrThreadNotFound) {
		t.Fatalf("collision published destination: %v", err)
	}
	if got := repo.artifacts[artifactRecordKey("source", item.ArtifactID)]; got != sourceRecord {
		t.Fatalf("collision mutated source record: %#v", got)
	}

	driftRepo, err := NewMemoryRepoWithLeasePolicy(DefaultLeasePolicy, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := driftRepo.CreateThread(ctx, ThreadMeta{ID: "source", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	driftRef, _ := seedMemoryArtifact(t, driftRepo, "source", "drift", "original", now)
	driftPath, err := driftRepo.Path(ctx, "source", "")
	if err != nil {
		t.Fatal(err)
	}
	driftClosure, err := driftRepo.ArtifactClosure(ctx, ArtifactClosureRequest{SourceThreadID: "source", DestinationThreadID: "drift-destination", EntryIDs: artifactEntryIDs(driftPath)})
	if err != nil {
		t.Fatal(err)
	}
	record := driftRepo.artifacts[artifactRecordKey("source", driftRef.ID)]
	record.Text = "changed"
	driftRepo.artifacts[artifactRecordKey("source", driftRef.ID)] = record
	if _, err := driftRepo.Fork(ctx, ForkOptions{SourceThreadID: "source", NewThreadID: "drift-destination", ArtifactClosure: driftClosure, Now: now}); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("payload drift fork err=%v", err)
	}
	if _, err := driftRepo.Thread(ctx, "drift-destination"); !errors.Is(err, ErrThreadNotFound) {
		t.Fatalf("payload drift published destination: %v", err)
	}
}

func prepareMemoryArtifactEffect(t *testing.T, repo *MemoryRepo, threadID, turnID, runID, callID string, now time.Time) (AdmitTurnResult, PrepareEffectAttemptResult) {
	t.Helper()
	ctx := context.Background()
	admitted, err := repo.AdmitTurn(ctx, AdmitTurnRequest{
		ThreadID: threadID, TurnID: turnID, RunID: runID, OwnerID: "owner-" + threadID,
		RequestFingerprint: "admit-" + threadID, Input: session.Message{Role: session.User, Content: "start"}, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	leaseCtx := ContextWithTurnLease(ctx, admitted.Lease)
	if _, err := repo.Append(leaseCtx, Entry{
		ThreadID: threadID, TurnID: turnID, Type: EntryToolCall,
		Message: session.Message{Role: session.Assistant, ToolCallID: callID, ToolName: "read"},
	}, AppendOptions{Now: now}); err != nil {
		t.Fatal(err)
	}
	prepared, err := repo.PrepareEffectAttempt(ctx, PrepareEffectAttemptRequest{
		Lease: admitted.Lease, RequestFingerprint: "effect-" + threadID, Now: now,
		Invocation: EffectInvocationIdentity{ThreadID: threadID, TurnID: turnID, RunID: runID, ToolCallID: callID, ToolName: "read", ArgumentHash: "arguments-" + threadID},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.BeginEffectDispatch(ctx, BeginEffectDispatchRequest{
		Lease: admitted.Lease, EffectAttemptID: prepared.Attempt.EffectAttemptID, RequestFingerprint: "effect-" + threadID,
		ObservedHeartbeat: admitted.Lease.Heartbeat, AuthorizationProofHash: "proof-" + threadID, Now: now,
	}); err != nil {
		t.Fatal(err)
	}
	return admitted, prepared
}

func seedMemoryArtifact(t *testing.T, repo *MemoryRepo, threadID, identity, text string, now time.Time) (artifact.Ref, Entry) {
	t.Helper()
	ctx := context.Background()
	turnID, runID, callID := "turn-"+identity, "run-"+identity, "call-"+identity
	admitted, prepared := prepareMemoryArtifactEffect(t, repo, threadID, turnID, runID, callID, now)
	finished, err := repo.FinishEffectDispatch(ctx, FinishEffectDispatchRequest{
		Lease: admitted.Lease, EffectAttemptID: prepared.Attempt.EffectAttemptID, RequestFingerprint: "effect-" + threadID,
		OutcomeFingerprint: "outcome-" + identity, FullOutput: &artifact.FullOutput{Text: text}, Now: now,
		Result: Entry{ThreadID: threadID, TurnID: turnID, Type: EntryToolResult,
			Message: session.Message{Role: session.Tool, ToolCallID: callID, ToolName: "read", ToolResult: &session.ToolResultView{Status: "success"}}},
	})
	if err != nil || finished.Artifact == nil {
		t.Fatalf("finish artifact effect=%#v err=%v", finished, err)
	}
	if _, err := repo.FinishTurn(ctx, FinishTurnRequest{
		Lease: admitted.Lease, RunID: runID, TerminalEntryID: "terminal-" + identity,
		Status: TurnCompleted, OutcomeFingerprint: "turn-outcome-" + identity, Now: now,
	}); err != nil {
		t.Fatal(err)
	}
	return *finished.Artifact, finished.Result
}

func artifactEntryIDs(entries []Entry) []string {
	ids := make([]string, 0, len(entries))
	for _, entry := range entries {
		ids = append(ids, entry.ID)
	}
	return ids
}

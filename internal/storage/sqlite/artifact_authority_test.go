package sqlite

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/session/artifact"
	"github.com/floegence/floret/internal/sessiontree"
	"github.com/floegence/floret/internal/storage"
)

func TestSQLiteArtifactAuthorityNegativeAndDeleteReadRace(t *testing.T) {
	ctx := context.Background()
	store, _ := openSQLiteStoreForTest(t)
	now := time.Now().UTC()
	for _, meta := range []sessiontree.ThreadMeta{
		{ID: "root"},
		{ID: "child", ParentThreadID: "root", TaskName: "child", AgentPath: "child"},
		{ID: "deep", ParentThreadID: "child", TaskName: "deep", AgentPath: "child/deep"},
		{ID: "sibling", ParentThreadID: "root", TaskName: "sibling", AgentPath: "sibling"},
		{ID: "foreign"},
	} {
		meta.CreatedAt, meta.UpdatedAt = now, now
		if _, err := store.CreateThread(ctx, meta); err != nil {
			t.Fatalf("create %q: %v", meta.ID, err)
		}
	}
	rootRef := seedSQLiteArtifactThroughEffect(t, ctx, store, "root", "root output")
	deepRef := seedSQLiteArtifactThroughEffect(t, ctx, store, "deep", "deep output")

	for _, request := range []sessiontree.ArtifactReadRequest{
		{ThreadID: "root", ArtifactID: rootRef.ID},
		{ParentThreadID: "root", ThreadID: "deep", ArtifactID: deepRef.ID},
		{ParentThreadID: "child", ThreadID: "deep", ArtifactID: deepRef.ID},
	} {
		content, err := store.ReadArtifact(ctx, request)
		if err != nil || content.Ref.ID != request.ArtifactID || content.Text == "" {
			t.Fatalf("authorized read %#v content=%#v err=%v", request, content, err)
		}
	}
	for _, test := range []struct {
		request sessiontree.ArtifactReadRequest
		want    error
	}{
		{sessiontree.ArtifactReadRequest{ThreadID: "deep", ArtifactID: deepRef.ID}, sessiontree.ErrSubAgentParentRequired},
		{sessiontree.ArtifactReadRequest{ParentThreadID: "root", ThreadID: "root", ArtifactID: rootRef.ID}, sessiontree.ErrSubAgentNotFound},
		{sessiontree.ArtifactReadRequest{ParentThreadID: "deep", ThreadID: "child", ArtifactID: deepRef.ID}, sessiontree.ErrSubAgentNotFound},
		{sessiontree.ArtifactReadRequest{ParentThreadID: "sibling", ThreadID: "deep", ArtifactID: deepRef.ID}, sessiontree.ErrSubAgentNotFound},
		{sessiontree.ArtifactReadRequest{ParentThreadID: "root", ThreadID: "foreign", ArtifactID: deepRef.ID}, sessiontree.ErrSubAgentNotFound},
		{sessiontree.ArtifactReadRequest{ThreadID: "root", ArtifactID: deepRef.ID}, sessiontree.ErrArtifactNotFound},
	} {
		content, err := store.ReadArtifact(ctx, test.request)
		if !errors.Is(err, test.want) || content != (sessiontree.ArtifactContent{}) {
			t.Fatalf("unauthorized read %#v content=%#v err=%v want=%v", test.request, content, err, test.want)
		}
	}

	path, err := store.Path(ctx, "root", "")
	if err != nil {
		t.Fatal(err)
	}
	pinnedLeafID := path[len(path)-1].ID
	closure, err := store.ArtifactClosure(ctx, sessiontree.ArtifactClosureRequest{
		SourceThreadID: "root", DestinationThreadID: "collision", EntryIDs: sqliteArtifactEntryIDs(path),
	})
	if err != nil || len(closure.Items) != 1 {
		t.Fatalf("closure=%#v err=%v", closure, err)
	}
	duplicateEntry, err := sessiontree.AppendMessage(ctx, store, "root", "duplicate", session.Message{
		Role: session.Tool, ToolCallID: "duplicate", ToolName: "read",
		ToolResult: &session.ToolResultView{Status: "success", FullOutput: &rootRef},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := store.ArtifactClosure(ctx, sessiontree.ArtifactClosureRequest{
		SourceThreadID: "root", DestinationThreadID: "off-canonical", EntryIDs: []string{duplicateEntry.ID},
	}); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) || !artifact.IsZeroClosure(got) {
		t.Fatalf("off-canonical closure=%#v err=%v", got, err)
	}
	duplicate := artifact.CloneClosure(closure)
	duplicate.DestinationThreadID = "duplicate-destination"
	duplicate.Items = append(duplicate.Items, duplicate.Items[0])
	duplicate.Fingerprint, err = artifact.ClosureFingerprint(duplicate.SourceThreadID, duplicate.DestinationThreadID, duplicate.Items)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Fork(ctx, sessiontree.ForkOptions{
		SourceThreadID: "root", EntryID: pinnedLeafID, EntryIDPinned: true,
		NewThreadID: "duplicate-destination", ArtifactClosure: duplicate, Now: now,
	}); !errors.Is(err, sessiontree.ErrStaleAuthority) {
		t.Fatalf("duplicate closure fork err=%v", err)
	}

	injectSQLiteOrphanArtifact(t, store, "collision", closure.Items[0].Ref, "collision")
	if _, err := store.Fork(ctx, sessiontree.ForkOptions{
		SourceThreadID: "root", EntryID: pinnedLeafID, EntryIDPinned: true,
		NewThreadID: "collision", ArtifactClosure: closure, Now: now,
		RunIDMap: map[string]string{"artifact-run-root": "artifact-run-collision"},
	}); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("artifact collision fork err=%v", err)
	}
	if _, err := store.Thread(ctx, "collision"); !errors.Is(err, sessiontree.ErrThreadNotFound) {
		t.Fatalf("collision published destination: %v", err)
	}
	if count := sqliteArtifactCount(t, store, "collision"); count != 1 {
		t.Fatalf("collision rollback artifact count=%d, want preexisting orphan only", count)
	}

	raceStore, _ := openSQLiteStoreForTest(t)
	if _, err := raceStore.CreateThread(ctx, sessiontree.ThreadMeta{ID: "race", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	raceRef := seedSQLiteArtifactThroughEffect(t, ctx, raceStore, "race", "race output")
	start := make(chan struct{})
	var wg sync.WaitGroup
	var readContent sessiontree.ArtifactContent
	var readErr, deleteErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		readContent, readErr = raceStore.ReadArtifact(ctx, sessiontree.ArtifactReadRequest{ThreadID: "race", ArtifactID: raceRef.ID})
	}()
	go func() {
		defer wg.Done()
		<-start
		_, deleteErr = raceStore.DeleteRootTree(ctx, "race")
	}()
	close(start)
	wg.Wait()
	if deleteErr != nil {
		t.Fatalf("delete race: %v", deleteErr)
	}
	if readErr == nil {
		if readContent.Ref != raceRef || readContent.Text != "race output" {
			t.Fatalf("partial successful race read: %#v", readContent)
		}
	} else if !errors.Is(readErr, sessiontree.ErrThreadDeleted) || readContent != (sessiontree.ArtifactContent{}) {
		t.Fatalf("race read content=%#v err=%v", readContent, readErr)
	}
	if content, err := raceStore.ReadArtifact(ctx, sessiontree.ArtifactReadRequest{ThreadID: "race", ArtifactID: raceRef.ID}); !errors.Is(err, sessiontree.ErrThreadDeleted) || content != (sessiontree.ArtifactContent{}) {
		t.Fatalf("post-delete read content=%#v err=%v", content, err)
	}
}

func TestSQLiteArtifactReadRejectsCorruptAuthorityAndTombstoneAncestry(t *testing.T) {
	ctx := context.Background()
	store, _ := openSQLiteStoreForTest(t)
	now := time.Now().UTC()
	for _, meta := range []sessiontree.ThreadMeta{
		{ID: "root", CreatedAt: now, UpdatedAt: now},
		{ID: "child", ParentThreadID: "root", TaskName: "child", AgentPath: "child", CreatedAt: now, UpdatedAt: now},
	} {
		if _, err := store.CreateThread(ctx, meta); err != nil {
			t.Fatal(err)
		}
	}
	assertCorrupt := func(t *testing.T, req sessiontree.ArtifactReadRequest) {
		t.Helper()
		content, err := store.ReadArtifact(ctx, req)
		if !errors.Is(err, sessiontree.ErrAuthorityCorrupt) || content != (sessiontree.ArtifactContent{}) {
			t.Fatalf("corrupt read %#v content=%#v err=%v", req, content, err)
		}
	}

	if _, err := store.db.ExecContext(ctx, `UPDATE threads SET task_name = 'not-root-metadata' WHERE id = 'root'`); err != nil {
		t.Fatal(err)
	}
	assertCorrupt(t, sessiontree.ArtifactReadRequest{ParentThreadID: "root", ThreadID: "child", ArtifactID: "missing"})
	if _, err := store.db.ExecContext(ctx, `UPDATE threads SET task_name = '' WHERE id = 'root'`); err != nil {
		t.Fatal(err)
	}

	if _, err := store.db.ExecContext(ctx, `UPDATE threads SET task_name = '' WHERE id = 'child'`); err != nil {
		t.Fatal(err)
	}
	assertCorrupt(t, sessiontree.ArtifactReadRequest{ParentThreadID: "root", ThreadID: "child", ArtifactID: "missing"})
	if _, err := store.db.ExecContext(ctx, `UPDATE threads SET task_name = 'child' WHERE id = 'child'`); err != nil {
		t.Fatal(err)
	}

	if _, err := store.db.ExecContext(ctx, `INSERT INTO thread_tombstones(thread_id, root_thread_id, parent_thread_id, deleted_at)
		VALUES('deleted-child', 'root', 'root', ?)`, formatTime(now)); err != nil {
		t.Fatal(err)
	}
	if content, err := store.ReadArtifact(ctx, sessiontree.ArtifactReadRequest{ParentThreadID: "root", ThreadID: "deleted-child", ArtifactID: "missing"}); !errors.Is(err, sessiontree.ErrThreadDeleted) || content != (sessiontree.ArtifactContent{}) {
		t.Fatalf("valid deleted descendant content=%#v err=%v", content, err)
	}
	if _, err := store.db.ExecContext(ctx, `UPDATE thread_tombstones SET parent_thread_id = 'deleted-child' WHERE thread_id = 'deleted-child'`); err != nil {
		t.Fatal(err)
	}
	assertCorrupt(t, sessiontree.ArtifactReadRequest{ParentThreadID: "root", ThreadID: "deleted-child", ArtifactID: "missing"})
	if _, err := store.db.ExecContext(ctx, `UPDATE thread_tombstones SET parent_thread_id = 'missing' WHERE thread_id = 'deleted-child'`); err != nil {
		t.Fatal(err)
	}
	assertCorrupt(t, sessiontree.ArtifactReadRequest{ParentThreadID: "root", ThreadID: "deleted-child", ArtifactID: "missing"})
	if _, err := store.db.ExecContext(ctx, `UPDATE thread_tombstones SET parent_thread_id = 'root', root_thread_id = 'foreign-root' WHERE thread_id = 'deleted-child'`); err != nil {
		t.Fatal(err)
	}
	assertCorrupt(t, sessiontree.ArtifactReadRequest{ParentThreadID: "root", ThreadID: "deleted-child", ArtifactID: "missing"})
}

func TestSQLiteArtifactForkOperationRollbackReplayAndCorruption(t *testing.T) {
	ctx := context.Background()
	store, _ := openSQLiteStoreForTest(t)
	now := time.Now().UTC()
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "source", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	ref := seedSQLiteArtifactThroughEffect(t, ctx, store, "source", "operation output")
	path, err := store.Path(ctx, "source", "")
	if err != nil {
		t.Fatal(err)
	}
	leafID := path[len(path)-1].ID
	closure, err := store.ArtifactClosure(ctx, sessiontree.ArtifactClosureRequest{
		SourceThreadID: "source", DestinationThreadID: "destination", EntryIDs: sqliteArtifactEntryIDs(path),
	})
	if err != nil {
		t.Fatal(err)
	}

	offPathPlan := storage.ForkOperationPlan{
		Version: storage.ForkOperationPlanVersion, OperationID: "off-path", RequestFingerprint: "off-path-fingerprint", PreparedAt: now,
		Root: storage.ForkOperationPlanNode{
			NodeID: "root", SourceThreadID: "source", SourceEntryID: path[0].ID, SourceLeafEntryID: leafID,
			DestinationThreadID: "off-path-destination", ArtifactClosure: artifact.CloneClosure(closure),
			RunIDMap: map[string]string{"artifact-run-source": "artifact-run-off-path"},
		},
	}
	offPathPlan.Root.ArtifactClosure.DestinationThreadID = "off-path-destination"
	offPathPlan.Root.ArtifactClosure.Fingerprint, err = artifact.ClosureFingerprint("source", "off-path-destination", offPathPlan.Root.ArtifactClosure.Items)
	if err != nil {
		t.Fatal(err)
	}
	offPathJSON := mustMarshalSQLiteForkOperationPlan(t, offPathPlan)
	if _, created, err := store.PrepareForkOperation(ctx, storage.ForkOperationRecord{
		OperationID: "off-path", RequestFingerprint: "off-path-fingerprint", SourceThreadIDs: []string{"source"},
		AuthorityThreadIDs: []string{"source", "off-path-destination"}, State: storage.ForkOperationPrepared,
		Plan: offPathJSON, CreatedAt: now, UpdatedAt: now,
	}); !errors.Is(err, sessiontree.ErrStaleAuthority) || created {
		t.Fatalf("off-path prepare created=%v err=%v", created, err)
	}

	plan := storage.ForkOperationPlan{
		Version: storage.ForkOperationPlanVersion, OperationID: "operation", RequestFingerprint: "fingerprint", PreparedAt: now,
		Root: storage.ForkOperationPlanNode{
			NodeID: "root", SourceThreadID: "source", SourceEntryID: leafID, SourceLeafEntryID: leafID,
			DestinationThreadID: "destination", ArtifactClosure: closure,
			RunIDMap: map[string]string{"artifact-run-source": "artifact-run-destination"},
		},
	}
	planJSON := mustMarshalSQLiteForkOperationPlan(t, plan)
	if _, created, err := store.PrepareForkOperation(ctx, storage.ForkOperationRecord{
		OperationID: "operation", RequestFingerprint: "fingerprint", SourceThreadIDs: []string{"source"},
		AuthorityThreadIDs: []string{"source", "destination"}, State: storage.ForkOperationPrepared,
		Plan: planJSON, CreatedAt: now, UpdatedAt: now,
	}); err != nil || !created {
		t.Fatalf("prepare created=%v err=%v", created, err)
	}
	if _, err := store.db.ExecContext(ctx, `CREATE TRIGGER fail_artifact_fork_terminal
		BEFORE UPDATE OF state ON fork_operations WHEN NEW.state = 'completed'
		BEGIN SELECT RAISE(ABORT, 'injected artifact fork terminal failure'); END`); err != nil {
		t.Fatal(err)
	}
	commit := storage.ForkOperationCommitRequest{
		OperationID: "operation", RequestFingerprint: "fingerprint", Plan: planJSON,
		Nodes: storage.ForkOperationPlanNodes(plan), Result: []byte(`{"operation_id":"operation","thread_id":"destination"}`), FinishedAt: now.Add(time.Minute),
	}
	if _, _, err := store.CommitForkOperation(ctx, commit); err == nil {
		t.Fatal("fork commit unexpectedly survived injected terminal failure")
	}
	if _, err := store.Thread(ctx, "destination"); !errors.Is(err, sessiontree.ErrThreadNotFound) {
		t.Fatalf("failed commit published destination: %v", err)
	}
	if count := sqliteArtifactCount(t, store, "destination"); count != 0 {
		t.Fatalf("failed commit left %d destination artifacts", count)
	}
	if record, err := store.ForkOperation(ctx, "operation"); err != nil || record.State != storage.ForkOperationPrepared {
		t.Fatalf("operation after rollback=%#v err=%v", record, err)
	}
	if _, err := store.db.ExecContext(ctx, `DROP TRIGGER fail_artifact_fork_terminal`); err != nil {
		t.Fatal(err)
	}
	committed, replayed, err := store.CommitForkOperation(ctx, commit)
	if err != nil || replayed || committed.State != storage.ForkOperationCompleted {
		t.Fatalf("commit=%#v replayed=%v err=%v", committed, replayed, err)
	}
	content, err := store.ReadArtifact(ctx, sessiontree.ArtifactReadRequest{ThreadID: "destination", ArtifactID: ref.ID})
	if err != nil || content.Ref != ref || content.Text != "operation output" {
		t.Fatalf("destination artifact=%#v err=%v", content, err)
	}
	if _, replayed, err := store.CommitForkOperation(ctx, commit); err != nil || !replayed {
		t.Fatalf("completed replay replayed=%v err=%v", replayed, err)
	}
	if _, err := store.db.ExecContext(ctx, `UPDATE tool_output_artifacts SET text = 'corrupt' WHERE thread_id = 'destination' AND id = ?`, ref.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ForkOperation(ctx, "operation"); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("corrupt completed replay err=%v", err)
	}
}

func TestSQLiteSubAgentArtifactPublicationTriggerRollbackAndReplay(t *testing.T) {
	ctx := context.Background()
	store, _ := openSQLiteStoreForTest(t)
	now := time.Now().UTC()
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "parent", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	ref := seedSQLiteArtifactThroughEffect(t, ctx, store, "parent", "subagent output")
	path, err := store.Path(ctx, "parent", "")
	if err != nil {
		t.Fatal(err)
	}
	leafID := path[len(path)-1].ID
	closure, err := store.ArtifactClosure(ctx, sessiontree.ArtifactClosureRequest{
		SourceThreadID: "parent", DestinationThreadID: "child", EntryIDs: sqliteArtifactEntryIDs(path),
	})
	if err != nil {
		t.Fatal(err)
	}
	request := sessiontree.PublishSubAgentRequest{
		PublicationID: "publication", RequestFingerprint: "fingerprint", ParentThreadID: "parent",
		ChildMeta: sessiontree.ThreadMeta{ID: "child", ParentThreadID: "parent", TaskName: "child", AgentPath: "child", CreatedAt: now, UpdatedAt: now},
		ForkOptions: &sessiontree.ForkOptions{
			SourceThreadID: "parent", EntryID: leafID, EntryIDPinned: true, ExpectedSourceLeafID: leafID, NewThreadID: "child",
			DestinationMeta: &sessiontree.ForkDestinationMeta{ParentThreadID: "parent", TaskName: "child", AgentPath: "child"},
			ArtifactClosure: closure, Now: now,
			RunIDMap: map[string]string{"artifact-run-parent": "artifact-run-child"},
		},
		ArtifactClosure: closure, Message: session.Message{Role: session.User, Content: "work"}, Now: now,
	}
	if _, err := store.db.ExecContext(ctx, `CREATE TRIGGER fail_artifact_subagent_input
		BEFORE INSERT ON subagent_inputs BEGIN SELECT RAISE(ABORT, 'injected subagent input failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PublishSubAgent(ctx, request); err == nil {
		t.Fatal("publication unexpectedly survived injected input failure")
	}
	if _, err := store.Thread(ctx, "child"); !errors.Is(err, sessiontree.ErrThreadNotFound) {
		t.Fatalf("failed publication exposed child: %v", err)
	}
	if count := sqliteArtifactCount(t, store, "child"); count != 0 {
		t.Fatalf("failed publication left %d child artifacts", count)
	}
	var publications int
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM subagent_publications WHERE publication_id = 'publication'`).Scan(&publications); err != nil || publications != 0 {
		t.Fatalf("failed publication ledger count=%d err=%v", publications, err)
	}
	if _, err := store.db.ExecContext(ctx, `DROP TRIGGER fail_artifact_subagent_input`); err != nil {
		t.Fatal(err)
	}
	published, err := store.PublishSubAgent(ctx, request)
	if err != nil || published.Replayed {
		t.Fatalf("publication=%#v err=%v", published, err)
	}
	content, err := store.ReadArtifact(ctx, sessiontree.ArtifactReadRequest{ParentThreadID: "parent", ThreadID: "child", ArtifactID: ref.ID})
	if err != nil || content.Ref != ref || content.Text != "subagent output" {
		t.Fatalf("child artifact=%#v err=%v", content, err)
	}
	replayed, err := store.PublishSubAgent(ctx, request)
	if err != nil || !replayed.Replayed || sqliteArtifactCount(t, store, "child") != 1 {
		t.Fatalf("publication replay=%#v err=%v", replayed, err)
	}
	conflict := request
	conflict.ArtifactClosure = artifact.CloneClosure(request.ArtifactClosure)
	conflict.ArtifactClosure.Fingerprint = "changed"
	if result, err := store.PublishSubAgent(ctx, conflict); !errors.Is(err, sessiontree.ErrRequestConflict) || result.Thread.ID != "" {
		t.Fatalf("changed closure replay=%#v err=%v", result, err)
	}
	if _, err := store.db.ExecContext(ctx, `UPDATE tool_output_artifacts SET text = 'corrupt' WHERE thread_id = 'child' AND id = ?`, ref.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PublishSubAgent(ctx, request); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("corrupt publication replay err=%v", err)
	}
}

func sqliteArtifactEntryIDs(entries []sessiontree.Entry) []string {
	ids := make([]string, 0, len(entries))
	for _, entry := range entries {
		ids = append(ids, entry.ID)
	}
	return ids
}

func sqliteArtifactCount(t *testing.T, store *Store, threadID string) int {
	t.Helper()
	var count int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM tool_output_artifacts WHERE thread_id = ?`, threadID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func injectSQLiteOrphanArtifact(t *testing.T, store *Store, threadID string, ref artifact.Ref, text string) {
	t.Helper()
	store.db.SetMaxOpenConns(1)
	if _, err := store.db.Exec(`PRAGMA foreign_keys = OFF`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`INSERT INTO tool_output_artifacts(
		id, run_id, thread_id, turn_id, prompt_scope_id, step, call_id, tool_name,
		effect_attempt_id, canonical_entry_id, kind, mime, safe_label, size_bytes, sha256, text, metadata_json, created_at
	) VALUES(?, '', ?, '', '', 0, '', '', NULL, 'missing', ?, ?, ?, ?, ?, ?, '{}', ?)`,
		ref.ID, threadID, ref.Kind, ref.MIME, ref.SafeLabel, ref.SizeBytes, ref.SHA256, text, formatTime(time.Now().UTC())); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		t.Fatal(err)
	}
}

package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/sessiontree"
)

func TestSQLiteMigratesSchemaVersion15ThreadTitleAuthority(t *testing.T) {
	path := filepath.Join(t.TempDir(), "title-v15.db")
	createSchemaVersion15TitleStore(t, path, func(db *sql.DB) {
		for _, statement := range []string{
			`INSERT INTO threads(id, created_at, updated_at) VALUES('empty', '2026-07-20T00:00:00Z', '2026-07-20T00:00:00Z')`,
			`INSERT INTO threads(id, title, title_status, title_source, title_updated_at, created_at, updated_at)
				VALUES('provider', 'Provider title', 'ready', 'provider', '2026-07-20T00:01:00Z', '2026-07-20T00:00:00Z', '2026-07-20T00:00:00Z')`,
			`INSERT INTO threads(id, title, title_status, title_source, title_updated_at, created_at, updated_at)
				VALUES('host', 'Host title', 'ready', 'host', '2026-07-20T00:02:00Z', '2026-07-20T00:00:00Z', '2026-07-20T00:00:00Z')`,
			`INSERT INTO threads(id, title_status, title_updated_at, title_error, created_at, updated_at)
				VALUES('failed', 'failed', '2026-07-20T00:03:00Z', 'provider unavailable', '2026-07-20T00:00:00Z', '2026-07-20T00:00:00Z')`,
		} {
			if _, err := db.Exec(statement); err != nil {
				t.Fatal(err)
			}
		}
	})
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if version, err := store.SchemaVersion(context.Background()); err != nil || version != schemaVersion {
		t.Fatalf("schema version=%q err=%v, want %q", version, err, schemaVersion)
	}
	for _, want := range []struct {
		id         string
		generation int64
		token      string
	}{
		{id: "empty"},
		{id: "provider", generation: 1, token: "migrated-v15:provider"},
		{id: "host", generation: 1},
		{id: "failed", generation: 1, token: "migrated-v15:failed"},
	} {
		meta, err := store.Thread(context.Background(), want.id)
		if err != nil {
			t.Fatal(err)
		}
		if meta.TitleGeneration != want.generation || meta.TitleToken != want.token {
			t.Fatalf("migrated title authority %q=%#v", want.id, meta)
		}
	}
	assertSQLiteIndexExists(t, store.db, "threads_pending_title_idx")
}

func TestSQLiteSchemaVersion15InvalidTitleMigrationIsAtomic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invalid-title-v15.db")
	createSchemaVersion15TitleStore(t, path, func(db *sql.DB) {
		if _, err := db.Exec(`INSERT INTO threads(id, title_status, title_updated_at, created_at, updated_at)
			VALUES('invalid', 'pending', '2026-07-20T00:01:00Z', '2026-07-20T00:00:00Z', '2026-07-20T00:00:00Z')`); err != nil {
			t.Fatal(err)
		}
	})
	if _, err := Open(path); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("Open invalid v15 err=%v, want ErrAuthorityCorrupt", err)
	}
	db, err := sql.Open(driverName, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var version, fingerprint string
	if err := db.QueryRow(`SELECT value FROM schema_meta WHERE key = 'schema_version'`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT value FROM schema_meta WHERE key = 'schema_fingerprint'`).Scan(&fingerprint); err != nil {
		t.Fatal(err)
	}
	if version != schemaVersion15 || fingerprint != schemaFingerprintVersion15 {
		t.Fatalf("failed migration metadata version=%q fingerprint=%q", version, fingerprint)
	}
	var addedColumns int
	if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('threads') WHERE name IN ('title_generation', 'title_token')`).Scan(&addedColumns); err != nil {
		t.Fatal(err)
	}
	if addedColumns != 0 {
		t.Fatalf("failed migration left %d title authority columns", addedColumns)
	}
}

func TestSQLitePendingAutomaticTitleFencesSubAgentCloseAcrossReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "pending-title-close.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{
		ID: "child", ParentThreadID: "parent", ParentTurnID: "parent-turn", TaskName: "child", AgentPath: "/root/child",
	}); err != nil {
		t.Fatal(err)
	}
	begin := sessiontree.BeginAutomaticThreadTitleRequest{ThreadID: "child", Token: "title-worker", Now: time.Now().UTC()}
	pending, err := store.BeginAutomaticThreadTitle(ctx, begin)
	if err != nil {
		t.Fatal(err)
	}
	closeRequest := sessiontree.PrepareSubAgentCloseRequest{
		CloseOperationID: "close-child", ParentThreadID: "parent", TargetThreadID: "child", Reason: "done", Now: time.Now().UTC(),
	}
	if _, err := store.PrepareSubAgentClose(ctx, closeRequest); !errors.Is(err, sessiontree.ErrThreadAuthorityBusy) {
		t.Fatalf("PrepareSubAgentClose pending title err = %v, want ErrThreadAuthorityBusy", err)
	}
	meta, err := store.Thread(ctx, "child")
	if err != nil {
		t.Fatal(err)
	}
	if meta.IsClosing() || meta.IsClosed() || meta.TitleStatus != sessiontree.ThreadTitlePending {
		t.Fatalf("pending title close changed child authority: %#v", meta)
	}
	if _, err := store.FailAutomaticThreadTitle(ctx, sessiontree.FailAutomaticThreadTitleRequest{
		ThreadID: "child", Generation: pending.Thread.TitleGeneration, Token: begin.Token,
		Error: "title stopped", Now: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PrepareSubAgentClose(ctx, closeRequest); err != nil {
		t.Fatal(err)
	}
	if _, err := store.FinishSubAgentClose(ctx, sessiontree.FinishSubAgentCloseRequest{
		CloseOperationID: "close-child", ParentThreadID: "parent", TargetThreadID: "child", Reason: "done", Now: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	meta, err = reopened.Thread(ctx, "child")
	if err != nil {
		t.Fatal(err)
	}
	if !meta.IsClosed() || meta.TitleStatus != sessiontree.ThreadTitleFailed {
		t.Fatalf("reopened settled title close state = %#v", meta)
	}
	if pending, err := reopened.PendingAutomaticThreadTitles(ctx); err != nil || len(pending) != 0 {
		t.Fatalf("reopened pending titles = %#v err=%v", pending, err)
	}
}

func TestSQLitePendingAutomaticThreadTitlesUsesIndex(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "pending-title-plan.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	rows, err := store.db.Query(`EXPLAIN QUERY PLAN SELECT id FROM threads
		WHERE title_status = 'pending' ORDER BY title_updated_at, id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, detail)
	}
	if details := strings.Join(plan, "\n"); !strings.Contains(details, "threads_pending_title_idx") {
		t.Fatalf("pending title query plan=%q", details)
	}
}

func createSchemaVersion15TitleStore(t *testing.T, path string, seed func(*sql.DB)) {
	t.Helper()
	db, err := sql.Open(driverName, path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(schemaVersion15SQL); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO schema_meta(key, value) VALUES
		('schema_version', ?), ('raw_encoder_version', ?), ('schema_fingerprint', ?)`,
		schemaVersion15, rawEncoderVersion, schemaFingerprintVersion15); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := persistLeasePolicy(context.Background(), db, sessiontree.DefaultLeasePolicy); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if seed != nil {
		seed(db)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

type titleAuthorityTestRepo interface {
	CreateThread(context.Context, sessiontree.ThreadMeta) (sessiontree.ThreadMeta, error)
	Thread(context.Context, string) (sessiontree.ThreadMeta, error)
	UpdateThread(context.Context, sessiontree.ThreadMeta) error
	AdmitTurn(context.Context, sessiontree.AdmitTurnRequest) (sessiontree.AdmitTurnResult, error)
	FinishTurn(context.Context, sessiontree.FinishTurnRequest) (sessiontree.FinishTurnResult, error)
	SetThreadTitle(context.Context, sessiontree.SetThreadTitleRequest) (sessiontree.ThreadTitleMutationResult, error)
	BeginAutomaticThreadTitle(context.Context, sessiontree.BeginAutomaticThreadTitleRequest) (sessiontree.ThreadTitleMutationResult, error)
	CompleteAutomaticThreadTitle(context.Context, sessiontree.CompleteAutomaticThreadTitleRequest) (sessiontree.ThreadTitleMutationResult, error)
	FailAutomaticThreadTitle(context.Context, sessiontree.FailAutomaticThreadTitleRequest) (sessiontree.ThreadTitleMutationResult, error)
	PendingAutomaticThreadTitles(context.Context) ([]sessiontree.ThreadMeta, error)
	DeleteRootTree(context.Context, string) (sessiontree.DeleteRootTreeResult, error)
}

func TestThreadTitleAuthorityMatchesMemory(t *testing.T) {
	now := time.Date(2026, 7, 19, 8, 0, 0, 0, time.UTC)
	policy := sessiontree.LeasePolicy{TTL: time.Hour, RenewInterval: 20 * time.Minute, ClockSkewAllowance: time.Second}
	memory, err := sessiontree.NewMemoryRepoWithLeasePolicy(policy, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	sqliteStore, err := Open(filepath.Join(t.TempDir(), "title-authority.db"), WithLeasePolicy(policy), WithAuthorityClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqliteStore.Close() })

	for backend, repo := range map[string]titleAuthorityTestRepo{"memory": memory, "sqlite": sqliteStore} {
		t.Run(backend, func(t *testing.T) {
			testAutomaticThreadTitleCAS(t, repo, now)
			testAutomaticThreadTitleFailureRetry(t, repo, now)
			testManualThreadTitleInvalidatesAutomaticWriter(t, repo, now)
			testGenericThreadUpdateCannotBypassTitleAuthority(t, repo, now)
			testPendingAutomaticThreadTitles(t, repo, now)
			testManualThreadTitleWinsConcurrentAutomaticCompletion(t, repo, now)
			testThreadTitleLeaseAndLifecycle(t, repo, now)
		})
	}
}

func testGenericThreadUpdateCannotBypassTitleAuthority(t *testing.T, repo titleAuthorityTestRepo, now time.Time) {
	t.Helper()
	meta, err := repo.CreateThread(context.Background(), sessiontree.ThreadMeta{ID: "title-update-bypass"})
	if err != nil {
		t.Fatal(err)
	}
	meta.Title = "Bypass"
	meta.TitleStatus = sessiontree.ThreadTitleReady
	meta.TitleSource = sessiontree.ThreadTitleSourceHost
	meta.TitleUpdatedAt = now
	meta.TitleGeneration = 1
	if err := repo.UpdateThread(context.Background(), meta); !errors.Is(err, sessiontree.ErrRequestConflict) {
		t.Fatalf("UpdateThread title bypass err=%v, want ErrRequestConflict", err)
	}
	got, err := repo.Thread(context.Background(), meta.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Title != "" || got.TitleStatus != "" || got.TitleGeneration != 0 {
		t.Fatalf("UpdateThread title bypass mutated state: %#v", got)
	}
}

func testManualThreadTitleWinsConcurrentAutomaticCompletion(t *testing.T, repo titleAuthorityTestRepo, now time.Time) {
	t.Helper()
	for iteration := 0; iteration < 16; iteration++ {
		threadID := fmt.Sprintf("title-race-%d", iteration)
		if _, err := repo.CreateThread(context.Background(), sessiontree.ThreadMeta{ID: threadID}); err != nil {
			t.Fatal(err)
		}
		begin := sessiontree.BeginAutomaticThreadTitleRequest{ThreadID: threadID, Token: "worker", Now: now}
		pending, err := repo.BeginAutomaticThreadTitle(context.Background(), begin)
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		var manualErr, automaticErr error
		go func() {
			defer wg.Done()
			<-start
			_, manualErr = repo.SetThreadTitle(context.Background(), sessiontree.SetThreadTitleRequest{
				ThreadID: threadID, Title: "Manual title", Now: now.Add(2 * time.Minute),
			})
		}()
		go func() {
			defer wg.Done()
			<-start
			_, automaticErr = repo.CompleteAutomaticThreadTitle(context.Background(), sessiontree.CompleteAutomaticThreadTitleRequest{
				ThreadID: threadID, Generation: pending.Thread.TitleGeneration, Token: begin.Token,
				Title: "Automatic title", Now: now.Add(time.Minute),
			})
		}()
		close(start)
		wg.Wait()
		if manualErr != nil {
			t.Fatalf("manual race err=%v", manualErr)
		}
		if automaticErr != nil && !errors.Is(automaticErr, sessiontree.ErrStaleAuthority) {
			t.Fatalf("automatic race err=%v", automaticErr)
		}
		meta, err := repo.Thread(context.Background(), threadID)
		if err != nil {
			t.Fatal(err)
		}
		if meta.Title != "Manual title" || meta.TitleSource != sessiontree.ThreadTitleSourceHost || meta.TitleToken != "" {
			t.Fatalf("manual did not win race: %#v", meta)
		}
	}
}

func testPendingAutomaticThreadTitles(t *testing.T, repo titleAuthorityTestRepo, now time.Time) {
	t.Helper()
	for _, item := range []struct {
		id    string
		token string
		at    time.Time
	}{
		{id: "pending-later", token: "later", at: now.Add(2 * time.Minute)},
		{id: "pending-first-b", token: "first-b", at: now.Add(time.Minute)},
		{id: "pending-first-a", token: "first-a", at: now.Add(time.Minute)},
	} {
		if _, err := repo.CreateThread(context.Background(), sessiontree.ThreadMeta{ID: item.id}); err != nil {
			t.Fatal(err)
		}
		if _, err := repo.BeginAutomaticThreadTitle(context.Background(), sessiontree.BeginAutomaticThreadTitleRequest{
			ThreadID: item.id, Token: item.token, Now: item.at,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := repo.CreateThread(context.Background(), sessiontree.ThreadMeta{ID: "not-pending"}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.SetThreadTitle(context.Background(), sessiontree.SetThreadTitleRequest{
		ThreadID: "not-pending", Title: "Manual", Now: now,
	}); err != nil {
		t.Fatal(err)
	}
	pending, err := repo.PendingAutomaticThreadTitles(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"pending-first-a", "pending-first-b", "pending-later"}
	if len(pending) != len(want) {
		t.Fatalf("pending titles=%#v, want ids=%#v", pending, want)
	}
	for index, id := range want {
		if pending[index].ID != id || pending[index].TitleStatus != sessiontree.ThreadTitlePending || pending[index].TitleGeneration != 1 {
			t.Fatalf("pending[%d]=%#v, want id=%q", index, pending[index], id)
		}
	}
}

func testAutomaticThreadTitleCAS(t *testing.T, repo titleAuthorityTestRepo, now time.Time) {
	t.Helper()
	threadID := "automatic-cas"
	created, err := repo.CreateThread(context.Background(), sessiontree.ThreadMeta{
		ID: threadID, ForkedFromThreadID: "source", ForkedFromEntryID: "source-entry",
		ForkOperationID: "fork-operation", ForkOperationNodeID: "fork-node",
	})
	if err != nil {
		t.Fatal(err)
	}

	begin := sessiontree.BeginAutomaticThreadTitleRequest{ThreadID: threadID, Token: "worker-1", Now: now}
	pending, err := repo.BeginAutomaticThreadTitle(context.Background(), begin)
	if err != nil || !pending.Changed {
		t.Fatalf("begin result=%#v err=%v", pending, err)
	}
	if pending.Thread.Title != "" || pending.Thread.TitleStatus != sessiontree.ThreadTitlePending || pending.Thread.TitleSource != "" ||
		pending.Thread.TitleGeneration != 1 || pending.Thread.TitleToken != begin.Token || !pending.Thread.TitleUpdatedAt.Equal(now) {
		t.Fatalf("pending title state=%#v", pending.Thread)
	}
	assertTitleLineageUnchanged(t, created, pending.Thread)

	replayBegin := begin
	replayBegin.Now = now.Add(time.Minute)
	replayed, err := repo.BeginAutomaticThreadTitle(context.Background(), replayBegin)
	if err != nil || replayed.Changed || !replayed.Thread.TitleUpdatedAt.Equal(now) || replayed.Thread.TitleGeneration != 1 {
		t.Fatalf("idempotent begin result=%#v err=%v", replayed, err)
	}

	conflictingBegin := begin
	conflictingBegin.Token = "worker-2"
	if _, err := repo.BeginAutomaticThreadTitle(context.Background(), conflictingBegin); !errors.Is(err, sessiontree.ErrRequestConflict) {
		t.Fatalf("conflicting begin err=%v, want ErrRequestConflict", err)
	}
	assertThreadTitleState(t, repo, pending.Thread)

	wrongToken := sessiontree.CompleteAutomaticThreadTitleRequest{
		ThreadID: threadID, Generation: 1, Token: "wrong", Title: "Generated title", Now: now.Add(2 * time.Minute),
	}
	if _, err := repo.CompleteAutomaticThreadTitle(context.Background(), wrongToken); !errors.Is(err, sessiontree.ErrStaleAuthority) {
		t.Fatalf("wrong-token completion err=%v, want ErrStaleAuthority", err)
	}
	wrongGeneration := wrongToken
	wrongGeneration.Token = begin.Token
	wrongGeneration.Generation = 2
	if _, err := repo.CompleteAutomaticThreadTitle(context.Background(), wrongGeneration); !errors.Is(err, sessiontree.ErrStaleAuthority) {
		t.Fatalf("wrong-generation completion err=%v, want ErrStaleAuthority", err)
	}
	assertThreadTitleState(t, repo, pending.Thread)

	complete := wrongToken
	complete.Token = begin.Token
	ready, err := repo.CompleteAutomaticThreadTitle(context.Background(), complete)
	if err != nil || !ready.Changed {
		t.Fatalf("complete result=%#v err=%v", ready, err)
	}
	if ready.Thread.Title != complete.Title || ready.Thread.TitleStatus != sessiontree.ThreadTitleReady ||
		ready.Thread.TitleSource != sessiontree.ThreadTitleSourceProvider || ready.Thread.TitleGeneration != 1 ||
		ready.Thread.TitleToken != begin.Token || !ready.Thread.TitleUpdatedAt.Equal(complete.Now) {
		t.Fatalf("ready title state=%#v", ready.Thread)
	}

	replayComplete := complete
	replayComplete.Now = now.Add(3 * time.Minute)
	replayed, err = repo.CompleteAutomaticThreadTitle(context.Background(), replayComplete)
	if err != nil || replayed.Changed || !replayed.Thread.TitleUpdatedAt.Equal(complete.Now) {
		t.Fatalf("idempotent completion result=%#v err=%v", replayed, err)
	}
	conflictingComplete := complete
	conflictingComplete.Title = "Different title"
	if _, err := repo.CompleteAutomaticThreadTitle(context.Background(), conflictingComplete); !errors.Is(err, sessiontree.ErrRequestConflict) {
		t.Fatalf("conflicting completion err=%v, want ErrRequestConflict", err)
	}

	ignored, err := repo.BeginAutomaticThreadTitle(context.Background(), sessiontree.BeginAutomaticThreadTitleRequest{
		ThreadID: threadID, Token: "worker-after-ready", Now: now.Add(4 * time.Minute),
	})
	if err != nil || ignored.Changed || ignored.Thread.Title != complete.Title || ignored.Thread.TitleGeneration != 1 {
		t.Fatalf("begin after ready result=%#v err=%v", ignored, err)
	}
}

func testAutomaticThreadTitleFailureRetry(t *testing.T, repo titleAuthorityTestRepo, now time.Time) {
	t.Helper()
	threadID := "automatic-retry"
	if _, err := repo.CreateThread(context.Background(), sessiontree.ThreadMeta{ID: threadID}); err != nil {
		t.Fatal(err)
	}
	firstBegin := sessiontree.BeginAutomaticThreadTitleRequest{ThreadID: threadID, Token: "attempt-1", Now: now}
	first, err := repo.BeginAutomaticThreadTitle(context.Background(), firstBegin)
	if err != nil {
		t.Fatal(err)
	}
	fail := sessiontree.FailAutomaticThreadTitleRequest{
		ThreadID: threadID, Generation: first.Thread.TitleGeneration, Token: firstBegin.Token,
		Error: "summary provider unavailable", Now: now.Add(time.Minute),
	}
	failed, err := repo.FailAutomaticThreadTitle(context.Background(), fail)
	if err != nil || !failed.Changed || failed.Thread.TitleStatus != sessiontree.ThreadTitleFailed ||
		failed.Thread.TitleError != fail.Error || failed.Thread.TitleGeneration != 1 || failed.Thread.TitleToken != firstBegin.Token {
		t.Fatalf("failed result=%#v err=%v", failed, err)
	}

	replayed, err := repo.FailAutomaticThreadTitle(context.Background(), fail)
	if err != nil || replayed.Changed || !replayed.Thread.TitleUpdatedAt.Equal(fail.Now) {
		t.Fatalf("idempotent failure result=%#v err=%v", replayed, err)
	}
	conflictingFailure := fail
	conflictingFailure.Error = "different failure"
	if _, err := repo.FailAutomaticThreadTitle(context.Background(), conflictingFailure); !errors.Is(err, sessiontree.ErrRequestConflict) {
		t.Fatalf("conflicting failure err=%v, want ErrRequestConflict", err)
	}
	if _, err := repo.CompleteAutomaticThreadTitle(context.Background(), sessiontree.CompleteAutomaticThreadTitleRequest{
		ThreadID: threadID, Generation: 1, Token: firstBegin.Token, Title: "late title", Now: now.Add(2 * time.Minute),
	}); !errors.Is(err, sessiontree.ErrRequestConflict) {
		t.Fatalf("completion after failure err=%v, want ErrRequestConflict", err)
	}

	secondBegin := sessiontree.BeginAutomaticThreadTitleRequest{ThreadID: threadID, Token: "attempt-2", Now: now.Add(3 * time.Minute)}
	second, err := repo.BeginAutomaticThreadTitle(context.Background(), secondBegin)
	if err != nil || !second.Changed || second.Thread.TitleStatus != sessiontree.ThreadTitlePending || second.Thread.TitleGeneration != 2 {
		t.Fatalf("retry begin result=%#v err=%v", second, err)
	}
	ready, err := repo.CompleteAutomaticThreadTitle(context.Background(), sessiontree.CompleteAutomaticThreadTitleRequest{
		ThreadID: threadID, Generation: 2, Token: secondBegin.Token, Title: "Recovered title", Now: now.Add(4 * time.Minute),
	})
	if err != nil || !ready.Changed || ready.Thread.Title != "Recovered title" || ready.Thread.TitleError != "" {
		t.Fatalf("retry completion result=%#v err=%v", ready, err)
	}
}

func testManualThreadTitleInvalidatesAutomaticWriter(t *testing.T, repo titleAuthorityTestRepo, now time.Time) {
	t.Helper()
	threadID := "manual-wins"
	if _, err := repo.CreateThread(context.Background(), sessiontree.ThreadMeta{ID: threadID}); err != nil {
		t.Fatal(err)
	}
	begin := sessiontree.BeginAutomaticThreadTitleRequest{ThreadID: threadID, Token: "automatic", Now: now}
	pending, err := repo.BeginAutomaticThreadTitle(context.Background(), begin)
	if err != nil {
		t.Fatal(err)
	}
	manual := sessiontree.SetThreadTitleRequest{ThreadID: threadID, Title: "Manual title", Now: now.Add(time.Minute)}
	owned, err := repo.SetThreadTitle(context.Background(), manual)
	if err != nil || !owned.Changed {
		t.Fatalf("manual result=%#v err=%v", owned, err)
	}
	if owned.Thread.Title != manual.Title || owned.Thread.TitleStatus != sessiontree.ThreadTitleReady ||
		owned.Thread.TitleSource != sessiontree.ThreadTitleSourceHost || owned.Thread.TitleGeneration != 2 || owned.Thread.TitleToken != "" {
		t.Fatalf("manual title state=%#v", owned.Thread)
	}

	idempotent := manual
	idempotent.Now = now.Add(2 * time.Minute)
	replayed, err := repo.SetThreadTitle(context.Background(), idempotent)
	if err != nil || replayed.Changed || replayed.Thread.TitleGeneration != 2 || !replayed.Thread.TitleUpdatedAt.Equal(manual.Now) {
		t.Fatalf("idempotent manual result=%#v err=%v", replayed, err)
	}

	lateComplete := sessiontree.CompleteAutomaticThreadTitleRequest{
		ThreadID: threadID, Generation: pending.Thread.TitleGeneration, Token: begin.Token,
		Title: "Late automatic title", Now: now.Add(3 * time.Minute),
	}
	if _, err := repo.CompleteAutomaticThreadTitle(context.Background(), lateComplete); !errors.Is(err, sessiontree.ErrStaleAuthority) {
		t.Fatalf("late automatic completion err=%v, want ErrStaleAuthority", err)
	}
	lateFailure := sessiontree.FailAutomaticThreadTitleRequest{
		ThreadID: threadID, Generation: pending.Thread.TitleGeneration, Token: begin.Token,
		Error: "late failure", Now: now.Add(3 * time.Minute),
	}
	if _, err := repo.FailAutomaticThreadTitle(context.Background(), lateFailure); !errors.Is(err, sessiontree.ErrStaleAuthority) {
		t.Fatalf("late automatic failure err=%v, want ErrStaleAuthority", err)
	}
	assertThreadTitleState(t, repo, owned.Thread)
}

func testThreadTitleLeaseAndLifecycle(t *testing.T, repo titleAuthorityTestRepo, now time.Time) {
	t.Helper()
	threadID := "lease-lifecycle"
	if _, err := repo.CreateThread(context.Background(), sessiontree.ThreadMeta{ID: threadID}); err != nil {
		t.Fatal(err)
	}
	admitted, err := repo.AdmitTurn(context.Background(), sessiontree.AdmitTurnRequest{
		ThreadID: threadID, TurnID: "turn", RunID: "run", OwnerID: "owner",
		RequestFingerprint: "admit", Input: session.Message{Role: session.User, Content: "hello"}, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	begin := sessiontree.BeginAutomaticThreadTitleRequest{ThreadID: threadID, Token: "active-worker", Now: now.Add(time.Minute)}
	if result, err := repo.BeginAutomaticThreadTitle(context.Background(), begin); err != nil || !result.Changed || result.Thread.TitleStatus != sessiontree.ThreadTitlePending {
		t.Fatalf("begin beside active turn result=%#v err=%v", result, err)
	}
	manual := sessiontree.SetThreadTitleRequest{ThreadID: threadID, Title: "manual", Now: now.Add(2 * time.Minute)}
	if result, err := repo.SetThreadTitle(context.Background(), manual); err != nil || !result.Changed || result.Thread.TitleSource != sessiontree.ThreadTitleSourceHost {
		t.Fatalf("manual title beside active turn result=%#v err=%v", result, err)
	}
	if _, err := repo.FinishTurn(context.Background(), sessiontree.FinishTurnRequest{
		Lease: admitted.Lease, RunID: "run", TerminalEntryID: "terminal", Status: sessiontree.TurnCompleted,
		OutcomeFingerprint: "finish", Now: now.Add(3 * time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.DeleteRootTree(context.Background(), threadID); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.SetThreadTitle(context.Background(), manual); !errors.Is(err, sessiontree.ErrThreadDeleted) {
		t.Fatalf("deleted thread title err=%v, want ErrThreadDeleted", err)
	}
	if _, err := repo.CompleteAutomaticThreadTitle(context.Background(), sessiontree.CompleteAutomaticThreadTitleRequest{
		ThreadID: threadID, Generation: 1, Token: begin.Token, Title: "late", Now: now.Add(4 * time.Minute),
	}); !errors.Is(err, sessiontree.ErrThreadDeleted) {
		t.Fatalf("deleted thread completion err=%v, want ErrThreadDeleted", err)
	}
}

func assertTitleLineageUnchanged(t *testing.T, before, after sessiontree.ThreadMeta) {
	t.Helper()
	if after.ForkedFromThreadID != before.ForkedFromThreadID || after.ForkedFromEntryID != before.ForkedFromEntryID ||
		after.ForkOperationID != before.ForkOperationID || after.ForkOperationNodeID != before.ForkOperationNodeID {
		t.Fatalf("title mutation changed fork lineage: before=%#v after=%#v", before, after)
	}
}

func assertThreadTitleState(t *testing.T, repo titleAuthorityTestRepo, want sessiontree.ThreadMeta) {
	t.Helper()
	got, err := repo.Thread(context.Background(), want.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Title != want.Title || got.TitleStatus != want.TitleStatus || got.TitleSource != want.TitleSource ||
		got.TitleGeneration != want.TitleGeneration || got.TitleToken != want.TitleToken || got.TitleError != want.TitleError ||
		!got.TitleUpdatedAt.Equal(want.TitleUpdatedAt) {
		t.Fatalf("title state changed: got=%#v want=%#v", got, want)
	}
}

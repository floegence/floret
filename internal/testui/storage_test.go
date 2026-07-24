package testui

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/config"
	"github.com/floegence/floret/internal/engine"
	"github.com/floegence/floret/internal/provider"
	"github.com/floegence/floret/internal/testing/harness"
	flruntime "github.com/floegence/floret/runtime"
)

func TestTestUIStorageDeleteSessionUsesBoundPublicCapability(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []string{StorageModeMemory, StorageModeSQLite} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			runner := NewRunner(root)
			runner.StorageMode = mode
			t.Cleanup(func() { _ = runner.Close() })
			store, err := runner.sessionStorage(ctx)
			if err != nil {
				t.Fatal(err)
			}
			create, err := store.capabilities.create.Bind("parent", "create-parent")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := create.CreateThread(ctx, flruntime.CreateThreadRequest{ThreadID: "parent", CreateIntentID: "create-parent"}); err != nil {
				t.Fatal(err)
			}
			if err := store.saveMetadata(ctx, agentSessionMetadata{Version: agentSessionMetadataVersion, ID: "parent"}); err != nil {
				t.Fatal(err)
			}
			deleted, err := store.deleteSession(ctx, "parent")
			if err != nil || len(deleted) != 1 || deleted[0] != "parent" {
				t.Fatalf("delete result=%v err=%v", deleted, err)
			}
			if _, err := store.capabilities.read.NewHost(ctx, "parent"); !errors.Is(err, flruntime.ErrThreadDeleted) {
				t.Fatalf("read host after delete err=%v, want ErrThreadDeleted", err)
			}
			if _, err := store.loadMetadata(ctx, "parent"); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("metadata after delete err=%v, want not exist", err)
			}
		})
	}
}

func TestTestUIStorageRejectsFileAuthorityFallback(t *testing.T) {
	runner := NewRunner(t.TempDir())
	runner.StorageMode = StorageModeFile
	if _, err := runner.sessionStorage(context.Background()); !errors.Is(err, flruntime.ErrUnsupportedStoreCapability) {
		t.Fatalf("file storage error=%v, want ErrUnsupportedStoreCapability", err)
	}
}

func TestSQLiteStoragePathRejectsHostMetadataDomainCollision(t *testing.T) {
	root := t.TempDir()
	first := NewRunner(root)
	first.StoragePath = filepath.Join(root, "custom.db")
	store, err := first.sessionStorage(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Close() })

	second := NewRunner(root)
	second.StoragePath = store.metadataPath
	if _, err := second.sessionStorage(context.Background()); err == nil || !strings.Contains(err.Error(), "reserved host-metadata path domain") {
		t.Fatalf("colliding custom StoragePath error = %v", err)
	}
}

func TestSQLiteStoragePathRejectsMemoryDatabase(t *testing.T) {
	runner := NewRunner(t.TempDir())
	runner.StoragePath = ":memory:"
	if _, err := runner.sessionStorage(context.Background()); err == nil || !strings.Contains(err.Error(), "use memory storage mode") {
		t.Fatalf("sqlite :memory: storage error = %v", err)
	}
}

func TestSQLiteHostMetadataPathFollowsCanonicalRuntimeDatabase(t *testing.T) {
	root := t.TempDir()
	realPath := filepath.Join(root, "runtime.db")
	seed := NewRunner(root)
	seed.StoragePath = realPath
	seedStore, err := seed.sessionStorage(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	wantMetadataPath := seedStore.metadataPath
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}

	aliasPath := filepath.Join(root, "runtime-alias.db")
	if err := os.Symlink(realPath, aliasPath); err != nil {
		t.Skipf("create database symlink: %v", err)
	}
	alias := NewRunner(root)
	alias.StoragePath = aliasPath
	aliasStore, err := alias.sessionStorage(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = alias.Close() })
	if aliasStore.metadataPath != wantMetadataPath {
		t.Fatalf("symlinked runtime metadata path = %q, want %q", aliasStore.metadataPath, wantMetadataPath)
	}

	metadataAlias := filepath.Join(root, "metadata-alias.db")
	if err := os.Symlink(wantMetadataPath, metadataAlias); err != nil {
		t.Skipf("create metadata symlink: %v", err)
	}
	colliding := NewRunner(root)
	colliding.StoragePath = metadataAlias
	if _, err := colliding.sessionStorage(context.Background()); err == nil || !strings.Contains(err.Error(), "reserved host-metadata path domain") {
		t.Fatalf("symlinked metadata collision error = %v", err)
	}
}

func TestSQLiteHostMetadataMigratesNonEmptyLegacySidecarExactlyOnce(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	runtimePath := filepath.Join(root, "runtime.db")
	runtimeStore, err := openTestUIRuntimeStore(ctx, runtimePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtimeStore.Close(); err != nil {
		t.Fatal(err)
	}
	metadataPath := runtimePath + hostMetadataPathSuffix
	legacy, err := sql.Open("sqlite", metadataPath)
	if err != nil {
		t.Fatal(err)
	}
	legacyData := `{"version":2,"id":"session","created_at":"2026-07-21T12:00:00Z","updated_at":"2026-07-21T12:01:00Z"}`
	if _, err := legacy.ExecContext(ctx, `CREATE TABLE metadata_records(
		namespace TEXT NOT NULL,
		id TEXT NOT NULL,
		created_at TEXT NOT NULL,
		updated_at TEXT NOT NULL,
		data_json TEXT NOT NULL,
		PRIMARY KEY(namespace, id)
	);
	CREATE INDEX metadata_records_namespace_updated_idx ON metadata_records(namespace, updated_at, id);
	INSERT INTO metadata_records(namespace, id, created_at, updated_at, data_json) VALUES(?, ?, ?, ?, ?)`,
		agentSessionMetadataNamespace, "session", "2026-07-21T12:00:00Z", "2026-07-21T12:01:00Z", legacyData); err != nil {
		t.Fatal(err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}

	runner := NewRunner(root)
	runner.StoragePath = runtimePath
	store, err := runner.sessionStorage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := testMetadataTableColumns(t, store.metadataDB); strings.Join(got, ",") != "namespace,id,data_json" {
		t.Fatalf("migrated metadata columns = %v", got)
	}
	loaded, err := store.loadMetadata(ctx, "session")
	if err != nil || loaded.ID != "session" || loaded.Version != agentSessionMetadataVersion {
		t.Fatalf("migrated metadata = %#v err=%v", loaded, err)
	}
	var migratedData string
	if err := store.metadataDB.QueryRowContext(ctx, `SELECT data_json FROM metadata_records WHERE namespace = ? AND id = ?`, agentSessionMetadataNamespace, "session").Scan(&migratedData); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(migratedData, "created_at") || strings.Contains(migratedData, "updated_at") {
		t.Fatalf("migrated metadata retained canonical timestamps: %s", migratedData)
	}
	if err := runner.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := NewRunner(root)
	reopened.StoragePath = runtimePath
	reopenedStore, err := reopened.sessionStorage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if got := testMetadataTableColumns(t, reopenedStore.metadataDB); strings.Join(got, ",") != "namespace,id,data_json" {
		t.Fatalf("reopened metadata columns = %v", got)
	}
	if loaded, err := reopenedStore.loadMetadata(ctx, "session"); err != nil || loaded.ID != "session" {
		t.Fatalf("reopened migrated metadata = %#v err=%v", loaded, err)
	}
}

func TestSQLiteHostMetadataMigrationRejectsInvalidRecordAtomically(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	runtimePath := filepath.Join(root, "runtime.db")
	runtimeStore, err := openTestUIRuntimeStore(ctx, runtimePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtimeStore.Close(); err != nil {
		t.Fatal(err)
	}
	metadataPath := runtimePath + hostMetadataPathSuffix
	legacy, err := sql.Open("sqlite", metadataPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := legacy.ExecContext(ctx, `CREATE TABLE metadata_records(
		namespace TEXT NOT NULL,
		id TEXT NOT NULL,
		created_at TEXT NOT NULL,
		updated_at TEXT NOT NULL,
		data_json TEXT NOT NULL,
		PRIMARY KEY(namespace, id)
	);
	INSERT INTO metadata_records(namespace, id, created_at, updated_at, data_json) VALUES(?, ?, '', '', ?)`,
		agentSessionMetadataNamespace, "broken", `{"version":2,"id":"broken","turns":[]}`); err != nil {
		t.Fatal(err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}

	runner := NewRunner(root)
	runner.StoragePath = runtimePath
	if _, err := runner.sessionStorage(ctx); err == nil || !strings.Contains(err.Error(), `unknown field "turns"`) {
		t.Fatalf("invalid migration error = %v", err)
	}
	verify, err := sql.Open("sqlite", metadataPath)
	if err != nil {
		t.Fatal(err)
	}
	defer verify.Close()
	if got := testMetadataTableColumns(t, verify); strings.Join(got, ",") != "namespace,id,created_at,updated_at,data_json" {
		t.Fatalf("failed migration changed schema: %v", got)
	}
	var records int
	if err := verify.QueryRowContext(ctx, `SELECT COUNT(*) FROM metadata_records`).Scan(&records); err != nil || records != 1 {
		t.Fatalf("failed migration records=%d err=%v", records, err)
	}
}

func testMetadataTableColumns(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.Query(`PRAGMA table_info(metadata_records)`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var cid int
		var name, columnType string
		var notNull, primaryKey int
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			t.Fatal(err)
		}
		columns = append(columns, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return columns
}

func TestSQLiteHostMetadataWriterAdmissionCancellationReleasesQueue(t *testing.T) {
	runner := NewRunner(t.TempDir())
	store, err := runner.sessionStorage(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runner.Close() })
	releaseBlocker, err := store.metadataWriter.Reserve(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer releaseBlocker()
	cancelled, cancel := context.WithCancel(context.Background())
	failed := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		failed <- store.withMetadataImmediate(cancelled, func(*sql.Conn) error {
			return errors.New("metadata mutation ran before writer admission")
		})
	}()
	<-started
	cancel()
	select {
	case err := <-failed:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled metadata writer error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled metadata begin did not return")
	}
	releaseBlocker()
	if err := store.withMetadataImmediate(context.Background(), func(conn *sql.Conn) error {
		_, err := conn.ExecContext(context.Background(), `INSERT INTO metadata_records(namespace, id, data_json)
			VALUES(?, ?, '{}')`, agentSessionMetadataNamespace, "after-cancel")
		return err
	}); err != nil {
		t.Fatalf("metadata connection retained a transaction after begin failure: %v", err)
	}
}

func TestSQLiteHostMetadataUncoordinatedWriterConflictDoesNotRetainTransaction(t *testing.T) {
	runner := NewRunner(t.TempDir())
	store, err := runner.sessionStorage(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runner.Close() })

	blockerDB, err := sql.Open("sqlite", store.metadataPath)
	if err != nil {
		t.Fatal(err)
	}
	defer blockerDB.Close()
	blocker, err := blockerDB.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	if _, err := blocker.ExecContext(context.Background(), "BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}

	failed := make(chan error, 1)
	go func() {
		failed <- store.withMetadataImmediate(context.Background(), func(*sql.Conn) error {
			return errors.New("metadata mutation ran without writer ownership")
		})
	}()
	select {
	case err := <-failed:
		if err == nil || strings.Contains(err.Error(), "metadata mutation ran") {
			t.Fatalf("uncoordinated writer conflict error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("uncoordinated writer conflict waited instead of returning")
	}
	if _, err := blocker.ExecContext(context.Background(), "ROLLBACK"); err != nil {
		t.Fatal(err)
	}
	if err := store.withMetadataImmediate(context.Background(), func(conn *sql.Conn) error {
		_, err := conn.ExecContext(context.Background(), `INSERT INTO metadata_records(namespace, id, data_json)
			VALUES(?, ?, '{}')`, agentSessionMetadataNamespace, "after-conflict")
		return err
	}); err != nil {
		t.Fatalf("metadata connection retained a transaction after writer conflict: %v", err)
	}
}

func TestSQLiteHostMetadataNeverReadsRuntimeMetadataRecords(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	runtimePath := filepath.Join(root, "legacy.db")
	runtimeStore, err := openTestUIRuntimeStore(ctx, runtimePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtimeStore.Close(); err != nil {
		t.Fatal(err)
	}
	legacy, err := sql.Open("sqlite", runtimePath)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	meta := agentSessionMetadata{Version: agentSessionMetadataVersion, ID: "session"}
	raw, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := legacy.ExecContext(ctx, `INSERT INTO metadata_records(namespace, id, created_at, updated_at, data_json)
		VALUES(?, ?, ?, ?, ?)`, agentSessionMetadataNamespace, meta.ID, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano), string(raw)); err != nil {
		t.Fatal(err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}

	runner := NewRunner(root)
	runner.StoragePath = runtimePath
	store, err := runner.sessionStorage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runner.Close() })
	if _, err := store.loadMetadata(ctx, meta.ID); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("host metadata read runtime database record: %v", err)
	}
	var sidecarRecords int
	if err := store.metadataDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM metadata_records`).Scan(&sidecarRecords); err != nil {
		t.Fatal(err)
	}
	if sidecarRecords != 0 {
		t.Fatalf("sidecar imported %d runtime metadata records", sidecarRecords)
	}
	meta.ProfileID = "sidecar-profile"
	if err := store.saveMetadata(ctx, meta); err != nil {
		t.Fatal(err)
	}
	if err := runner.Close(); err != nil {
		t.Fatal(err)
	}

	legacy, err = sql.Open("sqlite", runtimePath)
	if err != nil {
		t.Fatal(err)
	}
	legacyMeta := meta
	legacyMeta.ProfileID = "runtime-profile"
	legacyRaw, err := json.Marshal(legacyMeta)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := legacy.ExecContext(ctx, `UPDATE metadata_records SET data_json = ? WHERE namespace = ? AND id = ?`, string(legacyRaw), agentSessionMetadataNamespace, meta.ID); err != nil {
		t.Fatal(err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := NewRunner(root)
	reopened.StoragePath = runtimePath
	reopenedStore, err := reopened.sessionStorage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	loaded, err := reopenedStore.loadMetadata(ctx, meta.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Version != agentSessionMetadataVersion || loaded.ID != meta.ID || loaded.ProfileID != "sidecar-profile" {
		t.Fatalf("host metadata did not remain sidecar-owned: %#v", loaded)
	}
	listed, err := reopenedStore.listMetadata(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0].ProfileID != "sidecar-profile" {
		t.Fatalf("metadata list read outside the sidecar: %#v", listed)
	}
}

func TestSQLiteHostMetadataDoesNotPersistOrTrackCanonicalTurnLifecycle(t *testing.T) {
	ctx := context.Background()
	runner := NewRunner(t.TempDir())
	runner.Now = fixedClock()
	runner.ProviderFactory = func(config.Config) (provider.Provider, error) {
		return harness.NewScriptedProvider(harness.Step(harness.Text("done"), harness.Done())), nil
	}
	t.Cleanup(func() { _ = runner.Close() })

	created, err := runner.CreateIdleAgentSession(ctx, AgentRunRequest{
		Profile:      ProviderProfile{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model"},
		SystemPrompt: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	store, err := runner.sessionStorage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.metadataDB.ExecContext(ctx, `CREATE TABLE metadata_write_audit(write_count INTEGER NOT NULL);
		INSERT INTO metadata_write_audit(write_count) VALUES(0);
		CREATE TRIGGER metadata_write_audit_insert AFTER INSERT ON metadata_records
		BEGIN
			UPDATE metadata_write_audit SET write_count = write_count + 1;
		END;
		CREATE TRIGGER metadata_write_audit_update AFTER UPDATE ON metadata_records
		BEGIN
			UPDATE metadata_write_audit SET write_count = write_count + 1;
		END;
		CREATE TRIGGER metadata_write_audit_delete AFTER DELETE ON metadata_records
		BEGIN
			UPDATE metadata_write_audit SET write_count = write_count + 1;
		END;`); err != nil {
		t.Fatal(err)
	}
	if got := testMetadataTableColumns(t, store.metadataDB); strings.Join(got, ",") != "namespace,id,data_json" {
		t.Fatalf("host metadata columns = %v", got)
	}

	var beforeData string
	if err := store.metadataDB.QueryRowContext(ctx, `SELECT data_json FROM metadata_records WHERE namespace = ? AND id = ?`,
		agentSessionMetadataNamespace, created.ID).Scan(&beforeData); err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(beforeData), &decoded); err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"created_at", "updated_at", "title", "turns", "status", "phase", "run_id", "turn_id"} {
		if _, exists := decoded[forbidden]; exists {
			t.Fatalf("host metadata persisted canonical lifecycle field %q: %s", forbidden, beforeData)
		}
	}

	result := runner.RunAgentTurn(ctx, created.ID, AgentTurnRequest{Message: "hello"})
	if result.Status != string(engine.Completed) {
		t.Fatalf("turn result = %#v", result)
	}
	var afterData string
	if err := store.metadataDB.QueryRowContext(ctx, `SELECT data_json FROM metadata_records WHERE namespace = ? AND id = ?`,
		agentSessionMetadataNamespace, created.ID).Scan(&afterData); err != nil {
		t.Fatal(err)
	}
	var writes int
	if err := store.metadataDB.QueryRowContext(ctx, `SELECT write_count FROM metadata_write_audit`).Scan(&writes); err != nil {
		t.Fatal(err)
	}
	if afterData != beforeData || writes != 0 {
		t.Fatalf("canonical turn rewrote host metadata: data_changed=%t writes=%d", afterData != beforeData, writes)
	}
}

func TestSQLiteHostMetadataUsesIndependentPhysicalBackend(t *testing.T) {
	ctx := context.Background()
	runner := NewRunner(t.TempDir())
	runner.StorageMode = StorageModeSQLite
	t.Cleanup(func() { _ = runner.Close() })
	store, err := runner.sessionStorage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	create, err := store.capabilities.create.Bind("thread", "create-thread")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := create.CreateThread(ctx, flruntime.CreateThreadRequest{ThreadID: "thread", CreateIntentID: "create-thread"}); err != nil {
		t.Fatal(err)
	}
	if err := store.saveMetadata(ctx, agentSessionMetadata{Version: agentSessionMetadataVersion, ID: "thread"}); err != nil {
		t.Fatal(err)
	}

	if store.metadataPath == "" || store.metadataPath == store.path {
		t.Fatalf("host metadata path = %q, runtime path = %q", store.metadataPath, store.path)
	}
	staleDB, err := sql.Open("sqlite", store.metadataPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = staleDB.Close() })
	staleDB.SetMaxOpenConns(1)
	staleDB.SetMaxIdleConns(1)
	stale, err := staleDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer stale.Rollback()
	var data string
	if err := stale.QueryRowContext(ctx, `SELECT data_json FROM metadata_records WHERE namespace = ? AND id = ?`, agentSessionMetadataNamespace, "thread").Scan(&data); err != nil {
		t.Fatal(err)
	}
	titleHost, err := store.capabilities.title.NewHost(ctx, "thread", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := titleHost.SetThreadTitle(ctx, flruntime.SetThreadTitleRequest{ThreadID: "thread", Title: "Committed title"}); err != nil {
		t.Fatal(err)
	}
	if _, err := stale.ExecContext(ctx, `UPDATE metadata_records SET data_json = data_json WHERE namespace = ? AND id = ?`, agentSessionMetadataNamespace, "thread"); err != nil {
		t.Fatalf("host metadata write conflicted with runtime title write: %v", err)
	}
}

func TestSQLiteHostMetadataUsesWALWithoutBlockingWriterOnReadSnapshot(t *testing.T) {
	ctx := context.Background()
	runner := NewRunner(t.TempDir())
	store, err := runner.sessionStorage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runner.Close() })
	var journalMode string
	var synchronous int
	if err := store.metadataDB.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journalMode); err != nil {
		t.Fatal(err)
	}
	if err := store.metadataDB.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&synchronous); err != nil {
		t.Fatal(err)
	}
	if !strings.EqualFold(journalMode, "wal") || synchronous != 2 {
		t.Fatalf("host metadata pragmas journal_mode=%q synchronous=%d", journalMode, synchronous)
	}

	meta := agentSessionMetadata{Version: agentSessionMetadataVersion, ID: "snapshot"}
	if err := store.saveMetadata(ctx, meta); err != nil {
		t.Fatal(err)
	}
	readerDB, err := sql.Open("sqlite", store.metadataPath)
	if err != nil {
		t.Fatal(err)
	}
	defer readerDB.Close()
	reader, err := readerDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Rollback()
	var raw string
	if err := reader.QueryRowContext(ctx, `SELECT data_json FROM metadata_records WHERE namespace = ? AND id = ?`, agentSessionMetadataNamespace, meta.ID).Scan(&raw); err != nil {
		t.Fatal(err)
	}

	meta.ProfileID = "updated-during-read"
	writeDone := make(chan error, 1)
	go func() { writeDone <- store.saveMetadata(ctx, meta) }()
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("host metadata writer waited for an unrelated read snapshot")
	}
}

func TestSQLiteHostMetadataWriterDoesNotBlockRuntimeWriter(t *testing.T) {
	ctx := context.Background()
	runner := NewRunner(t.TempDir())
	runner.StorageMode = StorageModeSQLite
	t.Cleanup(func() { _ = runner.Close() })
	store, err := runner.sessionStorage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	create, err := store.capabilities.create.Bind("thread", "create-thread")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := create.CreateThread(ctx, flruntime.CreateThreadRequest{ThreadID: "thread", CreateIntentID: "create-thread"}); err != nil {
		t.Fatal(err)
	}
	titleHost, err := store.capabilities.title.NewHost(ctx, "thread", nil)
	if err != nil {
		t.Fatal(err)
	}

	metadataAdmitted := make(chan struct{})
	releaseMetadata := make(chan struct{})
	metadataDone := make(chan error, 1)
	go func() {
		metadataDone <- store.withMetadataImmediate(ctx, func(conn *sql.Conn) error {
			close(metadataAdmitted)
			<-releaseMetadata
			_, err := conn.ExecContext(ctx, `INSERT INTO metadata_records(namespace, id, data_json) VALUES(?, ?, '{}')`, agentSessionMetadataNamespace, "thread")
			return err
		})
	}()
	<-metadataAdmitted
	titleDone := make(chan error, 1)
	go func() {
		_, err := titleHost.SetThreadTitle(ctx, flruntime.SetThreadTitleRequest{ThreadID: "thread", Title: "Concurrent title"})
		titleDone <- err
	}()
	select {
	case err := <-titleDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("runtime writer blocked on independent host metadata transaction")
	}
	close(releaseMetadata)
	if err := <-metadataDone; err != nil {
		t.Fatal(err)
	}
}

package testui

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/floegence/floret/internal/storage/sqlite"
	flruntime "github.com/floegence/floret/runtime"
)

const (
	StorageModeSQLite      = "sqlite"
	StorageModeFile        = "file"
	StorageModeMemory      = "memory"
	hostMetadataPathSuffix = ".host-metadata.db"

	agentSessionMetadataNamespace = "testui.agent_session.v1"
)

type storageStatus struct {
	Mode          string `json:"mode"`
	Path          string `json:"path,omitempty"`
	SchemaVersion string `json:"schema_version,omitempty"`
	Error         string `json:"error,omitempty"`
}

// testUIStorage owns one public runtime Store and an independent host-metadata
// backend. Canonical Agent lifecycle never flows through the metadata backend.
type testUIStorage struct {
	mode           string
	path           string
	metadataPath   string
	runtimeStore   *flruntime.Store
	capabilities   testUIRuntimeCapabilityBinders
	metadataDB     *sql.DB
	metadataWriter *sqlite.WriterAdmission
	metadata       map[string]agentSessionMetadata
}

func (s *testUIStorage) withMetadataImmediate(ctx context.Context, fn func(*sql.Conn) error) error {
	if s == nil || s.metadataDB == nil {
		return errors.New("sqlite metadata storage is not configured")
	}
	return withMetadataImmediateDB(ctx, s.metadataDB, s.metadataWriter, fn)
}

func withMetadataImmediateDB(ctx context.Context, db *sql.DB, writer *sqlite.WriterAdmission, fn func(*sql.Conn) error) error {
	return withMetadataWriterConn(ctx, db, writer, func(conn *sql.Conn) error {
		return withMetadataImmediateConn(ctx, conn, fn)
	})
}

func withMetadataWriterConn(ctx context.Context, db *sql.DB, writer *sqlite.WriterAdmission, fn func(*sql.Conn) error) error {
	releaseWriter, err := writer.Reserve(ctx)
	if err != nil {
		return err
	}
	defer releaseWriter()

	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	return fn(conn)
}

func withMetadataImmediateConn(ctx context.Context, conn *sql.Conn, fn func(*sql.Conn) error) error {
	if conn == nil {
		return errors.New("sqlite metadata connection is required")
	}
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		// Cancellation can race with a successful BEGIN inside the driver.
		// Reset the connection before returning it to the single-connection pool.
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		}
	}()
	if err := fn(conn); err != nil {
		return err
	}
	if _, err := conn.ExecContext(context.Background(), "COMMIT"); err != nil {
		return err
	}
	committed = true
	return nil
}

type testUIRuntimeCapabilityBinders struct {
	create       *flruntime.ThreadCreateHostBinder
	read         *flruntime.ThreadReadHostBinder
	title        *flruntime.ThreadTitleHostBinder
	turn         *flruntime.TurnExecutionHostBinder
	subagent     *flruntime.SubAgentHostBinder
	subagentRead *flruntime.SubAgentReadHostBinder
	delete       *flruntime.ThreadDeleteHostBinder
}

func normalizeStorageMode(mode string) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(mode))
	switch normalized {
	case "", StorageModeSQLite:
		return StorageModeSQLite, nil
	case StorageModeMemory:
		return StorageModeMemory, nil
	case StorageModeFile:
		return "", fmt.Errorf("%w: file agent-session storage lacks the public atomic authority kernel; use sqlite or memory", flruntime.ErrUnsupportedStoreCapability)
	default:
		return "", fmt.Errorf("unsupported storage mode %q; want sqlite or memory", mode)
	}
}

func (r *Runner) storageMode() (string, error) {
	return normalizeStorageMode(r.StorageMode)
}

func (r *Runner) storagePath() string {
	if strings.TrimSpace(r.StoragePath) != "" {
		return r.StoragePath
	}
	return sqlite.DefaultTestUIPath(r.Root)
}

func validateTestUIStoragePath(path string) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return errors.New("test UI sqlite storage path is required")
	}
	if path == ":memory:" {
		return errors.New("test UI sqlite storage path cannot use :memory:; use memory storage mode")
	}
	canonicalPath, err := sqlite.CanonicalDatabasePath(path)
	if err != nil {
		return err
	}
	if strings.HasSuffix(canonicalPath, hostMetadataPathSuffix) {
		return errors.New("test UI sqlite storage path uses the reserved host-metadata path domain")
	}
	return nil
}

func (r *Runner) sessionStorage(ctx context.Context) (*testUIStorage, error) {
	if r == nil {
		return nil, errors.New("test UI runner is required")
	}
	r.storageMu.Lock()
	defer r.storageMu.Unlock()
	if r.storage != nil {
		return r.storage, nil
	}
	mode, err := r.storageMode()
	if err != nil {
		return nil, err
	}
	store := &testUIStorage{mode: mode}
	if mode == StorageModeMemory {
		store.runtimeStore = flruntime.NewMemoryStore()
		store.metadata = map[string]agentSessionMetadata{}
	} else {
		store.path = r.storagePath()
		if err := validateTestUIStoragePath(store.path); err != nil {
			return nil, err
		}
		store.runtimeStore, err = flruntime.OpenSQLiteStore(store.path)
		if err != nil {
			return nil, err
		}
		canonicalRuntimePath, err := sqlite.CanonicalDatabasePath(store.path)
		if err != nil {
			_ = store.runtimeStore.Close()
			return nil, err
		}
		store.metadataPath = canonicalRuntimePath + hostMetadataPathSuffix
		store.metadataWriter, err = sqlite.NewWriterAdmission(store.metadataPath)
		if err != nil {
			_ = store.runtimeStore.Close()
			return nil, err
		}
		store.metadataDB, err = sql.Open("sqlite", store.metadataPath)
		if err != nil {
			_ = store.Close()
			return nil, err
		}
		store.metadataDB.SetMaxOpenConns(1)
		store.metadataDB.SetMaxIdleConns(1)
		for _, pragma := range []string{"PRAGMA foreign_keys = ON", "PRAGMA busy_timeout = 0"} {
			if _, err := store.metadataDB.ExecContext(ctx, pragma); err != nil {
				_ = store.Close()
				return nil, err
			}
		}
		if err := initializeHostMetadataDatabase(ctx, store.metadataDB, store.metadataWriter); err != nil {
			_ = store.Close()
			return nil, err
		}
	}
	if err := store.configureCapabilities(); err != nil {
		_ = store.Close()
		return nil, err
	}
	r.storage = store
	return store, nil
}

func initializeHostMetadataDatabase(ctx context.Context, db *sql.DB, writer *sqlite.WriterAdmission) error {
	if err := withMetadataImmediateDB(ctx, db, writer, func(conn *sql.Conn) error {
		return ensureHostMetadataSchema(ctx, conn)
	}); err != nil {
		return err
	}
	return withMetadataWriterConn(ctx, db, writer, func(conn *sql.Conn) error {
		for _, pragma := range []string{"PRAGMA journal_mode = WAL", "PRAGMA synchronous = FULL"} {
			if _, err := conn.ExecContext(ctx, pragma); err != nil {
				return err
			}
		}
		return nil
	})
}

const createHostMetadataRecordsSQL = `CREATE TABLE metadata_records(
	namespace TEXT NOT NULL,
	id TEXT NOT NULL,
	data_json TEXT NOT NULL,
	PRIMARY KEY(namespace, id)
)`

func ensureHostMetadataSchema(ctx context.Context, conn *sql.Conn) error {
	columns, err := hostMetadataTableColumns(ctx, conn)
	if err != nil {
		return err
	}
	switch strings.Join(columns, ",") {
	case "":
		_, err := conn.ExecContext(ctx, createHostMetadataRecordsSQL)
		return err
	case "namespace,id,data_json":
		return nil
	case "namespace,id,created_at,updated_at,data_json":
		return migrateLegacyHostMetadataSchema(ctx, conn)
	default:
		return fmt.Errorf("unsupported test UI host metadata schema columns %q", strings.Join(columns, ","))
	}
}

func hostMetadataTableColumns(ctx context.Context, conn *sql.Conn) ([]string, error) {
	rows, err := conn.QueryContext(ctx, `PRAGMA table_info(metadata_records)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var cid int
		var name, columnType string
		var notNull, primaryKey int
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			return nil, err
		}
		columns = append(columns, name)
	}
	return columns, rows.Err()
}

type hostMetadataMigrationRecord struct {
	namespace string
	id        string
	data      string
}

func migrateLegacyHostMetadataSchema(ctx context.Context, conn *sql.Conn) error {
	rows, err := conn.QueryContext(ctx, `SELECT namespace, id, data_json FROM metadata_records ORDER BY namespace, id`)
	if err != nil {
		return err
	}
	var records []hostMetadataMigrationRecord
	for rows.Next() {
		var record hostMetadataMigrationRecord
		if err := rows.Scan(&record.namespace, &record.id, &record.data); err != nil {
			_ = rows.Close()
			return err
		}
		record.data, err = migrateLegacyHostMetadataRecord(record)
		if err != nil {
			_ = rows.Close()
			return err
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, `CREATE TABLE metadata_records_next(
		namespace TEXT NOT NULL,
		id TEXT NOT NULL,
		data_json TEXT NOT NULL,
		PRIMARY KEY(namespace, id)
	)`); err != nil {
		return err
	}
	for _, record := range records {
		if _, err := conn.ExecContext(ctx, `INSERT INTO metadata_records_next(namespace, id, data_json) VALUES(?, ?, ?)`, record.namespace, record.id, record.data); err != nil {
			return err
		}
	}
	if _, err := conn.ExecContext(ctx, `DROP TABLE metadata_records`); err != nil {
		return err
	}
	_, err = conn.ExecContext(ctx, `ALTER TABLE metadata_records_next RENAME TO metadata_records`)
	return err
}

func migrateLegacyHostMetadataRecord(record hostMetadataMigrationRecord) (string, error) {
	if record.namespace != agentSessionMetadataNamespace {
		return "", fmt.Errorf("unsupported host metadata namespace %q during schema migration", record.namespace)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(record.data), &raw); err != nil {
		return "", fmt.Errorf("decode host metadata %q during schema migration: %w", record.id, err)
	}
	delete(raw, "created_at")
	delete(raw, "updated_at")
	data, err := json.Marshal(raw)
	if err != nil {
		return "", err
	}
	meta, err := decodeAgentSessionMetadata(data)
	if err != nil {
		return "", fmt.Errorf("validate host metadata %q during schema migration: %w", record.id, err)
	}
	if meta.ID != record.id {
		return "", fmt.Errorf("host metadata %q id does not match encoded id %q during schema migration", record.id, meta.ID)
	}
	clean, err := json.Marshal(meta)
	if err != nil {
		return "", err
	}
	return string(clean), nil
}

func (s *testUIStorage) configureCapabilities() error {
	if s == nil || s.runtimeStore == nil {
		return errors.New("test UI runtime store is required")
	}
	return flruntime.ConfigureHostCapabilities(s.runtimeStore, func(bootstrap *flruntime.HostBootstrap) error {
		var err error
		if s.capabilities.create, err = flruntime.NewThreadCreateHostBinder(bootstrap); err != nil {
			return err
		}
		if s.capabilities.read, err = flruntime.NewThreadReadHostBinder(bootstrap); err != nil {
			return err
		}
		if s.capabilities.title, err = flruntime.NewThreadTitleHostBinder(bootstrap); err != nil {
			return err
		}
		if s.capabilities.turn, err = flruntime.NewTurnExecutionHostBinder(bootstrap); err != nil {
			return err
		}
		if s.capabilities.subagent, err = flruntime.NewSubAgentHostBinder(bootstrap); err != nil {
			return err
		}
		if s.capabilities.subagentRead, err = flruntime.NewSubAgentReadHostBinder(bootstrap); err != nil {
			return err
		}
		s.capabilities.delete, err = flruntime.NewThreadDeleteHostBinder(bootstrap)
		return err
	})
}

func (r *Runner) Close() error {
	if r == nil {
		return nil
	}
	registry := r.sessionRegistry()
	registry.mu.Lock()
	sessions := make([]*agentSession, 0, len(registry.sessions))
	for _, sess := range registry.sessions {
		sessions = append(sessions, sess)
	}
	registry.sessions = map[string]*agentSession{}
	registry.order = nil
	registry.mu.Unlock()
	for _, sess := range sessions {
		sess.mu.Lock()
		sess.close()
		sess.mu.Unlock()
	}
	r.storageMu.Lock()
	store := r.storage
	r.storage = nil
	r.storageMu.Unlock()
	if store == nil {
		return nil
	}
	return store.Close()
}

func (s *testUIStorage) Close() error {
	if s == nil {
		return nil
	}
	var errs []error
	if s.runtimeStore != nil {
		errs = append(errs, s.runtimeStore.Close())
		s.runtimeStore = nil
	}
	if s.metadataDB != nil {
		errs = append(errs, s.metadataDB.Close())
		s.metadataDB = nil
	}
	if s.metadataWriter != nil {
		s.metadataWriter.Close()
		s.metadataWriter = nil
	}
	return errors.Join(errs...)
}

func (r *Runner) storageStatus(ctx context.Context) storageStatus {
	mode, modeErr := r.storageMode()
	if modeErr != nil {
		return storageStatus{Mode: strings.ToLower(strings.TrimSpace(r.StorageMode)), Path: r.storagePath(), Error: modeErr.Error()}
	}
	store, err := r.sessionStorage(ctx)
	if err != nil {
		return storageStatus{Mode: mode, Path: r.storagePath(), Error: err.Error()}
	}
	return storageStatus{Mode: store.mode, Path: store.path}
}

func (s *testUIStorage) saveMetadata(ctx context.Context, meta agentSessionMetadata) error {
	if s.mode == StorageModeMemory {
		s.metadata[meta.ID] = meta
		return nil
	}
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return s.withMetadataImmediate(ctx, func(conn *sql.Conn) error {
		_, err := conn.ExecContext(ctx, `INSERT INTO metadata_records(namespace, id, data_json)
			VALUES(?, ?, ?)
			ON CONFLICT(namespace, id) DO UPDATE SET data_json = excluded.data_json`,
			agentSessionMetadataNamespace, meta.ID, string(data))
		return err
	})
}

func (s *testUIStorage) loadMetadata(ctx context.Context, id string) (agentSessionMetadata, error) {
	if s.mode == StorageModeMemory {
		meta, ok := s.metadata[id]
		if !ok {
			return agentSessionMetadata{}, os.ErrNotExist
		}
		return meta, nil
	}
	var data string
	err := s.metadataDB.QueryRowContext(ctx, `SELECT data_json FROM metadata_records WHERE namespace = ? AND id = ?`, agentSessionMetadataNamespace, id).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return agentSessionMetadata{}, os.ErrNotExist
	}
	if err != nil {
		return agentSessionMetadata{}, err
	}
	return decodeAgentSessionMetadata([]byte(data))
}

func (s *testUIStorage) listMetadata(ctx context.Context) ([]agentSessionMetadata, error) {
	if s.mode == StorageModeMemory {
		out := make([]agentSessionMetadata, 0, len(s.metadata))
		for _, meta := range s.metadata {
			out = append(out, meta)
		}
		return out, nil
	}
	rows, err := s.metadataDB.QueryContext(ctx, `SELECT data_json FROM metadata_records WHERE namespace = ? ORDER BY id`, agentSessionMetadataNamespace)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []agentSessionMetadata
	for rows.Next() {
		var data string
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		meta, err := decodeAgentSessionMetadata([]byte(data))
		if err != nil {
			return nil, err
		}
		out = append(out, meta)
	}
	return out, rows.Err()
}

func (s *testUIStorage) deleteMetadata(ctx context.Context, sessionID string) error {
	if s.mode == StorageModeMemory {
		delete(s.metadata, sessionID)
		return nil
	}
	return s.withMetadataImmediate(ctx, func(conn *sql.Conn) error {
		_, err := conn.ExecContext(ctx, `DELETE FROM metadata_records WHERE namespace = ? AND id = ?`, agentSessionMetadataNamespace, sessionID)
		return err
	})
}

func (s *testUIStorage) deleteSession(ctx context.Context, sessionID string) ([]string, error) {
	deleteHost, err := s.capabilities.delete.NewHost(ctx, flruntime.ThreadID(sessionID))
	if err != nil {
		return nil, err
	}
	if err := deleteHost.DeleteThread(ctx, flruntime.ThreadID(sessionID)); err != nil {
		return nil, err
	}
	cleanupError := func(err error) ([]string, error) {
		return nil, &flruntime.CommittedCleanupError{ThreadID: flruntime.ThreadID(sessionID), Err: err}
	}
	if err := s.deleteMetadata(ctx, sessionID); err != nil {
		return cleanupError(err)
	}
	return []string{sessionID}, nil
}

package testui

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/floegence/floret/internal/storage/sqlite"
	flruntime "github.com/floegence/floret/runtime"
)

const (
	StorageModeSQLite = "sqlite"
	StorageModeFile   = "file"
	StorageModeMemory = "memory"

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
	mode         string
	path         string
	runtimeStore *flruntime.Store
	capabilities testUIRuntimeCapabilityBinders
	metadataDB   *sql.DB
	metadata     map[string]agentSessionMetadata
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

func (r Runner) storageMode() (string, error) {
	return normalizeStorageMode(r.StorageMode)
}

func (r Runner) storagePath() string {
	if strings.TrimSpace(r.StoragePath) != "" {
		return r.StoragePath
	}
	return sqlite.DefaultTestUIPath(r.Root)
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
		store.runtimeStore, err = flruntime.OpenSQLiteStore(store.path)
		if err != nil {
			return nil, err
		}
		store.metadataDB, err = sql.Open("sqlite", store.path)
		if err != nil {
			_ = store.runtimeStore.Close()
			return nil, err
		}
		store.metadataDB.SetMaxOpenConns(1)
		store.metadataDB.SetMaxIdleConns(1)
		for _, pragma := range []string{"PRAGMA foreign_keys = ON", "PRAGMA busy_timeout = 5000"} {
			if _, err := store.metadataDB.ExecContext(ctx, pragma); err != nil {
				_ = store.Close()
				return nil, err
			}
		}
	}
	if err := store.configureCapabilities(); err != nil {
		_ = store.Close()
		return nil, err
	}
	r.storage = store
	return store, nil
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
	status := storageStatus{Mode: store.mode, Path: store.path}
	if store.metadataDB != nil {
		_ = store.metadataDB.QueryRowContext(ctx, `SELECT schema_version FROM store_metadata WHERE singleton = 1`).Scan(&status.SchemaVersion)
	}
	return status
}

func (s *testUIStorage) saveMetadata(ctx context.Context, meta agentSessionMetadata, fileStore func(agentSessionMetadata) error) error {
	if s.mode == StorageModeMemory {
		s.metadata[meta.ID] = meta
		return nil
	}
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	_, err = s.metadataDB.ExecContext(ctx, `INSERT INTO metadata_records(namespace, id, created_at, updated_at, data_json)
		VALUES(?, ?, ?, ?, ?)
		ON CONFLICT(namespace, id) DO UPDATE SET updated_at = excluded.updated_at, data_json = excluded.data_json`,
		agentSessionMetadataNamespace, meta.ID, formatMetadataTime(meta.CreatedAt), formatMetadataTime(meta.UpdatedAt), string(data))
	return err
}

func (s *testUIStorage) loadMetadata(ctx context.Context, id string, fileStore func(string) (agentSessionMetadata, error)) (agentSessionMetadata, error) {
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

func (s *testUIStorage) listMetadata(ctx context.Context, fileStore func() ([]agentSessionMetadata, error)) ([]agentSessionMetadata, error) {
	if s.mode == StorageModeMemory {
		out := make([]agentSessionMetadata, 0, len(s.metadata))
		for _, meta := range s.metadata {
			out = append(out, meta)
		}
		return out, nil
	}
	rows, err := s.metadataDB.QueryContext(ctx, `SELECT data_json FROM metadata_records WHERE namespace = ? ORDER BY updated_at DESC, id DESC`, agentSessionMetadataNamespace)
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
	_, err := s.metadataDB.ExecContext(ctx, `DELETE FROM metadata_records WHERE namespace = ? AND id = ?`, agentSessionMetadataNamespace, sessionID)
	return err
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

func formatMetadataTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

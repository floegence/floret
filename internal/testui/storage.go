package testui

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/floegence/floret/internal/provider/cache"
	"github.com/floegence/floret/internal/sessiontree"
	floretstorage "github.com/floegence/floret/internal/storage"
	"github.com/floegence/floret/internal/storage/sqlite"
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

type testUIStorage struct {
	mode   string
	path   string
	sqlite *sqlite.Store
	memory *memoryStorage
}

type memoryStorage struct {
	repo     *sessiontree.MemoryRepo
	prompt   *cache.MemoryStore
	metadata map[string]agentSessionMetadata
}

func normalizeStorageMode(mode string) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(mode))
	switch normalized {
	case "", StorageModeSQLite:
		return StorageModeSQLite, nil
	case StorageModeFile:
		return StorageModeFile, nil
	case StorageModeMemory:
		return StorageModeMemory, nil
	default:
		return "", fmt.Errorf("unsupported storage mode %q; want sqlite, file, or memory", mode)
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
	mode, err := r.storageMode()
	if err != nil {
		return nil, err
	}
	if mode == StorageModeMemory {
		if r.storageMemory == nil {
			r.storageMemory = &memoryStorage{
				repo:     sessiontree.NewMemoryRepo(),
				prompt:   cache.NewMemoryStore(),
				metadata: map[string]agentSessionMetadata{},
			}
		}
		return &testUIStorage{mode: mode, memory: r.storageMemory}, nil
	}
	if mode == StorageModeFile {
		return &testUIStorage{mode: mode, path: r.agentSessionDataRoot()}, nil
	}
	if r.storageSQLite != nil {
		return &testUIStorage{mode: mode, path: r.storageSQLite.DBPath(), sqlite: r.storageSQLite}, nil
	}
	store, err := sqlite.Open(r.storagePath())
	if err != nil {
		return nil, err
	}
	r.storageSQLite = store
	return &testUIStorage{mode: mode, path: store.DBPath(), sqlite: store}, nil
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
	if store.sqlite != nil {
		if version, err := store.sqlite.SchemaVersion(ctx); err == nil {
			status.SchemaVersion = version
		}
	}
	return status
}

func (s *testUIStorage) repo(root string) sessiontree.Repo {
	switch s.mode {
	case StorageModeSQLite:
		return s.sqlite
	case StorageModeMemory:
		return s.memory.repo
	default:
		return sessiontree.NewFileRepo(filepath.Join(root, ".floret-test-ui", "agent-sessions", "tree"))
	}
}

func (s *testUIStorage) prompt(root string) cache.Store {
	switch s.mode {
	case StorageModeSQLite:
		return s.sqlite
	case StorageModeMemory:
		return s.memory.prompt
	default:
		return cache.NewFileStore(filepath.Join(root, ".floret-test-ui", "prompt-cache"))
	}
}

func (s *testUIStorage) saveMetadata(ctx context.Context, meta agentSessionMetadata, fileStore func(agentSessionMetadata) error) error {
	switch s.mode {
	case StorageModeSQLite:
		data, err := json.Marshal(meta)
		if err != nil {
			return err
		}
		return s.sqlite.PutMetadata(ctx, floretstorage.MetadataRecord{
			Namespace: agentSessionMetadataNamespace,
			ID:        meta.ID,
			CreatedAt: meta.CreatedAt,
			UpdatedAt: meta.UpdatedAt,
			Data:      data,
		})
	case StorageModeMemory:
		s.memory.metadata[meta.ID] = meta
		return nil
	default:
		return fileStore(meta)
	}
}

func (s *testUIStorage) loadMetadata(ctx context.Context, id string, fileStore func(string) (agentSessionMetadata, error)) (agentSessionMetadata, error) {
	switch s.mode {
	case StorageModeSQLite:
		rec, err := s.sqlite.Metadata(ctx, agentSessionMetadataNamespace, id)
		if errors.Is(err, floretstorage.ErrMetadataNotFound) {
			return agentSessionMetadata{}, os.ErrNotExist
		}
		if err != nil {
			return agentSessionMetadata{}, err
		}
		var meta agentSessionMetadata
		if err := json.Unmarshal(rec.Data, &meta); err != nil {
			return agentSessionMetadata{}, err
		}
		return meta, nil
	case StorageModeMemory:
		meta, ok := s.memory.metadata[id]
		if !ok {
			return agentSessionMetadata{}, os.ErrNotExist
		}
		return meta, nil
	default:
		return fileStore(id)
	}
}

func (s *testUIStorage) listMetadata(ctx context.Context, fileStore func() ([]agentSessionMetadata, error)) ([]agentSessionMetadata, error) {
	switch s.mode {
	case StorageModeSQLite:
		records, err := s.sqlite.ListMetadata(ctx, agentSessionMetadataNamespace)
		if err != nil {
			return nil, err
		}
		out := make([]agentSessionMetadata, 0, len(records))
		for _, rec := range records {
			var meta agentSessionMetadata
			if err := json.Unmarshal(rec.Data, &meta); err != nil || meta.ID == "" {
				continue
			}
			out = append(out, meta)
		}
		return out, nil
	case StorageModeMemory:
		out := make([]agentSessionMetadata, 0, len(s.memory.metadata))
		for _, meta := range s.memory.metadata {
			out = append(out, meta)
		}
		return out, nil
	default:
		return fileStore()
	}
}

func (s *testUIStorage) deleteSession(ctx context.Context, root string, sessionID string, fileStore func() error) error {
	switch s.mode {
	case StorageModeSQLite:
		return s.sqlite.DeleteThreadTreeData(ctx, floretstorage.DeleteThreadTreeDataRequest{
			RootThreadID:   sessionID,
			ThreadIDs:      []string{sessionID},
			PromptScopeIDs: []string{sessionID},
		})
	case StorageModeMemory:
		_ = s.memory.repo.DeleteThread(ctx, sessionID)
		_ = s.memory.prompt.DeletePromptScopes(ctx, sessionID)
		delete(s.memory.metadata, sessionID)
		return nil
	default:
		return fileStore()
	}
}

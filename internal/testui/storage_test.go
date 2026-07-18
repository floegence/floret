package testui

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/floegence/floret/internal/sessiontree"
	floretstorage "github.com/floegence/floret/internal/storage"
)

func TestTestUIStorageDeleteSessionRemovesThreadAuthorityTree(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []string{StorageModeMemory, StorageModeFile, StorageModeSQLite} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			runner := NewRunner(root)
			runner.StorageMode = mode
			store, err := runner.sessionStorage(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if runner.storageSQLite != nil {
				t.Cleanup(func() { _ = runner.storageSQLite.Close() })
			}
			repo := store.repo(root)
			now := time.Date(2026, 7, 18, 18, 0, 0, 0, time.UTC)
			if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "parent", CreatedAt: now, UpdatedAt: now}); err != nil {
				t.Fatal(err)
			}
			if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{
				ID: "child", ParentThreadID: "parent", TaskName: "child", AgentPath: "/root/child",
				Closed: true, Status: "closed", CreatedAt: now, UpdatedAt: now,
			}); err != nil {
				t.Fatal(err)
			}

			for _, threadID := range []string{"parent", "child"} {
				artifactDir := toolOutputArtifactSessionDir(runner.managedArtifactsRoot(), threadID)
				if err := os.MkdirAll(artifactDir, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(artifactDir, "output.log"), []byte("output"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			switch mode {
			case StorageModeMemory:
				store.memory.metadata["parent"] = agentSessionMetadata{ID: "parent"}
				store.memory.metadata["child"] = agentSessionMetadata{ID: "child"}
			case StorageModeFile:
				if err := os.MkdirAll(runner.agentSessionMetadataRoot(), 0o700); err != nil {
					t.Fatal(err)
				}
				for _, threadID := range []string{"parent", "child"} {
					if err := os.WriteFile(runner.agentSessionMetadataPath(threadID), []byte(`{}`), 0o600); err != nil {
						t.Fatal(err)
					}
				}
			case StorageModeSQLite:
				for _, threadID := range []string{"parent", "child"} {
					if err := store.sqlite.PutMetadata(ctx, floretstorage.MetadataRecord{Namespace: agentSessionMetadataNamespace, ID: threadID, CreatedAt: now, UpdatedAt: now, Data: []byte(`{}`)}); err != nil {
						t.Fatal(err)
					}
				}
			}

			deleted, err := store.deleteSession(ctx, root, "parent")
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(deleted, []string{"parent", "child"}) {
				t.Fatalf("deleted thread ids = %v", deleted)
			}
			for _, threadID := range deleted {
				if _, err := repo.Thread(ctx, threadID); !errors.Is(err, sessiontree.ErrThreadNotFound) {
					t.Fatalf("Thread(%q) after delete err = %v", threadID, err)
				}
				if _, err := os.Stat(toolOutputArtifactSessionDir(runner.managedArtifactsRoot(), threadID)); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("artifact directory for %q remains: %v", threadID, err)
				}
				switch mode {
				case StorageModeMemory:
					if _, ok := store.memory.metadata[threadID]; ok {
						t.Fatalf("memory metadata for %q remains", threadID)
					}
				case StorageModeFile:
					if _, err := os.Stat(runner.agentSessionMetadataPath(threadID)); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("file metadata for %q remains: %v", threadID, err)
					}
				case StorageModeSQLite:
					if _, err := store.sqlite.Metadata(ctx, agentSessionMetadataNamespace, threadID); !errors.Is(err, floretstorage.ErrMetadataNotFound) {
						t.Fatalf("sqlite metadata for %q remains: %v", threadID, err)
					}
				}
			}
			if mode == StorageModeFile {
				if _, err := sessiontree.ListThreads(ctx, sessiontree.NewFileRepo(runner.agentSessionTreeRoot()), sessiontree.ListThreadsOptions{IncludeArchived: true}); err != nil {
					t.Fatalf("reopen file repo after tree delete: %v", err)
				}
			}
		})
	}
}

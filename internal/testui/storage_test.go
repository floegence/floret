package testui

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

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
			now := time.Date(2026, 7, 18, 18, 0, 0, 0, time.UTC)
			if err := store.saveMetadata(ctx, agentSessionMetadata{Version: agentSessionMetadataVersion, ID: "parent", CreatedAt: now, UpdatedAt: now}, nil); err != nil {
				t.Fatal(err)
			}
			deleted, err := store.deleteSession(ctx, "parent")
			if err != nil || len(deleted) != 1 || deleted[0] != "parent" {
				t.Fatalf("delete result=%v err=%v", deleted, err)
			}
			if _, err := store.capabilities.read.NewHost(ctx, "parent"); !errors.Is(err, flruntime.ErrThreadDeleted) {
				t.Fatalf("read host after delete err=%v, want ErrThreadDeleted", err)
			}
			if _, err := store.loadMetadata(ctx, "parent", nil); !errors.Is(err, os.ErrNotExist) {
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

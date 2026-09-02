package sessiontree

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/storagebridge"
	publicstorage "github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestV9MigrationAgainstV8SQLiteCopy(t *testing.T) {
	sourcePath := os.Getenv("FLORET_V8_SOURCE_DB")
	threadID := os.Getenv("FLORET_V8_THREAD_ID")
	if sourcePath == "" || threadID == "" {
		t.Skip("set FLORET_V8_SOURCE_DB and FLORET_V8_THREAD_ID for an opt-in migration qualification")
	}
	before, err := fileSHA256(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	copyPath := filepath.Join(t.TempDir(), "floret-v8-qualification.sqlite")
	if err := copyFile(sourcePath, copyPath); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	backend, err := storagebridge.Open(ctx, storagebridge.Source(publicstorage.SQLite(copyPath)))
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	repo, err := NewBackendRepo(ctx, backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	thread, err := repo.Thread(ctx, threadID)
	if err != nil {
		t.Fatal(err)
	}
	path, err := repo.Path(ctx, threadID, thread.LeafID)
	if err != nil {
		t.Fatal(err)
	}
	migratedTurns := map[string]struct{}{}
	for index, entry := range path {
		if entry.Type != EntryTurnMarker || entry.TurnStatus != TurnSavePoint || entry.Metadata["reason"] != "context_continue" {
			continue
		}
		if index == 0 || path[index-1].Message.Kind != "control_signal" {
			t.Fatalf("context continuation %q was not classified", entry.ID)
		}
		migratedTurns[entry.TurnID] = struct{}{}
	}
	if len(migratedTurns) == 0 {
		t.Fatal("target thread contains no context continuation migration fixture")
	}
	for turnID := range migratedTurns {
		if _, err := repo.ReadCanonicalTurn(ctx, threadID, turnID); err != nil {
			t.Fatalf("canonical turn %q after v8 migration: %v", turnID, err)
		}
	}
	if err := backend.View(ctx, func(tx spi.ReadTx) error {
		if records, err := scanBackendDomainV7(ctx, tx); err != nil || len(records) != 0 {
			return errors.Join(err, errors.New("v7 records remain after qualification"))
		}
		_, found, err := loadBackendDomainV9(ctx, tx, time.Now)
		if err != nil || !found {
			return errors.Join(err, errors.New("v9 records are missing after qualification"))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	after, err := fileSHA256(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatal("source database changed while qualifying its copy")
	}
}

func TestLargeV7SQLiteStartupQualification(t *testing.T) {
	sourcePath := os.Getenv("FLORET_V7_LARGE_SOURCE_DB")
	if sourcePath == "" {
		t.Skip("set FLORET_V7_LARGE_SOURCE_DB for the opt-in large-store startup qualification")
	}
	before, err := fileSHA256(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	copyPath := filepath.Join(t.TempDir(), "floret-v7-large-qualification.sqlite")
	if err := copyFile(sourcePath, copyPath); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	backend, err := storagebridge.Open(ctx, storagebridge.Source(publicstorage.SQLite(copyPath)))
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if _, err := NewBackendRepo(ctx, backend, time.Now); err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	elapsed := time.Since(started)
	if elapsed > 15*time.Second {
		_ = backend.Close()
		t.Fatalf("large v7 startup took %s, want at most 15s", elapsed)
	}
	if err := backend.View(ctx, func(tx spi.ReadTx) error {
		if records, err := scanBackendDomainV7(ctx, tx); err != nil || len(records) != 0 {
			return errors.Join(err, errors.New("v7 records remain after large-store qualification"))
		}
		_, found, err := loadBackendDomainV9(ctx, tx, time.Now)
		if err != nil || !found {
			return errors.Join(err, errors.New("v9 records are missing after large-store qualification"))
		}
		return nil
	}); err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
	after, err := fileSHA256(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatal("source database changed while qualifying its copy")
	}
	t.Logf("large v7 startup completed in %s", elapsed)
}

func copyFile(sourcePath, destinationPath string) error {
	source, err := os.Open(sourcePath)
	if err != nil {
		return err
	}
	defer source.Close()
	destination, err := os.OpenFile(destinationPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := io.Copy(destination, source); err != nil {
		_ = destination.Close()
		return err
	}
	return destination.Close()
}

func fileSHA256(path string) ([sha256.Size]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return [sha256.Size]byte{}, err
	}
	var sum [sha256.Size]byte
	copy(sum[:], hash.Sum(nil))
	return sum, nil
}

package storage

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/internal/threadsqlite"
	"github.com/floegence/floret/v7/storage/spi"
)

func makeThreadFiles(t *testing.T, root string) *threadsqlite.Files {
	t.Helper()
	directory, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	files, err := threadsqlite.NewFiles(directory, threadsqlite.FileID{1}, 3, 2, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := files.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return files
}

func TestThreadFileLocalTransactions(t *testing.T) {
	files := makeThreadFiles(t, t.TempDir())
	first := threadsqlite.LocalRecords{Files: files, Reference: threadsqlite.Reference{File: threadsqlite.FileID{3}, Thread: "first"}}
	second := threadsqlite.LocalRecords{Files: files, Reference: threadsqlite.Reference{File: threadsqlite.FileID{4}, Thread: "second"}}
	for _, file := range []threadsqlite.LocalRecords{first, second} {
		if err := files.Create(t.Context(), file.Reference); err != nil {
			t.Fatal(err)
		}
	}
	body := bytes.Repeat([]byte("abcd"), 4096)
	if err := first.Update(t.Context(), func(tx spi.WriteTx) error {
		return storagebridge.WriteRecords(tx, "history", []spi.Record{{Key: []byte("entry"), Value: body}, {Key: []byte("leaf"), Value: []byte("entry")}})
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if err := first.Update(ctx, func(tx spi.WriteTx) error {
		if err := tx.Delete("history", []byte("entry")); err != nil {
			return err
		}
		cancel()
		return nil
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled transaction: %v", err)
	}
	var expired spi.ReadTx
	if err := first.View(t.Context(), func(tx spi.ReadTx) error {
		expired = tx
		rows, err := storagebridge.ReadRecords(tx, storagebridge.RecordBatch{Namespace: "history", Keys: [][]byte{[]byte("leaf"), []byte("entry")}})
		if err != nil {
			return err
		}
		if string(rows[0]) != "entry" || !bytes.Equal(rows[1], body) {
			t.Fatal("committed rows changed")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := expired.Get("history", []byte("entry")); !errors.Is(err, spi.ErrTransactionClosed) {
		t.Fatal(err)
	}
	if err := second.View(t.Context(), func(tx spi.ReadTx) error {
		rows, err := tx.Scan(spi.ScanRequest{Namespace: "history", Limit: 1})
		if len(rows.Records) != 0 {
			t.Fatal("read another thread's records")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestThreadFileProcessExit(t *testing.T) {
	const marker = "FLORET_TEST_INTERRUPTED_THREAD_FILE"
	root := os.Getenv(marker)
	ref := threadsqlite.Reference{File: threadsqlite.FileID{4}, Thread: "interrupted"}
	if root != "" {
		files := makeThreadFiles(t, root)
		if err := files.Create(t.Context(), ref); err != nil {
			t.Fatal(err)
		}
		file := threadsqlite.LocalRecords{Files: files, Reference: ref}
		if err := file.Update(t.Context(), func(tx spi.WriteTx) error { return tx.Put("history", []byte("leaf"), []byte("committed")) }); err != nil {
			t.Fatal(err)
		}
		err := file.Update(t.Context(), func(tx spi.WriteTx) error {
			if err := tx.Put("history", []byte("leaf"), []byte("uncommitted")); err != nil {
				return err
			}
			os.Exit(72)
			return nil
		})
		t.Fatalf("did not interrupt transaction: %v", err)
	}
	root = t.TempDir()
	command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestThreadFileProcessExit$")
	command.Env = append(os.Environ(), marker+"="+root)
	output, err := command.CombinedOutput()
	var exited *exec.ExitError
	if !errors.As(err, &exited) || exited.ExitCode() != 72 {
		t.Fatalf("unexpected child exit: %v: %s", err, output)
	}
	files := makeThreadFiles(t, root)
	file := threadsqlite.LocalRecords{Files: files, Reference: ref}
	if err := file.View(t.Context(), func(tx spi.ReadTx) error {
		value, err := tx.Get("history", []byte("leaf"))
		if err != nil {
			return err
		}
		if string(value) != "committed" {
			t.Fatalf("observed %q", value)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestLegacySQLiteRejectsThreadCatalog(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "floret_threads.sqlite")
	catalog, err := threadsqlite.CreateCatalog(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	store := catalog.Store
	if err := catalog.Close(); err != nil {
		t.Fatal(err)
	}
	legacy, err := (sqliteSource{path: path}).Open(t.Context())
	if legacy != nil || !errors.Is(err, ErrSQLiteTooNew) {
		t.Fatalf("legacy accepted catalog or returned wrong status: %v", err)
	}
	catalog, err = threadsqlite.OpenCatalog(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer catalog.Close()
	if catalog.Store != store {
		t.Fatal("legacy modified the store identity")
	}
}

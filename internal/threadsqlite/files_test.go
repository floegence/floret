package threadsqlite

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

func fixtureFiles(t *testing.T, limit, idle int) *Files {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	files, err := NewFiles(root, FileID{1}, limit, idle, time.Minute)
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

func empty(tx *sql.Tx) error { return nil }

func TestFileIdentityAndReopen(t *testing.T) {
	files := fixtureFiles(t, 2, 0)
	ref, err := Allocate("../chat?mode=rw")
	if err != nil {
		t.Fatal(err)
	}
	if err := files.transaction(t.Context(), ref, true, empty); !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if err := files.Create(t.Context(), ref); err != nil {
		t.Fatal(err)
	}
	if err := files.Create(t.Context(), ref); !errors.Is(err, os.ErrExist) {
		t.Fatal(err)
	}
	if err := files.transaction(t.Context(), ref, false, func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT INTO floret_backend_records VALUES('test',x'01',x'ff')`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	foreign := ref
	foreign.Thread = "different"
	if err := files.transaction(t.Context(), foreign, true, empty); !errors.Is(err, ErrFileIdentity) {
		t.Fatal(err)
	}
	if err := files.transaction(t.Context(), ref, true, func(tx *sql.Tx) error {
		var value string
		if err := tx.QueryRow(`SELECT hex(value) FROM floret_backend_records`).Scan(&value); err != nil {
			return err
		}
		if value != "FF" {
			t.Fatalf("value %s", value)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	other, err := NewFiles(files.directory, FileID{2}, 1, 0, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close(context.Background())
	if err := other.transaction(t.Context(), ref, true, empty); !errors.Is(err, ErrFileIdentity) {
		t.Fatal(err)
	}
}

func TestFileCreationSharesCapacityAndShutdown(t *testing.T) {
	files := fixtureFiles(t, 1, 1)
	first := Reference{File: FileID{1}, Thread: "first"}
	other := Reference{File: FileID{2}, Thread: "other"}
	if err := files.Create(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	if err := files.transaction(t.Context(), first, true, func(tx *sql.Tx) error {
		ctx, cancel := context.WithTimeout(t.Context(), 40*time.Millisecond)
		defer cancel()
		if err := files.Create(ctx, other); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("a second file bypassed the budget: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := files.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := files.Create(t.Context(), other); !errors.Is(err, spi.ErrClosed) {
		t.Errorf("create after shutdown: %v", err)
	}
	if _, err := os.Stat(files.path(other.File)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unexpected file after rejection: %v", err)
	}
}

func TestFileChecksIdentityBeforeMutation(t *testing.T) {
	files := fixtureFiles(t, 1, 0)
	ref := Reference{File: FileID{3}, Thread: "first"}
	if err := files.Create(t.Context(), ref); err != nil {
		t.Fatal(err)
	}
	db, err := existingDB(files.path(ref.File))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE floret_thread_header SET format=2`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := files.transaction(t.Context(), ref, false, func(*sql.Tx) error { t.Fatal("executed mutation on future format"); return nil }); !errors.Is(err, ErrFileFormat) {
		t.Fatal(err)
	}
}

func TestPoolOneOpenPerActiveFile(t *testing.T) {
	var calls atomic.Int32
	opening, continueOpen := make(chan struct{}), make(chan struct{})
	pool, err := newFilePool(1, 1, time.Minute, func(ctx context.Context, key FileID) (*sql.DB, error) {
		if calls.Add(1) == 1 {
			close(opening)
		}
		<-continueOpen
		return sql.Open("sqlite", ":memory:")
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.shutdown(context.Background())
	var workers sync.WaitGroup
	for range 100 {
		workers.Go(func() {
			if err := pool.withFile(t.Context(), FileID{7}, func(db *sql.DB) error { return db.PingContext(t.Context()) }); err != nil {
				t.Error(err)
			}
		})
	}
	<-opening
	close(continueOpen)
	workers.Wait()
	if calls.Load() != 1 {
		t.Fatalf("opened %d times", calls.Load())
	}
}

package member

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/floegence/floret/v7/storage/spi"
	_ "modernc.org/sqlite"
)

func TestInitializeFreshOnly(t *testing.T) {
	ctx := t.Context()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "member.sqlite")+"?_pragma=foreign_keys(ON)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	initialize := func(ctx context.Context, publish bool) error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := Initialize(ctx, tx, [16]byte{1}, [16]byte{2}, "thread-test"); err != nil {
			return err
		}
		if publish {
			return tx.Commit()
		}
		return tx.Rollback()
	}
	if err := initialize(ctx, false); err != nil {
		t.Fatal(err)
	}
	if err := initialize(ctx, true); err != nil {
		t.Fatal(err)
	}
	if err := initialize(ctx, true); !errors.Is(err, spi.ErrConflict) {
		t.Fatalf("reinitialize: %v", err)
	}
	var objects, format, rev int
	if err := db.QueryRow(`SELECT (SELECT count(*) FROM sqlite_schema WHERE type='table'),format,commit_rev FROM floret_thread_header WHERE singleton=1 AND leaf_seq IS NULL`).Scan(&objects, &format, &rev); err != nil || objects != 5 || format != 2 || rev != 0 {
		t.Fatalf("header: %d %d %d %v", objects, format, rev, err)
	}
	if _, err := db.Exec(`INSERT INTO content_blocks VALUES(100,0,0,0,x'01',zeroblob(32))`); err == nil {
		t.Fatal("orphan body was accepted")
	}
}

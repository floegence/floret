package threadsqlite

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/threadsqlite/member"
)

func TestMemberFilePoolRejectsOtherFormatsAndDDLDrift(t *testing.T) {
	ctx := t.Context()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	store := FileID{1}
	records, err := NewFiles(root, store, 2, 0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer records.Close(context.Background())
	members, err := NewMemberFiles(root, store, 2, 0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer members.Close(context.Background())
	previous := Reference{FileID{2}, "old"}
	target := Reference{FileID{3}, "new"}
	if err := records.Create(ctx, previous); err != nil {
		t.Fatal(err)
	}
	if err := members.Create(ctx, target); err != nil {
		t.Fatal(err)
	}
	reject := func(pool *Files, ref Reference, want error) {
		t.Helper()
		if err := pool.Update(ctx, ref, func(*sql.Tx) error { t.Fatal("mutation ran on invalid file"); return nil }); !errors.Is(err, want) {
			t.Fatalf("expected %v, got %v", want, err)
		}
	}
	reject(records, target, ErrFileFormat)
	reject(members, previous, ErrFileFormat)
	reject(members, Reference{target.File, previous.Thread}, ErrFileIdentity)
	abort := errors.New("rollback requested")
	if err := members.Update(ctx, target, func(tx *sql.Tx) error {
		journal, err := member.OpenJournal(ctx, tx, target.Thread, false)
		if err != nil {
			return err
		}
		if _, err := journal.PutBody("raw", 1, []byte("rollback")); err != nil {
			return err
		}
		return abort
	}); !errors.Is(err, abort) {
		t.Fatal(err)
	}
	if err := members.View(ctx, target, func(tx *sql.Tx) error {
		var count, rev int
		if err := tx.QueryRow(`SELECT (SELECT count(*) FROM history_events),commit_rev FROM floret_thread_header`).Scan(&count, &rev); err != nil {
			return err
		}
		if count != 0 || rev != 0 {
			t.Fatal("rollback published a revision")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := members.Update(ctx, target, func(tx *sql.Tx) error { _, err := tx.Exec(`DROP INDEX history_entry_order`); return err }); err != nil {
		t.Fatal(err)
	}
	reject(members, target, ErrFileFormat)
}

//go:build floret_layout_cutover

package sessiontree

import (
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/floegence/floret/v7/internal/threadsqlite/member"
	"github.com/floegence/floret/v7/storage/spi"
	_ "modernc.org/sqlite"
)

func TestMemberOutputMigrationKeepsCommitWatermarks(t *testing.T) {
	ctx := t.Context()
	source := newMigrationTestBackend()
	var pinned OutputSnapshot
	if err := source.Update(ctx, func(tx spi.WriteTx) error {
		stream, err := newOutputSource(tx, "stream")
		if err != nil {
			return err
		}
		stream, err = appendOutputSource(ctx, tx, stream, "first")
		if err != nil {
			return err
		}
		pinned = snapshotOutput(stream)
		if _, err := appendOutputSource(ctx, tx, stream, "second"); err != nil {
			return err
		}
		return retainOutputSource(tx, "stream")
	}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "member.sqlite")
	open := func() *sql.DB {
		db, err := sql.Open("sqlite", path+"?_pragma=foreign_keys(ON)")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		return db
	}
	db := open()
	var result memberOutputPins
	if err := source.View(ctx, func(old spi.ReadTx) error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := member.Initialize(ctx, tx, [16]byte{1}, [16]byte{2}, "thread"); err != nil {
			return err
		}
		journal, err := member.OpenJournal(ctx, tx, "thread", false)
		if err != nil {
			return err
		}
		result, err = importMemberOutput(ctx, old, journal, "stream", &offlineOutputNeed{full: true, references: 2, pins: map[OutputSnapshot]struct{}{pinned: {}}}, 10)
		if err != nil {
			return err
		}
		return tx.Commit()
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = open()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	journal, err := member.OpenJournal(ctx, tx, "thread", false)
	if err != nil {
		t.Fatal(err)
	}
	frozen, err := journal.OutputAt("stream", result.Frozen[pinned].BoundarySeq)
	if err != nil || !frozen.Sealed || frozen.Reason != "interrupted" || frozen.Retained != 5 {
		t.Fatalf("migrated pin: %+v %v", frozen, err)
	}
	first, err := journal.BlockAtOffset(frozen, 4)
	if err != nil || string(first.Bytes) != "first" {
		t.Fatalf("prefix: %+v %v", first, err)
	}
	current, err := journal.OutputAt("stream", 0)
	if err != nil || current.Sealed || current.Retained != 11 || current.Digest != result.Current.Digest {
		t.Fatalf("migrated writable stream: %+v %v", current, err)
	}
	if _, err := journal.AppendOutput("stream", 11, 20, []byte("third"), ""); err != nil {
		t.Fatal(err)
	}
	if _, err := journal.AppendOutput("stream", 11, 21, []byte("twice"), ""); !errors.Is(err, spi.ErrConflict) {
		t.Fatalf("accepted duplicate append: %v", err)
	}
	if _, err := journal.BlockAtOffset(frozen, 5); !errors.Is(err, spi.ErrInvalidArgument) {
		t.Fatal("frozen view expanded", err)
	}
}

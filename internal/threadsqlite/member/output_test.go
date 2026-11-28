package member

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/storage/spi"
)

type testMember struct {
	db     *sql.DB
	thread identity.ThreadID
	path   string
}

func newTestMember(t *testing.T, thread identity.ThreadID, key byte) testMember {
	t.Helper()
	path := filepath.Join(t.TempDir(), "member.sqlite")
	db, err := sql.Open("sqlite", path+"?_pragma=foreign_keys(ON)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := Initialize(t.Context(), tx, [16]byte{1}, [16]byte{key}, thread); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return testMember{db, thread, path}
}

func (file testMember) transaction(ctx context.Context, use func(*Journal) error) error {
	tx, err := file.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	journal, err := OpenJournal(ctx, tx, file.thread, false)
	if err != nil {
		return err
	}
	if err := use(journal); err != nil {
		return err
	}
	return tx.Commit()
}

func TestMemberForkPinsEachReachableOutput(t *testing.T) {
	ctx := t.Context()
	source, target := newTestMember(t, "source", 2), newTestMember(t, "target", 3)
	var first, empty Output
	if err := source.transaction(ctx, func(writer *Journal) error {
		var err error
		empty, err = writer.CreateOutput("capture", 1)
		if err != nil {
			return err
		}
		first, err = writer.AppendOutput("capture", 0, 2, []byte("trace"), "")
		if err != nil {
			return err
		}
		_, err = writer.AppendOutput("capture", 5, 3, []byte("tail"), "")
		if err != nil {
			return err
		}
		_, err = writer.AppendOutput("capture", 9, 4, []byte("lost"), "storage_limit")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	var copied []Output
	if err := source.transaction(ctx, func(reader *Journal) error {
		return target.transaction(ctx, func(writer *Journal) error {
			var err error
			copied, err = reader.ForkOutput(writer, "capture", []int64{0, first.BoundarySeq, empty.BoundarySeq, first.BoundarySeq}, 5)
			return err
		})
	}); err != nil {
		t.Fatal(err)
	}
	if err := source.transaction(ctx, func(writer *Journal) error {
		_, err := writer.AppendOutput("capture", 13, 6, []byte("later"), "")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := source.db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(source.path); err != nil {
		t.Fatal(err)
	}
	if copied[0].Observed != 13 || copied[0].Retained != 9 || copied[0].Reason != "storage_limit" || !copied[0].Sealed || copied[1].Observed != 5 || copied[1].Reason != "interrupted" || !copied[1].Sealed || copied[2].Retained != 0 || copied[2].Blocks != 0 || copied[1].BoundarySeq != copied[3].BoundarySeq {
		t.Fatalf("incorrect inherited watermarks: %+v", copied)
	}
	var blocks int
	if err := target.db.QueryRow(`SELECT count(*) FROM content_blocks`).Scan(&blocks); err != nil || blocks != 2 {
		t.Fatalf("physical blocks=%d: %v", blocks, err)
	}
	grandchild := newTestMember(t, "grandchild", 4)
	if err := target.transaction(ctx, func(reader *Journal) error {
		if _, err := reader.AppendOutput("capture", 13, 7, []byte("invalid"), ""); !errors.Is(err, spi.ErrConflict) {
			t.Fatalf("appended to inherited source: %v", err)
		}
		return grandchild.transaction(ctx, func(writer *Journal) error {
			pins, err := reader.ForkOutput(writer, "capture", []int64{copied[1].BoundarySeq}, 8)
			if err != nil {
				return err
			}
			if _, err := writer.BlockAtOffset(pins[0], 5); !errors.Is(err, spi.ErrInvalidArgument) {
				t.Fatalf("range crossed pin: %v", err)
			}
			block, err := writer.BlockAtOffset(pins[0], 4)
			if err != nil || string(block.Bytes) != "trace" {
				t.Fatalf("re-fork: %+v %v", block, err)
			}
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
}

func TestFailedOutputCopyCannotPublishPartialRevision(t *testing.T) {
	ctx := t.Context()
	source, target := newTestMember(t, "source", 2), newTestMember(t, "target", 3)
	if err := source.transaction(ctx, func(writer *Journal) error {
		if _, err := writer.CreateOutput("log", 1); err != nil {
			return err
		}
		if _, err := writer.AppendOutput("log", 0, 1, []byte("abcd"), ""); err != nil {
			return err
		}
		if _, err := writer.AppendOutput("log", 4, 1, []byte("efgh"), ""); err != nil {
			return err
		}
		_, err := writer.SealOutput("log", 1, "")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := source.db.Exec(`UPDATE content_blocks SET sha256=zeroblob(32) WHERE block_no=1`); err != nil {
		t.Fatal(err)
	}
	err := source.transaction(ctx, func(reader *Journal) error {
		return target.transaction(ctx, func(writer *Journal) error {
			_, err := reader.ForkOutput(writer, "log", []int64{0}, 2)
			return err
		})
	})
	if !errors.Is(err, ErrContentIntegrity) {
		t.Fatalf("copy corrupt tail: %v", err)
	}
	var revision, blocks, events int
	if err := target.db.QueryRow(`SELECT commit_rev,(SELECT count(*) FROM history_events),(SELECT count(*) FROM content_blocks) FROM floret_thread_header`).Scan(&revision, &events, &blocks); err != nil || revision != 0 || events != 0 || blocks != 0 {
		t.Fatalf("partial publication: revision=%d events=%d blocks=%d %v", revision, events, blocks, err)
	}
}

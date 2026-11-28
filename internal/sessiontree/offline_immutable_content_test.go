//go:build floret_layout_cutover

package sessiontree

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/threadsqlite/member"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/tools"
	_ "modernc.org/sqlite"
)

func TestStageImmutableBodyPreservesPublishedBytes(t *testing.T) {
	ctx := t.Context()
	source := newMigrationTestBackend()
	text := strings.Repeat("a", member.BlockBytes-1) + "\u754c" + strings.Repeat("b", member.BlockBytes+21)
	var body canonicalBodyRef
	if err := source.Update(ctx, func(tx spi.WriteTx) error {
		var err error
		body, err = retainCanonicalBody(ctx, tx, text)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "member.sqlite")+"?_pragma=foreign_keys(ON)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := member.Initialize(ctx, tx, [16]byte{1}, [16]byte{2}, "thread-test"); err != nil {
		t.Fatal(err)
	}
	journal, err := member.OpenJournal(ctx, tx, "thread-test", false)
	if err != nil {
		t.Fatal(err)
	}
	var ref member.BodyRef
	if err := source.View(ctx, func(reader spi.ReadTx) error {
		var err error
		ref, err = stageImmutableBody(ctx, reader, journal, "text", 0, body)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if ref.Digest != StableHash(text) || ref.End != int64(len(text)) {
		t.Fatal("source identity changed")
	}
	if _, err := journal.PutBodyStream("text", 0, ref.End, sha256.Sum256([]byte(text)), strings.NewReader(text+"!")); !errors.Is(err, member.ErrContentIntegrity) {
		t.Fatalf("duplicate ignored excess bytes: %v", err)
	}
	if _, err := journal.PutBodyStream("text", 0, ref.End, sha256.Sum256([]byte(text)), strings.NewReader(text[1:]+"!")); !errors.Is(err, member.ErrContentIntegrity) {
		t.Fatalf("duplicate ignored wrong digest: %v", err)
	}
	for _, offset := range []int{0, member.BlockBytes - 1, member.BlockBytes + 1, len(text) - 15, len(text)} {
		got, err := journal.ReadBody("text", ref, int64(offset), 30)
		if err != nil || string(got) != text[offset:min(offset+30, len(text))] {
			t.Fatalf("range %d: %x %v", offset, got, err)
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := member.OpenJournal(canceled, tx, "thread-test", false); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE content_blocks SET sha256=zeroblob(32) WHERE block_no=1`); err != nil {
		t.Fatal(err)
	}
	if got, err := journal.ReadBody("text", ref, member.BlockBytes-1, 30); !errors.Is(err, member.ErrContentIntegrity) || len(got) != 0 {
		t.Fatalf("corruption returned partial content: %x %v", got, err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM sqlite_schema WHERE type='table'`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("published rejected candidate: %d %v", count, err)
	}
	if err := source.View(ctx, func(reader spi.ReadTx) error {
		var original bytes.Buffer
		err := visitCanonicalBody(ctx, reader, body, func(part []byte) error { _, err := original.Write(part); return err })
		if err == nil && original.String() != text {
			t.Fatal("rollback modified source")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestMemberMigrationPreservesExactActivityJSON(t *testing.T) {
	ctx := t.Context()
	source := newMigrationTestBackend()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "member.sqlite")+"?_pragma=foreign_keys(ON)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := member.Initialize(ctx, tx, [16]byte{1}, [16]byte{2}, "thread"); err != nil {
		t.Fatal(err)
	}
	journal, err := member.OpenJournal(ctx, tx, "thread", false)
	if err != nil {
		t.Fatal(err)
	}
	for ordinal, size := range []int{16, member.BlockBytes + 1} {
		activity := &tools.ActivityPresentation{Label: "read", Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Rows: []tools.StructuredActivityRow{{Content: "retained"}}}}
		original, err := json.MarshalIndent(activity, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		original = append(original, bytes.Repeat([]byte(" "), size)...)
		entry := Entry{ID: []string{"inline", "blocks"}[ordinal], ThreadID: "thread", Type: EntryToolCall, CreatedAt: time.Unix(17, 0).UTC(), PathDepth: 1}
		entry.Message.Activity = activity
		entry = PrepareEntry(entry)
		stored := canonicalStoredEntry{Entry: entry, Fields: map[string]canonicalBodyRef{}}
		stored.Entry.Raw = ""
		stored.Entry.Message.Activity = nil
		if err := source.Update(ctx, func(old spi.WriteTx) error {
			ref, err := retainCanonicalBody(ctx, old, string(original))
			if err != nil {
				return err
			}
			stored.Fields["activity_json"] = ref
			return stageStoredMemberEntry(ctx, old, journal, int64(ordinal), stored)
		}); err != nil {
			t.Fatal(err)
		}
		event, err := journal.Entry(entry.ID)
		if err != nil {
			t.Fatal(err)
		}
		data, err := readMemberEntryData(journal, event)
		if err != nil {
			t.Fatal(err)
		}
		got := data.ActivityJSON
		if size > memberInlineFieldBytes {
			var text bytes.Buffer
			if err := journal.CopyBody(memberBodyClass(entry.Type, "activity_json"), data.Bodies["activity_json"], &text); err != nil {
				t.Fatal(err)
			}
			got = text.String()
		}
		if got != string(original) {
			t.Fatal("activity bytes changed")
		}
		restored, err := restoreMemberEntry(journal, event, data)
		if err != nil || restored.RawHash != entry.RawHash {
			t.Fatalf("semantic hash changed: %v", err)
		}
	}
}

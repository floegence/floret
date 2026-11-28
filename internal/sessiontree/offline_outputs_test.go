//go:build floret_layout_cutover

package sessiontree

import (
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session/artifact"
	"github.com/floegence/floret/v7/internal/threadsqlite/member"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestOfflineJournalOwnsExactOutputBytes(t *testing.T) {
	ctx := t.Context()
	backend, repo, attempt := canonicalSettlementFixture(t, true)
	work := HostWork{ID: "capture", ExecutorID: "owner", Invocation: attempt.Invocation, State: "bound", OutputStream: HostWorkOutputSourceID("thread", "capture")}
	if _, err := repo.BindHostWork(ctx, work); err != nil {
		t.Fatal(err)
	}
	appendCapture := func(offset int64, text string) {
		t.Helper()
		if _, err := repo.AppendHostWorkOutput(ctx, HostWorkOutputAppend{ThreadID: "thread", WorkID: work.ID, ExecutorID: work.ExecutorID, Offset: offset, Text: text}); err != nil {
			t.Fatal(err)
		}
	}
	appendCapture(0, "prefix")
	request := canonicalEffectRequest(attempt)
	text := strings.Repeat("stored-output-", 15000)
	request.FullOutput = &artifact.FullOutput{Text: text}
	result, err := repo.FinishEffectDispatch(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Fork(ctx, ForkOptions{SourceThreadID: "thread", NewThreadID: "fork"}); err != nil {
		t.Fatal(err)
	}
	appendCapture(6, "-tail")
	owner, err := openCurrentGoalRepo(t, backend)
	if err != nil {
		t.Fatal(err)
	}
	for owner.HasPendingMigration() {
		progress, err := owner.MigrateNextThread(ctx)
		if err != nil || !progress && owner.HasPendingMigration() {
			t.Fatalf("source migration not ready: %v", err)
		}
	}
	for _, thread := range []string{"thread", "fork"} {
		target := newMigrationTestBackend()
		err := backend.View(ctx, func(source spi.ReadTx) error {
			return target.Update(ctx, func(destination spi.WriteTx) error {
				return StageOfflineJournal(ctx, source, destination, thread)
			})
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Run(thread+"-member", func(t *testing.T) {
			verifyDetachedOutputCutover(t, target, thread, result.Artifact.ID, work.OutputStream, text)
		})
		if err := target.View(ctx, func(tx spi.ReadTx) error {
			capture, err := readOutputSource(tx, work.OutputStream)
			if err != nil {
				return err
			}
			want := "prefix-tail"
			if thread == "fork" {
				want = "prefix"
			}
			body, err := materializeOutputSource(ctx, tx, capture)
			if err != nil || body != want || capture.Sealed != (thread == "fork") || capture.References != 1 {
				t.Fatalf("%s: capture or ownership changed: %v", thread, err)
			}
			item, found, err := canonicalArtifactReader(ctx, tx).record(thread, result.Artifact.ID)
			if err != nil || !found || item.ThreadID != thread || item.Ref != *result.Artifact || item.Text != "" {
				t.Fatalf("%s: artifact identity mismatch: %v", thread, err)
			}
			if err := hydrateArtifactOutput(ctx, tx, &item); err != nil || item.Text != text {
				t.Fatalf("%s: detached output mismatch: %v", thread, err)
			}
			used, err := outputStoreUsage(tx)
			if err != nil || used != int64(len(want)+len(text)) {
				t.Fatalf("%s: local usage=%d: %v", thread, used, err)
			}
			if thread == "fork" {
				if _, err := tx.Get(outputSourceNamespace, outputChunkKey(work.OutputStream, 2)); !errors.Is(err, spi.ErrNotFound) {
					t.Fatalf("copied bytes past the fork watermark: %v", err)
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func verifyDetachedOutputCutover(t *testing.T, source *migrationTestBackend, thread, artifactID, stream, text string) {
	ctx := t.Context()
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
	stage := func(old spi.ReadTx) error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := member.Initialize(ctx, tx, [16]byte{1}, [16]byte{2}, identity.ThreadID(thread)); err != nil {
			return err
		}
		journal, err := member.OpenJournal(ctx, tx, identity.ThreadID(thread), false)
		if err != nil {
			return err
		}
		if _, err := StageMemberJournal(ctx, old, journal); err != nil {
			return err
		}
		if _, err := StageMemberOutputs(ctx, old, journal, time.Unix(100, 5)); err != nil {
			return err
		}
		return tx.Commit()
	}
	for problem, damage := range map[string]func(spi.WriteTx) error{
		"wrong charge":   func(tx spi.WriteTx) error { return putOutputRecord(tx, outputUsageKey(), 0) },
		"unowned stream": func(tx spi.WriteTx) error { _, err := newOutputSource(tx, "alien"); return err },
		"missing chunk":  func(tx spi.WriteTx) error { return tx.Delete(outputSourceNamespace, outputChunkKey(stream, 1)) },
	} {
		t.Run(problem, func(t *testing.T) {
			broken := &migrationTestBackend{records: cloneMigrationRecords(source.records)}
			if err := broken.Update(ctx, damage); err != nil {
				t.Fatal(err)
			}
			before := cloneMigrationRecords(broken.records)
			if err := broken.View(ctx, stage); err == nil || !reflect.DeepEqual(before, broken.records) {
				t.Fatal("invalid input accepted or mutated")
			}
			var tables int
			if err := db.QueryRow(`SELECT count(*) FROM sqlite_schema WHERE type='table'`).Scan(&tables); err != nil || tables != 0 {
				t.Fatalf("partially published member: %d tables, %v", tables, err)
			}
		})
	}
	if err := source.View(ctx, stage); err != nil {
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
	journal, err := member.OpenJournal(ctx, tx, identity.ThreadID(thread), false)
	if err != nil {
		t.Fatal(err)
	}
	if err := source.View(ctx, func(old spi.ReadTx) error {
		got, err := VerifyMemberOutputs(ctx, old, journal)
		if err != nil {
			return err
		}
		want := "prefix-tail"
		if thread == "fork" {
			want = "prefix"
		}
		if got.Streams != 2 || got.Bindings != 1 || got.Artifacts != 1 || got.RetainedBytes != int64(len(want)+len(text)) {
			t.Fatalf("unexpected inventory: %+v", got)
		}
		original, found, err := canonicalArtifactReader(ctx, old).record(thread, artifactID)
		if err != nil || !found {
			t.Fatalf("expected released artifact: %v", err)
		}
		copied, err := journal.Artifact(artifactID, original.Ref.SizeBytes, original.Ref.SHA256)
		if err != nil || copied.Stream != original.OutputSourceID || copied.Entry != original.CanonicalEntryID || copied.At != original.CreatedAt.UnixNano() {
			t.Fatalf("artifact identity: %v", err)
		}
		if _, err := tx.Exec(`UPDATE content_blocks SET bytes=zeroblob(length(bytes)) WHERE object_seq=? AND block_no=0`, copied.Output.ObjectSeq); err != nil {
			return err
		}
		if _, err := VerifyMemberOutputs(ctx, old, journal); !errors.Is(err, member.ErrContentIntegrity) {
			t.Fatalf("damaged artifact accepted: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

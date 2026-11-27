//go:build floret_layout_cutover

package sessiontree

import (
	"errors"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/internal/session/artifact"
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

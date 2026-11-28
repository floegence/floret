//go:build floret_layout_cutover

package sessiontree

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session/artifact"
	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/internal/threadsqlite/member"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestMemberRepoForkGrowingOutputAfterConversion(t *testing.T) {
	ctx := t.Context()
	old, original, attempt := canonicalSettlementFixture(t, true)
	work := HostWork{ID: "capture", ExecutorID: "owner", Invocation: attempt.Invocation, State: "bound", OutputStream: HostWorkOutputSourceID("thread", "capture")}
	if _, err := original.BindHostWork(ctx, work); err != nil {
		t.Fatal(err)
	}
	request := canonicalEffectRequest(attempt)
	text := strings.Repeat("original artifact\n", 5000)
	request.FullOutput = &artifact.FullOutput{Text: text}
	confirmed, err := original.FinishEffectDispatch(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "member.sqlite")
	repo, backend := openDiskMemberRepo(t, path, true)
	meta, err := original.Thread(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	err = old.View(ctx, func(source spi.ReadTx) error {
		return backend.CreateThreadRecords(ctx, storagebridge.ThreadCreation{Thread: "thread"}, func(tx spi.WriteTx) error {
			empty := meta
			empty.LeafID = ""
			if _, err := initializeCanonicalThread(ctx, tx, empty); err != nil {
				return err
			}
			journal := member.From(tx)
			if _, err := StageMemberJournal(ctx, source, journal); err != nil {
				return err
			}
			if _, err := StageMemberOutputs(ctx, source, journal, time.Now()); err != nil {
				return err
			}
			if err := persistThreadMetadata(ctx, tx, meta); err != nil {
				return err
			}
			_, err := rebuildCanonicalQuery(ctx, tx, meta)
			return err
		})
	})
	if err != nil {
		t.Fatal("convert", err)
	}
	appendOutput := func(thread string, offset int64, body string) error {
		_, err := repo.AppendHostWorkOutput(ctx, HostWorkOutputAppend{ThreadID: thread, WorkID: work.ID, ExecutorID: work.ExecutorID, Offset: offset, Text: body})
		return err
	}
	if err := appendOutput("thread", 0, "trace"); err != nil {
		t.Fatal("append source", err)
	}
	fork := func(source, selected, target string) {
		t.Helper()
		_, err := repo.Fork(ctx, ForkOptions{SourceThreadID: source, EntryID: selected, NewThreadID: target})
		if err != nil {
			t.Fatal("fork "+target, err)
		}
	}
	fork("thread", meta.LeafID, "child")
	if err := appendOutput("thread", 5, "-later"); err != nil {
		t.Fatal(err)
	}
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
	repo, _ = openDiskMemberRepo(t, path, false)
	fork("child", "", "grandchild")
	read := func(thread, want string, sealed bool) {
		t.Helper()
		page, err := repo.ReadHostWorkOutputPage(ctx, HostWorkOutputRequest{ThreadID: thread, WorkID: work.ID, MaxBytes: 64})
		if err != nil || page.Text != want || page.Source.Sealed != sealed || page.Source.SHA256 != StableHash(want) || sealed && page.Source.Reason != "interrupted" {
			t.Fatalf("%s: text=%q sealed=%t %v", thread, page.Text, page.Source.Sealed, err)
		}
	}
	read("thread", "trace-later", false)
	for _, id := range []string{"child", "grandchild"} {
		read(id, "trace", true)
		if err := appendOutput(id, 5, "forbidden"); !errors.Is(err, ErrInvalidThreadAuthority) {
			t.Fatal("fork acquired work", err)
		}
		item, err := repo.ReadArtifact(ctx, ArtifactReadRequest{ThreadID: id, ArtifactID: confirmed.Artifact.ID})
		if err != nil || item.Text != text {
			t.Fatalf("%s artifact: bytes=%d %v", id, len(item.Text), err)
		}
	}
}

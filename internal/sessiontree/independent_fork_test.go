package sessiontree

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/session/artifact"
	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestIndependentForkCutSurvivesSourceFileRemoval(t *testing.T) {
	fixture, canonical, attempt := canonicalSettlementFixture(t, true)
	request := canonicalEffectRequest(attempt)
	text := "tool result\n" + strings.Repeat("four bytes ", 21000) + "end of output"
	request.FullOutput = &artifact.FullOutput{Text: text}
	finished, err := canonical.FinishEffectDispatch(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	open := func(name string) spi.Backend {
		backend, err := storagebridge.Open(t.Context(), storagebridge.Source(storage.SQLite(filepath.Join(root, name))))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = backend.Close() })
		return backend
	}
	source := open("source.sqlite")
	target := open("fork.sqlite")
	if err := source.Update(t.Context(), func(tx spi.WriteTx) error {
		for namespace, records := range fixture.records {
			for key, value := range records {
				if err := tx.Put(namespace, []byte(key), value); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	owner, err := openCurrentGoalRepo(t, source)
	if err != nil {
		t.Fatal(err)
	}
	for owner.HasPendingMigration() {
		progressed, err := owner.MigrateNextThread(t.Context())
		if err != nil || !progressed && owner.HasPendingMigration() {
			t.Fatalf("source migration did not finish: %v", err)
		}
	}
	if _, err = owner.Append(t.Context(), Entry{ID: "after-cut", ThreadID: "thread", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "do not inherit this"}}, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err = openCurrentGoalRepo(t, target); err != nil {
		t.Fatal(err)
	}
	if err := source.View(t.Context(), func(reader spi.ReadTx) error {
		return target.Update(t.Context(), func(writer spi.WriteTx) error {
			_, err := StageIndependentFork(t.Context(), reader, writer, ForkOptions{SourceThreadID: "thread", NewThreadID: "branch", EntryID: finished.Result.ID, EntryIDPinned: true, Now: time.Now().UTC()})
			return err
		})
	}); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(root, "source.sqlite")); err != nil {
		t.Fatal(err)
	}
	target = open("fork.sqlite")
	destination, err := openCurrentGoalRepo(t, target)
	if err != nil {
		t.Fatal(err)
	}
	output, err := destination.ReadArtifact(t.Context(), ArtifactReadRequest{ThreadID: "branch", ArtifactID: finished.Artifact.ID})
	if err != nil || output.Text != text {
		t.Fatalf("retained output: %v (%d bytes)", err, len(output.Text))
	}
	entries, err := destination.Entries(t.Context(), "branch")
	if err != nil || len(entries) == 0 {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.ThreadID != "branch" || entry.Type == EntryEffectAttempt || entry.Message.Content == "do not inherit this" {
			t.Fatalf("unexpected fork entry %s", entry.ID)
		}
	}
	if _, err := destination.Thread(t.Context(), "thread"); !errors.Is(err, ErrThreadNotFound) {
		t.Fatalf("source thread unexpectedly readable: %v", err)
	}
	if _, err = destination.Append(t.Context(), Entry{ID: "continued", ThreadID: "branch", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "continue"}}, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestDetachedOutputCopiesOnlyPinnedChunks(t *testing.T) {
	before, after := newMigrationTestBackend(), newMigrationTestBackend()
	var pin OutputSnapshot
	if err := before.Update(t.Context(), func(tx spi.WriteTx) error {
		live, err := newOutputSource(tx, "stream")
		if err != nil {
			return err
		}
		live, err = appendOutputSource(t.Context(), tx, live, "first")
		if err != nil {
			return err
		}
		pin = snapshotOutput(live)
		_, err = appendOutputSource(t.Context(), tx, live, "later")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := before.View(t.Context(), func(reader spi.ReadTx) error {
		return after.Update(t.Context(), func(tx spi.WriteTx) error {
			source, err := readOutputSource(reader, "stream")
			if err != nil {
				return err
			}
			return retainDetachedOutput(t.Context(), reader, tx, source, pin)
		})
	}); err != nil {
		t.Fatal(err)
	}
	if err := after.View(t.Context(), func(tx spi.ReadTx) error {
		source, err := readOutputSource(tx, "stream")
		if err != nil {
			return err
		}
		if snapshotOutput(source) != pin {
			t.Fatal("pin changed")
		}
		text, err := materializeOutputSource(t.Context(), tx, source)
		if err != nil || text != "first" {
			t.Fatalf("read %q, %v", text, err)
		}
		_, err = tx.Get(outputSourceNamespace, outputChunkKey(source.ID, 2))
		if !errors.Is(err, spi.ErrNotFound) {
			t.Fatal("later output copied")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

package sessiontree

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/session/artifact"
	"github.com/floegence/floret/v7/internal/storagebridge"
	publicstorage "github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestV32ForkRollbackReplayAndSourceDeletion(t *testing.T) {
	for _, name := range []string{"memory", "sqlite"} {
		t.Run(name, func(t *testing.T) {
			fixture, original, attempt := canonicalSettlementFixture(t, true)
			request := canonicalEffectRequest(attempt)
			request.FullOutput = &artifact.FullOutput{Text: "Complete output survives its source"}
			finished, err := original.FinishEffectDispatch(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			source := publicstorage.Memory()
			if name == "sqlite" {
				source = publicstorage.SQLite(t.TempDir() + "/fork.sqlite")
			}
			backend, err := storagebridge.Open(t.Context(), storagebridge.Source(source))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = backend.Close() })
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
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
			repo, err := openCurrentGoalRepo(t, backend)
			if err != nil {
				t.Fatal(err)
			}
			if err := repo.PrepareQueryThread(t.Context(), "thread"); err != nil {
				t.Fatal(err)
			}
			opts := ForkOptions{SourceThreadID: "thread", NewThreadID: "fork", EntryID: finished.Result.ID, EntryIDPinned: true, OriginRequestKey: "fork-request", OriginFingerprint: "fork-fingerprint", Now: time.Now().UTC()}
			before, err := repo.Entries(t.Context(), "thread")
			if err != nil {
				t.Fatal(err)
			}
			rollback := errors.New("fork commit failure")
			repo.backend = &domainCommitBoundaryBackend{Backend: backend, afterCallback: func() error { return rollback }}
			if _, err := repo.Fork(t.Context(), opts); !errors.Is(err, rollback) {
				t.Fatalf("fork rollback = %v", err)
			}
			repo.backend = backend
			if _, err := repo.Thread(t.Context(), "fork"); !errors.Is(err, ErrThreadNotFound) {
				t.Fatalf("rolled back fork is visible: %v", err)
			}
			after, err := repo.Entries(t.Context(), "thread")
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("failed fork changed source: %v", err)
			}
			fork, err := repo.Fork(t.Context(), opts)
			if err != nil {
				t.Fatal(err)
			}
			entries, err := repo.Entries(t.Context(), fork.ID)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				if entry.Type == EntryEffectAttempt {
					t.Fatal("fork inherited effect execution authority")
				}
			}
			if replay, err := repo.Fork(t.Context(), opts); err != nil || replay != fork {
				t.Fatalf("fork replay = %+v, %v", replay, err)
			}
			replayed, err := repo.Entries(t.Context(), fork.ID)
			if err != nil || !reflect.DeepEqual(entries, replayed) {
				t.Fatalf("fork replay changed journal: %v", err)
			}
			conflict := opts
			conflict.OriginFingerprint = "changed"
			if _, err := repo.Fork(t.Context(), conflict); !errors.Is(err, ErrThreadExists) {
				t.Fatalf("conflicting replay = %v", err)
			}
			if _, err := repo.DeleteRootTree(t.Context(), "thread"); err != nil {
				t.Fatal(err)
			}
			for repo.HasPendingMigration() {
				if _, err := repo.MigrateNextThread(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			repo, err = openCurrentGoalRepo(t, backend)
			if err != nil {
				t.Fatal(err)
			}
			if err := repo.PrepareQueryThread(t.Context(), fork.ID); err != nil {
				t.Fatal(err)
			}
			retained, err := repo.Entries(t.Context(), fork.ID)
			if err != nil || !reflect.DeepEqual(entries, retained) {
				t.Fatalf("source deletion changed fork: %v", err)
			}
			var ref *artifact.Ref
			for _, entry := range retained {
				if result := entry.Message.ToolResult; result != nil && result.FullOutput != nil {
					ref = result.FullOutput
				}
			}
			if ref == nil {
				t.Fatal("fork lost its artifact")
			}
			content, err := repo.ReadArtifact(t.Context(), ArtifactReadRequest{ThreadID: fork.ID, ArtifactID: ref.ID})
			if err != nil || content.Text != request.FullOutput.Text {
				t.Fatalf("fork artifact = %+v, %v", content, err)
			}
			if _, err := repo.Append(t.Context(), Entry{ID: "continued", ThreadID: fork.ID, Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "Continue from retained facts"}}, AppendOptions{}); err != nil {
				t.Fatal(err)
			}
			if repo.domainMemory != nil || repo.threadMemory != nil {
				t.Fatal("fork installed a memory repository")
			}
		})
	}
}

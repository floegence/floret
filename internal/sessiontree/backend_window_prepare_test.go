package sessiontree

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestWindowPreparationRejectsChangedOwnerBeforePublication(t *testing.T) {
	for _, operation := range []string{"append", "move", "delete"} {
		t.Run(operation, func(t *testing.T) {
			backend, repo := projectionSourcesFixture(t, 40)
			opts := ItemMembershipWindowOptions{ThreadID: "thread", Limit: 10}
			fields := func(PathMetadata, bool) []string { return nil }
			if _, err := repo.ReadWindowProjection(t.Context(), opts, fields); err != nil {
				t.Fatal(err)
			}
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				return deleteItemMembership(t.Context(), tx, "thread")
			}); err != nil {
				t.Fatal(err)
			}
			writer := reopenCurrentStartupRepo(t, backend, false)
			wrapped := &appendBetweenWindowReads{Backend: backend}
			var afterMutation map[string]map[string][]byte
			wrapped.after = func() {
				var err error
				switch operation {
				case "append":
					_, err = writer.Append(t.Context(), Entry{ID: "new-owner", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "new fact"}}, AppendOptions{})
				case "move":
					err = writer.MoveLeaf(t.Context(), "thread", "call")
				case "delete":
					_, err = writer.DeleteRootTree(t.Context(), "thread")
				}
				if err != nil {
					t.Fatal(err)
				}
				afterMutation = cloneMigrationRecords(backend.records)
			}
			repo.backend = wrapped
			err := repo.prepareWindowProjectionSnapshot(t.Context(), "thread")
			repo.backend = backend
			if !errors.Is(err, spi.ErrConflict) {
				t.Fatalf("changed owner was published: %v", err)
			}
			if !reflect.DeepEqual(afterMutation, backend.records) {
				t.Fatal("stale preparation modified the new owner's records")
			}
			if operation == "delete" {
				return
			}
			got, err := repo.ReadWindowProjection(t.Context(), opts, fields)
			if err != nil {
				t.Fatal(err)
			}
			meta, err := writer.Thread(t.Context(), "thread")
			if err != nil || got.Membership.LeafID != meta.LeafID || got.Facts.LeafID != meta.LeafID || got.ModelContext.LeafID != meta.LeafID {
				t.Fatalf("retry did not use current owner: %v", err)
			}
		})
	}
}

type windowPreparationBackend struct {
	spi.Backend
	batches, maxWrites, maxDeletes int
	after                          func(int) error
}
type windowPreparationTx struct {
	spi.WriteTx
	writes, deletes int
}

func (tx *windowPreparationTx) Put(ns string, key, value []byte) error {
	if ns == itemMembershipNamespace {
		tx.writes++
	}
	return tx.WriteTx.Put(ns, key, value)
}
func (tx *windowPreparationTx) Delete(ns string, key []byte) error {
	if ns == itemMembershipNamespace {
		tx.deletes++
	}
	return tx.WriteTx.Delete(ns, key)
}
func (b *windowPreparationBackend) Update(ctx context.Context, f func(spi.WriteTx) error) error {
	return b.Backend.Update(ctx, func(tx spi.WriteTx) error {
		counted := &windowPreparationTx{WriteTx: tx}
		if err := f(counted); err != nil {
			return err
		}
		b.maxWrites = max(b.maxWrites, counted.writes)
		b.maxDeletes = max(b.maxDeletes, counted.deletes)
		if counted.writes > 0 {
			b.batches++
			if b.after != nil {
				return b.after(b.batches)
			}
		}
		return nil
	})
}
func TestWindowPreparationBatchesStayPrivateAcrossInterruption(t *testing.T) {
	for _, mode := range []string{"failure", "cancel", "panic"} {
		t.Run(mode, func(t *testing.T) {
			backend, repo := projectionSourcesFixture(t, 300)
			opts := ItemMembershipWindowOptions{ThreadID: "thread", Limit: 20}
			fields := func(PathMetadata, bool) []string { return nil }
			want, err := repo.ReadWindowProjection(t.Context(), opts, fields)
			if err != nil {
				t.Fatal(err)
			}
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return deleteItemMembership(t.Context(), tx, "thread") }); err != nil {
				t.Fatal(err)
			}
			canonical := cloneMigrationRecords(backend.records)
			failure := errors.New("injected preparation failure")
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			wrapped := &windowPreparationBackend{Backend: backend}
			wrapped.after = func(batch int) error {
				if batch != 2 {
					return nil
				}
				switch mode {
				case "cancel":
					cancel()
					return ctx.Err()
				case "panic":
					panic(failure)
				default:
					return failure
				}
			}
			repo.backend = wrapped
			func() {
				defer func() {
					if recovered := recover(); recovered != nil {
						if mode != "panic" || recovered != failure {
							t.Fatalf("unexpected panic: %v", recovered)
						}
					}
				}()
				_, err = repo.ReadWindowProjection(ctx, opts, fields)
				if mode == "panic" {
					t.Fatal("missing injected panic")
				}
				expected := failure
				if mode == "cancel" {
					expected = context.Canceled
				}
				if !errors.Is(err, expected) {
					t.Fatalf("interrupted read=%v", err)
				}
			}()
			if wrapped.maxWrites > 128 || wrapped.maxDeletes > 128 {
				t.Fatalf("unbounded batch: puts=%d deletes=%d", wrapped.maxWrites, wrapped.maxDeletes)
			}
			if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
				for _, key := range [][]byte{itemManifestKey("thread"), windowSourcesKey("thread"), contextProjectionKey("thread"), modelContextSourcesKey("thread")} {
					if _, err := tx.Get(itemMembershipNamespace, key); !errors.Is(err, spi.ErrNotFound) {
						t.Fatalf("partial anchor published: %v", err)
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			for namespace, records := range canonical {
				if namespace == itemMembershipNamespace {
					continue
				}
				if !reflect.DeepEqual(records, backend.records[namespace]) {
					t.Fatalf("canonical namespace changed: %q", namespace)
				}
			}
			repo = reopenCurrentStartupRepo(t, backend, false)
			got, err := repo.ReadWindowProjection(t.Context(), opts, fields)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("restart lost window: %v", err)
			}
			before := cloneMigrationRecords(backend.records)
			if _, err := repo.ReadWindowProjection(t.Context(), opts, fields); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, backend.records) {
				t.Fatal("prepared read rewrote metadata")
			}
		})
	}
}

func TestMembershipRebuildPreservesPreparedContextSources(t *testing.T) {
	backend, repo := projectionSourcesFixture(t, 30)
	opts := ItemMembershipWindowOptions{ThreadID: "thread", Limit: 10}
	if _, err := repo.ReadWindowProjection(t.Context(), opts, func(PathMetadata, bool) []string { return nil }); err != nil {
		t.Fatal(err)
	}
	keys := [][]byte{windowSourcesKey("thread"), contextProjectionKey("thread"), modelContextSourcesKey("thread")}
	saved := make([][]byte, len(keys))
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		for i, key := range keys {
			var err error
			saved[i], err = tx.Get(itemMembershipNamespace, key)
			if err != nil {
				return err
			}
		}
		return tx.Delete(itemMembershipNamespace, itemManifestKey("thread"))
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.ItemMembershipWindow(t.Context(), opts); err != nil {
		t.Fatal(err)
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		for i, key := range keys {
			got, err := tx.Get(itemMembershipNamespace, key)
			if err != nil || !bytes.Equal(got, saved[i]) {
				t.Fatalf("unrelated projection changed: %v", err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSourcePreparationUsesBoundedPublicationForEveryReader(t *testing.T) {
	for _, reader := range []string{"membership", "window", "context", "model"} {
		t.Run(reader, func(t *testing.T) {
			backend, repo := projectionSourcesFixture(t, 300)
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return deleteItemMembership(t.Context(), tx, "thread") }); err != nil {
				t.Fatal(err)
			}
			wrapped := &windowPreparationBackend{Backend: backend}
			repo.backend = wrapped
			var err error
			switch reader {
			case "membership":
				_, err = repo.ItemMembershipWindow(t.Context(), ItemMembershipWindowOptions{ThreadID: "thread", Limit: 10})
			case "window":
				_, err = repo.ReadWindowSources(t.Context(), "thread", "", []string{"turn"})
			case "context":
				_, err = repo.ReadThreadContextProjection(t.Context(), "thread", "")
			case "model":
				_, err = repo.ReadModelContextSources(t.Context(), "thread", "")
			}
			if err != nil {
				t.Fatal(err)
			}
			if wrapped.maxWrites > 128 || wrapped.maxDeletes > 128 {
				t.Fatalf("unbounded reader preparation: puts=%d deletes=%d", wrapped.maxWrites, wrapped.maxDeletes)
			}
		})
	}
}

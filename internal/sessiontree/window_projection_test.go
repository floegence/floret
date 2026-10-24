package sessiontree

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

type appendBetweenWindowReads struct {
	spi.Backend
	after func()
}

func (b *appendBetweenWindowReads) View(ctx context.Context, f func(spi.ReadTx) error) error {
	err := b.Backend.View(ctx, f)
	if err == nil && b.after != nil {
		after := b.after
		b.after = nil
		after()
	}
	return err
}
func TestWindowProjectionReadsOneCanonicalSnapshotDuringAppend(t *testing.T) {
	backend, repo := projectionSourcesFixture(t, 180)
	var writer *BackendRepo
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		var err error
		writer, _, err = NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, false)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	opts := ItemMembershipWindowOptions{ThreadID: "thread", Limit: 10}
	fields := func(PathMetadata, bool) []string { return nil }
	if _, err := repo.ReadWindowProjection(t.Context(), opts, fields); err != nil {
		t.Fatal(err)
	}
	for phase := 0; phase < 10; phase++ {
		count := 0
		wrapper := &appendBetweenWindowReads{Backend: backend}
		var hook func()
		hook = func() {
			count++
			if count == phase+1 {
				_, err := writer.Append(t.Context(), Entry{ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "concurrent"}}, AppendOptions{})
				if err != nil {
					t.Fatal(err)
				}
			} else {
				wrapper.after = hook
			}
		}
		wrapper.after = hook
		repo.backend = wrapper
		got, err := repo.ReadWindowProjection(t.Context(), opts, fields)
		repo.backend = backend
		if err != nil {
			t.Fatalf("append after read %d: %v", phase, err)
		}
		if got.Membership.LeafID != got.Facts.LeafID || got.Membership.LeafID != got.ModelContext.LeafID {
			t.Fatal("mixed leaves")
		}
	}
}
func TestMemoryWindowProjectionMatchesPreparedBackend(t *testing.T) {
	_, repo := projectionSourcesFixture(t, 10)
	path, err := repo.Path(t.Context(), "thread", "")
	if err != nil {
		t.Fatal(err)
	}
	memory := newMemoryRepo(time.Now)
	memory.threads["thread"] = ThreadMeta{ID: "thread", LeafID: path[len(path)-1].ID}
	memory.entries["thread"] = path
	opts := ItemMembershipWindowOptions{ThreadID: "thread", Limit: 5}
	fields := func(PathMetadata, bool) []string { return nil }
	want, err := repo.ReadWindowProjection(t.Context(), opts, fields)
	if err != nil {
		t.Fatal(err)
	}
	got, err := memory.ReadWindowProjection(t.Context(), opts, fields)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Membership, want.Membership) || !reflect.DeepEqual(got.Context, want.Context) || !reflect.DeepEqual(got.ModelContext, want.ModelContext) {
		t.Fatal("memory and backend window facts differ")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := memory.ReadWindowProjection(ctx, opts, fields); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestWindowProjectionUsesPublishedOwnerOptions(t *testing.T) {
	backend, _ := projectionSourcesFixture(t, 8)
	owner := reopenCurrentStartupRepo(t, backend, true)
	for _, delayed := range []bool{false, true} {
		t.Run(fmt.Sprint(delayed), func(t *testing.T) {
			var facade *BackendRepo
			wait := func(context.Context) error { return nil }
			if delayed {
				wait = func(context.Context) error {
					if facade.delegate.Load() == nil {
						return facade.InstallDelegate(owner)
					}
					return nil
				}
			}
			facade = NewDeferredBackendRepo(backend, time.Now, wait)
			if !delayed {
				if err := facade.InstallDelegate(owner); err != nil {
					t.Fatal(err)
				}
			}
			options := ItemMembershipWindowOptions{ThreadID: "thread", Limit: 5}
			fields := func(PathMetadata, bool) []string { return nil }
			want, err := owner.ReadWindowProjection(t.Context(), options, fields)
			if err != nil || want.ParentEvents == nil {
				t.Fatalf("published owner: %+v, %v", want.ParentEvents, err)
			}
			got, err := facade.ReadWindowProjection(t.Context(), options, fields)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("deferred facade lost published window facts: parents=%+v want=%+v err=%v", got.ParentEvents, want.ParentEvents, err)
			}
		})
	}
}

func TestWindowProjectionFreezesQueueWithItemWindow(t *testing.T) {
	backend, repo := projectionSourcesFixture(t, 8)
	writer := reopenCurrentStartupRepo(t, backend, false)
	opts := ItemMembershipWindowOptions{ThreadID: "thread", Limit: 5}
	fields := func(PathMetadata, bool) []string { return nil }
	if _, err := repo.ReadWindowProjection(t.Context(), opts, fields); err != nil {
		t.Fatal(err)
	}
	meta, err := repo.Thread(t.Context(), "thread")
	if err != nil {
		t.Fatal(err)
	}
	queueCounts := map[string]int{meta.LeafID: 0}
	queueCount := 0
	for phase := range 16 {
		reads := 0
		wrapper := &appendBetweenWindowReads{Backend: backend}
		var hook func()
		hook = func() {
			reads++
			if reads != phase+1 {
				wrapper.after = hook
				return
			}
			entry, err := writer.Append(t.Context(), Entry{ID: fmt.Sprint("queued-", phase), ThreadID: "thread", Type: EntryQueueAdded, Payload: []byte(fmt.Sprintf("{\"id\":\"queued-%d\"}", phase))}, AppendOptions{})
			if err != nil {
				t.Fatal(err)
			}
			queueCount++
			queueCounts[entry.ID] = queueCount
		}
		wrapper.after = hook
		repo.backend = wrapper
		got, err := repo.ReadWindowProjection(t.Context(), opts, fields)
		repo.backend = backend
		if err != nil {
			t.Fatal(err)
		}
		want, found := queueCounts[got.Membership.LeafID]
		if !found || len(got.QueueSources.Pending) != want {
			t.Fatalf("phase %d mixed snapshots: queue=%d want=%d leaf=%s", phase, len(got.QueueSources.Pending), want, got.Membership.LeafID)
		}
	}
}

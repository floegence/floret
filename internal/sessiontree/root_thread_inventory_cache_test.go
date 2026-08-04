package sessiontree_test

import (
	"context"
	"errors"
	"testing"
	"time"

	. "github.com/floegence/floret/v3/internal/sessiontree"
	"github.com/floegence/floret/v3/internal/storagebridge"
	"github.com/floegence/floret/v3/internal/storagecodec"
	publicstorage "github.com/floegence/floret/v3/storage"
	"github.com/floegence/floret/v3/storage/spi"
)

var testDomainNamespace = "floret.domain"
var testInventoryKey = storagecodec.Tuple(
	storagecodec.TupleString("sessiontree"), storagecodec.TupleString("root_thread_inventory"),
)

func TestBackendRootThreadInventoryCacheTracksCommittedStateAndIsolatesCallers(t *testing.T) {
	ctx := context.Background()
	backend, err := storagebridge.Open(ctx, storagebridge.Source(publicstorage.Memory()))
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	repo, err := NewBackendRepo(ctx, backend, DefaultLeasePolicy, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}

	if _, err := repo.CreateThread(ctx, ThreadMeta{
		ID: "thread", CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	first, err := repo.ListRootThreadInventory(ctx, ListThreadsOptions{IncludeArchived: true, RootOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || first[0].Meta.ID != "thread" {
		t.Fatalf("unexpected first inventory: %#v", first)
	}
	if first[0].ProjectionFingerprint == [32]byte{} {
		t.Fatal("root inventory projection fingerprint is empty")
	}
	firstFingerprint := first[0].ProjectionFingerprint
	first[0].Meta.ID = "caller mutation"
	first[0].Authority.Thread.ID = "caller mutation"

	second, err := repo.ListRootThreadInventory(ctx, ListThreadsOptions{IncludeArchived: true, RootOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 1 || second[0].Meta.ID != "thread" || second[0].Authority.Thread.ID != "thread" {
		t.Fatalf("cached inventory leaked caller mutation: %#v", second)
	}
	if second[0].ProjectionFingerprint != firstFingerprint {
		t.Fatal("unchanged inventory projection fingerprint changed")
	}
	if _, err := repo.CreateThread(ctx, ThreadMeta{
		ID: "newer", CreatedAt: now.Add(time.Second), UpdatedAt: now.Add(time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	third, err := repo.ListRootThreadInventory(ctx, ListThreadsOptions{IncludeArchived: true, RootOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(third) != 2 || third[0].Meta.ID != "newer" || third[1].Meta.ID != "thread" {
		t.Fatalf("cache did not track committed mutation: %#v", third)
	}
	if third[0].ProjectionFingerprint == [32]byte{} || third[0].ProjectionFingerprint == firstFingerprint || third[1].ProjectionFingerprint != firstFingerprint {
		t.Fatal("committed inventory did not preserve per-item projection fingerprints")
	}
}

func TestBackendRootThreadInventoryCacheStillRejectsExternalDrift(t *testing.T) {
	ctx := context.Background()
	backend, err := storagebridge.Open(ctx, storagebridge.Source(publicstorage.Memory()))
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	repo, err := NewBackendRepo(ctx, backend, DefaultLeasePolicy, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.ListRootThreadInventory(ctx, ListThreadsOptions{IncludeArchived: true, RootOnly: true}); err != nil {
		t.Fatal(err)
	}

	drifted, err := storagecodec.EncodeEnvelope("sessiontree-root-thread-inventory", []byte(`{"version":1,"items":null}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(ctx, func(tx spi.WriteTx) error {
		return tx.Put(testDomainNamespace, testInventoryKey, drifted)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.ListRootThreadInventory(ctx, ListThreadsOptions{IncludeArchived: true, RootOnly: true}); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("drifted inventory error=%v, want ErrAuthorityCorrupt", err)
	}
}

package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/provider/cache"
	"github.com/floegence/floret/v7/internal/storagebridge"
	publicstorage "github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestContextRequestLookupIsBoundedAcrossRestartAndMissingMatches(t *testing.T) {
	inner, err := storagebridge.Open(t.Context(), storagebridge.Source(publicstorage.SQLite(t.TempDir()+"/requests.sqlite")))
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	backend := &requestReadBackend{Backend: inner}
	kernel, err := NewBackendKernel(t.Context(), backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1100; i++ {
		entry := "other"
		if i < 100 {
			entry = "wanted"
		}
		request := cache.ProviderRequestRecord{ID: fmt.Sprint(i), PromptScopeID: "scope", ThreadID: "thread", ModelContextEntryID: entry}
		if err := kernel.AppendProviderRequest(t.Context(), request); err != nil {
			t.Fatal(err)
		}
	}
	check := func(entry, want string) {
		t.Helper()
		backend.gets, backend.scans = 0, 0
		got, found, err := kernel.LatestModelContextRequest(t.Context(), "scope", "thread", entry)
		if err != nil || found != (want != "") || got.ID != want || backend.gets > 4 || backend.scans != 0 {
			t.Fatalf("got=%s found=%v err=%v gets=%d scans=%d", got.ID, found, err, backend.gets, backend.scans)
		}
	}
	check("wanted", "99")
	check("unused", "")
	kernel, err = NewBackendKernel(t.Context(), backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	check("wanted", "99")
	check("unused", "")
	// A released store has canonical requests but no new disposable index.
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return deleteContextRequestLocators(tx, "scope") }); err != nil {
		t.Fatal(err)
	}
	got, found, err := kernel.LatestModelContextRequest(t.Context(), "scope", "thread", "wanted")
	if err != nil || !found || got.ID != "99" {
		t.Fatalf("prepare: %+v %v %v", got, found, err)
	}
	check("wanted", "99")
	// Proving an absent identity finishes the remaining historical suffix once.
	if _, found, err := kernel.LatestModelContextRequest(t.Context(), "scope", "thread", "unused"); err != nil || found {
		t.Fatalf("missing: %v %v", found, err)
	}
	check("unused", "")
	if err := kernel.DeletePromptScopes(t.Context(), "scope"); err != nil {
		t.Fatal(err)
	}
	check("wanted", "")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := kernel.LatestModelContextRequest(ctx, "scope", "thread", "wanted"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

type contextLookupBackend struct {
	spi.Backend
	requestReads, updates int
	fail                  bool
}
type contextLookupTx struct {
	spi.WriteTx
	owner *contextLookupBackend
}

func (b *contextLookupBackend) Update(ctx context.Context, f func(spi.WriteTx) error) error {
	b.updates++
	return b.Backend.Update(ctx, func(tx spi.WriteTx) error { return f(contextLookupTx{tx, b}) })
}
func (t contextLookupTx) Get(ns string, key []byte) ([]byte, error) {
	if ns == promptRecordsNamespace && bytes.HasPrefix(key, promptRecordPrefix("scope", promptRequest)) {
		t.owner.requestReads++
	}
	return t.WriteTx.Get(ns, key)
}
func (t contextLookupTx) Put(ns string, key, value []byte) error {
	if t.owner.fail && ns == promptRequestLocatorNamespace {
		return errors.New("injected index failure")
	}
	return t.WriteTx.Put(ns, key, value)
}
func TestContextRequestReversePreparationAndRollback(t *testing.T) {
	inner, err := storagebridge.Open(t.Context(), storagebridge.Source(publicstorage.Memory()))
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	backend := &contextLookupBackend{Backend: inner}
	kernel, err := NewBackendKernel(t.Context(), backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 100 {
		if err := kernel.AppendProviderRequest(t.Context(), cache.ProviderRequestRecord{ID: fmt.Sprint(i), PromptScopeID: "scope", ThreadID: "thread", ModelContextEntryID: "context"}); err != nil {
			t.Fatal(err)
		}
	}
	clear := func() {
		t.Helper()
		if err := inner.Update(t.Context(), func(tx spi.WriteTx) error { return deleteContextRequestLocators(tx, "scope") }); err != nil {
			t.Fatal(err)
		}
	}
	clear()
	backend.requestReads, backend.updates = 0, 0
	got, found, err := kernel.LatestModelContextRequest(t.Context(), "scope", "thread", "context")
	if err != nil || !found || got.ID != "99" || backend.requestReads != 16 || backend.updates != 1 {
		t.Fatalf("latest=%s found=%v err=%v source reads=%d updates=%d", got.ID, found, err, backend.requestReads, backend.updates)
	}
	clear()
	backend.fail = true
	if _, _, err := kernel.LatestModelContextRequest(t.Context(), "scope", "thread", "context"); err == nil {
		t.Fatal("accepted failed index write")
	}
	if err := inner.View(t.Context(), func(tx spi.ReadTx) error {
		_, found, err := contextRequestCoverage(tx, "scope")
		if found {
			t.Fatal("failed transaction published coverage")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	backend.fail = false
	if got, found, err := kernel.LatestModelContextRequest(t.Context(), "scope", "thread", "context"); err != nil || !found || got.ID != "99" {
		t.Fatalf("retry: %+v %v %v", got, found, err)
	}
	// A previous writer appended a newer request without updating the index.
	if err := inner.Update(t.Context(), func(tx spi.WriteTx) error {
		c, err := readPromptCounters(tx, "scope")
		if err != nil {
			return err
		}
		c.Counts[promptRequest]++
		r, err := makePromptRecord("scope", promptRequest, cache.ProviderRequestRecord{ID: "old-writer", PromptScopeID: "scope", ThreadID: "thread", ModelContextEntryID: "context"})
		if err != nil {
			return err
		}
		r.Ordinal = c.Counts[promptRequest]
		if err := putPromptValue(tx, promptRecordKey(r), "prompt_record", r); err != nil {
			return err
		}
		return putPromptValue(tx, promptScopeKey("scope"), "prompt_scope", c)
	}); err != nil {
		t.Fatal(err)
	}
	if got, found, err := kernel.LatestModelContextRequest(t.Context(), "scope", "thread", "context"); err != nil || !found || got.ID != "old-writer" {
		t.Fatalf("gap: %+v %v %v", got, found, err)
	}
	if err := inner.Update(t.Context(), func(tx spi.WriteTx) error {
		raw, _ := json.Marshal(uint64(102))
		return tx.Put(promptRequestLocatorNamespace, contextRequestKey("scope", "thread", "context"), raw)
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := kernel.LatestModelContextRequest(t.Context(), "scope", "thread", "context"); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("corrupt locator accepted: %v", err)
	}
}

package storage

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/provider/cache"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/internal/storagebridge"
	publicstorage "github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
)

type requestReadBackend struct {
	spi.Backend
	gets, scans int
}
type requestReadTx struct {
	spi.ReadTx
	owner *requestReadBackend
}

func (b *requestReadBackend) View(ctx context.Context, read func(spi.ReadTx) error) error {
	return b.Backend.View(ctx, func(tx spi.ReadTx) error { return read(requestReadTx{tx, b}) })
}
func (tx requestReadTx) Get(namespace string, key []byte) ([]byte, error) {
	tx.owner.gets++
	return tx.ReadTx.Get(namespace, key)
}
func (tx requestReadTx) Scan(request spi.ScanRequest) (spi.ScanPage, error) {
	tx.owner.scans++
	return tx.ReadTx.Scan(request)
}

func TestLatestRequestPreservesPendingOrderRestartAndBoundedReads(t *testing.T) {
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
	if _, found, err := kernel.LatestProviderRequest(t.Context(), "missing"); err != nil || found {
		t.Fatalf("empty: %v %v", found, err)
	}
	for i := range 1096 {
		request := cache.ProviderRequestRecord{ID: fmt.Sprint(i), PromptScopeID: "thread", ThreadID: "thread", Attempt: i + 1}
		if err := kernel.AppendProviderRequest(t.Context(), request); err != nil {
			t.Fatal(err)
		}
	}
	assertLatest := func(wantID string, wantGets int) {
		t.Helper()
		backend.gets, backend.scans = 0, 0
		got, found, err := kernel.LatestProviderRequest(t.Context(), "thread")
		if err != nil || !found || got.ID != wantID || backend.gets != wantGets || backend.scans != 0 {
			t.Fatalf("latest=%+v found=%v err=%v reads=%d scans=%d", got, found, err, backend.gets, backend.scans)
		}
		all, err := kernel.ProviderRequests(t.Context(), "thread")
		if err != nil || !reflect.DeepEqual(got, all[len(all)-1]) {
			t.Fatalf("full adapter parity: %v", err)
		}
	}
	assertLatest("1095", 2)
	if err := kernel.Checkpoint(t.Context()); err != nil {
		t.Fatal(err)
	}
	assertLatest("1095", 2)
	kernel, err = NewBackendKernel(t.Context(), backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	assertLatest("1095", 2)
	if err := kernel.AppendProviderResponse(t.Context(), cache.ProviderResponseRecord{PromptScopeID: "thread"}); err != nil {
		t.Fatal(err)
	}
	assertLatest("1095", 2)
	if err := kernel.AppendProviderRequest(t.Context(), cache.ProviderRequestRecord{ID: "last", PromptScopeID: "thread"}); err != nil {
		t.Fatal(err)
	}
	assertLatest("last", 2)
	if err := kernel.appendPrompt(t.Context(), "thread", promptRequest, cache.ProviderRequestRecord{ID: "pending", PromptScopeID: "thread"}); err != nil {
		t.Fatal(err)
	}
	assertLatest("pending", 0)
	if err := kernel.Checkpoint(t.Context()); err != nil {
		t.Fatal(err)
	}
	assertLatest("pending", 2)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := kernel.LatestProviderRequest(ctx, "thread"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
	if err := kernel.DeletePromptScopes(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	if _, found, err := kernel.LatestProviderRequest(t.Context(), "thread"); err != nil || found {
		t.Fatalf("delete: %v %v", found, err)
	}
}

func TestLatestRequestRejectsMissingOrMismatchedAuthority(t *testing.T) {
	for _, mode := range []string{"missing", "scope", "ordinal", "value"} {
		t.Run(mode, func(t *testing.T) {
			backend, err := storagebridge.Open(t.Context(), storagebridge.Source(publicstorage.Memory()))
			if err != nil {
				t.Fatal(err)
			}
			defer backend.Close()
			kernel, err := NewBackendKernel(t.Context(), backend, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			if err := kernel.CheckpointProviderRequest(t.Context(), nil, nil, cache.ProviderRequestRecord{ID: "request", PromptScopeID: "thread"}); err != nil {
				t.Fatal(err)
			}
			err = backend.Update(t.Context(), func(tx spi.WriteTx) error {
				record := promptRecord{ScopeID: "thread", Category: promptRequest, Ordinal: 1, Value: []byte("{\"prompt_scope_id\":\"thread\"}")}
				key := promptRecordKey(record)
				switch mode {
				case "missing":
					return tx.Delete(promptRecordsNamespace, key)
				case "scope":
					record.ScopeID = "other"
				case "ordinal":
					record.Ordinal = 2
				case "value":
					record.Value = []byte("{\"prompt_scope_id\":\"other\"}")
				}
				return putPromptValue(tx, key, "prompt_record", record)
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := kernel.LatestProviderRequest(t.Context(), "thread"); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
				t.Fatalf("corrupt authority accepted: %v", err)
			}
		})
	}
}

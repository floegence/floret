package storage_test

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/floegence/floret/v2/storage"
)

func TestMemoryBackendContract(t *testing.T) {
	runBackendContract(t, storage.Memory())
}

func runBackendContract(t *testing.T, source storage.Source) {
	t.Helper()
	ctx := context.Background()
	backend, err := source.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()

	value := []byte("one")
	if err := backend.Update(ctx, func(tx storage.WriteTx) error {
		if err := tx.Put("turns", []byte("a"), value); err != nil {
			return err
		}
		if err := tx.Put("turns", []byte("b"), []byte("two")); err != nil {
			return err
		}
		return tx.Put("turns", []byte("c"), []byte("three"))
	}); err != nil {
		t.Fatal(err)
	}
	value[0] = 'X'

	if err := backend.View(ctx, func(tx storage.ReadTx) error {
		got, err := tx.Get("turns", []byte("a"))
		if err != nil {
			return err
		}
		if string(got) != "one" {
			t.Fatalf("stored value = %q", got)
		}
		got[0] = 'X'
		again, err := tx.Get("turns", []byte("a"))
		if err != nil {
			return err
		}
		if string(again) != "one" {
			t.Fatalf("returned bytes alias backend state: %q", again)
		}
		page, err := tx.Scan(storage.ScanRequest{Namespace: "turns", Start: []byte("a"), End: []byte("z"), Limit: 2})
		if err != nil {
			return err
		}
		if len(page.Records) != 2 || !bytes.Equal(page.Records[0].Key, []byte("a")) || !bytes.Equal(page.Records[1].Key, []byte("b")) || !page.HasMore || !bytes.Equal(page.Next, []byte("b")) {
			t.Fatalf("first page = %#v", page)
		}
		page, err = tx.Scan(storage.ScanRequest{Namespace: "turns", Start: []byte("a"), End: []byte("z"), After: page.Next, Limit: 2})
		if err != nil {
			return err
		}
		if len(page.Records) != 1 || string(page.Records[0].Key) != "c" || page.HasMore || page.Next != nil {
			t.Fatalf("second page = %#v", page)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	rollback := errors.New("rollback")
	calls := 0
	if err := backend.Update(ctx, func(tx storage.WriteTx) error {
		calls++
		if err := tx.Put("turns", []byte("rollback"), []byte("no")); err != nil {
			return err
		}
		return rollback
	}); !errors.Is(err, rollback) {
		t.Fatalf("rollback error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("update callback calls = %d", calls)
	}
	if err := backend.View(ctx, func(tx storage.ReadTx) error {
		_, err := tx.Get("turns", []byte("rollback"))
		if !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("rolled-back value error = %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
	if err := backend.View(ctx, func(storage.ReadTx) error { return nil }); !errors.Is(err, storage.ErrClosed) {
		t.Fatalf("view after close = %v", err)
	}
}

func TestMemoryBackendRollsBackPanicAndCancellation(t *testing.T) {
	backend, err := storage.Memory().Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()

	func() {
		defer func() {
			if recovered := recover(); recovered != "stop" {
				t.Fatalf("recovered panic = %#v", recovered)
			}
		}()
		_ = backend.Update(context.Background(), func(tx storage.WriteTx) error {
			if err := tx.Put("turns", []byte("panic"), []byte("no")); err != nil {
				t.Fatal(err)
			}
			panic("stop")
		})
	}()

	cancelled, cancel := context.WithCancel(context.Background())
	if err := backend.Update(cancelled, func(tx storage.WriteTx) error {
		if err := tx.Put("turns", []byte("cancelled"), []byte("no")); err != nil {
			return err
		}
		cancel()
		return nil
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled update = %v", err)
	}

	for _, key := range []string{"panic", "cancelled"} {
		if err := backend.View(context.Background(), func(tx storage.ReadTx) error {
			_, err := tx.Get("turns", []byte(key))
			if !errors.Is(err, storage.ErrNotFound) {
				t.Fatalf("value %q survived rollback: %v", key, err)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestMemoryBackendTransactionsExpireAfterCallback(t *testing.T) {
	backend, err := storage.Memory().Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()

	var retained storage.WriteTx
	if err := backend.Update(context.Background(), func(tx storage.WriteTx) error {
		retained = tx
		return tx.Put("turns", []byte("a"), []byte("one"))
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := retained.Get("turns", []byte("a")); !errors.Is(err, storage.ErrTransactionClosed) {
		t.Fatalf("retained get = %v", err)
	}
	if err := retained.Put("turns", []byte("b"), []byte("two")); !errors.Is(err, storage.ErrTransactionClosed) {
		t.Fatalf("retained put = %v", err)
	}
}

func TestMemoryBackendDetectsConcurrentWriteConflictWithoutRetry(t *testing.T) {
	backend, err := storage.Memory().Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()

	started := make(chan struct{}, 2)
	release := make(chan struct{})
	errorsByWriter := make(chan error, 2)
	calls := make([]int, 2)
	var callsMu sync.Mutex
	for writer := range 2 {
		go func() {
			errorsByWriter <- backend.Update(context.Background(), func(tx storage.WriteTx) error {
				callsMu.Lock()
				calls[writer]++
				callsMu.Unlock()
				started <- struct{}{}
				<-release
				return tx.Put("turns", []byte{byte('a' + writer)}, []byte("value"))
			})
		}()
	}
	<-started
	<-started
	close(release)
	first, second := <-errorsByWriter, <-errorsByWriter
	conflicts := 0
	for _, err := range []error{first, second} {
		if errors.Is(err, storage.ErrConflict) {
			conflicts++
		} else if err != nil {
			t.Fatalf("concurrent update = %v", err)
		}
	}
	if conflicts != 1 {
		t.Fatalf("conflicts = %d, errors = [%v, %v]", conflicts, first, second)
	}
	if calls[0] != 1 || calls[1] != 1 {
		t.Fatalf("callback calls = %v", calls)
	}
}

func TestMemoryBackendValidatesRequestsAndDeletes(t *testing.T) {
	backend, err := storage.Memory().Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()

	if err := backend.Update(context.Background(), func(tx storage.WriteTx) error {
		if err := tx.Put("", []byte("a"), []byte("one")); !errors.Is(err, storage.ErrInvalidArgument) {
			t.Fatalf("empty namespace put = %v", err)
		}
		if err := tx.Put("turns", nil, []byte("one")); !errors.Is(err, storage.ErrInvalidArgument) {
			t.Fatalf("empty key put = %v", err)
		}
		if _, err := tx.Scan(storage.ScanRequest{Namespace: "turns", Limit: 0}); !errors.Is(err, storage.ErrInvalidArgument) {
			t.Fatalf("zero limit scan = %v", err)
		}
		if err := tx.Put("turns", []byte("a"), []byte("one")); err != nil {
			return err
		}
		return tx.Delete("turns", []byte("a"))
	}); err != nil {
		t.Fatal(err)
	}
	if err := backend.View(context.Background(), func(tx storage.ReadTx) error {
		_, err := tx.Get("turns", []byte("a"))
		if !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("deleted get = %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

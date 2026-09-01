package florettest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

// RunBackendContract verifies the transactional, ownership, ordering, rollback,
// cancellation, and lifecycle semantics required from a storage Backend.
func RunBackendContract(t *testing.T, source spi.Source) {
	t.Helper()
	if source == nil {
		t.Fatal("florettest: backend source is required")
	}

	t.Run("bytes and pagination", func(t *testing.T) {
		backend := openContractBackend(t, source)
		defer backend.Close()
		key, value := []byte("a"), []byte("one")
		if err := backend.Update(context.Background(), func(tx spi.WriteTx) error {
			for _, record := range []spi.Record{
				{Key: key, Value: value},
				{Key: []byte("b"), Value: []byte("two")},
				{Key: []byte("c"), Value: []byte("three")},
			} {
				if err := tx.Put("contract", record.Key, record.Value); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		key[0], value[0] = 'x', 'x'
		if err := backend.View(context.Background(), func(tx spi.ReadTx) error {
			got, err := tx.Get("contract", []byte("a"))
			if err != nil || string(got) != "one" {
				return fmt.Errorf("get = %q, %w", got, err)
			}
			got[0] = 'x'
			again, err := tx.Get("contract", []byte("a"))
			if err != nil || string(again) != "one" {
				return fmt.Errorf("aliased get = %q, %w", again, err)
			}
			first, err := tx.Scan(spi.ScanRequest{Namespace: "contract", Start: []byte("a"), End: []byte("z"), Limit: 2})
			if err != nil {
				return err
			}
			if len(first.Records) != 2 || string(first.Records[0].Key) != "a" || string(first.Records[1].Key) != "b" || !first.HasMore || string(first.Next) != "b" {
				return fmt.Errorf("first scan page = %#v", first)
			}
			first.Records[0].Value[0] = 'x'
			second, err := tx.Scan(spi.ScanRequest{Namespace: "contract", After: first.Next, Limit: 2})
			if err != nil {
				return err
			}
			if len(second.Records) != 1 || string(second.Records[0].Key) != "c" || second.HasMore || second.Next != nil {
				return fmt.Errorf("second scan page = %#v", second)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("snapshot isolation", func(t *testing.T) {
		backend := openContractBackend(t, source)
		defer backend.Close()
		if err := backend.Update(context.Background(), func(tx spi.WriteTx) error {
			return tx.Put("contract", []byte("key"), []byte("before"))
		}); err != nil {
			t.Fatal(err)
		}
		readStarted := make(chan struct{})
		writeDone := make(chan error, 1)
		readDone := make(chan error, 1)
		go func() {
			readDone <- backend.View(context.Background(), func(tx spi.ReadTx) error {
				before, err := tx.Get("contract", []byte("key"))
				if err != nil || string(before) != "before" {
					return fmt.Errorf("initial snapshot read = %q, %w", before, err)
				}
				close(readStarted)
				if err := <-writeDone; err != nil {
					return err
				}
				after, err := tx.Get("contract", []byte("key"))
				if err != nil || string(after) != "before" {
					return fmt.Errorf("snapshot changed = %q, %w", after, err)
				}
				return nil
			})
		}()
		<-readStarted
		go func() {
			writeDone <- backend.Update(context.Background(), func(tx spi.WriteTx) error {
				return tx.Put("contract", []byte("key"), []byte("after"))
			})
		}()
		select {
		case err := <-readDone:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("snapshot read and concurrent update did not complete")
		}
	})

	t.Run("rollback", func(t *testing.T) {
		backend := openContractBackend(t, source)
		defer backend.Close()
		rollback := errors.New("rollback")
		calls := 0
		if err := backend.Update(context.Background(), func(tx spi.WriteTx) error {
			calls++
			if err := tx.Put("contract", []byte("error"), []byte("no")); err != nil {
				return err
			}
			return rollback
		}); !errors.Is(err, rollback) {
			t.Fatalf("callback error = %v", err)
		}
		if calls != 1 {
			t.Fatalf("callback calls = %d", calls)
		}
		func() {
			defer func() {
				if recovered := recover(); recovered != "contract panic" {
					t.Fatalf("recovered panic = %#v", recovered)
				}
			}()
			_ = backend.Update(context.Background(), func(tx spi.WriteTx) error {
				if err := tx.Put("contract", []byte("panic"), []byte("no")); err != nil {
					return err
				}
				panic("contract panic")
			})
		}()
		for _, key := range [][]byte{[]byte("error"), []byte("panic")} {
			if err := backend.View(context.Background(), func(tx spi.ReadTx) error {
				_, err := tx.Get("contract", key)
				if !errors.Is(err, spi.ErrNotFound) {
					return fmt.Errorf("rolled-back key %q: %w", key, err)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		}
	})

	t.Run("cancellation and transaction lifetime", func(t *testing.T) {
		backend := openContractBackend(t, source)
		defer backend.Close()
		ctx, cancel := context.WithCancel(context.Background())
		var retained spi.WriteTx
		if err := backend.Update(ctx, func(tx spi.WriteTx) error {
			retained = tx
			if err := tx.Put("contract", []byte("cancel"), []byte("no")); err != nil {
				return err
			}
			cancel()
			return nil
		}); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled update = %v", err)
		}
		if _, err := retained.Get("contract", []byte("cancel")); !errors.Is(err, spi.ErrTransactionClosed) {
			t.Fatalf("retained transaction = %v", err)
		}
		if err := backend.View(context.Background(), func(tx spi.ReadTx) error {
			_, err := tx.Get("contract", []byte("cancel"))
			if !errors.Is(err, spi.ErrNotFound) {
				return fmt.Errorf("cancelled value persisted: %w", err)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("concurrent writes", func(t *testing.T) {
		backend := openContractBackend(t, source)
		defer backend.Close()
		var calls atomic.Int32
		results := make(chan error, 2)
		for writer := range 2 {
			go func() {
				results <- backend.Update(context.Background(), func(tx spi.WriteTx) error {
					calls.Add(1)
					return tx.Put("contract", []byte{byte('a' + writer)}, []byte("value"))
				})
			}()
		}
		successes := 0
		for range 2 {
			err := <-results
			if err == nil {
				successes++
			} else if !errors.Is(err, spi.ErrConflict) {
				t.Fatalf("concurrent update error = %v", err)
			}
		}
		if calls.Load() != 2 || successes == 0 {
			t.Fatalf("callback calls = %d, successful commits = %d", calls.Load(), successes)
		}
	})

	t.Run("close", func(t *testing.T) {
		backend := openContractBackend(t, source)
		if err := backend.Close(); err != nil {
			t.Fatal(err)
		}
		if err := backend.Close(); err != nil {
			t.Fatal(err)
		}
		if err := backend.View(context.Background(), func(spi.ReadTx) error { return nil }); !errors.Is(err, spi.ErrClosed) {
			t.Fatalf("view after close = %v", err)
		}
	})
}

func openContractBackend(t *testing.T, source spi.Source) spi.Backend {
	t.Helper()
	backend, err := source.Open(context.Background())
	if err != nil {
		t.Fatalf("florettest: open backend: %v", err)
	}
	if backend == nil {
		t.Fatal("florettest: source returned a nil backend")
	}
	return backend
}

func equalRecord(left, right spi.Record) bool {
	return bytes.Equal(left.Key, right.Key) && bytes.Equal(left.Value, right.Value)
}

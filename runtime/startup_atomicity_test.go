package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/internal/storagecodec"
	publicstorage "github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestOpenRollsBackDomainWhenLogicalSchemaWriteFails(t *testing.T) {
	ctx := context.Background()
	underlying, err := storagebridge.Open(ctx, storagebridge.Source(publicstorage.Memory()))
	if err != nil {
		t.Fatal(err)
	}
	previous, err := json.Marshal(logicalSchemaEnvelope{Version: previousLogicalSchemaVersion, Fingerprint: previousLogicalSchemaFingerprint})
	if err != nil {
		t.Fatal(err)
	}
	if err := underlying.Update(ctx, func(tx spi.WriteTx) error {
		return tx.Put(logicalSchemaNamespace, []byte(logicalSchemaKey), previous)
	}); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("logical schema write failed")
	source := publicstorage.NewSource(startupBackendSource{backend: startupFailingBackend{Backend: underlying, err: injected}})
	if _, err := Open(ctx, Options{Storage: source}); !errors.Is(err, injected) {
		t.Fatalf("Open error=%v, want injected failure", err)
	}
	if err := underlying.View(ctx, func(tx spi.ReadTx) error {
		if got, err := tx.Get(logicalSchemaNamespace, []byte(logicalSchemaKey)); err != nil || !bytes.Equal(got, previous) {
			t.Fatalf("logical schema changed after rollback: value=%s err=%v", got, err)
		}
		for _, record := range []struct {
			namespace string
			key       []byte
		}{
			{namespace: "floret.domain", key: storagecodec.Tuple(storagecodec.TupleString("sessiontree"), storagecodec.TupleString("state"))},
			{namespace: "floret.domain", key: storagecodec.Tuple(storagecodec.TupleString("sessiontree"), storagecodec.TupleString("root_thread_inventory"))},
			{namespace: "floret.domain", key: storagecodec.Tuple(storagecodec.TupleString("prompt"), storagecodec.TupleString("state"))},
		} {
			if _, err := tx.Get(record.namespace, record.key); !errors.Is(err, spi.ErrNotFound) {
				t.Fatalf("partial startup record %x survived rollback: %v", record.key, err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestOpenRollsBackDomainWhenStartupPanics(t *testing.T) {
	ctx := context.Background()
	underlying, err := storagebridge.Open(ctx, storagebridge.Source(publicstorage.Memory()))
	if err != nil {
		t.Fatal(err)
	}
	defer underlying.Close()
	previous, err := json.Marshal(logicalSchemaEnvelope{Version: previousLogicalSchemaVersion, Fingerprint: previousLogicalSchemaFingerprint})
	if err != nil {
		t.Fatal(err)
	}
	if err := underlying.Update(ctx, func(tx spi.WriteTx) error {
		return tx.Put(logicalSchemaNamespace, []byte(logicalSchemaKey), previous)
	}); err != nil {
		t.Fatal(err)
	}
	source := publicstorage.NewSource(startupBackendSource{backend: startupPanickingBackend{Backend: underlying}})
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("Open did not propagate startup panic")
			}
		}()
		_, _ = Open(ctx, Options{Storage: source})
	}()
	if err := underlying.View(ctx, func(tx spi.ReadTx) error {
		if got, err := tx.Get(logicalSchemaNamespace, []byte(logicalSchemaKey)); err != nil || !bytes.Equal(got, previous) {
			t.Fatalf("logical schema changed after panic rollback: value=%s err=%v", got, err)
		}
		for _, record := range []struct {
			namespace string
			key       []byte
		}{
			{namespace: "floret.domain", key: storagecodec.Tuple(storagecodec.TupleString("sessiontree"), storagecodec.TupleString("state"))},
			{namespace: "floret.domain", key: storagecodec.Tuple(storagecodec.TupleString("sessiontree"), storagecodec.TupleString("root_thread_inventory"))},
			{namespace: "floret.domain", key: storagecodec.Tuple(storagecodec.TupleString("prompt"), storagecodec.TupleString("state"))},
		} {
			if _, err := tx.Get(record.namespace, record.key); !errors.Is(err, spi.ErrNotFound) {
				t.Fatalf("partial startup record survived panic rollback: %v", err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestOpenMapsSessionTreeAuthorityCorruption(t *testing.T) {
	path := t.TempDir() + "/floret.sqlite"
	host, err := Open(t.Context(), Options{Storage: publicstorage.SQLite(path)})
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	backend, err := storagebridge.Open(t.Context(), storagebridge.Source(publicstorage.SQLite(path)))
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		return tx.Delete("floret.domain.sessiontree.v16", storagecodec.Tuple(storagecodec.TupleString("root_index")))
	}); err != nil {
		t.Fatal(err)
	}
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(t.Context(), Options{Storage: publicstorage.SQLite(path)}); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("Open error=%v, want ErrAuthorityCorrupt", err)
	}
}

type startupBackendSource struct{ backend spi.Backend }

func (source startupBackendSource) Open(context.Context) (spi.Backend, error) {
	return source.backend, nil
}

type startupFailingBackend struct {
	spi.Backend
	err error
}

type startupPanickingBackend struct{ spi.Backend }

func (backend startupPanickingBackend) Update(ctx context.Context, mutate func(spi.WriteTx) error) error {
	return backend.Backend.Update(ctx, func(tx spi.WriteTx) error {
		if err := mutate(tx); err != nil {
			return err
		}
		panic("injected startup panic")
	})
}

func (startupPanickingBackend) Close() error { return nil }

func (backend startupFailingBackend) Update(ctx context.Context, mutate func(spi.WriteTx) error) error {
	return backend.Backend.Update(ctx, func(tx spi.WriteTx) error {
		return mutate(startupFailingWriteTx{WriteTx: tx, err: backend.err})
	})
}

func (startupFailingBackend) Close() error { return nil }

type startupFailingWriteTx struct {
	spi.WriteTx
	err error
}

func (tx startupFailingWriteTx) Put(namespace string, key, value []byte) error {
	if namespace == logicalSchemaNamespace && bytes.Equal(key, []byte(logicalSchemaKey)) {
		return tx.err
	}
	return tx.WriteTx.Put(namespace, key, value)
}

func TestLogicalV9UpgradeRetiresTermsInSameTransaction(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint("write_failure=", fail), func(t *testing.T) {
			backend, err := storagebridge.Open(t.Context(), storagebridge.Source(publicstorage.Memory()))
			if err != nil {
				t.Fatal(err)
			}
			defer backend.Close()
			old, _ := json.Marshal(logicalSchemaEnvelope{Version: legacyV9LogicalSchemaVersion, Fingerprint: legacyV9LogicalSchemaFingerprint})
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				if err := tx.Put(logicalSchemaNamespace, []byte(logicalSchemaKey), old); err != nil {
					return err
				}
				return tx.Put("floret.query.documents.v1", []byte("old-document"), []byte("old-terms"))
			}); err != nil {
				t.Fatal(err)
			}
			var source spi.Backend = backend
			injected := errors.New("logical version commit failed")
			if fail {
				source = startupFailingBackend{Backend: backend, err: injected}
			}
			host, err := Open(t.Context(), Options{Storage: publicstorage.NewSource(startupBackendSource{backend: source}), DeferExecution: true})
			if fail {
				if !errors.Is(err, injected) {
					t.Fatalf("failure=%v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				defer host.Shutdown(t.Context())
			}
			if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
				state, err := inspectLogicalSchemaTransaction(tx)
				if err != nil {
					return err
				}
				_, termsErr := tx.Get("floret.query.documents.v1", []byte("old-document"))
				if fail {
					if state != logicalSchemaV9 || termsErr != nil {
						t.Fatalf("partial upgrade: %s %v", state, termsErr)
					}
				} else if state != logicalSchemaCurrent || !errors.Is(termsErr, spi.ErrNotFound) {
					t.Fatalf("incomplete upgrade: %s %v", state, termsErr)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestLogicalV9RejectsFingerprintDrift(t *testing.T) {
	backend, err := storagebridge.Open(t.Context(), storagebridge.Source(publicstorage.Memory()))
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	raw, _ := json.Marshal(logicalSchemaEnvelope{Version: legacyV9LogicalSchemaVersion, Fingerprint: "unknown"})
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return tx.Put(logicalSchemaNamespace, []byte(logicalSchemaKey), raw) }); err != nil {
		t.Fatal(err)
	}
	if _, err := inspectLogicalSchema(t.Context(), backend); !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatalf("drift accepted: %v", err)
	}
}

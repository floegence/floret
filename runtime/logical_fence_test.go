package runtime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/internal/storagecodec"
	publicstorage "github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
)

const releasedV11Fingerprint = "sha256:8c7b5edf140b2950900551ebf83cc70475a970109feba1c9148a6384691cf75a"

func logicalFenceFixture(t *testing.T, currentDomain bool) string {
	t.Helper()
	file := filepath.Join(t.TempDir(), "history.sqlite")
	if !currentDomain {
		raw, err := os.ReadFile("../internal/sessiontree/testdata/tool-names-v7.107.9/input.sqlite")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(file, raw, 0600); err != nil {
			t.Fatal(err)
		}
		return file
	}
	host, err := Open(t.Context(), Options{Storage: publicstorage.SQLite(file), DeferExecution: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := host.startup.wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: "retained", CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.store.repo.Append(t.Context(), sessiontree.Entry{ID: "retained-entry", ThreadID: "retained", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: "preserve canonical fact"}}, sessiontree.AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := host.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", file)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	old, _ := json.Marshal(logicalSchemaEnvelope{Version: "11", Fingerprint: releasedV11Fingerprint})
	if _, err := db.Exec("UPDATE floret_backend_records SET value=? WHERE namespace=? AND key=?", old, logicalSchemaNamespace, []byte(logicalSchemaKey)); err != nil {
		t.Fatal(err)
	}
	return file
}

func logicalFenceDigest(t *testing.T, file string, includeLogical bool) [32]byte {
	t.Helper()
	db, err := sql.Open("sqlite", file)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query("SELECT namespace,key,value FROM floret_backend_records ORDER BY namespace,key")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	hash := sha256.New()
	for rows.Next() {
		var namespace string
		var key, value []byte
		if err := rows.Scan(&namespace, &key, &value); err != nil {
			t.Fatal(err)
		}
		if !includeLogical && namespace == logicalSchemaNamespace && bytes.Equal(key, []byte(logicalSchemaKey)) {
			continue
		}
		fmt.Fprintf(hash, "%d:%s%d:", len(namespace), namespace, len(key))
		hash.Write(key)
		fmt.Fprintf(hash, "%d:", len(value))
		hash.Write(value)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	var digest [32]byte
	copy(digest[:], hash.Sum(nil))
	return digest
}

func TestLogicalV11FencePreservesCurrentRecordsAndBoundedRestart(t *testing.T) {
	file := logicalFenceFixture(t, true)
	before := logicalFenceDigest(t, file, false)
	for round := range 2 {
		counts := &startupScanCounts{}
		host, err := Open(t.Context(), Options{Storage: publicstorage.NewSource(startupCountingSource{source: publicstorage.SQLite(file), counts: counts}), DeferExecution: true})
		if err != nil {
			t.Fatal(err)
		}
		if err := host.startup.wait(t.Context()); err != nil {
			t.Fatal(err)
		}
		var envelope logicalSchemaEnvelope
		if err := host.backend.View(t.Context(), func(tx spi.ReadTx) error {
			raw, err := tx.Get(logicalSchemaNamespace, []byte(logicalSchemaKey))
			if err != nil {
				return err
			}
			return json.Unmarshal(raw, &envelope)
		}); err != nil {
			t.Fatal(err)
		}
		if envelope.Version != logicalSchemaVersion || envelope.Fingerprint == releasedV11Fingerprint {
			t.Fatalf("logical fence missing: %+v", envelope)
		}
		if counts.valueBytesRead() > 32<<10 || counts.recordsRead() > 80 {
			t.Fatalf("unbounded round %d startup: records=%d bytes=%d", round, counts.recordsRead(), counts.valueBytesRead())
		}
		if err := host.Shutdown(t.Context()); err != nil {
			t.Fatal(err)
		}
		if got := logicalFenceDigest(t, file, false); got != before {
			t.Fatalf("canonical records changed on logical-only upgrade/restart %d", round)
		}
	}
}

func TestLogicalV11ReleasedDomainMigratesAndRestarts(t *testing.T) {
	file := logicalFenceFixture(t, false)
	for round := range 2 {
		host, err := Open(t.Context(), Options{Storage: publicstorage.SQLite(file), DeferExecution: true})
		if err != nil {
			t.Fatal(err)
		}
		if err := host.startup.wait(t.Context()); err != nil {
			t.Fatal(err)
		}
		state, err := inspectLogicalSchema(t.Context(), host.backend)
		if err != nil || state != logicalSchemaCurrent {
			t.Fatalf("round %d schema: %s %v", round, state, err)
		}
		if err := host.Shutdown(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestLogicalV11DomainUpgradeAndFenceRollBackTogether(t *testing.T) {
	for _, failure := range []string{"write", "cancel", "panic", "fingerprint", "future-domain"} {
		t.Run(failure, func(t *testing.T) {
			file := logicalFenceFixture(t, false)
			backend, err := storagebridge.Open(t.Context(), storagebridge.Source(publicstorage.SQLite(file)))
			if err != nil {
				t.Fatal(err)
			}
			defer backend.Close()
			if failure == "fingerprint" || failure == "future-domain" {
				err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
					if failure == "fingerprint" {
						raw, _ := json.Marshal(logicalSchemaEnvelope{Version: "11", Fingerprint: "drift"})
						return tx.Put(logicalSchemaNamespace, []byte(logicalSchemaKey), raw)
					}
					raw, err := storagecodec.EncodeEnvelope("sessiontree-v999-record", []byte(`{"version":999,"kind":"manifest","value":{"version":999,"sequence":0}}`))
					if err != nil {
						return err
					}
					return tx.Put("floret.domain.sessiontree.v17", storagecodec.Tuple(storagecodec.TupleString("manifest")), raw)
				})
				if err != nil {
					t.Fatal(err)
				}
			}
			before := logicalFenceDigest(t, file, true)
			injected := errors.New("logical fence publication rejected")
			var owned spi.Backend = startupFailingBackend{Backend: backend, err: injected}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			switch failure {
			case "panic":
				owned = startupPanickingBackend{Backend: backend}
			case "cancel":
				owned = logicalFenceCancelBackend{Backend: backend, cancel: cancel}
			case "fingerprint", "future-domain":
				owned = logicalFenceNoCloseBackend{Backend: backend}
			}
			var host *Host
			panicked := false
			func() {
				defer func() {
					if recover() != nil {
						panicked = true
					}
				}()
				host, err = Open(ctx, Options{Storage: publicstorage.NewSource(startupBackendSource{backend: owned}), DeferExecution: true})
			}()
			if host != nil {
				defer host.Shutdown(context.Background())
				t.Fatal("failed upgrade returned a host")
			}
			switch failure {
			case "panic":
				if !panicked {
					t.Fatal("panic not propagated")
				}
			case "write":
				if !errors.Is(err, injected) {
					t.Fatalf("failure=%v", err)
				}
			case "cancel":
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("failure=%v", err)
				}
			case "fingerprint":
				if !errors.Is(err, ErrUnsupportedSchema) {
					t.Fatalf("failure=%v", err)
				}
			case "future-domain":
				if err == nil {
					t.Fatal("future domain accepted")
				}
			}
			if got := logicalFenceDigest(t, file, true); got != before {
				t.Fatal("failed upgrade changed source records")
			}
		})
	}
}

type logicalFenceNoCloseBackend struct{ spi.Backend }

func (logicalFenceNoCloseBackend) Close() error { return nil }

type logicalFenceCancelBackend struct {
	spi.Backend
	cancel context.CancelFunc
}

func (logicalFenceCancelBackend) Close() error { return nil }
func (b logicalFenceCancelBackend) Update(ctx context.Context, mutate func(spi.WriteTx) error) error {
	return b.Backend.Update(ctx, func(tx spi.WriteTx) error {
		if err := mutate(tx); err != nil {
			return err
		}
		b.cancel()
		return ctx.Err()
	})
}

func TestLogicalFenceRejectsFutureVersionsWithoutReclassifyingReservedMigration(t *testing.T) {
	for _, version := range []string{"14", "16", "999"} {
		t.Run(version, func(t *testing.T) {
			backend, err := storagebridge.Open(t.Context(), storagebridge.Source(publicstorage.Memory()))
			if err != nil {
				t.Fatal(err)
			}
			defer backend.Close()
			raw, _ := json.Marshal(logicalSchemaEnvelope{Version: version, Fingerprint: "unrecognized"})
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return tx.Put(logicalSchemaNamespace, []byte(logicalSchemaKey), raw) }); err != nil {
				t.Fatal(err)
			}
			_, err = inspectLogicalSchema(t.Context(), backend)
			if version == "16" {
				if !errors.Is(err, ErrMigrationRequired) || errors.Is(err, ErrStoreTooNew) {
					t.Fatalf("reserved migration classification: %v", err)
				}
			} else if !errors.Is(err, ErrStoreTooNew) || !errors.Is(err, ErrUnsupportedSchema) {
				t.Fatalf("future classification: %v", err)
			}
			if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
				got, err := tx.Get(logicalSchemaNamespace, []byte(logicalSchemaKey))
				if err != nil {
					return err
				}
				if !bytes.Equal(got, raw) {
					t.Fatal("inspection mutated marker")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestLogicalV12MigrationPreservesRecords(t *testing.T) {
	file := logicalFenceFixture(t, true)
	db, err := sql.Open("sqlite", file)
	if err != nil {
		t.Fatal(err)
	}
	old, _ := json.Marshal(logicalSchemaEnvelope{Version: legacyV12LogicalSchemaVersion, Fingerprint: legacyV12LogicalSchemaFingerprint})
	if _, err = db.Exec("UPDATE floret_backend_records SET value=? WHERE namespace=? AND key=?", old, logicalSchemaNamespace, []byte(logicalSchemaKey)); err != nil {
		t.Fatal(err)
	}
	db.Close()
	before := logicalFenceDigest(t, file, false)
	host, err := Open(t.Context(), Options{Storage: publicstorage.SQLite(file), DeferExecution: true})
	if err != nil {
		t.Fatal(err)
	}
	if err = host.startup.wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err = host.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if logicalFenceDigest(t, file, false) != before {
		t.Fatal("v12 upgrade changed canonical records")
	}
}

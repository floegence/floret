//go:build floret_layout_cutover

package cutover

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
)

func sourceFixture(t *testing.T) string {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "old.sqlite")
	backend, err := storagebridge.Open(t.Context(), storagebridge.Source(storage.SQLite(path)))
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	err = backend.Update(t.Context(), func(tx spi.WriteTx) error {
		for _, key := range []string{"one", "two"} {
			if err := tx.Put("synthetic", []byte(key), []byte("unchanged")); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func TestOfflineAuditExcludesLiveOwner(t *testing.T) {
	const env = "FLORET_TEST_CUTOVER_PROBE"
	if path := os.Getenv(env); path != "" {
		err := WithSource(t.Context(), path, func(source *Source) error {
			t.Fatal("entered source callback while writer held the database")
			return nil
		})
		if !errors.Is(err, spi.ErrConflict) {
			t.Fatalf("unexpected fence result: %v", err)
		}
		return
	}
	path := sourceFixture(t)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(t.Context(), "BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestOfflineAuditExcludesLiveOwner$")
	cmd.Env = append(os.Environ(), env+"="+path)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("competing owner: %v: %s", err, out)
	}
	if _, err := conn.ExecContext(t.Context(), "ROLLBACK"); err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := WithSource(t.Context(), path, func(*Source) error { return nil }); err != nil {
		t.Fatal(err)
	}
}

func TestOfflineAuditDetectsSubstitutionOmissionAndExtraRows(t *testing.T) {
	path := sourceFixture(t)
	for _, change := range []string{"none", "changed", "missing", "extra", "wrong-key"} {
		t.Run(change, func(t *testing.T) {
			err := WithSource(t.Context(), path, func(source *Source) error {
				candidate := sourceFixture(t)
				db, err := sql.Open("sqlite", candidate)
				if err != nil {
					return err
				}
				defer db.Close()
				var mutation string
				switch change {
				case "changed":
					mutation = `UPDATE floret_backend_records SET value=x'787878787878787878' WHERE key=CAST('one' AS BLOB)`
				case "missing":
					mutation = `DELETE FROM floret_backend_records WHERE key=CAST('one' AS BLOB)`
				case "extra":
					mutation = `INSERT INTO floret_backend_records VALUES('synthetic',x'03',x'11')`
				case "wrong-key":
					mutation = `UPDATE floret_backend_records SET key=x'030201' WHERE key=CAST('one' AS BLOB)`
				}
				if mutation != "" {
					if _, err := db.ExecContext(t.Context(), mutation); err != nil {
						return err
					}
				}
				if err := db.Close(); err != nil {
					return err
				}
				return WithSource(t.Context(), candidate, func(target *Source) error {
					left, err := source.Records(t.Context())
					if err != nil {
						return err
					}
					right, err := target.Records(t.Context())
					if err != nil {
						return err
					}
					if (left == right) != (change == "none") {
						t.Fatalf("mutation %s: equal=%t", change, left == right)
					}
					if change == "none" && (left.Rows != 2 || left.ValueBytes != 18) {
						t.Fatalf("source inventory: %+v", left)
					}
					return nil
				})
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestOfflineAuditRejectsMissingFutureAndMalformedSources(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.sqlite")
	never := func(*Source) error { t.Fatal("admitted invalid source"); return nil }
	if err := WithSource(t.Context(), missing, never); !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if _, err := os.Stat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("created missing source")
	}
	for _, query := range []string{`UPDATE floret_backend_metadata SET value=CAST('6' AS BLOB)`, `CREATE TABLE foreign_rows(x)`, `CREATE VIEW unknown_shape AS SELECT 1`, `INSERT INTO floret_backend_metadata VALUES('incomplete',x'01')`} {
		path := sourceFixture(t)
		db, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(query); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		before, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := WithSource(t.Context(), path, never); err == nil {
			t.Fatal("unexpected success")
		}
		after, err := os.ReadFile(path)
		if err != nil || string(after) != string(before) {
			t.Fatalf("unsupported input was changed: %v", err)
		}
	}
	path := sourceFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := WithSource(ctx, path, never); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestOfflineAuditDigestAcrossRecordSizeBoundary(t *testing.T) {
	path := sourceFixture(t)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`DELETE FROM floret_backend_records`); err != nil {
		t.Fatal(err)
	}
	digest := sha256.New()
	digest.Write([]byte("floret.offline.record-inventory.v1\x00"))
	var expected Inventory
	for index, size := range []int{0, 128<<10 - 1, 128 << 10, 128<<10 + 1, 512<<10 + 9} {
		key, value := []byte{byte(index)}, bytes.Repeat([]byte{byte(index + 1)}, size)
		if _, err := db.Exec(`INSERT INTO floret_backend_records VALUES('fixture',?,?)`, key, value); err != nil {
			t.Fatal(err)
		}
		for _, field := range [][]byte{[]byte("fixture"), key, value} {
			writeLength(digest, uint64(len(field)))
			digest.Write(field)
		}
		expected.Rows++
		expected.ValueBytes += uint64(size)
	}
	copy(expected.Digest[:], digest.Sum(nil))
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := WithSource(t.Context(), path, func(source *Source) error {
		got, err := source.Records(t.Context())
		if err == nil && got != expected {
			t.Fatalf("inventory differs from independent contiguous input: %+v", got)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

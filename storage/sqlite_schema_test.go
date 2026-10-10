package storage

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/storage/spi"
)

func createPhysicalV1(t *testing.T, path string) {
	t.Helper()
	db, err := sql.Open(sqliteDriverName, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, statement := range []string{
		`CREATE TABLE floret_backend_metadata(name TEXT PRIMARY KEY,value BLOB NOT NULL) WITHOUT ROWID`,
		`CREATE TABLE floret_backend_records(namespace TEXT NOT NULL,key BLOB NOT NULL,value BLOB NOT NULL,PRIMARY KEY(namespace,key)) WITHOUT ROWID`,
		`INSERT INTO floret_backend_metadata VALUES('physical_schema',CAST('1' AS BLOB))`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`INSERT INTO floret_backend_records VALUES('opaque',X'01',?),('opaque',X'02',X'CAFE')`, bytes.Repeat([]byte("blob"), 1<<18)); err != nil {
		t.Fatal(err)
	}
}

func TestPhysicalMigrationPreservesOpaqueRecordsAndRollsBack(t *testing.T) {
	for _, failure := range []string{"none", "cancel", "write", "panic"} {
		t.Run(failure, func(t *testing.T) {
			path := t.TempDir() + "/legacy.sqlite"
			createPhysicalV1(t, path)
			opened, err := (sqliteSource{path: path}).Open(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer opened.Close()
			backend := opened.(*sqliteBackend)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			func() {
				defer func() {
					if failure == "panic" && recover() == nil {
						t.Fatal("missing panic")
					}
				}()
				err = backend.Update(ctx, func(tx spi.WriteTx) error {
					migrated, err := storagebridge.PrepareStartup(tx, nil)
					if err != nil {
						return err
					}
					if !migrated {
						t.Fatal("not migrated")
					}
					switch failure {
					case "cancel":
						cancel()
						return ctx.Err()
					case "write":
						return errors.New("injected write failure")
					case "panic":
						panic("injected")
					}
					return nil
				})
				if failure == "none" && err != nil {
					t.Fatal(err)
				}
				if failure != "none" && err == nil {
					t.Fatal("injected failure succeeded")
				}
			}()
			version, err := inspectSQLitePhysicalSchema(t.Context(), backend.db)
			if err != nil {
				t.Fatal(err)
			}
			want := "1"
			if failure == "none" {
				want = "2"
			}
			if version != want {
				t.Fatalf("version=%s want %s", version, want)
			}
			if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
				value, err := tx.Get("opaque", []byte{1})
				if err != nil || !bytes.Equal(value, bytes.Repeat([]byte("blob"), 1<<18)) {
					t.Fatal("large opaque value changed")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if failure != "none" {
				if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { _, err := storagebridge.PrepareStartup(tx, nil); return err }); err != nil {
					t.Fatal(err)
				}
			}
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				changed, err := storagebridge.PrepareStartup(tx, nil)
				if changed {
					t.Fatal("rewrote current physical layout")
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPhysicalSchemaRejectsFutureAndLayoutDrift(t *testing.T) {
	for _, mutation := range []string{
		`UPDATE floret_backend_metadata SET value=CAST('3' AS BLOB) WHERE name='physical_schema'`,
		`UPDATE floret_backend_metadata SET value=CAST('2' AS BLOB) WHERE name='physical_schema'`,
		`ALTER TABLE floret_backend_records ADD COLUMN extra BLOB`,
		`CREATE INDEX unsupported ON floret_backend_records(namespace)`,
	} {
		t.Run(mutation, func(t *testing.T) {
			path := t.TempDir() + "/drift.sqlite"
			createPhysicalV1(t, path)
			db, err := sql.Open(sqliteDriverName, path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(mutation); err != nil {
				t.Fatal(err)
			}
			db.Close()
			if backend, err := (sqliteSource{path: path}).Open(t.Context()); err == nil {
				backend.Close()
				t.Fatal("accepted drift")
			}
		})
	}
}

func TestPhysicalMigrationDiskFullRollsBackAndCanRetry(t *testing.T) {
	path := t.TempDir() + "/full.sqlite"
	createPhysicalV1(t, path)
	opened, err := (sqliteSource{path: path}).Open(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	backend := opened.(*sqliteBackend)
	backend.db.SetMaxOpenConns(1)
	var pages int
	if err := backend.db.QueryRow(`PRAGMA page_count`).Scan(&pages); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.db.Exec(fmt.Sprintf(`PRAGMA max_page_count=%d`, pages)); err != nil {
		t.Fatal(err)
	}
	err = backend.Update(t.Context(), func(tx spi.WriteTx) error { _, err := storagebridge.PrepareStartup(tx, nil); return err })
	var coded interface{ Code() int }
	if !errors.As(err, &coded) || coded.Code()&255 != 13 {
		t.Fatalf("disk-full error=%v", err)
	}
	version, err := inspectSQLitePhysicalSchema(t.Context(), backend.db)
	if err != nil || version != "1" {
		t.Fatalf("rolled back schema=%q err=%v", version, err)
	}
	if _, err := backend.db.Exec(`PRAGMA max_page_count=2147483646`); err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { _, err := storagebridge.PrepareStartup(tx, nil); return err }); err != nil {
		t.Fatal(err)
	}
}

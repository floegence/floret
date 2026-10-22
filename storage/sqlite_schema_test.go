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
				want = "5"
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
		`UPDATE floret_backend_metadata SET value=CAST('6' AS BLOB) WHERE name='physical_schema'`,
		`UPDATE floret_backend_metadata SET value=CAST('3' AS BLOB) WHERE name='physical_schema'`,
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

func createPhysicalV3Index(t *testing.T, path string) {
	t.Helper()
	createPhysicalV1(t, path)
	db, err := sql.Open(sqliteDriverName, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, statement := range []string{
		"CREATE TABLE records_v2(namespace TEXT NOT NULL,key BLOB NOT NULL,value BLOB NOT NULL,PRIMARY KEY(namespace,key))",
		"INSERT INTO records_v2 SELECT * FROM floret_backend_records",
		"DROP TABLE floret_backend_records",
		"ALTER TABLE records_v2 RENAME TO floret_backend_records",
		sqliteIndexDocuments, sqliteIndexThreads, sqliteIndexTermsV3,
		"INSERT INTO floret_index_documents(document_key,thread_id,object,metadata) VALUES('document','thread','messages',X'0102')",
		"INSERT INTO floret_index_terms(rowid,tokens) SELECT rowid,'x61 x62' FROM floret_index_documents",
		"UPDATE floret_backend_metadata SET value=CAST('3' AS BLOB) WHERE name='physical_schema'",
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPhysicalSearchRemovalIsAtomicAndPreservesCanonicalBytes(t *testing.T) {
	for _, source := range []string{"3", "4"} {
		t.Run("v"+source, func(t *testing.T) {
			for _, failure := range []string{"none", "cancel", "write", "panic"} {
				t.Run(failure, func(t *testing.T) {
					path := t.TempDir() + "/v3.sqlite"
					createPhysicalV3Index(t, path)
					if source == "4" {
						db, err := sql.Open(sqliteDriverName, path)
						if err != nil {
							t.Fatal(err)
						}
						for _, statement := range []string{"DROP TABLE floret_index_terms", sqliteIndexPostings, sqliteIndexPostingDocuments, "INSERT INTO floret_index_postings SELECT 'x61',rowid FROM floret_index_documents", "UPDATE floret_backend_metadata SET value=CAST('4' AS BLOB) WHERE name='physical_schema'"} {
							if _, err := db.Exec(statement); err != nil {
								t.Fatal(err)
							}
						}
						if err := db.Close(); err != nil {
							t.Fatal(err)
						}
					}
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
							changed, err := storagebridge.PrepareStartup(tx, nil)
							if err != nil {
								return err
							}
							if !changed {
								t.Fatal("migration skipped")
							}
							if err := tx.Put("opaque", []byte("after-upgrade"), []byte("committed")); err != nil {
								return err
							}
							switch failure {
							case "cancel":
								cancel()
								return ctx.Err()
							case "write":
								return errors.New("write failed")
							case "panic":
								panic("injected")
							}
							return nil
						})
						if failure == "none" && err != nil || failure != "none" && err == nil {
							t.Fatalf("failure=%s err=%v", failure, err)
						}
					}()
					wantVersion := source
					if failure == "none" {
						wantVersion = "5"
					}
					version, err := inspectSQLitePhysicalSchema(t.Context(), backend.db)
					if err != nil || version != wantVersion {
						t.Fatalf("version=%s err=%v", version, err)
					}
					if failure == "none" {
						var count int
						if err := backend.db.QueryRow("SELECT count(*) FROM sqlite_master WHERE name LIKE 'floret_index_%'").Scan(&count); err != nil || count != 0 {
							t.Fatalf("search tables remain: %d, %v", count, err)
						}
					} else {
						var key string
						if err := backend.db.QueryRow("SELECT document_key FROM floret_index_documents").Scan(&key); err != nil || key != "document" {
							t.Fatalf("rollback lost old index: %s, %v", key, err)
						}
					}
					if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
						raw, err := tx.Get("opaque", []byte{1})
						if err != nil {
							return err
						}
						if !bytes.Equal(raw, bytes.Repeat([]byte("blob"), 1<<18)) {
							t.Fatal("canonical bytes changed")
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
							t.Fatal("current schema rewritten")
						}
						return err
					}); err != nil {
						t.Fatal(err)
					}
				})
			}
		})
	}
}

func TestPhysicalV2AcceptsWithoutRowIDRecords(t *testing.T) {
	path := t.TempDir() + "/v2-without-rowid.sqlite"
	createPhysicalV1(t, path)
	db, err := sql.Open(sqliteDriverName, path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE floret_backend_metadata SET value=CAST('2' AS BLOB) WHERE name='physical_schema'`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	backend, err := (sqliteSource{path: path}).Open(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		_, err := storagebridge.PrepareStartup(tx, nil)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if version, err := inspectSQLitePhysicalSchema(t.Context(), backend.(*sqliteBackend).db); err != nil || version != "5" {
		t.Fatalf("version=%q err=%v", version, err)
	}
}

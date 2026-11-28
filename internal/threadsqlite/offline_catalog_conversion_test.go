//go:build floret_layout_cutover

package threadsqlite

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestPrepareMemberCatalogPreservesDirectory(t *testing.T) {
	ctx := t.Context()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "input.sqlite")
	catalog, err := CreateCatalog(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	files, err := NewFiles(path+".d", catalog.Store, 2, 0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	source := &Backend{ThreadViews: ThreadViews{Catalog: catalog, Files: files}}
	defer source.Close()
	ref := Reference{File: FileID{4}, Thread: "active"}
	if err := files.Create(ctx, ref); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Publish(ctx, files, "publish", []Replacement{{Thread: ref.Thread, Next: ref.File}}); err != nil {
		t.Fatal(err)
	}
	if err := source.ImportDeletedThread(ctx, "deleted", func(spi.WriteTx) error { return nil }); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct{ name, install string }{{"staging", `INSERT INTO floret_thread_staging VALUES(zeroblob(16),'staged')`}, {"unknown", `INSERT INTO floret_backend_records VALUES('new.fact',x'01',x'01')`}} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := catalog.owner.ExecContext(ctx, test.install); err != nil {
				t.Fatal(err)
			}
			dest := filepath.Join(root, test.name)
			if _, err := PrepareMemberCatalog(ctx, source, dest); !errors.Is(err, ErrFileIdentity) && !errors.Is(err, ErrFileFormat) {
				t.Fatal(err)
			}
			if _, err := catalog.owner.ExecContext(ctx, `DELETE FROM floret_thread_staging; DELETE FROM floret_backend_records`); err != nil {
				t.Fatal(err)
			}
		})
	}
	dest := filepath.Join(root, "candidate.sqlite")
	candidate, err := PrepareMemberCatalog(ctx, source, dest)
	if err != nil {
		t.Fatal(err)
	}
	if err := candidate.Close(); err != nil {
		t.Fatal(err)
	}
	converted, err := OpenMemberCatalog(ctx, dest)
	if err != nil {
		t.Fatal(err)
	}
	defer converted.Close()
	if got, err := converted.Resolve(ctx, ref.Thread); err != nil || got != ref || converted.Store != catalog.Store {
		t.Fatal("changed route or store", got, err)
	}
	if _, err := converted.Resolve(ctx, "deleted"); !errors.Is(err, storagebridge.ErrRouteDeleted) {
		t.Fatal("lost tombstone", err)
	}
	for _, owner := range []*Catalog{catalog, converted} {
		var count int
		if err := owner.owner.QueryRowContext(ctx, `SELECT count(*) FROM floret_thread_decisions WHERE operation='publish'`).Scan(&count); err != nil || count != 1 {
			t.Fatal("lost publication", err)
		}
	}
	if err := files.Update(ctx, ref, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO floret_backend_records VALUES('future.member',x'01',x'01')`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := ViewRecordFile(ctx, files.path(ref.File), catalog.Store, ref, func(spi.ReadTx) error { t.Fatal("admitted unknown member namespace"); return nil }); !errors.Is(err, ErrFileFormat) {
		t.Fatal(err)
	}
	if err := files.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

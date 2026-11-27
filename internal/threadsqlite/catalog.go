package threadsqlite

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sqliterecords"
	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/storage/spi"
)

type Catalog struct {
	db        *sql.DB
	owner     *sql.Conn
	fence     *Catalog
	path      string
	gate      chan struct{}
	closeOnce sync.Once
	closeErr  error
	Store     FileID
}

type Replacement struct {
	Thread   identity.ThreadID
	Expected FileID
	Next     FileID
}

const metadataSchema = `CREATE TABLE floret_backend_metadata (name TEXT PRIMARY KEY, value BLOB NOT NULL) WITHOUT ROWID`
const routesSchema = `CREATE TABLE floret_thread_routes (thread TEXT NOT NULL PRIMARY KEY,file BLOB UNIQUE CHECK(file IS NULL OR length(file)=16)) WITHOUT ROWID`
const decisionsSchema = `CREATE TABLE floret_thread_decisions (operation TEXT NOT NULL PRIMARY KEY,fingerprint BLOB NOT NULL CHECK(length(fingerprint)=32)) WITHOUT ROWID`
const stagingSchema = `CREATE TABLE floret_thread_staging (file BLOB PRIMARY KEY CHECK(length(file)=16), thread TEXT NOT NULL UNIQUE) WITHOUT ROWID`

func openCatalogFile(ctx context.Context, path string, create, exclusive bool) (*Catalog, error) {
	if !filepath.IsAbs(path) {
		return nil, spi.ErrInvalidArgument
	}
	parent := filepath.Dir(path)
	resolved, err := filepath.EvalSymlinks(parent)
	if err != nil {
		return nil, err
	}
	if parent != resolved {
		return nil, ErrFileIdentity
	}
	if err := checkSidecars(path, !create); err != nil {
		return nil, err
	}
	if create {
		file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return nil, err
		}
		if err = file.Close(); err != nil {
			return nil, err
		}
	} else if err := checkFile(path, false); err != nil {
		return nil, err
	}
	locking := "normal"
	if exclusive {
		locking = "exclusive"
	}
	db, err := existingDB(path, "locking_mode("+locking+")")
	if err != nil {
		return nil, err
	}
	owner, err := db.Conn(ctx)
	if err != nil {
		return nil, errors.Join(sqliterecords.ClassifyError(ctx, err), db.Close())
	}
	catalog := &Catalog{db: db, owner: owner, path: path, gate: make(chan struct{}, 1)}
	var mode string
	if err := owner.QueryRowContext(ctx, `PRAGMA locking_mode`).Scan(&mode); err != nil || mode != locking {
		return nil, errors.Join(ErrFileFormat, err, catalog.Close())
	}
	return catalog, nil
}

func CreateCatalog(ctx context.Context, path string) (*Catalog, error) {
	if ctx == nil {
		return nil, spi.ErrInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	fence, err := openCatalogFile(ctx, path, true, true)
	if err != nil {
		return nil, err
	}
	if _, err = rand.Read(fence.Store[:]); err != nil {
		return nil, errors.Join(err, fence.Close())
	}
	if err := os.Mkdir(path+".d", 0700); err != nil {
		return nil, errors.Join(err, fence.Close())
	}
	catalog, err := openCatalogFile(ctx, fence.catalogPath(), true, false)
	if err != nil {
		return nil, errors.Join(err, fence.Close())
	}
	catalog.fence, catalog.Store = fence, fence.Store
	err = initializeCatalog(ctx, catalog.owner, catalog.Store, false)
	if err == nil {
		err = syncDirectory(filepath.Dir(catalog.path))
	}
	if err == nil {
		err = initializeCatalog(ctx, fence.owner, fence.Store, true)
	}
	if err == nil {
		err = syncDirectory(filepath.Dir(path))
	}
	if err != nil {
		return nil, errors.Join(err, catalog.Close())
	}
	return catalog, nil
}

func (fence *Catalog) catalogPath() string {
	return filepath.Join(fence.path+".d", "floret-catalog-"+hex.EncodeToString(fence.Store[:])+".sqlite")
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func initializeCatalog(ctx context.Context, db *sql.Conn, store FileID, fence bool) error {
	expected := "delete"
	schemas := []string{metadataSchema, recordsSchema, routesSchema, decisionsSchema, stagingSchema}
	if fence {
		expected, schemas = "wal", []string{metadataSchema}
	}
	var mode string
	if err := db.QueryRowContext(ctx, `PRAGMA journal_mode=`+expected).Scan(&mode); err != nil || mode != expected {
		return errors.Join(ErrFileFormat, err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, schema := range schemas {
		if _, err = tx.ExecContext(ctx, schema); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO floret_backend_metadata VALUES('physical_schema',CAST('6' AS BLOB)),('store',?)`, store[:]); err != nil {
		return err
	}
	return tx.Commit()
}

func OpenCatalog(ctx context.Context, path string) (*Catalog, error) {
	if ctx == nil {
		return nil, spi.ErrInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	fence, err := openCatalogFile(ctx, path, false, true)
	if err != nil {
		return nil, err
	}
	if err := fence.verify(ctx, "wal", []string{metadataSchema}); err != nil {
		return nil, errors.Join(sqliterecords.ClassifyError(ctx, err), fence.Close())
	}
	catalog, err := openCatalogFile(ctx, fence.catalogPath(), false, false)
	if err != nil {
		return nil, errors.Join(err, fence.Close())
	}
	catalog.fence = fence
	if err := catalog.verify(ctx, "delete", []string{metadataSchema, recordsSchema, decisionsSchema, routesSchema, stagingSchema}); err != nil || catalog.Store != fence.Store {
		return nil, errors.Join(ErrFileIdentity, err, catalog.Close())
	}
	return catalog, nil
}

func (catalog *Catalog) verify(ctx context.Context, expectedMode string, schemas []string) error {
	var mode string
	if err := catalog.owner.QueryRowContext(ctx, `PRAGMA journal_mode`).Scan(&mode); err != nil {
		return err
	} else if mode != expectedMode {
		return ErrFileFormat
	}
	tx, err := catalog.owner.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := checkSchema(ctx, tx, schemas); err != nil {
		return err
	}
	var version string
	var raw []byte
	if err := tx.QueryRowContext(ctx, `SELECT (SELECT value FROM floret_backend_metadata WHERE name='physical_schema'),(SELECT value FROM floret_backend_metadata WHERE name='store')`).Scan(&version, &raw); err != nil {
		return err
	}
	if version != "6" || len(raw) != len(catalog.Store) {
		return ErrFileFormat
	}
	copy(catalog.Store[:], raw)
	if catalog.Store == (FileID{}) {
		return ErrFileIdentity
	}
	return nil
}

func (catalog *Catalog) acquire(ctx context.Context) error {
	select {
	case catalog.gate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (catalog *Catalog) release() { <-catalog.gate }

func (catalog *Catalog) Close() error {
	catalog.closeOnce.Do(func() {
		catalog.gate <- struct{}{}
		defer catalog.release()
		catalog.closeErr = errors.Join(catalog.owner.Close(), catalog.db.Close())
		if catalog.fence != nil {
			catalog.closeErr = errors.Join(catalog.closeErr, catalog.fence.Close())
		}
	})
	return catalog.closeErr
}

func (catalog *Catalog) Resolve(ctx context.Context, thread identity.ThreadID) (Reference, error) {
	if ctx == nil || thread == "" {
		return Reference{}, spi.ErrInvalidArgument
	}
	if err := catalog.acquire(ctx); err != nil {
		return Reference{}, err
	}
	defer catalog.release()
	var raw []byte
	err := catalog.owner.QueryRowContext(ctx, `SELECT file FROM floret_thread_routes WHERE thread=?`, string(thread)).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return Reference{}, errors.Join(spi.ErrNotFound, storagebridge.ErrRouteMissing)
	}
	if err == nil && raw == nil {
		return Reference{}, errors.Join(spi.ErrNotFound, storagebridge.ErrRouteDeleted)
	}
	if err != nil {
		return Reference{}, err
	}
	var file FileID
	if len(raw) != len(file) {
		return Reference{}, ErrFileIdentity
	}
	copy(file[:], raw)
	if file == (FileID{}) {
		return Reference{}, ErrFileIdentity
	}
	return Reference{file, thread}, nil
}

func (catalog *Catalog) Publish(ctx context.Context, files *Files, operation string, changes []Replacement) error {
	return catalog.publish(ctx, files, operation, changes, nil, nil)
}

func (catalog *Catalog) publish(ctx context.Context, files *Files, operation string, changes []Replacement, source *Reference, preparation []byte) error {
	if ctx == nil || files == nil || files.store != catalog.Store || operation == "" || len(changes) == 0 {
		return spi.ErrInvalidArgument
	}
	ordered := slices.Clone(changes)
	slices.SortFunc(ordered, func(left, right Replacement) int { return strings.Compare(string(left.Thread), string(right.Thread)) })
	for index, change := range ordered {
		if change.Thread == "" || change.Expected == change.Next || index > 0 && ordered[index-1].Thread == change.Thread {
			return spi.ErrInvalidArgument
		}
	}
	encoded, err := json.Marshal(struct {
		Changes     []Replacement
		Source      *Reference
		Preparation []byte
	}{ordered, source, preparation})
	if err != nil {
		return err
	}
	fingerprint := sha256.Sum256(encoded)
	known, err := catalog.decided(ctx, operation, fingerprint[:])
	if known || err != nil {
		return err
	}
	for _, change := range ordered {
		if change.Next == (FileID{}) {
			continue
		}
		ref := Reference{change.Next, change.Thread}
		if err := files.View(ctx, ref, func(tx *sql.Tx) error {
			if preparation == nil {
				return nil
			}
			var saved []byte
			if err := tx.QueryRowContext(ctx, `SELECT fingerprint FROM floret_thread_preparation WHERE singleton=1`).Scan(&saved); err != nil {
				return err
			}
			if !bytes.Equal(saved, preparation) {
				return spi.ErrConflict
			}
			return nil
		}); err != nil {
			return err
		}
	}
	if err := catalog.acquire(ctx); err != nil {
		return err
	}
	defer catalog.release()
	tx, err := catalog.owner.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `INSERT INTO floret_thread_decisions VALUES(?,?) ON CONFLICT(operation) DO NOTHING`, operation, fingerprint[:])
	if err != nil {
		return err
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if inserted == 0 {
		var saved []byte
		if err := tx.QueryRowContext(ctx, `SELECT fingerprint FROM floret_thread_decisions WHERE operation=?`, operation).Scan(&saved); err != nil {
			return err
		}
		if !bytes.Equal(saved, fingerprint[:]) {
			return spi.ErrConflict
		}
		return nil
	}
	if source != nil {
		var current bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM floret_thread_routes WHERE thread=? AND file=?)`, string(source.Thread), source.File[:]).Scan(&current); err != nil {
			return err
		}
		if !current {
			return spi.ErrConflict
		}
	}
	for _, change := range ordered {
		var updated sql.Result
		if change.Expected == (FileID{}) {
			updated, err = tx.ExecContext(ctx, `INSERT INTO floret_thread_routes VALUES(?,?) ON CONFLICT(thread) DO NOTHING`, string(change.Thread), change.Next[:])
		} else {
			var next any
			if change.Next != (FileID{}) {
				next = change.Next[:]
			}
			updated, err = tx.ExecContext(ctx, `UPDATE floret_thread_routes SET file=? WHERE thread=? AND file=?`, next, string(change.Thread), change.Expected[:])
		}
		if err != nil {
			return err
		}
		count, err := updated.RowsAffected()
		if err != nil {
			return err
		}
		if count != 1 {
			return spi.ErrConflict
		}
	}
	return tx.Commit()
}

func (catalog *Catalog) decided(ctx context.Context, operation string, fingerprint []byte) (bool, error) {
	if err := catalog.acquire(ctx); err != nil {
		return false, err
	}
	defer catalog.release()
	var saved []byte
	err := catalog.owner.QueryRowContext(ctx, `SELECT fingerprint FROM floret_thread_decisions WHERE operation=?`, operation).Scan(&saved)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !bytes.Equal(saved, fingerprint) {
		return false, spi.ErrConflict
	}
	return true, nil
}

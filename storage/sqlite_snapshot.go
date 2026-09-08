package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"

	"github.com/floegence/floret/v7/storage/spi"
	"modernc.org/sqlite"
)

// SQLite inspection errors distinguish unsupported formats from damaged data.
var (
	ErrUnsupportedSQLiteFormat = errors.New("unsupported Floret SQLite format")
	ErrSQLiteTooNew            = errors.New("Floret SQLite format is newer than supported")
	ErrSQLiteIntegrity         = errors.New("Floret SQLite integrity check failed")
)

// BackupSQLite writes a new, complete SQLite snapshot, including committed WAL
// records. The caller must stop all writers before calling and retain that
// exclusion when coordinating a snapshot with other stores. Existing targets
// are never overwritten. No runtime Host may own the source.
func BackupSQLite(ctx context.Context, path, destination string) (err error) {
	if ctx == nil {
		return fmt.Errorf("%w: backup context is required", spi.ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	backend, err := openSQLiteReadOnly(ctx, path)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, backend.Close()) }()
	destination, err = filepath.Abs(destination)
	if err != nil {
		return err
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, e := os.Lstat(destination + suffix); e == nil {
			return os.ErrExist
		} else if !errors.Is(e, os.ErrNotExist) {
			return e
		}
	}
	file, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	if err = file.Close(); err != nil {
		_ = os.Remove(destination)
		return err
	}
	defer func() {
		if err != nil {
			for _, suffix := range []string{"", "-wal", "-shm", "-journal"} {
				_ = os.Remove(destination + suffix)
			}
		}
	}()
	conn, err := backend.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	err = conn.Raw(func(raw any) error {
		copier, ok := raw.(interface {
			NewBackup(string) (*sqlite.Backup, error)
		})
		if !ok {
			return errors.New("SQLite driver has no backup support")
		}
		backup, e := copier.NewBackup((&url.URL{Scheme: "file", Path: destination}).String())
		if e != nil {
			return e
		}
		for {
			if e = ctx.Err(); e != nil {
				return errors.Join(e, backup.Finish())
			}
			more, stepErr := backup.Step(128)
			if stepErr != nil {
				return errors.Join(stepErr, backup.Finish())
			}
			if !more {
				return backup.Finish()
			}
		}
	})
	if err != nil {
		return err
	}
	file, err = os.OpenFile(destination, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	return errors.Join(file.Sync(), file.Close())
}

// OpenReadOnly is used only by Floret's internal storage bridge. The returned
// connection cannot initialize, migrate, checkpoint, or change journal mode.
func (source sqliteSource) OpenReadOnly(ctx context.Context) (spi.Backend, error) {
	return openSQLiteReadOnly(ctx, source.path)
}

func openSQLiteReadOnly(ctx context.Context, path string) (*sqliteBackend, error) {
	if ctx == nil || path == "" || path == ":memory:" {
		return nil, fmt.Errorf("%w: SQLite inspection requires a context and file", spi.ErrInvalidArgument)
	}
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if resolved, e := filepath.EvalSymlinks(path); e == nil {
		path = resolved
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: SQLite source is not a regular file", spi.ErrInvalidArgument)
	}
	sqliteOwnership.Lock()
	if sqliteOwnership.open[path] > 0 {
		sqliteOwnership.Unlock()
		return nil, spi.ErrConflict
	}
	sqliteOwnership.open[path]++
	sqliteOwnership.maintenance[path] = true
	sqliteOwnership.Unlock()
	u := &url.URL{Scheme: "file", Path: path, RawQuery: "mode=ro"}
	db, err := sql.Open(sqliteDriverName, u.String())
	backend := &sqliteBackend{db: db, ownedPath: path, maintenance: true}
	if err != nil {
		sqliteOwnership.Lock()
		delete(sqliteOwnership.open, path)
		delete(sqliteOwnership.maintenance, path)
		sqliteOwnership.Unlock()
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if err = validateSQLiteMaintenanceDatabase(ctx, db); err != nil {
		_ = backend.Close()
		return nil, err
	}
	return backend, nil
}

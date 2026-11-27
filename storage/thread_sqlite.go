package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/floegence/floret/v7/internal/threadsqlite"
	"github.com/floegence/floret/v7/storage/spi"
)

type threadSQLiteSource struct{ path string }

// ThreadSQLite opens a local environment with one SQLite history per thread.
// The absolute path names its fixed version fence. Its sibling .d directory
// belongs to the same store. Neither part is a standalone environment backup.
func ThreadSQLite(path string) Source { return NewSource(threadSQLiteSource{path}) }

func (source threadSQLiteSource) Open(ctx context.Context) (spi.Backend, error) {
	path := source.path
	if ctx == nil || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, spi.ErrInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	location := threadsqlite.Source{CatalogPath: path, Directory: path + ".d", Limit: 16, MaxIdle: 4, IdleTTL: time.Minute}
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		if _, err := os.Lstat(location.Directory); !errors.Is(err, os.ErrNotExist) {
			return nil, errors.Join(threadsqlite.ErrFileIdentity, err)
		}
		catalog, err := threadsqlite.CreateCatalog(ctx, path)
		if err != nil {
			return nil, err
		}
		if err := catalog.Close(); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	return location.Open(ctx)
}

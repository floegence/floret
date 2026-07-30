package runtime

import (
	"context"

	"github.com/floegence/floret/v3/internal/storagebridge"
	publicstorage "github.com/floegence/floret/v3/storage"
)

func openSQLiteStoreForTest(path string) (*runtimeStore, error) {
	ctx := context.Background()
	backend, err := storagebridge.Open(ctx, storagebridge.Source(publicstorage.SQLite(path)))
	if err != nil {
		return nil, err
	}
	store, err := newBackendRuntimeStore(ctx, backend)
	if err != nil {
		_ = backend.Close()
		return nil, err
	}
	store.close = backend.Close
	return store, nil
}

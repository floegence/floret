package runtime

import (
	"context"

	publicstorage "github.com/floegence/floret/v2/storage"
)

func openSQLiteStoreForTest(path string) (*runtimeStore, error) {
	ctx := context.Background()
	backend, err := publicstorage.SQLite(path).Open(ctx)
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

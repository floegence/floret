//go:build floret_layout_cutover

package runtime_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/threadsqlite"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
)

type memberHostFixture string

func (path memberHostFixture) Open(ctx context.Context) (spi.Backend, error) {
	open := threadsqlite.OpenMemberCatalog
	if _, err := os.Stat(string(path)); errors.Is(err, os.ErrNotExist) {
		open = threadsqlite.CreateMemberCatalog
	} else if err != nil {
		return nil, err
	}
	catalog, err := open(ctx, string(path))
	if err != nil {
		return nil, err
	}
	files, err := threadsqlite.NewMemberFiles(string(path)+".d", catalog.Store, 16, 4, time.Minute)
	if err != nil {
		return nil, errors.Join(err, catalog.Close())
	}
	return &threadsqlite.Backend{ThreadViews: threadsqlite.ThreadViews{Catalog: catalog, Files: files}}, nil
}

func TestMemberHostForkOutput(t *testing.T) {
	qualifyOutputFork(t, func(path string) storage.Source { return storage.NewSource(memberHostFixture(path)) })
}

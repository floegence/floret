package storage

import (
	"context"

	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/storage/spi"
)

func StageIndependentFork(ctx context.Context, source spi.ReadTx, target spi.WriteTx, opts sessiontree.ForkOptions) error {
	if err := sessiontree.InitializeFreshCurrentRecords(ctx, target); err != nil {
		return err
	}
	if err := preparePromptRecords(ctx, target, true, nil); err != nil {
		return err
	}
	_, err := sessiontree.StageIndependentFork(ctx, source, target, opts)
	return err
}

package sessiontree

import (
	"context"
	"github.com/floegence/floret/v7/storage/spi"
)

// QueueMetadataReader selects queue source records across the complete journal,
// including inactive branches. The runtime applies the existing queue reducer.
type QueueMetadataReader interface {
	QueueMetadata(context.Context, string) ([]Entry, error)
}

func (repo *BackendRepo) QueueMetadata(ctx context.Context, thread string) ([]Entry, error) {
	if err := repo.prepareSparseSources(ctx, thread); err != nil {
		return nil, err
	}
	var entries []Entry
	err := repo.canonicalThreadView(ctx, thread, func(tx spi.ReadTx, _ ThreadMeta) error {
		return visitSparseSources(ctx, tx, thread, "queue", func(entry Entry) error {
			metadata, err := canonicalPathMetadata(entry)
			if err == nil {
				entries = append(entries, metadata.Entry)
			}
			return err
		})
	}, func(memory *MemoryRepo) error {
		var err error
		entries, err = memory.QueueMetadata(ctx, thread)
		return err
	})
	return entries, err
}
func (repo *MemoryRepo) QueueMetadata(ctx context.Context, thread string) ([]Entry, error) {
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, found := repo.threads[thread]; !found {
		return nil, ErrThreadNotFound
	}
	var entries []Entry
	for _, entry := range repo.entries[thread] {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if sparseSourceCategory(entry) != "queue" {
			continue
		}
		metadata, err := canonicalPathMetadata(entry)
		if err != nil {
			return nil, err
		}
		entries = append(entries, metadata.Entry)
	}
	return entries, nil
}

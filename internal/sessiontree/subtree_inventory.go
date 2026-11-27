package sessiontree

import (
	"context"
	"errors"

	"github.com/floegence/floret/v7/storage/spi"
)

// SubtreeThreadIDs captures membership, including archived descendants, in
// one metadata snapshot. Siblings retain ListThreads creation order.
func SubtreeThreadIDs(ctx context.Context, repo JournalRepo, root string) ([]string, error) {
	if scoped, ok := repo.(interface {
		SubtreeThreadIDs(context.Context, string) ([]string, error)
	}); ok {
		return scoped.SubtreeThreadIDs(ctx, root)
	}
	metas, err := ListThreads(ctx, repo, ListThreadsOptions{IncludeArchived: true})
	if err != nil {
		return nil, err
	}
	return listedSubtree(ctx, root, metas)
}

func listedSubtree(ctx context.Context, root string, metas []ThreadMeta) ([]string, error) {
	children := make(map[string][]ThreadMeta)
	found := false
	for _, meta := range metas {
		found = found || meta.ID == root
		if meta.ParentThreadID != "" {
			children[meta.ParentThreadID] = append(children[meta.ParentThreadID], meta)
		}
	}
	if !found {
		return nil, ErrThreadNotFound
	}
	return walkSubtree(ctx, root, func(parent string) ([]ThreadMeta, error) { return children[parent], nil })
}

func walkSubtree(ctx context.Context, root string, children func(string) ([]ThreadMeta, error)) ([]string, error) {
	ids := []string{root}
	seen := map[string]bool{root: true}
	for i := 0; i < len(ids); i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		rows, err := children(ids[i])
		if err != nil {
			return nil, err
		}
		SortThreadsByCreatedAtDesc(rows)
		for _, child := range rows {
			if seen[child.ID] || child.ParentThreadID != ids[i] {
				return nil, ErrAuthorityCorrupt
			}
			seen[child.ID] = true
			ids = append(ids, child.ID)
		}
	}
	return ids, nil
}

func (repo *BackendRepo) SubtreeThreadIDs(ctx context.Context, root string) (ids []string, err error) {
	err = repo.canonicalMetadataView(ctx, func(tx spi.ReadTx) error {
		owner := repo
		if delegate := repo.delegate.Load(); delegate != nil {
			owner = delegate
		}
		if owner.migrationSource != nil || owner.childMembershipPending {
			// Published and retained records can both contribute descendants.
			var rows []ThreadMeta
			if err := owner.visitThreadMetadata(ctx, tx, func(meta ThreadMeta) error { rows = append(rows, meta); return nil }); err != nil {
				return err
			}
			ids, err = listedSubtree(ctx, root, rows)
			return err
		}
		format := owner.domainFormat()
		meta, err := readPublishedThreadMeta(tx, format, root)
		if errors.Is(err, ErrThreadDeleted) {
			return ErrThreadNotFound
		}
		if err != nil {
			return err
		}
		if err := owner.validateListedParents(ctx, tx, format, meta); err != nil {
			return err
		}
		if meta.ParentThreadID == "" {
			if err := validateListedRoot(tx, format, meta.ID); err != nil {
				return err
			}
		}
		ids, err = walkSubtree(ctx, root, func(parent string) ([]ThreadMeta, error) {
			var rows []ThreadMeta
			err := owner.visitListedThreads(ctx, tx, parent, func(child ThreadMeta) error { rows = append(rows, child); return nil })
			return rows, err
		})
		return err
	}, func(memory *MemoryRepo) error {
		var readErr error
		ids, readErr = SubtreeThreadIDs(ctx, memory, root)
		return readErr
	})
	return ids, err
}

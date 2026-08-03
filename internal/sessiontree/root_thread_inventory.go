package sessiontree

import (
	"context"

	"github.com/floegence/floret/v3/storage/spi"
)

// RootThreadInventoryItem is one exact root-thread projection read from the
// same canonical domain snapshot as the surrounding page.
type RootThreadInventoryItem struct {
	Meta      ThreadMeta
	Path      []Entry
	Authority ThreadAuthoritySnapshot
	Revision  ThreadRevision
}

// ListRootThreadInventory reads a bounded root-thread page and the lifecycle
// facts needed to project each item inside one backend snapshot.
func (repo *BackendRepo) ListRootThreadInventory(ctx context.Context, opts ListThreadsOptions) ([]RootThreadInventoryItem, error) {
	var out []RootThreadInventoryItem
	err := repo.ViewDomain(ctx, func(memory *MemoryRepo, _ spi.ReadTx) error {
		var err error
		out, err = memory.ListRootThreadInventory(ctx, opts)
		return err
	})
	return out, err
}

func (r *MemoryRepo) ListRootThreadInventory(_ context.Context, opts ListThreadsOptions) ([]RootThreadInventoryItem, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	metas := make([]ThreadMeta, 0, len(r.threads))
	for _, meta := range r.threads {
		if err := ValidateThreadMetaAuthority(meta); err != nil {
			return nil, err
		}
		metas = append(metas, meta)
	}
	metas = ApplyThreadListOptions(metas, opts)
	out := make([]RootThreadInventoryItem, 0, len(metas))
	for _, meta := range metas {
		path, err := pathLocked(r.threads, r.entries, meta.ID, meta.LeafID)
		if err != nil {
			return nil, err
		}
		authority, err := r.inspectThreadAuthorityLocked(meta.ID)
		if err != nil {
			return nil, err
		}
		revision, found := r.threadRevisions[meta.ID]
		if !found {
			revision = 0
		}
		out = append(out, RootThreadInventoryItem{
			Meta: meta, Path: path, Authority: authority, Revision: revision,
		})
	}
	return out, nil
}

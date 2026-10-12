package sessiontree

import (
	"context"
	"sort"
)

// CanonicalMediaEntries returns every retained journal, including inactive
// paths and archived threads. It is used only by host resource maintenance.
type CanonicalMediaReader interface {
	CanonicalMediaEntries(context.Context) ([]Entry, error)
}

func (repo *BackendRepo) CanonicalMediaEntries(ctx context.Context) ([]Entry, error) {
	var result []Entry
	err := repo.view(ctx, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.CanonicalMediaEntries(ctx)
		return err
	})
	return result, err
}

func (repo *MemoryRepo) CanonicalMediaEntries(ctx context.Context) ([]Entry, error) {
	repo.mu.Lock()
	defer repo.mu.Unlock()
	threadIDs := make([]string, 0, len(repo.threads))
	for threadID := range repo.threads {
		threadIDs = append(threadIDs, threadID)
	}
	sort.Strings(threadIDs)
	var result []Entry
	for _, threadID := range threadIDs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		for _, entry := range repo.entries[threadID] {
			if len(entry.Message.Attachments) > 0 || entry.Message.ToolResult != nil && len(entry.Message.ToolResult.Attachments) > 0 || entry.Type == EntryQueueAdded || entry.Type == EntryTurnMarker {
				result = append(result, cloneEntry(entry))
			}
		}
	}
	return result, nil
}

func (repo *FileRepo) CanonicalMediaEntries(ctx context.Context) ([]Entry, error) {
	return repo.mem.CanonicalMediaEntries(ctx)
}

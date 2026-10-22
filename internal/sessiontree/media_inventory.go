package sessiontree

import (
	"context"
	"errors"
	"sort"
	"strings"
)

// CanonicalMediaEntries returns every retained journal, including inactive
// paths and archived threads. It is used only by host resource maintenance.
type CanonicalMediaReader interface {
	CanonicalMediaEntries(context.Context) ([]Entry, error)
}

// CanonicalThreadMediaReader reads retention facts for one thread without
// returning another thread's journal entries.
type CanonicalThreadMediaReader interface {
	CanonicalMediaEntriesForThread(context.Context, string) ([]Entry, error)
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

func (repo *BackendRepo) CanonicalMediaEntriesForThread(ctx context.Context, threadID string) ([]Entry, error) {
	var result []Entry
	err := repo.viewThread(ctx, threadID, func(memory *MemoryRepo) error {
		var err error
		result, err = memory.CanonicalMediaEntriesForThread(ctx, threadID)
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

func (repo *MemoryRepo) CanonicalMediaEntriesForThread(ctx context.Context, threadID string) ([]Entry, error) {
	if ctx == nil || strings.TrimSpace(threadID) == "" {
		return nil, errors.New("canonical thread media inventory requires a context and thread id")
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if _, found := repo.threads[threadID]; !found {
		return nil, ErrThreadNotFound
	}
	var result []Entry
	for _, entry := range repo.entries[threadID] {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if len(entry.Message.Attachments) > 0 || entry.Message.ToolResult != nil && len(entry.Message.ToolResult.Attachments) > 0 || entry.Type == EntryQueueAdded || entry.Type == EntryTurnMarker {
			result = append(result, cloneEntry(entry))
		}
	}
	return result, nil
}

func (repo *FileRepo) CanonicalMediaEntries(ctx context.Context) ([]Entry, error) {
	return repo.mem.CanonicalMediaEntries(ctx)
}

func (repo *FileRepo) CanonicalMediaEntriesForThread(ctx context.Context, threadID string) ([]Entry, error) {
	return repo.mem.CanonicalMediaEntriesForThread(ctx, threadID)
}

package sessiontree

import (
	"context"
	"errors"
	"testing"
)

func TestMemoryRepoEntryRejectsCorruptOrdinalIndex(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	first, err := repo.Append(ctx, Entry{ThreadID: "thread", Type: EntryCustom}, AppendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := repo.Append(ctx, Entry{ThreadID: "thread", Type: EntryCustom}, AppendOptions{})
	if err != nil {
		t.Fatal(err)
	}

	repo.mu.Lock()
	repo.entryOrdinals["thread"][first.ID] = repo.entryOrdinals["thread"][second.ID]
	repo.mu.Unlock()

	if _, err := repo.Entry(ctx, "thread", first.ID); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("Entry error = %v, want ErrAuthorityCorrupt", err)
	}
	if _, err := repo.Entry(ctx, "thread", "missing"); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("missing Entry error = %v, want ErrEntryNotFound", err)
	}
}

package sessiontree

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestJournalMetadataRetainsFrozenInventoryAcrossAppendAndBranchChange(t *testing.T) {
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	for i := range 600 {
		parent := ""
		if i > 0 {
			parent = fmt.Sprint((i - 1) / 2)
		}
		if _, err := repo.Append(t.Context(), Entry{ThreadID: "thread", ParentID: parent, Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "exact body 中文🌸"}}, AppendOptions{ID: fmt.Sprint(i)}); err != nil {
			t.Fatal(err)
		}
	}
	want, err := repo.Entries(t.Context(), "thread")
	if err != nil {
		t.Fatal(err)
	}
	path, err := repo.Path(t.Context(), "thread", "")
	if err != nil || len(path) >= len(want) {
		t.Fatalf("fixture must include inactive branches: path=%d inventory=%d err=%v", len(path), len(want), err)
	}
	options := JournalMetadataOptions{ThreadID: "thread", Limit: 17}
	var actual []PathMetadata
	for {
		page, err := repo.JournalMetadata(t.Context(), options)
		if err != nil {
			t.Fatal(err)
		}
		actual = append(actual, page.Entries...)
		if options.After == 0 {
			if _, err := repo.Append(t.Context(), Entry{ThreadID: "thread", Type: EntryCustom}, AppendOptions{ID: "late"}); err != nil {
				t.Fatal(err)
			}
			if err := repo.MoveLeaf(t.Context(), "thread", "1"); err != nil {
				t.Fatal(err)
			}
		}
		if !page.HasMore {
			break
		}
		if page.After <= options.After || page.Through != len(want) {
			t.Fatal("inventory boundary changed")
		}
		options.After, options.Through = page.After, page.Through
	}
	if len(actual) != len(want) {
		t.Fatal("inventory lost inactive records or included later append")
	}
	for index, entry := range want {
		metadata, err := canonicalPathMetadata(entry)
		if err != nil || !reflect.DeepEqual(actual[index], metadata) {
			t.Fatalf("record %d differs: %v", index, err)
		}
		if actual[index].Entry.Message.Content != "" || actual[index].Entry.Raw != "" {
			t.Fatal("metadata retained body")
		}
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := repo.JournalMetadata(cancelled, options); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	for _, invalid := range []JournalMetadataOptions{{ThreadID: "thread", Limit: 0}, {ThreadID: "thread", Limit: 257}, {ThreadID: "thread", After: -1, Limit: 1}} {
		if _, err := repo.JournalMetadata(t.Context(), invalid); !errors.Is(err, spi.ErrInvalidArgument) {
			t.Fatal(err)
		}
	}
	if _, err := repo.JournalMetadata(t.Context(), JournalMetadataOptions{ThreadID: "thread", After: 10, Through: 2, Limit: 1}); !errors.Is(err, ErrEntryNotFound) {
		t.Fatal(err)
	}
}

package sessiontree

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

func linearPageFixture(t *testing.T, branch bool) (*MemoryRepo, *migrationTestBackend, *BackendRepo) {
	t.Helper()
	memory := newMemoryRepo(func() time.Time { return time.Date(2026, 10, 23, 0, 0, 0, 0, time.UTC) })
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	for index := range 18 {
		options := AppendOptions{Now: time.Date(2026, 10, 23, 0, 0, index, 0, time.UTC)}
		if branch && index == 12 {
			options.ParentID = "entry-04"
		}
		if _, err := memory.Append(t.Context(), Entry{ID: fmt.Sprintf("entry-%02d", index), ThreadID: "thread", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "source"}}, options); err != nil {
			t.Fatal(err)
		}
	}
	backend, repo := openV31ReviewFixture(t, memory)
	if err := repo.PrepareThread(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	return memory, backend, repo
}

func TestCanonicalBoundedPagesMatchMemoryAcrossBranches(t *testing.T) {
	for _, branch := range []bool{false, true} {
		t.Run(fmt.Sprint(branch), func(t *testing.T) {
			memory, _, repo := linearPageFixture(t, branch)
			for _, limit := range []int{1, 5, 256} {
				before := ""
				for {
					expected, err := memory.PathMetadataPage(t.Context(), "thread", "", before, limit)
					if err != nil {
						t.Fatal(err)
					}
					actual, err := repo.PathMetadataPage(t.Context(), "thread", "", before, limit)
					if err != nil || !reflect.DeepEqual(actual, expected) {
						t.Fatalf("branch=%v limit=%d before=%q mismatch: %v\nactual=%#v\nexpected=%#v", branch, limit, before, err, actual, expected)
					}
					if !actual.HasMore {
						break
					}
					if actual.NextEntryID == before {
						t.Fatal("nonadvancing page")
					}
					before = actual.NextEntryID
				}
			}
		})
	}
}

func TestCanonicalLinearPageRejectsGapsAndBrokenLinks(t *testing.T) {
	for _, corruption := range []string{"missing", "parent", "depth"} {
		t.Run(corruption, func(t *testing.T) {
			_, backend, repo := linearPageFixture(t, false)
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				key := backendDomainEntryKey("thread", 15)
				if corruption == "missing" {
					return tx.Delete(backendDomainV32Namespace, key)
				}
				raw, err := tx.Get(backendDomainV32Namespace, key)
				if err != nil {
					return err
				}
				record, err := decodeBackendDomainRecord(backendDomainV32Format, raw)
				if err != nil {
					return err
				}
				entry, err := decodeCanonicalStoredEntry(backendDomainV32Format, record)
				if err != nil {
					return err
				}
				if corruption == "parent" {
					entry.Entry.ParentID = "entry-01"
				} else {
					entry.Entry.PathDepth--
				}
				raw, err = encodeBackendDomainRecord(backendDomainV32Format, record.Kind, record.ID, record.ThreadID, record.Ordinal, entry)
				if err != nil {
					return err
				}
				return tx.Put(backendDomainV32Namespace, key, raw)
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := repo.PathMetadataPage(t.Context(), "thread", "", "", 5); !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("corruption accepted: %v", err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if _, err := repo.PathMetadataPage(ctx, "thread", "", "", 5); !errors.Is(err, context.Canceled) || errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("cancelled page=%v", err)
			}
		})
	}
}

package sessiontree

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

func TestV32TailDeletionRestoresEveryLocator(t *testing.T) {
	backend, repo, _ := canonicalSettlementFixture(t, true)
	if _, err := repo.CancelTurn(t.Context(), testCancelTurnRequest(time.Now())); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.AppendRuntimeFacts(t.Context(), "thread", []Entry{
		{ID: "queued", Type: EntryQueueAdded, Payload: []byte("{\"id\":\"queued-input\"}")},
		{ID: "dequeued", Type: EntryQueueDeleted, Payload: []byte("\"queued-input\"")},
	}); err != nil {
		t.Fatal(err)
	}
	before := cloneMigrationRecords(backend.records)
	rollback := errors.New("rollback truncation")
	err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		count, err := readJournalCount(tx, "thread")
		if err != nil {
			return err
		}
		for ordinal := count - 1; ordinal >= 0; ordinal-- {
			if err := deleteCanonicalStoredEntry(t.Context(), tx, backendDomainV32Format, "thread", ordinal); err != nil {
				t.Fatalf("delete ordinal %d: %v", ordinal, err)
			}
			if err := verifyCanonicalThreadLocators(t.Context(), tx, "thread", ordinal); err != nil {
				t.Fatalf("verify after deleting ordinal %d: %v", ordinal, err)
			}
		}
		return rollback
	})
	if !errors.Is(err, rollback) || !reflect.DeepEqual(before, backend.records) {
		t.Fatalf("truncation did not roll back: %v", err)
	}
}

func TestV32RejectsReplacementAndNonTailDeletion(t *testing.T) {
	backend, _, _ := canonicalSettlementFixture(t, false)
	for _, operation := range []string{"replace", "delete-middle"} {
		t.Run(operation, func(t *testing.T) {
			before := cloneMigrationRecords(backend.records)
			err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				if operation == "delete-middle" {
					return deleteCanonicalStoredEntry(t.Context(), tx, backendDomainV32Format, "thread", 0)
				}
				entry, err := readCanonicalEntryOrdinal(tx, "thread", 0)
				if err != nil {
					return err
				}
				fields := entry.bodyFields
				entry.bodyFields = nil
				entry, err = materializeCanonicalStoredEntry(t.Context(), tx, canonicalStoredEntry{Entry: entry, Fields: fields})
				if err != nil {
					return err
				}
				return putCanonicalStoredEntry(t.Context(), tx, backendDomainV32Format, backendDomainEntryKey("thread", 0), entry.ID, "thread", 0, entry)
			})
			if !errors.Is(err, ErrAuthorityCorrupt) || !reflect.DeepEqual(before, backend.records) {
				t.Fatalf("invalid %s changed storage: %v", operation, err)
			}
		})
	}
}

package sessiontree

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

func TestMigrationRejectsIncompleteRootInventoryBeforeCleanup(t *testing.T) {
	for _, mode := range []string{"missing", "omitted", "reordered", "duplicate"} {
		t.Run(mode, func(t *testing.T) {
			memory := newMemoryRepo(time.Now)
			for index, id := range []string{"one", "two"} {
				if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: id, CreatedAt: time.Unix(int64(index+1), 0).UTC()}); err != nil {
					t.Fatal(err)
				}
			}
			backend, repo := openV31ReviewFixture(t, memory)
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				key := backendDomainKey(backendDomainRecordRootIndex)
				if mode == "missing" {
					return tx.Delete(backendDomainV31Namespace, key)
				}
				ids := []string{"two"}
				if mode == "reordered" {
					ids = []string{"one", "two"}
				}
				if mode == "duplicate" {
					ids = []string{"two", "two"}
				}
				return putBackendDomainRecordContext(t.Context(), tx, backendDomainV31Format, key, backendDomainRecordRootIndex, "", "", 0, backendDomainRootIndex{ThreadIDs: ids})
			}); err != nil {
				t.Fatal(err)
			}
			source := cloneMigrationRecords(backend.records)[backendDomainV31Namespace]
			for _, id := range []string{"one", "two"} {
				if err := repo.PrepareThread(t.Context(), id); err != nil {
					t.Fatal(err)
				}
			}
			var failure error
			for i := 0; i < 50 && repo.HasPendingMigration(); i++ {
				_, failure = repo.MigrateNextThread(t.Context())
				if failure != nil {
					break
				}
			}
			if !errors.Is(failure, ErrAuthorityCorrupt) {
				t.Fatalf("incomplete source accepted: %v", failure)
			}
			if !reflect.DeepEqual(source, backend.records[backendDomainV31Namespace]) {
				t.Fatal("failed inventory verification deleted source")
			}
			if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "new-after-source-error"}); err != nil {
				t.Fatal(err)
			}
			for _, id := range []string{"one", "two"} {
				if _, err := repo.Thread(t.Context(), id); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

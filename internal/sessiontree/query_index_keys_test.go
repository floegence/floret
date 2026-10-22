package sessiontree

import (
	"errors"
	"reflect"
	"testing"

	"github.com/floegence/floret/v7/internal/storagebridge"
	publicstorage "github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestQueryMetadataExactDocumentScopeAndQueryParity(t *testing.T) {
	for _, native := range []bool{false, true} {
		t.Run(map[bool]string{false: "kv", true: "sqlite"}[native], func(t *testing.T) {
			var backend spi.Backend = newMigrationTestBackend()
			if native {
				var err error
				backend, err = storagebridge.Open(t.Context(), storagebridge.Source(publicstorage.SQLite(t.TempDir()+"/index.sqlite")))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = backend.Close() })
			}
			documents := []queryStoredDocument{
				{Key: "a\x00one", ThreadID: "a", Object: "messages", Metadata: []byte("one")},
				{Key: "a\x00two", ThreadID: "a", Object: "tools", Metadata: []byte("two")},
				{Key: "b\x00denied", ThreadID: "b", Object: "messages", Metadata: []byte("denied")},
			}
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				for _, document := range documents {
					if err := queryMetadataWriter(tx).putDocument(document); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			rollback := errors.New("rollback exact replacement")
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				replacement := documents[0]
				replacement.ThreadID = "b"
				if err := queryMetadataWriter(tx).putDocument(replacement); err != nil {
					return err
				}
				return rollback
			}); !errors.Is(err, rollback) {
				t.Fatal(err)
			}
			if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
				index := queryMetadataReader(tx)
				reader := index
				for _, test := range []struct {
					scope, key string
					found      bool
				}{
					{"a", "a\x00one", true}, {"a", "a\x00two", true},
					{"a", "b\x00denied", false}, {"", "a\x00one", false},
					{"a", "missing", false}, {"b", "a\x00one", false},
				} {
					document, err := reader.readDocument(test.scope, test.key)
					if !test.found {
						if !errors.Is(err, spi.ErrNotFound) {
							t.Fatalf("scope=%q key=%q: %v", test.scope, test.key, err)
						}
					} else if err != nil || document.ThreadID != test.scope || document.Key != test.key {
						t.Fatalf("exact identity=%+v err=%v", document, err)
					}
				}
				for _, test := range []struct {
					query queryMetadataSelection
					want  []string
				}{
					{queryMetadataSelection{ThreadIDs: []string{"a", "a"}}, []string{"a\x00one", "a\x00two"}},
					{queryMetadataSelection{ThreadIDs: []string{"a"}, Object: "messages"}, []string{"a\x00one"}},

					{queryMetadataSelection{}, nil},
				} {
					documents, err := index.listDocuments(test.query)
					if err != nil {
						return err
					}
					var got []string
					for _, document := range documents {
						got = append(got, document.Key)
					}
					if !reflect.DeepEqual(got, test.want) {
						t.Fatalf("query=%+v keys=%q want=%q", test.query, got, test.want)
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

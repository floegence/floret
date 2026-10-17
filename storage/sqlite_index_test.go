package storage

import (
	"errors"
	"github.com/floegence/floret/v7/storage/spi"
	"testing"
)

func TestSQLiteIndexAtomicReplacementAndScope(t *testing.T) {
	backend, err := (sqliteSource{path: t.TempDir() + "/index.sqlite"}).Open(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	document := spi.IndexDocument{Key: "thread\x00message", ThreadID: "thread", Object: "messages", Metadata: []byte("source"), Terms: []string{"x00", "x61"}}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return tx.(spi.IndexWriter).PutIndexDocument(document) }); err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("rollback")
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		document.Terms = []string{"x62"}
		if err := tx.(spi.IndexWriter).PutIndexDocument(document); err != nil {
			return err
		}
		return sentinel
	}); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		index := tx.(spi.IndexReader)
		for _, test := range []struct {
			scope []string
			term  string
			want  int
		}{{[]string{"thread"}, "x61", 1}, {[]string{"thread"}, "x62", 0}, {[]string{"denied"}, "x61", 0}, {nil, "x61", 0}} {
			rows, err := index.QueryIndex(spi.IndexQuery{ThreadIDs: test.scope, Object: "messages", Terms: [][]string{{test.term}}})
			if err != nil {
				return err
			}
			if len(rows) != test.want {
				t.Fatalf("rows=%v want %d", rows, test.want)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return tx.(spi.IndexWriter).DeleteIndexThread("thread") }); err != nil {
		t.Fatal(err)
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		rows, err := tx.(spi.IndexReader).QueryIndex(spi.IndexQuery{ThreadIDs: []string{"thread"}})
		if len(rows) != 0 {
			t.Fatal("delete left documents")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

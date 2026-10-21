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

func TestSQLiteIndexAppendPreservesTermsAndAtomicScope(t *testing.T) {
	opened, err := (sqliteSource{path: t.TempDir() + "/append.sqlite"}).Open(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	document := spi.IndexDocument{Key: "key", ThreadID: "thread", Object: "messages", Metadata: []byte("first"), Terms: []string{"x61"}}
	if err := opened.Update(t.Context(), func(tx spi.WriteTx) error { return tx.(spi.IndexWriter).PutIndexDocument(document) }); err != nil {
		t.Fatal(err)
	}
	rollback := errors.New("rollback append")
	for _, abort := range []bool{true, false} {
		err := opened.Update(t.Context(), func(tx spi.WriteTx) error {
			appender := tx.(spi.IndexDocumentAppender)
			for _, scope := range []string{"denied", "thread"} {
				update := document
				update.ThreadID = scope
				update.Metadata = []byte("second")
				update.Terms = []string{"x62", "x62"}
				err := appender.AppendIndexDocument(update)
				if scope == "denied" {
					if !errors.Is(err, spi.ErrNotFound) {
						t.Fatalf("scope error: %v", err)
					}
				} else if err != nil {
					return err
				}
			}
			if abort {
				return rollback
			}
			return nil
		})
		if abort && !errors.Is(err, rollback) || !abort && err != nil {
			t.Fatal(err)
		}
		if err := opened.View(t.Context(), func(tx spi.ReadTx) error {
			for _, term := range []string{"x61", "x62"} {
				rows, err := tx.(spi.IndexReader).QueryIndex(spi.IndexQuery{ThreadIDs: []string{"thread"}, Terms: [][]string{{term}}})
				if err != nil {
					return err
				}
				want := 1
				if abort && term == "x62" {
					want = 0
				}
				if len(rows) != want {
					t.Fatalf("abort=%v term=%s rows=%d", abort, term, len(rows))
				}
				if len(rows) > 0 {
					wantMetadata := "first"
					if !abort {
						wantMetadata = "second"
					}
					if string(rows[0].Metadata) != wantMetadata {
						t.Fatal("metadata is not atomic")
					}
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := opened.Update(t.Context(), func(tx spi.WriteTx) error {
		document.Terms = nil
		if err := tx.(spi.IndexWriter).PutIndexDocument(document); err != nil {
			return err
		}
		return tx.(spi.IndexIntegrityChecker).CheckIndexIntegrity()
	}); err != nil {
		t.Fatal(err)
	}
	backend := opened.(*sqliteBackend)
	if _, err := backend.db.Exec("DELETE FROM floret_index_postings WHERE term=''"); err != nil {
		t.Fatal(err)
	}
	if err := opened.Update(t.Context(), func(tx spi.WriteTx) error { return tx.(spi.IndexIntegrityChecker).CheckIndexIntegrity() }); !errors.Is(err, spi.ErrIndexCorrupt) {
		t.Fatalf("missing inventory=%v", err)
	}
}

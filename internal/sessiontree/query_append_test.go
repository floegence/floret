package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/storagebridge"
	publicstorage "github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/threadquery"
)

func TestMergedQueryAppendKeepsExactUnicodeMatchesAcrossRestartAndRollback(t *testing.T) {
	for _, sqlite := range []bool{false, true} {
		t.Run(fmt.Sprint("sqlite=", sqlite), func(t *testing.T) {
			var backend spi.Backend = newMigrationTestBackend()
			if sqlite {
				var err error
				backend, err = storagebridge.Open(t.Context(), storagebridge.Source(publicstorage.SQLite(t.TempDir()+"/append.sqlite")))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = backend.Close() })
			}
			repo, err := NewBackendRepo(t.Context(), backend, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
				t.Fatal(err)
			}
			var full strings.Builder
			segments := []string{"İ", "中", "🌸", "é", "\r", "\n", "Alpha", "\x00", "終"}
			for i, text := range segments {
				if i == 4 {
					repo, err = NewBackendRepo(t.Context(), backend, time.Now)
					if err != nil {
						t.Fatal(err)
					}
				}
				entry := Entry{ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: text, ContentSourceID: "answer"}}
				if i == 5 {
					rollback := errors.New("rollback merged source")
					repo.backend = &domainCommitBoundaryBackend{Backend: backend, afterCallback: func() error { return rollback }}
					if _, err := repo.Append(t.Context(), entry, AppendOptions{ID: fmt.Sprint(i)}); !errors.Is(err, rollback) {
						t.Fatal(err)
					}
					repo.backend = backend
				}
				if _, err := repo.Append(t.Context(), entry, AppendOptions{ID: fmt.Sprint(i)}); err != nil {
					t.Fatal(err)
				}
				full.WriteString(text)
				assertQueryProjectionMatches(t, repo, "thread")
				for _, term := range []string{"İ中🌸", "中🌸é", "é\r\n", "\nAlpha", "Alpha\x00終", "missing"} {
					page, err := repo.Query(t.Context(), queryScope("thread"), threadquery.QueryInput{Object: threadquery.Messages, Filter: threadquery.Filter{Text: threadquery.TextMatch{Contains: term, CaseSensitive: true}}})
					if err != nil {
						t.Fatal(err)
					}
					want := 0
					if strings.Contains(full.String(), term) {
						want = 1
					}
					if page.Total != want {
						t.Fatalf("i=%d term=%q got=%d want=%d", i, term, page.Total, want)
					}
				}
			}
		})
	}
}

func TestMergedQueryAppendReadsNoHistoricalBody(t *testing.T) {
	repo := newMergedHistorySQLiteRepo(t, 1096)
	memory := repo.domainMemory
	if err := repo.backend.View(t.Context(), func(tx spi.ReadTx) error {
		index := queryMetadataReader(tx)
		document, err := index.readDocument("history", "history\x00messages\x000")
		if err != nil {
			return err
		}
		if len(document.Metadata) > 2048 {
			t.Fatalf("unbounded metadata: %d", len(document.Metadata))
		}
		reader := &queryBodyReadTx{ReadTx: tx}
		memory.bodyTx, memory.bodyContext = reader, t.Context()
		defer func() { memory.bodyTx, memory.bodyContext = nil, nil }()
		record, err := memory.queryRecordByKey(index, "history", document.Key)
		if err != nil {
			return err
		}
		if record.firstSource != "0" || len(record.parts) != 0 || reader.reads != 0 {
			t.Fatalf("unbounded read: %+v reads=%d", record, reader.reads)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var before queryDocumentMetadata
	if err := repo.backend.View(t.Context(), func(tx spi.ReadTx) error {
		document, err := queryMetadataReader(tx).readDocument("history", "history\x00messages\x000")
		if err != nil {
			return err
		}
		return json.Unmarshal(document.Metadata, &before)
	}); err != nil {
		t.Fatal(err)
	}
	if err := repo.UpdateDomain(t.Context(), func(memory *MemoryRepo, _ spi.WriteTx) error {
		for i, text := range []string{"中", "🌸", "tail"} {
			if _, err := memory.Append(t.Context(), Entry{ThreadID: "history", TurnID: "turn", RunID: "run", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: text, ContentSourceID: "shared-source"}}, AppendOptions{ID: fmt.Sprint("batch-", i)}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	assertQueryProjectionMatches(t, repo, "history")
	if err := repo.backend.View(t.Context(), func(tx spi.ReadTx) error {
		document, err := queryMetadataReader(tx).readDocument("history", "history\x00messages\x000")
		if err != nil {
			return err
		}
		var after queryDocumentMetadata
		if err := json.Unmarshal(document.Metadata, &after); err != nil {
			return err
		}
		if !reflect.DeepEqual(before.Row, after.Row) || before.FirstSource != after.FirstSource || after.LastSource != "batch-2" || after.PreviousSource != "batch-1" {
			t.Fatal("append changed identity or locator")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := repo.Append(ctx, Entry{ThreadID: "history"}, AppendOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

type queryBodyReadTx struct {
	spi.ReadTx
	reads int
}

func (tx *queryBodyReadTx) Get(namespace string, key []byte) ([]byte, error) {
	if namespace == canonicalBodyNamespace {
		tx.reads++
	}
	return tx.ReadTx.Get(namespace, key)
}

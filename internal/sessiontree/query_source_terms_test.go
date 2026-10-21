package sessiontree

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/observation"
	publicstorage "github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/tools"
)

func TestCanonicalQueryTermsMatchCompleteProjectionWithoutSourceResidency(t *testing.T) {
	backend, err := storagebridge.Open(t.Context(), storagebridge.Source(publicstorage.SQLite(t.TempDir()+"/terms.sqlite")))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Close() })
	memory := newMemoryRepo(time.Now)
	queryFixture(t, memory)
	at := time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC)
	private := strings.Repeat("private reasoning must not be indexed", 1000)
	for i := range 33 {
		text := strings.Repeat("Alpha İKΣ 中é🌸\r\n\x00<>&", 1500) + fmt.Sprint(i)
		appendQueryFixture(t, memory, "a", Entry{ID: fmt.Sprint("segment-", i), Type: EntryAssistantMessage, TurnID: "later", RunID: "later-run", Message: session.Message{Role: session.Assistant, Content: text, ContentSourceID: "answer", Reasoning: private, ReasoningSourceID: "reason"}}, at)
	}
	for i, typ := range []observation.EventType{observation.EventTypeHostedToolCall, observation.EventTypeHostedToolResult} {
		entry, err := NewHostedToolEntry(observation.Event{Type: typ, ThreadID: "a", TurnID: "later", RunID: "later-run", ToolID: "hosted", ToolName: "web_search", ObservedAt: at, Activity: &tools.ActivityPresentation{Label: "Search", Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Operation: "search", Summary: strings.Repeat("result 中🌸\r\n", 20)}}})
		if err != nil {
			t.Fatal(err)
		}
		entry.ID = fmt.Sprint("hosted-", i)
		appendQueryFixture(t, memory, "a", entry, at)
	}
	want, err := memory.projectQueryThread(t.Context(), "a")
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		return saveCompleteBackendDomainContext(t.Context(), tx, memory, backendDomainV31Format)
	}); err != nil {
		t.Fatal(err)
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		loaded, _, err := loadBackendDomain(t.Context(), tx, time.Now, backendDomainV31Format)
		if err != nil {
			return err
		}
		metadata, err := loaded.projectQueryThreadMode(t.Context(), "a", true)
		if err != nil {
			return err
		}
		privateRef := canonicalBodyRef{SHA256: stableHash(private), Bytes: len(private)}
		reader := &queryTermReadTx{ReadTx: tx, forbidden: string(canonicalBodyKey(privateRef.SHA256))}
		for key, record := range metadata.records {
			document, err := queryDocument(record, false)
			if err != nil {
				return err
			}
			for _, source := range record.sources {
				entry := loaded.entries["a"][loaded.entryOrdinals["a"][source]]
				for _, ref := range entry.bodyFields {
					releaseCachedBody(ref)
				}
			}
			actual, err := loaded.canonicalQueryDocumentTerms(t.Context(), reader, document)
			if err != nil {
				return err
			}
			if !reflect.DeepEqual(actual, referenceQueryDocumentTerms(want.records[key])) {
				t.Fatalf("terms differ for %q", key)
			}
			for _, source := range record.sources {
				entry := loaded.entries["a"][loaded.entryOrdinals["a"][source]]
				for _, ref := range entry.bodyFields {
					if _, found := cachedBody(ref); found {
						t.Fatal("streaming terms filled complete source cache")
					}
				}
			}
		}
		if reader.reads == 0 || reader.maxEncodedBytes > 24<<10 {
			t.Fatalf("unbounded physical read: %+v", reader)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

type queryTermReadTx struct {
	spi.ReadTx
	forbidden              string
	reads, maxEncodedBytes int
}

func (tx *queryTermReadTx) Get(namespace string, key []byte) ([]byte, error) {
	if namespace == canonicalBodyNamespace && string(key) == tx.forbidden {
		return nil, errors.New("indexed private reasoning")
	}
	raw, err := tx.ReadTx.Get(namespace, key)
	if namespace == canonicalBodyNamespace {
		tx.reads++
		tx.maxEncodedBytes = max(tx.maxEncodedBytes, len(raw))
	}
	return raw, err
}

func TestCanonicalQueryTermsCancelAndCorruptionReturnNoResult(t *testing.T) {
	backend := newMigrationTestBackend()
	memory := newMemoryRepo(time.Now)
	queryFixture(t, memory)
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		return saveCompleteBackendDomainContext(t.Context(), tx, memory, backendDomainV31Format)
	}); err != nil {
		t.Fatal(err)
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		loaded, _, err := loadBackendDomain(t.Context(), tx, time.Now, backendDomainV31Format)
		if err != nil {
			return err
		}
		state, err := loaded.projectQueryThreadMode(t.Context(), "a", true)
		if err != nil {
			return err
		}
		document, err := queryDocument(state.records["a\x00messages\x00a0"], false)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		actual, err := loaded.canonicalQueryDocumentTerms(ctx, &startupCancelBodyTx{ReadTx: tx, cancel: cancel}, document)
		if !errors.Is(err, context.Canceled) || actual != nil {
			t.Fatalf("cancel retained result: %v %v", actual, err)
		}
		failure := errors.New("canonical block unavailable")
		actual, err = loaded.canonicalQueryDocumentTerms(t.Context(), failingBodyReadTx{ReadTx: tx, failure: failure}, document)
		if !errors.Is(err, failure) || actual != nil {
			t.Fatalf("failure retained result: %v %v", actual, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkCanonicalQueryTermsStorage(b *testing.B) {
	for _, count := range []int{1, 100} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			backend, err := storagebridge.Open(b.Context(), storagebridge.Source(publicstorage.SQLite(b.TempDir()+"/terms.sqlite")))
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = backend.Close() })
			memory := newMemoryRepo(time.Now)
			if _, err := memory.CreateThread(b.Context(), ThreadMeta{ID: "thread"}); err != nil {
				b.Fatal(err)
			}
			for i := range count {
				if _, err := memory.Append(b.Context(), Entry{ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: strings.Repeat("Alpha 中é🌸\r\n", 2560), ContentSourceID: "answer"}}, AppendOptions{ID: fmt.Sprint(i)}); err != nil {
					b.Fatal(err)
				}
			}
			if err := backend.Update(b.Context(), func(tx spi.WriteTx) error {
				return saveCompleteBackendDomainContext(b.Context(), tx, memory, backendDomainV31Format)
			}); err != nil {
				b.Fatal(err)
			}
			var loaded *MemoryRepo
			var document spi.IndexDocument
			if err := backend.View(b.Context(), func(tx spi.ReadTx) error {
				var err error
				loaded, _, err = loadBackendDomain(b.Context(), tx, time.Now, backendDomainV31Format)
				if err != nil {
					return err
				}
				state, err := loaded.projectQueryThreadMode(b.Context(), "thread", true)
				if err != nil {
					return err
				}
				document, err = queryDocument(state.records["thread\x00messages\x000"], false)
				return err
			}); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := backend.View(b.Context(), func(tx spi.ReadTx) error {
					terms, err := loaded.canonicalQueryDocumentTerms(b.Context(), tx, document)
					if err == nil && len(terms) == 0 {
						b.Fatal("no terms")
					}
					return err
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

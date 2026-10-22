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

	"github.com/floegence/floret/v7/internal/citation"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/observation"
	publicstorage "github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/tools"
)

func TestCurrentStartupRetainsLocatorsBeforeDomainValidation(t *testing.T) {
	backend := newMigrationTestBackend()
	memory := newMemoryRepo(time.Now)
	queryFixture(t, memory)
	text := strings.Repeat("body 中文🌸\r\n", 10000)
	start, end := 0, 4
	for i := range 12 {
		appendQueryFixture(t, memory, "a", Entry{ID: fmt.Sprint("large-", i), Type: EntryAssistantMessage, TurnID: "later", RunID: "later-run", Message: session.Message{Role: session.Assistant, Content: text, ContentSourceID: "shared", Reasoning: text, ReasoningSourceID: "shared-reason", Sources: []citation.Source{{URL: "https://example.com", StartIndex: &start, EndIndex: &end}}}}, time.Now())
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		return saveCompleteBackendDomainContext(t.Context(), tx, memory, backendDomainV31Format)
	}); err != nil {
		t.Fatal(err)
	}
	before := cloneMigrationRecords(backend.records)
	checked := false
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		format := backendDomainV31Format
		validate := format.validate
		format.validate = func(candidate *MemoryRepo) error {
			checked = true
			for _, entries := range candidate.entries {
				for _, entry := range entries {
					if len(entry.bodyFields) > 0 && (entry.Raw != "" || entry.Message.Content != "" || entry.Message.Reasoning != "" || entry.Message.ToolArgs != "" || entry.Message.Activity != nil) {
						t.Fatal("startup retained complete body before domain validation")
					}
				}
			}
			return validate(candidate)
		}
		loaded, found, err := loadBackendDomain(t.Context(), tx, time.Now, format)
		if err != nil || !found {
			t.Fatalf("load: %v", err)
		}
		if loaded.bodyTx != nil || loaded.bodyContext != nil {
			t.Fatal("loader retained startup transaction")
		}
		loaded.bodyTx, loaded.bodyContext = tx, t.Context()
		defer func() { loaded.bodyTx, loaded.bodyContext = nil, nil }()
		for id, expected := range memory.entries {
			got, err := loaded.completeEntriesLocked(loaded.entries[id])
			if err != nil {
				t.Fatal(err)
			}
			for i := range expected {
				actual, err := json.Marshal(got[i])
				if err != nil {
					t.Fatal(err)
				}
				want, err := json.Marshal(expected[i])
				if err != nil || string(actual) != string(want) {
					t.Fatalf("canonical fact changed for %s/%d: %v", id, i, err)
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !checked || !equalMigrationRecords(before, backend.records) {
		t.Fatal("startup skipped validation or mutated canonical facts")
	}
}

func TestStartupMetadataQueryMatchesCompleteProjectionWithoutBodyReads(t *testing.T) {
	backend := newMigrationTestBackend()
	memory := newMemoryRepo(time.Now)
	queryFixture(t, memory)
	at := time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC)
	for i := range 600 {
		appendQueryFixture(t, memory, "a", Entry{ID: fmt.Sprint("segment-", i), Type: EntryAssistantMessage, TurnID: "later", RunID: "later-run", Message: session.Message{Role: session.Assistant, Content: "segment 中🌸", ContentSourceID: "shared"}}, at.Add(time.Duration(i)*time.Second))
	}
	appendQueryFixture(t, memory, "a", Entry{ID: "empty", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant}}, at)
	appendQueryFixture(t, memory, "a", Entry{ID: "hidden", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Kind: session.MessageKindControlSignal, Content: "hidden"}}, at)
	appendQueryFixture(t, memory, "a", Entry{ID: "steer", Type: EntrySteeringRequested, TurnID: "later", RunID: "later-run", RequestKey: "steer", RequestFingerprint: "fingerprint", Message: session.Message{Role: session.User, Content: "accepted steering"}}, at)
	appendQueryFixture(t, memory, "a", Entry{ID: "steer-applied", Type: EntrySteeringApplied, TurnID: "later", RunID: "later-run", Payload: []byte("{\"steering_id\":\"steer\"}")}, at)
	for i, typ := range []observation.EventType{observation.EventTypeHostedToolCall, observation.EventTypeHostedToolResult, observation.EventTypeHostedToolCall} {
		id := "hosted"
		if i == 2 {
			id = "pending"
		}
		entry, err := NewHostedToolEntry(observation.Event{Type: typ, ThreadID: "a", TurnID: "later", RunID: "later-run", ToolID: id, ToolName: "web_search", ObservedAt: at, Activity: &tools.ActivityPresentation{Label: "Search", Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Operation: "search", Summary: "result"}}})
		if err != nil {
			t.Fatal(err)
		}
		entry.ID = fmt.Sprint("hosted-", i)
		appendQueryFixture(t, memory, "a", entry, at)
	}
	appendQueryFixture(t, memory, "a", Entry{ID: "later-end", Type: EntryTurnMarker, TurnID: "later", RunID: "later-run", TurnStatus: TurnCompleted}, at)
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
		loaded.bodyTx = failingBodyReadTx{ReadTx: tx, failure: errors.New("metadata query read body")}
		loaded.bodyContext = t.Context()
		got, err := loaded.projectQueryThreadMode(t.Context(), "a", true)
		if err != nil {
			return err
		}
		if len(got.records) != len(want.records) {
			t.Fatal("query record count changed")
		}
		for key, record := range want.records {
			expected, err := queryDocument(record)
			if err != nil {
				return err
			}
			actual, err := queryDocument(got.records[key])
			if err != nil || !reflect.DeepEqual(actual, expected) {
				t.Fatalf("query metadata differs: %s: %v", key, err)
			}
			if len(got.records[key].parts) != 0 || len(got.records[key].activities) != 0 {
				t.Fatal("metadata query retained bodies")
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestCurrentStartupCancellationDuringBodyVerificationReturnsNoCandidate(t *testing.T) {
	backend := newMigrationTestBackend()
	repo, err := NewBackendRepo(t.Context(), backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Append(t.Context(), Entry{ThreadID: "thread", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: strings.Repeat("中文🌸", 20000)}}, AppendOptions{ID: "body"}); err != nil {
		t.Fatal(err)
	}
	before := cloneMigrationRecords(backend.records)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		reader := &startupCancelBodyTx{ReadTx: tx, cancel: cancel}
		candidate, found, err := loadBackendDomain(ctx, reader, time.Now, backendDomainV31Format)
		if !errors.Is(err, context.Canceled) || candidate != nil || found {
			t.Fatalf("partial cancelled candidate: %v %v", found, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !equalMigrationRecords(before, backend.records) {
		t.Fatal("cancelled validation changed facts")
	}
}

type startupCancelBodyTx struct {
	spi.ReadTx
	cancel context.CancelFunc
}

func (tx *startupCancelBodyTx) Get(namespace string, key []byte) ([]byte, error) {
	result, err := tx.ReadTx.Get(namespace, key)
	if namespace == canonicalBodyNamespace {
		tx.cancel()
	}
	return result, err
}

func BenchmarkCurrentStartupCanonicalLoad(b *testing.B) {
	for _, count := range []int{16, 160} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			backend, err := storagebridge.Open(b.Context(), storagebridge.Source(publicstorage.SQLite(b.TempDir()+"/startup.sqlite")))
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = backend.Close() })
			memory := newMemoryRepo(time.Now)
			if _, err := memory.CreateThread(b.Context(), ThreadMeta{ID: "thread"}); err != nil {
				b.Fatal(err)
			}
			for i := range count {
				text := fmt.Sprint(i) + strings.Repeat("source 中é🌸\r\n", 4096)
				if _, err := memory.Append(b.Context(), Entry{ThreadID: "thread", Type: EntryAssistantMessage, TurnID: "turn", RunID: "run", Message: session.Message{Role: session.Assistant, Content: text}}, AppendOptions{ID: fmt.Sprint(i)}); err != nil {
					b.Fatal(err)
				}
			}
			if err := backend.Update(b.Context(), func(tx spi.WriteTx) error {
				return saveCompleteBackendDomainContext(b.Context(), tx, memory, backendDomainV31Format)
			}); err != nil {
				b.Fatal(err)
			}
			memory = nil
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := backend.View(b.Context(), func(tx spi.ReadTx) error {
					loaded, found, err := loadBackendDomain(b.Context(), tx, time.Now, backendDomainV31Format)
					if err != nil {
						return err
					}
					if !found || len(loaded.entries["thread"]) != count {
						return errors.New("startup lost entries")
					}
					for _, entry := range loaded.entries["thread"] {
						if entry.Raw != "" || entry.Message.Content != "" {
							return errors.New("startup retained source")
						}
					}
					return nil
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

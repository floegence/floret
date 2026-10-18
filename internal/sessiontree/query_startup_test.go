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
	publicstorage "github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/threadquery"
)

func TestInspectionPreservesProjectionValidation(t *testing.T) {
	for _, corruption := range []string{"none", "tool identity", "occurrence time"} {
		t.Run(corruption, func(t *testing.T) {
			memory := newMemoryRepo(time.Now)
			queryFixture(t, memory)
			for threadID, entries := range memory.entries {
				for ordinal := range entries {
					entry := &entries[ordinal]
					if entry.Type != EntryToolResult {
						continue
					}
					switch corruption {
					case "tool identity":
						entry.Message.ToolName = "conflicting-tool"
					case "occurrence time":
						entry.Metadata = map[string]string{OccurrenceTimeMetadataKey: "invalid"}
					}
					entry.Raw = rawForEntry(*entry)
					entry.RawHash = stableHash(entry.Raw)
				}
				memory.entries[threadID] = entries
			}
			backend := newMigrationTestBackend()
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				return saveCompleteBackendDomainV20(tx, memory)
			}); err != nil {
				t.Fatal(err)
			}
			before := cloneMigrationRecords(backend.records)
			for _, inspection := range []bool{true, false} {
				err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
					repo, err := NewBackendRepoInTransaction(t.Context(), backend, tx, time.Now, nil)
					if err != nil {
						t.Fatalf("load fixture before projection verification: %v", err)
					}
					if inspection {
						err := repo.VerifyInspectionStateInTransaction(t.Context(), tx)
						if repo.domainMemory.querySnapshots != nil {
							t.Fatal("inspection retained its disposable query index")
						}
						return err
					}
					return repo.VerifyCurrentStateInTransaction(t.Context(), tx)
				})
				if corruption == "none" {
					if err != nil {
						t.Fatal(err)
					}
				} else if !errors.Is(err, ErrAuthorityCorrupt) {
					t.Fatalf("inspection=%v: got %v, want ErrAuthorityCorrupt", inspection, err)
				}
				if !equalCanonicalMigrationRecords(backend.records, before) {
					t.Fatal("startup verification changed canonical bytes")
				}
			}
		})
	}
}

func TestInspectionDoesNotReplaceLiveSearchIndex(t *testing.T) {
	backend := newMigrationTestBackend()
	memory := newMemoryRepo(time.Now)
	queryFixture(t, memory)
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		return saveCompleteBackendDomainV20(tx, memory)
	}); err != nil {
		t.Fatal(err)
	}
	repo, err := NewBackendRepo(t.Context(), backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	index := repo.domainMemory.querySnapshots
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		return repo.VerifyInspectionStateInTransaction(t.Context(), tx)
	}); err != nil {
		t.Fatal(err)
	}
	if repo.domainMemory.querySnapshots != index {
		t.Fatal("inspection replaced the canonical owner's search index")
	}
	page, err := repo.Query(t.Context(), queryScope("a", "b"), threadquery.QueryInput{
		Object: threadquery.Messages, Filter: threadquery.Filter{Text: threadquery.TextMatch{Contains: "中文"}},
	})
	if err != nil || page.Total != 4 {
		t.Fatalf("search after inspection: total=%d err=%v", page.Total, err)
	}
}

func TestThreadQueryUnicodeFragmentParity(t *testing.T) {
	for _, backendName := range []string{"direct", "sqlite", "custom"} {
		t.Run(backendName, func(t *testing.T) {
			var repo queryTestRepo = NewMemoryRepo()
			if backendName != "direct" {
				var backend spi.Backend = newMigrationTestBackend()
				if backendName == "sqlite" {
					var err error
					backend, err = storagebridge.Open(t.Context(), storagebridge.Source(publicstorage.SQLite(t.TempDir()+"/unicode.sqlite")))
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = backend.Close() })
				}
				canonical, err := NewBackendRepo(t.Context(), backend, time.Now)
				if err != nil {
					t.Fatal(err)
				}
				repo = canonical
			}

			if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "unicode"}); err != nil {
				t.Fatal(err)
			}
			texts := []string{"İKΣ 中文🙂𐐀 code(foo_bar) e\u0301", "中文🙂🙂\x00重复重复 XYZ", "plain unrelated text"}
			for i, text := range texts {
				appendQueryFixture(t, repo, "unicode", Entry{ID: fmt.Sprint(i), Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: text}}, time.Unix(int64(i), 0))
			}
			for _, sensitive := range []bool{false, true} {
				for _, term := range []string{"İ", "i", "kσ", "中文", "文🙂", "🙂𐐀", "🙂🙂", "\x00重", "重复重复", "foo_bar)", "e\u0301", "XYZ", "missing"} {
					var want []string
					for i, text := range texts {
						target := term
						if !sensitive {
							text, target = strings.ToLower(text), strings.ToLower(target)
						}
						if strings.Contains(text, target) {
							want = append(want, fmt.Sprint(i))
						}
					}
					page, err := repo.Query(t.Context(), queryScope("unicode"), threadquery.QueryInput{
						Object: threadquery.Messages, Filter: threadquery.Filter{Text: threadquery.TextMatch{Contains: term, CaseSensitive: sensitive}},
					})
					if err != nil {
						t.Fatal(err)
					}
					var got []string
					// Query refs are opaque; compare the canonical bodies through Read.
					for _, record := range page.Records {
						read, err := repo.Read(t.Context(), queryScope("unicode"), threadquery.ReadInput{Ref: record.Ref})
						if err != nil {
							t.Fatal(err)
						}
						for i, text := range texts {
							if len(read.Chunks) == 1 && read.Chunks[0].Text == text {
								got = append(got, fmt.Sprint(i))
							}
						}
					}
					if !reflect.DeepEqual(got, want) || page.Total != len(want) {
						t.Fatalf("term=%q case_sensitive=%v: got %v total=%d, want %v", term, sensitive, got, page.Total, want)
					}
				}
			}
		})
	}

}

func BenchmarkQueryStartup(b *testing.B) {
	for _, inspection := range []bool{true, false} {
		b.Run(fmt.Sprintf("inspection=%v", inspection), func(b *testing.B) {
			memory := newMemoryRepo(time.Now)
			ctx := context.Background()
			for thread := 0; thread < 10; thread++ {
				id := fmt.Sprint(thread)
				if _, err := memory.CreateThread(ctx, ThreadMeta{ID: id}); err != nil {
					b.Fatal(err)
				}
				for message := 0; message < 100; message++ {
					text := strings.Repeat(fmt.Sprintf("Message %d code(foo_bar) 中文🙂 result %d. ", message, thread), 50)
					if _, err := memory.Append(ctx, Entry{ThreadID: id, Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: text}}, AppendOptions{}); err != nil {
						b.Fatal(err)
					}
				}
			}
			backend := newMigrationTestBackend()
			repo := &BackendRepo{domainMemory: memory}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				memory.querySnapshots = nil
				if err := backend.View(ctx, func(tx spi.ReadTx) error {
					if inspection {
						return repo.VerifyInspectionStateInTransaction(ctx, tx)
					}
					return repo.VerifyCurrentStateInTransaction(ctx, tx)
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Derived indexes are allowed to initialize; canonical namespaces stay exact.
func equalCanonicalMigrationRecords(a, b map[string]map[string][]byte) bool {
	selectCanonical := func(records map[string]map[string][]byte) map[string]map[string][]byte {
		out := map[string]map[string][]byte{}
		for ns, values := range records {
			if ns != queryDocumentNamespace && ns != queryManifestNamespace {
				out[ns] = values
			}
		}
		return out
	}
	return equalMigrationRecords(selectCanonical(a), selectCanonical(b))
}

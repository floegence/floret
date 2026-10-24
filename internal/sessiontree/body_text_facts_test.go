package sessiontree

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestFieldWindowReadsBoundedTextPreviews(t *testing.T) {
	for _, size := range []int{1024, 1 << 20} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			backend, repo := projectionSourcesFixture(t, 0)
			text := strings.Repeat("中文🌸 ", size/11+1)
			if _, err := repo.Append(t.Context(), Entry{ID: "visible", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: text, ContentSourceID: "answer"}}, AppendOptions{}); err != nil {
				t.Fatal(err)
			}
			if err := repo.prepareWindowProjection(t.Context(), "thread"); err != nil {
				t.Fatal(err)
			}
			measured := &activityBudgetBackend{Backend: backend}
			repo.backend = measured
			page, err := repo.ReadWindowProjection(t.Context(), ItemMembershipWindowOptions{ThreadID: "thread", Limit: 1, ActivityFields: true}, func(source PathMetadata, _ bool) []string {
				if source.Entry.ID == "visible" {
					return []string{"content"}
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			var found bool
			for _, source := range page.Entries {
				if source.Entry.ID != "visible" {
					continue
				}
				found = true
				facts := source.Entry.ProjectedText["content"]
				if source.Entry.Message.Content != "" || facts.Bytes != len(text) || facts.Runes != utf8.RuneCountInString(text) || !facts.NonSpace {
					t.Fatalf("wrong text metadata: %+v", facts)
				}
			}
			if !found || measured.bodyBytes > 48<<10 {
				t.Fatalf("window materialized body: found=%v bytes=%d", found, measured.bodyBytes)
			}
		})
	}
}
func TestBodyTextFactsPreparationPreservesSourceAndRejectsCorruption(t *testing.T) {
	backend, repo := projectionSourcesFixture(t, 0)
	text := strings.Repeat(" ", 65536) + "🌸"
	saved, err := repo.Append(t.Context(), Entry{ID: "visible", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: text}}, AppendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ref := canonicalBodyRef{SHA256: stableHash(text), Bytes: len(text)}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return tx.Delete(bodyTextFactsNamespace, canonicalBodyKey(ref.SHA256)) }); err != nil {
		t.Fatal(err)
	}
	missing := missingBodyTextFacts{"visible", "content", ref}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if err := repo.prepareBodyTextFacts(canceled, "thread", missing); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel=%v", err)
	}
	for range 2 {
		if err := repo.prepareBodyTextFacts(t.Context(), "thread", missing); err != nil {
			t.Fatal(err)
		}
	}
	after, err := repo.Entry(t.Context(), "thread", saved.ID)
	if err != nil || after.RawHash != saved.RawHash || after.Message.Content != text {
		t.Fatal("preparation changed canonical source", err)
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		facts, err := readBodyTextFacts(tx, ref)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(facts, canonicalTextFacts(text)) {
			t.Fatal("prepared facts differ")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		return tx.Put(bodyTextFactsNamespace, canonicalBodyKey(ref.SHA256), []byte("corrupt"))
	}); err != nil {
		t.Fatal(err)
	}
	if err := repo.prepareBodyTextFacts(t.Context(), "thread", missing); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("corrupt metadata was repaired silently: %v", err)
	}
}

func TestBodyTextFactsCommitRollbackRestartAndDelete(t *testing.T) {
	for _, panicAfter := range []bool{false, true} {
		t.Run(fmt.Sprint(panicAfter), func(t *testing.T) {
			backend, repo := projectionSourcesFixture(t, 0)
			before := cloneMigrationRecords(backend.records)
			failure := errors.New("injected text metadata commit failure")
			repo.backend = &domainCommitBoundaryBackend{Backend: backend, afterCallback: func() error {
				if panicAfter {
					panic(failure)
				}
				return failure
			}}
			entry := Entry{ID: "text", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "retained 🌸"}}
			var panicValue any
			func() {
				defer func() {
					panicValue = recover()
				}()
				if _, err := repo.Append(t.Context(), entry, AppendOptions{}); !panicAfter && !errors.Is(err, failure) {
					t.Fatal(err)
				}
			}()
			if panicAfter && panicValue != failure || !panicAfter && panicValue != nil {
				t.Fatalf("unexpected panic boundary: %v", panicValue)
			}
			if !reflect.DeepEqual(before, backend.records) {
				t.Fatal("failed transaction published text metadata")
			}
			repo.backend = backend
			if _, err := repo.Append(t.Context(), entry, AppendOptions{}); err != nil {
				t.Fatal(err)
			}
			repo = reopenCurrentStartupRepo(t, backend, false)
			ref := canonicalBodyRef{SHA256: stableHash(entry.Message.Content), Bytes: len(entry.Message.Content)}
			if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
				facts, err := readBodyTextFacts(tx, ref)
				if err == nil && !reflect.DeepEqual(facts, canonicalTextFacts(entry.Message.Content)) {
					t.Fatal("restart changed facts")
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := repo.DeleteRootTree(t.Context(), "thread"); err != nil {
				t.Fatal(err)
			}
			if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
				_, err := readBodyTextFacts(tx, ref)
				if !errors.Is(err, spi.ErrNotFound) {
					t.Fatalf("delete retained metadata: %v", err)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestBodyTextPreviewSkipsUnboundedPadding(t *testing.T) {
	for _, text := range []string{strings.Repeat(" ", 1<<20) + "end", "start" + strings.Repeat(" ", 1<<20), strings.Repeat(" ", 1<<20), strings.Repeat("🌸", 1000)} {
		backend, repo := projectionSourcesFixture(t, 0)
		if _, err := repo.Append(t.Context(), Entry{ID: "text", ThreadID: "thread", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: text}}, AppendOptions{}); err != nil {
			t.Fatal(err)
		}
		measured := &activityBudgetBackend{Backend: backend}
		err := measured.View(t.Context(), func(tx spi.ReadTx) error {
			ref := canonicalBodyRef{SHA256: stableHash(text), Bytes: len(text)}
			facts, err := readBodyTextFacts(tx, ref)
			if err != nil {
				return err
			}
			facts, err = readBodyTextPreview(t.Context(), tx, ref, facts)
			if err != nil {
				return err
			}
			want := []rune(strings.TrimSpace(text))
			if len(want) > 160 {
				want = want[:160]
			}
			got := facts.Summary
			if !facts.SummaryHasTail {
				got = strings.TrimRightFunc(got, unicode.IsSpace)
			}
			if got != string(want) {
				t.Fatal("preview differs")
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if measured.bodyBytes > 96<<10 {
			t.Fatalf("padding caused %d bytes read", measured.bodyBytes)
		}
	}
}

func TestProjectedTextCannotEnterCanonicalHistory(t *testing.T) {
	backend, repo := projectionSourcesFixture(t, 0)
	before := cloneMigrationRecords(backend.records)
	_, err := repo.Append(t.Context(), Entry{ID: "projected", ThreadID: "thread", Type: EntryUserMessage, Message: session.Message{Role: session.User}, ProjectedText: map[string]CanonicalTextFacts{"content": {Bytes: 10, Runes: 10, NonSpace: true}}}, AppendOptions{})
	if err == nil || !reflect.DeepEqual(before, backend.records) {
		t.Fatal("projection entered canonical history", err)
	}
}

func TestBodyMigrationReleasesDerivedMetadataTogether(t *testing.T) {
	backend, _ := projectionSourcesFixture(t, 0)
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		for _, namespace := range []string{canonicalBodyNamespace, bodyTextFactsNamespace, activityFieldsNamespace} {
			for i := range 300 {
				if err := tx.Put(namespace, []byte(fmt.Sprintf("key-%04d", i)), []byte("old")); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return deleteCanonicalBodyRecords(t.Context(), tx) }); err != nil {
		t.Fatal(err)
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		for _, namespace := range []string{canonicalBodyNamespace, bodyTextFactsNamespace, activityFieldsNamespace} {
			page, err := tx.Scan(spi.ScanRequest{Namespace: namespace, Limit: 1})
			if err != nil {
				return err
			}
			if len(page.Records) != 0 {
				t.Fatalf("migration retained orphaned %s", namespace)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

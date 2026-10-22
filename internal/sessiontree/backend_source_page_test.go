package sessiontree

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

type sourcePageBackend struct{ spi.Backend }

func (backend sourcePageBackend) View(ctx context.Context, read func(spi.ReadTx) error) error {
	return backend.Backend.View(ctx, func(tx spi.ReadTx) error { return read(sourcePageTx{tx}) })
}

type sourcePageTx struct{ spi.ReadTx }

func (tx sourcePageTx) Get(namespace string, key []byte) ([]byte, error) {
	if namespace == canonicalBodyNamespace {
		return nil, errors.New("source page read a historical body")
	}
	return tx.ReadTx.Get(namespace, key)
}

func (tx sourcePageTx) Scan(request spi.ScanRequest) (spi.ScanPage, error) {
	if request.Namespace != backendEntryIndexNamespace || !bytes.HasPrefix(request.Start, backendDomainKey("source")) || request.Limit > 256 {
		return spi.ScanPage{}, errors.New("source page scanned outside its bounded locators")
	}
	return tx.ReadTx.Scan(request)
}

func TestV32SourcePagesMatchCanonicalMemoryWithoutLoadingJournal(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	appendPathTestEntry(t, memory, "root", "")
	for i := range 600 {
		if _, err := memory.Append(t.Context(), Entry{ThreadID: "thread", ParentID: "root", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "sibling", ContentSourceID: "shared", Reasoning: "reason", ReasoningSourceID: "reason"}}, AppendOptions{ID: fmt.Sprint(i)}); err != nil {
			t.Fatal(err)
		}
	}
	base, repo := openV31ReviewFixture(t, memory)
	if err := repo.PrepareThread(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	for restart := range 2 {
		if restart > 0 {
			if err := base.View(t.Context(), func(tx spi.ReadTx) error {
				var err error
				repo, _, err = NewCurrentBackendRepoInTransaction(t.Context(), base, tx, time.Now, false)
				return err
			}); err != nil {
				t.Fatal(err)
			}
		}
		repo.backend = sourcePageBackend{base}
		for _, field := range []string{"content", "reasoning"} {
			source := "shared"
			if field == "reasoning" {
				source = "reason"
			}
			options := SourceBodyPageOptions{ThreadID: "thread", LeafID: "599", SourceID: source, Field: field, Limit: 150}
			var sizes []int
			for {
				want, err := memory.SourceBodyPage(t.Context(), options)
				if err != nil {
					t.Fatal(err)
				}
				got, err := repo.SourceBodyPage(t.Context(), options)
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("source page = %+v, %v; want %+v", got, err, want)
				}
				sizes = append(sizes, len(got.Bodies))
				if !got.HasMore {
					break
				}
				options.AfterEntryID = got.NextEntryID
			}
			if !reflect.DeepEqual(sizes, []int{0, 0, 1}) {
				t.Fatalf("source scan pages = %v", sizes)
			}
		}
		if repo.threadMemory != nil {
			t.Fatal("source pages loaded the complete journal")
		}
	}
}

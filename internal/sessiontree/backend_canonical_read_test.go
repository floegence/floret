package sessiontree

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

type boundedPageBackend struct {
	spi.Backend
	gets                     int
	scans                    int
	allowedStart, allowedEnd []byte
}

func (backend *boundedPageBackend) View(ctx context.Context, read func(spi.ReadTx) error) error {
	return backend.Backend.View(ctx, func(tx spi.ReadTx) error { return read(boundedPageTx{ReadTx: tx, backend: backend}) })
}

type boundedPageTx struct {
	spi.ReadTx
	backend *boundedPageBackend
}

func (tx boundedPageTx) Scan(request spi.ScanRequest) (spi.ScanPage, error) {
	if request.Namespace != backendDomainV32Namespace || request.Limit != 5 || len(request.After) != 0 || !bytes.Equal(request.Start, tx.backend.allowedStart) || !bytes.Equal(request.End, tx.backend.allowedEnd) {
		return spi.ScanPage{}, errors.New("prepared page scanned outside its exact source interval")
	}
	tx.backend.scans++
	return tx.ReadTx.Scan(request)
}

func (tx boundedPageTx) Get(namespace string, key []byte) ([]byte, error) {
	tx.backend.gets++
	if namespace == canonicalBodyNamespace {
		return nil, errors.New("metadata page read canonical body bytes")
	}
	return tx.ReadTx.Get(namespace, key)
}

func TestV32CanonicalPagesReadOnlyRequestedSourceRecords(t *testing.T) {
	for _, count := range []int{32, 1024} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			memory := newMemoryRepo(time.Now)
			if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < count; i++ {
				if _, err := memory.Append(t.Context(), Entry{ID: fmt.Sprintf("entry-%04d", i), ThreadID: "thread", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "historical content"}}, AppendOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			backend, repo := openV31ReviewFixture(t, memory)
			if err := repo.PrepareThread(t.Context(), "thread"); err != nil {
				t.Fatal(err)
			}
			if _, err := repo.Thread(t.Context(), "thread"); err != nil {
				t.Fatal(err)
			}
			// A prepared page must remain bounded even after evicting the optional
			// mutation snapshot. Only canonical locators may serve this read.
			repo.threadMemoryID, repo.threadMemory = "", nil
			repo.verifiedThreadLeaves = nil
			bounded := &boundedPageBackend{Backend: backend, allowedStart: backendDomainEntryKey("thread", count-5), allowedEnd: backendDomainEntryKey("thread", count)}
			repo.backend = bounded
			page, err := repo.PathMetadataPage(t.Context(), "thread", "", "", 5)
			if err != nil || len(page.Entries) != 5 || page.NewestOrdinal != int64(count) {
				t.Fatalf("metadata page = %+v, %v", page, err)
			}
			journal, err := repo.JournalMetadata(t.Context(), JournalMetadataOptions{ThreadID: "thread", After: 3, Limit: 5})
			if err != nil || len(journal.Entries) != 5 || journal.Through != count {
				t.Fatalf("journal page = %+v, %v", journal, err)
			}
			bounded.allowedStart, bounded.allowedEnd = backendDomainEntryKey("thread", 8), backendDomainEntryKey("thread", 13)
			window, err := repo.PathMetadataWindow(t.Context(), PathWindowOptions{ThreadID: "thread", AnchorID: "entry-0010", Direction: PathWindowAround, Limit: 5})
			if err != nil || len(window.Entries) != 5 || window.FirstEntryID != "entry-0008" || window.LastEntryID != "entry-0012" {
				t.Fatalf("path window = %+v, %v", window, err)
			}
			// Exact interval scans and logarithmic ancestry remain bounded;
			// no request may scan the complete 1,024-entry journal.
			if bounded.scans != 2 {
				t.Fatalf("bounded scans=%d", bounded.scans)
			}
			if bounded.gets > 160 {
				t.Fatalf("bounded pages read %d records", bounded.gets)
			}
			if repo.threadMemory != nil {
				t.Fatal("direct pages rebuilt the complete journal")
			}
		})
	}
}

func TestV32CanonicalWindowRejectsInactiveBranch(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"root", "old-branch"} {
		if _, err := memory.Append(t.Context(), Entry{ID: id, ThreadID: "thread", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: id}}, AppendOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	_, repo := openV31ReviewFixture(t, memory)
	if err := repo.PrepareThread(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	if err := repo.MoveLeaf(t.Context(), "thread", "root"); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Append(t.Context(), Entry{ID: "new-branch", ThreadID: "thread", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "new"}}, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	_, err := repo.PathMetadataWindow(t.Context(), PathWindowOptions{ThreadID: "thread", LeafID: "old-branch", Direction: PathWindowBefore, Limit: 5})
	if !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("inactive window = %v", err)
	}
	entry, err := repo.Entry(t.Context(), "thread", "old-branch")
	if err != nil || entry.Message.Content != "old-branch" {
		t.Fatalf("immutable inactive entry was lost: %+v, %v", entry, err)
	}
}

func TestV32CanonicalBodyReadsSurviveSnapshotEviction(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	body := strings.Repeat("界🌸", 4096)
	if _, err := memory.Append(t.Context(), Entry{ID: "entry", ThreadID: "thread", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: body}}, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	backend, repo := openV31ReviewFixture(t, memory)
	if err := repo.PrepareThread(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Thread(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	repo.threadMemoryID, repo.threadMemory = "", nil
	repo.backend = &boundedPageBackend{Backend: backend}
	metadata, err := repo.CanonicalBodyMetadata(t.Context(), "thread", "entry", "content")
	if err != nil || metadata.Bytes != len(body) || metadata.SHA256 != stableHash(body) {
		t.Fatalf("metadata = %+v, %v", metadata, err)
	}
	// Body reads need only the requested immutable blocks, never a journal scan.
	repo.backend = noScanCanonicalBackend{backend}
	chunk, err := repo.ReadCanonicalBody(t.Context(), "thread", "entry", "content", 7, 20)
	if err != nil || chunk.Text != body[7:24] || chunk.Bytes != len(body) {
		t.Fatalf("body chunk = %+v, %v", chunk, err)
	}
	if repo.threadMemory != nil {
		t.Fatal("body read hydrated the thread journal")
	}
}

type noScanCanonicalBackend struct{ spi.Backend }

func (backend noScanCanonicalBackend) View(ctx context.Context, read func(spi.ReadTx) error) error {
	return backend.Backend.View(ctx, func(tx spi.ReadTx) error { return read(noScanCanonicalTx{tx}) })
}

type noScanCanonicalTx struct{ spi.ReadTx }

func (tx noScanCanonicalTx) Scan(spi.ScanRequest) (spi.ScanPage, error) {
	return spi.ScanPage{}, errors.New("canonical body read scanned the journal")
}

func TestV32BulkChangesMaintainLocatorInventory(t *testing.T) {
	backend := newMigrationTestBackend()
	before := newMemoryRepo(time.Now)
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return saveCompleteBackendDomain(tx, before, backendDomainV32Format) }); err != nil {
		t.Fatal(err)
	}
	after := newMemoryRepo(time.Now)
	if _, err := after.CreateThread(t.Context(), ThreadMeta{ID: "empty"}); err != nil {
		t.Fatal(err)
	}
	if _, err := after.CreateThread(t.Context(), ThreadMeta{ID: "populated"}); err != nil {
		t.Fatal(err)
	}
	if _, err := after.Append(t.Context(), Entry{ID: "entry", ThreadID: "populated", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "retained"}}, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		_, err := persistBackendDomainChangesContext(t.Context(), tx, before, after, backendDomainV32Format)
		if err != nil {
			return err
		}
		return verifyThreadEntryLocators(t.Context(), tx, after)
	}); err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		_, err := persistBackendDomainChangesContext(t.Context(), tx, after, before, backendDomainV32Format)
		if err != nil {
			return err
		}
		page, err := tx.Scan(spi.ScanRequest{Namespace: backendEntryIndexNamespace, Limit: 1})
		if err != nil {
			return err
		}
		if len(page.Records) != 0 {
			return errors.New("deleted thread retained canonical locators")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

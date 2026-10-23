package sessiontree

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

type mediaMutationBackend struct {
	spi.Backend
	afterScan func()
}

func (backend *mediaMutationBackend) View(ctx context.Context, read func(spi.ReadTx) error) error {
	journal := false
	err := backend.Backend.View(ctx, func(tx spi.ReadTx) error { return read(mediaMutationTx{tx, &journal}) })
	if journal && backend.afterScan != nil {
		mutate := backend.afterScan
		backend.afterScan = nil
		mutate()
	}
	return err
}

type mediaMutationTx struct {
	spi.ReadTx
	journal *bool
}

func (tx mediaMutationTx) Scan(request spi.ScanRequest) (spi.ScanPage, error) {
	if request.Namespace == backendDomainV32Namespace && bytes.HasPrefix(request.Start, backendDomainKey(backendDomainRecordEntry)) {
		*tx.journal = true
	}
	return tx.ReadTx.Scan(request)
}

func TestCanonicalMediaInventoryRejectsConcurrentChanges(t *testing.T) {
	for _, change := range []string{"append", "create", "delete", "cancel"} {
		t.Run(change, func(t *testing.T) {
			memory := newMemoryRepo(time.Now)
			if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
				t.Fatal(err)
			}
			entry := Entry{ID: "entry", ThreadID: "thread", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "source"}}
			if _, err := memory.Append(t.Context(), entry, AppendOptions{}); err != nil {
				t.Fatal(err)
			}
			backend, repo := openV31ReviewFixture(t, memory)
			for {
				more, err := repo.MigrateNextThread(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				if !more {
					break
				}
			}
			var err error
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			repo.backend = &mediaMutationBackend{Backend: backend, afterScan: func() {
				// This read must complete between journal pages, proving retention
				// owns neither the repository mutex nor the backend transaction.
				if _, err := repo.Thread(t.Context(), "thread"); err != nil {
					t.Fatal(err)
				}
				switch change {
				case "append":
					entry.ID = "second"
					_, err = repo.Append(t.Context(), entry, AppendOptions{})
				case "create":
					_, err = repo.CreateThread(t.Context(), ThreadMeta{ID: "new"})
				case "delete":
					_, err = repo.DeleteRootTree(t.Context(), "thread")
				case "cancel":
					cancel()
				}
				if err != nil {
					t.Fatal(err)
				}
			}}
			entries, err := repo.CanonicalMediaEntries(ctx)
			want := spi.ErrConflict
			if change == "cancel" {
				want = context.Canceled
			}
			if !errors.Is(err, want) || errors.Is(err, ErrAuthorityCorrupt) || len(entries) != 0 {
				t.Fatalf("inventory=%d error=%v, want %v", len(entries), err, want)
			}
		})
	}
}

func TestCanonicalMediaInventoryRejectsUnpublishedHistories(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "old"}); err != nil {
		t.Fatal(err)
	}
	_, repo := openV31ReviewFixture(t, memory)
	if _, err := repo.CanonicalMediaEntries(t.Context()); !errors.Is(err, spi.ErrConflict) {
		t.Fatalf("unpublished inventory=%v", err)
	}
}

type mediaMetadataBackend struct{ spi.Backend }

func (b mediaMetadataBackend) View(ctx context.Context, read func(spi.ReadTx) error) error {
	return b.Backend.View(ctx, func(tx spi.ReadTx) error { return read(mediaMetadataTx{tx}) })
}

type mediaMetadataTx struct{ spi.ReadTx }

func (tx mediaMetadataTx) Get(ns string, key []byte) ([]byte, error) {
	value, err := tx.ReadTx.Get(ns, key)
	if ns == canonicalBodyNamespace && len(value) > canonicalBodyReadBytes {
		return nil, errors.New("retention exceeded bounded body read")
	}
	return value, err
}
func TestCanonicalMediaInventoryStreamsIntegrityAndRetainsOnlyMedia(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "media"}); err != nil {
		t.Fatal(err)
	}
	if _, err := memory.Append(t.Context(), Entry{ID: "message", ThreadID: "media", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "body must not remain in memory", Attachments: []session.MessageAttachment{{ResourceRef: "image://retained", Name: "image.png", MIMEType: "image/png", SizeBytes: 1}}}}, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	backend, repo := openV31ReviewFixture(t, memory)
	if err := repo.PrepareThread(t.Context(), "media"); err != nil {
		t.Fatal(err)
	}
	for {
		more, err := repo.MigrateNextThread(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if !more {
			break
		}
	}
	repo.threadMemory = nil
	repo.verifiedThreadLeaves = nil
	repo.backend = mediaMetadataBackend{backend}
	for _, read := range []func(context.Context) ([]Entry, error){repo.CanonicalMediaEntries, func(ctx context.Context) ([]Entry, error) { return repo.CanonicalMediaEntriesForThread(ctx, "media") }} {
		entries, err := read(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 1 || len(entries[0].Message.Attachments) != 1 || entries[0].Message.Content != "" {
			t.Fatalf("retention entries=%+v", entries)
		}
		if repo.threadMemory != nil {
			t.Fatal("media inventory hydrated a journal")
		}
	}
	// Losing the last reference must not turn a corrupt source into a safe
	// empty inventory, even when its serialized envelope remains valid.
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		key := backendDomainEntryKey("media", 0)
		raw, err := tx.Get(backendDomainV32Namespace, key)
		if err != nil {
			return err
		}
		record, err := decodeBackendDomainRecord(backendDomainV32Format, raw)
		if err != nil {
			return err
		}
		stored, err := decodeCanonicalStoredEntry(backendDomainV32Format, record)
		if err != nil {
			return err
		}
		stored.Entry.Message.Attachments = nil
		raw, err = encodeBackendDomainRecord(backendDomainV32Format, record.Kind, record.ID, record.ThreadID, record.Ordinal, stored)
		if err != nil {
			return err
		}
		return tx.Put(backendDomainV32Namespace, key, raw)
	}); err != nil {
		t.Fatal(err)
	}
	if entries, err := repo.CanonicalMediaEntries(t.Context()); !errors.Is(err, ErrAuthorityCorrupt) || len(entries) != 0 {
		t.Fatalf("corrupt retention source: entries=%d err=%v", len(entries), err)
	}
}

type cancelCanonicalBackend struct {
	spi.Backend
	cancel context.CancelFunc
}

func (b cancelCanonicalBackend) View(ctx context.Context, read func(spi.ReadTx) error) error {
	return b.Backend.View(ctx, func(tx spi.ReadTx) error { b.cancel(); return read(tx) })
}
func TestCanonicalReadCancellationIsNotCorruption(t *testing.T) {
	backend := newMigrationTestBackend()
	repo, err := NewBackendRepo(t.Context(), backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	repo.backend = cancelCanonicalBackend{backend, cancel}
	_, err = repo.Thread(ctx, "thread")
	if !errors.Is(err, context.Canceled) || errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("cancelled read classified as %v", err)
	}
}

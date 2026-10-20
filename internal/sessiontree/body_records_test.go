package sessiontree

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/storagecodec"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestCanonicalBodyChunkReadsPreserveExactBytes(t *testing.T) {
	for _, size := range []int{40 << 10, 400 << 10, 4 << 20} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			backend := newMigrationTestBackend()
			text := strings.Repeat("a中文🌸é\r\n", size/14+1)
			var ref canonicalBodyRef
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				var err error
				ref, err = retainCanonicalBody(t.Context(), tx, text)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
				limits := []int{16384}
				if size == 40<<10 {
					limits = []int{4, 17, 16384}
				}
				for _, limit := range limits {
					hash := sha256.New()
					for offset := 0; offset < ref.Bytes; {
						chunk, err := readCanonicalBodyText(t.Context(), tx, ref, offset, limit)
						if err != nil {
							return err
						}
						if len(chunk) == 0 || len(chunk) > limit || !utf8.ValidString(chunk) {
							t.Fatalf("invalid chunk at %d", offset)
						}
						hash.Write([]byte(chunk))
						offset += len(chunk)
					}
					if fmt.Sprintf("%x", hash.Sum(nil)) != ref.SHA256 {
						t.Fatal("chunk hash mismatch")
					}
				}
				got, err := materializeCanonicalBody(t.Context(), tx, ref)
				if err != nil {
					return err
				}
				if got != text {
					t.Fatal("materialization changed bytes")
				}
				for _, input := range [][2]int{{-1, 4}, {1, 3}, {ref.Bytes + 1, 4}, {0, canonicalBodyBlockBytes + 1}} {
					if _, err := readCanonicalBodyText(t.Context(), tx, ref, input[0], input[1]); !errors.Is(err, spi.ErrInvalidArgument) {
						t.Fatalf("invalid read accepted: %v %v", input, err)
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func installV29BodySource(t *testing.T, backend *migrationTestBackend) *MemoryRepo {
	t.Helper()
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		if _, err := memory.Append(t.Context(), Entry{ThreadID: "thread", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: strings.Repeat("long 中文🌸\r\n", 5000)}}, AppendOptions{ID: fmt.Sprint(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := memory.Fork(t.Context(), ForkOptions{SourceThreadID: "thread", NewThreadID: "fork", Now: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return saveCompleteBackendDomainV29(tx, memory) }); err != nil {
		t.Fatal(err)
	}
	return memory
}

func TestBackendDomainV29ToV30PreservesFactsAndSharesBodies(t *testing.T) {
	for _, populated := range []bool{false, true} {
		t.Run(fmt.Sprint(populated), func(t *testing.T) {
			backend := newMigrationTestBackend()
			before := newMemoryRepo(time.Now)
			if populated {
				before = installV29BodySource(t, backend)
			}
			expected, err := before.EncodeMemoryState()
			if err != nil {
				t.Fatal(err)
			}
			repo, err := NewBackendRepo(t.Context(), backend, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			actual, err := repo.domainMemory.EncodeMemoryState()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(expected, actual) {
				t.Fatal("canonical facts changed")
			}
			if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
				if _, _, err := loadBackendDomainV29(t.Context(), tx, time.Now); err == nil {
					t.Fatal("v28 reader accepted upgraded store")
				}
				if populated {
					entry := before.entries["thread"][0]
					ref := canonicalBodyRef{SHA256: stableHash(entry.Message.Content), Bytes: len(entry.Message.Content)}
					body, err := readCanonicalBodyRecord(tx, ref)
					if err != nil {
						return err
					}
					if body.References != 6 {
						t.Fatalf("shared body reference count %d", body.References)
					}
					raw, err := tx.Get(backendDomainV30Namespace, backendDomainEntryKey("thread", 0))
					if err != nil {
						return err
					}
					if bytes.Contains(raw, []byte("long ")) || len(raw) > 4096 {
						t.Fatal("entry retained full body")
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			current := cloneMigrationRecords(backend.records)
			if _, err := NewBackendRepo(t.Context(), backend, time.Now); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(current, backend.records) {
				t.Fatal("current startup rewrote bytes")
			}
			if populated {
				if _, err := repo.DeleteRootTree(t.Context(), "thread"); err != nil {
					t.Fatal(err)
				}
				if _, err := NewBackendRepo(t.Context(), backend, time.Now); err != nil {
					t.Fatal(err)
				}
				if _, err := repo.DeleteRootTree(t.Context(), "fork"); err != nil {
					t.Fatal(err)
				}
				if len(backend.records[canonicalBodyNamespace]) != 0 {
					t.Fatal("deleted bodies retained")
				}
			}
		})
	}
}

type bodyFailureBackend struct {
	spi.Backend
	mode   string
	cancel context.CancelFunc
}
type bodyFailureTx struct {
	spi.WriteTx
	owner  bodyFailureBackend
	writes int
}

func (backend bodyFailureBackend) Update(ctx context.Context, write func(spi.WriteTx) error) error {
	return backend.Backend.Update(ctx, func(tx spi.WriteTx) error { return write(&bodyFailureTx{WriteTx: tx, owner: backend}) })
}
func (tx *bodyFailureTx) Put(namespace string, key, value []byte) error {
	if namespace == canonicalBodyNamespace {
		tx.writes++
		if tx.writes == 3 {
			switch tx.owner.mode {
			case "panic":
				panic("injected body write panic")
			case "cancel":
				tx.owner.cancel()
			case "write":
				return errors.New("injected body write failure")
			}
		}
	}
	return tx.WriteTx.Put(namespace, key, value)
}

func TestBackendDomainV29ToV30RollsBackPartialBodies(t *testing.T) {
	for _, mode := range []string{"write", "cancel", "panic", "source_drift", "future"} {
		t.Run(mode, func(t *testing.T) {
			backend := newMigrationTestBackend()
			memory := installV29BodySource(t, backend)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var target spi.Backend = bodyFailureBackend{Backend: backend, mode: mode, cancel: cancel}
			if mode == "source_drift" {
				entry := memory.entries["thread"][0]
				entry.RawHash = "corrupt"
				raw, err := encodeBackendDomainRecord(backendDomainV29Format, backendDomainRecordEntry, entry.ID, "thread", 0, entry)
				if err != nil {
					t.Fatal(err)
				}
				backend.records[backendDomainV29Namespace][string(backendDomainEntryKey("thread", 0))] = raw
			}
			if mode == "future" {
				format := backendDomainV30Format
				format.version++
				raw, err := encodeBackendDomainRecord(format, backendDomainRecordManifest, "", "", 0, backendDomainManifest{Version: format.version})
				if err != nil {
					t.Fatal(err)
				}
				backend.records[format.namespace][string(backendDomainKey(backendDomainRecordManifest))] = raw
			}
			before := cloneMigrationRecords(backend.records)
			func() {
				if mode == "panic" {
					defer func() {
						if recover() == nil {
							t.Error("expected panic")
						}
					}()
				}
				if _, err := NewBackendRepo(ctx, target, time.Now); err == nil {
					t.Error("failed migration accepted")
				}
			}()
			if !reflect.DeepEqual(before, backend.records) {
				t.Fatal("failed migration mutated durable bytes")
			}
		})
	}
}

func TestCanonicalBodiesRejectCorruptionWithoutMutation(t *testing.T) {
	for _, mode := range []string{"missing", "wrong_hash", "extra_block", "orphan", "references", "inline", "unknown_field"} {
		t.Run(mode, func(t *testing.T) {
			backend := newMigrationTestBackend()
			memory := installV29BodySource(t, backend)
			if _, err := NewBackendRepo(t.Context(), backend, time.Now); err != nil {
				t.Fatal(err)
			}
			text := memory.entries["thread"][0].Message.Content
			ref := canonicalBodyRef{SHA256: stableHash(text), Bytes: len(text)}
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				switch mode {
				case "missing":
					return tx.Delete(canonicalBodyNamespace, canonicalBodyBlockKey(ref.SHA256, 1))
				case "wrong_hash":
					raw, err := storagecodec.EncodeEnvelope("sessiontree-v30-body-block", []byte("\"aGVsbG8=\""))
					if err != nil {
						return err
					}
					return tx.Put(canonicalBodyNamespace, canonicalBodyBlockKey(ref.SHA256, 0), raw)
				case "extra_block":
					return tx.Put(canonicalBodyNamespace, canonicalBodyBlockKey(ref.SHA256, 100), []byte("extra"))
				case "orphan":
					_, err := retainCanonicalBody(t.Context(), tx, "orphan body")
					return err
				case "references":
					body, err := readCanonicalBodyRecord(tx, ref)
					if err != nil {
						return err
					}
					body.References++
					return writeCanonicalBodyRecord(tx, body)
				case "inline", "unknown_field":
					raw, err := tx.Get(backendDomainV30Namespace, backendDomainEntryKey("thread", 0))
					if err != nil {
						return err
					}
					record, err := decodeBackendDomainRecord(backendDomainV30Format, raw)
					if err != nil {
						return err
					}
					stored, err := decodeCanonicalStoredEntry(record)
					if err != nil {
						return err
					}
					if mode == "inline" {
						stored.Entry.Message.Content = "inline body"
					} else {
						stored.Fields["unsupported"] = ref
					}
					raw, err = encodeBackendDomainRecord(backendDomainV30Format, record.Kind, record.ID, record.ThreadID, record.Ordinal, stored)
					if err != nil {
						return err
					}
					return tx.Put(backendDomainV30Namespace, backendDomainEntryKey("thread", 0), raw)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			before := cloneMigrationRecords(backend.records)
			if _, err := NewBackendRepo(t.Context(), backend, time.Now); !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("corruption accepted: %v", err)
			}
			if !reflect.DeepEqual(before, backend.records) {
				t.Fatal("corruption recovery changed durable bytes")
			}
		})
	}
}

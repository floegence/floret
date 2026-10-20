package sessiontree

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/tools"
)

func installV30ActivitySource(t *testing.T, backend *migrationTestBackend) (*MemoryRepo, string) {
	t.Helper()
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	activity := &tools.ActivityPresentation{Label: "Inspect file", Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Operation: "read", Status: "success", Rows: []tools.StructuredActivityRow{{Title: "Source", Content: strings.Repeat("line 中文🌸\r\n", 3000), Format: tools.StructuredActivityRowFormatCode}}}}
	raw, err := json.Marshal(activity)
	if err != nil {
		t.Fatal(err)
	}
	entry := Entry{ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "read", ToolCallID: "tool", Content: "result", Activity: activity}}
	if _, err := memory.Append(t.Context(), entry, AppendOptions{ID: "result"}); err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		return saveCompleteBackendDomainContext(t.Context(), tx, memory, backendDomainV30Format)
	}); err != nil {
		t.Fatal(err)
	}
	return memory, string(raw)
}

func TestBackendDomainV30ToV31MovesOnlyActivityStorage(t *testing.T) {
	backend := newMigrationTestBackend()
	before, source := installV30ActivitySource(t, backend)
	expected, err := before.EncodeMemoryState()
	if err != nil {
		t.Fatal(err)
	}
	repo, err := NewBackendRepo(t.Context(), backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := repo.domainMemory.EncodeMemoryState()
	if err != nil || !bytes.Equal(expected, actual) {
		t.Fatalf("canonical facts changed: %v", err)
	}
	retained := repo.domainMemory.entries["thread"][0]
	if retained.Message.Activity != nil || retained.bodyFields["activity_json"].Bytes != len(source) {
		t.Fatal("journal retained activity bytes")
	}
	meta, err := repo.PathMetadataWindow(t.Context(), PathWindowOptions{ThreadID: "thread", Direction: PathWindowBefore, Limit: 1})
	if err != nil || len(meta.Entries) != 1 || meta.Entries[0].Entry.Message.Activity != nil {
		t.Fatalf("metadata read retained activity: %v", err)
	}
	hash := sha256.New()
	for offset := 0; offset < len(source); {
		part, err := repo.ReadCanonicalBody(t.Context(), "thread", "result", "activity_json", offset, 16384)
		if err != nil || part.Text == "" {
			t.Fatalf("body read at %d: %v", offset, err)
		}
		hash.Write([]byte(part.Text))
		offset += len(part.Text)
	}
	if fmt.Sprintf("%x", hash.Sum(nil)) != stableHash(source) {
		t.Fatal("activity chunk hash changed")
	}
	frozen := cloneMigrationRecords(backend.records)
	if _, err := NewBackendRepo(t.Context(), backend, time.Now); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(frozen, backend.records) {
		t.Fatal("current startup rewrote canonical bytes")
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		if _, _, err := loadBackendDomain(t.Context(), tx, time.Now, backendDomainV30Format); err == nil {
			t.Fatal("old reader accepted upgraded format")
		}
		raw, err := tx.Get(backendDomainV31Namespace, backendDomainEntryKey("thread", 0))
		if err != nil {
			return err
		}
		if len(raw) > 4096 || bytes.Contains(raw, []byte("Source")) {
			t.Fatal("stored entry retained activity payload")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.DeleteRootTree(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	if len(backend.records[canonicalBodyNamespace]) != 0 {
		t.Fatal("deleted activity blocks retained")
	}
}

func TestBackendDomainV30ToV31RollsBackEveryFailure(t *testing.T) {
	for _, mode := range []string{"write", "cancel", "panic", "source_drift", "future"} {
		t.Run(mode, func(t *testing.T) {
			backend := newMigrationTestBackend()
			_, _ = installV30ActivitySource(t, backend)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var target spi.Backend = bodyFailureBackend{Backend: backend, mode: mode, cancel: cancel}
			if mode == "source_drift" || mode == "future" {
				if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
					key := backendDomainKey(backendDomainRecordManifest)
					if mode == "source_drift" {
						return tx.Put(canonicalBodyNamespace, canonicalBodyBlockKey(stableHash("result"), 0), []byte("corrupt"))
					}
					format := backendDomainV31Format
					format.version++
					raw, err := encodeBackendDomainRecord(format, backendDomainRecordManifest, "", "", 0, backendDomainManifest{Version: format.version})
					if err != nil {
						return err
					}
					return tx.Put(format.namespace, key, raw)
				}); err != nil {
					t.Fatal(err)
				}
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
				t.Fatal("failed migration changed durable facts")
			}
		})
	}
}

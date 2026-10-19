package sessiontree

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
)

func historicalCompactionBackend(t *testing.T) *migrationTestBackend {
	t.Helper()
	return historicalCompactionBackendAt(t, filepath.Join("testdata", "compaction-v7.68.2"))
}

func historicalCompactionBackendAt(t *testing.T, root string) *migrationTestBackend {
	t.Helper()
	var manifest struct{ Files map[string]string }
	raw, err := os.ReadFile(filepath.Join(root, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	for name, expected := range manifest.Files {
		data, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(data)
		if hex.EncodeToString(sum[:]) != expected {
			t.Fatalf("historical fixture %s checksum changed", name)
		}
	}
	data, err := os.ReadFile(filepath.Join(root, "history.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	copyPath := filepath.Join(t.TempDir(), "history.sqlite")
	if err := os.WriteFile(copyPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	source, err := storagebridge.Open(t.Context(), storagebridge.Source(storage.SQLite(copyPath)))
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	backend := newMigrationTestBackend()
	if err := source.View(t.Context(), func(tx spi.ReadTx) error {
		records, err := scanBackendDomainV26(t.Context(), tx)
		if err != nil {
			return err
		}
		return backend.Update(t.Context(), func(target spi.WriteTx) error {
			for _, record := range records {
				if err := target.Put(backendDomainV26Namespace, record.Key, record.Value); err != nil {
					return err
				}
			}
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
	return backend
}

func TestHistoricalReleasedCompactionWriterMigratesEveryCheckpoint(t *testing.T) {
	roots, err := filepath.Glob(filepath.Join("testdata", "compaction-v*"))
	if err != nil || len(roots) < 2 {
		t.Fatalf("released writer fixtures missing: %v", err)
	}
	for _, root := range roots {
		t.Run(filepath.Base(root), func(t *testing.T) { verifyHistoricalCompactionBackend(t, historicalCompactionBackendAt(t, root)) })
	}
}

func verifyHistoricalCompactionBackend(t *testing.T, backend *migrationTestBackend) {
	var before *MemoryRepo
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		var err error
		source, err := detectBackendDomainSource(t.Context(), tx)
		if err != nil {
			return err
		}
		if source == backendDomainSourceV24 {
			before, _, err = loadBackendDomainV24(t.Context(), tx, time.Now)
			if err != nil {
				return err
			}
			if err := migrateBackendDomainV24ToV25(t.Context(), before); err != nil {
				return err
			}
			return migrateBackendDomainV25ToV26(t.Context(), before)
		}
		before, _, err = loadBackendDomainV26(t.Context(), tx, time.Now)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	repo, err := NewBackendRepo(t.Context(), backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	checkpoints := 0
	for threadID, entries := range before.entries {
		after := repo.domainMemory.entries[threadID]
		if len(entries) != len(after) {
			t.Fatalf("changed entry count in %s", threadID)
		}
		for i, entry := range entries {
			got := cloneEntry(after[i])
			if got.Type == EntryCompaction {
				checkpoints++
				if got.FirstKeptEntryID == "" || got.CompactedThroughEntryID == "" {
					t.Fatal("unresolved boundary")
				}
				got.FirstKeptEntryID, got.CompactedThroughEntryID, got.KeptUserEntryIDs = entry.FirstKeptEntryID, entry.CompactedThroughEntryID, entry.KeptUserEntryIDs
				got.Raw, got.RawHash = entry.Raw, entry.RawHash
			}
			if !reflect.DeepEqual(entry, got) {
				t.Fatalf("migration changed unrelated fact %s", entry.ID)
			}
		}
		path, err := repo.Path(t.Context(), threadID, "")
		if err != nil {
			t.Fatal(err)
		}
		messages, err := BuildContextChecked(path, ContextOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if err := session.ValidateToolHistory(messages); err != nil {
			t.Fatal(err)
		}
		if len(messages) < 4 {
			t.Fatalf("lost retained context in %s", threadID)
		}
		if _, err := repo.Fork(t.Context(), ForkOptions{SourceThreadID: threadID, NewThreadID: threadID + "-current-fork"}); err != nil {
			t.Fatal(err)
		}
	}
	if checkpoints != 6 {
		t.Fatalf("checkpoints=%d, want 6 across source, fork and nested fork", checkpoints)
	}
	canonical := cloneMigrationRecords(backend.records)
	if _, err := NewBackendRepo(t.Context(), backend, time.Now); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(canonical, backend.records) {
		t.Fatal("restart rewrote canonical bytes")
	}
}

func TestBackendDomainV26ToV27RollsBack(t *testing.T) {
	for _, failure := range []string{"write", "cancel", "panic", "missing_authority", "count_drift", "wrong_run", "future"} {
		t.Run(failure, func(t *testing.T) {
			backend := historicalCompactionBackend(t)
			ctx := t.Context()
			var target spi.Backend = backend
			switch failure {
			case "write":
				target = migrationFailingBackend{Backend: backend, err: errors.New("injected")}
			case "cancel":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "panic":
				target = migrationPanickingBackend{Backend: backend}
			case "future":
				format := backendDomainV27Format
				format.version++
				raw, err := encodeBackendDomainRecord(format, backendDomainRecordManifest, "", "", 0, backendDomainManifest{Version: format.version})
				if err != nil {
					t.Fatal(err)
				}
				if err := backend.Update(ctx, func(tx spi.WriteTx) error {
					return tx.Put(format.namespace, backendDomainKey(backendDomainRecordManifest), raw)
				}); err != nil {
					t.Fatal(err)
				}
			default:
				if err := backend.Update(ctx, func(tx spi.WriteTx) error {
					memory, _, err := loadBackendDomainV26(ctx, tx, time.Now)
					if err != nil {
						return err
					}
					for i := range memory.entries["thread"] {
						entry := &memory.entries["thread"][i]
						if entry.Type != EntryCompaction {
							continue
						}
						switch failure {
						case "missing_authority":
							entry.FirstKeptEntryID = "missing"
						case "count_drift":
							entry.Metadata["retained_tail_messages"] = "999"
						case "wrong_run":
							entry.CompactedThroughEntryID = "other:entry:dc782fdc64ee5ee4"
						}
						*entry = PrepareEntry(*entry)
						break
					}
					return saveCompleteBackendDomainV26(tx, memory)
				}); err != nil {
					t.Fatal(err)
				}
			}
			before := cloneMigrationRecords(backend.records)
			func() {
				if failure == "panic" {
					defer func() {
						if recover() == nil {
							t.Error("expected panic")
						}
					}()
				}
				if _, err := NewBackendRepo(ctx, target, time.Now); err == nil {
					t.Error("invalid startup succeeded")
				}
			}()
			if !reflect.DeepEqual(before, backend.records) {
				t.Fatal("failed migration changed source bytes")
			}
		})
	}
}

func TestHistoricalCompactionResolvesPhysicalControlResultAuthority(t *testing.T) {
	call := Entry{ID: "control", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, Kind: session.MessageKindControlSignal, ToolCallID: "call", ToolName: "continue", ControlSignal: &session.ControlSignalView{Name: "continue", CallID: "call", Disposition: "continue", OutputText: "saved continuation"}}}
	path := []Entry{{ID: "user", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "begin"}}, call, {ID: "physical-result", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolResult, Message: session.Message{Role: session.Tool, ToolCallID: "call", ToolName: "continue", Content: "saved continuation"}}, {ID: "tail", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "next"}}}
	checkpoint := Entry{ID: "checkpoint", Type: EntryCompaction, Summary: "Earlier work.", CompactedThroughEntryID: "physical-result", FirstKeptEntryID: "tail", Metadata: map[string]string{"compacted_messages": "3", "history_messages": "4", "retained_tail_messages": "1"}}
	repaired, err := migrateCompactionBoundaryV27(path, checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	if repaired.CompactedThroughEntryID != "control:result" {
		t.Fatalf("boundary=%s", repaired.CompactedThroughEntryID)
	}
	messages, err := BuildContextChecked(append(path, repaired), ContextOptions{})
	if err != nil || len(messages) != 2 || messages[1].EntryID != "tail" {
		t.Fatalf("messages=%+v err=%v", messages, err)
	}
}

func TestHistoricalCompactionRejectsAmbiguousStoredAlias(t *testing.T) {
	path := []Entry{{ID: "one", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "same", EntryID: "alias"}}, {ID: "two", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "same", EntryID: "alias"}}}
	_, err := migrateCompactionBoundaryV27(path, Entry{ID: "checkpoint", Type: EntryCompaction, Summary: "Earlier.", CompactedThroughEntryID: "alias"})
	if err == nil {
		t.Fatal("ambiguous historical source accepted")
	}
}

func TestHistoricalCompactionCanonicalEmptyTailNeedsNoGuessedCounts(t *testing.T) {
	path := []Entry{{ID: "one", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "begin"}}}
	checkpoint := Entry{ID: "checkpoint", Type: EntryCompaction, Summary: "Earlier.", CompactedThroughEntryID: "one", KeptUserEntryIDs: []string{"one"}}
	repaired, err := migrateCompactionBoundaryV27(path, checkpoint)
	if err != nil || !reflect.DeepEqual(repaired, checkpoint) {
		t.Fatalf("checkpoint=%+v err=%v", repaired, err)
	}
}

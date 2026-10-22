package sessiontree

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

func indexJournalForTest(t *testing.T, backend spi.Backend, sequence uint64) {
	t.Helper()
	for i := 0; i < 100; i++ {
		var checkpoint snapshotImportCheckpoint
		if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
			var err error
			checkpoint, err = indexSnapshotJournalBatch(t.Context(), tx, sequence)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if checkpoint.Complete {
			return
		}
	}
	t.Fatal("journal index did not finish")
}

func TestSnapshotJournalChecksumMatchesReleasedWriter(t *testing.T) {
	for _, patch := range []backendDomainJournalPatch{
		{},
		{Set: map[string]map[string]json.RawMessage{"threads": {"long-key": json.RawMessage(" { \"text\":\"<>&\u2028 中文\" } "), "a": json.RawMessage(" [ 1, true, null ] ")}, "empty": {}, "nil": nil}, Delete: map[string][]string{"threads": {"a", "<>&\u2028"}, "nil": nil, "empty": {}}, Replace: map[string]json.RawMessage{"sequence": json.RawMessage(" 10 "), "deleted": json.RawMessage("null")}},
		{Set: map[string]map[string]json.RawMessage{"entries": {"history": json.RawMessage("[\"" + strings.Repeat("preserved 中文 ", 100000) + "\"]")}}},
	} {
		backend := newMigrationTestBackend()
		checksum, err := backendDomainJournalChecksum(1, patch)
		if err != nil {
			t.Fatal(err)
		}
		frame := backendDomainJournalFrame{Version: 1, Sequence: 1, Patch: patch, Checksum: checksum}
		raw, err := encodeBackendDomainJournalFrame(frame)
		if err != nil {
			t.Fatal(err)
		}
		if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return tx.Put(backendDomainJournalNamespace, backendJournalKey(1), raw) }); err != nil {
			t.Fatal(err)
		}
		indexJournalForTest(t, backend, 1)
		before := cloneMigrationRecords(backend.records)
		reader := migrationSourceReader{ctx: t.Context(), backend: backend}
		if err := verifySnapshotJournalFrame(t.Context(), reader, 1); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(before, backend.records) {
			t.Fatal("verification changed source")
		}
		if len(raw) > backendThreadMigrationBatchBytes {
			changed := bytes.Replace(raw, []byte("preserved"), []byte("different"), 1)
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				return tx.Put(backendDomainJournalNamespace, backendJournalKey(1), changed)
			}); err != nil {
				t.Fatal(err)
			}
			if err := verifySnapshotJournalFrame(t.Context(), reader, 1); !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("changed frame accepted: %v", err)
			}
		}
	}
}

func TestSnapshotJournalIndexRejectsCorruptionAndPreservesSource(t *testing.T) {
	for _, raw := range []string{
		"{\"version\":1,\"sequence\":2,\"patch\":{},\"checksum\":\"" + strings.Repeat("0", 64) + "\"}",
		"{\"version\":1,\"sequence\":1,\"patch\":{\"unknown\":{}},\"checksum\":\"" + strings.Repeat("0", 64) + "\"}",
		"{\"version\":1,\"sequence\":1,\"patch\":{\"set\":{\"threads\":{},\"threads\":{}}},\"checksum\":\"" + strings.Repeat("0", 64) + "\"}",
	} {
		backend := newMigrationTestBackend()
		if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
			return tx.Put(backendDomainJournalNamespace, backendJournalKey(1), []byte(raw))
		}); err != nil {
			t.Fatal(err)
		}
		before := cloneMigrationRecords(backend.records)
		err := backend.Update(t.Context(), func(tx spi.WriteTx) error { _, err := indexSnapshotJournalBatch(t.Context(), tx, 1); return err })
		if !errors.Is(err, ErrAuthorityCorrupt) {
			t.Fatalf("invalid frame=%v", err)
		}
		if !reflect.DeepEqual(before, backend.records) {
			t.Fatal("failed scan changed source")
		}
	}
}

func TestSnapshotJournalIndexRollbackAndResume(t *testing.T) {
	backend := newMigrationTestBackend()
	patch := backendDomainJournalPatch{Replace: map[string]json.RawMessage{"sequence": json.RawMessage("1")}}
	checksum, _ := backendDomainJournalChecksum(1, patch)
	raw, _ := encodeBackendDomainJournalFrame(backendDomainJournalFrame{Version: 1, Sequence: 1, Patch: patch, Checksum: checksum})
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return tx.Put(backendDomainJournalNamespace, backendJournalKey(1), raw) }); err != nil {
		t.Fatal(err)
	}
	before := cloneMigrationRecords(backend.records)
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if err := backend.Update(cancelled, func(tx spi.WriteTx) error { _, err := indexSnapshotJournalBatch(cancelled, tx, 1); return err }); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel=%v", err)
	}
	if !reflect.DeepEqual(before, backend.records) {
		t.Fatal("cancelled scan wrote state")
	}
	indexJournalForTest(t, backend, 1)
	complete := cloneMigrationRecords(backend.records)
	indexJournalForTest(t, backend, 1)
	if !reflect.DeepEqual(complete, backend.records) {
		t.Fatal("completed frame rewrote state")
	}
}

func TestSnapshotClosureAppliesReleasedJournalInOrder(t *testing.T) {
	clock := func() time.Time { return time.Unix(1900000000, 0).UTC() }
	original := newMemoryRepo(clock)
	for _, id := range []string{"source", "removed", "unrelated"} {
		if _, err := original.CreateThread(t.Context(), ThreadMeta{ID: id}); err != nil {
			t.Fatal(err)
		}
	}
	base, err := original.EncodeMemoryState()
	if err != nil {
		t.Fatal(err)
	}
	backend, _ := installSnapshotImportFixture(t, base)
	final, err := DecodeMemoryState(base, clock)
	if err != nil {
		t.Fatal(err)
	}
	before := base
	for i := range 3 {
		if i == 0 {
			if _, err := final.CreateThread(t.Context(), ThreadMeta{ID: "new"}); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := final.Append(t.Context(), Entry{ThreadID: "source", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: fmt.Sprintf("committed journal fact %d 中文", i)}}, AppendOptions{ID: fmt.Sprint(i)}); err != nil {
			t.Fatal(err)
		}
		if _, err := final.SetThreadTitle(t.Context(), SetThreadTitleRequest{ThreadID: "source", Title: fmt.Sprintf("Title %d", i), Now: clock()}); err != nil {
			t.Fatal(err)
		}
		after, err := final.EncodeMemoryState()
		if err != nil {
			t.Fatal(err)
		}
		frame, changed, err := buildBackendDomainJournalFrame(uint64(i+1), before, after)
		if err != nil || !changed {
			t.Fatal(err)
		}
		raw, err := encodeBackendDomainJournalFrame(frame)
		if err != nil {
			t.Fatal(err)
		}
		if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
			return tx.Put(backendDomainJournalNamespace, backendJournalKey(uint64(i+1)), raw)
		}); err != nil {
			t.Fatal(err)
		}
		before = after
	}
	indexSnapshotForTest(t, backend)
	for i := uint64(1); i <= 3; i++ {
		indexJournalForTest(t, backend, i)
	}
	checkpoint, err := DecodeMemoryState(base, clock)
	if err != nil {
		t.Fatal(err)
	}
	oracle, _, err := replayBackendDomainJournal(t.Context(), migrationSourceReader{ctx: t.Context(), backend: backend}, checkpoint, clock)
	if err != nil {
		t.Fatal(err)
	}
	retained := cloneMigrationRecords(backend.records)
	reader := migrationSourceReader{ctx: t.Context(), backend: backend}
	for _, id := range []string{"source", "new", "unrelated"} {
		got, err := loadSnapshotThreadClosure(t.Context(), reader, id, clock)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got.threads[id], oracle.threads[id]) || !reflect.DeepEqual(got.entries[id], oracle.entries[id]) {
			t.Fatalf("%s differs from released replay: meta=%#v want=%#v entries=%#v want=%#v", id, got.threads[id], oracle.threads[id], got.entries[id], oracle.entries[id])
		}
	}
	if !reflect.DeepEqual(retained, backend.records) {
		t.Fatal("scoped replay mutated source")
	}
}

func TestSnapshotJournalRejectsSequenceGaps(t *testing.T) {
	for _, keys := range [][]string{{"00000000000000000002"}, {"00000000000000000000"}, {"00000000000000000001", "00000000000000000003"}, {"invalid"}} {
		backend := newMigrationTestBackend()
		for _, key := range keys {
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return tx.Put(backendDomainJournalNamespace, []byte(key), []byte("{}")) }); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := snapshotJournalSequences(t.Context(), migrationSourceReader{ctx: t.Context(), backend: backend}); !errors.Is(err, ErrAuthorityCorrupt) {
			t.Fatalf("keys=%v error=%v", keys, err)
		}
	}
}

func TestSnapshotJournalVerificationRejectsChangedWrapperKeys(t *testing.T) {
	backend := newMigrationTestBackend()
	patch := backendDomainJournalPatch{Replace: map[string]json.RawMessage{"sequence": json.RawMessage("1")}}
	checksum, _ := backendDomainJournalChecksum(1, patch)
	raw, _ := encodeBackendDomainJournalFrame(backendDomainJournalFrame{Version: 1, Sequence: 1, Patch: patch, Checksum: checksum})
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return tx.Put(backendDomainJournalNamespace, backendJournalKey(1), raw) }); err != nil {
		t.Fatal(err)
	}
	indexJournalForTest(t, backend, 1)
	changed := bytes.Replace(raw, []byte("replace"), []byte("invalid"), 1)
	if bytes.Equal(raw, changed) {
		t.Fatal("fixture did not change wrapper")
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		return tx.Put(backendDomainJournalNamespace, backendJournalKey(1), changed)
	}); err != nil {
		t.Fatal(err)
	}
	if err := verifySnapshotJournalFrame(t.Context(), migrationSourceReader{ctx: t.Context(), backend: backend}, 1); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("changed wrapper accepted: %v", err)
	}
}

func TestSnapshotRootInventoryRejectsMissingReorderedAndChangedHistory(t *testing.T) {
	for _, mode := range []string{"valid", "missing", "duplicate", "reordered", "path-drift"} {
		t.Run(mode, func(t *testing.T) {
			memory := newMemoryRepo(time.Now)
			for i, id := range []string{"one", "two"} {
				if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: id, CreatedAt: time.Unix(int64(i+1), 0).UTC()}); err != nil {
					t.Fatal(err)
				}
				if _, err := memory.Append(t.Context(), Entry{ThreadID: id, Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "retained fact"}}, AppendOptions{ID: "fact"}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := repairLegacyFallbackThreadTitles(memory); err != nil {
				t.Fatal(err)
			}
			payload, err := memory.EncodeMemoryState()
			if err != nil {
				t.Fatal(err)
			}
			backend, _ := installSnapshotImportFixture(t, payload)
			items, err := memory.rootThreadInventoryLocked()
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "missing":
				items = items[:1]
			case "duplicate":
				items[1] = items[0]
			case "reordered":
				items[0], items[1] = items[1], items[0]
			case "path-drift":
				items[0].Path[0].Message.Content = "changed fact"
				items[0].Path[0].Raw = rawForEntry(items[0].Path[0])
				items[0].Path[0].RawHash = stableHash(items[0].Path[0].Raw)
			}
			raw, err := encodeRootThreadInventoryItems(items)
			if err != nil {
				t.Fatal(err)
			}
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return tx.Put(backendDomainNamespace, backendRootThreadInventoryKey, raw) }); err != nil {
				t.Fatal(err)
			}
			indexSnapshotForTest(t, backend)
			for {
				var checkpoint snapshotImportCheckpoint
				if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
					var err error
					checkpoint, err = indexSnapshotRootsBatch(t.Context(), tx)
					return err
				}); err != nil {
					t.Fatal(err)
				}
				if checkpoint.Complete {
					break
				}
			}
			retained := cloneMigrationRecords(backend.records)
			source, err := newSnapshotEffectiveSource(t.Context(), migrationSourceReader{ctx: t.Context(), backend: backend}, 5)
			if err != nil {
				t.Fatal(err)
			}
			err = verifySnapshotRootInventory(t.Context(), source, time.Now)
			if mode == "valid" && err != nil {
				t.Fatal(err)
			}
			if mode != "valid" && !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("accepted %s: %v", mode, err)
			}
			if !reflect.DeepEqual(retained, backend.records) {
				t.Fatal("inventory verification changed source")
			}
		})
	}
}

func TestSnapshotClosureRejectsInvalidIntermediateJournalState(t *testing.T) {
	clock := func() time.Time { return time.Unix(1900000000, 0).UTC() }
	memory := newMemoryRepo(clock)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "source"}); err != nil {
		t.Fatal(err)
	}
	base, err := memory.EncodeMemoryState()
	if err != nil {
		t.Fatal(err)
	}
	backend, _ := installSnapshotImportFixture(t, base)
	valid, _ := json.Marshal(memory.threads["source"])
	invalidMeta := memory.threads["source"]
	invalidMeta.ParentThreadID = "missing-parent"
	invalid, _ := json.Marshal(invalidMeta)
	for i, value := range []json.RawMessage{invalid, valid} {
		patch := backendDomainJournalPatch{Set: map[string]map[string]json.RawMessage{"threads": {"source": value}}}
		sequence := uint64(i + 1)
		checksum, _ := backendDomainJournalChecksum(sequence, patch)
		raw, _ := encodeBackendDomainJournalFrame(backendDomainJournalFrame{Version: 1, Sequence: sequence, Patch: patch, Checksum: checksum})
		if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
			return tx.Put(backendDomainJournalNamespace, backendJournalKey(sequence), raw)
		}); err != nil {
			t.Fatal(err)
		}
	}
	reader := migrationSourceReader{ctx: t.Context(), backend: backend}
	if _, _, err := replayBackendDomainJournal(t.Context(), reader, memory, clock); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("released oracle accepted invalid intermediate state: %v", err)
	}
	indexSnapshotForTest(t, backend)
	indexJournalForTest(t, backend, 1)
	indexJournalForTest(t, backend, 2)
	if _, err := loadSnapshotThreadClosure(t.Context(), reader, "source", clock); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("scoped replay accepted invalid intermediate state: %v", err)
	}
}

func TestSnapshotJournalReplacementAndDeletionMatchReleasedReplay(t *testing.T) {
	clock := func() time.Time { return time.Unix(1900000000, 0).UTC() }
	memory := newMemoryRepo(clock)
	for _, id := range []string{"retained", "removed"} {
		if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: id}); err != nil {
			t.Fatal(err)
		}
	}
	base, err := memory.EncodeMemoryState()
	if err != nil {
		t.Fatal(err)
	}
	backend, _ := installSnapshotImportFixture(t, base)
	retained, _ := json.Marshal(memory.threads["retained"])
	removed, _ := json.Marshal(memory.threads["removed"])
	threads, _ := json.Marshal(map[string]json.RawMessage{"retained": retained, "removed": removed})
	patch := backendDomainJournalPatch{
		Replace: map[string]json.RawMessage{"threads": threads, "sequence": json.RawMessage("42")},
		Set:     map[string]map[string]json.RawMessage{"threads": {"removed": removed}},
		Delete:  map[string][]string{"threads": {"removed"}},
	}
	checksum, _ := backendDomainJournalChecksum(1, patch)
	raw, _ := encodeBackendDomainJournalFrame(backendDomainJournalFrame{Version: 1, Sequence: 1, Patch: patch, Checksum: checksum})
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return tx.Put(backendDomainJournalNamespace, backendJournalKey(1), raw) }); err != nil {
		t.Fatal(err)
	}
	indexSnapshotForTest(t, backend)
	indexJournalForTest(t, backend, 1)
	reader := migrationSourceReader{ctx: t.Context(), backend: backend}
	oracle, _, err := replayBackendDomainJournal(t.Context(), reader, memory, clock)
	if err != nil {
		t.Fatal(err)
	}
	got, err := loadSnapshotThreadClosure(t.Context(), reader, "retained", clock)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.threads["retained"], oracle.threads["retained"]) || got.seq != oracle.seq {
		t.Fatal("replace/set/delete replay differs from released decoder")
	}
	if _, err := loadSnapshotThreadClosure(t.Context(), reader, "removed", clock); !errors.Is(err, ErrThreadNotFound) {
		t.Fatalf("deleted member remains visible: %v", err)
	}
}

func TestSnapshotJournalRejectsUnrelatedTypedValueBeforeDeletion(t *testing.T) {
	clock := func() time.Time { return time.Unix(1900000000, 0).UTC() }
	memory := newMemoryRepo(clock)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "retained"}); err != nil {
		t.Fatal(err)
	}
	base, err := memory.EncodeMemoryState()
	if err != nil {
		t.Fatal(err)
	}
	backend, _ := installSnapshotImportFixture(t, base)
	patches := []backendDomainJournalPatch{
		{Set: map[string]map[string]json.RawMessage{"todos": {"unrelated": json.RawMessage("true")}}},
		{Delete: map[string][]string{"todos": {"unrelated"}}},
	}
	for index, patch := range patches {
		sequence := uint64(index + 1)
		checksum, _ := backendDomainJournalChecksum(sequence, patch)
		raw, _ := encodeBackendDomainJournalFrame(backendDomainJournalFrame{Version: 1, Sequence: sequence, Patch: patch, Checksum: checksum})
		if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
			return tx.Put(backendDomainJournalNamespace, backendJournalKey(sequence), raw)
		}); err != nil {
			t.Fatal(err)
		}
	}
	reader := migrationSourceReader{ctx: t.Context(), backend: backend}
	if _, _, err := replayBackendDomainJournal(t.Context(), reader, memory, clock); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("oracle=%v", err)
	}
	indexSnapshotForTest(t, backend)
	indexJournalForTest(t, backend, 1)
	indexJournalForTest(t, backend, 2)
	if _, err := loadSnapshotThreadClosure(t.Context(), reader, "retained", clock); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("scoped replay=%v", err)
	}
}

func TestSnapshotJournalRejectsCheckpointRepairBeforeRecovery(t *testing.T) {
	clock := func() time.Time { return time.Unix(1900000000, 0).UTC() }
	memory := newMemoryRepo(clock)
	for _, id := range []string{"requested", "unrelated"} {
		if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: id}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := memory.Append(t.Context(), Entry{ThreadID: "unrelated", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "needs fallback title"}}, AppendOptions{ID: "fact"}); err != nil {
		t.Fatal(err)
	}
	base, err := memory.EncodeMemoryState()
	if err != nil {
		t.Fatal(err)
	}
	_, migrated, err := decodeMemoryState(base, clock)
	if err != nil || !migrated {
		t.Fatalf("fixture migration=%v err=%v", migrated, err)
	}
	backend, _ := installSnapshotImportFixture(t, base)
	patch := backendDomainJournalPatch{Replace: map[string]json.RawMessage{"sequence": json.RawMessage("42")}}
	checksum, _ := backendDomainJournalChecksum(1, patch)
	raw, _ := encodeBackendDomainJournalFrame(backendDomainJournalFrame{Version: 1, Sequence: 1, Patch: patch, Checksum: checksum})
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return tx.Put(backendDomainJournalNamespace, backendJournalKey(1), raw) }); err != nil {
		t.Fatal(err)
	}
	indexSnapshotForTest(t, backend)
	indexJournalForTest(t, backend, 1)
	reader := migrationSourceReader{ctx: t.Context(), backend: backend}
	if _, err := loadSnapshotThreadClosure(t.Context(), reader, "requested", clock); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("checkpoint repair hidden by selection: %v", err)
	}
}

func TestSnapshotAuxiliaryIndexesRollBackWithCheckpoint(t *testing.T) {
	for _, kind := range []string{"journal", "roots"} {
		for _, mode := range []string{"write", "panic", "cancel"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				backend := newMigrationTestBackend()
				key := snapshotJournalCheckpointKey(1)
				index := func(ctx context.Context, tx spi.WriteTx) (snapshotImportCheckpoint, error) {
					return indexSnapshotJournalBatch(ctx, tx, 1)
				}
				patch := backendDomainJournalPatch{Replace: map[string]json.RawMessage{"sequence": json.RawMessage("1")}}
				checksum, _ := backendDomainJournalChecksum(1, patch)
				raw, _ := encodeBackendDomainJournalFrame(backendDomainJournalFrame{Version: 1, Sequence: 1, Patch: patch, Checksum: checksum})
				namespace, sourceKey := backendDomainJournalNamespace, backendJournalKey(1)
				if kind == "roots" {
					key, index = snapshotRootsCheckpointKey, indexSnapshotRootsBatch
					raw, _ = encodeRootThreadInventoryItems([]RootThreadInventoryItem{})
					namespace, sourceKey = backendDomainNamespace, backendRootThreadInventoryKey
				}
				if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return tx.Put(namespace, sourceKey, raw) }); err != nil {
					t.Fatal(err)
				}
				before := cloneMigrationRecords(backend.records)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				func() {
					if mode == "panic" {
						defer func() {
							if recover() == nil {
								t.Error("missing injected panic")
							}
						}()
					}
					err := backend.Update(ctx, func(tx spi.WriteTx) error {
						_, err := index(ctx, snapshotCheckpointFailureTx{WriteTx: tx, mode: mode, key: key, cancel: cancel})
						return err
					})
					if err == nil {
						t.Fatal("failed checkpoint committed")
					}
				}()
				if !reflect.DeepEqual(before, backend.records) {
					t.Fatal("failed checkpoint retained locators")
				}
				if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
					checkpoint, err := index(t.Context(), tx)
					if err == nil && !checkpoint.Complete {
						t.Fatal("retry incomplete")
					}
					return err
				}); err != nil {
					t.Fatal(err)
				}
				complete := cloneMigrationRecords(backend.records)
				if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { _, err := index(t.Context(), tx); return err }); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(complete, backend.records) {
					t.Fatal("completed scan rewrote records")
				}
			})
		}
	}
}

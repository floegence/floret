package sessiontree

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/storagecodec"
	"github.com/floegence/floret/v7/storage/spi"
)

func indexSnapshotForTest(t *testing.T, backend spi.Backend) {
	t.Helper()
	for {
		var checkpoint snapshotImportCheckpoint
		if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
			var err error
			checkpoint, err = indexHistoricalSnapshotBatch(t.Context(), tx)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if checkpoint.Complete {
			return
		}
	}
}

func TestSnapshotClosureRejectsUnappliedRecoveryJournal(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "source"}); err != nil {
		t.Fatal(err)
	}
	payload, err := memory.EncodeMemoryState()
	if err != nil {
		t.Fatal(err)
	}
	backend, _ := installSnapshotImportFixture(t, payload)
	indexSnapshotForTest(t, backend)
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		return tx.Put(backendDomainJournalNamespace, backendJournalKey(1), []byte(`{"unapplied":true}`))
	}); err != nil {
		t.Fatal(err)
	}
	before := cloneMigrationRecords(backend.records)
	_, err = loadSnapshotThreadClosure(t.Context(), migrationSourceReader{ctx: t.Context(), backend: backend}, "source", time.Now)
	if !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("unapplied journal error = %v", err)
	}
	if !reflect.DeepEqual(before, backend.records) {
		t.Fatal("rejected snapshot read changed its source")
	}
}

func TestSnapshotClosureRejectsSameLengthSourceMutation(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "source"}); err != nil {
		t.Fatal(err)
	}
	if _, err := memory.SetThreadTitle(t.Context(), SetThreadTitleRequest{ThreadID: "source", Title: "original fact", Now: time.Now()}); err != nil {
		t.Fatal(err)
	}
	payload, err := memory.EncodeMemoryState()
	if err != nil {
		t.Fatal(err)
	}
	backend, raw := installSnapshotImportFixture(t, payload)
	indexSnapshotForTest(t, backend)
	if _, err := loadSnapshotThreadClosure(t.Context(), migrationSourceReader{ctx: t.Context(), backend: backend}, "source", time.Now); err != nil {
		t.Fatalf("unchanged source: %v", err)
	}
	changed := bytes.ReplaceAll(raw, []byte("original fact"), []byte("replaced fact"))
	if bytes.Equal(raw, changed) || len(raw) != len(changed) {
		t.Fatal("fixture must change content without changing size")
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		return tx.Put(backendDomainNamespace, backendStateKey, changed)
	}); err != nil {
		t.Fatal(err)
	}
	before := cloneMigrationRecords(backend.records)
	_, err = loadSnapshotThreadClosure(t.Context(), migrationSourceReader{ctx: t.Context(), backend: backend}, "source", time.Now)
	if !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("changed source error = %v", err)
	}
	if !reflect.DeepEqual(before, backend.records) {
		t.Fatal("rejected source read changed stored bytes")
	}
}

func TestSnapshotClosureValidatesDeletedThreadAuthority(t *testing.T) {
	for _, valid := range []bool{false, true} {
		memory := newMemoryRepo(time.Now)
		memory.tombstones["deleted"] = ThreadTombstone{ThreadID: "deleted", RootThreadID: "deleted", DeletedAt: time.Unix(1900000000, 0).UTC()}
		if !valid {
			tombstone := memory.tombstones["deleted"]
			tombstone.ThreadID = "different"
			memory.tombstones["deleted"] = tombstone
		}
		payload, err := memory.EncodeMemoryState()
		if err != nil {
			t.Fatal(err)
		}
		backend, _ := installSnapshotImportFixture(t, payload)
		indexSnapshotForTest(t, backend)
		_, err = loadSnapshotThreadClosure(t.Context(), migrationSourceReader{ctx: t.Context(), backend: backend}, "deleted", time.Now)
		want := ErrAuthorityCorrupt
		if valid {
			want = ErrThreadDeleted
		}
		if !errors.Is(err, want) {
			t.Fatalf("valid=%v: error=%v, want %v", valid, err, want)
		}
	}
}

func TestSnapshotClosurePreservesExactSubAgentAdmissionLineage(t *testing.T) {
	for version := 2; version <= 5; version++ {
		state, _, _ := legacyActiveSubAgentState(t)
		if version >= 3 {
			if err := migrateMemoryStateV2ToV3(&state); err != nil {
				t.Fatal(err)
			}
		}
		if version >= 4 {
			if err := migrateMemoryStateV3ToV4(&state); err != nil {
				t.Fatal(err)
			}
		}
		if version == 5 {
			if err := migrateMemoryStateV4ToV5(&state); err != nil {
				t.Fatal(err)
			}
		}
		payload, err := json.Marshal(state)
		if version < 5 {
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(payload, &fields); err != nil {
				t.Fatal(err)
			}
			fields["root_create_intents"] = json.RawMessage("{}")
			payload, err = json.Marshal(fields)
		}
		if err != nil {
			t.Fatal(err)
		}
		backend, _ := installSnapshotImportFixture(t, payload)
		indexSnapshotForTest(t, backend)
		clock := func() time.Time { return time.Unix(1900000000, 0).UTC() }
		oracle, err := DecodeMemoryState(payload, clock)
		if err != nil {
			t.Fatal(err)
		}
		got, err := loadSnapshotThreadClosure(t.Context(), migrationSourceReader{ctx: t.Context(), backend: backend}, "child", clock)
		if err != nil {
			t.Fatalf("v%d: %v", version, err)
		}
		wantState, err := oracle.EncodeMemoryState()
		if err != nil {
			t.Fatal(err)
		}
		gotState, err := got.EncodeMemoryState()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(wantState, gotState) {
			t.Fatalf("v%d closure differs from exact whole-state migration", version)
		}
		for _, memory := range []*MemoryRepo{oracle, got} {
			if err := migrateHistoricalMemoryThroughV29(WithParentEvents(t.Context(), false), memory, 6); err != nil {
				t.Fatalf("v%d lineage: %v", version, err)
			}
		}
		if !reflect.DeepEqual(got.entries, oracle.entries) {
			t.Fatalf("v%d permanent lineage diverged", version)
		}
	}
}

type snapshotBodyGuard struct {
	spi.ReadTx
	raw       []byte
	forbidden storagecodec.JSONSpan
}

func (reader snapshotBodyGuard) Scan(request spi.ScanRequest) (spi.ScanPage, error) {
	if request.Namespace == snapshotImportNamespace {
		for field := range snapshotStateFields() {
			if snapshotFieldKeyedByThread(field) && bytes.Equal(request.Start, snapshotSourceKey("payload", field)) {
				return spi.ScanPage{}, errors.New("scanned unrelated thread metadata")
			}
		}
	}
	return reader.ReadTx.Scan(request)
}

func (reader snapshotBodyGuard) NamespaceHasRecords(namespace string) (bool, error) {
	page, err := reader.Scan(spi.ScanRequest{Namespace: namespace, Limit: 1})
	return len(page.Records) > 0, err
}
func (reader snapshotBodyGuard) RecordSize(namespace string, key []byte) (int64, error) {
	if namespace == backendDomainNamespace && bytes.Equal(key, backendStateKey) {
		return int64(len(reader.raw)), nil
	}
	raw, err := reader.ReadTx.Get(namespace, key)
	return int64(len(raw)), err
}
func (reader snapshotBodyGuard) Get(namespace string, key []byte) ([]byte, error) {
	if namespace == backendDomainNamespace && bytes.Equal(key, backendStateKey) {
		return nil, errors.New("unbounded snapshot Get")
	}
	return reader.ReadTx.Get(namespace, key)
}
func (reader snapshotBodyGuard) ReadRecordRange(namespace string, key []byte, offset int64, limit int) ([]byte, error) {
	if namespace != backendDomainNamespace || !bytes.Equal(key, backendStateKey) {
		return nil, errors.New("unexpected source range")
	}
	end := min(offset+int64(limit), int64(len(reader.raw)))
	if offset < reader.forbidden.Offset+reader.forbidden.Length && end > reader.forbidden.Offset {
		return nil, errors.New("unrelated history read")
	}
	if limit > 64<<10 {
		return nil, errors.New("unbounded physical source read")
	}
	return bytes.Clone(reader.raw[offset:end]), nil
}

func TestSnapshotClosureNeverReadsUnrelatedHistory(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	for _, id := range []string{"selected", "unrelated"} {
		if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: id}); err != nil {
			t.Fatal(err)
		}
		if _, err := memory.Append(t.Context(), Entry{ThreadID: id, Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: id}}, AppendOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	payload, err := memory.EncodeMemoryState()
	if err != nil {
		t.Fatal(err)
	}
	backend, raw := installSnapshotImportFixture(t, payload)
	indexSnapshotForTest(t, backend)
	before := cloneMigrationRecords(backend.records)
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		span, err := readSnapshotSourceSpan(tx, "payload", "entries", "unrelated")
		if err != nil {
			return err
		}
		guard := snapshotBodyGuard{ReadTx: tx, raw: raw, forbidden: span.JSONSpan}
		got, err := loadSnapshotThreadClosure(t.Context(), guard, "selected", time.Now)
		if err != nil {
			return err
		}
		if len(got.threads) != 1 || len(got.entries["selected"]) != 1 {
			t.Fatal("closure has wrong extent")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, backend.records) {
		t.Fatal("read changed source")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = loadSnapshotThreadClosure(ctx, migrationSourceReader{ctx: ctx, backend: backend}, "selected", time.Now)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation=%v", err)
	}
}

func TestSnapshotClosurePreservesPublishedUTF8Repair(t *testing.T) {
	for _, repairable := range []bool{false, true} {
		backend := newMigrationTestBackend()
		want := installLegacyUTF8ToolResultStore(t, backend, false, repairable)
		indexSnapshotForTest(t, backend)
		before := cloneMigrationRecords(backend.records)
		got, err := loadSnapshotThreadClosure(t.Context(), migrationSourceReader{ctx: t.Context(), backend: backend}, "thread", time.Now)
		if !repairable {
			if !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("unrepairable projection error = %v", err)
			}
		} else {
			if err != nil {
				t.Fatal(err)
			}
			if len(got.entries["thread"]) != 1 || got.entries["thread"][0].Raw != want || got.entries["thread"][0].RawHash != StableHash(want) {
				t.Fatal("exact released UTF8 repair changed")
			}
		}
		if !reflect.DeepEqual(before, backend.records) {
			t.Fatal("scoped repair modified source")
		}
	}
}

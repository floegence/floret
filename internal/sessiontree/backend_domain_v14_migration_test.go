package sessiontree

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/storagecodec"
	"github.com/floegence/floret/v7/storage/spi"
)

// installV13RunIdentityStore creates the historical shape that v14 has to
// accept: a continuation run has an explicit identity on a canonical entry,
// while its save point is missing RunID and relies on the ancestor path. The
// legacy observation entry is intentionally left without an identity.
func installV13RunIdentityStore(t *testing.T, backend *migrationTestBackend) *MemoryRepo {
	t.Helper()
	ctx := t.Context()
	now := time.Date(2026, 10, 12, 10, 0, 0, 0, time.UTC)
	memory := newMemoryRepo(func() time.Time { return now })
	if _, err := memory.CreateThread(ctx, ThreadMeta{ID: "thread", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	appendEntry := func(entry Entry, options AppendOptions) {
		t.Helper()
		if _, err := memory.Append(ctx, entry, options); err != nil {
			t.Fatalf("append %q: %v", entry.ID, err)
		}
	}
	appendEntry(Entry{
		ID: "started", ThreadID: "thread", TurnID: "turn", RunID: "run-1", Type: EntryTurnMarker,
		TurnStatus: TurnStarted, Metadata: map[string]string{"run_id": "run-1"},
	}, AppendOptions{ID: "started", Now: now})
	appendEntry(Entry{
		ID: "user", ThreadID: "thread", TurnID: "turn", RunID: "run-1", Type: EntryUserMessage,
		Message: session.Message{Role: session.User, Content: "hello"},
	}, AppendOptions{ID: "user", ParentID: "started", Now: now.Add(time.Millisecond)})
	appendEntry(Entry{
		ID: "waiting", ThreadID: "thread", TurnID: "turn", RunID: "run-1", Type: EntryTurnMarker,
		TurnStatus: TurnWaiting, Metadata: map[string]string{"run_id": "run-1"},
	}, AppendOptions{ID: "waiting", ParentID: "user", Now: now.Add(2 * time.Millisecond)})
	// A continuation can advance RunID without creating another started marker.
	appendEntry(Entry{
		ID: "continuation", ThreadID: "thread", TurnID: "turn", RunID: "run-2", Type: EntryAssistantMessage,
		Message: session.Message{Role: session.Assistant, Content: "resumed answer"},
	}, AppendOptions{ID: "continuation", ParentID: "waiting", Now: now.Add(3 * time.Millisecond)})
	// This old observation custom entry has no RunID and must remain untouched.
	appendEntry(Entry{
		ID: "legacy-observation", ThreadID: "thread", TurnID: "turn", Type: EntryCustom,
		Metadata: map[string]string{"observation": "legacy"},
	}, AppendOptions{ID: "legacy-observation", ParentID: "continuation", Now: now.Add(4 * time.Millisecond)})
	appendEntry(Entry{
		ID: "save", ThreadID: "thread", TurnID: "turn", RunID: "run-2", Type: EntryTurnMarker,
		TurnStatus: TurnSavePoint,
	}, AppendOptions{ID: "save", ParentID: "legacy-observation", Now: now.Add(5 * time.Millisecond)})
	appendEntry(Entry{
		ID: "done", ThreadID: "thread", TurnID: "turn", RunID: "run-2", Type: EntryTurnMarker,
		TurnStatus: TurnCompleted, Metadata: map[string]string{"run_id": "run-2", FinalResponseEntriesMetadataKey: `["continuation"]`},
	}, AppendOptions{ID: "done", ParentID: "save", Now: now.Add(6 * time.Millisecond)})
	if _, err := memory.Fork(ctx, ForkOptions{SourceThreadID: "thread", NewThreadID: "fork", Now: now.Add(7 * time.Millisecond)}); err != nil {
		t.Fatal(err)
	}
	if _, err := memory.Fork(ctx, ForkOptions{SourceThreadID: "fork", NewThreadID: "nested-fork", Now: now.Add(8 * time.Millisecond)}); err != nil {
		t.Fatal(err)
	}

	meta := memory.threads["thread"]
	memory.providerStates["thread"] = ProviderStateRecord{
		ThreadID: "thread", LeafEntryID: meta.LeafID, CompatibilityKey: "provider-key",
		State:           provider.State{Kind: "continuation", ID: "provider-state"},
		CreatedByTurnID: "turn", CreatedByRunID: "run-2", UpdatedAt: now.Add(6 * time.Millisecond),
	}

	// Recreate the missing RunID shapes that were accepted by schema 13.
	for threadID, stored := range memory.entries {
		entries := cloneEntries(stored)
		for index := range entries {
			if entries[index].Type == EntryTurnMarker && (entries[index].TurnStatus == TurnStarted || entries[index].TurnStatus == TurnSavePoint || entries[index].TurnStatus == TurnCompleted) {
				entries[index].RunID = ""
			}
			entries[index] = PrepareEntry(entries[index])
		}
		if err := memory.replaceLegacyIndexedEntriesLocked(threadID, entries); err != nil {
			t.Fatal(err)
		}
	}
	if err := backend.Update(ctx, func(tx spi.WriteTx) error { return saveCompleteBackendDomainV13(tx, memory) }); err != nil {
		t.Fatal(err)
	}
	return memory
}

func TestBackendDomainV13ToV14RepairsCanonicalRunMarkers(t *testing.T) {
	backend := newMigrationTestBackend()
	memory := installV13RunIdentityStore(t, backend)
	before := cloneEntries(memory.entries["thread"])
	beforeByThread := make(map[string][]Entry, len(memory.entries))
	for threadID, entries := range memory.entries {
		beforeByThread[threadID] = cloneEntries(entries)
	}
	beforeRecords := cloneMigrationRecords(backend.records)

	repo, err := NewBackendRepo(t.Context(), backend, memory.now)
	if err != nil {
		t.Fatal(err)
	}
	after, err := repo.domainMemory.Entries(t.Context(), "thread")
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("entry count=%d, want %d", len(after), len(before))
	}
	wantRuns := map[string]string{"started": "run-1", "save": "run-2", "done": "run-2"}
	for index, want := range before {
		got := after[index]
		if got.ID != want.ID || got.ThreadID != want.ThreadID || got.ParentID != want.ParentID || got.CreatedAt != want.CreatedAt || got.Raw != want.Raw || got.RawHash != want.RawHash {
			t.Fatalf("entry %q changed durable content: got=%#v want=%#v", want.ID, got, want)
		}
		if expected, ok := wantRuns[want.ID]; ok {
			if got.RunID != expected {
				t.Fatalf("entry %q run_id=%q, want %q", got.ID, got.RunID, expected)
			}
		} else if got.RunID != want.RunID {
			t.Fatalf("entry %q run_id changed from %q to %q", got.ID, want.RunID, got.RunID)
		}
	}
	if after[4].RunID != "" {
		t.Fatal("legacy custom observation unexpectedly acquired a run identity")
	}
	for threadID, entries := range beforeByThread {
		migrated := repo.domainMemory.entries[threadID]
		if len(migrated) != len(entries) {
			t.Fatalf("thread %q lost journal entries", threadID)
		}
		for index, source := range entries {
			got := cloneEntry(migrated[index])
			if source.Type == EntryTurnMarker && source.RunID == "" {
				wantRunID := "run-2"
				if source.TurnStatus == TurnStarted {
					wantRunID = "run-1"
				}
				if got.RunID != wantRunID {
					t.Fatalf("thread %q marker %q run=%q, want %q", threadID, got.ID, got.RunID, wantRunID)
				}
			}
			got.RunID = source.RunID
			if !reflect.DeepEqual(got, source) {
				t.Fatalf("thread %q migration changed facts outside RunID on entry %q", threadID, source.ID)
			}
		}
	}
	if got := repo.domainMemory.providerStates["thread"]; got.CreatedByRunID != "run-2" || got.LeafEntryID != "done" {
		t.Fatalf("provider state changed: %#v", got)
	}
	if len(migrationTestNamespaceRecords(t, backend, backendDomainV13Namespace)) != 0 {
		t.Fatal("v13 records survived migration")
	}
	current := migrationTestNamespaceRecords(t, backend, backendDomainV16Namespace)
	if len(current) == 0 {
		t.Fatal("v14 records are missing after migration")
	}
	if _, err := NewBackendRepo(t.Context(), backend, memory.now); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(migrationTestNamespaceRecords(t, backend, backendDomainV16Namespace), current) {
		t.Fatal("current restart rewrote migrated bytes")
	}
	// The v13 source is consumed atomically; a failed attempt below must still
	// leave the original source bytes available in a fresh fixture.
	if reflect.DeepEqual(backend.records, beforeRecords) {
		t.Fatal("successful migration did not replace the source namespace")
	}
}

func TestBackendDomainV13ToV14RejectsAmbiguousRunIdentity(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*MemoryRepo)
	}{
		{
			name: "marker_metadata_conflict",
			mutate: func(memory *MemoryRepo) {
				rewriteV13RunEntry(memory, "waiting", func(entry *Entry) {
					entry.Metadata["run_id"] = "other-run"
				})
			},
		},
		{
			name: "missing_ancestor_identity",
			mutate: func(memory *MemoryRepo) {
				entries := cloneEntries(memory.entries["thread"])
				var legacy Entry
				for _, entry := range entries {
					if entry.ID == "legacy-observation" {
						legacy = entry
					}
				}
				for index := range entries {
					if entries[index].ID == legacy.ID {
						entries[index].ParentID = ""
						entries[index].PathDepth = 1
						legacy.PathDepth = 1
					}
					if entries[index].ID == "save" {
						entries[index].ParentID = legacy.ID
						entries[index].PathDepth = legacy.PathDepth + 1
					}
					if entries[index].ID == "done" {
						entries[index].ParentID = "save"
						entries[index].PathDepth = entries[index-1].PathDepth + 1
					}
					entries[index] = PrepareEntry(entries[index])
				}
				if err := memory.replaceLegacyIndexedEntriesLocked("thread", entries); err != nil {
					panic(err)
				}
			},
		},
		{
			name: "redacted_metadata",
			mutate: func(memory *MemoryRepo) {
				rewriteV13RunEntry(memory, "done", func(entry *Entry) {
					entry.Metadata["run_id"] = "[redacted]#run-2"
				})
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backend := newMigrationTestBackend()
			memory := installV13RunIdentityStore(t, backend)
			test.mutate(memory)
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return saveCompleteBackendDomainV13(tx, memory) }); err != nil {
				t.Fatal(err)
			}
			before := cloneMigrationRecords(backend.records)
			if _, err := NewBackendRepo(t.Context(), backend, memory.now); !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("migration error=%v, want ErrAuthorityCorrupt", err)
			}
			if !reflect.DeepEqual(backend.records, before) {
				t.Fatal("rejected migration changed source records")
			}
		})
	}
}

func TestBackendDomainV13ToV14RollsBackWriteCancellationPanicAndFuture(t *testing.T) {
	for _, failure := range []string{"write", "cancel", "panic", "future"} {
		t.Run(failure, func(t *testing.T) {
			backend := newMigrationTestBackend()
			memory := installV13RunIdentityStore(t, backend)
			ctx := t.Context()
			var target spi.Backend = backend
			switch failure {
			case "write":
				target = migrationFailingBackend{Backend: backend, err: errors.New("v14 migration write failed")}
			case "cancel":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "panic":
				target = migrationPanickingBackend{Backend: backend}
			case "future":
				format := backendDomainV13Format
				format.version++
				raw, err := encodeBackendDomainRecord(format, backendDomainRecordManifest, "", "", 0, backendDomainManifest{Version: format.version})
				if err != nil {
					t.Fatal(err)
				}
				if err := backend.Update(ctx, func(tx spi.WriteTx) error {
					return tx.Put(format.namespace, backendDomainV13Key(backendDomainRecordManifest), raw)
				}); err != nil {
					t.Fatal(err)
				}
			}
			before := cloneMigrationRecords(backend.records)
			if failure == "panic" {
				defer func() {
					if recover() == nil {
						t.Error("migration did not propagate injected panic")
					}
					if !reflect.DeepEqual(backend.records, before) {
						t.Error("panicked migration changed source records")
					}
				}()
			}
			if _, err := NewBackendRepo(ctx, target, memory.now); err == nil {
				t.Fatal("invalid migration succeeded")
			}
			if !reflect.DeepEqual(backend.records, before) {
				t.Fatal("failed migration changed source records")
			}
		})
	}
}

func rewriteV13RunEntry(memory *MemoryRepo, id string, mutate func(*Entry)) {
	entries := cloneEntries(memory.entries["thread"])
	for index := range entries {
		if entries[index].ID == id {
			mutate(&entries[index])
			entries[index] = PrepareEntry(entries[index])
		}
	}
	if err := memory.replaceLegacyIndexedEntriesLocked("thread", entries); err != nil {
		panic(err)
	}
}

func TestCanonicalIndexesRejectLegacyStartedIdentityOutsideMigration(t *testing.T) {
	backend := newMigrationTestBackend()
	memory := installV13RunIdentityStore(t, backend)
	if err := newMemoryRepo(memory.now).replaceIndexedEntriesLocked("thread", memory.entries["thread"]); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("current index error=%v, want ErrAuthorityCorrupt", err)
	}
}

func TestBackendDomainV14FreshAndEmptySourceAreIdempotent(t *testing.T) {
	for _, sourceV13 := range []bool{false, true} {
		t.Run(map[bool]string{false: "fresh", true: "empty_v13"}[sourceV13], func(t *testing.T) {
			backend := newMigrationTestBackend()
			if sourceV13 {
				if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
					return saveCompleteBackendDomainV13(tx, newMemoryRepo(time.Now))
				}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := NewBackendRepo(t.Context(), backend, time.Now); err != nil {
				t.Fatal(err)
			}
			if len(migrationTestNamespaceRecords(t, backend, backendDomainV16Namespace)) == 0 {
				t.Fatal("current schema records are missing")
			}
			before := cloneMigrationRecords(backend.records)
			if _, err := NewBackendRepo(t.Context(), backend, time.Now); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, backend.records) {
				t.Fatal("current restart changed canonical records")
			}
		})
	}
}

func TestBackendDomainV13ToV14RejectsWithoutPartialMemoryMutation(t *testing.T) {
	backend := newMigrationTestBackend()
	source := installV13RunIdentityStore(t, backend)
	rewriteV13RunEntry(source, "waiting", func(entry *Entry) { entry.Metadata["run_id"] = "other-run" })
	before, err := source.EncodeMemoryState()
	if err != nil {
		t.Fatal(err)
	}
	// Map iteration can encounter a valid nested fork before the invalid source.
	// Every failure must preserve the complete source memory regardless of order.
	for range 20 {
		candidate := cloneMemoryRepoForStartup(source)
		if err := migrateBackendDomainV13ToV14(t.Context(), candidate); !errors.Is(err, ErrAuthorityCorrupt) {
			t.Fatalf("migration error=%v, want ErrAuthorityCorrupt", err)
		}
		after, err := candidate.EncodeMemoryState()
		if err != nil {
			t.Fatal(err)
		}
		if string(after) != string(before) {
			t.Fatal("rejected migration partially changed source memory")
		}
	}
}

func TestLegacyDomainV2ThroughV4ReachesV14(t *testing.T) {
	for _, version := range []int{2, 3, 4} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			backend := newMigrationTestBackend()
			encoded, err := storagecodec.EncodeEnvelope("sessiontree", legacyEmptyMemoryState(t, version))
			if err != nil {
				t.Fatal(err)
			}
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				return tx.Put(backendDomainNamespace, backendStateKey, encoded)
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := NewBackendRepo(t.Context(), backend, time.Now); err != nil {
				t.Fatal(err)
			}
			if len(migrationTestNamespaceRecords(t, backend, backendDomainV16Namespace)) == 0 {
				t.Fatal("contiguous migration did not reach v14")
			}
			before := cloneMigrationRecords(backend.records)
			if _, err := NewBackendRepo(t.Context(), backend, time.Now); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, backend.records) {
				t.Fatal("current restart rewrote migrated lineage")
			}
		})
	}
}

package sessiontree

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/observation"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestBackendDomainV8ToV9MigratesResumedAndNestedForkContext(t *testing.T) {
	backend := newMigrationTestBackend()
	fixture := installV8ThreadContextStore(t, backend, nil)

	repo, err := NewBackendRepo(t.Context(), backend, func() time.Time { return fixture.now })
	if err != nil {
		t.Fatal(err)
	}
	for _, threadID := range []string{fixture.rootThreadID, fixture.directThreadID, fixture.nestedThreadID} {
		assertV9ThreadContext(t, repo, threadID, fixture.turnID, fixture.runID, fixture.resumedRunID)
	}
	if got := migrationTestNamespaceRecords(t, backend, backendDomainV8Namespace); len(got) != 0 {
		t.Fatalf("v8 records survived migration: %d", len(got))
	}
	current := migrationTestNamespaceRecords(t, backend, backendDomainV9Namespace)
	if len(current) == 0 {
		t.Fatal("v9 records are missing after migration")
	}
	if _, err := NewBackendRepo(t.Context(), backend, func() time.Time { return fixture.now }); err != nil {
		t.Fatal(err)
	}
	if got := migrationTestNamespaceRecords(t, backend, backendDomainV9Namespace); !reflect.DeepEqual(got, current) {
		t.Fatal("current v9 restart rewrote canonical bytes")
	}
}

func TestBackendDomainV8ToV9RejectsContextDriftWithoutMutation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*MemoryRepo, v8ThreadContextFixture)
	}{
		{
			name: "non_ancestor_thread",
			mutate: func(memory *MemoryRepo, fixture v8ThreadContextFixture) {
				rewriteLegacyV8ContextStatus(t, memory, fixture.directThreadID, func(status *observation.ContextStatus) {
					status.ThreadID = "unrelated-thread"
				})
			},
		},
		{
			name: "turn_drift",
			mutate: func(memory *MemoryRepo, fixture v8ThreadContextFixture) {
				rewriteLegacyV8ContextStatus(t, memory, fixture.rootThreadID, func(status *observation.ContextStatus) {
					status.TurnID = "other-turn"
				})
			},
		},
		{
			name: "run_drift",
			mutate: func(memory *MemoryRepo, fixture v8ThreadContextFixture) {
				rewriteLegacyV8ContextStatus(t, memory, fixture.rootThreadID, func(status *observation.ContextStatus) {
					status.RunID = "other-run"
				})
			},
		},
		{
			name: "fork_copy_proof_drift",
			mutate: func(memory *MemoryRepo, fixture v8ThreadContextFixture) {
				entries := cloneEntries(memory.entries[fixture.directThreadID])
				for index := range entries {
					if ThreadContextEntryKind(entries[index]) == legacyThreadContextStatusEntryKind {
						entries[index].CreatedAt = entries[index].CreatedAt.Add(time.Nanosecond)
					}
				}
				mustReplaceMigrationEntries(t, memory, fixture.directThreadID, entries)
			},
		},
		{
			name: "bad_json",
			mutate: func(memory *MemoryRepo, fixture v8ThreadContextFixture) {
				entries := cloneEntries(memory.entries[fixture.rootThreadID])
				for index := range entries {
					if ThreadContextEntryKind(entries[index]) == legacyThreadContextStatusEntryKind {
						entries[index].Metadata[threadContextStatusKey] = "{"
						entries[index].Raw = rawForEntry(entries[index])
						entries[index].RawHash = stableHash(entries[index].Raw)
					}
				}
				mustReplaceMigrationEntries(t, memory, fixture.rootThreadID, entries)
			},
		},
		{
			name: "mixed_context_kinds",
			mutate: func(memory *MemoryRepo, fixture v8ThreadContextFixture) {
				entries := cloneEntries(memory.entries[fixture.rootThreadID])
				for index := range entries {
					if ThreadContextEntryKind(entries[index]) == legacyThreadContextPolicyEntryKind {
						entries[index].Metadata[threadContextKindKey] = ThreadContextPolicyEntryKind
						entries[index].Metadata[threadContextTypeKey] = ThreadContextPolicyEntryKind
						entries[index].Raw = rawForEntry(entries[index])
						entries[index].RawHash = stableHash(entries[index].Raw)
					}
				}
				mustReplaceMigrationEntries(t, memory, fixture.rootThreadID, entries)
			},
		},
		{
			name: "metadata_schema_drift",
			mutate: func(memory *MemoryRepo, fixture v8ThreadContextFixture) {
				entries := cloneEntries(memory.entries[fixture.rootThreadID])
				for index := range entries {
					if ThreadContextEntryKind(entries[index]) == legacyThreadContextStatusEntryKind {
						entries[index].Metadata["unexpected"] = "drift"
						entries[index].Raw = rawForEntry(entries[index])
						entries[index].RawHash = stableHash(entries[index].Raw)
					}
				}
				mustReplaceMigrationEntries(t, memory, fixture.rootThreadID, entries)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backend := newMigrationTestBackend()
			installV8ThreadContextStore(t, backend, test.mutate)
			before := cloneMigrationRecords(backend.records)
			if _, err := NewBackendRepo(t.Context(), backend, time.Now); !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("migration error=%v, want ErrAuthorityCorrupt", err)
			}
			if !reflect.DeepEqual(backend.records, before) {
				t.Fatal("rejected v8 context migration changed durable records")
			}
		})
	}
}

func TestBackendDomainV8ToV9RollsBackWriteCancellationAndPanic(t *testing.T) {
	t.Run("write_failure", func(t *testing.T) {
		backend := newMigrationTestBackend()
		installV8ThreadContextStore(t, backend, nil)
		before := cloneMigrationRecords(backend.records)
		injected := errors.New("v9 write failed")
		if _, err := NewBackendRepo(t.Context(), migrationFailingBackend{Backend: backend, err: injected}, time.Now); !errors.Is(err, injected) {
			t.Fatalf("migration error=%v, want %v", err, injected)
		}
		if !reflect.DeepEqual(backend.records, before) {
			t.Fatal("failed v8 to v9 write changed durable records")
		}
	})

	t.Run("cancellation", func(t *testing.T) {
		backend := newMigrationTestBackend()
		installV8ThreadContextStore(t, backend, nil)
		before := cloneMigrationRecords(backend.records)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := NewBackendRepo(ctx, backend, time.Now); !errors.Is(err, context.Canceled) {
			t.Fatalf("migration error=%v, want context cancellation", err)
		}
		if !reflect.DeepEqual(backend.records, before) {
			t.Fatal("cancelled v8 to v9 migration changed durable records")
		}
	})

	t.Run("panic", func(t *testing.T) {
		backend := newMigrationTestBackend()
		installV8ThreadContextStore(t, backend, nil)
		before := cloneMigrationRecords(backend.records)
		func() {
			defer func() {
				if recovered := recover(); recovered == nil {
					t.Fatal("migration did not propagate injected panic")
				}
			}()
			_, _ = NewBackendRepo(t.Context(), migrationPanickingBackend{Backend: backend}, time.Now)
		}()
		if !reflect.DeepEqual(backend.records, before) {
			t.Fatal("panicked v8 to v9 migration changed durable records")
		}
	})
}

type v8ThreadContextFixture struct {
	now            time.Time
	rootThreadID   string
	directThreadID string
	nestedThreadID string
	turnID         string
	runID          string
	resumedRunID   string
}

func installV8ThreadContextStore(t *testing.T, backend spi.Backend, mutate func(*MemoryRepo, v8ThreadContextFixture)) v8ThreadContextFixture {
	t.Helper()
	fixture := v8ThreadContextFixture{
		now:            time.Date(2026, 9, 2, 9, 0, 0, 0, time.UTC),
		rootThreadID:   "thread-root",
		directThreadID: "thread-direct",
		nestedThreadID: "thread-nested",
		turnID:         "turn-context",
		runID:          "run-context",
		resumedRunID:   "run-context-resumed",
	}
	memory := newMemoryRepo(func() time.Time { return fixture.now })
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: fixture.rootThreadID, CreatedAt: fixture.now, UpdatedAt: fixture.now}); err != nil {
		t.Fatal(err)
	}
	if _, err := memory.Append(t.Context(), Entry{
		ThreadID: fixture.rootThreadID, TurnID: fixture.turnID, RunID: fixture.runID,
		Type: EntryTurnMarker, TurnStatus: TurnStarted, Metadata: map[string]string{"run_id": fixture.runID},
	}, AppendOptions{ID: "turn-started", Now: fixture.now.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	policy := ThreadContextPolicy{ContextWindowTokens: 128_000, MaxOutputTokens: 4_096, ReservedOutputTokens: 4_096}
	policyJSON, err := json.Marshal(policy)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := memory.Append(t.Context(), Entry{
		ThreadID: fixture.rootThreadID, TurnID: fixture.turnID, Type: EntryCustom,
		Metadata: map[string]string{
			threadContextKindKey:     legacyThreadContextPolicyEntryKind,
			threadContextTypeKey:     legacyThreadContextPolicyEntryKind,
			threadContextProviderKey: "test",
			threadContextModelKey:    "scripted",
			threadContextPolicyKey:   string(policyJSON),
		},
	}, AppendOptions{ID: "context-policy", Now: fixture.now.Add(2 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	status := observation.ContextStatus{
		RunID: identity.RunID(fixture.runID), ThreadID: identity.ThreadID(fixture.rootThreadID), TurnID: identity.TurnID(fixture.turnID),
		Phase: observation.ContextPhaseProviderUsage, Status: observation.ContextStatusStable,
		Provider: "test", Model: "scripted", RequestID: "request-context", ObservedAt: fixture.now.Add(3 * time.Second),
		Usage: observation.ProviderUsage{InputTokens: 80, OutputTokens: 20},
	}
	statusJSON, err := json.Marshal(status)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := memory.Append(t.Context(), Entry{
		ThreadID: fixture.rootThreadID, TurnID: fixture.turnID, RunID: fixture.runID, Type: EntryCustom,
		Metadata: map[string]string{
			threadContextKindKey:   legacyThreadContextStatusEntryKind,
			threadContextTypeKey:   legacyThreadContextStatusEntryKind,
			threadContextStatusKey: string(statusJSON),
		},
	}, AppendOptions{ID: "context-status", Now: status.ObservedAt}); err != nil {
		t.Fatal(err)
	}
	compaction := ThreadContextCompaction{
		RunID: fixture.runID, ThreadID: fixture.rootThreadID, TurnID: fixture.turnID,
		OperationID: "compact-context", RequestID: "compact-request", Phase: string(observation.CompactionPhaseNoop),
		Status: string(observation.CompactionStatusNoop), ObservedAt: fixture.now.Add(4 * time.Second),
	}
	compactionJSON, err := json.Marshal(compaction)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := memory.Append(t.Context(), Entry{
		ThreadID: fixture.rootThreadID, TurnID: fixture.turnID, Type: EntryCustom,
		Metadata: map[string]string{
			threadContextKindKey:       legacyThreadContextCompactionEntryKind,
			threadContextTypeKey:       legacyThreadContextCompactionEntryKind,
			threadContextCompactionKey: string(compactionJSON),
		},
	}, AppendOptions{ID: "context-compaction", Now: compaction.ObservedAt}); err != nil {
		t.Fatal(err)
	}
	resumedStatus := status
	resumedStatus.RunID = identity.RunID(fixture.resumedRunID)
	resumedStatus.RequestID = "request-context-resumed"
	resumedStatus.ObservedAt = fixture.now.Add(4500 * time.Millisecond)
	resumedStatusJSON, err := json.Marshal(resumedStatus)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := memory.Append(t.Context(), Entry{
		ThreadID: fixture.rootThreadID, TurnID: fixture.turnID, RunID: fixture.resumedRunID, Type: EntryCustom,
		Metadata: map[string]string{
			threadContextKindKey:   legacyThreadContextStatusEntryKind,
			threadContextTypeKey:   legacyThreadContextStatusEntryKind,
			threadContextStatusKey: string(resumedStatusJSON),
		},
	}, AppendOptions{ID: "context-status-resumed", Now: resumedStatus.ObservedAt}); err != nil {
		t.Fatal(err)
	}
	resumedCompaction := compaction
	resumedCompaction.RunID = fixture.resumedRunID
	resumedCompaction.OperationID = "compact-context-resumed"
	resumedCompaction.RequestID = "compact-request-resumed"
	resumedCompaction.ObservedAt = fixture.now.Add(4750 * time.Millisecond)
	resumedCompactionJSON, err := json.Marshal(resumedCompaction)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := memory.Append(t.Context(), Entry{
		ThreadID: fixture.rootThreadID, TurnID: fixture.turnID, Type: EntryCustom,
		Metadata: map[string]string{
			threadContextKindKey:       legacyThreadContextCompactionEntryKind,
			threadContextTypeKey:       legacyThreadContextCompactionEntryKind,
			threadContextCompactionKey: string(resumedCompactionJSON),
		},
	}, AppendOptions{ID: "context-compaction-resumed", Now: resumedCompaction.ObservedAt}); err != nil {
		t.Fatal(err)
	}
	if _, err := memory.Append(t.Context(), Entry{
		ThreadID: fixture.rootThreadID, TurnID: fixture.turnID, RunID: fixture.resumedRunID,
		Type: EntryTurnMarker, TurnStatus: TurnCompleted, Metadata: map[string]string{"run_id": fixture.resumedRunID},
	}, AppendOptions{ID: "turn-completed", Now: fixture.now.Add(5 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	if _, err := memory.Fork(t.Context(), ForkOptions{
		SourceThreadID: fixture.rootThreadID, NewThreadID: fixture.directThreadID, Now: fixture.now.Add(10 * time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := memory.Fork(t.Context(), ForkOptions{
		SourceThreadID: fixture.directThreadID, NewThreadID: fixture.nestedThreadID, Now: fixture.now.Add(20 * time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	if mutate != nil {
		mutate(memory, fixture)
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return saveCompleteBackendDomainV8(tx, memory) }); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func assertV9ThreadContext(t *testing.T, repo *BackendRepo, threadID, turnID string, runIDs ...string) {
	t.Helper()
	meta, err := repo.Thread(t.Context(), threadID)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := repo.Path(t.Context(), threadID, meta.LeafID)
	if err != nil {
		t.Fatal(err)
	}
	wantRunIDs := make(map[string]struct{}, len(runIDs))
	for _, runID := range runIDs {
		wantRunIDs[runID] = struct{}{}
	}
	statusRunIDs := make(map[string]struct{}, len(runIDs))
	compactionRunIDs := make(map[string]struct{}, len(runIDs))
	var policyCount, statusCount, compactionCount int
	for _, entry := range entries {
		switch ThreadContextEntryKind(entry) {
		case ThreadContextPolicyEntryKind:
			policyCount++
			if _, err := DecodeThreadContextPolicyEntry(entry); err != nil {
				t.Fatal(err)
			}
		case ThreadContextStatusEntryKind:
			statusCount++
			status, err := DecodeThreadContextStatusEntry(entry)
			if err != nil {
				t.Fatal(err)
			}
			if status.ThreadID.String() != threadID || status.TurnID.String() != turnID {
				t.Fatalf("thread %q status identity=%#v", threadID, status)
			}
			statusRunIDs[status.RunID.String()] = struct{}{}
			assertContextPayloadHasNoIdentity(t, entry.Metadata[threadContextStatusKey])
		case ThreadContextCompactionEntryKind:
			compactionCount++
			compaction, err := DecodeThreadContextCompactionEntry(entry)
			if err != nil {
				t.Fatal(err)
			}
			if compaction.ThreadID != threadID || compaction.TurnID != turnID {
				t.Fatalf("thread %q compaction identity=%#v", threadID, compaction)
			}
			compactionRunIDs[compaction.RunID] = struct{}{}
			assertContextPayloadHasNoIdentity(t, entry.Metadata[threadContextCompactionKey])
		case legacyThreadContextPolicyEntryKind, legacyThreadContextStatusEntryKind, legacyThreadContextCompactionEntryKind:
			t.Fatalf("thread %q retained legacy context entry %q", threadID, entry.ID)
		}
	}
	if policyCount != 1 || statusCount != len(runIDs) || compactionCount != len(runIDs) {
		t.Fatalf("thread %q context counts policy=%d status=%d compaction=%d", threadID, policyCount, statusCount, compactionCount)
	}
	if !reflect.DeepEqual(statusRunIDs, wantRunIDs) || !reflect.DeepEqual(compactionRunIDs, wantRunIDs) {
		t.Fatalf("thread %q context run identities status=%v compaction=%v, want %v", threadID, statusRunIDs, compactionRunIDs, wantRunIDs)
	}
}

func assertContextPayloadHasNoIdentity(t *testing.T, raw string) {
	t.Helper()
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &fields); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"thread_id", "turn_id", "run_id"} {
		if _, found := fields[key]; found {
			t.Fatalf("context payload retained %s: %s", key, raw)
		}
	}
}

func rewriteLegacyV8ContextStatus(t *testing.T, memory *MemoryRepo, threadID string, mutate func(*observation.ContextStatus)) {
	t.Helper()
	entries := cloneEntries(memory.entries[threadID])
	for index := range entries {
		if ThreadContextEntryKind(entries[index]) != legacyThreadContextStatusEntryKind {
			continue
		}
		var status observation.ContextStatus
		if err := json.Unmarshal([]byte(entries[index].Metadata[threadContextStatusKey]), &status); err != nil {
			t.Fatal(err)
		}
		mutate(&status)
		raw, err := json.Marshal(status)
		if err != nil {
			t.Fatal(err)
		}
		entries[index].Metadata[threadContextStatusKey] = string(raw)
		entries[index].Raw = rawForEntry(entries[index])
		entries[index].RawHash = stableHash(entries[index].Raw)
	}
	mustReplaceMigrationEntries(t, memory, threadID, entries)
}

func mustReplaceMigrationEntries(t *testing.T, memory *MemoryRepo, threadID string, entries []Entry) {
	t.Helper()
	if err := memory.replaceIndexedEntriesLocked(threadID, entries); err != nil {
		t.Fatal(err)
	}
}

type migrationPanickingBackend struct {
	spi.Backend
}

func (backend migrationPanickingBackend) Update(ctx context.Context, mutate func(spi.WriteTx) error) error {
	return backend.Backend.Update(ctx, func(tx spi.WriteTx) error {
		return mutate(migrationPanickingTx{WriteTx: tx})
	})
}

type migrationPanickingTx struct {
	spi.WriteTx
}

func (tx migrationPanickingTx) Put(namespace string, key, value []byte) error {
	if namespace == backendDomainV9Namespace && bytes.Equal(key, backendDomainV9Key(backendDomainRecordRootIndex)) {
		panic("injected v9 migration panic")
	}
	return tx.WriteTx.Put(namespace, key, value)
}

func TestBackendDomainV9RejectsFutureRecordVersionWithoutMutation(t *testing.T) {
	backend := newMigrationTestBackend()
	fixture := installV8ThreadContextStore(t, backend, nil)
	if _, err := NewBackendRepo(t.Context(), backend, func() time.Time { return fixture.now }); err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		key := backendDomainV9Key(backendDomainRecordManifest)
		future := backendDomainV9Format
		future.version = backendDomainV9Version + 1
		encoded, err := encodeBackendDomainRecord(future, backendDomainRecordManifest, "", "", 0, backendDomainManifest{Version: future.version, Sequence: 1})
		if err != nil {
			return err
		}
		return tx.Put(backendDomainV9Namespace, key, encoded)
	}); err != nil {
		t.Fatal(err)
	}
	before := cloneMigrationRecords(backend.records)
	if _, err := NewBackendRepo(t.Context(), backend, time.Now); err == nil {
		t.Fatal("future v9 record version was accepted")
	}
	if !reflect.DeepEqual(backend.records, before) {
		t.Fatal("future v9 rejection changed durable records")
	}
}

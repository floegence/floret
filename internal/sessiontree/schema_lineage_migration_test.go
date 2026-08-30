package sessiontree

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v6/internal/session"
	"github.com/floegence/floret/v6/internal/storagebridge"
	"github.com/floegence/floret/v6/internal/storagecodec"
	publicstorage "github.com/floegence/floret/v6/storage"
	"github.com/floegence/floret/v6/storage/spi"
)

func TestMemoryStateMigrationLineageReachesV5(t *testing.T) {
	for _, version := range []int{2, 3, 4} {
		t.Run(string(rune('0'+version)), func(t *testing.T) {
			encoded := legacyEmptyMemoryState(t, version)
			repo, migrated, err := decodeMemoryState(encoded, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			if !migrated {
				t.Fatalf("version %d was not marked migrated", version)
			}
			current, err := repo.EncodeMemoryState()
			if err != nil {
				t.Fatal(err)
			}
			var header struct {
				Version int `json:"version"`
			}
			if err := json.Unmarshal(current, &header); err != nil {
				t.Fatal(err)
			}
			if header.Version != memoryStateVersion {
				t.Fatalf("version=%d, want %d", header.Version, memoryStateVersion)
			}
			reopened, migratedAgain, err := decodeMemoryState(current, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			again, err := reopened.EncodeMemoryState()
			if err != nil {
				t.Fatal(err)
			}
			if migratedAgain || !bytes.Equal(again, current) {
				t.Fatal("current v5 state was rewritten during idempotent reopen")
			}
		})
	}
}

func TestMemoryStateV2ReconstructsExactActiveSubAgentAdmission(t *testing.T) {
	state, input, lease := legacyActiveSubAgentState(t)
	if err := migrateMemoryStateV2ToV3(&state); err != nil {
		t.Fatal(err)
	}
	var admissions map[string]legacyTurnAdmission
	if err := json.Unmarshal(state.TurnAdmissions, &admissions); err != nil {
		t.Fatal(err)
	}
	admission, ok := admissions[legacyTurnKey(input.ChildThreadID, input.AdmittedTurnID)]
	if !ok {
		t.Fatal("v2 admission was not reconstructed")
	}
	if admission.ThreadID != input.ChildThreadID || admission.TurnID != input.AdmittedTurnID || admission.RunID != input.AdmittedRunID || admission.Lease != lease {
		t.Fatalf("admission=%#v, want exact source identity", admission)
	}
	if err := migrateMemoryStateV3ToV4(&state); err != nil {
		t.Fatal(err)
	}
	if err := migrateMemoryStateV4ToV5(&state); err != nil {
		t.Fatal(err)
	}
	if state.Version != 5 || state.TurnAdmissions != nil || state.Leases != nil {
		t.Fatalf("final state retained legacy authority: version=%d", state.Version)
	}
}

func TestMemoryStateV2AdmissionDriftFailsClosed(t *testing.T) {
	state, input, _ := legacyActiveSubAgentState(t)
	var leases map[string]legacyTurnLease
	if err := json.Unmarshal(state.Leases, &leases); err != nil {
		t.Fatal(err)
	}
	lease := leases[input.ChildThreadID]
	lease.TurnID = "other-turn"
	leases[input.ChildThreadID] = lease
	state.Leases, _ = json.Marshal(leases)
	encoded, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeMemoryState(encoded, time.Now); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("drifted v2 error=%v, want ErrAuthorityCorrupt", err)
	}
}

func TestMemoryStateV4AdmissionAuthorityDriftFailsClosed(t *testing.T) {
	state, input, _ := legacyActiveSubAgentState(t)
	if err := migrateMemoryStateV2ToV3(&state); err != nil {
		t.Fatal(err)
	}
	if err := migrateMemoryStateV3ToV4(&state); err != nil {
		t.Fatal(err)
	}
	var admissions map[string]legacyTurnAdmission
	if err := json.Unmarshal(state.TurnAdmissions, &admissions); err != nil {
		t.Fatal(err)
	}
	key := legacyTurnKey(input.ChildThreadID, input.AdmittedTurnID)
	admission := admissions[key]
	admission.Lease.OwnerID = "different-owner"
	admissions[key] = admission
	state.TurnAdmissions, _ = json.Marshal(admissions)
	if err := migrateMemoryStateV4ToV5(&state); err == nil {
		t.Fatal("drifted v4 admission was accepted")
	}
}

func TestLegacyV4PascalCaseRootAndTombstoneShapesDecode(t *testing.T) {
	stateBytes := legacyEmptyMemoryState(t, 4)
	var state map[string]json.RawMessage
	if err := json.Unmarshal(stateBytes, &state); err != nil {
		t.Fatal(err)
	}
	state["root_create_intents"] = json.RawMessage(`{"request":{"ThreadID":"thread","CreateIntentID":"request","Fingerprint":"` + StableHash("thread\x00request\x00v4") + `","ContractVersion":"v4"}}`)
	state["tombstones"] = json.RawMessage(`{"thread":{"ThreadID":"thread","RootThreadID":"thread","CreateIntentID":"request","DeletedAt":"2026-08-18T00:00:00Z"}}`)
	encoded, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	var decoded memoryState
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if got := decoded.RootCreateIntents["request"]; got.ThreadID != "thread" || got.CreateIntentID != "request" || got.Fingerprint != StableHash("thread\x00request\x00v4") {
		t.Fatalf("root intent=%#v", got)
	}
	if got := decoded.Tombstones["thread"]; got.ThreadID != "thread" || got.LegacyCreateIntent != "request" {
		t.Fatalf("tombstone=%#v", got)
	}
	repo, err := DecodeMemoryState(encoded, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	tombstone, err := repo.ThreadTombstone(t.Context(), "thread")
	if err != nil || tombstone.OriginRequestKey != "request" {
		t.Fatalf("migrated tombstone=%#v err=%v", tombstone, err)
	}
}

func TestBackendRepoMigrationRollbackAndCurrentBytes(t *testing.T) {
	ctx := context.Background()
	backend := newMigrationTestBackend()
	legacyPayload := legacyEmptyMemoryState(t, 3)
	legacyEnvelope, err := storagecodec.EncodeEnvelope("sessiontree", legacyPayload)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(ctx, func(tx spi.WriteTx) error {
		return tx.Put(backendDomainNamespace, backendStateKey, legacyEnvelope)
	}); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("v6 root index write failed")
	if _, err := NewBackendRepo(ctx, migrationFailingBackend{Backend: backend, err: injected}, time.Now); !errors.Is(err, injected) {
		t.Fatalf("migration error=%v, want injected failure", err)
	}
	if got := migrationTestRecord(t, backend, backendDomainNamespace, backendStateKey); !bytes.Equal(got, legacyEnvelope) {
		t.Fatal("failed migration changed canonical bytes")
	}
	repo, err := NewBackendRepo(ctx, backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	current := cloneMigrationRecords(backend.records)
	if _, err := NewBackendRepo(ctx, backend, time.Now); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(backend.records, current) {
		t.Fatal("current startup rewrote canonical records")
	}
	if err := backend.View(ctx, func(tx spi.ReadTx) error { return repo.VerifyCurrentStateInTransaction(ctx, tx) }); err != nil {
		t.Fatal(err)
	}
}

func TestBackendRepoStartupReplaysJournalBeforeFinalVerification(t *testing.T) {
	ctx := context.Background()
	backend, err := storagebridge.Open(ctx, storagebridge.Source(publicstorage.Memory()))
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	now := time.Date(2026, 8, 18, 2, 0, 0, 0, time.UTC)
	base := newMemoryRepo(func() time.Time { return now })
	basePayload, err := base.EncodeMemoryState()
	if err != nil {
		t.Fatal(err)
	}
	final, err := DecodeMemoryState(basePayload, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := final.CreateThread(ctx, ThreadMeta{ID: "journal-thread", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	finalPayload, err := final.EncodeMemoryState()
	if err != nil {
		t.Fatal(err)
	}
	frame, changed, err := buildBackendDomainJournalFrame(1, basePayload, finalPayload)
	if err != nil || !changed {
		t.Fatalf("journal frame changed=%v err=%v", changed, err)
	}
	encodedFrame, err := encodeBackendDomainJournalFrame(frame)
	if err != nil {
		t.Fatal(err)
	}
	stateEnvelope, err := storagecodec.EncodeEnvelope("sessiontree", basePayload)
	if err != nil {
		t.Fatal(err)
	}
	inventoryEnvelope, err := encodeRootThreadInventory(final)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(ctx, func(tx spi.WriteTx) error {
		if err := tx.Put(backendDomainNamespace, backendStateKey, stateEnvelope); err != nil {
			return err
		}
		if err := tx.Put(backendDomainNamespace, backendRootThreadInventoryKey, inventoryEnvelope); err != nil {
			return err
		}
		return tx.Put(backendDomainJournalNamespace, backendJournalKey(1), encodedFrame)
	}); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewBackendRepo(ctx, backend, func() time.Time { return now })
	if err != nil {
		t.Fatalf("startup with pending journal failed: %v", err)
	}
	if _, err := reopened.Thread(ctx, "journal-thread"); err != nil {
		t.Fatalf("replayed thread missing after restart: %v", err)
	}
	if err := backend.View(ctx, func(tx spi.ReadTx) error { return rejectLegacyBackendDomain(ctx, tx) }); err != nil {
		t.Fatalf("v5 records survived migration: %v", err)
	}
}

func TestBackendRepoRepairsLegacyUTF8ToolResultProjection(t *testing.T) {
	for _, withJournal := range []bool{false, true} {
		name := "checkpoint"
		if withJournal {
			name = "journal"
		}
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			backend, err := storagebridge.Open(ctx, storagebridge.Source(publicstorage.Memory()))
			if err != nil {
				t.Fatal(err)
			}
			defer backend.Close()
			wantRaw := installLegacyUTF8ToolResultStore(t, backend, withJournal, true)

			repo, err := NewBackendRepo(ctx, backend, time.Now)
			if err != nil {
				t.Fatalf("startup repair failed: %v", err)
			}
			meta, err := repo.Thread(ctx, "thread")
			if err != nil {
				t.Fatal(err)
			}
			path, err := repo.Path(ctx, "thread", meta.LeafID)
			if err != nil {
				t.Fatal(err)
			}
			if len(path) != 1 || path[0].Raw != wantRaw || path[0].RawHash != StableHash(wantRaw) {
				t.Fatalf("repaired path=%#v, want canonical raw", path)
			}
			if _, replacements := normalizeLegacyJSONReplacementEscapes(path[0].Raw); replacements != 0 {
				t.Fatalf("legacy replacement escape survived: %q", path[0].Raw)
			}
			if err := backend.View(ctx, func(tx spi.ReadTx) error {
				records, err := scanBackendDomainJournal(ctx, tx)
				if err != nil {
					return err
				}
				if len(records) != 0 {
					t.Fatalf("repair did not checkpoint journal: %d records", len(records))
				}
				return repo.VerifyCurrentStateInTransaction(ctx, tx)
			}); err != nil {
				t.Fatal(err)
			}
			stateAfterFirstOpen := migrationTestNamespaceRecords(t, backend, backendDomainV7Namespace)
			if _, err := NewBackendRepo(ctx, backend, time.Now); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(migrationTestNamespaceRecords(t, backend, backendDomainV7Namespace), stateAfterFirstOpen) {
				t.Fatal("repaired state was rewritten during idempotent reopen")
			}
		})
	}
}

func TestBackendRepoRejectsOtherEntryProjectionMismatchWithoutMutation(t *testing.T) {
	ctx := t.Context()
	backend := newMigrationTestBackend()
	installLegacyUTF8ToolResultStore(t, backend, false, false)
	beforeState := migrationTestRecord(t, backend, backendDomainNamespace, backendStateKey)
	beforeInventory := migrationTestRecord(t, backend, backendDomainNamespace, backendRootThreadInventoryKey)

	if _, err := NewBackendRepo(ctx, backend, time.Now); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("startup error=%v, want ErrAuthorityCorrupt", err)
	}
	if got := migrationTestRecord(t, backend, backendDomainNamespace, backendStateKey); !bytes.Equal(got, beforeState) {
		t.Fatal("rejected state mismatch changed canonical state")
	}
	if got := migrationTestRecord(t, backend, backendDomainNamespace, backendRootThreadInventoryKey); !bytes.Equal(got, beforeInventory) {
		t.Fatal("rejected state mismatch changed root inventory")
	}
}

func TestBackendRepoLegacyUTF8RepairRollsBackAtomically(t *testing.T) {
	ctx := t.Context()
	backend := newMigrationTestBackend()
	installLegacyUTF8ToolResultStore(t, backend, false, true)
	beforeState := migrationTestRecord(t, backend, backendDomainNamespace, backendStateKey)
	beforeInventory := migrationTestRecord(t, backend, backendDomainNamespace, backendRootThreadInventoryKey)
	injected := errors.New("repair inventory write failed")

	if _, err := NewBackendRepo(ctx, migrationFailingBackend{Backend: backend, err: injected}, time.Now); !errors.Is(err, injected) {
		t.Fatalf("startup error=%v, want injected failure", err)
	}
	if got := migrationTestRecord(t, backend, backendDomainNamespace, backendStateKey); !bytes.Equal(got, beforeState) {
		t.Fatal("failed repair changed canonical state")
	}
	if got := migrationTestRecord(t, backend, backendDomainNamespace, backendRootThreadInventoryKey); !bytes.Equal(got, beforeInventory) {
		t.Fatal("failed repair changed root inventory")
	}
}

func installLegacyUTF8ToolResultStore(t *testing.T, backend spi.Backend, withJournal, repairable bool) string {
	t.Helper()
	ctx := t.Context()
	now := time.Date(2026, 8, 26, 15, 24, 0, 0, time.UTC)
	base := NewMemoryRepo()
	if _, err := base.CreateThread(ctx, ThreadMeta{ID: "thread", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	basePayload, err := base.EncodeMemoryState()
	if err != nil {
		t.Fatal(err)
	}
	final, err := DecodeMemoryState(basePayload, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	entry, err := final.Append(ctx, Entry{
		ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolResult,
		Message: session.Message{
			Role: session.Tool, Content: "nested JSON keeps \\ufffd while terminal output ends in \uFFFD", ToolCallID: "call", ToolName: "terminal.exec",
			ToolResult: &session.ToolResultView{Status: "success"},
		},
	}, AppendOptions{ID: "result", Now: now})
	if err != nil {
		t.Fatal(err)
	}
	canonicalRaw := entry.Raw
	items, err := final.rootThreadInventoryLocked()
	if err != nil {
		t.Fatal(err)
	}
	legacyRaw := strings.ReplaceAll(canonicalRaw, "\ufffd", legacyJSONReplacementEscape)
	if !repairable {
		legacyRaw = canonicalRaw + " "
	}
	if legacyRaw == canonicalRaw {
		t.Fatal("fixture did not create a distinct legacy projection")
	}
	final.mu.Lock()
	final.entries["thread"][0].Raw = legacyRaw
	final.entries["thread"][0].RawHash = StableHash(legacyRaw)
	final.mu.Unlock()
	items[0].Path[0].Raw = legacyRaw
	items[0].Path[0].RawHash = StableHash(legacyRaw)
	finalPayload, err := final.EncodeMemoryState()
	if err != nil {
		t.Fatal(err)
	}
	statePayload := finalPayload
	if withJournal {
		statePayload = basePayload
	}
	stateEnvelope, err := storagecodec.EncodeEnvelope("sessiontree", statePayload)
	if err != nil {
		t.Fatal(err)
	}
	inventoryEnvelope, err := encodeRootThreadInventoryItems(items)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(ctx, func(tx spi.WriteTx) error {
		if err := tx.Put(backendDomainNamespace, backendStateKey, stateEnvelope); err != nil {
			return err
		}
		if err := tx.Put(backendDomainNamespace, backendRootThreadInventoryKey, inventoryEnvelope); err != nil {
			return err
		}
		if !withJournal {
			return nil
		}
		frame, changed, err := buildBackendDomainJournalFrame(1, basePayload, finalPayload)
		if err != nil {
			return err
		}
		if !changed {
			return errors.New("legacy fixture journal did not change domain state")
		}
		encodedFrame, err := encodeBackendDomainJournalFrame(frame)
		if err != nil {
			return err
		}
		return tx.Put(backendDomainJournalNamespace, backendJournalKey(1), encodedFrame)
	}); err != nil {
		t.Fatal(err)
	}
	return canonicalRaw
}

func TestBackendRepoDoesNotWriteLegacyRecoveryJournal(t *testing.T) {
	ctx := context.Background()
	backend, err := storagebridge.Open(ctx, storagebridge.Source(publicstorage.Memory()))
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	now := time.Date(2026, 8, 25, 1, 0, 0, 0, time.UTC)
	repo, err := NewBackendRepo(ctx, backend, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < backendDomainJournalMaxFrames+1; index++ {
		if err := repo.UpdateDomain(ctx, func(memory *MemoryRepo, _ spi.WriteTx) error {
			memory.mu.Lock()
			defer memory.mu.Unlock()
			memory.seq++
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := backend.View(ctx, func(tx spi.ReadTx) error {
		records, err := scanBackendDomainJournal(ctx, tx)
		if err != nil {
			return err
		}
		if len(records) != 0 {
			t.Fatalf("durable journal contains %d records", len(records))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBackendDomainJournalUsageThresholds(t *testing.T) {
	if !(backendDomainJournalUsage{Sequence: backendDomainJournalMaxFrames}).requiresCheckpoint() {
		t.Fatal("frame limit did not require checkpoint")
	}
	if !(backendDomainJournalUsage{Bytes: backendDomainJournalMaxBytes}).requiresCheckpoint() {
		t.Fatal("byte limit did not require checkpoint")
	}
	if (backendDomainJournalUsage{Sequence: backendDomainJournalMaxFrames - 1, Bytes: backendDomainJournalMaxBytes - 1}).requiresCheckpoint() {
		t.Fatal("usage below both limits required checkpoint")
	}
}

func TestBackendRepoRejectsCorruptJournalWithoutDeletingIt(t *testing.T) {
	ctx := context.Background()
	backend, err := storagebridge.Open(ctx, storagebridge.Source(publicstorage.Memory()))
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	if _, err := NewBackendRepo(ctx, backend, time.Now); err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(ctx, func(tx spi.WriteTx) error {
		return tx.Put(backendDomainJournalNamespace, backendJournalKey(1), []byte("corrupt"))
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := NewBackendRepo(ctx, backend, time.Now); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("corrupt journal error=%v, want ErrAuthorityCorrupt", err)
	}
	if err := backend.View(ctx, func(tx spi.ReadTx) error {
		_, err := tx.Get(backendDomainJournalNamespace, backendJournalKey(1))
		return err
	}); err != nil {
		t.Fatalf("corrupt journal was deleted during failed startup: %v", err)
	}
}

func legacyEmptyMemoryState(t *testing.T, version int) []byte {
	t.Helper()
	encoded, err := NewMemoryRepo().EncodeMemoryState()
	if err != nil {
		t.Fatal(err)
	}
	var state map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &state); err != nil {
		t.Fatal(err)
	}
	state["version"] = json.RawMessage([]byte{'0' + byte(version)})
	for _, field := range []string{
		"leases", "lease_generation", "authority_claims", "subagent_inputs", "subagent_input_sequence",
		"subagent_publications", "subagent_input_requests", "root_create_intents", "turn_admissions", "turn_finishes",
		"approval_queues", "approvals", "approval_by_effect_attempt", "approval_decisions", "subagent_close_operations",
		"pending_tool_completions", "subagent_pending_tool_completions", "compaction_operations", "thread_revisions",
		"thread_revision_history",
	} {
		state[field] = json.RawMessage(`{}`)
	}
	state["lease_policy"] = json.RawMessage(`{"TTL":30000000000,"RenewInterval":10000000000,"ClockSkewAllowance":2000000000}`)
	out, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func legacyActiveSubAgentState(t *testing.T) (memoryState, SubAgentInputRecord, legacyTurnLease) {
	t.Helper()
	now := time.Date(2026, 8, 17, 8, 0, 0, 0, time.UTC)
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "parent", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "child", ParentThreadID: "parent", TaskName: "worker", AgentPath: "/root/worker", HostProfileRef: "test", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	started, err := repo.Append(t.Context(), Entry{ThreadID: "child", TurnID: "turn", RunID: "run", Type: EntryTurnMarker, TurnStatus: TurnStarted, Metadata: map[string]string{"run_id": "run"}}, AppendOptions{ID: "started", Now: now})
	if err != nil {
		t.Fatal(err)
	}
	message := session.Message{Role: session.User, Content: "continue"}
	if _, err := repo.Append(t.Context(), Entry{ThreadID: "child", TurnID: "turn", RunID: "run", Type: EntryUserMessage, Message: message, Metadata: map[string]string{SubAgentInputIDMetadataKey: "input", SubAgentUserMessageOriginMetadataKey: SubAgentUserMessageOriginInput}}, AppendOptions{ID: "user", ParentID: started.ID, Now: now}); err != nil {
		t.Fatal(err)
	}
	payload := legacyEmptyMemoryState(t, 2)
	var state memoryState
	if err := json.Unmarshal(payload, &state); err != nil {
		t.Fatal(err)
	}
	current := repo.memoryStateLocked()
	state.Threads, state.Entries = current.Threads, current.Entries
	state.EntryOrdinals, state.EntryDepths = current.EntryOrdinals, current.EntryDepths
	state.TurnEntryOrdinals, state.TurnEntryCounts = current.TurnEntryOrdinals, current.TurnEntryCounts
	input := SubAgentInputRecord{SubAgentInputID: "input", ParentThreadID: "parent", ChildThreadID: "child", RequestKind: SubAgentRequestInput, State: SubAgentInputAdmitted, Message: message, AdmittedTurnID: "turn", AdmittedRunID: "run", AdmittedAt: now}
	lease := legacyTurnLease{ThreadID: "child", Purpose: "turn", TurnID: "turn", OwnerID: "owner", Generation: 1, Heartbeat: 1, AcquiredAt: now, RenewedAt: now, ExpiresAt: now.Add(time.Minute)}
	state.SubAgentInputs, _ = json.Marshal(map[string][]SubAgentInputRecord{"child": {input}})
	state.Leases, _ = json.Marshal(map[string]legacyTurnLease{"child": lease})
	state.LeaseGeneration, _ = json.Marshal(map[string]int64{"child": 1})
	return state, input, lease
}

type migrationTestBackend struct{ records map[string]map[string][]byte }

func newMigrationTestBackend() *migrationTestBackend {
	return &migrationTestBackend{records: make(map[string]map[string][]byte)}
}
func (backend *migrationTestBackend) View(_ context.Context, read func(spi.ReadTx) error) error {
	return read(&migrationTestTx{records: cloneMigrationRecords(backend.records), readOnly: true})
}
func (backend *migrationTestBackend) Update(_ context.Context, mutate func(spi.WriteTx) error) error {
	clone := cloneMigrationRecords(backend.records)
	if err := mutate(&migrationTestTx{records: clone}); err != nil {
		return err
	}
	backend.records = clone
	return nil
}
func (*migrationTestBackend) Close() error { return nil }

type migrationTestTx struct {
	records  map[string]map[string][]byte
	readOnly bool
}

func (tx *migrationTestTx) Get(namespace string, key []byte) ([]byte, error) {
	value, ok := tx.records[namespace][string(key)]
	if !ok {
		return nil, spi.ErrNotFound
	}
	return bytes.Clone(value), nil
}
func (tx *migrationTestTx) Scan(request spi.ScanRequest) (spi.ScanPage, error) {
	if request.Namespace == "" || request.Limit <= 0 {
		return spi.ScanPage{}, spi.ErrInvalidArgument
	}
	keys := make([]string, 0, len(tx.records[request.Namespace]))
	for key := range tx.records[request.Namespace] {
		if len(request.Start) != 0 && bytes.Compare([]byte(key), request.Start) < 0 ||
			len(request.End) != 0 && bytes.Compare([]byte(key), request.End) >= 0 ||
			len(request.After) != 0 && bytes.Compare([]byte(key), request.After) <= 0 {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	page := spi.ScanPage{}
	if len(keys) > request.Limit {
		page.HasMore = true
		keys = keys[:request.Limit]
	}
	for _, key := range keys {
		page.Records = append(page.Records, spi.Record{Key: []byte(key), Value: bytes.Clone(tx.records[request.Namespace][key])})
	}
	if page.HasMore {
		page.Next = bytes.Clone(page.Records[len(page.Records)-1].Key)
	}
	return page, nil
}
func (tx *migrationTestTx) Put(namespace string, key, value []byte) error {
	if tx.readOnly {
		return spi.ErrTransactionClosed
	}
	if tx.records[namespace] == nil {
		tx.records[namespace] = make(map[string][]byte)
	}
	tx.records[namespace][string(key)] = bytes.Clone(value)
	return nil
}
func (tx *migrationTestTx) Delete(namespace string, key []byte) error {
	if tx.readOnly {
		return spi.ErrTransactionClosed
	}
	delete(tx.records[namespace], string(key))
	return nil
}

type migrationFailingBackend struct {
	spi.Backend
	err error
}

func (backend migrationFailingBackend) Update(ctx context.Context, mutate func(spi.WriteTx) error) error {
	return backend.Backend.Update(ctx, func(tx spi.WriteTx) error { return mutate(migrationFailingTx{WriteTx: tx, err: backend.err}) })
}

type migrationFailingTx struct {
	spi.WriteTx
	err error
}

func (tx migrationFailingTx) Put(namespace string, key, value []byte) error {
	if namespace == backendDomainV7Namespace && bytes.Equal(key, backendDomainV7Key(backendDomainRecordRootIndex)) {
		return tx.err
	}
	return tx.WriteTx.Put(namespace, key, value)
}

func cloneMigrationRecords(source map[string]map[string][]byte) map[string]map[string][]byte {
	out := make(map[string]map[string][]byte, len(source))
	for namespace, records := range source {
		out[namespace] = make(map[string][]byte, len(records))
		for key, value := range records {
			out[namespace][key] = bytes.Clone(value)
		}
	}
	return out
}
func migrationTestRecord(t *testing.T, backend spi.Backend, namespace string, key []byte) []byte {
	t.Helper()
	var value []byte
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error { var err error; value, err = tx.Get(namespace, key); return err }); err != nil {
		t.Fatal(err)
	}
	return value
}

func migrationTestNamespaceRecords(t *testing.T, backend spi.Backend, namespace string) map[string][]byte {
	t.Helper()
	records := make(map[string][]byte)
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		var after []byte
		for {
			page, err := tx.Scan(spi.ScanRequest{Namespace: namespace, After: after, Limit: 256})
			if err != nil {
				return err
			}
			for _, record := range page.Records {
				records[string(record.Key)] = bytes.Clone(record.Value)
			}
			if !page.HasMore {
				return nil
			}
			after = page.Next
		}
	}); err != nil {
		t.Fatal(err)
	}
	return records
}

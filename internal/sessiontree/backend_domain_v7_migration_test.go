package sessiontree

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/floegence/floret/v6/internal/provider"
	"github.com/floegence/floret/v6/internal/session"
	"github.com/floegence/floret/v6/internal/storagecodec"
	"github.com/floegence/floret/v6/storage/spi"
)

func TestBackendDomainV6ToV7BackfillsRunIdentityAndTerminatesUnknownEffects(t *testing.T) {
	ctx := t.Context()
	now := time.Date(2026, 8, 30, 10, 0, 0, 0, time.UTC)
	memory := newMemoryRepo(func() time.Time { return now })
	if _, err := memory.CreateThread(ctx, ThreadMeta{ID: "thread", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := memory.AcceptTurn(ctx, AcceptTurnRequest{
		ThreadID: "thread", TurnID: "turn-history", RunID: "run-history", LogicalRequestID: "request-history",
		RequestFingerprint: "accept-history", InputRequestFingerprint: "input-history",
		Input: session.Message{Role: session.User, Content: "history"}, Now: now,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := memory.AppendRuntimeFacts(ctx, "thread", []Entry{{
		ID: "assistant-history", ThreadID: "thread", TurnID: "turn-history", RunID: "run-history",
		Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "done"}, CreatedAt: now,
	}, {
		ID: "interaction-requested:history", ThreadID: "thread", TurnID: "turn-history", RunID: "run-history",
		Type: EntryInteractionAsked, Payload: json.RawMessage(`{"id":"history"}`), CreatedAt: now,
	}, {
		ID: "interaction-resolved:history", ThreadID: "thread", TurnID: "turn-history", RunID: "run-history",
		Type: EntryInteractionDone, Payload: json.RawMessage(`{"accepted":true}`), CreatedAt: now,
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := memory.FinishTurn(ctx, FinishTurnRequest{
		ThreadID: "thread", TurnID: "turn-history", RunID: "run-history", TerminalEntryID: "terminal-history",
		Status: TurnCompleted, OutcomeFingerprint: "outcome-history", Now: now,
	}); err != nil {
		t.Fatal(err)
	}
	for index := range memory.entries["thread"] {
		entry := &memory.entries["thread"][index]
		if entry.TurnID == "turn-history" && presentationEntryRequiresRunIdentity(entry.Type) {
			entry.RunID = ""
		}
	}
	if err := memory.replaceIndexedEntriesLocked("thread", memory.entries["thread"]); err != nil {
		t.Fatal(err)
	}
	if _, err := memory.AcceptTurn(ctx, AcceptTurnRequest{
		ThreadID: "thread", TurnID: "turn-active", RunID: "run-active", LogicalRequestID: "request-active",
		RequestFingerprint: "accept-active", InputRequestFingerprint: "input-active",
		Input: session.Message{Role: session.User, Content: "delegate"}, Now: now.Add(time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 2; index++ {
		callID := fmt.Sprintf("subagent-%d", index)
		if _, err := memory.AppendRuntimeFacts(ctx, "thread", []Entry{{
			ID: "tool-call:" + callID, ThreadID: "thread", TurnID: "turn-active", RunID: "run-active", Type: EntryToolCall,
			Message: session.Message{Role: session.Assistant, ToolCallID: callID, ToolName: "subagents", ToolArgs: `{}`}, CreatedAt: now.Add(time.Second),
		}}); err != nil {
			t.Fatal(err)
		}
		invocation := EffectInvocationIdentity{
			ThreadID: "thread", TurnID: "turn-active", RunID: "run-active", ToolCallID: callID, ToolName: "subagents", ArgumentHash: StableHash(`{}`),
		}
		prepared, err := memory.PrepareEffectAttempt(ctx, PrepareEffectAttemptRequest{Invocation: invocation, RequestFingerprint: "effect-" + callID, Now: now.Add(time.Second)})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := memory.BeginEffectDispatch(ctx, BeginEffectDispatchRequest{
			EffectAttemptID: prepared.Attempt.EffectAttemptID, RequestFingerprint: prepared.Attempt.RequestFingerprint,
			AuthorizationProofHash: "proof-" + callID, Now: now.Add(time.Second),
		}); err != nil {
			t.Fatal(err)
		}
	}
	meta := memory.threads["thread"]
	memory.providerStates["thread"] = ProviderStateRecord{
		ThreadID: "thread", LeafEntryID: meta.LeafID, CompatibilityKey: "provider-key",
		State: provider.State{Kind: "continuation", ID: "provider-state"}, CreatedByTurnID: "turn-active", CreatedByRunID: "run-active", UpdatedAt: now,
	}

	backend := newMigrationTestBackend()
	if err := backend.Update(ctx, func(tx spi.WriteTx) error { return saveBackendDomainV6Fixture(tx, memory) }); err != nil {
		t.Fatal(err)
	}
	beforeFailure := cloneMigrationRecords(backend.records)
	injected := errors.New("v7 migration write failed")
	if _, err := NewBackendRepo(ctx, migrationFailingBackend{Backend: backend, err: injected}, func() time.Time { return now.Add(2 * time.Second) }); !errors.Is(err, injected) {
		t.Fatalf("migration error=%v, want injected failure", err)
	}
	if !equalMigrationRecords(backend.records, beforeFailure) {
		t.Fatal("failed v6 to v7 migration changed canonical records")
	}
	repo, err := NewBackendRepo(ctx, backend, func() time.Time { return now.Add(2 * time.Second) })
	if err != nil {
		t.Fatal(err)
	}
	meta, err = repo.Thread(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	path, err := repo.Path(ctx, "thread", meta.LeafID)
	if err != nil {
		t.Fatal(err)
	}
	var failureCount, closedTools int
	for _, entry := range path {
		if presentationEntryRequiresRunIdentity(entry.Type) && entry.TurnID == "turn-history" && entry.RunID != "run-history" {
			t.Fatalf("historical entry %q run_id=%q", entry.ID, entry.RunID)
		}
		if entry.Type == EntryTurnMarker && entry.TurnID == "turn-active" && entry.TurnStatus == TurnFailed {
			failureCount++
			if entry.Metadata[TurnFailureCodeMetadataKey] != TurnFailureEffectOutcomeUnknown {
				t.Fatalf("failure code=%q", entry.Metadata[TurnFailureCodeMetadataKey])
			}
		}
		if entry.Type == EntryToolResult && entry.TurnID == "turn-active" && entry.Message.ToolResult != nil && entry.Message.ToolResult.Status == "error" {
			closedTools++
		}
	}
	if failureCount != 1 || closedTools != 2 {
		t.Fatalf("failure terminals=%d closed tools=%d", failureCount, closedTools)
	}
	if _, err := repo.ProviderState(ctx, "thread"); !errors.Is(err, ErrProviderStateNotFound) {
		t.Fatalf("provider continuation survived migration: %v", err)
	}
	if err := backend.View(ctx, func(tx spi.ReadTx) error {
		if records, err := scanBackendDomainV6(ctx, tx); err != nil || len(records) != 0 {
			return fmt.Errorf("v6 records=%d err=%w", len(records), err)
		}
		_, found, err := loadBackendDomainV7(ctx, tx, time.Now)
		if err != nil || !found {
			return fmt.Errorf("v7 found=%v err=%w", found, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := NewBackendRepo(ctx, backend, func() time.Time { return now.Add(3 * time.Second) }); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewBackendRepo(ctx, backend, func() time.Time { return now.Add(4 * time.Second) })
	if err != nil {
		t.Fatal(err)
	}
	meta, _ = reopened.Thread(ctx, "thread")
	path, _ = reopened.Path(ctx, "thread", meta.LeafID)
	failureCount = 0
	for _, entry := range path {
		if entry.Type == EntryTurnMarker && entry.TurnID == "turn-active" && entry.TurnStatus == TurnFailed {
			failureCount++
		}
	}
	if failureCount != 1 {
		t.Fatalf("restarts produced %d unknown-effect terminals", failureCount)
	}
}

func TestBackendDomainV6ToV7RejectsFutureRecordWithoutMutation(t *testing.T) {
	ctx := t.Context()
	backend := newMigrationTestBackend()
	memory := newMemoryRepo(time.Now)
	if err := backend.Update(ctx, func(tx spi.WriteTx) error {
		if err := saveBackendDomainV6Fixture(tx, memory); err != nil {
			return err
		}
		value, err := json.Marshal(backendDomainV6Manifest{Version: backendDomainV6Version})
		if err != nil {
			return err
		}
		record, err := json.Marshal(backendDomainV6Record{Version: backendDomainV6Version + 1, Kind: backendDomainRecordManifest, Value: value})
		if err != nil {
			return err
		}
		encoded, err := storagecodec.EncodeEnvelope("sessiontree-v6-record", record)
		if err != nil {
			return err
		}
		return tx.Put(backendDomainV6Namespace, backendDomainV6Key(backendDomainRecordManifest), encoded)
	}); err != nil {
		t.Fatal(err)
	}
	before := cloneMigrationRecords(backend.records)
	if _, err := NewBackendRepo(ctx, backend, time.Now); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("startup error=%v, want authority corrupt", err)
	}
	if !equalMigrationRecords(backend.records, before) {
		t.Fatal("future v6 record changed canonical records")
	}
}

func saveBackendDomainV6Fixture(tx spi.WriteTx, memory *MemoryRepo) error {
	put := func(key []byte, kind, id, threadID string, ordinal int, value any) error {
		payload, err := json.Marshal(value)
		if err != nil {
			return err
		}
		record, err := json.Marshal(backendDomainV6Record{Version: backendDomainV6Version, Kind: kind, ID: id, ThreadID: threadID, Ordinal: ordinal, Value: payload})
		if err != nil {
			return err
		}
		encoded, err := storagecodec.EncodeEnvelope("sessiontree-v6-record", record)
		if err != nil {
			return err
		}
		return tx.Put(backendDomainV6Namespace, key, encoded)
	}
	if err := put(backendDomainV6Key(backendDomainRecordManifest), backendDomainRecordManifest, "", "", 0, backendDomainV6Manifest{Version: backendDomainV6Version, Sequence: memory.seq}); err != nil {
		return err
	}
	if err := put(backendDomainV6Key(backendDomainRecordRootIndex), backendDomainRecordRootIndex, "", "", 0, backendDomainV6RootIndex{ThreadIDs: backendDomainV7RootThreadIDs(memory)}); err != nil {
		return err
	}
	for id, value := range memory.threads {
		if err := put(backendDomainV6Key(backendDomainRecordThread, id), backendDomainRecordThread, id, "", 0, value); err != nil {
			return err
		}
	}
	for threadID, entries := range memory.entries {
		for ordinal, entry := range entries {
			if err := put(backendDomainV6EntryKey(threadID, ordinal), backendDomainRecordEntry, entry.ID, threadID, ordinal, entry); err != nil {
				return err
			}
		}
	}
	for id, value := range memory.providerStates {
		if err := put(backendDomainV6Key(backendDomainRecordProviderState, id), backendDomainRecordProviderState, id, "", 0, value); err != nil {
			return err
		}
	}
	return nil
}

package sessiontree

import (
	"context"
	"errors"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
	"reflect"
	"testing"
	"time"
)

func installV24OutputSourceStore(t *testing.T, backend *migrationTestBackend) *MemoryRepo {
	t.Helper()
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if _, err := memory.AcceptTurn(t.Context(), AcceptTurnRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", RequestFingerprint: "accept", InputRequestFingerprint: "input", Input: session.Message{Role: session.User, Content: "hello"}, Now: time.Now()}); err != nil {
		t.Fatal(err)
	}
	delete(memory.entries["thread"][0].Metadata, ResponseSegmentsMetadataKey)
	memory.entries["thread"][0] = PrepareEntry(memory.entries["thread"][0])
	messages := []struct {
		id      string
		message session.Message
	}{
		{"first", session.Message{Role: session.Assistant, Content: "A", Reasoning: "shared"}},
		{"second", session.Message{Role: session.Assistant, Content: "B", Reasoning: "shared"}},
		{"invalid", session.Message{Role: session.Assistant, Kind: session.MessageKindToolValidationError, ToolCallID: "bad", ToolName: "inspect", ToolArgs: "{}", Reasoning: "batch"}},
		{"valid", session.Message{Role: session.Assistant, ToolCallID: "good", ToolName: "inspect", ToolArgs: "{}", Reasoning: "batch"}},
		{"bad-result", session.Message{Role: session.Tool, Kind: session.MessageKindToolValidationError, ToolCallID: "bad", ToolName: "inspect", Content: "schema validation"}},
		{"good-result", session.Message{Role: session.Tool, ToolCallID: "good", ToolName: "inspect", Content: "ok"}},
		{"hidden", session.Message{Role: session.Assistant, Kind: session.MessageKindToolValidationError, ToolCallID: "hidden", ToolName: "inspect", ToolArgs: "{}", Reasoning: "correction"}},
		{"hidden-result", session.Message{Role: session.Tool, Kind: session.MessageKindToolValidationError, ToolCallID: "hidden", ToolName: "inspect", Content: "schema validation"}},
		{"answer", session.Message{Role: session.Assistant, Content: "done", Reasoning: "final"}},
	}
	for _, value := range messages {
		entry := RunMessageEntry("thread", "turn", "run", value.message, time.Now())
		entry.ID = value.id
		if _, err := memory.Append(t.Context(), entry, AppendOptions{ID: value.id}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := memory.FinishTurn(t.Context(), FinishTurnRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", TerminalEntryID: "done", Status: TurnCompleted, OutcomeFingerprint: "done", Metadata: map[string]string{FinalResponseEntriesMetadataKey: `["answer"]`}, Now: time.Now()}); err != nil {
		t.Fatal(err)
	}
	for _, pair := range [][2]string{{"thread", "fork"}, {"fork", "nested"}} {
		if _, err := memory.Fork(t.Context(), ForkOptions{SourceThreadID: pair[0], NewThreadID: pair[1], Now: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	// Exact released shape has no output-source fields. Do not manufacture native IDs.
	for id, entries := range memory.entries {
		for i := range entries {
			entries[i].Message.ContentSourceID = ""
			entries[i].Message.ReasoningSourceID = ""
			entries[i] = PrepareEntry(entries[i])
		}
		memory.entries[id] = entries
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return saveCompleteBackendDomainV24(tx, memory) }); err != nil {
		t.Fatal(err)
	}
	return memory
}

func TestBackendDomainV24ToV25OutputSources(t *testing.T) {
	backend := newMigrationTestBackend()
	before := installV24OutputSourceStore(t, backend)
	repo, err := NewBackendRepo(t.Context(), backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	for id := range repo.domainMemory.entries {
		entries, err := repo.Entries(t.Context(), id)
		if err != nil {
			t.Fatal(err)
		}
		original := before.entries[id]
		for i, entry := range entries {
			stripped := entry
			stripped.Message.ContentSourceID = ""
			stripped.Message.ReasoningSourceID = ""
			stripped = PrepareEntry(stripped)
			stripped.CreatedAt = stripped.CreatedAt.UTC()
			original[i].CreatedAt = original[i].CreatedAt.UTC()
			if !reflect.DeepEqual(stripped, original[i]) {
				t.Fatalf("canonical fact changed thread=%s entry=%s\nafter=%+v\nbefore=%+v", id, entry.ID, stripped, original[i])
			}
		}
	}
	entries, err := repo.Entries(t.Context(), "thread")
	if err != nil {
		t.Fatal(err)
	}
	byID := map[string]Entry{}
	for _, e := range entries {
		byID[e.ID] = e
	}
	for id, want := range map[string]string{"first": "assistant:turn:1", "second": "assistant:turn:1", "answer": "assistant:turn:2"} {
		if got := byID[id].Message.ContentSourceID; got != want {
			t.Fatalf("%s source=%s want=%s", id, got, want)
		}
	}
	if byID["first"].Message.ReasoningSourceID != "thinking:turn:1" || byID["second"].Message.ReasoningSourceID != "thinking:turn:1" || byID["valid"].Message.ReasoningSourceID != "thinking:turn:2" || byID["invalid"].Message.ReasoningSourceID != "thinking:turn:2" || byID["hidden"].Message.ReasoningSourceID != "thinking:entry:hidden" || byID["answer"].Message.ReasoningSourceID != "thinking:turn:3" {
		t.Fatalf("incorrect reasoning sources: %+v", byID)
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error { _, _, err := loadBackendDomainV24(t.Context(), tx, time.Now); return err }); err == nil {
		t.Fatal("old writer accepted new envelope")
	}
	current := cloneMigrationRecords(backend.records)
	if _, err := NewBackendRepo(t.Context(), backend, time.Now); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(current, backend.records) {
		t.Fatal("current restart rewrote records")
	}
}

func TestBackendDomainV24ToV25RollsBackFailures(t *testing.T) {
	for _, failure := range []string{"write", "cancel", "panic", "source_drift", "future"} {
		t.Run(failure, func(t *testing.T) {
			backend := newMigrationTestBackend()
			memory := installV24OutputSourceStore(t, backend)
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
			case "source_drift":
				memory.entries["thread"][1].RunID = ""
				format := backendDomainV24Format
				format.validate = func(*MemoryRepo) error { return nil }
				if err := backend.Update(ctx, func(tx spi.WriteTx) error { return saveCompleteBackendDomain(tx, memory, format) }); err != nil {
					t.Fatal(err)
				}
			case "future":
				format := backendDomainV25Format
				format.version++
				raw, err := encodeBackendDomainRecord(format, backendDomainRecordManifest, "", "", 0, backendDomainManifest{Version: format.version})
				if err != nil {
					t.Fatal(err)
				}
				if err := backend.Update(ctx, func(tx spi.WriteTx) error {
					return tx.Put(format.namespace, backendDomainV25Key(backendDomainRecordManifest), raw)
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
					t.Error("invalid migration succeeded")
				}
			}()
			if !reflect.DeepEqual(before, backend.records) {
				t.Fatal("failed migration mutated backend")
			}
		})
	}
}

func TestOutputSourcesRejectConflictingAdmission(t *testing.T) {
	memory := NewMemoryRepo()
	request := providerContextFixture(t, memory)
	request.Entries = []Entry{RunMessageEntry("thread", "turn", "run", session.Message{Role: session.Assistant, Reasoning: "first", ReasoningSourceID: "output:source"}, time.Time{})}
	if _, err := memory.CommitProviderContext(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	before, _ := memory.EncodeMemoryState()
	request.Entries[0].Message.Reasoning = "different"
	if _, err := memory.CommitProviderContext(t.Context(), request); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("conflicting source admitted: %v", err)
	}
	after, _ := memory.EncodeMemoryState()
	if string(before) != string(after) {
		t.Fatal("conflicting source changed canonical state")
	}
}

func TestLegacyOutputSourcesPreserveControlCountersAndBranchAuthority(t *testing.T) {
	entries := []Entry{
		{ID: "a", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "first"}},
		{ID: "complete", ParentID: "a", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, Kind: session.MessageKindControlSignal, Content: "tool_call", ToolCallID: "complete", ToolName: "retired_complete", ControlSignal: &session.ControlSignalView{CallID: "complete", Name: "retired_complete", Disposition: "terminal", OutputText: "done"}}},
		{ID: "question", ParentID: "complete", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, Kind: session.MessageKindControlSignal, Content: "tool_call", ToolCallID: "question", ToolName: "ask_user", ControlSignal: &session.ControlSignalView{CallID: "question", Name: "ask_user", Disposition: "waiting", Payload: controlResultTestPayload()}}},
		{ID: "failed", ParentID: "question", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, Kind: session.MessageKindControlSignal, Content: "tool_call", ToolCallID: "failed", ToolName: "ask_user", ControlSignal: &session.ControlSignalView{CallID: "failed", Name: "ask_user", Disposition: "failed", ErrorCode: session.ControlSignalErrorCodeControlError}}},
		{ID: "left", ParentID: "failed", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "left"}},
		{ID: "right", ParentID: "failed", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "right"}},
	}
	normalized, err := normalizeLegacyOutputSources(t.Context(), entries)
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{"assistant:turn:1", "assistant:turn:2", "", "", "assistant:turn:3", "assistant:turn:3"} {
		if got := normalized[i].Message.ContentSourceID; got != want {
			t.Fatalf("entry %s=%s want %s", normalized[i].ID, got, want)
		}
	}
	memory := NewMemoryRepo()
	memory.entries["thread"] = normalized
	if err := validateOutputSources(memory); err != nil {
		t.Fatalf("independent branches conflated: %v", err)
	}
}

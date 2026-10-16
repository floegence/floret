package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/tools"
)

func parentEventRepoFixture(t *testing.T) (*BackendRepo, *migrationTestBackend) {
	t.Helper()
	backend := newMigrationTestBackend()
	repo, err := NewBackendRepo(WithParentEvents(t.Context(), true), backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "child", ParentThreadID: "parent", ParentTurnID: "delegation", TaskName: "child", TaskDescription: "work", AgentPath: "/child", HostProfileRef: "test"}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.AcceptTurn(t.Context(), AcceptTurnRequest{ThreadID: "child", TurnID: "turn", RunID: "run", LogicalRequestID: "send", RequestFingerprint: "send", InputRequestFingerprint: "send", Input: session.Message{Role: session.User, Content: "work"}}); err != nil {
		t.Fatal(err)
	}
	return repo, backend
}

func TestParentEventsTerminalKindsAndTypedTimeout(t *testing.T) {
	for _, scenario := range []string{"completed", "failed", "cancelled", "interrupted", "timed_out", "untyped_timeout"} {
		t.Run(scenario, func(t *testing.T) {
			repo, _ := parentEventRepoFixture(t)
			status := TurnFailed
			metadata := map[string]string{TurnFailureCodeMetadataKey: TurnFailureProvider}
			switch scenario {
			case "completed":
				status = TurnCompleted
				metadata = nil
			case "cancelled":
				status = TurnAborted
				metadata[TurnFailureCodeMetadataKey] = TurnFailureCancelled
			case "interrupted":
				status = TurnAborted
				metadata[TurnFailureCodeMetadataKey] = TurnFailureCancelled
			case "timed_out":
				now := time.Now()
				data, _ := json.Marshal(provider.RequestTimeoutDetails{Stage: "first_output", StartedAt: now, TimedOutAt: now.Add(time.Second)})
				metadata["provider_timeout"] = string(data)
			case "untyped_timeout":
				metadata["provider_timeout"] = "context deadline exceeded"
			}
			request := FinishTurnRequest{ThreadID: "child", TurnID: "turn", RunID: "run", TerminalEntryID: "terminal", OutcomeFingerprint: "terminal", Status: status, Metadata: metadata, FailureMessage: map[bool]string{true: "canonical failure"}[status == TurnFailed]}
			if _, err := repo.FinishTurn(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			if _, err := repo.FinishTurn(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			page, err := repo.ParentEvents(t.Context(), ParentEventQuery{ThreadID: "parent"})
			if err != nil || len(page.Events) != 1 {
				t.Fatalf("page=%+v %v", page, err)
			}
			want := scenario
			if scenario == "untyped_timeout" {
				want = "failed"
			}
			if scenario == "interrupted" {
				want = "cancelled"
			}
			if page.Events[0].Kind != want {
				t.Fatalf("kind=%s want=%s", page.Events[0].Kind, want)
			}
		})
	}
}

func TestParentEventsSourceAndReceiptRollbackTogether(t *testing.T) {
	repo, backend := parentEventRepoFixture(t)
	before := cloneMigrationRecords(backend.records)
	repo.backend = parentEventFailingBackend{Backend: backend}
	request := FinishTurnRequest{ThreadID: "child", TurnID: "turn", RunID: "run", TerminalEntryID: "terminal", OutcomeFingerprint: "terminal", Status: TurnCompleted}
	if _, err := repo.FinishTurn(t.Context(), request); err == nil {
		t.Fatal("write should fail")
	}
	if !reflect.DeepEqual(before, backend.records) {
		t.Fatal("partial backend commit")
	}
	page, err := repo.ParentEvents(t.Context(), ParentEventQuery{ThreadID: "parent"})
	if err != nil || len(page.Events) != 0 {
		t.Fatal("partial receipt in memory")
	}
	if _, err := repo.Entry(t.Context(), "child", "terminal"); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("partial source in memory: %v", err)
	}
	repo.backend = backend
	if _, err := repo.FinishTurn(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	page, err = repo.ParentEvents(t.Context(), ParentEventQuery{ThreadID: "parent"})
	if err != nil || len(page.Events) != 1 {
		t.Fatal("retry lost receipt")
	}
}

func TestParentEventsOptInBootstrapsOnlyUnresolvedInteractionsOnce(t *testing.T) {
	repo, backend := parentEventRepoFixture(t)
	repo.domainMemory.parentEvents = false
	payload := json.RawMessage("{\"kind\":\"approval\"}")
	if _, err := repo.AppendRuntimeFacts(t.Context(), "child", []Entry{{ID: "interaction-requested:ask", ThreadID: "child", TurnID: "turn", RunID: "run", Type: EntryInteractionAsked, Payload: payload}}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return repo.PrepareParentEventsInTransaction(t.Context(), tx, true) }); err != nil {
			t.Fatal(err)
		}
		page, err := repo.ParentEvents(t.Context(), ParentEventQuery{ThreadID: "parent"})
		if err != nil || len(page.Events) != 1 || page.Events[0].Kind != "approval_required" {
			t.Fatalf("page=%+v %v", page, err)
		}
	}
	// No historical terminal is replayed when enabling the capability.
	other, _ := parentEventRepoFixture(t)
	other.domainMemory.parentEvents = false
	if _, err := other.FinishTurn(t.Context(), FinishTurnRequest{ThreadID: "child", TurnID: "turn", RunID: "run", TerminalEntryID: "old", OutcomeFingerprint: "old", Status: TurnCompleted}); err != nil {
		t.Fatal(err)
	}
	other.domainMemory.parentEvents = true
	if err := other.BootstrapParentEvents(t.Context()); err != nil {
		t.Fatal(err)
	}
	page, _ := other.ParentEvents(t.Context(), ParentEventQuery{ThreadID: "parent"})
	if len(page.Events) != 0 {
		t.Fatal("backfilled historical completion")
	}
}

func TestParentEventsProjectionBoundsIncludeEscapingAndPriority(t *testing.T) {
	records := make([]ParentEventRecord, 40)
	for i := range records {
		records[i] = ParentEventRecord{ParentEvent: ParentEvent{ID: strings.Repeat("e", 64) + string(rune('A'+i)), SourceThreadID: "child", SourceTurnID: "turn", SourceRunID: "run", SourceEntryID: "source", Kind: "report", Message: strings.Repeat("\"\\界\n", 400)}}
	}
	records[39].Kind = "approval_required"
	records[39].Message = ""
	records[39].InteractionID = "approval"
	for _, record := range records {
		line := parentEventProjection(record) + "\n"
		if len(line) > ParentEventMaxBytes || contextpolicy.EstimateTextTokens(line)+8 > ParentEventMaxTokens {
			t.Fatal("item bound")
		}
	}
	text, err := renderParentEvents(records)
	if err != nil {
		t.Fatal(err)
	}
	if len(text) > ParentEventBatchMaxBytes || contextpolicy.EstimateTextTokens(text)+8 > ParentEventBatchMaxTokens || !strings.Contains(text, "Fixed event range:") {
		t.Fatal("batch bound or omitted range")
	}
	if strings.Index(text, "approval_required") > strings.Index(text, "\"kind\":\"report\"") {
		t.Fatal("unresolved interaction must have priority")
	}
}

func TestParentEventsInvalidRecordsFailClosed(t *testing.T) {
	repo, _ := parentEventRepoFixture(t)
	if _, err := repo.FinishTurn(t.Context(), FinishTurnRequest{ThreadID: "child", TurnID: "turn", RunID: "run", TerminalEntryID: "terminal", OutcomeFingerprint: "terminal", Status: TurnCompleted}); err != nil {
		t.Fatal(err)
	}
	for _, corruption := range []string{"source", "relationship", "kind"} {
		t.Run(corruption, func(t *testing.T) {
			memory := cloneMemoryRepoForStartup(repo.domainMemory)
			entry := memory.entries["parent"][0]
			event, err := decodeParentEvent(entry)
			if err != nil {
				t.Fatal(err)
			}
			switch corruption {
			case "source":
				event.SourceEntryID = "missing"
			case "relationship":
				event.ParentThreadID = "other"
			case "kind":
				event.Kind = "running"
			}
			entry.Payload, _ = json.Marshal(event)
			memory.entries["parent"][0] = PrepareEntry(entry)
			if err := validateBackendDomainV19Memory(memory); err == nil {
				t.Fatal("corruption accepted")
			}
		})
	}
}

type parentEventFailingBackend struct{ spi.Backend }

func (b parentEventFailingBackend) Update(ctx context.Context, mutate func(spi.WriteTx) error) error {
	return b.Backend.Update(ctx, func(tx spi.WriteTx) error {
		if err := mutate(tx); err != nil {
			return err
		}
		return errors.New("injected commit failure")
	})
}

func TestParentEventsHostInputWaitsForCanonicalBatchAndAdmissionPairing(t *testing.T) {
	repo, _ := parentEventRepoFixture(t)
	call := Entry{ID: "call", ThreadID: "child", TurnID: "turn", RunID: "run", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, Content: "tool_call", ToolName: "work", ToolCallID: "call", ToolArgs: "{}"}}
	result := Entry{ID: "result", ThreadID: "child", TurnID: "turn", RunID: "run", Type: EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "work", ToolCallID: "call", Content: "input required", ToolResult: &session.ToolResultView{Status: "success", InputRequired: &tools.InputRequest{Summary: "Choose a source", Questions: []tools.InputQuestion{{ID: "source", Prompt: "Source?", Kind: "write"}}}}}}
	if _, err := repo.AppendRuntimeFacts(t.Context(), "child", []Entry{call, result}); err != nil {
		t.Fatal(err)
	}
	page, err := repo.ParentEvents(t.Context(), ParentEventQuery{ThreadID: "parent"})
	if err != nil || len(page.Events) != 0 {
		t.Fatal("premature host input event")
	}
	if _, err := repo.FinishTurn(t.Context(), FinishTurnRequest{ThreadID: "child", TurnID: "turn", RunID: "run", TerminalEntryID: "wait", OutcomeFingerprint: "wait", Status: TurnWaiting}); err != nil {
		t.Fatal(err)
	}
	page, err = repo.ParentEvents(t.Context(), ParentEventQuery{ThreadID: "parent"})
	if err != nil || len(page.Events) != 1 || page.Events[0].Resolved {
		t.Fatalf("page=%+v %v", page, err)
	}
}

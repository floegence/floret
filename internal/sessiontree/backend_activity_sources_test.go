package sessiontree

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/tools"
)

// Measuring the transaction separately prevents a cheap read API from hiding
// a writer that replays every prior result for the same tool invocation.
type activityAppendReadBackend struct {
	spi.Backend
	sources int
}
type activityAppendReadTx struct {
	spi.WriteTx
	owner *activityAppendReadBackend
}

func (tx activityAppendReadTx) Get(namespace string, key []byte) ([]byte, error) {
	if namespace == backendDomainV32Namespace {
		tx.owner.sources++
	}
	return tx.WriteTx.Get(namespace, key)
}
func (backend *activityAppendReadBackend) Update(ctx context.Context, visit func(spi.WriteTx) error) error {
	return backend.Backend.Update(ctx, func(tx spi.WriteTx) error { return visit(activityAppendReadTx{tx, backend}) })
}

func TestItemMembershipRepeatedToolResultAppendIsBounded(t *testing.T) {
	backend, repo := projectionSourcesFixture(t, 1)
	if err := repo.prepareWindowProjection(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	counted := &activityAppendReadBackend{Backend: backend}
	repo.backend = counted
	maximum := 0
	for i := 0; i < 128; i++ {
		counted.sources = 0
		_, err := repo.Append(t.Context(), Entry{ID: fmt.Sprint("bounded-update-", i), ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "inspect", ToolCallID: "work", Activity: &tools.ActivityPresentation{Label: "Inspect", Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Summary: fmt.Sprint(i)}}}}, AppendOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if counted.sources > 32 {
			t.Fatalf("append %d reread %d canonical records", i, counted.sources)
		}
		maximum = max(maximum, counted.sources)
	}
	t.Logf("128 repeated appends: maximum %d canonical record reads", maximum)
	repo.backend = backend
	page, err := repo.ItemMembershipWindow(t.Context(), ItemMembershipWindowOptions{ThreadID: "thread", Limit: 256})
	if err != nil {
		t.Fatal(err)
	}
	path, err := repo.Path(t.Context(), "thread", "")
	if err != nil {
		t.Fatal(err)
	}
	var oracle ItemMembership
	for _, source := range path {
		if _, err := oracle.Observe(source, uint64(source.PathDepth)); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(page.Members, oracle.Members) {
		t.Fatal("bounded append differs from complete canonical membership")
	}
}

func TestItemMembershipBoundedResultPreservesReasoningAndControlOwnership(t *testing.T) {
	for _, controlled := range []bool{false, true} {
		t.Run(fmt.Sprint(controlled), func(t *testing.T) {
			backend, repo := projectionSourcesFixture(t, 1)
			call := Entry{ID: "reasoned-call", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, ToolCallID: "reasoned", ToolName: "inspect", Reasoning: "retain visible thought"}}
			if _, err := repo.Append(t.Context(), call, AppendOptions{}); err != nil {
				t.Fatal(err)
			}
			if err := repo.prepareWindowProjection(t.Context(), "thread"); err != nil {
				t.Fatal(err)
			}
			if controlled {
				control := Entry{ID: "control-reuses-call", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, Kind: session.MessageKindControlSignal, ToolCallID: "reasoned", ToolName: "continue", ControlSignal: &session.ControlSignalView{Name: "continue", CallID: "reasoned", Disposition: "continue", OutputText: "exact control output"}}}
				if _, err := repo.Append(t.Context(), control, AppendOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			result := Entry{ID: "reasoned-result", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolResult, Message: session.Message{Role: session.Tool, ToolCallID: "reasoned", ToolName: "inspect", Content: "done"}}
			before := cloneMigrationRecords(backend.records)
			_, err := repo.Append(t.Context(), result, AppendOptions{})
			if controlled {
				if !errors.Is(err, ErrAuthorityCorrupt) || !reflect.DeepEqual(before, backend.records) {
					t.Fatalf("conflicting control result was published: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			page, err := repo.ItemMembershipWindow(t.Context(), ItemMembershipWindowOptions{ThreadID: "thread", Limit: 256})
			if err != nil {
				t.Fatal(err)
			}
			path, err := repo.Path(t.Context(), "thread", "")
			if err != nil {
				t.Fatal(err)
			}
			var oracle ItemMembership
			for _, source := range path {
				if _, err := oracle.Observe(source, uint64(source.PathDepth)); err != nil {
					t.Fatal(err)
				}
			}
			if !reflect.DeepEqual(page.Members, oracle.Members) {
				t.Fatal("reasoning or tool position differs")
			}
		})
	}
}

func TestItemActivitySourcesBoundRepeatedResults(t *testing.T) {
	for _, count := range []int{20, 300, 1000} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			memory := newMemoryRepo(time.Now)
			if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
				t.Fatal(err)
			}
			var want *tools.ActivityPresentation
			for i := 0; i <= count; i++ {
				value := &tools.ActivityPresentation{Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Summary: fmt.Sprint("current-", i)}}
				if i == 0 {
					value.Label = "Inspect"
					value.Payload = tools.StructuredActivityPayload{Inputs: []tools.StructuredActivityRow{{Content: strings.Repeat("retained 中文🌸", 1000), Format: tools.StructuredActivityRowFormatCode}}}
				}
				entry := Entry{ID: fmt.Sprint("source-", i), ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "inspect", ToolCallID: "call", Activity: value}}
				if i == 0 {
					entry.Type = EntryToolCall
					entry.Message.Role = session.Assistant
					entry.Message.ToolArgs = "{}"
				}
				if _, err := memory.Append(t.Context(), entry, AppendOptions{}); err != nil {
					t.Fatal(err)
				}
				want = tools.MergeActivityPresentations(want, value)
			}
			backend, repo := openV31ReviewFixture(t, memory)
			if err := repo.prepareWindowReadSources(t.Context(), "thread", windowPreparationNeeds{membership: true}); err != nil {
				t.Fatal(err)
			}
			repo = reopenCurrentStartupRepo(t, backend, false)
			measured := &currentStartupBackend{Backend: backend}
			repo.backend = measured
			input := ItemActivityInput{ThreadID: "thread", ItemID: "tool:turn:call", Path: "/payload/summary", Limit: 16}
			got, err := repo.ItemActivityStructure(t.Context(), input)
			if err != nil {
				t.Fatal(err)
			}
			field := got.Page.Field
			text, err := repo.ReadItemActivityField(t.Context(), input.ItemID, ActivityFieldInput{ThreadID: "thread", EntryID: field.EntryID, Path: field.SourcePath, Limit: 16384, SourceSHA256: field.SourceSHA256})
			if err != nil || text.Text != want.Payload.(tools.StructuredActivityPayload).Summary {
				t.Fatalf("result differs: %+v %v", text, err)
			}
			if measured.gets > 250 || measured.journalRows != 0 || measured.bodies > 4 {
				t.Fatalf("linear read: count=%d gets=%d bodies=%d journal=%d", count, measured.gets, measured.bodies, measured.journalRows)
			}
			t.Logf("sources=%d gets=%d bodies=%d journal=%d", count, measured.gets, measured.bodies, measured.journalRows)
		})
	}
}

func TestItemActivitySourcesAppendRollbackPathAndFrozenRead(t *testing.T) {
	backend, repo := projectionSourcesFixture(t, 1)
	appendSource := func(id, tool, text string) Entry {
		entry, err := repo.Append(t.Context(), Entry{ID: id, ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "inspect", ToolCallID: tool, Activity: &tools.ActivityPresentation{Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Summary: text}}}}, AppendOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return entry
	}
	appendSource("first", "activity", "original")
	input := ItemActivityInput{ThreadID: "thread", ItemID: "tool:turn:activity", Path: "/payload/summary", Limit: 16}
	first, err := repo.ItemActivityStructure(t.Context(), input)
	if err != nil {
		t.Fatal(err)
	}
	failed := Entry{ID: "failed-append", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "inspect", ToolCallID: "activity"}}
	before := cloneMigrationRecords(backend.records)
	failure := errors.New("failure")
	repo.backend = &domainCommitBoundaryBackend{Backend: backend, afterCallback: func() error { return failure }}
	if _, err := repo.Append(t.Context(), failed, AppendOptions{}); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, backend.records) {
		t.Fatal("rollback published field sources")
	}
	repo.backend = backend
	appendSource("second", "activity", "replacement")
	current, err := repo.ItemActivityStructure(t.Context(), input)
	if err != nil || current.Version == first.Version {
		t.Fatalf("replacement missing: %v", err)
	}
	old := first.Page.Field
	read := ActivityFieldInput{ThreadID: "thread", EntryID: old.EntryID, Path: old.SourcePath, SourceSHA256: old.SourceSHA256, Limit: 16384}
	frozen, err := repo.ReadItemActivityField(t.Context(), input.ItemID, read)
	if err != nil || frozen.Text != "original" {
		t.Fatalf("frozen source invalidated: %+v %v", frozen, err)
	}
	appendSource("other", "different", "private")
	read.EntryID = "other"
	read.SourceSHA256 = ""
	if _, err := repo.ReadItemActivityField(t.Context(), input.ItemID, read); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("foreign item authorized: %v", err)
	}
	if err := repo.MoveLeaf(t.Context(), "thread", "first"); err != nil {
		t.Fatal(err)
	}
	restored, err := repo.ItemActivityStructure(t.Context(), input)
	if err != nil || restored.Version != first.Version {
		t.Fatalf("branch rebuild differs: %v", err)
	}
	read.EntryID = "second"
	if _, err := repo.ReadItemActivityField(t.Context(), input.ItemID, read); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("inactive source authorized: %v", err)
	}
}

func TestItemActivitySourcesPreserveMissingPresentationDefaults(t *testing.T) {
	_, repo := projectionSourcesFixture(t, 1)
	if _, err := repo.Append(t.Context(), Entry{ID: "default-call", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: "default"}}, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	got, err := repo.ItemActivityStructure(t.Context(), ItemActivityInput{ThreadID: "thread", ItemID: "tool:turn:default", Path: "/label", Limit: 16})
	if err != nil || got.Page.Field.Text != "inspect" || got.Page.Field.EntryID != "" {
		t.Fatalf("default label differs: %+v %v", got, err)
	}
	if _, err := repo.Append(t.Context(), Entry{ID: "default-result", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "inspect", ToolCallID: "default"}}, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	got, err = repo.ItemActivityStructure(t.Context(), ItemActivityInput{ThreadID: "thread", ItemID: "tool:turn:default", Path: "/payload/status", Limit: 16})
	if err != nil || got.Page.Field.Text != "success" {
		t.Fatalf("default completion differs: %+v %v", got, err)
	}
}

func TestItemActivitySourcesRejectCorruptLocator(t *testing.T) {
	backend, repo := projectionSourcesFixture(t, 1)
	input := ItemActivityInput{ThreadID: "thread", ItemID: "tool:turn:work", Path: "", Limit: 16}
	initial, err := repo.ItemActivityStructure(t.Context(), input)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		state, err := readStoredActivitySources(tx, "thread", initial.Member.SourceID)
		if err != nil {
			return err
		}
		state.Last.Hash = "drift"
		return persistActivitySelection(tx, "thread", initial.Member.SourceID, state)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.ItemActivityStructure(t.Context(), input); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("corrupt source accepted: %v", err)
	}
}

func TestItemActivitySummaryPreservesAtomicSafetyFacts(t *testing.T) {
	backend, repo := projectionSourcesFixture(t, 1)
	values := []*tools.ActivityPresentation{
		{Label: " original ", Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Error: &tools.ActivityError{Message: "hidden error"}}, Chips: []tools.ActivityChip{{Kind: "effect_outcome", Label: "Effect", Value: "uncertain"}}},
		{Label: " trimmed ", Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Status: "success"}},
		{Renderer: tools.ActivityRendererSubAgentOperation, Payload: tools.SubAgentOperationActivityPayload{Action: "wait"}, Chips: []tools.ActivityChip{{Kind: "status", Label: "Clear"}}},
	}
	for i, value := range values {
		_, err := repo.Append(t.Context(), Entry{ID: fmt.Sprint("summary-", i), ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "inspect", ToolCallID: "summary", Activity: value}}, AppendOptions{})
		if err != nil {
			t.Fatal(err)
		}
		got, err := repo.ReadItemActivitySummary(t.Context(), "thread", "tool:turn:summary")
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 && (got.Label != " original " || !got.Facts.HasError || !got.Facts.UncertainEffect) {
			t.Fatalf("initial facts lost: %+v", got)
		}
		if i == 1 && (got.Label != "trimmed" || !got.Facts.HasError || !got.Facts.UncertainEffect) {
			t.Fatalf("merge facts lost: %+v", got)
		}
		if i == 2 && (got.Label != "trimmed" || got.Facts.HasError || got.Facts.UncertainEffect || got.Facts.Action != "wait") {
			t.Fatalf("replacement facts wrong: %+v", got)
		}
	}
	repo = reopenCurrentStartupRepo(t, backend, false)
	got, err := repo.ReadItemActivitySummary(t.Context(), "thread", "tool:turn:summary")
	if err != nil || got.Label != "trimmed" || got.Facts.Action != "wait" {
		t.Fatalf("restart differs: %+v %v", got, err)
	}
}

func TestItemActivitySourcesAcceptEmptyPresentation(t *testing.T) {
	_, repo := projectionSourcesFixture(t, 1)
	if _, err := repo.Append(t.Context(), Entry{ID: "empty-presentation", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "inspect", ToolCallID: "empty", Activity: &tools.ActivityPresentation{}}}, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	page, err := repo.ItemActivityStructure(t.Context(), ItemActivityInput{ThreadID: "thread", ItemID: "tool:turn:empty", Limit: 32})
	if err != nil || page.Page.Field.Kind != "object" || page.Page.Field.Count != 0 {
		t.Fatalf("legal empty presentation rejected: %+v %v", page, err)
	}
}

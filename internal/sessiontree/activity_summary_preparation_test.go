package sessiontree

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/tools"
)

func TestHistoricalSummaryDoesNotPrepareHiddenActivityDetails(t *testing.T) {
	backend, repo := projectionSourcesFixture(t, 1)
	rows := make([]tools.StructuredActivityRow, 80)
	for i := range rows {
		rows[i] = tools.StructuredActivityRow{Title: fmt.Sprint(i), Content: strings.Repeat("retained 中文🌸", 40), Format: tools.StructuredActivityRowFormatCode}
	}
	entry, err := repo.Append(t.Context(), Entry{ID: "historical-summary", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "read", ToolCallID: "summary", Activity: &tools.ActivityPresentation{Label: "Read", Description: "Inspect source", Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Rows: rows}}}}, AppendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	input := ItemActivityInput{ThreadID: "thread", ItemID: "tool:turn:summary", Path: "/payload/rows/79/content", Limit: 16}
	want, err := repo.ItemActivityStructure(t.Context(), input)
	if err != nil {
		t.Fatal(err)
	}
	field := want.Page.Field
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		if err := deleteActivityFieldIndex(t.Context(), tx, field.SourceSHA256); err != nil {
			return err
		}
		return deleteItemMembership(t.Context(), tx, "thread")
	}); err != nil {
		t.Fatal(err)
	}
	canonical := cloneMigrationRecords(backend.records)[backendDomainV32Namespace]
	measured := &interruptedActivityBackend{Backend: backend}
	repo.backend = measured
	summary, err := repo.ReadItemActivitySummary(t.Context(), "thread", input.ItemID)
	if err != nil || summary.Label != "Read" || summary.Description != "Inspect source" {
		t.Fatalf("summary=%+v err=%v", summary, err)
	}
	if measured.maximum > 16 || measured.batches > 1 {
		t.Fatalf("summary prepared hidden detail index: batches=%d max=%d", measured.batches, measured.maximum)
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		_, err := tx.Get(activityFieldsNamespace, activityFieldReadyKey(field.SourceSHA256))
		if !errors.Is(err, spi.ErrNotFound) {
			t.Fatalf("hidden details unexpectedly ready: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	repo = reopenCurrentStartupRepo(t, backend, false)
	got, err := repo.ItemActivityStructure(t.Context(), input)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("on-demand detail changed after restart: %v", err)
	}
	chunk, err := repo.ReadItemActivityField(context.Background(), input.ItemID, ActivityFieldInput{ThreadID: "thread", EntryID: entry.ID, Path: field.SourcePath, SourceSHA256: field.SourceSHA256, Limit: 16384})
	if err != nil || chunk.Text != rows[79].Content {
		t.Fatalf("detail lost content: %v", err)
	}
	if !reflect.DeepEqual(canonical, backend.records[backendDomainV32Namespace]) {
		t.Fatal("preparation changed canonical source")
	}
}

func TestSummaryReadyDoesNotRepairCorruptFactsOnDetailRead(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(fmt.Sprint(corrupt), func(t *testing.T) {
			backend, repo := projectionSourcesFixture(t, 1)
			activity := &tools.ActivityPresentation{Label: "Read", Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Summary: "retained"}}
			entry, err := repo.Append(t.Context(), Entry{ID: "summary-source", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: "summary", Activity: activity}}, AppendOptions{})
			if err != nil {
				t.Fatal(err)
			}
			input := ItemActivityInput{ThreadID: "thread", ItemID: "tool:turn:summary", Path: "/payload/summary", Limit: 16}
			page, err := repo.ItemActivityStructure(t.Context(), input)
			if err != nil {
				t.Fatal(err)
			}
			hash := page.Page.Field.SourceSHA256
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				if err := deleteActivityFieldIndex(t.Context(), tx, hash); err != nil {
					return err
				}
				return deleteItemMembership(t.Context(), tx, "thread")
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := repo.ReadItemActivitySummary(t.Context(), "thread", input.ItemID); err != nil {
				t.Fatal(err)
			}
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				if corrupt {
					return tx.Put(activityFieldsNamespace, activityMergeKey(hash), []byte("corrupt"))
				}
				return tx.Delete(activityFieldsNamespace, activityMergeKey(hash))
			}); err != nil {
				t.Fatal(err)
			}
			before := cloneMigrationRecords(backend.records)
			if _, err := repo.ReadActivityField(t.Context(), ActivityFieldInput{ThreadID: "thread", EntryID: entry.ID, Path: "/payload/summary", Limit: 16}); !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("corrupt summary repaired on detail read: %v", err)
			}
			if _, err := repo.Append(t.Context(), Entry{ID: "same-body", ThreadID: "thread", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: "second", Activity: activity}}, AppendOptions{}); !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("corrupt summary repaired on append: %v", err)
			}
			if !reflect.DeepEqual(before, backend.records) {
				t.Fatal("corrupt published source was rewritten")
			}
		})
	}
}

func TestHistoricalSummaryRetainsShapeWithoutReadingHiddenText(t *testing.T) {
	backend, repo := projectionSourcesFixture(t, 1)
	activity := &tools.ActivityPresentation{Label: "Inspect browser", Renderer: tools.ActivityRendererStructured,
		TargetRefs: []tools.ActivityTargetRef{{Kind: "computer_frame", Label: "Saved frame", ResourceRef: "computer://desktop/0123456789012345678901234567890123456789012345678901234567890123"}},
		Chips:      []tools.ActivityChip{{Kind: "computer_result", Label: "Result", Value: "page"}},
		Payload:    tools.StructuredActivityPayload{RowsProvided: true, Rows: []tools.StructuredActivityRow{{Title: "Output", Content: strings.Repeat("HIDDEN", 10000), Format: tools.StructuredActivityRowFormatCode}}}}
	entry, err := repo.Append(t.Context(), Entry{ID: "shape-source", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "browser.read", ToolCallID: "shape", Activity: activity}}, AppendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	page, err := repo.ItemActivityStructure(t.Context(), ItemActivityInput{ThreadID: "thread", ItemID: "tool:turn:shape", Path: "/payload/rows", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		if err := deleteActivityFieldIndex(t.Context(), tx, page.Page.Field.SourceSHA256); err != nil {
			return err
		}
		return deleteItemMembership(t.Context(), tx, "thread")
	}); err != nil {
		t.Fatal(err)
	}
	summary, err := repo.ReadItemActivitySummary(t.Context(), "thread", "tool:turn:shape")
	if err != nil {
		t.Fatal(err)
	}
	fields := map[string]ActivityFieldSource{}
	for _, field := range summary.Fields {
		fields[field.Path] = field
	}
	for _, path := range []string{"/target_refs", "/chips", "/payload/rows"} {
		field, ok := fields[path]
		if !ok || field.Kind != "array" || field.Count != 1 || field.EntryID != entry.ID || field.SourceSHA256 == "" {
			t.Fatalf("missing exact shape for %s: %+v", path, field)
		}
	}
	if field := fields["/payload/rows_provided"]; field.Kind != "scalar" || string(field.Value) != "true" {
		t.Fatalf("lost explicit empty/output distinction: %+v", field)
	}
	if _, ok := fields["/payload/rows/0/content"]; ok {
		t.Fatal("summary expanded hidden descendants")
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		if _, err := tx.Get(activityFieldsNamespace, activityFieldReadyKey(page.Page.Field.SourceSHA256)); !errors.Is(err, spi.ErrNotFound) {
			t.Fatalf("summary prepared hidden detail: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestHistoricalWindowDoesNotPrepareUnselectedActivitySources(t *testing.T) {
	backend, repo := projectionSourcesFixture(t, 1)
	var hashes []string
	for i := range 24 {
		entry, err := repo.Append(t.Context(), Entry{ID: fmt.Sprint("hidden-", i), ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: fmt.Sprint("hidden-", i), Activity: &tools.ActivityPresentation{Label: "Inspect", Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Rows: []tools.StructuredActivityRow{{Content: strings.Repeat("hidden", 10000), Format: tools.StructuredActivityRowFormatCode}}}}}}, AppendOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
			source, err := readCanonicalEntryMetadata(tx, "thread", entry.ID)
			if err == nil {
				hashes = append(hashes, source.bodyFields["activity_json"].SHA256)
			}
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := repo.Append(t.Context(), Entry{ID: "visible-tail", ThreadID: "thread", TurnID: "next", RunID: "next-run", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "Visible message"}}, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		for _, hash := range hashes {
			if err := deleteActivityFieldIndex(t.Context(), tx, hash); err != nil {
				return err
			}
		}
		return deleteItemMembership(t.Context(), tx, "thread")
	}); err != nil {
		t.Fatal(err)
	}
	measured := &activityBudgetBackend{Backend: backend, bodyReads: map[string]int{}}
	repo.backend = measured
	page, err := repo.ReadWindowProjection(t.Context(), ItemMembershipWindowOptions{ThreadID: "thread", Limit: 1, ActivityFields: true}, func(PathMetadata, bool) []string { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Membership.Members) != 1 || page.Membership.Members[0].ID != "visible-tail" {
		t.Fatalf("wrong selected window: %+v", page.Membership)
	}
	for _, hash := range hashes {
		for key, size := range measured.bodyReads {
			if strings.Contains(key, hash) && size != 0 {
				t.Fatalf("unselected historical activity read %d body bytes", size)
			}
		}
	}
	t.Logf("hidden_activity_body_bytes=0 other_required_body_bytes=%d", measured.bodyBytes)
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		for _, hash := range hashes {
			if err := readActivitySummaryReady(tx, canonicalBodyRef{SHA256: hash}); !errors.Is(err, spi.ErrNotFound) {
				t.Fatalf("unrequested summary was prepared: %v", err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestDemandSummaryPreservesLateResultsAndRangeFacts(t *testing.T) {
	backend, repo := projectionSourcesFixture(t, 1)
	call, err := repo.Append(t.Context(), Entry{ID: "demand-call", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: "demand", Activity: &tools.ActivityPresentation{Label: "Original label", Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Summary: "Original summary"}}}}, AppendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var hash string
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		source, err := readCanonicalEntryMetadata(tx, "thread", call.ID)
		hash = source.bodyFields["activity_json"].SHA256
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		if err := deleteActivityFieldIndex(t.Context(), tx, hash); err != nil {
			return err
		}
		return deleteItemMembership(t.Context(), tx, "thread")
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.ItemMembershipWindow(t.Context(), ItemMembershipWindowOptions{ThreadID: "thread", Limit: 1}); err != nil {
		t.Fatal(err)
	}
	result := Entry{ID: "late-demand-result", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "inspect", ToolCallID: "demand", ToolResult: &session.ToolResultView{Status: "error"}, Activity: &tools.ActivityPresentation{Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Status: "error", Error: &tools.ActivityError{Message: "Retained failure"}}}}}
	if _, err := repo.Append(t.Context(), result, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	summary, err := repo.ReadItemActivitySummary(t.Context(), "thread", "tool:turn:demand")
	if err != nil || summary.Label != "Original label" || !summary.Facts.HasError {
		t.Fatalf("lost late-result selection: %+v %v", summary, err)
	}
	got, err := repo.ItemMembershipRange(t.Context(), ItemMembershipRangeOptions{ThreadID: "thread", AnchorID: "tool:turn:demand", Kinds: []string{"tool"}})
	if err != nil || got.Activity.Error != 1 || got.Facts&ItemRangeErrorPayload == 0 {
		t.Fatalf("wrong range after demand preparation: %+v %v", got, err)
	}
	before := cloneMigrationRecords(backend.records)
	repo = reopenCurrentStartupRepo(t, backend, false)
	again, err := repo.ReadItemActivitySummary(t.Context(), "thread", "tool:turn:demand")
	if err != nil || !reflect.DeepEqual(summary, again) {
		t.Fatalf("restart changed summary: %+v %v", again, err)
	}
	if !reflect.DeepEqual(before, backend.records) {
		t.Fatal("ready summary rewrote metadata")
	}
}

// A cold window must collect all selected pending summaries before retrying.
// Returning only the first miss repeatedly rereads every selected membership.
func TestHistoricalWindowCollectsSelectedSummaryPreparation(t *testing.T) {
	backend, repo := projectionSourcesFixture(t, 1)
	const count = 12
	var hashes []string
	for i := range count {
		id := fmt.Sprint("selected-", i)
		entry, err := repo.Append(t.Context(), Entry{ID: id, ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: id, Activity: &tools.ActivityPresentation{Label: id, Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Summary: "visible"}}}}, AppendOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
			source, err := readCanonicalEntryMetadata(tx, "thread", entry.ID)
			if err == nil {
				hashes = append(hashes, source.bodyFields["activity_json"].SHA256)
			}
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		for _, hash := range hashes {
			if err := deleteActivityFieldIndex(t.Context(), tx, hash); err != nil {
				return err
			}
		}
		return deleteItemMembership(t.Context(), tx, "thread")
	}); err != nil {
		t.Fatal(err)
	}
	if err := repo.prepareWindowProjection(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	measured := &summaryWindowReadBackend{Backend: backend}
	repo.backend = measured
	page, err := repo.ReadWindowProjection(t.Context(), ItemMembershipWindowOptions{ThreadID: "thread", Limit: count, ActivityFields: true}, func(PathMetadata, bool) []string { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if len(page.ActivitySummaries) != count {
		t.Fatalf("summaries=%d", len(page.ActivitySummaries))
	}
	// There are two membership reads plus one demand validation per selected
	// item. Source authority may read each slot again, but never O(window^2).
	if measured.identityReads > count*4 {
		t.Fatalf("repeated window membership: %d reads for %d items", measured.identityReads, count)
	}
	t.Logf("selected=%d source_slot_reads=%d", count, measured.identityReads)
}

type summaryWindowReadBackend struct {
	spi.Backend
	identityReads int
}
type summaryWindowReadTx struct {
	spi.ReadTx
	owner *summaryWindowReadBackend
}

// A live result may arrive after the historical prefix has been prepared. The
// same owner must catch it up without restarting that prefix or discarding it.
func pendingSummaryFixture(t *testing.T) (*migrationTestBackend, *BackendRepo, Entry) {
	t.Helper()
	backend, repo := projectionSourcesFixture(t, 1)
	call, err := repo.Append(t.Context(), Entry{ID: "catchup-call", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: "catchup", Activity: &tools.ActivityPresentation{Label: "Retained label", Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Summary: "initial"}}}}, AppendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		source, err := readCanonicalEntryMetadata(tx, "thread", call.ID)
		if err != nil {
			return err
		}
		if err := deleteActivityFieldIndex(t.Context(), tx, source.bodyFields["activity_json"].SHA256); err != nil {
			return err
		}
		return deleteItemMembership(t.Context(), tx, "thread")
	}); err != nil {
		t.Fatal(err)
	}
	if err := repo.prepareWindowProjection(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	return backend, repo, call
}

func appendSummaryResult(t *testing.T, repo *BackendRepo, index int) error {
	t.Helper()
	_, err := repo.Append(t.Context(), Entry{ID: fmt.Sprint("catchup-result-", index), ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "inspect", ToolCallID: "catchup", ToolResult: &session.ToolResultView{Status: "error"}, Activity: &tools.ActivityPresentation{Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Status: "error", Error: &tools.ActivityError{Message: fmt.Sprint("Retained failure ", index)}}}}}, AppendOptions{})
	return err
}

func TestDemandSummaryCatchesUpSameToolWithoutRestart(t *testing.T) {
	for _, count := range []int{1, 270} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			backend, repo, call := pendingSummaryFixture(t)
			writer := reopenCurrentStartupRepo(t, backend, false)
			wrapped := &appendAfterSummaryBackend{Backend: backend}
			wrapped.after = func() error {
				for i := range count {
					if err := appendSummaryResult(t, writer, i); err != nil {
						return err
					}
				}
				return nil
			}
			repo.backend = wrapped
			if err := repo.prepareItemActivitySummaryOwned(t.Context(), "thread", call.ID); err != nil {
				t.Fatalf("same-tool append restarted frozen prefix: %v", err)
			}
			if !wrapped.appended {
				t.Fatal("append was not injected")
			}
			got, err := repo.ReadItemActivitySummary(t.Context(), "thread", "tool:turn:catchup")
			if err != nil || got.Label != "Retained label" || !got.Facts.HasError {
				t.Fatalf("lost merged result: %+v %v", got, err)
			}
			before := cloneMigrationRecords(backend.records)
			restarted := reopenCurrentStartupRepo(t, backend, false)
			again, err := restarted.ReadItemActivitySummary(t.Context(), "thread", "tool:turn:catchup")
			if err != nil || !reflect.DeepEqual(got, again) {
				t.Fatalf("restart changed activity: %v", err)
			}
			if !reflect.DeepEqual(before, backend.records) {
				t.Fatal("restart rewrote prepared metadata")
			}
		})
	}
}

func TestDemandSummaryCatchupPublicationRollback(t *testing.T) {
	for _, mode := range []string{"failure", "cancel", "panic"} {
		t.Run(mode, func(t *testing.T) {
			backend, repo, call := pendingSummaryFixture(t)
			writer := reopenCurrentStartupRepo(t, backend, false)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			sentinel := errors.New("summary publication failed")
			failed := &failSummaryPublicationBackend{Backend: backend, fail: func() error {
				switch mode {
				case "cancel":
					cancel()
					return ctx.Err()
				case "panic":
					panic(sentinel)
				default:
					return sentinel
				}
			}}
			wrapped := &appendAfterSummaryBackend{Backend: failed}
			wrapped.after = func() error { return appendSummaryResult(t, writer, 0) }
			repo.backend = wrapped
			func() {
				defer func() {
					if value := recover(); value != nil {
						if mode != "panic" || value != sentinel {
							panic(value)
						}
					}
				}()
				err := repo.prepareItemActivitySummaryOwned(ctx, "thread", call.ID)
				if mode == "panic" || !errors.Is(err, sentinel) && !errors.Is(err, context.Canceled) {
					t.Fatalf("missing injected failure: %v", err)
				}
			}()
			if !failed.caught {
				t.Fatal("publication was not intercepted")
			}
			if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
				member, err := pendingActivityMember(tx, "thread", call.ID)
				if err != nil {
					return err
				}
				if !member.ActivityPending {
					t.Fatal("partial summary became visible")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			restarted := reopenCurrentStartupRepo(t, backend, false)
			got, err := restarted.ReadItemActivitySummary(t.Context(), "thread", "tool:turn:catchup")
			if err != nil || got.Label != "Retained label" || !got.Facts.HasError {
				t.Fatalf("restart lost result: %+v %v", got, err)
			}
		})
	}
}

type failSummaryPublicationBackend struct {
	spi.Backend
	fail   func() error
	caught bool
}
type failSummaryPublicationTx struct {
	spi.WriteTx
	published bool
}

func (tx *failSummaryPublicationTx) Put(ns string, key, value []byte) error {
	if ns == itemMembershipNamespace && bytes.HasPrefix(key, backendDomainKey("thread", "slot")) {
		var member storedItemMember
		if err := decodeCanonicalBodyJSON(value, &member); err != nil {
			return err
		}
		if member.ID == "tool:turn:catchup" && !member.ActivityPending {
			tx.published = true
		}
	}
	return tx.WriteTx.Put(ns, key, value)
}
func (b *failSummaryPublicationBackend) Update(ctx context.Context, visit func(spi.WriteTx) error) error {
	return b.Backend.Update(ctx, func(tx spi.WriteTx) error {
		tracked := &failSummaryPublicationTx{WriteTx: tx}
		if err := visit(tracked); err != nil {
			return err
		}
		if tracked.published {
			b.caught = true
			return b.fail()
		}
		return nil
	})
}

type appendAfterSummaryBackend struct {
	spi.Backend
	after    func() error
	appended bool
}

func (b *appendAfterSummaryBackend) Update(ctx context.Context, visit func(spi.WriteTx) error) error {
	writes := 0
	err := b.Backend.Update(ctx, func(tx spi.WriteTx) error {
		counted := &interruptedActivityTx{WriteTx: tx}
		err := visit(counted)
		writes = counted.writes
		return err
	})
	if err == nil && writes > 0 && !b.appended {
		b.appended = true
		return b.after()
	}
	return err
}

func (b *summaryWindowReadBackend) View(ctx context.Context, read func(spi.ReadTx) error) error {
	return b.Backend.View(ctx, func(tx spi.ReadTx) error { return read(summaryWindowReadTx{tx, b}) })
}
func (tx summaryWindowReadTx) Get(ns string, key []byte) ([]byte, error) {
	if ns == itemMembershipNamespace && strings.HasPrefix(string(key), string(backendDomainKey("thread", "slot"))) {
		tx.owner.identityReads++
	}
	return tx.ReadTx.Get(ns, key)
}

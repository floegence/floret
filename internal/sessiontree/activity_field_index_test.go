package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"unicode"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/tools"
)

func TestActivityFieldIndexDescribesWhitespaceWithoutChangingSources(t *testing.T) {
	texts := []string{"", " \t\n", strings.Repeat("\n", 70000) + "    preserved indentation\n", strings.Repeat(" ", 40000) + "Title", "\u2003中🌸\u00a0", "a\n  b"}
	for _, text := range texts {
		raw, err := json.Marshal(map[string]any{"payload": map[string]any{"rows": []any{map[string]any{"content": text}}}})
		if err != nil {
			t.Fatal(err)
		}
		nodes, err := indexActivityJSON(t.Context(), string(raw))
		if err != nil {
			t.Fatal(err)
		}
		node := nodes["/payload/rows/0/content"]
		if node.TrimBytes != len(strings.TrimSpace(text)) || node.TrimOffset != len(text)-len(strings.TrimLeftFunc(text, unicode.IsSpace)) {
			t.Fatalf("whitespace extents differ: bytes=%d offset=%d length=%d", len(text), node.TrimOffset, node.TrimBytes)
		}
		if node.Bytes != len(text) || node.SHA256 != stableHash(text) {
			t.Fatal("raw source changed")
		}
		if node.TextExtentVersion != 1 {
			t.Fatal("missing extent version")
		}
	}
}

func TestActivityFieldTextExtentRejectsInvalidMetadata(t *testing.T) {
	raw := `" abc "`
	nodes, err := indexActivityJSON(t.Context(), raw)
	if err != nil {
		t.Fatal(err)
	}
	node := nodes[""]
	node.Parts = nil
	ref := canonicalBodyRef{Bytes: len(raw)}
	if !validActivityFieldNode(ref, node) {
		t.Fatal("valid extent rejected")
	}
	for _, mutate := range []func(*activityFieldNode){
		func(n *activityFieldNode) { n.TextExtentVersion = 2 },
		func(n *activityFieldNode) { n.TrimOffset = n.Bytes + 1 },
		func(n *activityFieldNode) { n.TrimBytes = n.Bytes },
		func(n *activityFieldNode) { n.TrimBytes = -1 },
	} {
		changed := node
		mutate(&changed)
		if validActivityFieldNode(ref, changed) {
			t.Fatal("invalid extent accepted")
		}
	}
	node.TextExtentVersion = 0
	node.TrimOffset = 0
	node.TrimBytes = 0
	if !validActivityFieldNode(ref, node) {
		t.Fatal("released metadata rejected")
	}
}

func TestActivityFieldIndexPreservesEscapesAndBoundsDecodedRanges(t *testing.T) {
	texts := []string{strings.Repeat("中文🌸\n\"\\\x00", 7000), "small", strings.Repeat("ending 🌸", 9000)}
	raw, err := json.Marshal(map[string]any{"payload": map[string]any{"rows": []any{map[string]any{"content": texts[0]}, map[string]any{"content": texts[1]}, map[string]any{"content": texts[2]}}}})
	if err != nil {
		t.Fatal(err)
	}
	nodes, err := indexActivityJSON(t.Context(), string(raw))
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range texts {
		path := fmt.Sprintf("/payload/rows/%d/content", i)
		node, ok := nodes[path]
		if !ok || node.Kind != "string" || node.Bytes != len(want) {
			t.Fatalf("missing exact field: %+v", node)
		}
		var got strings.Builder
		for _, part := range node.Parts {
			if part.Bytes > canonicalBodyBlockBytes || part.EncodedBytes > 6*canonicalBodyBlockBytes {
				t.Fatalf("unbounded field part: %+v", part)
			}
			encoded := append([]byte{'"'}, raw[part.Offset:part.Offset+part.EncodedBytes]...)
			encoded = append(encoded, '"')
			var text string
			if err := json.Unmarshal(encoded, &text); err != nil {
				t.Fatal(err)
			}
			if len(text) != part.Bytes || stableHash(text) != part.SHA256 {
				t.Fatal("wrong field block identity")
			}
			got.WriteString(text)
		}
		if got.String() != want {
			t.Fatalf("field %d differs", i)
		}
	}
	if !reflect.DeepEqual(nodes["/payload/rows"].Children, []string{"0", "1", "2"}) {
		t.Fatal("array order lost")
	}
}

func TestActivityFieldStorageDoesNotReadSiblingBodies(t *testing.T) {
	for _, count := range []int{20, 180} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			backend, repo := projectionSourcesFixture(t, 1)
			rows := make([]tools.StructuredActivityRow, count)
			value := strings.Repeat("中文🌸\n\"\\\x00", 3500)
			for i := range rows {
				rows[i] = tools.StructuredActivityRow{Title: fmt.Sprint(i), Content: value, Format: tools.StructuredActivityRowFormatCode}
			}
			activity := &tools.ActivityPresentation{Label: "Inspect", Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Rows: rows}}
			entry, err := repo.Append(t.Context(), Entry{ID: "field-result", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "inspect", ToolCallID: "call", Content: "done", Activity: activity}}, AppendOptions{})
			if err != nil {
				t.Fatal(err)
			}
			repo = reopenCurrentStartupRepo(t, backend, false)
			if err := repo.PrepareDisplayThread(t.Context(), "thread"); err != nil {
				t.Fatal(err)
			}
			counted := &currentStartupBackend{Backend: backend}
			repo.backend = counted
			for _, row := range []int{0, count / 2, count - 1} {
				counted.gets, counted.bodies, counted.journalRows = 0, 0, 0
				got, err := repo.ReadActivityField(t.Context(), ActivityFieldInput{ThreadID: "thread", EntryID: entry.ID, Path: fmt.Sprintf("/payload/rows/%d/content", row), Offset: 0, Limit: 16384})
				if err != nil {
					t.Fatal(err)
				}
				if !strings.HasPrefix(value, got.Text) || len(got.Text) < 16380 || got.Bytes != len(value) || got.SHA256 != stableHash(value) {
					t.Fatalf("wrong decoded prefix: %+v", got)
				}
				if counted.bodies > 8 || counted.gets > 48 || counted.journalRows != 0 {
					t.Fatalf("sibling history read: rows=%d gets=%d bodies=%d journal=%d", count, counted.gets, counted.bodies, counted.journalRows)
				}
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			counted.gets = 0
			if _, err := repo.ReadActivityField(ctx, ActivityFieldInput{ThreadID: "thread", EntryID: entry.ID, Path: "/payload/rows/0/content", Limit: 16384}); !errors.Is(err, context.Canceled) || counted.gets != 0 {
				t.Fatalf("cancelled read performed IO: %v gets=%d", err, counted.gets)
			}
		})
	}
}

func TestActivityFieldMetadataRebuildAndIntegrity(t *testing.T) {
	backend, repo := projectionSourcesFixture(t, 1)
	value := strings.Repeat("decoded 中文🌸", 3000)
	activity := &tools.ActivityPresentation{Label: "Read", Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Rows: []tools.StructuredActivityRow{{Content: value, Format: tools.StructuredActivityRowFormatCode}}}}
	entry, err := repo.Append(t.Context(), Entry{ID: "field", ThreadID: "thread", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "read", ToolCallID: "tool", Activity: activity}}, AppendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	input := ActivityFieldInput{ThreadID: "thread", EntryID: entry.ID, Path: "/payload/rows/0/content", Limit: 16384}
	want, err := repo.ReadActivityField(t.Context(), input)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return deleteActivityFieldIndex(t.Context(), tx, want.SourceSHA256) }); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("injected field preparation failure")
	repo.backend = &domainCommitBoundaryBackend{Backend: backend, afterCallback: func() error { return failure }}
	if _, err := repo.ReadActivityField(t.Context(), input); !errors.Is(err, failure) {
		t.Fatalf("failed preparation accepted: %v", err)
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		_, err := tx.Get(activityFieldsNamespace, activityFieldReadyKey(want.SourceSHA256))
		if !errors.Is(err, spi.ErrNotFound) {
			t.Fatalf("partial readiness: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	repo.backend = backend
	got, err := repo.ReadActivityField(t.Context(), input)
	if err != nil || got != want {
		t.Fatalf("rebuild differs: %v", err)
	}
	if _, err := repo.Append(t.Context(), Entry{ID: "new-tail", ThreadID: "thread", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "ordinary append"}}, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	got, err = repo.ReadActivityField(t.Context(), input)
	if err != nil || got != want {
		t.Fatalf("append invalidated immutable field: %v", err)
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		key := activityFieldKey(want.SourceSHA256, input.Path)
		raw, err := tx.Get(activityFieldsNamespace, key)
		if err != nil {
			return err
		}
		raw[len(raw)/2] ^= 1
		return tx.Put(activityFieldsNamespace, key, raw)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.ReadActivityField(t.Context(), input); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("corrupt metadata accepted: %v", err)
	}
}

// A ready field locator does not certify missing or corrupt merge metadata.
// Reads and a subsequent writer must both reject the inconsistent authority.
func TestActivityFieldReadyRequiresMergeMetadata(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(fmt.Sprint(corrupt), func(t *testing.T) {
			backend, repo := projectionSourcesFixture(t, 1)
			activity := &tools.ActivityPresentation{Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Rows: []tools.StructuredActivityRow{{Content: "retained"}}}}
			entry, err := repo.Append(t.Context(), Entry{ID: "source", ThreadID: "thread", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: "call", Activity: activity}}, AppendOptions{})
			if err != nil {
				t.Fatal(err)
			}
			input := ActivityFieldInput{ThreadID: "thread", EntryID: entry.ID, Path: "/payload/rows/0/content", Limit: 4096}
			field, err := repo.ReadActivityField(t.Context(), input)
			if err != nil {
				t.Fatal(err)
			}
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				key := activityMergeKey(field.SourceSHA256)
				if corrupt {
					return tx.Put(activityFieldsNamespace, key, []byte("corrupt"))
				}
				return tx.Delete(activityFieldsNamespace, key)
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := repo.ReadActivityField(t.Context(), input); !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("ready read accepted invalid merge metadata: %v", err)
			}
			if _, err := repo.ActivityStructure(t.Context(), ActivityStructureInput{ThreadID: "thread", EntryIDs: []string{entry.ID}, Limit: 32}); !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("structure accepted invalid merge metadata: %v", err)
			}
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return deleteItemMembership(t.Context(), tx, "thread") }); err != nil {
				t.Fatal(err)
			}
			before := cloneMigrationRecords(backend.records)
			if err := repo.prepareWindowProjection(t.Context(), "thread"); !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("window preparation repaired corrupt published metadata: %v", err)
			}
			if !reflect.DeepEqual(before, backend.records) {
				t.Fatal("corrupt published source was rewritten")
			}
			if _, err := repo.Append(t.Context(), Entry{ID: "duplicate-body", ThreadID: "thread", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: "second", Activity: activity}}, AppendOptions{}); !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("writer accepted invalid merge metadata: %v", err)
			}
			if _, err := repo.Entry(t.Context(), "thread", "duplicate-body"); !errors.Is(err, ErrEntryNotFound) {
				t.Fatalf("failed writer published canonical data: %v", err)
			}
		})
	}
}

// This appends on the transition from snapshot reads to metadata publication.
// A normal tail advance must not invalidate an immutable field source.
type activityPrepareAppendBackend struct {
	spi.Backend
	beforeUpdate func()
}

func (b *activityPrepareAppendBackend) Update(ctx context.Context, write func(spi.WriteTx) error) error {
	if b.beforeUpdate != nil {
		f := b.beforeUpdate
		b.beforeUpdate = nil
		f()
	}
	return b.Backend.Update(ctx, write)
}
func TestActivityFieldPreparationSurvivesAppend(t *testing.T) {
	backend, repo := projectionSourcesFixture(t, 1)
	activity := &tools.ActivityPresentation{Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Rows: []tools.StructuredActivityRow{{Content: strings.Repeat("large 中文🌸", 3000), Format: tools.StructuredActivityRowFormatCode}}}}
	_, err := repo.Append(t.Context(), Entry{ID: "source", ThreadID: "thread", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "read", ToolCallID: "source", Activity: activity}}, AppendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	input := ActivityFieldInput{ThreadID: "thread", EntryID: "source", Path: "/payload/rows/0/content", Limit: 16384}
	want, err := repo.ReadActivityField(t.Context(), input)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return deleteActivityFieldIndex(t.Context(), tx, want.SourceSHA256) }); err != nil {
		t.Fatal(err)
	}
	other := reopenCurrentStartupRepo(t, backend, false)
	called := false
	repo.backend = &activityPrepareAppendBackend{Backend: backend, beforeUpdate: func() {
		called = true
		if _, err := other.Append(t.Context(), Entry{ID: "appended", ThreadID: "thread", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "keep working"}}, AppendOptions{}); err != nil {
			t.Fatal(err)
		}
	}}
	got, err := repo.ReadActivityField(t.Context(), input)
	if err != nil || got != want || !called {
		t.Fatalf("concurrent append rejected: %v", err)
	}
}

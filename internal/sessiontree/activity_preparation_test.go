package sessiontree

import (
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

type interruptedActivityBackend struct {
	spi.Backend
	batches, maximum int
	after            func(int) error
}
type interruptedActivityTx struct {
	spi.WriteTx
	writes int
}

func (tx *interruptedActivityTx) Put(namespace string, key, value []byte) error {
	if namespace == activityFieldsNamespace {
		tx.writes++
	}
	return tx.WriteTx.Put(namespace, key, value)
}
func (backend *interruptedActivityBackend) Update(ctx context.Context, visit func(spi.WriteTx) error) error {
	return backend.Backend.Update(ctx, func(tx spi.WriteTx) error {
		counted := &interruptedActivityTx{WriteTx: tx}
		if err := visit(counted); err != nil {
			return err
		}
		backend.maximum = max(backend.maximum, counted.writes)
		if counted.writes > 0 {
			backend.batches++
			if backend.after != nil {
				return backend.after(backend.batches)
			}
		}
		return nil
	})
}

func TestWindowLegacyActivityPreparationResumesAfterInterruption(t *testing.T) {
	for _, mode := range []string{"failure", "cancel", "panic"} {
		t.Run(mode, func(t *testing.T) {
			backend, repo := projectionSourcesFixture(t, 1)
			rows := make([]tools.StructuredActivityRow, 80)
			for i := range rows {
				rows[i] = tools.StructuredActivityRow{Title: fmt.Sprint(i), Content: strings.Repeat("retain 中文🌸", 100), Format: tools.StructuredActivityRowFormatCode}
			}
			entry, err := repo.Append(t.Context(), Entry{ID: "legacy-field", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "read", ToolCallID: "legacy", Activity: &tools.ActivityPresentation{Label: "Read", Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Rows: rows}}}}, AppendOptions{})
			if err != nil {
				t.Fatal(err)
			}
			input := ItemActivityInput{ThreadID: "thread", ItemID: "tool:turn:legacy", Path: "/payload/rows/79/content", Limit: 16}
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
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			sentinel := errors.New("interrupted legacy activity preparation")
			wrapped := &interruptedActivityBackend{Backend: backend, after: func(batch int) error {
				if batch != 2 {
					return nil
				}
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
			repo.backend = wrapped
			func() {
				defer func() {
					if value := recover(); value != nil {
						if mode != "panic" || value != sentinel {
							panic(value)
						}
					}
				}()
				_, err := repo.ItemActivityStructure(ctx, input)
				if mode == "panic" {
					t.Fatal("missing panic")
				}
				if !errors.Is(err, sentinel) && !errors.Is(err, context.Canceled) {
					t.Fatalf("unexpected interruption: %v", err)
				}
			}()
			if wrapped.batches != 2 || wrapped.maximum > 128 {
				t.Fatalf("unbounded preparation: batches=%d max=%d", wrapped.batches, wrapped.maximum)
			}
			if !reflect.DeepEqual(canonical, backend.records[backendDomainV32Namespace]) {
				t.Fatal("preparation rewrote canonical source")
			}
			if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
				if _, err := tx.Get(activityFieldsNamespace, activityFieldReadyKey(field.SourceSHA256)); !errors.Is(err, spi.ErrNotFound) {
					t.Fatalf("partial field was published: %v", err)
				}
				if manifest, found, err := readItemManifest(tx, "thread"); err != nil || !found || manifest.Leaf != entry.ID {
					t.Fatalf("complete summary membership was lost during detail preparation: %v", err)
				}
				summary, err := readItemActivitySummary(t.Context(), tx, ThreadMeta{ID: "thread", LeafID: entry.ID}, want.Member)
				if err != nil || summary.Label != "Read" {
					t.Fatalf("detail interruption invalidated complete summary: %+v %v", summary, err)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			repo = reopenCurrentStartupRepo(t, backend, false)
			got, err := repo.ItemActivityStructure(t.Context(), input)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("restart changed field: %v", err)
			}
			chunk, err := repo.ReadItemActivityField(t.Context(), input.ItemID, ActivityFieldInput{ThreadID: "thread", EntryID: entry.ID, Path: field.SourcePath, SourceSHA256: field.SourceSHA256, Limit: 16384})
			if err != nil || chunk.Text != rows[79].Content {
				t.Fatalf("restart lost body: %v", err)
			}
			if !reflect.DeepEqual(canonical, backend.records[backendDomainV32Namespace]) {
				t.Fatal("restart rewrote canonical source")
			}
		})
	}
}

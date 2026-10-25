package sessiontree

import (
	"context"
	"fmt"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/tools"
	"strings"
	"testing"
)

type activityBudgetBackend struct {
	spi.Backend
	metadataBytes, bodyBytes, gets int
	bodyReads                      map[string]int
}
type activityBudgetTx struct {
	spi.ReadTx
	owner *activityBudgetBackend
}

func (b *activityBudgetBackend) View(ctx context.Context, read func(spi.ReadTx) error) error {
	return b.Backend.View(ctx, func(tx spi.ReadTx) error { return read(activityBudgetTx{tx, b}) })
}
func (tx activityBudgetTx) Get(namespace string, key []byte) ([]byte, error) {
	raw, err := tx.ReadTx.Get(namespace, key)
	tx.owner.gets++
	if namespace == activityFieldsNamespace {
		tx.owner.metadataBytes += len(raw)
	}
	if namespace == canonicalBodyNamespace {
		tx.owner.bodyBytes += len(raw)
		if tx.owner.bodyReads != nil {
			tx.owner.bodyReads[string(key)] += len(raw)
		}
	}
	return raw, err
}
func TestActivityStructureArrayMetadataIsBounded(t *testing.T) {
	for _, count := range []int{20, 200} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			backend, repo := projectionSourcesFixture(t, 1)
			rows := make([]tools.StructuredActivityRow, count)
			for i := range rows {
				rows[i] = tools.StructuredActivityRow{Title: fmt.Sprint(i), Content: "retained"}
			}
			_, err := repo.Append(t.Context(), Entry{ID: "large-array", ThreadID: "thread", Type: EntryToolResult, Message: session.Message{Role: session.Tool, Activity: &tools.ActivityPresentation{Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Rows: rows}}}}, AppendOptions{})
			if err != nil {
				t.Fatal(err)
			}
			repo = reopenCurrentStartupRepo(t, backend, false)
			if err := repo.PrepareDisplayThread(t.Context(), "thread"); err != nil {
				t.Fatal(err)
			}
			measured := &activityBudgetBackend{Backend: backend}
			repo.backend = measured
			for _, offset := range []int{0, count / 2, count - 1} {
				measured.metadataBytes, measured.bodyBytes, measured.gets = 0, 0, 0
				page, err := repo.ActivityStructure(t.Context(), ActivityStructureInput{ThreadID: "thread", EntryIDs: []string{"large-array"}, Path: "/payload/rows", Offset: offset, Limit: 1})
				if err != nil {
					t.Fatal(err)
				}
				if page.Field.Count != count || len(page.Children) != 1 {
					t.Fatalf("wrong count: %+v", page)
				}
				if measured.metadataBytes > 8000 || measured.bodyBytes != 0 || measured.gets > 50 {
					t.Fatalf("unbounded read: metadata=%d body=%d gets=%d", measured.metadataBytes, measured.bodyBytes, measured.gets)
				}
				t.Logf("rows=%d offset=%d metadata=%d body=%d gets=%d", count, offset, measured.metadataBytes, measured.bodyBytes, measured.gets)
			}
		})
	}
}
func TestActivityFieldSuffixMetadataIsBounded(t *testing.T) {
	for _, count := range []int{1, 180} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			backend, repo := projectionSourcesFixture(t, 1)
			size := 64 << 10
			value := strings.Repeat("x", size-4) + "tail"
			rows := make([]tools.StructuredActivityRow, count)
			for i := range rows {
				rows[i] = tools.StructuredActivityRow{Content: value, Format: tools.StructuredActivityRowFormatCode}
			}
			_, err := repo.Append(t.Context(), Entry{ID: "large-field", ThreadID: "thread", Type: EntryToolResult, Message: session.Message{Role: session.Tool, Activity: &tools.ActivityPresentation{Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Rows: rows}}}}, AppendOptions{})
			if err != nil {
				t.Fatal(err)
			}
			repo = reopenCurrentStartupRepo(t, backend, false)
			if err := repo.PrepareDisplayThread(t.Context(), "thread"); err != nil {
				t.Fatal(err)
			}
			measured := &activityBudgetBackend{Backend: backend}
			repo.backend = measured
			part, err := repo.ReadActivityField(t.Context(), ActivityFieldInput{ThreadID: "thread", EntryID: "large-field", Path: fmt.Sprintf("/payload/rows/%d/content", count-1), Offset: size - 4, Limit: 4})
			if err != nil {
				t.Fatal(err)
			}
			if part.Text != "tail" || part.Bytes != size {
				t.Fatalf("wrong suffix: %+v", part)
			}
			if measured.metadataBytes > 12000 || measured.bodyBytes > 64<<10 || measured.gets > 64 {
				t.Fatalf("unbounded suffix: metadata=%d body=%d gets=%d", measured.metadataBytes, measured.bodyBytes, measured.gets)
			}
			t.Logf("activity=%d field=%d metadata=%d body=%d gets=%d", count*size, size, measured.metadataBytes, measured.bodyBytes, measured.gets)
		})
	}
}

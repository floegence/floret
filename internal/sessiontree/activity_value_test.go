package sessiontree

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/internal/activityfields"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/tools"
)

func TestAtomicActivityValueReadDoesNotScaleWithSiblingBodies(t *testing.T) {
	for _, count := range []int{1, 180} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			backend, repo := projectionSourcesFixture(t, 1)
			rows := make([]tools.StructuredActivityRow, count)
			for i := range rows {
				rows[i] = tools.StructuredActivityRow{Content: strings.Repeat("x", 64000), Format: tools.StructuredActivityRowFormatCode, Truncated: true}
			}
			entry := Entry{ID: "atomic-result", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "inspect", ToolCallID: "work", Activity: &tools.ActivityPresentation{Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Rows: rows, RowsProvided: true}}}}
			if _, err := repo.Append(t.Context(), entry, AppendOptions{}); err != nil {
				t.Fatal(err)
			}
			if err := repo.prepareWindowProjection(t.Context(), "thread"); err != nil {
				t.Fatal(err)
			}
			repo = reopenCurrentStartupRepo(t, backend, false)
			if err := repo.PrepareDisplayThread(t.Context(), "thread"); err != nil {
				t.Fatal(err)
			}
			measured := &activityBudgetBackend{Backend: backend}
			repo.backend = measured
			input := ItemActivityValueInput{ThreadID: "thread", ItemID: "tool:turn:work", Path: fmt.Sprintf("/payload/rows/%d", count-1), MaximumBytes: 1024, MaximumNodes: 32}
			if _, err := repo.ReadItemActivityValue(t.Context(), input); !errors.Is(err, activityfields.ErrRange) {
				t.Fatalf("budget %v", err)
			}
			if measured.bodyBytes != 0 {
				t.Fatalf("rejected value read %d body bytes", measured.bodyBytes)
			}
			measured.metadataBytes, measured.bodyBytes, measured.gets = 0, 0, 0
			input.MaximumBytes = 96 << 10
			value, err := repo.ReadItemActivityValue(t.Context(), input)
			if err != nil {
				t.Fatal(err)
			}
			var got tools.StructuredActivityRow
			if json.Unmarshal(value.Value, &got) != nil || got != rows[count-1] {
				t.Fatal("value differs")
			}
			if measured.bodyBytes > 256<<10 || measured.metadataBytes > 64<<10 || measured.gets > 200 {
				t.Fatalf("unbounded reads: %+v", measured)
			}
			t.Logf("rows=%d metadata=%d body=%d gets=%d wire=%d", count, measured.metadataBytes, measured.bodyBytes, measured.gets, len(value.Value))
			measured.metadataBytes, measured.bodyBytes, measured.gets = 0, 0, 0
			input.Fields, input.TextFields = []string{"truncated"}, []string{"content"}
			input.MaximumBytes = 1024
			selected, err := repo.ReadItemActivityValue(t.Context(), input)
			if err != nil || len(selected.TextFields) != 1 || selected.TextFields[0].Bytes != 64000 {
				t.Fatalf("frozen text metadata: %+v %v", selected, err)
			}
			if measured.bodyBytes != 0 || measured.metadataBytes > 64<<10 || measured.gets > 200 {
				t.Fatalf("deferred source read bodies or unbounded metadata: %+v", measured)
			}
			t.Logf("deferred rows=%d metadata=%d body=%d gets=%d", count, measured.metadataBytes, measured.bodyBytes, measured.gets)
		})
	}
}

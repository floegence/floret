package sessiontree

import (
	"fmt"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/tools"
	"strings"
	"testing"
)

func TestActivityFactsPreserveSafetyAcrossMergeWithoutBodyReads(t *testing.T) {
	for _, count := range []int{1, 180} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			backend, repo := projectionSourcesFixture(t, 1)
			rows := make([]tools.StructuredActivityRow, count)
			for i := range rows {
				rows[i] = tools.StructuredActivityRow{Content: strings.Repeat("hidden", 10000), Format: tools.StructuredActivityRowFormatCode}
			}
			values := []*tools.ActivityPresentation{
				{Renderer: tools.ActivityRendererStructured, Chips: []tools.ActivityChip{{Kind: "effect_outcome", Label: "Effect", Value: "uncertain"}}, Payload: tools.StructuredActivityPayload{Rows: rows, Error: &tools.ActivityError{Message: "evidence"}}},
				{Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Status: "success"}},
				{Renderer: tools.ActivityRendererSubAgentOperation, Payload: tools.SubAgentOperationActivityPayload{Action: "wait"}},
				{Chips: []tools.ActivityChip{{Kind: "status", Label: "Done"}}},
			}
			ids := []string{}
			wants := []ActivityFacts{
				{Renderer: "structured", HasError: true, UncertainEffect: true},
				{Renderer: "structured", HasError: true, UncertainEffect: true},
				{Renderer: "subagent_operation", Action: "wait", UncertainEffect: true},
				{Renderer: "subagent_operation", Action: "wait"},
			}
			for i, value := range values {
				entry, err := repo.Append(t.Context(), Entry{ID: fmt.Sprint("fact-", i), ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "inspect", ToolCallID: "call", Activity: value}}, AppendOptions{})
				if err != nil {
					t.Fatal(err)
				}
				ids = append(ids, entry.ID)
				measured := &currentStartupBackend{Backend: backend}
				repo.backend = measured
				got, err := repo.ActivityFacts(t.Context(), ActivityStructureInput{ThreadID: "thread", EntryIDs: ids})
				if err != nil || got != wants[i] {
					t.Fatalf("step %d got=%+v want=%+v err=%v", i, got, wants[i], err)
				}
				if measured.bodies != 0 || measured.journalRows != 0 {
					t.Fatalf("facts opened hidden bodies: %+v", measured)
				}
				repo.backend = backend
			}
			repo = reopenCurrentStartupRepo(t, backend, false)
			got, err := repo.ActivityFacts(t.Context(), ActivityStructureInput{ThreadID: "thread", EntryIDs: ids})
			if err != nil || got != wants[len(wants)-1] {
				t.Fatalf("restart changed facts: %+v %v", got, err)
			}
		})
	}
}

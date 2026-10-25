package sessiontree

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/tools"
)

func TestActivityStructureMatchesCompleteMerge(t *testing.T) {
	present := func(renderer tools.ActivityRenderer, payload tools.ActivityPayload) *tools.ActivityPresentation {
		return &tools.ActivityPresentation{Label: "Inspect", Renderer: renderer, Payload: payload}
	}
	tests := []struct {
		name   string
		values []*tools.ActivityPresentation
	}{
		{"empty payload", []*tools.ActivityPresentation{
			present(tools.ActivityRendererStructured, tools.StructuredActivityPayload{})}},
		{"trim updated labels", []*tools.ActivityPresentation{
			{Label: " original ", Description: " initial ", Renderer: tools.ActivityRendererStructured},
			{Label: "  updated 🌸  ", Description: "  description  "}}},
		{"renderer without payload", []*tools.ActivityPresentation{
			present(tools.ActivityRendererStructured, tools.StructuredActivityPayload{}),
			{Renderer: tools.ActivityRendererTerminal}}},
		{"structured", []*tools.ActivityPresentation{
			present(tools.ActivityRendererStructured, tools.StructuredActivityPayload{Inputs: []tools.StructuredActivityRow{{Title: "Input", Content: strings.Repeat("large 中文🌸", 1000), Format: tools.StructuredActivityRowFormatCode}}, Rows: []tools.StructuredActivityRow{{Content: "old"}}, Summary: "start"}),
			present(tools.ActivityRendererStructured, tools.StructuredActivityPayload{RowsProvided: true, Status: "success"}),
			present(tools.ActivityRendererStructured, tools.StructuredActivityPayload{Summary: "done"})}},
		{"terminal", []*tools.ActivityPresentation{
			present(tools.ActivityRendererTerminal, tools.TerminalActivityPayload{Command: "inspect", Output: "text", FirstSeq: 10, LastSeq: 20, LatestSeq: 21, HasMore: true}),
			present(tools.ActivityRendererTerminal, tools.TerminalActivityPayload{Operation: "read", LastSeq: 25}),
			present(tools.ActivityRendererTerminal, tools.TerminalActivityPayload{Status: "success", HasMore: true})}},
		{"file", []*tools.ActivityPresentation{
			present(tools.ActivityRendererFile, tools.FileActivityPayload{Path: "a.go", ChangeType: "modify", UnifiedDiff: "old diff", Additions: 10, Deletions: 4}),
			present(tools.ActivityRendererFile, tools.FileActivityPayload{ChangeType: "add", UnifiedDiff: "new diff"})}},
		{"search", []*tools.ActivityPresentation{
			present(tools.ActivityRendererWebSearch, tools.WebSearchActivityPayload{Query: "q", Results: []tools.WebSearchActivityResult{{Title: "title", URL: "https://example.com"}}}),
			present(tools.ActivityRendererWebSearch, tools.WebSearchActivityPayload{ResultsProvided: true})}},
		{"renderer change", []*tools.ActivityPresentation{
			present(tools.ActivityRendererStructured, tools.StructuredActivityPayload{Rows: []tools.StructuredActivityRow{{Content: "old"}}}),
			present(tools.ActivityRendererTerminal, tools.TerminalActivityPayload{Output: "new", Status: "success"})}},
		{"completion replacement", []*tools.ActivityPresentation{
			present(tools.ActivityRendererCompletion, tools.CompletionActivityPayload{Summary: "before", Status: "running"}),
			present(tools.ActivityRendererCompletion, tools.CompletionActivityPayload{Status: "success"})}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backend, repo := projectionSourcesFixture(t, 1)
			ids := []string{}
			var expected *tools.ActivityPresentation
			for i, value := range test.values {
				entry, err := repo.Append(t.Context(), Entry{ID: fmt.Sprint("activity-", i), ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "inspect", ToolCallID: "tool", Activity: value}}, AppendOptions{})
				if err != nil {
					t.Fatal(err)
				}
				ids = append(ids, entry.ID)
				expected = tools.MergeActivityPresentations(expected, value)
			}
			repo = reopenCurrentStartupRepo(t, backend, false)
			var read func(string) any
			read = func(path string) any {
				input := ActivityStructureInput{ThreadID: "thread", EntryIDs: ids, Path: path, Limit: 2}
				page, err := repo.ActivityStructure(t.Context(), input)
				if err != nil {
					t.Fatal(err)
				}
				switch page.Field.Kind {
				case "string":
					var text strings.Builder
					for text.Len() < page.Field.Bytes {
						part, err := repo.ReadActivityField(t.Context(), ActivityFieldInput{ThreadID: "thread", EntryID: page.Field.EntryID, Path: page.Field.SourcePath, SourceSHA256: page.Field.SourceSHA256, Offset: text.Len() + page.Field.SourceOffset, Limit: 4096})
						if err != nil {
							t.Fatal(err)
						}
						if part.Text == "" {
							t.Fatal("nonadvancing field")
						}
						text.WriteString(part.Text[:min(len(part.Text), page.Field.Bytes-text.Len())])
					}
					return text.String()
				case "scalar":
					var v any
					if json.Unmarshal(page.Field.Value, &v) != nil {
						t.Fatal("invalid scalar")
					}
					return v
				}
				children := append([]ActivityFieldSource(nil), page.Children...)
				for page.HasMore {
					input.Offset = page.Next
					page, err = repo.ActivityStructure(t.Context(), input)
					if err != nil {
						t.Fatal(err)
					}
					children = append(children, page.Children...)
				}
				if page.Field.Kind == "array" {
					out := make([]any, 0, len(children))
					for _, child := range children {
						out = append(out, read(child.Path))
					}
					return out
				}
				out := map[string]any{}
				for _, child := range children {
					key := child.Path[strings.LastIndex(child.Path, "/")+1:]
					out[key] = read(child.Path)
				}
				return out
			}
			got := read("")
			raw, err := json.Marshal(expected)
			if err != nil {
				t.Fatal(err)
			}
			var want any
			if json.Unmarshal(raw, &want) != nil {
				t.Fatal("expected json")
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("effective fields differ\nwant=%s\ngot=%#v", raw, got)
			}
		})
	}
}

func TestActivityStructureReadsOnlyVisibleRows(t *testing.T) {
	for _, count := range []int{20, 180} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			backend, repo := projectionSourcesFixture(t, 1)
			rows := make([]tools.StructuredActivityRow, count)
			for i := range rows {
				rows[i] = tools.StructuredActivityRow{Title: fmt.Sprint(i), Content: strings.Repeat("large field", 5000), Format: tools.StructuredActivityRowFormatCode}
			}
			_, err := repo.Append(t.Context(), Entry{ID: "activity", ThreadID: "thread", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "read", ToolCallID: "tool", Activity: &tools.ActivityPresentation{Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Rows: rows}}}}, AppendOptions{})
			if err != nil {
				t.Fatal(err)
			}
			repo = reopenCurrentStartupRepo(t, backend, false)
			if err := repo.PrepareDisplayThread(t.Context(), "thread"); err != nil {
				t.Fatal(err)
			}
			counted := &currentStartupBackend{Backend: backend}
			repo.backend = counted
			for _, offset := range []int{0, count / 2, count - 1} {
				counted.gets, counted.bodies, counted.journalRows = 0, 0, 0
				page, err := repo.ActivityStructure(t.Context(), ActivityStructureInput{ThreadID: "thread", EntryIDs: []string{"activity"}, Path: "/payload/rows", Offset: offset, Limit: 1})
				if err != nil {
					t.Fatal(err)
				}
				if page.Field.Count != count || len(page.Children) != 1 || page.Children[0].Path != fmt.Sprintf("/payload/rows/%d", offset) {
					t.Fatalf("wrong row page: %+v", page)
				}
				if counted.bodies != 0 || counted.journalRows != 0 || counted.gets > 50 {
					t.Fatalf("hidden IO gets=%d bodies=%d journal=%d", counted.gets, counted.bodies, counted.journalRows)
				}
			}
		})
	}
}

func TestActivityStructureRejectsChildrenOfSyntheticScalar(t *testing.T) {
	_, repo := projectionSourcesFixture(t, 1)
	for i, payload := range []tools.WebSearchActivityPayload{{Query: "q"}, {Results: []tools.WebSearchActivityResult{{Title: "t", URL: "https://example.com"}}}} {
		_, err := repo.Append(t.Context(), Entry{ID: fmt.Sprint("activity-", i), ThreadID: "thread", Type: EntryToolResult, Message: session.Message{Role: session.Tool, Activity: &tools.ActivityPresentation{Renderer: tools.ActivityRendererWebSearch, Payload: payload}}}, AppendOptions{})
		if err != nil {
			t.Fatal(err)
		}
	}
	_, err := repo.ActivityStructure(t.Context(), ActivityStructureInput{ThreadID: "thread", EntryIDs: []string{"activity-0", "activity-1"}, Path: "/payload/results_provided/invalid", Limit: 1})
	if !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("nonexistent child: %v", err)
	}
}

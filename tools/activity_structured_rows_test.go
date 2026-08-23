package tools

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestStructuredActivityRowsRoundTrip(t *testing.T) {
	t.Parallel()

	want := ActivityPresentation{
		Label:    "Knowledge search",
		Renderer: ActivityRendererStructured,
		Payload: StructuredActivityPayload{
			Status: "completed",
			Rows: []StructuredActivityRow{
				{Title: "Overview", Meta: "concept", Content: "Safe summary", Format: StructuredActivityRowFormatText},
				{Title: "Details", Content: "**Bounded** body", Format: StructuredActivityRowFormatMarkdown},
			},
		},
	}

	encoded, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	var got ActivityPresentation
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatal(err)
	}
	payload, ok := got.Payload.(StructuredActivityPayload)
	if !ok {
		t.Fatalf("payload type = %T", got.Payload)
	}
	if len(payload.Rows) != 2 || payload.Rows[1].Format != StructuredActivityRowFormatMarkdown || payload.Rows[1].Content != "**Bounded** body" {
		t.Fatalf("rows = %#v", payload.Rows)
	}
}

func TestStructuredActivityRowsCloneMergeAndFinalize(t *testing.T) {
	t.Parallel()

	left := &ActivityPresentation{
		Renderer: ActivityRendererStructured,
		Payload: StructuredActivityPayload{
			Status: "running",
			Rows:   []StructuredActivityRow{{Title: "old", Content: "old content"}},
		},
	}
	clone := CloneActivityPresentation(left)
	clonePayload := clone.Payload.(StructuredActivityPayload)
	clonePayload.Rows[0].Title = "mutated"
	if left.Payload.(StructuredActivityPayload).Rows[0].Title != "old" {
		t.Fatal("clone shares structured activity rows")
	}

	right := &ActivityPresentation{
		Renderer: ActivityRendererStructured,
		Payload: StructuredActivityPayload{
			Rows: []StructuredActivityRow{{Title: "new", Content: "new content", Format: StructuredActivityRowFormatCode}},
		},
	}
	merged := MergeActivityPresentations(left, right)
	mergedPayload := merged.Payload.(StructuredActivityPayload)
	if len(mergedPayload.Rows) != 1 || mergedPayload.Rows[0].Title != "new" {
		t.Fatalf("merged rows = %#v", mergedPayload.Rows)
	}
	right.Payload.(StructuredActivityPayload).Rows[0].Title = "changed"
	if left.Payload.(StructuredActivityPayload).Rows[0].Title != "old" {
		t.Fatal("merge shares structured activity rows")
	}
	if merged.Payload.(StructuredActivityPayload).Rows[0].Title != "new" {
		t.Fatal("merge shares rows with the right payload")
	}

	preserved := MergeActivityPresentations(merged, &ActivityPresentation{
		Renderer: ActivityRendererStructured,
		Payload:  StructuredActivityPayload{Status: "completed"},
	})
	preservedPayload := preserved.Payload.(StructuredActivityPayload)
	if len(preservedPayload.Rows) != 1 || preservedPayload.Rows[0].Title != "new" {
		t.Fatalf("terminal merge cleared rows = %#v", preservedPayload.Rows)
	}
	finalized := FinalizeActivityPresentation(preserved, "completed")
	finalPayload := finalized.Payload.(StructuredActivityPayload)
	if len(finalPayload.Rows) != 1 || finalPayload.Rows[0].Title != "new" {
		t.Fatalf("finalize cleared rows = %#v", finalPayload.Rows)
	}
}

func TestStructuredActivityRowsValidateBounds(t *testing.T) {
	t.Parallel()

	valid := func(rows []StructuredActivityRow) error {
		return (ActivityPresentation{
			Renderer: ActivityRendererStructured,
			Payload:  StructuredActivityPayload{Rows: rows},
		}).Validate()
	}

	tests := []struct {
		name string
		rows []StructuredActivityRow
	}{
		{name: "empty row", rows: []StructuredActivityRow{{}}},
		{name: "unknown format", rows: []StructuredActivityRow{{Title: "row", Format: "html"}}},
		{name: "oversized text", rows: []StructuredActivityRow{{Content: strings.Repeat("x", maxActivityTextRunes+1)}}},
		{name: "too many rows", rows: make([]StructuredActivityRow, maxActivityPayloadItems+1)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := valid(test.rows); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
	if err := valid([]StructuredActivityRow{{Content: "plain text"}}); err != nil {
		t.Fatalf("default text format: %v", err)
	}
}

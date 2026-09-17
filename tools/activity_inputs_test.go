package tools

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestStructuredActivityInputsSurviveResultUpdates(t *testing.T) {
	source := "  // Inspect the page\n" + strings.Repeat("x", 12_000) + "\n"
	call := &ActivityPresentation{Label: "Inspect the page", Renderer: ActivityRendererStructured,
		Payload: StructuredActivityPayload{Inputs: []StructuredActivityRow{{Content: source, Format: StructuredActivityRowFormatCode, Language: "javascript"}}}}
	result := &ActivityPresentation{Renderer: ActivityRendererStructured,
		Payload: StructuredActivityPayload{RowsProvided: true, Rows: []StructuredActivityRow{{Content: "Page title", Truncated: true}}}}
	merged := MergeActivityPresentations(call, result)
	encoded, err := json.Marshal(merged)
	if err != nil {
		t.Fatal(err)
	}
	var replay ActivityPresentation
	if err := json.Unmarshal(encoded, &replay); err != nil {
		t.Fatal(err)
	}
	payload := replay.Payload.(StructuredActivityPayload)
	if replay.Label != call.Label || !payload.RowsProvided || len(payload.Inputs) != 1 || payload.Inputs[0].Content != source || payload.Inputs[0].Language != "javascript" || !payload.Rows[0].Truncated {
		t.Fatalf("round trip lost input or result: %#v", payload)
	}
	cloned := CloneActivityPresentation(merged)
	cloned.Payload.(StructuredActivityPayload).Inputs[0].Content = "mutated"
	if merged.Payload.(StructuredActivityPayload).Inputs[0].Content != source || call.Payload.(StructuredActivityPayload).Inputs[0].Content != source {
		t.Fatal("inputs share memory")
	}
	empty := MergeActivityPresentations(merged, &ActivityPresentation{Renderer: ActivityRendererStructured, Payload: StructuredActivityPayload{RowsProvided: true}})
	empty = FinalizeActivityPresentation(empty, "success")
	payload = empty.Payload.(StructuredActivityPayload)
	if !payload.RowsProvided || len(payload.Rows) != 0 || len(payload.Inputs) != 1 {
		t.Fatalf("explicit empty result = %#v", payload)
	}
}

func TestStructuredActivityInputLimits(t *testing.T) {
	for _, test := range []struct {
		name  string
		row   StructuredActivityRow
		valid bool
	}{
		{"complete script", StructuredActivityRow{Content: strings.Repeat("x", 64<<10), Format: StructuredActivityRowFormatCode, Language: "javascript"}, true},
		{"oversized script", StructuredActivityRow{Content: strings.Repeat("x", (64<<10)+1), Format: StructuredActivityRowFormatCode}, false},
		{"oversized unicode script", StructuredActivityRow{Content: strings.Repeat("界", (64<<10)/3+1), Format: StructuredActivityRowFormatCode}, false},
		{"oversized text", StructuredActivityRow{Content: strings.Repeat("x", 8001)}, false},
		{"invalid language", StructuredActivityRow{Content: "code", Format: StructuredActivityRowFormatCode, Language: "<script>"}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			activity := ActivityPresentation{Renderer: ActivityRendererStructured, Payload: StructuredActivityPayload{Inputs: []StructuredActivityRow{test.row}}}
			if err := activity.Validate(); (err == nil) != test.valid {
				t.Fatalf("Validate() = %v, valid=%v", err, test.valid)
			}
		})
	}
}

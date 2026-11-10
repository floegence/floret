package session

import (
	"encoding/json"
	"testing"
)

func TestRetainedOutputEnvelopeKeepsErrorAndExecutionFacts(t *testing.T) {
	original := Message{Role: Tool, Content: `ERROR: {"status":"error","confirmed_effect":"created","uncertain":true}`, ToolResult: &ToolResultView{Status: "error", Truncated: true, OutputUnavailable: "storage_error", OriginalBytes: 50000}}
	projected, _, err := ProjectProviderHistory([]Message{original}, "")
	if err != nil || len(projected) != 1 {
		t.Fatal(err)
	}
	var result struct {
		Status string `json:"status"`
		Result struct {
			Confirmed string `json:"confirmed_effect"`
			Uncertain bool   `json:"uncertain"`
		} `json:"result"`
		Output struct {
			Reason  string `json:"unavailable_reason"`
			Omitted bool   `json:"preview_omitted"`
		} `json:"retained_output"`
	}
	if err := json.Unmarshal([]byte(projected[0].Content), &result); err != nil {
		t.Fatal(err)
	}
	if result.Status != "error" || result.Result.Confirmed != "created" || !result.Result.Uncertain || result.Output.Reason != "storage_error" || !result.Output.Omitted {
		t.Fatalf("facts changed: %+v", result)
	}
	if ProviderContent(projected[0]) != projected[0].Content {
		t.Fatal("projected output was wrapped twice")
	}
	if original.ToolResult.OutputUnavailable != "storage_error" {
		t.Fatal("projection mutated original")
	}
}

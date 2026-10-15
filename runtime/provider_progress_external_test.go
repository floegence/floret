package runtime_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/floegence/floret/v7/runtime"
	"github.com/floegence/floret/v7/tools"
)

func TestProviderProgressPublicContractPreservesUnknownAndZero(t *testing.T) {
	zero := 0
	definition := tools.Definition{NewArgumentPreview: func() tools.ArgumentPreviewer {
		return func(delta string, complete bool) (*tools.ArgumentPreview, error) {
			return &tools.ArgumentPreview{DisplayName: "main.go", Additions: &zero}, nil
		}
	}}
	preview, err := definition.NewArgumentPreview()("partial", false)
	if err != nil {
		t.Fatal(err)
	}
	view := runtime.ThreadView{ThreadID: "thread", TurnID: "turn", RunID: "run", RunProgress: &runtime.ThreadRunProgress{Phase: runtime.ThreadRunPhaseStreaming, Request: &runtime.ProviderRequestProgress{LogicalRequestID: "logical", AttemptID: "attempt", AttemptEpoch: 1, StartedAt: time.Now(), EstimatedOutputTokens: 3}}, ToolCallPreviews: []runtime.ThreadToolCallPreview{{ThreadID: "thread", TurnID: "turn", RunID: "run", LogicalRequestID: "logical", AttemptID: "attempt", AttemptEpoch: 1, CallID: "call", Name: "editing_tool", Preview: preview}}}
	data, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	var decoded runtime.ThreadView
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	got := decoded.ToolCallPreviews[0].Preview
	if got.Additions == nil || *got.Additions != 0 || got.Deletions != nil || decoded.RunProgress.Request.EstimatedOutputTokens != 3 || len(decoded.Items) != 0 {
		t.Fatalf("contract round trip: %s", data)
	}
}

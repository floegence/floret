package sessiontree

import (
	"encoding/json"
	"testing"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/tools"
)

func TestActivityErrorCodeSurvivesCanonicalRestart(t *testing.T) {
	backend, repo := projectionSourcesFixture(t, 1)
	want := tools.ActivityError{Message: "Observation unavailable", Code: "TARGET_OBSERVATION_UNAVAILABLE"}
	activity := &tools.ActivityPresentation{Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Error: &want}}
	_, err := repo.Append(t.Context(), Entry{ID: "coded-error", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "inspect", ToolCallID: "work", Activity: activity}}, AppendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		repo = reopenCurrentStartupRepo(t, backend, false)
		if err := repo.PrepareDisplayThread(t.Context(), "thread"); err != nil {
			t.Fatal(err)
		}
		value, err := repo.ReadItemActivityValue(t.Context(), ItemActivityValueInput{ThreadID: "thread", ItemID: "tool:turn:work", Path: "/payload/error", MaximumBytes: 1024, MaximumNodes: 16})
		if err != nil {
			t.Fatal(err)
		}
		var got tools.ActivityError
		if err := json.Unmarshal(value.Value, &got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("diagnosis changed after restart: %+v", got)
		}
	}
}

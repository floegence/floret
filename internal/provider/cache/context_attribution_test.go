package cache

import (
	"reflect"
	"testing"

	"github.com/floegence/floret/v7/internal/session"
)

func TestBuildPlanPreservesCanonicalContextAttribution(t *testing.T) {
	store := NewMemoryStore()
	canonical := []session.Message{{Role: session.User, Kind: session.MessageKindModelContext, ModelContextSnapshot: &session.ModelContextSnapshot{Sections: []session.ModelContextSection{{Key: "skills", Title: "Skills", Text: "目录"}, {Key: "execution_environment", Title: "Environment", Text: "local"}}}}}
	history, _, err := session.ProjectProviderHistory(canonical, "")
	if err != nil {
		t.Fatal(err)
	}
	input := BuildInput{PromptScopeID: "scope", RunID: "run", Provider: "deepseek", Model: "deepseek-flash", SystemPrompt: "system", History: history}
	for _, stage := range []string{"new", "reused"} {
		plan, messages, err := BuildPlan(t.Context(), store, input)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(messages[1].ModelContextParts, history[0].ModelContextParts) {
			t.Fatalf("%s plan lost canonical source intervals: %+v", stage, messages[1])
		}
		if stage == "reused" && plan.ReusedSegments != 2 {
			t.Fatalf("expected cached segments: %+v", plan)
		}
		messages[1].ModelContextParts[0].Text = "mutated"
		if history[0].ModelContextParts[0].Text == "mutated" {
			t.Fatal("canonical attribution aliased")
		}
	}
}

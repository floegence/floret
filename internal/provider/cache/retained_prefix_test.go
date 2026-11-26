package cache

import (
	"errors"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/session/artifact"
	"testing"
)

func TestRetainedPrefixUpgradeRequiresExactHistory(t *testing.T) {
	for _, tc := range []struct {
		name                                               string
		legacy, short, alterText, alterTool, reset, reject bool
	}{
		{name: "pre-range", legacy: true, reset: true}, {name: "already ranged"}, {name: "shortened", legacy: true, short: true, reject: true}, {name: "changed ordinary message", legacy: true, alterText: true, reject: true}, {name: "changed tool result", legacy: true, alterTool: true, reject: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := []session.Message{{Role: session.User, Content: "Inspect."}, {Role: session.Tool, ToolCallID: "one", Content: "saved", ToolResult: &session.ToolResultView{Truncated: true, FullOutput: &artifact.Ref{SizeBytes: 50}, OutputRef: "ref", Status: "ok"}}}
			project := func() []session.Message {
				h, _, err := session.ProjectProviderHistory(source, "")
				if err != nil {
					t.Fatal(err)
				}
				return h
			}
			old := project()
			if tc.legacy {
				old = session.ProjectPreRangeHistory(source)
			}
			store := NewMemoryStore()
			plan := lineagePlan("test", "model", "ns", "old")
			plan.ContextProjectionRevision = contextProjectionV12
			if err := ValidateCanonicalLineage(t.Context(), store, "scope", &plan, "", nil, nil, nil, old, nil); err != nil {
				t.Fatal(err)
			}
			if _, err := RecordRequest(t.Context(), store, PromptScopeRef{PromptScopeID: "scope", RunID: "old"}, 1, "test", "model", CachePolicy{}, plan); err != nil {
				t.Fatal(err)
			}
			if tc.short {
				source = source[:1]
			} else {
				if tc.alterText {
					source[0].Content += "!"
				}
				if tc.alterTool {
					source[1].Content += "!"
				}
				source = append(source, session.Message{Role: session.User, Content: "Continue."})
			}
			next := lineagePlan("test", "model", "ns", "new")
			err := ValidateCanonicalLineage(t.Context(), store, "scope", &next, "", nil, nil, nil, project(), source)
			if tc.reject {
				if !errors.Is(err, ErrContextPrefixDrift) {
					t.Fatalf("accepted drift: %v", err)
				}
				return
			}
			if err != nil || next.CanonicalLineageReset != tc.reset {
				t.Fatalf("reset=%v err=%v", next.CanonicalLineageReset, err)
			}
		})
	}
}

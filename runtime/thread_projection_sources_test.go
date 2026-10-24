package runtime

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/tools"
)

// Dependency reads feed the released reducer. In particular an approval before
// a call, a later tool-input resolution and fragments across a window boundary
// must reconstruct the same item without a second presentation implementation.
func TestProjectionSourcesReconstructCanonicalItems(t *testing.T) {
	host, _ := testThreadService(t, newBlockingThreadGateway())
	for _, memory := range []bool{false, true} {
		name := "backend"
		var repo sessiontree.Repo = host.store.repo
		if memory {
			name, repo = "memory", sessiontree.NewMemoryRepo()
		}
		t.Run(name, func(t *testing.T) {
			if _, err := repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: "thread"}); err != nil {
				t.Fatal(err)
			}
			var entries []sessiontree.Entry
			for _, entry := range incrementalProjectionFixture(t) {
				saved, err := repo.Append(t.Context(), entry, sessiontree.AppendOptions{ID: entry.ID})
				if err != nil {
					t.Fatalf("append %s: %v", entry.ID, err)
				}
				entries = append(entries, saved)
			}
			want, _, err := threadRuntimeItemsFromEntries(entries)
			if err != nil {
				t.Fatal(err)
			}
			reader := repo.(sessiontree.ProjectionSourceScanner)
			for _, example := range []struct{ seed, item string }{
				{"text-b", "answer"}, {"text-a", "thinking"},
				{"work-result", threadToolSegmentID("turn", "work")},
				{"work-call", "interaction:tool-input:work-result"},
				{"review-result", threadToolSegmentID("turn", "review")},
				{"hosted", "hosted:turn:run:search"},
			} {
				t.Run(example.seed+"/"+example.item, func(t *testing.T) {
					page, err := reader.ReadProjectionSources(t.Context(), "thread", "", []string{example.seed}, func(source sessiontree.PathMetadata, _ bool) []string {
						fields := make([]string, len(source.Bodies))
						for i, body := range source.Bodies {
							fields[i] = body.Field
						}
						return fields
					})
					if err != nil {
						t.Fatal(err)
					}
					var sources []sessiontree.Entry
					for _, source := range page.Entries {
						sources = append(sources, source.Entry)
					}
					got, _, err := threadRuntimeItemsFromEntries(sources)
					if err != nil {
						t.Fatal(err)
					}
					find := func(items []ThreadItem) []byte {
						for _, item := range items {
							if item.ID == example.item {
								// Position is supplied by item membership, not this dependency API.
								item.Ordinal, item.CanonicalOrdinal = 0, 0
								raw, err := json.Marshal(item)
								if err != nil {
									t.Fatal(err)
								}
								return raw
							}
						}
						return nil
					}
					if actual, expected := find(got), find(want); actual == nil || !reflect.DeepEqual(actual, expected) {
						t.Fatalf("item differs:\ngot %s\nwant %s", actual, expected)
					}
				})
			}
		})
	}
}

func TestProjectionSourceTailMatchesFullItemOrder(t *testing.T) {
	host, public := testThreadService(t, newBlockingThreadGateway())
	service := public.(*threadRuntimeService)
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	for i := range 300 {
		entry := sessiontree.Entry{ID: fmt.Sprint("earlier-", i), ThreadID: "thread", TurnID: "old-turn", RunID: "old-run", Type: sessiontree.EntryUserMessage, CreatedAt: time.Unix(10, 0).UTC(), Message: session.Message{Role: session.User, Content: "Earlier"}}
		if _, err := host.store.repo.Append(t.Context(), entry, sessiontree.AppendOptions{ID: entry.ID}); err != nil {
			t.Fatal(err)
		}
	}
	for _, entry := range incrementalProjectionFixture(t) {
		if _, err := host.store.repo.Append(t.Context(), entry, sessiontree.AppendOptions{ID: entry.ID}); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := host.store.repo.Path(t.Context(), "thread", "")
	if err != nil {
		t.Fatal(err)
	}
	want, _, err := threadRuntimeItemsFromEntries(entries)
	if err != nil {
		t.Fatal(err)
	}
	for _, limit := range []int{1, 8, 150} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			got, err := service.projectThreadSourceTail(t.Context(), "thread", limit)
			if err != nil {
				t.Fatal(err)
			}
			if len(got.items) != limit {
				t.Fatalf("items=%d want=%d", len(got.items), limit)
			}
			for i, item := range got.items {
				expected := want[len(want)-limit+i]
				if item.ID != expected.ID || item.CanonicalOrdinal != expected.CanonicalOrdinal || item.Text != expected.Text || !reflect.DeepEqual(item.Interaction, expected.Interaction) {
					t.Fatalf("item %d differs: got=%+v want=%+v", i, item, expected)
				}
			}
		})
	}
}

func TestItemMembershipWindowMatchesEveryCanonicalPrefix(t *testing.T) {
	host, _ := testThreadService(t, newBlockingThreadGateway())
	repo := host.store.repo
	if _, err := repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	reader := repo.(sessiontree.ItemMembershipReader)
	var entries []sessiontree.Entry
	for _, entry := range incrementalProjectionFixture(t) {
		saved, err := repo.Append(t.Context(), entry, sessiontree.AppendOptions{ID: entry.ID})
		if err != nil {
			t.Fatalf("append %s: %v", entry.ID, err)
		}
		entries = append(entries, saved)
		want, _, err := threadRuntimeItemsFromEntries(entries)
		if err != nil {
			t.Fatal(err)
		}
		for _, limit := range []int{1, 4, 150} {
			page, err := reader.ItemMembershipWindow(t.Context(), sessiontree.ItemMembershipWindowOptions{ThreadID: "thread", Limit: limit})
			if err != nil {
				t.Fatalf("prefix %s limit %d: %v", entry.ID, limit, err)
			}
			selected := want[max(0, len(want)-limit):]
			if len(page.Members) != len(selected) {
				t.Fatalf("prefix %s count=%d want=%d", entry.ID, len(page.Members), len(selected))
			}
			for i, member := range page.Members {
				item := selected[i]
				if member.ID != item.ID || member.Kind != string(item.Kind) || member.CanonicalOrdinal != item.CanonicalOrdinal || page.Ordinals[i] != item.Ordinal {
					t.Fatalf("prefix %s member=%+v ordinal=%d item=%+v", entry.ID, member, page.Ordinals[i], item)
				}
			}
		}
	}
}

func TestSparseGoalSourcesMatchFullHistoricalPresentation(t *testing.T) {
	host, public := testThreadService(t, newBlockingThreadGateway())
	service := public.(*threadRuntimeService)
	repo := host.store.repo
	if _, err := repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: "goal-sparse"}); err != nil {
		t.Fatal(err)
	}
	add := func(e sessiontree.Entry) {
		t.Helper()
		e.ThreadID = "goal-sparse"
		e.TurnID = "turn"
		e.RunID = "run"
		if _, err := repo.Append(t.Context(), e, sessiontree.AppendOptions{ID: e.ID}); err != nil {
			t.Fatalf("%s: %v", e.ID, err)
		}
	}
	add(sessiontree.Entry{ID: "start", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted})
	for i := 1; i <= 100; i++ {
		payload, _ := json.Marshal(sessiontree.GoalChange{State: session.GoalState{Version: int64(i), Text: fmt.Sprint("Goal ", i), Status: "active", Actor: "user"}, Operation: "set"})
		add(sessiontree.Entry{ID: fmt.Sprint("goal-", i), Type: sessiontree.EntryGoal, Payload: payload})
	}
	encode := func(v any) []byte {
		raw, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	add(sessiontree.Entry{ID: "review-call", Type: sessiontree.EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "goal", ToolCallID: "review", ToolArgs: string(encode(map[string]any{"operation": "review", "expected_version": 100, "outcome": "completed", "evidence": "verified"}))}})
	add(sessiontree.Entry{ID: "matching-review", Type: sessiontree.EntryGoalReview, Payload: encode(sessiontree.GoalReview{Version: 100, Outcome: "completed", Evidence: "verified"})})
	add(sessiontree.Entry{ID: "review-result", Type: sessiontree.EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "goal", ToolCallID: "review", ToolResult: &session.ToolResultView{Status: "success"}}})
	add(sessiontree.Entry{ID: "complete", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnCompleted, Metadata: map[string]string{sessiontree.GoalCompletedMetadataKey: "100"}})
	path, err := repo.Path(t.Context(), "goal-sparse", "")
	if err != nil {
		t.Fatal(err)
	}
	want, _, err := threadRuntimeItemsFromEntries(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range want {
		if expected.Kind != ThreadItemGoal && expected.Kind != ThreadItemTool {
			continue
		}
		got, err := service.projectThreadMembershipWindow(t.Context(), sessiontree.ItemMembershipWindowOptions{ThreadID: "goal-sparse", AnchorID: expected.ID, Direction: sessiontree.PathWindowAround, Limit: 1})
		if err != nil {
			t.Fatalf("%s: %v", expected.ID, err)
		}
		if len(got.items) != 1 {
			t.Fatalf("%s: %d items", expected.ID, len(got.items))
		}
		actual := got.items[0]
		if actual.ID != expected.ID || !reflect.DeepEqual(actual.Goal, expected.Goal) || !reflect.DeepEqual(actual.Activity, expected.Activity) {
			t.Fatalf("%s differs: got=%+v want=%+v", expected.ID, actual, expected)
		}
		if got.sources > 8 {
			t.Fatalf("%s read %d sources", expected.ID, got.sources)
		}
	}
	view, _, err := service.loadSourceWindow(t.Context(), ThreadWindowInput{ThreadID: "goal-sparse", Limit: 1})
	if err != nil || view.Goal == nil || view.Goal.Version != 100 || view.Goal.Status != "completed" {
		t.Fatalf("current Goal=%+v err=%v", view.Goal, err)
	}
}

func TestFieldWindowEffectiveToolSourcesMatchCompleteProjection(t *testing.T) {
	for _, fixture := range []string{"replacements", "interactions", "late-dispatch"} {
		t.Run(fixture, func(t *testing.T) {
			host, public := testThreadService(t, newBlockingThreadGateway())
			service := public.(*threadRuntimeService)
			if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: "thread"}); err != nil {
				t.Fatal(err)
			}
			entries := incrementalProjectionFixture(t)
			if fixture != "interactions" {
				entries = []sessiontree.Entry{{ID: "call", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: sessiontree.EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: "call", Activity: &tools.ActivityPresentation{Label: "Inspect", Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Inputs: []tools.StructuredActivityRow{{Content: "retained input", Format: tools.StructuredActivityRowFormatCode}}}}}}}
				for i := range 80 {
					entries = append(entries, sessiontree.Entry{ID: fmt.Sprint("result-", i), ThreadID: "thread", TurnID: "turn", RunID: "run", Type: sessiontree.EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "inspect", ToolCallID: "call", Activity: &tools.ActivityPresentation{Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Summary: fmt.Sprint(i)}}, ToolResult: &session.ToolResultView{Status: "success"}}})
				}
				if fixture == "late-dispatch" {
					entries = append(entries, sessiontree.Entry{ID: "dispatch", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: sessiontree.EntryCustom, Metadata: map[string]string{"kind": "tool_dispatch", "tool_id": "call"}})
				}
			}
			for _, entry := range entries {
				if _, err := host.store.repo.Append(t.Context(), entry, sessiontree.AppendOptions{ID: entry.ID}); err != nil {
					t.Fatal(err)
				}
			}
			input := ThreadWindowInput{ThreadID: "thread", Limit: 150}
			full, _, err := service.loadSourceWindow(t.Context(), input)
			if err != nil {
				t.Fatal(err)
			}
			input.ActivityFields = true
			bounded, _, err := service.loadSourceWindow(t.Context(), input)
			if err != nil {
				t.Fatal(err)
			}
			actor := service.runtime("thread")
			project := func(view canonicalThreadView) []byte {
				t.Helper()
				for i, item := range view.Items {
					view.Items[i], err = actor.windowItemMode(t.Context(), item, 4096, true)
					if err != nil {
						t.Fatalf("item %s (%s): %v", item.ID, item.Kind, err)
					}
				}
				value, err := json.Marshal(view.ThreadView)
				if err != nil {
					t.Fatal(err)
				}
				return value
			}
			if got, want := project(bounded), project(full); string(got) != string(want) {
				t.Fatalf("field window changed canonical display: got %s want %s", got, want)
			}
		})
	}
}

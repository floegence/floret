package runtime

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
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

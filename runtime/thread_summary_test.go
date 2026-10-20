package runtime

import (
	"context"
	"errors"
	"fmt"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"reflect"
	"testing"
	"time"
)

func TestThreadSummaryReaderDoesNotCreateActor(t *testing.T) {
	host, service := testThreadService(t, newBlockingThreadGateway())
	_ = host

	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "summary"})
	if err != nil {
		t.Fatal(err)
	}
	before := len(service.(*threadRuntimeService).runtimes)
	summary, err := service.(ThreadSummaryReader).Summary(t.Context(), identity.ThreadID(created.ThreadID))
	if err != nil {
		t.Fatal(err)
	}
	if summary.ID != created.ThreadID || len(service.(*threadRuntimeService).runtimes) != before {
		t.Fatal("summary changed actor inventory")
	}
	listed, err := service.List(t.Context(), ThreadScope{})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || !reflect.DeepEqual(listed[0], summary) {
		t.Fatalf("summary differs from list: %+v %+v", summary, listed)
	}
}

type summaryReadCountingRepo struct {
	sessiontree.Repo
	paths, inventories int
}

func (repo *summaryReadCountingRepo) Path(ctx context.Context, threadID, leaf string) ([]sessiontree.Entry, error) {
	repo.paths++
	return repo.Repo.Path(ctx, threadID, leaf)
}
func (repo *summaryReadCountingRepo) ListThreads(ctx context.Context, options sessiontree.ListThreadsOptions) ([]sessiontree.ThreadMeta, error) {
	return sessiontree.ListThreads(ctx, repo.Repo, options)
}
func (repo *summaryReadCountingRepo) ListRootThreadInventory(context.Context, sessiontree.ListThreadsOptions) ([]sessiontree.RootThreadInventoryItem, error) {
	repo.inventories++
	return nil, errors.New("full inventory must not be read")
}

func TestThreadActiveSummariesNeverReadHistoricalBodies(t *testing.T) {
	host, full := testThreadService(t, newBlockingThreadGateway())
	created, err := full.Create(t.Context(), CreateThreadInput{RequestKey: "bounded-summary"})
	if err != nil {
		t.Fatal(err)
	}
	service := full.(*threadRuntimeService)
	actor := service.runtime(created.ThreadID)
	if err := actor.apply(t.Context(), func() error {
		actor.state.view.Items = []ThreadItem{{ID: "first", Text: "summary fact", CreatedAt: time.Unix(100, 0)}}
		actor.state.view.ViewVersion++
		actor.markItem("first", ThreadItemAdded)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	repo := &summaryReadCountingRepo{Repo: host.store.repo}
	host.store.repo = repo
	for range 3 {
		summary, err := service.Summary(t.Context(), created.ThreadID)
		if err != nil {
			t.Fatal(err)
		}
		if summary.LastItemPreview != "summary fact" {
			t.Fatalf("summary=%+v", summary)
		}
		list, err := service.List(t.Context(), ThreadScope{})
		if err != nil {
			t.Fatal(err)
		}
		if len(list) != 1 || !reflect.DeepEqual(summary, list[0]) {
			t.Fatal("summary and list differ")
		}
	}
	if repo.paths != 0 || repo.inventories != 0 {
		t.Fatalf("full paths=%d inventories=%d", repo.paths, repo.inventories)
	}
}

func TestActorSummaryIndexMatchesCompleteAfterEveryMutation(t *testing.T) {
	actor := &threadRuntimeState{state: threadRuntimeData{view: ThreadView{ThreadID: "summary", ViewVersion: 1}}}
	meta := sessiontree.ThreadMeta{ID: "summary", Title: "Summary"}
	compare := func() {
		t.Helper()
		actor.refreshAttention()
		want := threadSummaryFromView(meta, actor.state.view)
		got := actor.threadSummary(meta)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("indexed summary differs\ngot=%+v\nwant=%+v", got, want)
		}
	}
	apply := func(mutate func()) {
		t.Helper()
		if err := actor.apply(t.Context(), func() error { mutate(); return nil }); err != nil {
			t.Fatal(err)
		}
		compare()
	}
	compare()
	for i := range 100 {
		apply(func() {
			item := ThreadItem{ID: fmt.Sprint(i), CreatedAt: time.Unix(int64(i), 0)}
			if i == 0 || i%17 == 0 {
				item.Text = fmt.Sprintf("preview %d", i)
			}
			if i%11 == 0 {
				item.CreatedAt = time.Time{}
			}
			actor.state.view.Items = append(actor.state.view.Items, item)
			actor.markItem(item.ID, ThreadItemAdded)
		})
	}
	apply(func() { actor.state.view.Items[99].Text = "latest "; actor.markItem("99", ThreadItemReplaced) })
	apply(func() { actor.state.view.Items[99].Text = " \t "; actor.markItem("99", ThreadItemReplaced) })
	apply(func() {
		actor.state.view.Items[98].Text = "time-less preview"
		actor.state.view.Items[98].CreatedAt = time.Time{}
		actor.markItem("98", ThreadItemReplaced)
	})
	apply(func() { actor.state.view.Items[99].CreatedAt = time.Time{}; actor.markItem("99", ThreadItemReplaced) })
	apply(func() { actor.replaceItems(append([]ThreadItem(nil), actor.state.view.Items[:50]...)) })
	apply(func() { actor.replaceItems([]ThreadItem{{ID: "single", Text: " new 🌸 ", CreatedAt: time.Now()}}) })
	apply(func() {
		actor.installInteraction(ThreadInteraction{ID: "ask", Kind: ThreadInteractionInput, Input: &InputPresentation{Summary: "Question"}}, 0)
	})
	apply(func() {
		actor.installInteraction(ThreadInteraction{ID: "ask", Kind: ThreadInteractionInput, Input: &InputPresentation{Summary: "Question"}, Resolved: true}, 0)
	})
	apply(func() { actor.replaceItems(nil) })
}

func BenchmarkActorSummaryFixedHistory(b *testing.B) {
	for _, count := range []int{1096, 10960, 109600} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			actor := &threadRuntimeState{state: threadRuntimeData{view: ThreadView{ThreadID: "summary", ViewVersion: 1, Items: make([]ThreadItem, count)}}}
			actor.state.view.Items[0] = ThreadItem{ID: "first", Text: "stable preview", CreatedAt: time.Unix(100, 0)}
			for i := 1; i < count; i++ {
				actor.state.view.Items[i] = ThreadItem{ID: fmt.Sprint(i), Kind: ThreadItemTool, CreatedAt: time.Unix(int64(i), 0)}
			}
			meta := sessiontree.ThreadMeta{ID: "summary"}
			actor.threadSummary(meta)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if actor.threadSummary(meta).LastItemPreview != "stable preview" {
					b.Fatal("preview changed")
				}
			}
		})
	}
}

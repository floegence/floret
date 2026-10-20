package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/floegence/floret/v7/internal/session"

	"github.com/floegence/floret/v7/internal/agentharness"
	"github.com/floegence/floret/v7/internal/sessiontree"
)

func TestCanonicalActorChangesMatchEveryCompletePrefix(t *testing.T) {
	entries := incrementalProjectionFixture(t)
	for i := 1; i < len(entries); i++ {
		entries[i].ParentID = entries[i-1].ID
	}
	for size := 1; size <= len(entries); size++ {
		projection := newThreadCanonicalProjection()
		actor := &threadRuntimeState{state: threadRuntimeData{view: ThreadView{ThreadID: "thread", Activity: ThreadActivityActive, Items: []ThreadItem{}, Interactions: []ThreadInteraction{}}}}
		for start := 0; start < len(entries); start += size {
			end := min(start+size, len(entries))
			projection.resetChanges()
			if err := projection.append(entries[start:end]); err != nil {
				t.Fatalf("chunk=%d prefix=%d: %v", size, end, err)
			}
			if err := actor.installCanonicalChanges(projection); err != nil {
				t.Fatalf("chunk=%d prefix=%d: %v", size, end, err)
			}
			items, interactions, err := threadRuntimeItemsFromEntries(entries[:end])
			if err != nil {
				t.Fatal(err)
			}
			turns, err := threadRuntimeTurnsFromEntries(entries[:end], items)
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := agentharness.ProjectThreadContext(entries[:end])
			if err != nil {
				t.Fatal(err)
			}
			context := runtimeCanonicalContext(snapshot, nil, projection.anchors, projection.compactionOrdinals)
			compare := func(label string, got, want any) {
				t.Helper()
				left, _ := json.Marshal(got)
				right, _ := json.Marshal(want)
				if string(left) != string(right) {
					t.Fatalf("chunk=%d prefix=%d %s\ngot=%s\nwant=%s", size, end, label, left, right)
				}
			}
			compare("items", actor.state.view.Items, items)
			compare("interactions", actor.state.view.Interactions, interactions)
			compare("turns", actor.state.view.Turns, turns)
			if actor.state.view.Context != nil {
				compare("context", actor.state.view.Context, &context)
			}
			compare("goal", actor.state.view.Goal, publicThreadGoal(projection.items.goal.State))
		}
	}
}

func TestCanonicalProjectionRejectsChangedParent(t *testing.T) {
	projection := newThreadCanonicalProjection()
	if err := projection.append([]sessiontree.Entry{{ID: "root", ThreadID: "thread", Type: sessiontree.EntryCustom}}); err != nil {
		t.Fatal(err)
	}
	if err := projection.append([]sessiontree.Entry{{ID: "wrong", ThreadID: "thread", ParentID: "other", Type: sessiontree.EntryCustom}}); err == nil {
		t.Fatal("changed canonical parent accepted")
	}
}

func BenchmarkCanonicalActorFixedHistoryChange(b *testing.B) {
	for _, count := range []int{1096, 10960, 109600} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			projection := newThreadCanonicalProjection()
			entries := make([]sessiontree.Entry, 0, 2*count+1)
			for i := range count {
				id := fmt.Sprint(i)
				entries = append(entries, sessiontree.Entry{ID: "start-" + id, TurnID: id, RunID: id, Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted}, sessiontree.Entry{ID: "end-" + id, TurnID: id, RunID: id, Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnCompleted})
			}
			entries = append(entries, sessiontree.Entry{ID: "active", TurnID: "active", RunID: "active", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted})
			for i := 1; i < len(entries); i++ {
				entries[i].ParentID = entries[i-1].ID
			}
			if err := projection.append(entries); err != nil {
				b.Fatal(err)
			}
			actor := &threadRuntimeState{state: threadRuntimeData{view: ThreadView{ThreadID: "thread", Activity: ThreadActivityActive}}}
			if err := actor.installCanonicalChanges(projection); err != nil {
				b.Fatal(err)
			}
			projection.resetChanges()
			entry := sessiontree.Entry{ID: "update", ParentID: "active", TurnID: "active", RunID: "active", Type: sessiontree.EntryCustom}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				projection.resetChanges()
				entry.ParentID = projection.leafID
				if err := projection.append([]sessiontree.Entry{entry}); err != nil {
					b.Fatal(err)
				}
				if err := actor.installCanonicalChanges(projection); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

type canonicalReadCountingRepo struct {
	sessiontree.Repo
	reader         sessiontree.PathEntriesWindowReader
	paths, windows int
	windowErr      error
}

func (repo *canonicalReadCountingRepo) Path(ctx context.Context, threadID, leaf string) ([]sessiontree.Entry, error) {
	repo.paths++
	return repo.Repo.Path(ctx, threadID, leaf)
}
func (repo *canonicalReadCountingRepo) PathEntriesWindow(ctx context.Context, options sessiontree.PathWindowOptions) (sessiontree.PathEntriesWindow, error) {
	repo.windows++
	if repo.windowErr != nil {
		return sessiontree.PathEntriesWindow{}, repo.windowErr
	}
	return repo.reader.PathEntriesWindow(ctx, options)
}
func TestCanonicalActorReadsOnlyAppendedStorageIntervalAndRecoversAfterFailure(t *testing.T) {
	host, full := testThreadService(t, newBlockingThreadGateway())
	created, err := full.Create(t.Context(), CreateThreadInput{RequestKey: "canonical-incremental"})
	if err != nil {
		t.Fatal(err)
	}
	service := full.(*threadRuntimeService)
	repo := &canonicalReadCountingRepo{Repo: host.store.repo, reader: host.store.repo.(sessiontree.PathEntriesWindowReader)}
	host.store.repo = repo
	actor := service.runtime(created.ThreadID)
	for i := range 600 {
		_, err := repo.Append(t.Context(), sessiontree.Entry{ThreadID: created.ThreadID.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: fmt.Sprintf("body-%d 中文🌸", i)}}, sessiontree.AppendOptions{ID: fmt.Sprint(i)})
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := actor.apply(t.Context(), func() error { return service.advanceCanonical(t.Context(), actor) }); err != nil {
		t.Fatal(err)
	}
	if repo.paths != 0 || repo.windows != 3 || len(actor.state.view.Items) != 600 {
		t.Fatalf("full reads=%d windows=%d items=%d", repo.paths, repo.windows, len(actor.state.view.Items))
	}
	before, err := json.Marshal(actor.state.view)
	if err != nil {
		t.Fatal(err)
	}
	repo.windowErr = errors.New("injected storage read failure")
	if err := actor.apply(t.Context(), func() error { return service.advanceCanonical(t.Context(), actor) }); !errors.Is(err, repo.windowErr) {
		t.Fatalf("failure=%v", err)
	}
	if actor.canonical != nil || actor.state.contextErr == nil {
		t.Fatal("failed projection remained eligible for incremental reuse")
	}
	after, err := json.Marshal(actor.state.view)
	if err != nil || string(before) != string(after) {
		t.Fatalf("failed interval changed accepted presentation: %v\nbefore=%s\nafter=%s", err, before, after)
	}
	repo.windowErr = nil
	recovered, err := full.View(t.Context(), created.ThreadID)
	if err != nil || len(recovered.Items) != 600 || actor.canonical == nil || actor.state.contextErr != nil {
		t.Fatalf("recovery=%v items=%d", err, len(recovered.Items))
	}
	if repo.paths != 1 {
		t.Fatalf("explicit recovery full reads=%d", repo.paths)
	}
	repo.paths, repo.windows = 0, 0
	if _, err := repo.Append(t.Context(), sessiontree.Entry{ThreadID: created.ThreadID.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: "after recovery"}}, sessiontree.AppendOptions{ID: "after"}); err != nil {
		t.Fatal(err)
	}
	if err := actor.apply(t.Context(), func() error { return service.advanceCanonical(t.Context(), actor) }); err != nil {
		t.Fatal(err)
	}
	if repo.paths != 0 || repo.windows != 1 || len(actor.state.view.Items) != 601 {
		t.Fatalf("resume paths=%d windows=%d items=%d", repo.paths, repo.windows, len(actor.state.view.Items))
	}
}

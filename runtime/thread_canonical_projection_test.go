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
			for _, turn := range turns {
				if got, want := projection.terminalCounts[turn.TurnID] > 0, hasTerminalPresentation(items, turn.TurnID); got != want {
					t.Fatalf("chunk=%d prefix=%d terminal presentation differs", size, end)
				}
			}
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
	failWindow     int
}

func (repo *canonicalReadCountingRepo) Path(ctx context.Context, threadID, leaf string) ([]sessiontree.Entry, error) {
	repo.paths++
	return repo.Repo.Path(ctx, threadID, leaf)
}
func (repo *canonicalReadCountingRepo) PathEntriesWindow(ctx context.Context, options sessiontree.PathWindowOptions) (sessiontree.PathEntriesWindow, error) {
	repo.windows++
	if repo.windowErr != nil && (repo.failWindow == 0 || repo.windows == repo.failWindow) {
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

func TestCanonicalCommittedEventAndInteractionUseOnlyIncrementalReads(t *testing.T) {
	host, full := testThreadService(t, newBlockingThreadGateway())
	created, err := full.Create(t.Context(), CreateThreadInput{RequestKey: "canonical-event"})
	if err != nil {
		t.Fatal(err)
	}
	service := full.(*threadRuntimeService)
	repo := &canonicalReadCountingRepo{Repo: host.store.repo, reader: host.store.repo.(sessiontree.PathEntriesWindowReader)}
	host.store.repo = repo
	actor := service.runtime(created.ThreadID)
	entry, err := repo.Append(t.Context(), sessiontree.Entry{ThreadID: created.ThreadID.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: "committed event"}}, sessiontree.AppendOptions{ID: "entry"})
	if err != nil {
		t.Fatal(err)
	}
	sink := threadRuntimeEventSink{service: service}
	sink.EmitEvent(Event{ThreadID: created.ThreadID, TurnID: "turn", RunID: "run", committed: &agentharness.ThreadDetailEvent{ID: entry.ID}})
	if repo.paths != 0 || repo.windows != 1 || len(actor.state.view.Items) != 1 || actor.state.view.Items[0].Text != "committed event" {
		t.Fatalf("committed event paths=%d windows=%d items=%+v", repo.paths, repo.windows, actor.state.view.Items)
	}
	interaction := ThreadInteraction{ID: "approval:event", TurnID: "turn", RunID: "run", Kind: ThreadInteractionApproval, ToolCallID: "tool"}
	waiter, err := service.requestInteraction(t.Context(), actor, created.ThreadID, interaction)
	if err != nil {
		t.Fatal(err)
	}
	if waiter == nil || repo.paths != 0 || repo.windows != 2 || len(actor.state.view.Interactions) != 1 || actor.state.view.Interactions[0].ID != interaction.ID {
		t.Fatalf("interaction paths=%d windows=%d interactions=%+v", repo.paths, repo.windows, actor.state.view.Interactions)
	}
}

func (repo *canonicalReadCountingRepo) AppendRuntimeFacts(ctx context.Context, threadID string, entries []sessiontree.Entry) ([]sessiontree.Entry, error) {
	return repo.Repo.(sessiontree.RuntimeJournalRepo).AppendRuntimeFacts(ctx, threadID, entries)
}

func TestCanonicalInstallationRejectsLateErrorsBeforeChangingAcceptedView(t *testing.T) {
	entries := []sessiontree.Entry{
		{ID: "start", TurnID: "turn", RunID: "run", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted},
		{ID: "user", ParentID: "start", TurnID: "turn", RunID: "run", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: "original"}},
	}
	projection := newThreadCanonicalProjection()
	if err := projection.append(entries); err != nil {
		t.Fatal(err)
	}
	actor := &threadRuntimeState{state: threadRuntimeData{view: ThreadView{ThreadID: "thread"}}}
	if err := actor.installCanonicalChanges(projection); err != nil {
		t.Fatal(err)
	}
	before, _ := json.Marshal(actor.state.view)
	projection.resetChanges()
	if err := projection.append([]sessiontree.Entry{{ID: "new", ParentID: "user", TurnID: "turn", RunID: "run", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: "must remain uninstalled"}}}); err != nil {
		t.Fatal(err)
	}
	projection.turns.turns["turn"].finalErr = errors.New("late turn validation failure")
	if err := actor.installCanonicalChanges(projection); err == nil {
		t.Fatal("late validation failure accepted")
	}
	after, _ := json.Marshal(actor.state.view)
	if string(before) != string(after) {
		t.Fatalf("late error changed accepted view\nbefore=%s\nafter=%s", before, after)
	}
}

func TestCanonicalSecondPageFailureRetainsAcceptedSnapshot(t *testing.T) {
	host, full := testThreadService(t, newBlockingThreadGateway())
	created, err := full.Create(t.Context(), CreateThreadInput{RequestKey: "second-page-failure"})
	if err != nil {
		t.Fatal(err)
	}
	service := full.(*threadRuntimeService)
	for i := range 3 {
		if _, err := host.store.repo.Append(t.Context(), sessiontree.Entry{ThreadID: created.ThreadID.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: fmt.Sprint(i)}}, sessiontree.AppendOptions{ID: fmt.Sprint(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := service.refreshCanonical(created.ThreadID, ""); err != nil {
		t.Fatal(err)
	}
	actor := service.runtime(created.ThreadID)
	before, _ := json.Marshal(actor.state.view)
	for i := 3; i < 600; i++ {
		if _, err := host.store.repo.Append(t.Context(), sessiontree.Entry{ThreadID: created.ThreadID.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: fmt.Sprint(i)}}, sessiontree.AppendOptions{ID: fmt.Sprint(i)}); err != nil {
			t.Fatal(err)
		}
	}
	repo := &canonicalReadCountingRepo{Repo: host.store.repo, reader: host.store.repo.(sessiontree.PathEntriesWindowReader), windowErr: errors.New("second page unavailable"), failWindow: 2}
	host.store.repo = repo
	if err := actor.apply(t.Context(), func() error { return service.advanceCanonical(t.Context(), actor) }); !errors.Is(err, repo.windowErr) {
		t.Fatalf("failure=%v", err)
	}
	after, _ := json.Marshal(actor.state.view)
	if string(before) != string(after) || actor.canonical != nil {
		t.Fatalf("failed partial interval changed accepted snapshot\nbefore=%s\nafter=%s", before, after)
	}
	repo.windowErr = nil
	view, err := full.View(t.Context(), created.ThreadID)
	if err != nil || len(view.Items) != 600 {
		t.Fatalf("recovery items=%d err=%v", len(view.Items), err)
	}
}

func TestCanonicalSnapshotDoesNotShareMutableReducerObjects(t *testing.T) {
	_, full := testThreadService(t, newBlockingThreadGateway())
	created, err := full.Create(t.Context(), CreateThreadInput{RequestKey: "projection-isolation"})
	if err != nil {
		t.Fatal(err)
	}
	service := full.(*threadRuntimeService)
	projection := newThreadCanonicalProjection()
	interaction := ThreadInteraction{ID: "approval:isolation", TurnID: "turn", RunID: "run", Kind: ThreadInteractionApproval, ToolCallID: "tool"}
	payload, _ := json.Marshal(interaction)
	entries := []sessiontree.Entry{
		{ID: "tool", ThreadID: created.ThreadID.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryToolCall, Message: session.Message{Role: session.Assistant, ToolCallID: "tool", ToolName: "read", ToolArgs: "{}"}},
		{ID: "interaction-requested:" + interaction.ID, ParentID: "tool", ThreadID: created.ThreadID.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryInteractionAsked, Payload: payload},
	}
	if err := projection.append(entries); err != nil {
		t.Fatal(err)
	}
	canonical, err := projection.snapshot(t.Context(), service, created.ThreadID, nil)
	if err != nil {
		t.Fatal(err)
	}
	before, _ := json.Marshal(canonical.ThreadView)
	projection.items.items[0].Activity.ToolName = "mutated disposable projection"
	projection.items.items[0].Interaction.Resolved = true
	projection.items.interactions[0].Resolved = true
	after, _ := json.Marshal(canonical.ThreadView)
	if string(before) != string(after) {
		t.Fatalf("accepted snapshot shared reducer ownership\nbefore=%s\nafter=%s", before, after)
	}
}

// Accepting a new turn must advance the installed reducer rather than reread
// unrelated history. Provider preparation may read context after admission.
func TestThreadAdmissionAdvancesCanonicalIntervalWithoutFullPath(t *testing.T) {
	gateway := newBlockingThreadGateway()
	host, full := testThreadService(t, gateway)
	created, err := full.Create(t.Context(), CreateThreadInput{RequestKey: "interval-create"})
	if err != nil {
		t.Fatal(err)
	}
	service := full.(*threadRuntimeService)
	actor := service.runtime(created.ThreadID)
	repo := &canonicalReadCountingRepo{Repo: host.store.repo, reader: host.store.repo.(sessiontree.PathEntriesWindowReader)}
	// Execute only admission under the actor; no provider goroutine or context
	// preparation can contaminate this display-path read count.
	err = actor.apply(t.Context(), func() error {
		accepted, err := service.acceptCanonicalTurn(t.Context(), created.ThreadID, turnExecutionRequest{LogicalRequestID: "interval-send", TurnID: "interval-turn", RunID: "interval-run", Input: UserInput{Text: "new"}, InputFingerprint: "fingerprint"})
		if err != nil {
			return err
		}
		if accepted.ThreadID != created.ThreadID {
			t.Fatal("admission identity changed")
		}
		host.store.repo = repo
		defer func() { host.store.repo = repo.Repo }()
		return service.advanceCanonical(t.Context(), actor)
	})
	if err != nil {
		t.Fatal(err)
	}
	if repo.paths != 0 || repo.windows != 1 || len(actor.state.view.Items) != 1 || actor.state.view.Items[0].Text != "new" {
		t.Fatalf("paths=%d windows=%d items=%d", repo.paths, repo.windows, len(actor.state.view.Items))
	}
}

func TestCanonicalActorBorrowsMembershipButPublicTurnSnapshotIsDetached(t *testing.T) {
	projection := newThreadCanonicalProjection()
	entries := []sessiontree.Entry{{ID: "start", TurnID: "turn", RunID: "run", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted, Metadata: map[string]string{sessiontree.ResponseSegmentsMetadataKey: "1"}}}
	for i := range 1000 {
		id := fmt.Sprint(i)
		entries = append(entries, sessiontree.Entry{ID: "text-" + id, TurnID: "turn", RunID: "run", Type: sessiontree.EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "text", ContentSourceID: "answer-" + id}}, sessiontree.Entry{ID: "call-" + id, TurnID: "turn", RunID: "run", Type: sessiontree.EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: id}})
	}
	for i := 1; i < len(entries); i++ {
		entries[i].ParentID = entries[i-1].ID
	}
	if err := projection.append(entries); err != nil {
		t.Fatal(err)
	}
	actor := &threadRuntimeState{state: threadRuntimeData{view: ThreadView{ThreadID: "thread", Activity: ThreadActivityActive}}}
	if err := actor.installCanonicalChanges(projection); err != nil {
		t.Fatal(err)
	}
	membership := actor.state.view.Turns[0].IntermediateItemIDs
	if len(membership) != 1000 {
		t.Fatalf("membership=%d", len(membership))
	}
	public, _, err := projection.turns.turnSnapshot("turn")
	if err != nil {
		t.Fatal(err)
	}
	public.IntermediateItemIDs[0] = "caller-change"
	if membership[0] == "caller-change" {
		t.Fatal("public snapshot aliases reducer")
	}
	projection.resetChanges()
	if err := projection.append([]sessiontree.Entry{{ID: "progress", ParentID: projection.leafID, TurnID: "turn", RunID: "run", Type: sessiontree.EntryCustom}}); err != nil {
		t.Fatal(err)
	}
	if err := actor.installCanonicalChanges(projection); err != nil {
		t.Fatal(err)
	}
	if &membership[0] != &actor.state.view.Turns[0].IntermediateItemIDs[0] {
		t.Fatal("unchanged giant membership copied on actor update")
	}
}

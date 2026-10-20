package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/observation"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/tools"
)

type terminalReadCountingRepo struct {
	*canonicalReadCountingRepo
}

func (repo *terminalReadCountingRepo) ReadCanonicalBody(ctx context.Context, threadID, entryID, field string, offset, limit int) (sessiontree.CanonicalBodyChunk, error) {
	return repo.Repo.(sessiontree.CanonicalBodyReader).ReadCanonicalBody(ctx, threadID, entryID, field, offset, limit)
}
func (repo *terminalReadCountingRepo) SourceBodyPage(ctx context.Context, options sessiontree.SourceBodyPageOptions) (sessiontree.SourceBodyPage, error) {
	return repo.Repo.(sessiontree.SourceBodyPageReader).SourceBodyPage(ctx, options)
}
func (repo *terminalReadCountingRepo) CanonicalBodyMetadata(ctx context.Context, threadID, entryID, field string) (sessiontree.CanonicalBodyMetadata, error) {
	return repo.Repo.(sessiontree.CanonicalBodyMetadataReader).CanonicalBodyMetadata(ctx, threadID, entryID, field)
}

func TestTerminalCanonicalIntervalMatchesFullSQLiteProjection(t *testing.T) {
	host, err := Open(t.Context(), Options{Storage: storage.SQLite(t.TempDir() + "/terminal.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	agent, err := testAgent(newBlockingThreadGateway())
	if err != nil {
		t.Fatal(err)
	}
	public, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
	if err != nil {
		t.Fatal(err)
	}
	service := public.(*threadRuntimeService)
	id := identity.ThreadID("terminal-interval")
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
		t.Fatal(err)
	}
	appendFact := func(entry sessiontree.Entry) {
		t.Helper()
		entry.ThreadID, entry.TurnID, entry.RunID = id.String(), "turn", "run"
		if _, err := host.store.repo.Append(t.Context(), entry, sessiontree.AppendOptions{ID: entry.ID}); err != nil {
			t.Fatal(err)
		}
	}
	appendFact(sessiontree.Entry{ID: "start", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted})
	for i := range 1096 {
		appendFact(sessiontree.Entry{ID: fmt.Sprint(i), Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: "historical fact"}})
	}
	call := &tools.ActivityPresentation{Label: "Read", Renderer: tools.ActivityRendererTerminal, Payload: tools.TerminalActivityPayload{Command: "read file"}}
	result := &tools.ActivityPresentation{Label: "Read", Renderer: tools.ActivityRendererTerminal, Payload: tools.TerminalActivityPayload{Command: "read file", Output: "canonical result"}}
	appendFact(sessiontree.Entry{ID: "call", Type: sessiontree.EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "read", ToolCallID: "tool", Activity: call}})
	appendFact(sessiontree.Entry{ID: "result", Type: sessiontree.EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "read", ToolCallID: "tool", Activity: result}})
	text := strings.Repeat("answer 中🌸\r\n", 10000)
	appendFact(sessiontree.Entry{ID: "answer-entry", Type: sessiontree.EntryAssistantMessage, Message: session.Message{Role: session.Assistant, ContentSourceID: "answer", Content: text}})
	actor := service.runtime(id)
	if err := actor.apply(t.Context(), func() error { actor.state.hydrationStarted = true; return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := public.View(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	repo := &terminalReadCountingRepo{&canonicalReadCountingRepo{Repo: host.store.repo, reader: host.store.repo.(sessiontree.PathEntriesWindowReader)}}
	host.store.repo = repo
	if err := actor.apply(t.Context(), func() error {
		actor.state.turnID, actor.state.runID = "turn", "run"
		actor.state.view.Activity = ThreadActivityActive
		actor.applyActivityItems(id, "turn", "run", []observation.ActivityItem{{ToolID: "tool", ToolName: "read", Kind: observation.ActivityKindTool, Status: observation.ActivityStatusSuccess, Presentation: &tools.ActivityPresentation{Label: "stale observation", Renderer: tools.ActivityRendererTerminal, Payload: tools.TerminalActivityPayload{Output: "must disappear"}}}}, actor.state.view.Items[0].CreatedAt)
		actor.appendLiveTextSegment(ThreadItemAssistant, "orphan draft", actor.state.view.Items[0].CreatedAt, "orphan")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	appendFact(sessiontree.Entry{ID: "terminal", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnCompleted})
	stale, err := service.readTerminalCanonical(t.Context(), actor)
	if err != nil {
		t.Fatal(err)
	}
	if err := actor.apply(t.Context(), func() error { return service.advanceCanonical(t.Context(), actor) }); err != nil {
		t.Fatal(err)
	}
	if stale.current(actor) {
		t.Fatal("advanced reducer accepted an older prepared interval")
	}
	repo.paths, repo.windows = 0, 0
	service.finishSend(actor, "turn", "run", TurnResult{Status: TurnStatusCompleted}, nil)
	if repo.paths != 0 || repo.windows != 1 {
		t.Fatalf("terminal reads paths=%d windows=%d", repo.paths, repo.windows)
	}
	actual, err := public.View(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if actual.Activity != ThreadActivityIdle || actual.LastOutcome == nil || *actual.LastOutcome != TurnOutcomeCompleted || actual.Failure != nil {
		t.Fatalf("terminal state=%+v", actual)
	}
	canonical, err := service.loadCanonicalThreadView(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	compare := func(label string, left, right any) {
		t.Helper()
		a, _ := json.Marshal(left)
		b, _ := json.Marshal(right)
		if string(a) != string(b) {
			t.Fatalf("%s differs from complete canonical projection", label)
		}
	}
	compare("items", actual.Items, canonical.Items)
	compare("turns", actual.Turns, canonical.Turns)
	compare("context", actual.Context, canonical.Context)
	compare("interactions", actual.Interactions, canonical.Interactions)
	fullSummary := threadSummaryFromView(sessiontree.ThreadMeta{}, actual)
	if actor.threadSummary(sessiontree.ThreadMeta{}).LastItemPreview != fullSummary.LastItemPreview {
		t.Fatal("terminal summary differs after body eviction")
	}
	if len(actor.observedItemIDs) != 0 || len(actor.state.attemptDraftIDs) != 0 {
		t.Fatal("terminal retained live overlays")
	}
	if !reflect.DeepEqual(actor.state.view.LastOutcome, canonical.LastOutcome) {
		t.Fatal("terminal outcome authority differs")
	}
	for _, item := range actor.state.view.Items {
		if item.canonicalText && item.Text != "" {
			t.Fatal("terminal retained canonical text")
		}
	}
}

// The final journal commit has already advanced the actor at this boundary.
// Initialization and explicit complete projections stay outside the timer.
func BenchmarkTerminalCanonicalSQLiteFixedBoundary(b *testing.B) {
	for _, count := range []int{1096, 10960, 109600} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			host, err := Open(b.Context(), Options{Storage: storage.SQLite(b.TempDir() + "/history.sqlite")})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = host.Shutdown(context.Background()) })
			agent, err := testAgent(newBlockingThreadGateway())
			if err != nil {
				b.Fatal(err)
			}
			public, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
			if err != nil {
				b.Fatal(err)
			}
			service := public.(*threadRuntimeService)
			const id = "terminal-scaling"
			if _, err := host.store.repo.CreateThread(b.Context(), sessiontree.ThreadMeta{ID: id}); err != nil {
				b.Fatal(err)
			}
			writer := host.store.repo.(sessiontree.RuntimeJournalRepo)
			batch := make([]sessiontree.Entry, 0, 256)
			flush := func() {
				if len(batch) == 0 {
					return
				}
				if _, err := writer.AppendRuntimeFacts(b.Context(), id, batch); err != nil {
					b.Fatal(err)
				}
				batch = batch[:0]
			}
			for i := range count {
				batch = append(batch, sessiontree.Entry{ID: fmt.Sprint(i), ThreadID: id, TurnID: "turn", RunID: "run", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: "synthetic historical fact"}})
				if len(batch) == cap(batch) {
					flush()
				}
			}
			flush()
			if _, err := host.store.repo.Append(b.Context(), sessiontree.Entry{ThreadID: id, TurnID: "turn", RunID: "run", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnCompleted}, sessiontree.AppendOptions{ID: "terminal"}); err != nil {
				b.Fatal(err)
			}
			canonical, err := service.loadCanonicalThreadView(b.Context(), id)
			if err != nil {
				b.Fatal(err)
			}
			actor := service.runtime(id)
			if err := actor.apply(b.Context(), func() error { return actor.installCanonicalView(canonical) }); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				read, err := service.readTerminalCanonical(b.Context(), actor)
				if err != nil {
					b.Fatal(err)
				}
				if err := actor.apply(b.Context(), func() error {
					if !read.current(actor) {
						return ErrStaleAuthority
					}
					_, err := service.installTerminalCanonical(b.Context(), actor, read)
					return err
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

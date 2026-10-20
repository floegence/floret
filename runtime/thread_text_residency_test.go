package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/citation"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/threadquery"
)

func TestThreadCanonicalTextResidencyPreservesCompleteAndWindowResults(t *testing.T) {
	path := t.TempDir() + "/resident.sqlite"
	open := func() (*Host, *threadRuntimeService) {
		host, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
		agent, err := testAgent(newBlockingThreadGateway())
		if err != nil {
			t.Fatal(err)
		}
		service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
		if err != nil {
			t.Fatal(err)
		}
		return host, service.(*threadRuntimeService)
	}
	host, service := open()
	id := identity.ThreadID("thread-residency")
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
		t.Fatal(err)
	}
	user := strings.Repeat("request 中文🌸\r\n", 10000)
	reasoning := strings.Repeat("reason 🌸", 10000)
	if _, err := host.store.repo.Append(t.Context(), sessiontree.Entry{ThreadID: id.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: user}}, sessiontree.AppendOptions{ID: "user"}); err != nil {
		t.Fatal(err)
	}
	var expected strings.Builder
	for i := range 160 {
		text := fmt.Sprintf("%d:", i) + strings.Repeat("中é🌸\r\n", 100)
		expected.WriteString(text)
		lo, hi := 0, utf8.RuneCountInString(text)
		entry := sessiontree.Entry{ThreadID: id.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: text, ContentSourceID: "answer", Reasoning: reasoning, ReasoningSourceID: "reason", Sources: []citation.Source{{Title: fmt.Sprint(i), URL: "https://example.com", StartIndex: &lo, EndIndex: &hi}}}}
		if _, err := host.store.repo.Append(t.Context(), entry, sessiontree.AppendOptions{ID: fmt.Sprint(i)}); err != nil {
			t.Fatal(err)
		}
	}
	scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{id}}
	for restart := range 2 {
		view, err := service.View(t.Context(), id)
		if err != nil {
			t.Fatal(err)
		}
		if len(view.Items) != 3 || view.Items[0].Text != user || view.Items[1].Text != reasoning || sha256.Sum256([]byte(view.Items[2].Text)) != sha256.Sum256([]byte(expected.String())) {
			t.Fatal("complete adapter lost canonical bytes")
		}
		actor := service.runtime(id)
		for _, item := range actor.state.view.Items {
			if item.Text != "" || !item.textOmitted || item.textBytes == 0 {
				t.Fatal("actor retained canonical text")
			}
		}
		for _, item := range actor.canonical.items.items {
			if item.Text != "" {
				t.Fatal("reducer retained canonical text")
			}
		}
		for _, buffer := range actor.canonical.items.contentBuffers {
			if buffer.text.Len() != 0 || len(buffer.incomplete) >= utf8.UTFMax {
				t.Fatal("reducer retained accumulated text")
			}
		}
		sub, err := service.SubscribeWindowUpdates(t.Context(), scope)
		if err != nil {
			t.Fatal(err)
		}
		if err := sub.SetWindow(t.Context(), scope, ThreadWindowInput{ThreadID: id}); err != nil {
			t.Fatal(err)
		}
		baseline := nextUpdate(t, sub)
		if baseline.Window == nil || len(baseline.Window.Value.Items) != 3 {
			t.Fatal("window missing source previews")
		}
		for i, item := range baseline.Window.Value.Items {
			if !utf8.ValidString(item.Text) || len(item.Text) > threadDisplayTextBytes || item.Body == nil || item.Body.Bytes != len(view.Items[i].Text) || !strings.HasPrefix(view.Items[i].Text, item.Text) {
				t.Fatal("window preview changed source bytes or extent")
			}
		}
		sub.Close()
		legacy, err := service.Subscribe(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		updates, err := service.SubscribeUpdates(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		before, _ := json.Marshal(view.Items[2].Sources)
		if restart == 0 {
			text := "appended 🌸"
			expected.WriteString(text)
			if _, err := host.store.repo.Append(t.Context(), sessiontree.Entry{ThreadID: id.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: text, ContentSourceID: "answer"}}, sessiontree.AppendOptions{ID: "tail"}); err != nil {
				t.Fatal(err)
			}
			if err := actor.apply(t.Context(), func() error { return service.advanceCanonical(t.Context(), actor) }); err != nil {
				t.Fatal(err)
			}
			updated, err := service.View(t.Context(), id)
			if err != nil || updated.Items[2].Text != expected.String() {
				t.Fatalf("append lost source: %v", err)
			}
			after, _ := json.Marshal(updated.Items[2].Sources)
			if string(before) != string(after) {
				t.Fatal("append changed citation offsets")
			}
			observed, err := legacy.Next(t.Context())
			if err != nil || observed.Items[2].Text != expected.String() {
				t.Fatalf("legacy view lost exact text: %v", err)
			}
			update := nextUpdate(t, updates)
			found := false
			for _, change := range update.Items {
				if change.ItemID == "answer" && change.Item != nil {
					found = true
					if change.Item.Text != expected.String() {
						t.Fatal("legacy native update lost source text")
					}
				}
			}
			if !found {
				t.Fatal("canonical append did not publish replacement")
			}
			if err := host.Shutdown(t.Context()); err != nil {
				t.Fatal(err)
			}
			host, service = open()
		}
		legacy.Close()
		updates.Close()
	}
}

func TestCanonicalTextSummaryMatchesCompleteAtEveryAppend(t *testing.T) {
	cases := [][]string{
		{" ", "\t", "hello ", "world", " "},
		{strings.Repeat("中", 158) + "  ", " ", "🌸", "tail"},
		{strings.Repeat("a", 159), strings.Repeat(" ", 1000), "x"},
		{"\u3000\t", strings.Repeat("🌸", 170), strings.Repeat("x", 10000)},
	}
	for _, parts := range cases {
		var prefix, complete string
		hasTail := false
		for _, part := range parts {
			complete += part
			prefix, hasTail = canonicalTextSummary(prefix, part, hasTail)
			view := ThreadView{Items: []ThreadItem{{Text: complete}}}
			want := threadSummaryFromView(sessiontree.ThreadMeta{}, view)
			view.Items[0] = ThreadItem{textOmitted: true, textSummary: prefix, textSummaryTail: hasTail}
			got := threadSummaryFromView(sessiontree.ThreadMeta{}, view)
			if got.LastItemPreview != want.LastItemPreview || utf8.RuneCountInString(prefix) > 160 {
				t.Fatalf("summary differs: got=%q want=%q", got.LastItemPreview, want.LastItemPreview)
			}
		}
	}
}

func TestLegacyPublicationFreezesTextBeforeLaterAppendAndDeletion(t *testing.T) {
	host, err := Open(t.Context(), Options{Storage: storage.SQLite(t.TempDir() + "/snapshot.sqlite")})
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
	id := identity.ThreadID("thread-frozen")
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
		t.Fatal(err)
	}
	appendText := func(entryID, text string) {
		t.Helper()
		if _, err := host.store.repo.Append(t.Context(), sessiontree.Entry{ThreadID: id.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryAssistantMessage, Message: session.Message{Role: session.Assistant, ContentSourceID: "answer", Content: text}}, sessiontree.AppendOptions{ID: entryID}); err != nil {
			t.Fatal(err)
		}
	}
	appendText("first", "first 🌸")
	if _, err := service.View(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	first, err := service.Subscribe(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := service.Subscribe(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	actor := service.runtime(id)
	if err := actor.apply(t.Context(), func() error { actor.state.view.ViewVersion++; return nil }); err != nil {
		t.Fatal(err)
	}
	frozen := cloneThreadRuntimeView(actor.state.view)
	appendText("tail", " later")
	if err := actor.apply(t.Context(), func() error { return service.advanceCanonical(t.Context(), actor) }); err != nil {
		t.Fatal(err)
	}
	complete, err := service.completeViewResult(t.Context(), frozen, nil)
	if err != nil || complete.Items[0].Text != "first 🌸" {
		t.Fatalf("frozen extent changed after append: %v", err)
	}
	if err := service.Delete(t.Context(), DeleteThreadInput{ThreadID: id, RequestKey: "delete-frozen"}); err != nil {
		t.Fatal(err)
	}
	left, err := first.Next(t.Context())
	if err != nil || left.Items[0].Text != "first 🌸" {
		t.Fatalf("publication changed after deletion: %v", err)
	}
	left.Items[0].Text = "consumer mutation"
	right, err := second.Next(t.Context())
	if err != nil || right.Items[0].Text != "first 🌸" {
		t.Fatalf("consumers share mutable snapshots: %v", err)
	}
	left, err = first.Next(t.Context())
	if err != nil || left.Items[0].Text != "first 🌸 later" {
		t.Fatalf("second publication lost appended bytes: %v", err)
	}
}

func TestTerminalCanonicalRefreshReleasesAcceptedText(t *testing.T) {
	host, public := testThreadService(t, newBlockingThreadGateway())
	service := public.(*threadRuntimeService)
	id := identity.ThreadID("thread-terminal-residency")
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
		t.Fatal(err)
	}
	text := strings.Repeat("complete 🌸", 10000)
	for _, entry := range []sessiontree.Entry{
		{ID: "start", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted},
		{ID: "answer-entry", Type: sessiontree.EntryAssistantMessage, Message: session.Message{Role: session.Assistant, ContentSourceID: "answer", Content: text}},
		{ID: "terminal", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnCompleted},
	} {
		entry.ThreadID, entry.TurnID, entry.RunID = id.String(), "turn", "run"
		if _, err := host.store.repo.Append(t.Context(), entry, sessiontree.AppendOptions{ID: entry.ID}); err != nil {
			t.Fatal(err)
		}
	}
	actor := service.runtime(id)
	if err := actor.apply(t.Context(), func() error {
		actor.state.turnID, actor.state.runID = "turn", "run"
		actor.state.view.ThreadID, actor.state.view.TurnID, actor.state.view.RunID = id, "turn", "run"
		actor.state.view.Activity = ThreadActivityActive
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	service.finishSend(actor, "turn", "run", TurnResult{Status: TurnStatusCompleted}, nil)
	if len(actor.state.view.Items) != 1 || actor.state.view.Items[0].Text != "" || !actor.state.view.Items[0].textOmitted || actor.canonical.items.items[0].Text != "" {
		t.Fatal("terminal refresh retained complete text")
	}
	view, err := service.View(t.Context(), id)
	if err != nil || view.Items[0].Text != text || view.Failure != nil || view.Activity != ThreadActivityIdle {
		t.Fatalf("terminal adapter lost canonical text or completion: %v", err)
	}
}

package runtime

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/floegence/floret/v7/internal/citation"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
)

func TestSourceWindowTextFactsPreserveBodiesCitationsAndTurnBoundaries(t *testing.T) {
	host, public := testThreadService(t, newBlockingThreadGateway())
	service := public.(*threadRuntimeService)
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: "text-facts"}); err != nil {
		t.Fatal(err)
	}
	var entries []sessiontree.Entry
	appendEntry := func(entry sessiontree.Entry) {
		entry.ThreadID, entry.TurnID, entry.RunID = "text-facts", "turn", "run"
		saved, err := host.store.repo.Append(t.Context(), entry, sessiontree.AppendOptions{ID: entry.ID})
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, saved)
	}
	appendEntry(sessiontree.Entry{ID: "start", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted, Metadata: map[string]string{sessiontree.ResponseSegmentsMetadataKey: "1"}})
	appendEntry(sessiontree.Entry{ID: "user", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: strings.Repeat("大🌸 ", 10000)}})
	for i, text := range []string{strings.Repeat(" \t", 6000), "中文🌸", strings.Repeat("visible ", 10000)} {
		lo, hi := 0, utf8.RuneCountInString(text)
		appendEntry(sessiontree.Entry{ID: []string{"part-a", "part-b", "part-c"}[i], Type: sessiontree.EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: text, ContentSourceID: "answer", Reasoning: "think🌸", ReasoningSourceID: "reason", Sources: []citation.Source{{Title: "reference", URL: "https://example.com", StartIndex: &lo, EndIndex: &hi}}}})
	}
	appendEntry(sessiontree.Entry{ID: "end", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnCompleted, Metadata: map[string]string{sessiontree.FinalResponseEntriesMetadataKey: "[\"part-c\"]"}})
	want, _, err := threadRuntimeItemsFromEntries(entries)
	if err != nil {
		t.Fatal(err)
	}
	turns, err := threadRuntimeTurnsFromEntries(entries, want)
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := service.loadSourceWindow(t.Context(), ThreadWindowInput{ThreadID: "text-facts", Limit: 40, ActivityFields: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Items) != len(want) {
		t.Fatalf("membership=%d want=%d", len(got.Items), len(want))
	}
	for i, item := range got.Items {
		if !item.textOmitted || item.Text != "" || item.textBytes != len(want[i].Text) || !reflect.DeepEqual(item.Sources, want[i].Sources) {
			t.Fatalf("item %d differs: %+v", i, item)
		}
		if summaryItemText(item) != truncateThreadSummaryPreview(strings.TrimSpace(want[i].Text), 160) {
			t.Fatalf("summary %d differs: %q", i, summaryItemText(item))
		}
		body, err := service.readItemText(t.Context(), "text-facts", item, -1)
		if err != nil || body != want[i].Text {
			t.Fatalf("body %d differs: %v", i, err)
		}
	}
	a, _ := json.Marshal(got.Turns)
	b, _ := json.Marshal(turns)
	if string(a) != string(b) {
		t.Fatalf("turns differ: %s / %s", a, b)
	}
}

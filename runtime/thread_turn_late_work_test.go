package runtime

import (
	"errors"
	"testing"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
)

func TestLateHostWorkPreservesFinalOwner(t *testing.T) {
	for _, source := range []string{"run-final", "run-earlier"} {
		t.Run(source, func(t *testing.T) {
			p := threadTurnProjection{}
			var journal []sessiontree.Entry
			lookup := func(id string) (ThreadItem, bool) {
				return ThreadItem{ID: "answer", TurnID: "turn", RunID: "run-final", Kind: ThreadItemAssistant, Ordinal: 1, Text: "answer"}, id == "answer"
			}
			observe := func(e sessiontree.Entry) {
				t.Helper()
				journal = append(journal, e)
				if err := p.observe(e, lookup); err != nil {
					t.Fatal(err)
				}
			}
			check := func(when string) {
				t.Helper()
				v, found, err := p.turnSnapshot("turn")
				if err != nil || !found || v.RunID != "run-final" || len(v.FinalItemIDs) != 1 {
					t.Fatalf("%s: projected run=%s, error=%v", when, p.turns["turn"].turn.RunID, err)
				}
				if id, err := sessiontree.CurrentRunID(journal, "turn"); err != nil || id != "run-final" {
					t.Fatalf("%s: journal execution %q: %v", when, id, err)
				}
			}
			observe(sessiontree.Entry{Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted, TurnID: "turn", RunID: "run-earlier", Metadata: map[string]string{sessiontree.ResponseSegmentsMetadataKey: "1"}})
			observe(sessiontree.Entry{Type: sessiontree.EntryAssistantMessage, ID: "answer", TurnID: "turn", RunID: "run-final", Message: session.Message{Role: session.Assistant, Content: "answer"}})
			observe(sessiontree.Entry{Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnCompleted, TurnID: "turn", RunID: "run-final", Metadata: map[string]string{sessiontree.FinalResponseEntriesMetadataKey: `["answer"]`}})
			check("before late event")
			observe(sessiontree.Entry{Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted, TurnID: "next", RunID: "run-next"})
			observe(sessiontree.Entry{Type: sessiontree.EntryHostWork, ID: "late", TurnID: "turn", RunID: source})
			check("after late event")
			turn, run, activity, _, _ := p.lifecycle.snapshot()
			if turn != "next" || run != "run-next" || activity != ThreadActivityActive {
				t.Fatalf("late receipt changed next turn: %s %s %s", turn, run, activity)
			}
			item, _ := lookup("answer")
			item.sourceEntryIDs = []string{"answer"}
			turns, err := threadRuntimeTurnsFromEntries(journal, []ThreadItem{item})
			if err != nil || len(turns) != 2 || turns[0].RunID != "run-final" || len(turns[0].FinalItemIDs) != 1 {
				t.Fatalf("rebuilt turns=%+v: %v", turns, err)
			}
			item.RunID = "run-earlier"
			if _, err := threadRuntimeTurnsFromEntries(journal, []ThreadItem{item}); !errors.Is(err, ErrAuthorityCorrupt) || !errors.Is(err, sessiontree.ErrFinalResponseOwnership) {
				t.Fatalf("accepted wrong-run reply or lost failure identity: %v", err)
			}
		})
	}
}

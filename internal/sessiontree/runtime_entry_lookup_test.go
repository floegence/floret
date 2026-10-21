package sessiontree

import (
	"errors"
	"fmt"
	"testing"

	"github.com/floegence/floret/v7/internal/session"
)

func TestRuntimeAdmissionRejectsCorruptExactLocators(t *testing.T) {
	for _, operation := range []string{"accept", "read"} {
		for _, locator := range []string{"started", "user"} {
			for _, corruption := range []string{"negative", "past-end", "other-entry", "depth", "thread"} {
				t.Run(operation+"/"+locator+"/"+corruption, func(t *testing.T) {
					repo := NewMemoryRepo()
					if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
						t.Fatal(err)
					}
					req := AcceptTurnRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", LogicalRequestID: "send", RequestFingerprint: "send-fp", InputRequestFingerprint: "input-fp", Input: session.Message{Role: session.User, Content: "hello"}}
					accepted, err := repo.AcceptTurn(t.Context(), req)
					if err != nil {
						t.Fatal(err)
					}
					id := accepted.TurnStarted.ID
					if locator == "user" {
						id = accepted.UserMessage.ID
					}
					ordinal := repo.entryOrdinals["thread"][id]
					switch corruption {
					case "negative":
						repo.entryOrdinals["thread"][id] = -1
					case "past-end":
						repo.entryOrdinals["thread"][id] = len(repo.entries["thread"])
					case "other-entry":
						repo.entryOrdinals["thread"][id] = 1 - ordinal
					case "depth":
						repo.entryDepths["thread"][id]++
					case "thread":
						repo.entries["thread"][ordinal].ThreadID = "other"
					}
					if operation == "accept" {
						_, err = repo.AcceptTurn(t.Context(), req)
					} else {
						_, _, err = repo.ReadAcceptedTurn(t.Context(), "thread", "turn", "run")
					}
					if !errors.Is(err, ErrAuthorityCorrupt) {
						t.Fatalf("accepted corrupt locator: %v", err)
					}
					if len(repo.entries["thread"]) != 2 {
						t.Fatal("corrupt read changed the canonical journal")
					}
				})
			}
		}
	}
}

func BenchmarkSQLiteFixedHistoryReadAcceptedTurn(b *testing.B) {
	for _, count := range []int{1096, 10960, 109600} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			repo := newFixedHistorySQLiteRepo(b, count)
			if _, err := repo.Append(b.Context(), Entry{ThreadID: "history", TurnID: "turn", RunID: "run", Type: EntryTurnMarker, TurnStatus: TurnCompleted}, AppendOptions{ID: "history-ended"}); err != nil {
				b.Fatal(err)
			}
			if _, err := repo.AcceptTurn(b.Context(), AcceptTurnRequest{ThreadID: "history", TurnID: "latest", RunID: "latest-run", LogicalRequestID: "latest-send", RequestFingerprint: "send-fp", InputRequestFingerprint: "input-fp", Input: session.Message{Role: session.User, Content: "latest input"}}); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				result, found, err := repo.ReadAcceptedTurn(b.Context(), "history", "latest", "latest-run")
				if err != nil || !found || result.UserMessage.Message.Content != "latest input" {
					b.Fatalf("read accepted turn: found=%v err=%v", found, err)
				}
			}
		})
	}
}

func TestRuntimeTerminalReplayRejectsCorruptExactLocators(t *testing.T) {
	for _, operation := range []string{"finish", "cancel"} {
		for _, target := range []string{"terminal", "cancel"} {
			if operation == "finish" && target == "cancel" {
				continue
			}
			t.Run(operation+"/"+target, func(t *testing.T) {
				repo := NewMemoryRepo()
				if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
					t.Fatal(err)
				}
				if _, err := repo.AcceptTurn(t.Context(), AcceptTurnRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", LogicalRequestID: "send", RequestFingerprint: "send-fp", InputRequestFingerprint: "input-fp", Input: session.Message{Role: session.User, Content: "hello"}}); err != nil {
					t.Fatal(err)
				}
				finish := FinishTurnRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", TerminalEntryID: "terminal", OutcomeFingerprint: "finish-fp", Status: TurnCompleted}
				cancel := testCancelTurnRequest(repo.now())
				var terminalID, cancelID string
				if operation == "finish" {
					finished, err := repo.FinishTurn(t.Context(), finish)
					if err != nil {
						t.Fatal(err)
					}
					terminalID = finished.Terminal.ID
				} else {
					canceled, err := repo.CancelTurn(t.Context(), cancel)
					if err != nil {
						t.Fatal(err)
					}
					terminalID, cancelID = canceled.Terminal.ID, canceled.CancelRequest.ID
				}
				id := terminalID
				if target == "cancel" {
					id = cancelID
				}
				count := len(repo.entries["thread"])
				repo.entryOrdinals["thread"][id] = count
				var err error
				if operation == "finish" {
					_, err = repo.FinishTurn(t.Context(), finish)
				} else {
					_, err = repo.CancelTurn(t.Context(), cancel)
				}
				if !errors.Is(err, ErrAuthorityCorrupt) {
					t.Fatalf("corrupt replay: %v", err)
				}
				if len(repo.entries["thread"]) != count || repo.threads["thread"].LeafID != terminalID {
					t.Fatal("replay mutated canonical state")
				}
			})
		}
	}
}

func TestFinishTurnChecksContextAdmissionBeforeMutation(t *testing.T) {
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	accepted, err := repo.AcceptTurn(t.Context(), AcceptTurnRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", LogicalRequestID: "send", RequestFingerprint: "send-fp", InputRequestFingerprint: "input-fp", Input: session.Message{Role: session.User, Content: "hello"}})
	if err != nil {
		t.Fatal(err)
	}
	count := len(repo.entries["thread"])
	repo.entryOrdinals["thread"][accepted.TurnStarted.ID] = count
	_, err = repo.FinishTurn(t.Context(), FinishTurnRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", TerminalEntryID: "terminal", OutcomeFingerprint: "finish-fp", Status: TurnCompleted, ClearProviderState: true})
	if !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("corrupt context admission: %v", err)
	}
	if len(repo.entries["thread"]) != count || repo.threads["thread"].LeafID != accepted.UserMessage.ID {
		t.Fatal("failed prevalidation changed canonical state")
	}
}

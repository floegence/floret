package sessiontree

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/tools"
)

func parentSourceFixture(t *testing.T, count int) (*MemoryRepo, []string) {
	t.Helper()
	memory := newMemoryRepo(time.Now)
	memory.parentEvents = true
	for _, meta := range []ThreadMeta{{ID: "parent"}, {ID: "child", ParentThreadID: "parent", ParentTurnID: "parent-turn", TaskName: "child", HostProfileRef: "test"}} {
		if _, err := memory.CreateThread(t.Context(), meta); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := memory.AcceptTurn(t.Context(), AcceptTurnRequest{ThreadID: "parent", TurnID: "parent-turn", RunID: "parent-run", LogicalRequestID: "parent-request", RequestFingerprint: "parent-request", InputRequestFingerprint: "parent-request", Input: session.Message{Role: session.User, Content: "work"}}); err != nil {
		t.Fatal(err)
	}
	for i := range count {
		id := fmt.Sprint("child-turn-", i)
		if _, err := memory.AcceptTurn(t.Context(), AcceptTurnRequest{ThreadID: "child", TurnID: id, RunID: id, LogicalRequestID: id, RequestFingerprint: id, InputRequestFingerprint: id, Input: session.Message{Role: session.User, Content: "work"}}); err != nil {
			t.Fatal(err)
		}
		finished, err := memory.FinishTurn(t.Context(), FinishTurnRequest{ThreadID: "child", TurnID: id, RunID: id, TerminalEntryID: id + "-done", OutcomeFingerprint: id, Status: TurnCompleted})
		if err != nil {
			t.Fatal(err)
		}
		memory.appendAutomaticParentEventLocked(finished.Terminal)
	}
	var ids []string
	for _, entry := range memory.entries["parent"] {
		if entry.Type == EntryParentEvent {
			ids = append(ids, entry.ID)
		}
	}
	if len(ids) != count {
		t.Fatalf("fixture has %d receipts, want %d", len(ids), count)
	}
	return memory, ids
}

func TestParentSourcePagesMatchCanonicalProjectionAcrossBranches(t *testing.T) {
	memory, ids := parentSourceFixture(t, 137)
	// Non-prefix disjoint admissions exercise both AVL rotations and copied
	// paths. Released canonical semantics permit this order.
	random := rand.New(rand.NewPCG(41, 73))
	for _, i := range random.Perm(60) {
		payload, _ := json.Marshal(ParentEventAdmission{FirstEventID: ids[2*i], LastEventID: ids[2*i]})
		if _, err := memory.Append(t.Context(), Entry{ID: fmt.Sprint("admission-", i), ThreadID: "parent", TurnID: "parent-turn", RunID: "parent-run", Type: EntryParentEventsAdmitted, Payload: payload, Message: session.Message{Role: session.User, Kind: session.MessageKindParentEvents, Content: "admitted"}}, AppendOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	backend, repo := openV31ReviewFixture(t, memory)
	if err := repo.PrepareThread(t.Context(), "parent"); err != nil {
		t.Fatal(err)
	}
	if repo.domainMemory != nil {
		t.Fatal("fixture did not use persistent current-schema reads")
	}
	check := func(thread string) {
		t.Helper()
		for _, pending := range []bool{false, true} {
			query := ParentEventQuery{ThreadID: thread, Limit: 7, PendingOnly: pending}
			for {
				want, err := memory.ParentEvents(t.Context(), query)
				if err != nil {
					t.Fatal(err)
				}
				got, err := repo.ParentEvents(t.Context(), query)
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("query=%+v got=%+v want=%+v error=%v", query, got, want, err)
				}
				if got.NextAfterEventID == "" {
					break
				}
				query.AfterEventID = got.NextAfterEventID
			}
			query = ParentEventQuery{ThreadID: thread, Limit: 7, PendingOnly: pending, Latest: true}
			var later string
			for {
				want, err := memory.ParentEvents(t.Context(), query)
				if err != nil {
					t.Fatal(err)
				}
				got, err := repo.ParentEvents(t.Context(), query)
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("backward query=%+v got=%+v want=%+v error=%v", query, got, want, err)
				}
				if later != "" && len(got.Events) > 0 && got.Events[len(got.Events)-1].ID == later {
					t.Fatal("repeated backward boundary")
				}
				if got.PreviousBeforeEventID == "" {
					break
				}
				later = got.PreviousBeforeEventID
				query.Latest, query.BeforeEventID = false, later
			}
		}
	}
	check("parent")
	repo = reopenCurrentStartupRepo(t, backend, false)
	check("parent")
	for _, store := range []Repo{memory, repo} {
		if _, err := store.Fork(t.Context(), ForkOptions{SourceThreadID: "parent", EntryID: ids[69], NewThreadID: "fork"}); err != nil {
			t.Fatal(err)
		}
	}
	check("fork")
	for _, store := range []Repo{memory, repo} {
		if err := store.MoveLeaf(t.Context(), "parent", ids[80]); err != nil {
			t.Fatal(err)
		}
	}
	check("parent")
	// Append on the new path and retain the independent fork.
	for _, store := range []Repo{memory, repo} {
		if _, err := store.Append(t.Context(), Entry{ID: "new-parent-message", ThreadID: "parent", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "new path"}}, AppendOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	check("parent")
	check("fork")
}

func TestParentSourceRejectsDriftBeforeSkippingAdmissions(t *testing.T) {
	for _, damage := range []string{"count", "source-hash", "admission-hash", "admission-rank", "future"} {
		t.Run(damage, func(t *testing.T) {
			memory, ids := parentSourceFixture(t, 4)
			if _, err := memory.AdmitParentEvents(t.Context(), "parent", "parent-turn", "parent-run"); err != nil {
				t.Fatal(err)
			}
			backend, repo := openV31ReviewFixture(t, memory)
			if _, err := repo.ParentEvents(t.Context(), ParentEventQuery{ThreadID: "parent"}); err != nil {
				t.Fatal(err)
			}
			meta, _ := repo.Thread(t.Context(), "parent")
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				state, err := readParentSourceContext(tx, "parent", meta.LeafID)
				if err != nil {
					return err
				}
				switch damage {
				case "count":
					state.Count++
				case "source-hash":
					state.Event.Hash = "wrong"
				case "future":
					raw, _ := encodeActivityMetadata("parent-sources-v99", state)
					return tx.Put(projectionSourcesNamespace, parentSourceKey("parent", meta.LeafID), raw)
				default:
					node, err := readParentAdmission(tx, "parent", state.Admissions)
					if err != nil {
						return err
					}
					if damage == "admission-hash" {
						node.Source.Hash = "wrong"
					} else {
						node.Last--
					}
					raw, _ := encodeActivityMetadata("parent-admission-v1", node)
					return tx.Put(projectionSourcesNamespace, parentAdmissionKey("parent", state.Admissions), raw)
				}
				raw, _ := encodeActivityMetadata("parent-sources-v1", state)
				return tx.Put(projectionSourcesNamespace, parentSourceKey("parent", meta.LeafID), raw)
			}); err != nil {
				t.Fatal(err)
			}
			before := cloneMigrationRecords(backend.records)
			for _, query := range []ParentEventQuery{{ThreadID: "parent", PendingOnly: true}, {ThreadID: "parent", AfterEventID: ids[3]}} {
				// An empty after-page need only validate its boundary sources.
				if query.AfterEventID != "" && (damage == "admission-hash" || damage == "admission-rank") {
					continue
				}
				if _, err := repo.ParentEvents(t.Context(), query); !errors.Is(err, ErrAuthorityCorrupt) {
					t.Fatalf("accepted %s: %v", damage, err)
				}
			}
			if !reflect.DeepEqual(before, backend.records) {
				t.Fatal("corrupt read changed canonical or derived records")
			}
		})
	}
}

func TestParentEventPageDoesNotScanUnrelatedTurns(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "parent"}); err != nil {
		t.Fatal(err)
	}
	for i := range 1000 {
		id := fmt.Sprint("turn-", i)
		if _, err := memory.Append(t.Context(), Entry{ID: id, ThreadID: "parent", TurnID: id, RunID: id, Type: EntryTurnMarker, TurnStatus: TurnStarted, RequestKey: id, RequestFingerprint: id}, AppendOptions{}); err != nil {
			t.Fatal(err)
		}
		if _, err := memory.Append(t.Context(), Entry{ID: id + "-message", ThreadID: "parent", TurnID: id, RunID: id, Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "read"}}, AppendOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	backend, repo := openV31ReviewFixture(t, memory)
	if err := repo.PrepareThread(t.Context(), "parent"); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.ParentEvents(t.Context(), ParentEventQuery{ThreadID: "parent", Limit: 10}); err != nil {
		t.Fatal(err)
	}
	measured := &parentPageReadBackend{Backend: backend}
	repo.backend = measured
	page, err := repo.ParentEvents(t.Context(), ParentEventQuery{ThreadID: "parent", Limit: 10})
	if err != nil || len(page.Events) != 0 {
		t.Fatalf("page=%+v error=%v", page, err)
	}
	if measured.gets > 50 || measured.scans != 0 {
		t.Fatalf("parent page traversed unrelated history: gets=%d scans=%d", measured.gets, measured.scans)
	}
	t.Logf("turns=1000 gets=%d scans=%d", measured.gets, measured.scans)
}

func TestParentPageInteractionReadsOnlySelectedSources(t *testing.T) {
	memory, _ := parentSourceFixture(t, 0)
	if _, err := memory.AcceptTurn(t.Context(), AcceptTurnRequest{ThreadID: "child", TurnID: "work", RunID: "work", LogicalRequestID: "work", RequestFingerprint: "work", InputRequestFingerprint: "work", Input: session.Message{Role: session.User, Content: "work"}}); err != nil {
		t.Fatal(err)
	}
	for i := range 1000 {
		if _, err := memory.Append(t.Context(), Entry{ID: fmt.Sprint("unrelated-", i), ThreadID: "child", TurnID: "work", RunID: "work", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "unrelated"}}, AppendOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	request, err := memory.Append(t.Context(), Entry{ID: "interaction-requested:approval", ThreadID: "child", TurnID: "work", RunID: "work", Type: EntryInteractionAsked, Payload: json.RawMessage("{\"kind\":\"approval\"}")}, AppendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	memory.appendAutomaticParentEventLocked(request)
	backend, repo := openV31ReviewFixture(t, memory)
	query := ParentEventQuery{ThreadID: "parent"}
	if _, err := repo.ParentEvents(t.Context(), query); err != nil {
		t.Fatal(err)
	}
	check := func(resolved bool) {
		t.Helper()
		measured := &parentPageReadBackend{Backend: backend}
		repo.backend = measured
		page, err := repo.ParentEvents(t.Context(), query)
		repo.backend = backend
		if err != nil || len(page.Events) != 1 || page.Events[0].Resolved != resolved {
			t.Fatalf("page=%+v err=%v", page, err)
		}
		if measured.scans != 0 || measured.gets > 125 {
			t.Fatalf("selected interaction scanned unrelated turn: gets=%d scans=%d", measured.gets, measured.scans)
		}
		t.Logf("resolved=%v gets=%d scans=%d", resolved, measured.gets, measured.scans)
	}
	check(false)
	if _, err := repo.Append(t.Context(), Entry{ID: "interaction-resolved:approval", ThreadID: "child", TurnID: "work", RunID: "work", Type: EntryInteractionDone}, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	check(true)
	repo = reopenCurrentStartupRepo(t, backend, false)
	check(true)
}

func TestParentSourceAppendFailureIsAtomic(t *testing.T) {
	for _, mode := range []string{"failure", "cancel", "panic"} {
		t.Run(mode, func(t *testing.T) {
			memory, ids := parentSourceFixture(t, 4)
			backend, repo := openV31ReviewFixture(t, memory)
			if _, err := repo.ParentEvents(t.Context(), ParentEventQuery{ThreadID: "parent"}); err != nil {
				t.Fatal(err)
			}
			before := cloneMigrationRecords(backend.records)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			injected := errors.New("injected parent source commit failure")
			repo.backend = &domainCommitBoundaryBackend{Backend: backend, afterCallback: func() error {
				switch mode {
				case "cancel":
					cancel()
					return ctx.Err()
				case "panic":
					panic(injected)
				default:
					return injected
				}
			}}
			payload, _ := json.Marshal(ParentEventAdmission{FirstEventID: ids[1], LastEventID: ids[2]})
			entry := Entry{ID: "new-admission", ThreadID: "parent", TurnID: "parent-turn", RunID: "parent-run", Type: EntryParentEventsAdmitted, Payload: payload, Message: session.Message{Role: session.User, Kind: session.MessageKindParentEvents, Content: "admitted"}}
			failed := false
			func() {
				defer func() {
					if recover() != nil {
						failed = true
					}
				}()
				_, err := repo.Append(ctx, entry, AppendOptions{})
				failed = err != nil
			}()
			if !failed || !reflect.DeepEqual(before, backend.records) {
				t.Fatal("failed admission published partial source metadata")
			}
			repo.backend = backend
			if _, err := repo.Append(t.Context(), entry, AppendOptions{}); err != nil {
				t.Fatal(err)
			}
			repo = reopenCurrentStartupRepo(t, backend, false)
			page, err := repo.ParentEvents(t.Context(), ParentEventQuery{ThreadID: "parent", PendingOnly: true})
			if err != nil || len(page.Events) != 2 || page.Events[0].ID != ids[0] || page.Events[1].ID != ids[3] {
				t.Fatalf("retry=%+v err=%v", page, err)
			}
		})
	}
}

type parentPageReadBackend struct {
	spi.Backend
	gets, scans int
}
type parentPageReadTx struct {
	spi.ReadTx
	owner *parentPageReadBackend
}

func (b *parentPageReadBackend) View(ctx context.Context, f func(spi.ReadTx) error) error {
	return b.Backend.View(ctx, func(tx spi.ReadTx) error { return f(parentPageReadTx{tx, b}) })
}
func (tx parentPageReadTx) Get(ns string, key []byte) ([]byte, error) {
	tx.owner.gets++
	return tx.ReadTx.Get(ns, key)
}
func (tx parentPageReadTx) Scan(q spi.ScanRequest) (spi.ScanPage, error) {
	tx.owner.scans++
	return tx.ReadTx.Scan(q)
}

func TestParentSelectedInteractionMatchesWholeTurnJournal(t *testing.T) {
	for _, scenario := range []string{"waiting-before", "waiting-after", "failed-control", "other-run", "retained-branch"} {
		t.Run(scenario, func(t *testing.T) {
			memory, _ := parentSourceFixture(t, 0)
			if _, err := memory.AcceptTurn(t.Context(), AcceptTurnRequest{ThreadID: "child", TurnID: "work", RunID: "run", LogicalRequestID: "work", RequestFingerprint: "work", InputRequestFingerprint: "work", Input: session.Message{Role: session.User, Content: "work"}}); err != nil {
				t.Fatal(err)
			}
			appendEntry := func(entry Entry) Entry {
				t.Helper()
				entry.ThreadID, entry.TurnID = "child", "work"
				if entry.RunID == "" {
					entry.RunID = "run"
				}
				stored, err := memory.Append(t.Context(), entry, AppendOptions{})
				if err != nil {
					t.Fatal(err)
				}
				return stored
			}
			var requested Entry
			if scenario == "waiting-before" || scenario == "waiting-after" {
				waiting := Entry{ID: "waiting", Type: EntryTurnMarker, TurnStatus: TurnWaiting}
				if scenario == "waiting-before" {
					appendEntry(waiting)
				}
				appendEntry(Entry{ID: "call", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "work", ToolCallID: "call", ToolArgs: "{}"}})
				requested = appendEntry(Entry{ID: "result", Type: EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "work", ToolCallID: "call", Content: "choose", ToolResult: &session.ToolResultView{Status: "success", InputRequired: &tools.InputRequest{Summary: "Choose", Questions: []tools.InputQuestion{{ID: "choice", Prompt: "Choose?", Kind: "write"}}}}}})
				if scenario == "waiting-after" {
					appendEntry(waiting)
				}
			} else {
				requested = appendEntry(Entry{ID: "interaction-requested:approval", Type: EntryInteractionAsked, Payload: json.RawMessage("{\"kind\":\"approval\"}")})
				if scenario == "failed-control" || scenario == "other-run" {
					run := "run"
					if scenario == "other-run" {
						run = "other-run"
					}
					appendEntry(Entry{ID: "failed-control", RunID: run, Type: EntryToolCall, Message: session.Message{Role: session.Assistant, Kind: session.MessageKindControlSignal, Content: "tool_call", ToolCallID: "approval", ToolName: "ask_user", ControlSignal: &session.ControlSignalView{CallID: "approval", Name: "ask_user", Disposition: "failed", ErrorCode: session.ControlSignalErrorCodeControlError}}})
				}
				if scenario == "retained-branch" {
					appendEntry(Entry{ID: "interaction-resolved:approval", Type: EntryInteractionDone})
					if err := memory.MoveLeaf(t.Context(), "child", requested.ID); err != nil {
						t.Fatal(err)
					}
					appendEntry(Entry{ID: "new-branch", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "new path"}})
				}
			}
			memory.appendAutomaticParentEventLocked(requested)
			backend, repo := openV31ReviewFixture(t, memory)
			query := ParentEventQuery{ThreadID: "parent"}
			want, err := memory.ParentEvents(t.Context(), query)
			if err != nil || len(want.Events) != 1 {
				t.Fatalf("fixture page=%+v error=%v", want, err)
			}
			if want.Events[0].Resolved != (scenario == "failed-control" || scenario == "retained-branch") {
				t.Fatalf("wrong fixture resolution: %+v", want)
			}
			for _, missing := range []bool{false, true} {
				if missing {
					if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
						for key := range backend.records[projectionSourcesNamespace] {
							if bytes.HasPrefix([]byte(key), backendDomainKey("child")) {
								if err := tx.Delete(projectionSourcesNamespace, []byte(key)); err != nil {
									return err
								}
							}
						}
						return nil
					}); err != nil {
						t.Fatal(err)
					}
				}
				got, err := repo.ParentEvents(t.Context(), query)
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("missing=%v got=%+v want=%+v error=%v", missing, got, want, err)
				}
				repo = reopenCurrentStartupRepo(t, backend, false)
			}
		})
	}
}

package sessiontree

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

// Source groups already own append-ordered membership. The window anchors
// must not copy those histories into a growing value on every append.
func TestWindowSourceAnchorsDoNotCopySourceHistory(t *testing.T) {
	var stored storedWindowSources
	var turn storedWindowTurnSources
	for i := range 1000 {
		entry := Entry{ID: fmt.Sprintf("source-%04d", i), RawHash: "hash", PathDepth: int64(i + 1), TurnID: "turn", RunID: "run", Type: EntryTurnMarker, TurnStatus: TurnSavePoint}
		if err := observeWindowSources(t.Context(), nil, &stored, &turn, entry); err != nil {
			t.Fatal(err)
		}
		entry.Type = EntryUserMessage
		entry.Message = session.Message{Role: session.User}
		entry.Metadata = map[string]string{SubAgentUserMessageOriginMetadataKey: SubAgentUserMessageOriginDelegatedMission}
		if err := observeWindowSources(t.Context(), nil, &stored, &turn, entry); err != nil {
			t.Fatal(err)
		}
	}
	for name, value := range map[string]any{"thread": stored, "turn": turn} {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if len(raw) > 512 {
			t.Errorf("%s anchor copied source history: %d bytes", name, len(raw))
		}
	}
}

func TestWindowSourcesRejectForgedCompactionAnchors(t *testing.T) {
	for _, change := range []string{"ordinal", "anchor", "missing", "source", "kind", "turn", "run", "slot", "hidden"} {
		t.Run(change, func(t *testing.T) {
			backend, repo := projectionSourcesFixture(t, 3)
			entry, err := NewThreadContextCompactionEntry(ThreadContextCompaction{ThreadID: "thread", TurnID: "turn", RunID: "run", RequestID: "request", OperationID: "compact", Phase: "start", Status: "running"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := repo.Append(t.Context(), entry, AppendOptions{ID: "compact-start"}); err != nil {
				t.Fatal(err)
			}
			if _, err := repo.ReadWindowSources(t.Context(), "thread", "", nil); err != nil {
				t.Fatal(err)
			}
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				var stored storedWindowSources
				if err := readItemJSON(tx, windowSourcesKey("thread"), &stored); err != nil {
					return err
				}
				switch change {
				case "ordinal":
					stored.CompactionOrdinals["compact"] = 1
				case "anchor":
					stored.Anchors["compact"] = "unrelated-0"
				case "missing":
					delete(stored.CompactionOrdinals, "compact")
				case "source", "kind", "turn", "run", "slot", "hidden":
					ref := stored.CompactionSources["compact"]
					if ref.Anchor == nil {
						return errors.New("fixture requires anchor")
					}
					switch change {
					case "source":
						ref.Anchor.SourceID = "call"
					case "kind":
						ref.Anchor.Kind = "tool"
					case "turn":
						ref.Anchor.TurnID = "different"
					case "run":
						ref.Anchor.RunID = "different"
					case "slot":
						ref.Anchor.Slot++
					case "hidden":
						ref.Anchor.Hidden = true
					}
					stored.CompactionSources["compact"] = ref
				}
				return putItemJSON(tx, windowSourcesKey("thread"), stored)
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := repo.ReadWindowSources(t.Context(), "thread", "", nil); !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("forged %s accepted: %v", change, err)
			}
		})
	}
}

// A compaction boundary fixes an item's identity and position. Its activity
// may subsequently settle or gain source-bound safety facts at the same slot.
func TestWindowCompactionAnchorSurvivesActivityUpdatesAndRebuild(t *testing.T) {
	for _, prepared := range []bool{false, true} {
		t.Run(fmt.Sprint(prepared), func(t *testing.T) {
			backend, repo := projectionSourcesFixture(t, 0)
			appendEntry := func(entry Entry) {
				t.Helper()
				entry.ThreadID, entry.TurnID, entry.RunID = "thread", "turn", "run"
				if _, err := repo.Append(t.Context(), entry, AppendOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			appendEntry(Entry{ID: "pending", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: "pending"}})
			compact, err := NewThreadContextCompactionEntry(ThreadContextCompaction{ThreadID: "thread", TurnID: "turn", RunID: "run", RequestID: "request", OperationID: "compact", Phase: "start", Status: "running"})
			if err != nil {
				t.Fatal(err)
			}
			appendEntry(compact)
			if prepared {
				if _, err := repo.ReadWindowSources(t.Context(), "thread", "", nil); err != nil {
					t.Fatal(err)
				}
			}
			appendEntry(Entry{ID: "settled", Type: EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "inspect", ToolCallID: "pending", Content: "failed", ToolResult: &session.ToolResultView{Status: "error"}}})
			check := func() {
				t.Helper()
				got, err := repo.ReadWindowSources(t.Context(), "thread", "", nil)
				if err != nil || got.Anchors["compact"] != "tool:turn:pending" {
					t.Fatalf("stable compaction anchor rejected: %v %+v", err, got.Anchors)
				}
			}
			check()
			repo = reopenCurrentStartupRepo(t, backend, false)
			check()
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return deleteItemMembership(t.Context(), tx, "thread") }); err != nil {
				t.Fatal(err)
			}
			check()
		})
	}
}

func TestWindowSourcesRejectWrongBoundaryIdentity(t *testing.T) {
	for _, kind := range []string{"identity", "category", "run"} {
		t.Run(kind, func(t *testing.T) {
			backend, repo := projectionSourcesFixture(t, 2)
			if _, err := repo.ReadWindowSources(t.Context(), "thread", "", []string{"turn"}); err != nil {
				t.Fatal(err)
			}
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				var turn storedWindowTurnSources
				if err := readItemJSON(tx, windowTurnSourcesKey("thread", "turn"), &turn); err != nil {
					return err
				}
				switch kind {
				case "identity":
					return putItemJSON(tx, windowTurnSourcesKey("thread", "wrong-turn"), turn)
				case "category":
					turn.Boundaries["run"] = "unrelated-0"
				case "run":
					turn.Boundaries["wrong-run"] = turn.Boundaries["run"]
				}
				return putItemJSON(tx, windowTurnSourcesKey("thread", "turn"), turn)
			}); err != nil {
				t.Fatal(err)
			}
			turn := "turn"
			if kind == "identity" {
				turn = "wrong-turn"
			}
			if _, err := repo.ReadWindowSources(t.Context(), "thread", "", []string{turn}); !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("wrong %s accepted: %v", kind, err)
			}
		})
	}
}

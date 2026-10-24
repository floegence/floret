package sessiontree

import (
	"context"
	"errors"
	"fmt"
	"github.com/floegence/floret/v7/storage/spi"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
)

func TestFieldWindowDoesNotReplayReplacedToolResults(t *testing.T) {
	for _, count := range []int{20, 300, 1000} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			memory := newMemoryRepo(time.Now)
			if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
				t.Fatal(err)
			}
			for i := 0; i <= count; i++ {
				entry := Entry{ID: fmt.Sprint("source-", i), ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "inspect", ToolCallID: "call", ToolResult: &session.ToolResultView{Status: "success"}}}
				if i == 0 {
					entry.Type = EntryToolCall
					entry.Message.Role = session.Assistant
					entry.Message.ToolResult = nil
				}
				if _, err := memory.Append(t.Context(), entry, AppendOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			backend, repo := openV31ReviewFixture(t, memory)
			input := ItemMembershipWindowOptions{ThreadID: "thread", Limit: 1, ActivityFields: true}
			if _, err := repo.ReadWindowProjection(t.Context(), input, func(PathMetadata, bool) []string { return nil }); err != nil {
				t.Fatal(err)
			}
			measured := &currentStartupBackend{Backend: backend}
			repo.backend = measured
			window, err := repo.ReadWindowProjection(t.Context(), input, func(PathMetadata, bool) []string { return nil })
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("results=%d sources=%d metadata_gets=%d body_reads=%d", count, len(window.Entries), measured.gets, measured.bodies)
			if len(window.Entries) > 4 || measured.gets > 150 || measured.journalRows > 6 {
				t.Fatalf("replayed replacements: results=%d sources=%d canonical=%d gets=%d", count, len(window.Entries), measured.journalRows, measured.gets)
			}
		})
	}
}

func TestFieldWindowSourceRollbackAndBranch(t *testing.T) {
	backend, repo := projectionSourcesFixture(t, 1)
	input := ItemMembershipWindowOptions{ThreadID: "thread", Limit: 1, ActivityFields: true}
	fields := func(PathMetadata, bool) []string { return nil }
	before, err := repo.ReadWindowProjection(t.Context(), input, fields)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := cloneMigrationRecords(backend.records)
	injected := errors.New("commit failure")
	repo.backend = &domainCommitBoundaryBackend{Backend: backend, afterCallback: func() error { return injected }}
	entry := Entry{ID: "replacement", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "inspect", ToolCallID: "work", ToolResult: &session.ToolResultView{Status: "error"}}}
	if _, err := repo.Append(t.Context(), entry, AppendOptions{}); !errors.Is(err, injected) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(snapshot, backend.records) {
		t.Fatal("failed append changed source index")
	}
	repo.backend = backend
	if _, err := repo.Append(t.Context(), entry, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	changed, err := repo.ReadWindowProjection(t.Context(), input, fields)
	if err != nil {
		t.Fatal(err)
	}
	if changed.Membership.LeafID == before.Membership.LeafID {
		t.Fatal("replacement missing")
	}
	if err := repo.MoveLeaf(t.Context(), "thread", "result"); err != nil {
		t.Fatal(err)
	}
	restored, err := repo.ReadWindowProjection(t.Context(), input, fields)
	if err != nil {
		t.Fatal(err)
	}
	if restored.Membership.LeafID != before.Membership.LeafID {
		t.Fatal("wrong branch")
	}
	for _, source := range restored.Entries {
		if source.Entry.ID == "replacement" {
			t.Fatal("inactive result escaped")
		}
	}
	repo = reopenCurrentStartupRepo(t, backend, false)
	if _, err := repo.ReadWindowProjection(t.Context(), input, fields); err != nil {
		t.Fatal(err)
	}
}
func TestFieldWindowRejectsCorruptCompactSource(t *testing.T) {
	for _, kind := range []string{"count", "hash", "missing"} {
		t.Run(kind, func(t *testing.T) {
			backend, repo := projectionSourcesFixture(t, 1)
			input := ItemMembershipWindowOptions{ThreadID: "thread", AnchorID: "tool:turn:work", Direction: PathWindowAround, Limit: 1, ActivityFields: true}
			fields := func(PathMetadata, bool) []string { return nil }
			if _, err := repo.ReadWindowProjection(t.Context(), input, fields); err != nil {
				t.Fatal(err)
			}
			group := projectionSourceGroup{Kind: "tool", Turn: "turn", Run: "run", ID: "work"}
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				state, err := readWindowToolSources(tx, "thread", group)
				if err != nil {
					return err
				}
				switch kind {
				case "count":
					state.Count++
				case "hash":
					state.Last.RawHash = "drift"
				case "missing":
					return tx.Delete(projectionSourcesNamespace, windowToolSourcesKey("thread", group))
				}
				return putWindowToolSources(tx, "thread", group, state)
			}); err != nil {
				t.Fatal(err)
			}
			before := cloneMigrationRecords(backend.records)
			if _, err := repo.ReadWindowProjection(context.Background(), input, fields); !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("corruption accepted: %v", err)
			}
			if !reflect.DeepEqual(before, backend.records) {
				t.Fatal("corruption mutated canonical storage")
			}
		})
	}
}

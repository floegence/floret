package agentharness

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/floegence/floret/internal/provider/cache"
	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/sessiontree"
	"github.com/floegence/floret/observation"
)

func TestListPendingToolSettlementTargetsReadsCompleteCanonicalPathAndExcludesSettled(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	harness := newTestHarness(nil, repo, cache.NewMemoryStore())
	thread, err := harness.StartThread(ctx, StartThreadOptions{ThreadID: "thread"})
	if err != nil {
		t.Fatal(err)
	}

	for index := 0; index < MaxThreadDetailEventLimit+10; index++ {
		turnID := fmt.Sprintf("noise-turn-%03d", index)
		runID := fmt.Sprintf("noise-run-%03d", index)
		if _, err := sessiontree.AppendTurnMarker(ctx, repo, "thread", turnID, sessiontree.TurnStarted, map[string]string{"run_id": runID}); err != nil {
			t.Fatal(err)
		}
		if _, err := sessiontree.AppendMessage(ctx, repo, "thread", turnID, session.Message{Role: session.User, Content: "noise"}); err != nil {
			t.Fatal(err)
		}
		if _, err := sessiontree.AppendTurnMarker(ctx, repo, "thread", turnID, sessiontree.TurnCompleted, map[string]string{"run_id": runID}); err != nil {
			t.Fatal(err)
		}
	}
	appendPendingToolResultFixture(t, ctx, repo, "thread", "turn-1")

	detail, err := harness.ListThreadDetailEvents(ctx, ListThreadDetailEventsOptions{ThreadID: "thread", Limit: MaxThreadDetailEventLimit, IncludeRaw: true})
	if err != nil {
		t.Fatal(err)
	}
	if !detail.HasMore {
		t.Fatal("detail projection unexpectedly covered the complete path")
	}
	for _, event := range detail.Events {
		if event.ToolResult != nil && event.ToolResult.Status == string(observation.ActivityStatusRunning) {
			t.Fatal("first detail page unexpectedly contained the pending target")
		}
	}

	targets, err := harness.ListPendingToolSettlementTargets(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	want := sessiontree.PendingToolSettlementTarget{
		ThreadID: "thread", TurnID: "turn-1", RunID: "run-1",
		ToolCallID: "exec-1", ToolName: "terminal.exec", Handle: "terminal:job:123",
	}
	if len(targets) != 1 || targets[0] != want {
		t.Fatalf("targets = %#v, want %#v", targets, want)
	}

	if _, err := thread.SettlePendingTool(ctx, PendingToolSettlement{
		TurnID: "turn-1", RunID: "run-1", ToolCallID: "exec-1", ToolName: "terminal.exec", Handle: "terminal:job:123",
		Status: PendingToolSettledCompleted, Summary: "completed",
	}); err != nil {
		t.Fatal(err)
	}
	targets, err = harness.ListPendingToolSettlementTargets(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 0 {
		t.Fatalf("settled target remained active: %#v", targets)
	}
}

func TestListPendingToolSettlementTargetsFailsClosedOnMalformedAuthority(t *testing.T) {
	ctx := context.Background()
	repo := sessiontree.NewMemoryRepo()
	harness := newTestHarness(nil, repo, cache.NewMemoryStore())
	if _, err := harness.StartThread(ctx, StartThreadOptions{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendTurnMarker(ctx, repo, "thread", "turn", sessiontree.TurnStarted, map[string]string{"run_id": "run"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendMessage(ctx, repo, "thread", "turn", session.Message{
		Role: session.Tool, ToolCallID: "call", ToolName: "terminal.exec",
		ToolResult: &session.ToolResultView{Status: string(observation.ActivityStatusRunning)},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := harness.ListPendingToolSettlementTargets(ctx, "thread"); !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("error = %v, want ErrAuthorityCorrupt", err)
	}
}

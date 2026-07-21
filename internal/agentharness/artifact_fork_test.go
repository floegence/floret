package agentharness

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/floegence/floret/internal/engine"
	"github.com/floegence/floret/internal/provider/cache"
	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/session/artifact"
	"github.com/floegence/floret/internal/sessiontree"
	"github.com/floegence/floret/internal/testing/harness"
	"github.com/floegence/floret/tools"
)

func TestRootForkCopiesExactFullOutputArtifactClosure(t *testing.T) {
	ctx := context.Background()
	h, repo, ref, fullText := artifactForkFixture(t, ctx, "source")

	if _, err := h.ForkThreadWithResult(ctx, ForkOptions{
		OperationID: "fork-with-artifact", SourceThreadID: "source", NewThreadID: "fork",
	}); err != nil {
		t.Fatal(err)
	}
	assertForkedArtifactContent(t, ctx, repo, "", "fork", ref, fullText)

	if _, err := h.ForkThreadWithResult(ctx, ForkOptions{
		OperationID: "fork-with-artifact", SourceThreadID: "source", NewThreadID: "fork",
	}); err != nil {
		t.Fatalf("exact fork replay: %v", err)
	}
}

func TestRootForkReplaysArtifactClosureWithRewrittenRetrySource(t *testing.T) {
	ctx := context.Background()
	h, repo, ref, fullText := artifactForkFixture(t, ctx, "source")
	entries, err := repo.Entries(ctx, "source")
	if err != nil {
		t.Fatal(err)
	}
	var sourceUser sessiontree.Entry
	for _, entry := range entries {
		if entry.Type == sessiontree.EntryUserMessage && entry.TurnID == "turn-1" {
			sourceUser = entry
			break
		}
	}
	if sourceUser.ID == "" {
		t.Fatal("source user entry is missing")
	}
	retryRequest := sessiontree.AdmitTurnRequest{
		ThreadID: "source", TurnID: "turn-retry", RunID: "run-retry", OwnerID: "owner-retry",
		RetrySourceTurnID: "turn-1", RetrySourceEntryID: sourceUser.ID,
	}
	retryRequest.RequestFingerprint, err = sessiontree.TurnAdmissionRequestFingerprint(retryRequest)
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := repo.AdmitTurn(ctx, retryRequest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Append(sessiontree.ContextWithTurnLease(ctx, admitted.Lease), sessiontree.Entry{
		ThreadID: "source", TurnID: "turn-retry", Type: sessiontree.EntryAssistantMessage,
		Message: session.Message{Role: session.Assistant, Content: "retried"},
	}, sessiontree.AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.FinishTurn(ctx, sessiontree.FinishTurnRequest{
		Lease: admitted.Lease, RunID: "run-retry", TerminalEntryID: "retry-terminal", Status: sessiontree.TurnCompleted,
		OutcomeFingerprint: "outcome-retry",
	}); err != nil {
		t.Fatal(err)
	}

	opts := ForkOptions{OperationID: "fork-retry-artifact", SourceThreadID: "source", NewThreadID: "fork"}
	if _, err := h.ForkThreadWithResult(ctx, opts); err != nil {
		t.Fatal(err)
	}
	assertForkedArtifactContent(t, ctx, repo, "", "fork", ref, fullText)
	page, err := repo.ListCanonicalTurns(ctx, sessiontree.ListCanonicalTurnsOptions{ThreadID: "fork", Tail: 2})
	if err != nil || len(page.Turns) != 2 || page.Turns[1].RetrySource == nil || page.Turns[1].RetrySource.TurnID != page.Turns[0].TurnID {
		t.Fatalf("fork retry page=%#v err=%v", page, err)
	}
	forkUserID := ""
	for _, item := range page.Turns[0].Entries {
		if item.Entry.Type == sessiontree.EntryUserMessage {
			forkUserID = item.Entry.ID
			break
		}
	}
	if forkUserID == "" || page.Turns[1].RetrySource.EntryID != forkUserID || forkUserID == sourceUser.ID {
		t.Fatalf("fork retry source=%#v source_user=%q fork_user=%q", page.Turns[1].RetrySource, sourceUser.ID, forkUserID)
	}
	if _, err := h.ForkThreadWithResult(ctx, opts); err != nil {
		t.Fatalf("exact fork replay: %v", err)
	}
	replayed, err := repo.ListCanonicalTurns(ctx, sessiontree.ListCanonicalTurnsOptions{ThreadID: "fork", Tail: 2})
	if err != nil || !reflect.DeepEqual(replayed.Turns, page.Turns) {
		t.Fatalf("replayed retry page=%#v err=%v want=%#v", replayed, err, page)
	}
}

func TestFullPathSubAgentCopiesAndReplaysExactFullOutputArtifactClosure(t *testing.T) {
	ctx := context.Background()
	h, repo, ref, fullText := artifactForkFixture(t, ctx, "parent")
	opts := SpawnSubAgentOptions{
		PublicationID: "publish-with-artifact", ParentThreadID: "parent", ThreadID: "child",
		TaskName: "artifact reader", Message: "inspect the inherited output", ForkMode: SubAgentForkFullPath,
	}
	if _, err := h.SpawnSubAgent(ctx, opts); err != nil {
		t.Fatal(err)
	}
	assertForkedArtifactContent(t, ctx, repo, "parent", "child", ref, fullText)

	if _, err := h.SpawnSubAgent(ctx, opts); err != nil {
		t.Fatalf("exact publication replay: %v", err)
	}
}

func TestSubAgentPublicationFingerprintIncludesArtifactClosure(t *testing.T) {
	meta := sessiontree.ThreadMeta{ID: "child", ParentThreadID: "parent", TaskName: "task", AgentPath: "/root/task"}
	message := session.Message{Role: session.User, Content: "work"}
	labels := engine.RunLabels{Host: map[string]string{"host": "value"}}

	left, err := subAgentPublicationFingerprint("publication", meta, SubAgentForkFullPath, "leaf", "closure-a", message, labels)
	if err != nil {
		t.Fatal(err)
	}
	right, err := subAgentPublicationFingerprint("publication", meta, SubAgentForkFullPath, "leaf", "closure-b", message, labels)
	if err != nil {
		t.Fatal(err)
	}
	if left == right {
		t.Fatalf("publication fingerprint did not bind artifact closure: %q", left)
	}
}

func TestSubAgentFullPathForkIdentitiesAreDeterministic(t *testing.T) {
	path := []sessiontree.Entry{
		{TurnID: "turn-a", Metadata: map[string]string{"run_id": "run-a"}},
		{TurnID: "turn-a", Metadata: map[string]string{"run_id": "run-a"}},
		{TurnID: "turn-b", Metadata: map[string]string{"run_id": "run-b"}},
	}
	turnIDs, runIDs := subAgentForkIdentityRewrite("publication", path)
	replayedTurns, replayedRuns := subAgentForkIdentityRewrite("publication", path)
	if !reflect.DeepEqual(turnIDs, replayedTurns) || !reflect.DeepEqual(runIDs, replayedRuns) {
		t.Fatalf("identity replay turns=%#v/%#v runs=%#v/%#v", turnIDs, replayedTurns, runIDs, replayedRuns)
	}
	if turnIDs["turn-a"] == "turn-a" || runIDs["run-a"] == "run-a" || turnIDs["turn-a"] == turnIDs["turn-b"] || runIDs["run-a"] == runIDs["run-b"] {
		t.Fatalf("identity rewrites turns=%#v runs=%#v", turnIDs, runIDs)
	}
}

func TestForkRequestFingerprintRetainsDurableV15Shape(t *testing.T) {
	opts := ForkOptions{SourceThreadID: "source", EntryID: "entry", Position: sessiontree.ForkBefore, NewThreadID: "fork"}
	got, err := forkRequestFingerprint(opts)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := json.Marshal(struct {
		SourceThreadID        string                   `json:"source_thread_id"`
		SourceEntryID         string                   `json:"source_entry_id,omitempty"`
		Position              sessiontree.ForkPosition `json:"position"`
		DestinationThreadID   string                   `json:"destination_thread_id"`
		RewriteTurnIdentities bool                     `json:"rewrite_turn_identities"`
	}{
		SourceThreadID: "source", SourceEntryID: "entry", Position: sessiontree.ForkBefore,
		DestinationThreadID: "fork", RewriteTurnIdentities: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := sessiontree.StableHash(string(legacy)); got != want {
		t.Fatalf("fork fingerprint=%q, want durable v15 fingerprint %q", got, want)
	}
}

func TestSubAgentPublicationFingerprintRetainsDurableV15Shape(t *testing.T) {
	meta := sessiontree.ThreadMeta{ID: "child", ParentThreadID: "parent", TaskName: "task", AgentPath: "/root/task"}
	message := session.Message{Role: session.User, Content: "work"}
	labels := engine.RunLabels{Host: map[string]string{"host": "value"}, Correlation: map[string]string{"trace": "value"}}
	got, err := subAgentPublicationFingerprint("publication", meta, SubAgentForkFullPath, "leaf", "closure", message, labels)
	if err != nil {
		t.Fatal(err)
	}
	legacy := struct {
		PublicationID              string
		Child                      sessiontree.ThreadMeta
		ForkMode                   SubAgentForkMode
		SourceLeafID               string
		ArtifactClosureFingerprint string
		Message                    session.Message
		HostLabels                 map[string]string
		CorrelationLabels          map[string]string
	}{
		PublicationID: "publication", Child: meta, ForkMode: SubAgentForkFullPath, SourceLeafID: "leaf",
		ArtifactClosureFingerprint: "closure", Message: message, HostLabels: labels.Host, CorrelationLabels: labels.Correlation,
	}
	want, err := hashSubAgentAuthorityPayload(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("publication fingerprint=%q, want durable v15 fingerprint %q", got, want)
	}
}

func artifactForkFixture(t *testing.T, ctx context.Context, threadID string) (*AgentHarness, *sessiontree.MemoryRepo, artifact.Ref, string) {
	t.Helper()
	const fullText = "0123456789abcdef"
	repo := sessiontree.NewMemoryRepo()
	provider := harness.NewScriptedProvider(
		harness.Step(harness.Tool("call-1", "shell", `{"value":"x"}`), harness.DoneReason("tool_calls")),
		harness.Step(harness.Text("done"), harness.Done()),
	)
	h := newTestHarness(provider, repo, cache.NewMemoryStore())
	mustRegister(h.options.Tools, tools.Define[stringArgs](
		tools.Definition{
			Name: "shell", InputSchema: tools.StrictObject(map[string]any{"value": tools.String("value")}, []string{"value"}),
			ReadOnly: true, Permission: tools.PermissionSpec{Mode: tools.PermissionAllow},
			OutputPolicy: tools.OutputPolicy{VisibleMaxBytes: 8, Strategy: tools.OutputTail, PreserveFull: true},
		},
		nil,
		nil,
		func(context.Context, tools.Invocation[stringArgs]) (tools.Result, error) {
			return tools.Result{Text: fullText}, nil
		},
	))
	thread, err := h.StartThread(ctx, StartThreadOptions{ThreadID: threadID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := thread.Run(ctx, "run", RunOptions{RunID: "run-1", TurnID: "turn-1"}); err != nil {
		t.Fatal(err)
	}
	journal, err := thread.Journal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range journal.Path {
		if entry.Type == sessiontree.EntryToolResult && entry.Message.ToolResult != nil && entry.Message.ToolResult.FullOutput != nil {
			ref := *entry.Message.ToolResult.FullOutput
			content, err := repo.ReadArtifact(ctx, sessiontree.ArtifactReadRequest{ThreadID: threadID, ArtifactID: ref.ID})
			if err != nil || content.Text != fullText || content.Ref != ref {
				t.Fatalf("source artifact content=%#v err=%v", content, err)
			}
			return h, repo, ref, fullText
		}
	}
	t.Fatal("source journal has no full-output artifact")
	return nil, nil, artifact.Ref{}, ""
}

func assertForkedArtifactContent(t *testing.T, ctx context.Context, repo *sessiontree.MemoryRepo, parentThreadID, threadID string, ref artifact.Ref, text string) {
	t.Helper()
	content, err := repo.ReadArtifact(ctx, sessiontree.ArtifactReadRequest{
		ParentThreadID: parentThreadID, ThreadID: threadID, ArtifactID: ref.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if content.Ref != ref || content.Text != text {
		t.Fatalf("forked artifact content=%#v, want ref=%#v text=%q", content, ref, text)
	}
}

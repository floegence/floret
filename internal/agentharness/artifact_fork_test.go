package agentharness

import (
	"context"
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

package runtime

import (
	"context"
	"errors"
	"testing"
)

func TestReadHostsListCanonicalPendingToolSettlementTargets(t *testing.T) {
	ctx := context.Background()
	store := newMemoryStore()
	host, err := newTestHost(t, providerHostOptions{
		Config: runtimeConfig{Provider: "fake", Model: "fake-model", SystemPrompt: "test"},
		store:  store,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, testCreateThreadRequest("root")); err != nil {
		t.Fatal(err)
	}
	seedRuntimePendingToolCompletionTargetOnRepo(t, store.repo, "root")

	rootRead, err := mustTestCapabilities(t, store).read.NewHost(ctx, "root")
	if err != nil {
		t.Fatal(err)
	}
	targets, err := rootRead.ListPendingToolSettlementTargets(ctx, "root")
	if err != nil {
		t.Fatal(err)
	}
	wantRoot := PendingToolSettlementTarget{
		ThreadID: "root", TurnID: "turn-pending", RunID: "run-pending",
		ToolCallID: "exec-1", ToolName: "terminal.exec", Handle: "terminal:job:123",
	}
	if len(targets) != 1 || targets[0] != wantRoot {
		t.Fatalf("root targets = %#v, want %#v", targets, wantRoot)
	}

	publishTestSubAgentFixture(t, ctx, store, "publish-child", "root", "child", "")
	seedRuntimePendingToolCompletionTargetOnRepo(t, store.repo, "child")
	childRead := newTestSubAgentReadHost(t, store, "root")
	childTargets, err := childRead.ListPendingToolSettlementTargets(ctx, listSubAgentPendingToolSettlementTargetsRequest{
		ParentThreadID: "root",
		ChildThreadID:  "child",
	})
	if err != nil {
		t.Fatal(err)
	}
	wantChild := wantRoot
	wantChild.ThreadID = "child"
	if len(childTargets) != 1 || childTargets[0] != wantChild {
		t.Fatalf("child targets = %#v, want %#v", childTargets, wantChild)
	}

	if _, err := childRead.ListPendingToolSettlementTargets(ctx, listSubAgentPendingToolSettlementTargetsRequest{
		ParentThreadID: "other",
		ChildThreadID:  "child",
	}); err == nil {
		t.Fatal("parent-bound child read accepted a different parent")
	}
	if _, err := childRead.ListPendingToolSettlementTargets(ctx, listSubAgentPendingToolSettlementTargetsRequest{
		ParentThreadID: "root",
		ChildThreadID:  "root",
	}); !errors.Is(err, ErrSubAgentNotFound) {
		t.Fatalf("root-as-child error = %v, want ErrSubAgentNotFound", err)
	}
}

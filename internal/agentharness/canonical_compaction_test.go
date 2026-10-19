package agentharness

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/engine"
	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/session/compaction"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/internal/testing/harness"
	"github.com/floegence/floret/v7/tools"
)

type canonicalSummary struct{}

func (canonicalSummary) GenerateSummary(context.Context, compaction.Preparation) (string, error) {
	return "Earlier inspection is complete. Continue with retained results.", nil
}

func TestCanonicalEngineMessagesSurviveRepeatedAutomaticCompactionAndReopen(t *testing.T) {
	repo := sessiontree.NewMemoryRepo()
	if _, err := repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.AcceptTurn(t.Context(), sessiontree.AcceptTurnRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", RequestFingerprint: "canonical", InputRequestFingerprint: "canonical", Input: session.Message{Role: session.User, Content: "work"}}); err != nil {
		t.Fatal(err)
	}
	thread := &Thread{id: "thread"}
	registry := tools.NewRegistry(tools.Define[map[string]any](tools.Definition{Name: "inspect", ReadOnly: true, InputSchema: tools.StrictObject(nil, nil), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, nil, nil, func(context.Context, tools.Invocation[map[string]any]) (tools.Result, error) {
		return tools.Result{Text: strings.Repeat("retained result ", 250)}, nil
	}))
	providerSteps := [][]provider.StreamEvent{}
	for i := 0; i < 6; i++ {
		providerSteps = append(providerSteps, harness.Step(harness.Text(strings.Repeat("observed fact ", 500)), harness.Tool(fmt.Sprint("call-", i), "inspect", "{}"), harness.Usage(provider.Usage{InputTokens: 4600, Available: true, Source: provider.UsageNative, InputTokensReported: true, InputTokenScope: "request_input"}), harness.DoneReason("tool_calls")))
	}
	providerSteps = append(providerSteps, harness.Step(harness.Text("finished"), harness.Done()))
	scripted := harness.NewScriptedProvider(providerSteps...)
	thread.harness = New(Options{Repo: repo, Provider: scripted, ProviderName: "test", Model: "model", Tools: registry, CompactionGenerator: canonicalSummary{}, SystemPrompt: "Inspect."})
	for i := 0; i < 3; i++ {
		if _, err := sessiontree.AppendRunMessage(t.Context(), repo, "thread", "turn", "run", session.Message{Role: session.Assistant, Content: strings.Repeat("historical fact ", 400)}); err != nil {
			t.Fatal(err)
		}
	}
	path, _ := repo.Path(t.Context(), "thread", "")
	history, err := sessiontree.BuildContextChecked(path, sessiontree.ContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	projection := &turnProjection{ctx: t.Context(), thread: thread, turnID: "turn", runID: "run"}
	policy := contextpolicy.Policy{ContextWindowTokens: 5000, ReservedOutputTokens: 500, ReservedSummaryTokens: 300, CompactedContextTargetTokens: 1500, RecentTailTokens: 500, RecentUserTokens: 50}
	if err := thread.appendContextPolicyEvent(t.Context(), "turn", "run", "test", "model", policy); err != nil {
		t.Fatal(err)
	}
	eng, err := engine.New(engine.Config{Provider: scripted, Tools: registry, SystemPrompt: "Inspect.", Compactor: &durableCompactionManager{thread: thread, turnID: "turn"}, Options: engine.Options{RunID: "run", ThreadID: "thread", TurnID: "turn", ProviderName: "test", Model: "model", ContextPolicy: policy, DuplicateToolLimit: 100, CommitProviderContext: projection.commitProviderContext, CommitMessages: projection.commitMessages}})
	if err != nil {
		t.Fatal(err)
	}
	eng.SetSink(projection)
	result := eng.RunTurn(t.Context(), engine.RunInput{RunID: "run", ThreadID: "thread", TurnID: "turn", PromptScopeID: "thread", History: history})
	if result.Err != nil {
		t.Fatalf("run status=%s: %v", result.Status, result.Err)
	}
	if result.Metrics.Compactions < 2 {
		t.Fatalf("compactions=%d, want repeated automatic compaction", result.Metrics.Compactions)
	}
	if err := projection.Flush(); err != nil {
		t.Fatal(err)
	}
	path, _ = repo.Path(t.Context(), "thread", "")
	canonical, err := sessiontree.BuildContextChecked(path, sessiontree.ContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(canonical, result.Messages) {
		t.Fatal("live Engine context differs from canonical journal projection")
	}
	if err := session.ValidateToolHistory(canonical); err != nil {
		t.Fatal(err)
	}
	encoded, err := repo.EncodeMemoryState()
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := sessiontree.DecodeMemoryState(encoded, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	path, _ = reopened.Path(t.Context(), "thread", "")
	restored, err := sessiontree.BuildContextChecked(path, sessiontree.ContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(canonical, restored) {
		t.Fatal("reopen changed canonical message identities or content")
	}
	if len(scripted.Requests) != 7 {
		t.Fatalf("provider requests=%d", len(scripted.Requests))
	}
}

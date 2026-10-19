package engine_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/internal/engine"
	"github.com/floegence/floret/v7/internal/event"
	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/provider/cache"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
	"github.com/floegence/floret/v7/internal/testing/harness"
)

func TestProviderStateSurfaceReusePolicy(t *testing.T) {
	for _, policy := range []provider.StateReusePolicy{provider.StateReuseExactSurface, provider.StateReuseCanonicalHistory} {
		t.Run(string(policy), func(t *testing.T) {
			completed := &provider.State{Kind: "native", ID: "first", ReusePolicy: policy}
			p := harness.NewScriptedProvider(harness.Step(harness.Text("part"), provider.StreamEvent{Type: provider.Done, Reason: "length", ResponseState: completed}), harness.Step(harness.Text("done"), harness.Done()))
			e := newTestEngine(p, &event.Recorder{})
			e.Options.ToolSurfaceProvider = func(_ context.Context, request engine.ToolSurfaceRequest) (engine.ToolSurface, error) {
				prompt := "read only"
				if request.Step >= 2 {
					prompt = "permission refreshed"
				}
				return engine.ToolSurface{RefreshProviderSurface: true, SystemPrompt: prompt, Epoch: prompt}, nil
			}
			result := e.Run(t.Context(), "inspect")
			if result.Err != nil || len(p.Requests) != 2 {
				t.Fatalf("result=%+v requests=%d", result, len(p.Requests))
			}
			state := p.Requests[1].PreviousState
			if policy == provider.StateReuseCanonicalHistory {
				if state == nil || state.ID != "first" {
					t.Fatal("native history lost on permission refresh")
				}
			} else if state != nil || result.ProviderState != nil {
				t.Fatal("exact-surface continuation crossed changed surface")
			}
		})
	}
}

func TestNativeHistoryStillInvalidatesAcrossModelAndAccountChanges(t *testing.T) {
	for _, change := range []string{"model", "account"} {
		t.Run(change, func(t *testing.T) {
			prompt := cache.NewMemoryStore()
			first := newTestEngine(harness.NewScriptedProvider(harness.Step(harness.Text("one"), harness.Done())), &event.Recorder{})
			first.Prompt = prompt
			first.Options.StateCompatibilityKey = "account:first"
			result := first.Run(t.Context(), "first")
			if result.Err != nil {
				t.Fatal(result.Err)
			}
			p := harness.NewScriptedProvider(harness.Step(harness.Text("two"), harness.Done()))
			second := newTestEngine(p, &event.Recorder{})
			second.Prompt = prompt
			second.Options.RunID = "second"
			second.Options.PromptScopeID = "run"
			second.Options.StateCompatibilityKey = "account:first"
			if change == "model" {
				second.Options.Model = "different"
			} else {
				second.Options.StateCompatibilityKey = "account:second"
			}
			second.Options.PreviousProviderState = &provider.State{Kind: "native", ID: "old", ReusePolicy: provider.StateReuseCanonicalHistory}
			if err := second.Store.AppendTranscript("second", result.Messages...); err != nil {
				t.Fatal(err)
			}
			if got := second.Run(t.Context(), "second"); got.Err != nil {
				t.Fatal(got.Err)
			}
			if p.Requests[0].PreviousState != nil {
				t.Fatal("native state crossed identity change")
			}
		})
	}
}

func TestProviderStatePromotesOnlyAfterCanonicalCommit(t *testing.T) {
	failed := errors.New("canonical transaction failed")
	p := harness.NewScriptedProvider(harness.Step(harness.Text("complete"), provider.StreamEvent{Type: provider.Done, Reason: "stop", ResponseState: &provider.State{Kind: "native", ID: "uncommitted"}}))
	e := newTestEngine(p, &event.Recorder{})
	e.Options.PreviousProviderState = &provider.State{Kind: "native", ID: "valid"}
	e.Options.CommitProviderContext = func(context.Context, *provider.State, engine.ResponseOutput, []session.Message, bool) (engine.ProviderContextCommitResult, error) {
		return engine.ProviderContextCommitResult{}, failed
	}
	result := e.Run(t.Context(), "inspect")
	if !errors.Is(result.Err, failed) || result.ProviderState == nil || result.ProviderState.ID != "valid" || result.ProviderStateFresh {
		t.Fatalf("uncommitted state replaced valid state: %+v", result)
	}
	for _, message := range result.Messages {
		if message.Content == "complete" {
			t.Fatal("uncommitted provider output entered the active model context")
		}
	}
}

func TestUnknownProviderStateReusePolicyFailsExplicitly(t *testing.T) {
	p := harness.NewScriptedProvider(harness.Step(harness.Text("complete"), provider.StreamEvent{Type: provider.Done, Reason: "stop", ResponseState: &provider.State{Kind: "native", ID: "invalid", ReusePolicy: "unknown"}}))
	e := newTestEngine(p, &event.Recorder{})
	if result := e.Run(t.Context(), "inspect"); result.Err == nil || result.ProviderState != nil {
		t.Fatalf("unknown policy accepted: %+v", result)
	}
}

type compactAfterFirstOutput struct{}

func (compactAfterFirstOutput) PollManualCompaction(_ context.Context, request engine.ManualCompactionPollRequest) (engine.ManualCompactionRequest, bool, error) {
	return engine.ManualCompactionRequest{RequestID: "compact", Source: "fixture"}, request.Step == 2, nil
}

func TestCompactionThenProviderFailureCannotRestorePreCompactionState(t *testing.T) {
	p := harness.NewScriptedProvider(harness.Step(harness.Text("first"), provider.StreamEvent{Type: provider.Done, Reason: "length", ResponseState: &provider.State{Kind: "native", ID: "before", ReusePolicy: provider.StateReuseCanonicalHistory}}))
	p.Errs[2] = errors.New("network unavailable after compaction")
	e := newTestEngine(p, &event.Recorder{})
	e.Options.ManualCompactions = compactAfterFirstOutput{}
	e.Options.ContextPolicy = contextpolicy.Policy{ContextWindowTokens: 256000, CompactedContextTargetTokens: 2000, RecentTailTokens: 100, RecentUserTokens: 100, ReservedSummaryTokens: 1000}
	e.Compactor = engine.LocalCompactionManager{Generator: boundedSummaryFixture{}}
	for i := 0; i < 6; i++ {
		role := session.User
		if i%2 == 1 {
			role = session.Assistant
		}
		if err := e.Store.AppendTranscript("run", session.Message{Role: role, Content: strings.Repeat("durable history ", 800)}); err != nil {
			t.Fatal(err)
		}
	}
	result := e.Run(t.Context(), "continue")
	if result.Status != engine.Failed || result.Metrics.Compactions != 1 || result.ProviderState != nil || result.ProviderStateFresh {
		t.Fatalf("old state survived installed compaction: status=%s compactions=%d state=%+v err=%v", result.Status, result.Metrics.Compactions, result.ProviderState, result.Err)
	}
	if len(p.Requests) != 2 || p.Requests[1].PreviousState != nil {
		t.Fatal("compacted request replayed pre-compaction native state")
	}
}

func TestCommittedOutputNotificationFailureDoesNotBecomeUncommitted(t *testing.T) {
	notification := errors.New("subscriber unavailable")
	p := harness.NewScriptedProvider(harness.Step(harness.Text("committed"), provider.StreamEvent{Type: provider.Done, Reason: "stop", ResponseState: &provider.State{Kind: "native", ID: "saved"}}))
	e := newTestEngine(p, &event.Recorder{})
	commits := 0
	e.Options.CommitProviderContext = func(_ context.Context, _ *provider.State, output engine.ResponseOutput, _ []session.Message, clear bool) (engine.ProviderContextCommitResult, error) {
		if clear {
			return engine.ProviderContextCommitResult{}, nil
		}
		commits++
		message := output.Items[0].Message
		message.EntryID = "canonical-output"
		return engine.ProviderContextCommitResult{Messages: []session.Message{message}, NotificationErr: notification}, nil
	}
	result := e.Run(t.Context(), "inspect")
	if !errors.Is(result.Err, notification) || commits != 1 || len(result.UncommittedOutput.Items) != 0 || result.ProviderState == nil || result.ProviderState.ID != "saved" {
		t.Fatalf("result=%+v commits=%d", result, commits)
	}
	if len(result.Messages) != 2 || result.Messages[1].EntryID != "canonical-output" {
		t.Fatalf("committed context=%+v", result.Messages)
	}
}

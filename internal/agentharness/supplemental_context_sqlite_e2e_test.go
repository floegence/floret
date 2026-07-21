package agentharness

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/floegence/floret/internal/engine"
	"github.com/floegence/floret/internal/provider"
	"github.com/floegence/floret/internal/provider/cache"
	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/session/compaction"
	"github.com/floegence/floret/internal/session/contextpolicy"
	"github.com/floegence/floret/internal/sessiontree"
	"github.com/floegence/floret/internal/storage/sqlite"
	scriptharness "github.com/floegence/floret/internal/testing/harness"
)

func TestSQLiteSupplementalContextStaysEphemeralAcrossCompactionReopenForkAndNextTurn(t *testing.T) {
	const (
		supplementalSecret = "SUPPLEMENTAL-SECRET-sqlite-e2e-4d27"
		referenceDisplay   = "PUBLIC-REFERENCE-sqlite-e2e-9a61"
	)
	references := []session.MessageReference{{
		ReferenceID: "context:0",
		Kind:        session.MessageReferenceText,
		Label:       "Selected output",
		Text:        referenceDisplay,
	}}
	supplemental := []engine.TurnSupplementalContextItem{{
		Kind:      "linked_context",
		Title:     "Selected output",
		Text:      supplementalSecret,
		Metadata:  map[string]string{"sensitive_locator": supplementalSecret},
		Sensitive: true,
	}}

	cases := []struct {
		name        string
		wantTrigger compaction.Trigger
		configure   func(*testing.T, *AgentHarness, *scriptharness.ScriptedProvider)
		provider    func() (provider.Provider, *scriptharness.ScriptedProvider)
		seed        bool
	}{
		{
			name:        "pre-request",
			wantTrigger: compaction.TriggerPreRequest,
			provider: func() (provider.Provider, *scriptharness.ScriptedProvider) {
				scripted := scriptharness.NewScriptedProvider(scriptharness.Step(
					scriptharness.Text("pre-request done"),
					provider.StreamEvent{Type: provider.Done, Reason: "stop", ResponseState: sensitiveProviderState(supplementalSecret)},
				))
				return &estimatingHarnessProvider{
					Provider: scripted,
					estimates: []provider.TokenEstimate{
						{PrefixTokens: 100, MessageTokens: 900, ToolDefinitionTokens: 100, EstimatedInputTokens: 1100, Source: "supplemental_e2e", Method: provider.TokenEstimateProviderRenderedPayload, Confidence: provider.EstimateConservative},
						{PrefixTokens: 100, MessageTokens: 780, ToolDefinitionTokens: 100, EstimatedInputTokens: 980, Source: "supplemental_e2e", Method: provider.TokenEstimateProviderRenderedPayload, Confidence: provider.EstimateConservative},
						{PrefixTokens: 100, MessageTokens: 200, ToolDefinitionTokens: 100, EstimatedInputTokens: 400, Source: "supplemental_e2e", Method: provider.TokenEstimateProviderRenderedPayload, Confidence: provider.EstimateConservative},
					},
				}, scripted
			},
			configure: func(_ *testing.T, harness *AgentHarness, _ *scriptharness.ScriptedProvider) {
				harness.options.TurnPolicy.ContextPolicy = contextpolicy.Policy{
					ContextWindowTokens: 1000, ReservedOutputTokens: 100, ReservedSummaryTokens: 80,
					RecentTailTokens: 80, RecentUserTokens: 60,
				}
			},
			seed: true,
		},
		{
			name:        "post-response",
			wantTrigger: compaction.TriggerPostResponse,
			provider: func() (provider.Provider, *scriptharness.ScriptedProvider) {
				scripted := scriptharness.NewScriptedProvider(
					scriptharness.Step(
						scriptharness.Usage(provider.Usage{InputTokens: 950, WindowInputTokens: 950, OutputTokens: 10, Source: provider.UsageNative, Available: true}),
						scriptharness.Tool("read-1", "read", `{"value":"large"}`),
						scriptharness.DoneReason("tool_calls"),
					),
					scriptharness.Step(
						scriptharness.Text("post-response done"),
						provider.StreamEvent{Type: provider.Done, Reason: "stop", ResponseState: sensitiveProviderState(supplementalSecret)},
					),
				)
				return &estimatingHarnessProvider{
					Provider: scripted,
					estimates: []provider.TokenEstimate{
						{PrefixTokens: 40, MessageTokens: 120, ToolDefinitionTokens: 20, EstimatedInputTokens: 180, Source: "supplemental_e2e", Method: provider.TokenEstimateProviderRenderedPayload, Confidence: provider.EstimateConservative},
						{PrefixTokens: 40, MessageTokens: 1000, ToolDefinitionTokens: 20, EstimatedInputTokens: 1060, Source: "supplemental_e2e", Method: provider.TokenEstimateProviderRenderedPayload, Confidence: provider.EstimateConservative},
						{PrefixTokens: 40, MessageTokens: 220, ToolDefinitionTokens: 20, EstimatedInputTokens: 280, Source: "supplemental_e2e", Method: provider.TokenEstimateProviderRenderedPayload, Confidence: provider.EstimateConservative},
					},
				}, scripted
			},
			configure: func(_ *testing.T, harness *AgentHarness, _ *scriptharness.ScriptedProvider) {
				harness.options.TurnPolicy.ContextPolicy = contextpolicy.Policy{
					ContextWindowTokens: 1000, ReservedOutputTokens: 100, ReservedSummaryTokens: 80,
					RecentTailTokens: 60, RecentUserTokens: 40,
				}
				mustRegister(harness.options.Tools, stringTool("read", func(context.Context, string) (string, error) {
					return strings.Repeat("large output ", 300), nil
				}))
			},
			seed: true,
		},
		{
			name:        "provider-overflow",
			wantTrigger: compaction.TriggerOverflow,
			provider: func() (provider.Provider, *scriptharness.ScriptedProvider) {
				scripted := scriptharness.NewScriptedProvider(
					nil,
					scriptharness.Step(
						scriptharness.Text("overflow done"),
						provider.StreamEvent{Type: provider.Done, Reason: "stop", ResponseState: sensitiveProviderState(supplementalSecret)},
					),
				)
				scripted.Errs[1] = provider.ErrContextOverflow
				return scripted, scripted
			},
			configure: func(_ *testing.T, harness *AgentHarness, _ *scriptharness.ScriptedProvider) {
				harness.options.TurnPolicy.ContextPolicy = contextpolicy.Policy{
					ContextWindowTokens: 8000, ReservedOutputTokens: 512, ReservedSummaryTokens: 512,
					RecentTailTokens: 256,
				}
			},
			seed: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "floret.db")
			repo, err := sqlite.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			closed := false
			t.Cleanup(func() {
				if !closed {
					_ = repo.Close()
				}
			})

			turnProvider, scripted := tc.provider()
			harness := newTestHarness(turnProvider, repo, repo)
			harness.options.CompactionGenerator = compaction.ExtractiveSummaryGenerator{}
			harness.options.StateCompatibilityKey = "responses:supplemental-e2e"
			tc.configure(t, harness, scripted)
			thread, err := harness.StartThread(ctx, StartThreadOptions{ThreadID: "source"})
			if err != nil {
				t.Fatal(err)
			}
			if tc.seed {
				seedCompactionHistory(t, ctx, repo)
			}

			result, err := thread.Run(ctx, "", RunOptions{
				RunID: "sensitive-run", TurnID: "sensitive-turn",
				References: references, SupplementalContext: supplemental,
			})
			if err != nil {
				t.Fatal(err)
			}
			if result.Status != engine.Completed || result.Metrics.Compactions != 1 {
				t.Fatalf("sensitive turn result=%#v", result)
			}
			snapshot, err := thread.Journal(ctx)
			if err != nil {
				t.Fatal(err)
			}
			assertCanonicalReferences(t, snapshot.Entries, "sensitive-turn", references)
			compactionEntry := latestEntry(snapshot.Entries, sessiontree.EntryCompaction)
			if compactionEntry.ID == "" || compactionEntry.CompactionTrigger != string(tc.wantTrigger) {
				t.Fatalf("compaction entry=%#v, want trigger %q", compactionEntry, tc.wantTrigger)
			}
			assertJSONOmits(t, "source journal", snapshot.Entries, supplementalSecret)
			assertJSONOmits(t, "compaction checkpoint", compactionEntry, referenceDisplay)
			assertSensitiveTurnProviderProjection(t, scripted.Requests, supplementalSecret, referenceDisplay)
			assertPromptStoreOmits(t, ctx, repo, "source", supplementalSecret, referenceDisplay)
			if _, err := repo.ProviderState(ctx, "source"); !errors.Is(err, sessiontree.ErrProviderStateNotFound) {
				t.Fatalf("supplemental turn persisted provider state: %v", err)
			}

			if err := repo.Close(); err != nil {
				t.Fatal(err)
			}
			closed = true
			reopened, err := sqlite.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = reopened.Close() })

			nextProvider := scriptharness.NewScriptedProvider(scriptharness.Step(
				scriptharness.Text("next turn done"),
				provider.StreamEvent{Type: provider.Done, Reason: "stop", ResponseState: &provider.State{
					Kind: "responses", ID: "safe-next-state", Attributes: map[string]string{"scope": "fork"},
				}},
			))
			reopenedHarness := newTestHarness(nextProvider, reopened, reopened)
			reopenedHarness.options.CompactionGenerator = compaction.ExtractiveSummaryGenerator{}
			reopenedHarness.options.StateCompatibilityKey = "responses:supplemental-e2e"
			reopenedHarness.options.TurnPolicy.ContextPolicy = contextpolicy.Policy{
				ContextWindowTokens: 256000, ReservedOutputTokens: 25600, ReservedSummaryTokens: 2560,
			}

			reopenedSource, err := reopenedHarness.ResumeThread(ctx, "source", ResumeOptions{})
			if err != nil {
				t.Fatal(err)
			}
			reopenedSnapshot, err := reopenedSource.Journal(ctx)
			if err != nil {
				t.Fatal(err)
			}
			assertCanonicalReferences(t, reopenedSnapshot.Entries, "sensitive-turn", references)
			assertJSONOmits(t, "reopened source journal", reopenedSnapshot.Entries, supplementalSecret)
			forked, err := reopenedHarness.ForkThread(ctx, ForkOptions{
				OperationID: "fork-sensitive", SourceThreadID: "source",
				EntryID: reopenedSnapshot.Path[len(reopenedSnapshot.Path)-1].ID, NewThreadID: "fork",
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := forked.Run(ctx, "continue safely", RunOptions{RunID: "next-run", TurnID: "next-turn"}); err != nil {
				t.Fatal(err)
			}

			forkSnapshot, err := forked.Journal(ctx)
			if err != nil {
				t.Fatal(err)
			}
			assertCanonicalReferences(t, forkSnapshot.Entries, "", references)
			assertJSONOmits(t, "fork journal", forkSnapshot.Entries, supplementalSecret)
			if len(nextProvider.Requests) != 1 {
				t.Fatalf("next turn provider requests=%d, want 1", len(nextProvider.Requests))
			}
			nextRequest := nextProvider.Requests[0]
			assertJSONOmits(t, "next provider history", nextRequest.Messages, supplementalSecret, referenceDisplay)
			assertJSONOmits(t, "next raw plan", nextRequest.RawPlan, supplementalSecret, referenceDisplay)
			if nextRequest.EphemeralUser != nil || nextRequest.PreviousState != nil {
				t.Fatalf("fork next request reused ephemeral context or source state: %#v", nextRequest)
			}
			assertPromptStoreOmits(t, ctx, reopened, "source", supplementalSecret, referenceDisplay)
			assertPromptStoreOmits(t, ctx, reopened, "fork", supplementalSecret, referenceDisplay)
			state, err := reopened.ProviderState(ctx, "fork")
			if err != nil {
				t.Fatal(err)
			}
			if state.State.ID != "safe-next-state" {
				t.Fatalf("fork provider state=%#v", state)
			}
			assertJSONOmits(t, "fork provider state", state, supplementalSecret, referenceDisplay)
		})
	}
}

func sensitiveProviderState(secret string) *provider.State {
	return &provider.State{Kind: "responses", ID: "sensitive-state", Attributes: map[string]string{"secret": secret}}
}

func seedCompactionHistory(t *testing.T, ctx context.Context, repo sessiontree.Repo) {
	t.Helper()
	if _, err := sessiontree.AppendMessage(ctx, repo, "source", "seed", session.Message{
		Role: session.User, Content: "older " + strings.Repeat("alpha ", 300), EntryID: "seed-user",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessiontree.AppendMessage(ctx, repo, "source", "seed", session.Message{
		Role: session.Assistant, Content: "answer " + strings.Repeat("bravo ", 100), EntryID: "seed-assistant",
	}); err != nil {
		t.Fatal(err)
	}
}

func assertCanonicalReferences(t *testing.T, entries []sessiontree.Entry, turnID string, want []session.MessageReference) {
	t.Helper()
	matches := 0
	for _, entry := range entries {
		if entry.Type != sessiontree.EntryUserMessage ||
			turnID != "" && entry.TurnID != turnID ||
			turnID == "" && len(entry.Message.References) == 0 {
			continue
		}
		matches++
		if !reflect.DeepEqual(entry.Message.References, want) {
			t.Fatalf("canonical references=%#v, want %#v", entry.Message.References, want)
		}
	}
	if matches != 1 {
		t.Fatalf("canonical user entries with references=%d, want 1", matches)
	}
}

func assertSensitiveTurnProviderProjection(t *testing.T, requests []provider.Request, secret, referenceDisplay string) {
	t.Helper()
	if len(requests) == 0 {
		t.Fatal("sensitive turn did not reach provider")
	}
	for index, request := range requests {
		assertJSONOmits(t, "sensitive raw plan", request.RawPlan, secret, referenceDisplay)
		encoded, err := json.Marshal(request.Messages)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(encoded), secret) {
			t.Fatalf("sensitive provider request %d missed supplemental overlay: %s", index, encoded)
		}
		if strings.Contains(string(encoded), referenceDisplay) {
			t.Fatalf("sensitive provider request %d exposed canonical reference: %s", index, encoded)
		}
	}
}

func assertPromptStoreOmits(t *testing.T, ctx context.Context, store cache.Store, promptScopeID string, forbidden ...string) {
	t.Helper()
	segments, err := store.Segments(ctx, promptScopeID, "fake", "fake-model")
	if err != nil {
		t.Fatal(err)
	}
	requests, err := store.ProviderRequests(ctx, promptScopeID)
	if err != nil {
		t.Fatal(err)
	}
	responses, err := store.ProviderResponses(ctx, promptScopeID)
	if err != nil {
		t.Fatal(err)
	}
	assertJSONOmits(t, "prompt segments", segments, forbidden...)
	assertJSONOmits(t, "provider request ledger", requests, forbidden...)
	assertJSONOmits(t, "provider response ledger", responses, forbidden...)
}

func assertJSONOmits(t *testing.T, name string, value any, forbidden ...string) {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal %s: %v", name, err)
	}
	for _, value := range forbidden {
		if strings.Contains(string(raw), value) {
			t.Fatalf("%s contains %q: %s", name, value, raw)
		}
	}
}

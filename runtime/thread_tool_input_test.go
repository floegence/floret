package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/tools"
)

func TestToolRequestedInputPausesAndResumesAfterRestartWithoutReplay(t *testing.T) {
	testCompletedToolInputRestart(t, false)
}
func TestErrorToolRequestedInputPausesAndResumesAfterRestartWithoutReplay(t *testing.T) {
	testCompletedToolInputRestart(t, true)
}
func testCompletedToolInputRestart(t *testing.T, failed bool) {
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "input", StateCompatibilityKey: "test:input"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "inspect-1", Name: "inspect", Args: `{}`}}}, {Type: provider.EventDone, Reason: "tool_calls"}}},
		florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "resumed"}, {Type: provider.EventDone, Reason: "stop"}}},
	)
	var calls atomic.Int32
	inspect := tools.Define[map[string]any](tools.Definition{Name: "inspect", InputSchema: tools.StrictObject(map[string]any{}, []string{}), ReadOnly: true, Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, nil, nil,
		func(context.Context, tools.Invocation[map[string]any]) (tools.Result, error) {
			calls.Add(1)
			return tools.Result{IsError: failed, Text: strings.Repeat("x", 65537), OutputPolicy: &tools.OutputPolicy{VisibleMaxBytes: 1 << 20, ArchiveAboveBytes: 65536}, InputRequired: &tools.InputRequest{Summary: "Complete the external step", Questions: []tools.InputQuestion{
				{ID: "ready", Prompt: "Return control when ready", Kind: "select", Options: []string{"Continue", "Stop"}},
				{ID: "claim", Prompt: "Describe the current state", Kind: "write"},
			}}}, nil
		})
	agent, err := testAgent(gateway, WithAgentTools(inspect), WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, req EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
		return effect(ctx, EffectAuthorizationProof{EffectAttemptID: req.EffectAttemptID, RequestFingerprint: req.RequestFingerprint, ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, ToolCallID: req.ToolCallID, PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now().UTC()})
	})))
	if err != nil {
		t.Fatal(err)
	}
	path := t.TempDir() + "/input.db"
	open := func() (*Host, ThreadService) {
		t.Helper()
		host, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
		if err != nil {
			t.Fatal(err)
		}
		svc, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
		if err != nil {
			t.Fatal(err)
		}
		return host, svc
	}
	host, svc := open()
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	created, err := svc.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "send", Input: UserInput{Text: "inspect"}}); err != nil {
		t.Fatal(err)
	}
	waiting := waitThreadView(t, svc, created.ThreadID, func(v ThreadView) bool { return len(v.Interactions) == 1 || v.LastOutcome != nil })
	if len(waiting.Interactions) != 1 || waiting.Interactions[0].Resolved || len(gateway.Requests()) != 1 {
		t.Fatalf("tool did not stop model for input: interactions=%+v requests=%d outcome=%v", waiting.Interactions, len(gateway.Requests()), waiting.LastOutcome)
	}

	assertArchive := func() {
		t.Helper()
		entries, err := host.store.repo.Entries(t.Context(), created.ThreadID.String())
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		for _, entry := range entries {
			if entry.Message.ToolCallID != "inspect-1" || entry.Message.ToolResult == nil {
				continue
			}
			count++
			view := entry.Message.ToolResult
			expectedStatus := "success"
			if failed {
				expectedStatus = "error"
			}
			if view.Status != expectedStatus || view.Truncated || view.FullOutput == nil {
				t.Fatalf("result/archive lost: %+v", view)
			}
			full, err := host.store.repo.(sessiontree.ArtifactAuthorityRepo).ReadArtifact(t.Context(), sessiontree.ArtifactReadRequest{ThreadID: created.ThreadID.String(), ArtifactID: view.FullOutput.ID})
			digest := sha256.Sum256([]byte(strings.Repeat("x", 65537)))
			if err != nil || full.Text != strings.Repeat("x", 65537) || full.Ref.SHA256 != fmt.Sprintf("%x", digest) {
				t.Fatalf("archive changed: %v", err)
			}
		}
		if count != 1 {
			t.Fatalf("result count=%d", count)
		}
	}
	assertArchive()
	interaction := waiting.Interactions[0]
	assertMediaIdle := func(want bool) {
		t.Helper()
		inventory, err := host.MediaInventory(t.Context())
		if err != nil || inventory.Idle != want {
			t.Fatalf("media inventory idle=%v, want %v: %v", inventory.Idle, want, err)
		}
	}
	assertMediaIdle(false)
	if interaction.ToolCallID != "inspect-1" || interaction.Input == nil || interaction.Input.Summary != "Complete the external step" {
		t.Fatalf("input lost source identity: %+v", interaction)
	}
	if err := host.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	host, svc = open()
	restored, err := svc.View(t.Context(), created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	if len(restored.Interactions) != 1 || restored.Interactions[0].ID != interaction.ID || restored.Interactions[0].Resolved {
		t.Fatalf("input not restored: %+v", restored.Interactions)
	}
	assertArchive()
	answer := map[string]string{"ready": "Continue", "claim": "The external step is complete, please check."}
	assertMediaIdle(false)
	response := RespondInput{ThreadID: created.ThreadID, InteractionID: interaction.ID, RequestKey: "respond", Answers: []InteractionAnswer{{Input: answer}}}
	if _, err := svc.Respond(t.Context(), response); err != nil {
		t.Fatal(err)
	}
	final := waitThreadView(t, svc, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if final.Failure != nil || *final.LastOutcome != TurnOutcomeCompleted || calls.Load() != 1 {
		t.Fatalf("resume failed or replayed tool: failure=%+v outcome=%v calls=%d", final.Failure, final.LastOutcome, calls.Load())
	}
	if final.TurnID != waiting.TurnID {
		t.Fatal("input response created another turn")
	}
	if final.RunID == waiting.RunID || final.RunID == "" {
		t.Fatal("input response did not start a fresh run")
	}
	requests := gateway.Requests()
	assertMediaIdle(true)
	if len(requests) != 2 {
		t.Fatalf("requests=%d", len(requests))
	}
	var callCount, resultCount int
	var answerCount int
	for _, msg := range requests[1].Messages {
		var response struct {
			Type    string            `json:"type"`
			Answers map[string]string `json:"answers"`
		}
		if json.Unmarshal([]byte(msg.Text), &response) == nil && response.Type == "interaction_response" {
			answerCount++
			if response.Answers["ready"] != answer["ready"] || response.Answers["claim"] != answer["claim"] || len(response.Answers) != len(answer) {
				t.Fatalf("user statement was changed before reaching the provider: %#v", response.Answers)
			}
		}
		for _, call := range msg.ToolCalls {
			if call.ID == "inspect-1" {
				callCount++
			}
		}
		if msg.ToolResult != nil && msg.ToolResult.CallID == "inspect-1" {
			resultCount++
			if strings.HasPrefix(msg.ToolResult.Text, "ERROR: ") != failed {
				t.Fatal("completed error status changed")
			}
		}
	}

	if callCount != 1 || resultCount != 1 {
		t.Fatalf("tool history is unpaired: calls=%d results=%d", callCount, resultCount)
	}
	if answerCount != 1 {
		t.Fatalf("provider received %d canonical answers", answerCount)
	}
	if _, err := svc.Respond(t.Context(), response); err != nil {
		t.Fatal(err)
	}
	assertArchive()
	if calls.Load() != 1 || len(gateway.Requests()) != 2 {
		t.Fatal("duplicate response replayed the tool or provider continuation")
	}
}

func TestToolRequestedInputBatchWaitsForEveryAnswer(t *testing.T) {
	for _, mode := range []string{"continue", "restart", "cancel", "sibling_failure"} {
		t.Run(mode, func(t *testing.T) {
			gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "input", StateCompatibilityKey: "test:input"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
				florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "first", Name: "inspect", Args: `{}`}, {ID: "second", Name: "inspect", Args: `{}`}}}, {Type: provider.EventDone, Reason: "tool_calls"}}},
				florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: "resumed"}, {Type: provider.EventDone, Reason: "stop"}}})
			release := make(chan struct{})
			entered := make(chan struct{})
			firstReturned := make(chan struct{})
			defer func() {
				select {
				case <-release:
				default:
					close(release)
				}
			}()
			var calls atomic.Int32
			inspect := tools.Define[map[string]any](tools.Definition{Name: "inspect", InputSchema: tools.StrictObject(map[string]any{}, []string{}), ReadOnly: true, Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, nil, nil,
				func(ctx context.Context, inv tools.Invocation[map[string]any]) (tools.Result, error) {
					calls.Add(1)
					if inv.CallID == "second" {
						close(entered)
						select {
						case <-release:
						case <-ctx.Done():
							return tools.Result{}, ctx.Err()
						}
						if mode == "sibling_failure" {
							return tools.Result{InputRequired: &tools.InputRequest{}}, nil
						}
					}
					if inv.CallID == "first" {
						close(firstReturned)
					}
					return tools.Result{IsError: true, Text: "Result requires verification.", InputRequired: &tools.InputRequest{Summary: "Complete the external step", Questions: []tools.InputQuestion{{ID: "ready", Prompt: "Return control when ready", Kind: "select", Options: []string{"Continue", "Stop"}}, {ID: "confirm", Prompt: "Confirm the step", Kind: "select", Options: []string{"Done", "Cancel"}}}}}, nil
				})
			agent, err := testAgent(gateway, WithAgentTools(inspect), WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, req EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
				return effect(ctx, EffectAuthorizationProof{EffectAttemptID: req.EffectAttemptID, RequestFingerprint: req.RequestFingerprint, ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, ToolCallID: req.ToolCallID, PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now().UTC()})
			})))
			if err != nil {
				t.Fatal(err)
			}
			path := t.TempDir() + "/input.db"
			open := func() (*Host, ThreadService) {
				t.Helper()
				host, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
				if err != nil {
					t.Fatal(err)
				}
				svc, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
				if err != nil {
					t.Fatal(err)
				}
				return host, svc
			}
			host, svc := open()
			t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
			created, err := svc.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := svc.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "send", Input: UserInput{Text: "inspect both"}}); err != nil {
				t.Fatal(err)
			}
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("sibling did not start")
			}
			// A sibling is still executing. No input can be answered before the batch's
			// canonical waiting boundary, even when an earlier handler already returned.
			select {
			case <-firstReturned:
			case <-time.After(5 * time.Second):
				t.Fatal("first handler did not finish while sibling ran")
			}
			early, err := svc.View(t.Context(), created.ThreadID)
			if err != nil {
				t.Fatal(err)
			}
			if len(early.Interactions) != 0 || len(gateway.Requests()) != 1 {
				t.Fatalf("premature input or continuation: %+v", early.Interactions)
			}
			close(release)
			waiting := waitThreadView(t, svc, created.ThreadID, func(v ThreadView) bool { return len(v.Interactions) == 2 || v.LastOutcome != nil })
			if mode == "sibling_failure" {
				if waiting.Failure == nil || len(waiting.Interactions) != 0 || len(gateway.Requests()) != 1 {
					t.Fatalf("failed batch exposed input: %+v", waiting)
				}
				return
			}
			if len(waiting.Interactions) != 2 || len(gateway.Requests()) != 1 {
				t.Fatalf("batch did not wait: failure=%+v interactions=%+v", waiting.Failure, waiting.Interactions)
			}
			first, second := waiting.Interactions[0], waiting.Interactions[1]
			respond := func(id, key string, answers map[string]string) error {
				_, err := svc.Respond(t.Context(), RespondInput{ThreadID: created.ThreadID, InteractionID: id, RequestKey: RequestKey(key), Answers: []InteractionAnswer{{Input: answers}}})
				return err
			}
			for i, invalid := range []map[string]string{{"ready": "Continue"}, {"ready": "arbitrary", "confirm": "Done"}, {"ready": "Continue", "confirm": "Done", "unknown": "x"}} {
				if err := respond(first.ID, fmt.Sprintf("invalid-%d", i), invalid); err == nil {
					t.Fatalf("invalid answer accepted: %+v", invalid)
				}
			}
			answers := map[string]string{"ready": "Continue", "confirm": "Done"}
			if err := respond(first.ID, "first-answer", answers); err != nil {
				t.Fatal(err)
			}
			partial, err := svc.View(t.Context(), created.ThreadID)
			if err != nil {
				t.Fatal(err)
			}
			if partial.RunID != waiting.RunID || len(gateway.Requests()) != 1 {
				t.Fatal("first answer resumed unresolved sibling input")
			}
			if mode == "restart" {
				if err := host.Shutdown(context.Background()); err != nil {
					t.Fatal(err)
				}
				host, svc = open()
			}
			if mode == "cancel" {
				if _, err := svc.Cancel(t.Context(), CancelInput{ThreadID: created.ThreadID, RequestKey: "cancel"}); err != nil {
					t.Fatal(err)
				}
				stopped := waitThreadView(t, svc, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle })
				if stopped.LastOutcome == nil || *stopped.LastOutcome != TurnOutcomeCancelled {
					t.Fatalf("cancel outcome: %+v", stopped)
				}
				if err := respond(second.ID, "after-cancel", answers); err == nil {
					t.Fatal("canceled interaction accepted answer")
				}
				if len(gateway.Requests()) != 1 {
					t.Fatal("cancel dispatched provider")
				}
				return
			}
			if err := respond(second.ID, "second-answer", answers); err != nil {
				t.Fatal(err)
			}
			final := waitThreadView(t, svc, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
			if final.Failure != nil || *final.LastOutcome != TurnOutcomeCompleted || calls.Load() != 2 {
				t.Fatalf("batch resume failed or replayed: %+v calls=%d", final, calls.Load())
			}
			requests := gateway.Requests()
			if len(requests) != 2 {
				t.Fatalf("requests=%d", len(requests))
			}
			responses := 0
			for _, msg := range requests[1].Messages {
				if msg.Role == "user" && strings.Contains(msg.Text, "Continue") {
					responses++
				}
			}
			if responses != 2 {
				t.Fatalf("provider lost input responses: %d", responses)
			}
		})
	}
}

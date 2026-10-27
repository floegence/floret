package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/tools"
	"modernc.org/sqlite"
)

func TestGoalCompactionContinuesInAdmittedTurn(t *testing.T) {
	for _, withHistory := range []bool{false, true} {
		name := "noop"
		var steps []florettest.Step
		if withHistory {
			name = "installed"
			steps = append(steps, compactionTestStep("ack"), compactionTestStep("ack"), compactionTestStep("ack"), compactionTestStep("The launch code is CERULEAN-739. Work in /work/first."))
		}
		steps = append(steps, parentEventDone(), goalCall("review", ` {"operation":"review","expected_version":1,"outcome":"completed","evidence":"The result is verified."}`), parentEventDone())
		t.Run(name, func(t *testing.T) {
			gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "compact", StateCompatibilityKey: "test:compact"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, steps...)
			_, service := testThreadServiceWithAgent(t, compactionTestAgent(t, gateway, WithAgentGoals()))
			thread, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
			if err != nil {
				t.Fatal(err)
			}
			if withHistory {
				seedCompactionHistory(t, service, thread.ThreadID)
			}
			if _, err = service.(ThreadGoalController).SetGoal(t.Context(), SetGoalInput{ThreadID: thread.ThreadID, RequestKey: "goal", Text: "Deliver the verified result"}); err != nil {
				t.Fatal(err)
			}
			var input CompactContextInput
			raw, _ := json.Marshal(map[string]any{"thread_id": thread.ThreadID, "request_key": "compact", "continue_goal": true})
			if err = json.Unmarshal(raw, &input); err != nil {
				t.Fatal(err)
			}
			admitted, err := service.(ThreadContextCompactor).CompactContext(t.Context(), input)
			if err != nil {
				t.Fatal(err)
			}
			if admitted.TurnKind != "goal_continuation" {
				t.Fatalf("kind=%q, want goal_continuation", admitted.TurnKind)
			}
			final := waitGoalCompactionView(t, service, thread.ThreadID, func(v ThreadView) bool {
				return v.TurnID == admitted.TurnID && v.Activity == ThreadActivityIdle && v.LastOutcome != nil
			})
			if final.Failure != nil || final.Goal == nil || final.Goal.Status != "completed" {
				t.Fatalf("goal=%+v failure=%+v", final.Goal, final.Failure)
			}
			for _, item := range final.Items {
				if item.TurnID == admitted.TurnID && item.Kind == ThreadItemUser {
					t.Fatal("continuation fabricated user input")
				}
			}
			if len(gateway.Requests()) != len(steps) {
				t.Fatalf("requests=%d want %d", len(gateway.Requests()), len(steps))
			}
			replay, err := service.(ThreadContextCompactor).CompactContext(t.Context(), input)
			if err != nil || replay.TurnID != admitted.TurnID || len(gateway.Requests()) != len(steps) {
				t.Fatalf("replay=%+v err=%v", replay, err)
			}
			raw, _ = json.Marshal(map[string]any{"thread_id": thread.ThreadID, "request_key": "compact", "continue_goal": false})
			if err = json.Unmarshal(raw, &input); err != nil {
				t.Fatal(err)
			}
			if _, err = service.(ThreadContextCompactor).CompactContext(t.Context(), input); err == nil {
				t.Fatal("changed intent reused request identity")
			}
		})
	}
}

func TestGoalCompactionPreservesPureIntentAndReplay(t *testing.T) {
	for _, active := range []bool{false, true} {
		for _, continueGoal := range []bool{false, true} {
			if active && continueGoal {
				continue
			}
			service, g := goalService(t)
			th, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
			if err != nil {
				t.Fatal(err)
			}
			if active {
				if _, err = service.(ThreadGoalController).SetGoal(t.Context(), SetGoalInput{ThreadID: th.ThreadID, RequestKey: "goal", Text: "Finish work"}); err != nil {
					t.Fatal(err)
				}
			}
			in := CompactContextInput{ThreadID: th.ThreadID, RequestKey: "compact", ContinueGoal: continueGoal}
			admitted, err := service.(ThreadContextCompactor).CompactContext(t.Context(), in)
			if err != nil {
				t.Fatal(err)
			}
			final := waitGoalCompactionView(t, service, th.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
			if final.Failure != nil || admitted.TurnKind != TurnKindContextCompaction || len(g.Requests()) != 0 {
				t.Fatalf("admitted=%+v final=%+v", admitted, final)
			}
			if !active {
				if _, err = service.(ThreadGoalController).SetGoal(t.Context(), SetGoalInput{ThreadID: th.ThreadID, RequestKey: "late-goal", Text: "Do not start from replay"}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err = service.(ThreadContextCompactor).CompactContext(t.Context(), in); err != nil {
				t.Fatal(err)
			}
			if len(g.Requests()) != 0 {
				t.Fatal("replay re-evaluated Goal")
			}
		}
	}
}

func TestGoalCompactionFailureAndRetry(t *testing.T) {
	for _, failSummary := range []bool{false, true} {
		t.Run(fmt.Sprint("summary=", failSummary), func(t *testing.T) {
			steps := []florettest.Step{compactionTestStep("ack"), compactionTestStep("ack"), compactionTestStep("ack")}
			if failSummary {
				steps = append(steps, florettest.Step{ReturnError: errors.New("summary failed")})
			}
			steps = append(steps, compactionTestStep("The launch code is CERULEAN-739."))
			if !failSummary {
				steps = append(steps, florettest.Step{ReturnError: errors.New("business failed")})
			}
			steps = append(steps, goalCall("review", `{"operation":"review","expected_version":1,"outcome":"completed","evidence":"Verified after retry."}`), parentEventDone())
			g := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "compact", StateCompatibilityKey: "test:compact"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, steps...)
			policy := DefaultProviderRetryPolicy()
			policy.MaxRetries = 0
			policy.MaxTimeoutRetries = 0
			_, service := testThreadServiceWithAgent(t, compactionTestAgent(t, g, WithAgentGoals(), WithAgentProviderRetryPolicy(policy)))
			th, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
			if err != nil {
				t.Fatal(err)
			}
			seedCompactionHistory(t, service, th.ThreadID)
			if _, err = service.(ThreadGoalController).SetGoal(t.Context(), SetGoalInput{ThreadID: th.ThreadID, RequestKey: "goal", Text: "Finish verified work"}); err != nil {
				t.Fatal(err)
			}
			admitted, err := service.(ThreadContextCompactor).CompactContext(t.Context(), CompactContextInput{ThreadID: th.ThreadID, RequestKey: "compact", ContinueGoal: true})
			if err != nil {
				t.Fatal(err)
			}
			failed := waitGoalCompactionView(t, service, th.ThreadID, func(v ThreadView) bool {
				return v.TurnID == admitted.TurnID && v.Activity == ThreadActivityIdle && v.Failure != nil
			})
			if failed.Goal.Status != "active" {
				t.Fatal(failed.Goal)
			}
			wantBefore := 5
			if failSummary {
				wantBefore = 4
			}
			if len(g.Requests()) != wantBefore {
				t.Fatalf("requests after failure=%d want %d", len(g.Requests()), wantBefore)
			}
			retry, err := service.Retry(t.Context(), RetryInput{ThreadID: th.ThreadID, SourceTurnID: admitted.TurnID, RequestKey: "retry"})
			if err != nil {
				t.Fatal(err)
			}
			final := waitGoalCompactionView(t, service, th.ThreadID, func(v ThreadView) bool {
				return v.TurnID == retry.TurnID && v.Activity == ThreadActivityIdle && v.LastOutcome != nil
			})
			if final.Failure != nil || final.Goal.Status != "completed" || len(g.Requests()) != len(steps) {
				t.Fatalf("goal=%+v failure=%+v requests=%d", final.Goal, final.Failure, len(g.Requests()))
			}
			snapshot, err := service.(ThreadContextReader).Context(t.Context(), th.ThreadID)
			if err != nil {
				t.Fatal(err)
			}
			installed := 0
			for _, c := range snapshot.Compactions {
				if c.Status == "compacted" {
					installed++
				}
			}
			if installed != 1 {
				t.Fatalf("installations=%d", installed)
			}
		})
	}
}

func TestGoalCompactionMutationAndStopDuringPreparation(t *testing.T) {
	for _, action := range []string{"update", "delete", "stop"} {
		t.Run(action, func(t *testing.T) {
			gate := make(chan struct{})
			summary := compactionTestStep("The launch code is CERULEAN-739.")
			summary.BlockUntil = gate
			steps := []florettest.Step{compactionTestStep("ack"), compactionTestStep("ack"), compactionTestStep("ack"), summary}
			if action == "update" {
				steps = append(steps, goalCall("review", `{"operation":"review","expected_version":2,"outcome":"completed","evidence":"Updated work verified."}`), parentEventDone())
			}
			g := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "compact", StateCompatibilityKey: "test:compact"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, steps...)
			_, service := testThreadServiceWithAgent(t, compactionTestAgent(t, g, WithAgentGoals()))
			th, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
			if err != nil {
				t.Fatal(err)
			}
			seedCompactionHistory(t, service, th.ThreadID)
			controller := service.(ThreadGoalController)
			if _, err = controller.SetGoal(t.Context(), SetGoalInput{ThreadID: th.ThreadID, RequestKey: "goal", Text: "Original work"}); err != nil {
				t.Fatal(err)
			}
			admitted, err := service.(ThreadContextCompactor).CompactContext(t.Context(), CompactContextInput{ThreadID: th.ThreadID, RequestKey: "compact", ContinueGoal: true})
			if err != nil {
				t.Fatal(err)
			}
			if err = g.WaitForRequests(t.Context(), 4); err != nil {
				t.Fatal(err)
			}
			switch action {
			case "update":
				_, err = controller.SetGoal(t.Context(), SetGoalInput{ThreadID: th.ThreadID, RequestKey: "update", ExpectedVersion: 1, Text: "Updated work"})
			case "delete":
				_, err = controller.DeleteGoal(t.Context(), DeleteGoalInput{ThreadID: th.ThreadID, RequestKey: "delete", ExpectedVersion: 1})
			case "stop":
				_, err = service.Cancel(t.Context(), CancelInput{ThreadID: th.ThreadID, RequestKey: "stop"})
			}
			if err != nil {
				t.Fatal(err)
			}
			close(gate)
			final := waitGoalCompactionView(t, service, th.ThreadID, func(v ThreadView) bool {
				return v.TurnID == admitted.TurnID && v.Activity == ThreadActivityIdle && v.LastOutcome != nil
			})
			if final.Failure != nil {
				t.Fatalf("failure=%+v", final.Failure)
			}
			if action == "update" && final.Goal.Status != "completed" || action == "delete" && final.Goal.Status != "absent" || action == "stop" && *final.LastOutcome != TurnOutcomeCancelled {
				t.Fatalf("final=%+v", final)
			}
			if len(g.Requests()) != len(steps) {
				t.Fatalf("requests=%d want %d", len(g.Requests()), len(steps))
			}
		})
	}
}

func TestGoalCompactionQuestionResumeAndReviewLimit(t *testing.T) {
	for _, outcome := range []string{"question", "blocked", "review_limit"} {
		t.Run(outcome, func(t *testing.T) {
			steps := []florettest.Step{parentEventDone(), parentEventDone(), parentEventDone()}
			switch outcome {
			case "question":
				steps = []florettest.Step{goalCall("ask", `{"operation":"review","expected_version":1,"outcome":"continue","evidence":"Need user input."}`), {Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "ask-user", Name: "ask_user", Args: `{"reason_code":"missing_external_input","required_from_user":["q"],"evidence_refs":[],"questions":[{"id":"q","header":"Question","question":"Continue?","response_mode":"write","is_secret":false}]}`}}}, {Type: provider.EventDone, Reason: "tool_calls"}}}, goalCall("review", `{"operation":"review","expected_version":1,"outcome":"completed","evidence":"User decision applied and verified."}`), parentEventDone()}
			case "blocked":
				steps = []florettest.Step{goalCall("review", `{"operation":"review","expected_version":1,"outcome":"blocked","evidence":"External credentials are needed."}`), parentEventDone()}
			}
			service, g := goalService(t, steps...)
			th, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = service.(ThreadGoalController).SetGoal(t.Context(), SetGoalInput{ThreadID: th.ThreadID, RequestKey: "goal", Text: "Finish work"}); err != nil {
				t.Fatal(err)
			}
			admitted, err := service.(ThreadContextCompactor).CompactContext(t.Context(), CompactContextInput{ThreadID: th.ThreadID, RequestKey: "compact", ContinueGoal: true})
			if err != nil {
				t.Fatal(err)
			}
			if outcome == "question" {
				waiting := waitGoalCompactionView(t, service, th.ThreadID, func(v ThreadView) bool { return len(v.Interactions) > 0 && !v.Interactions[0].Resolved })
				if _, err = service.Respond(t.Context(), RespondInput{ThreadID: th.ThreadID, InteractionID: waiting.Interactions[0].ID, RequestKey: "answer", Answers: []InteractionAnswer{{Input: map[string]string{"q": "yes"}}}}); err != nil {
					t.Fatal(err)
				}
			}
			final := waitGoalCompactionView(t, service, th.ThreadID, func(v ThreadView) bool {
				return v.TurnID == admitted.TurnID && v.Activity == ThreadActivityIdle && v.LastOutcome != nil
			})
			if (outcome == "review_limit") != (final.Failure != nil) {
				t.Fatalf("failure=%+v", final.Failure)
			}
			want := "active"
			if outcome == "question" {
				want = "completed"
			}
			if final.Goal.Status != want || len(g.Requests()) != len(steps) {
				t.Fatalf("goal=%+v requests=%d", final.Goal, len(g.Requests()))
			}
			snapshot, err := service.(ThreadContextReader).Context(t.Context(), th.ThreadID)
			if err != nil {
				t.Fatal(err)
			}
			if len(snapshot.Compactions) != 1 {
				t.Fatalf("preparation repeated: %+v", snapshot.Compactions)
			}
		})
	}
}

func TestGoalCompactionRestartFromCanonicalBoundary(t *testing.T) {
	for _, boundaryCase := range []string{"noop", "installed", "confirmed_tool", "factory_failure", "unknown_effect", "noop_without_opt_in", "confirmed_tool_without_opt_in"} {
		t.Run(boundaryCase, func(t *testing.T) {
			boundary := strings.TrimSuffix(boundaryCase, "_without_opt_in")
			var goalOptions []AgentOption
			if boundary == boundaryCase {
				goalOptions = append(goalOptions, WithAgentGoals())
			}
			source := filepath.Join(t.TempDir(), "source.sqlite")
			copyPath := filepath.Join(t.TempDir(), "copy.sqlite")
			ready := make(chan struct{})
			release := make(chan struct{})
			var once sync.Once
			var enabled atomic.Bool
			surface := WithAgentDynamicToolSurface(func(ctx context.Context, req ToolSurfaceRequest) (ToolSurface, error) {
				if enabled.Load() && req.Phase == "provider_request" && (boundary != "confirmed_tool" || req.Step > 1) {
					once.Do(func() { close(ready) })
					select {
					case <-release:
					case <-ctx.Done():
						return ToolSurface{}, ctx.Err()
					}
				}
				return ToolSurface{}, nil
			})
			var steps []florettest.Step
			if boundary != "noop" {
				steps = append(steps, compactionTestStep("ack"), compactionTestStep("ack"), compactionTestStep("ack"), compactionTestStep("The launch code is CERULEAN-739."))
			}
			if boundary == "confirmed_tool" {
				steps = append(steps, goalCall("confirmed", `{"operation":"review","expected_version":1,"outcome":"continue","evidence":"More work needed."}`))
			}
			g := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "compact", StateCompatibilityKey: "test:compact"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, steps...)
			host, err := Open(t.Context(), Options{Storage: storage.SQLite(source)})
			if err != nil {
				t.Fatal(err)
			}
			defer host.Shutdown(context.Background())
			service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) {
				return compactionTestAgent(t, g, append(goalOptions, surface)...), nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			th, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
			if err != nil {
				t.Fatal(err)
			}
			if boundary != "noop" {
				seedCompactionHistory(t, service, th.ThreadID)
			}
			enabled.Store(true)
			if _, err = service.(ThreadGoalController).SetGoal(t.Context(), SetGoalInput{ThreadID: th.ThreadID, RequestKey: "goal", Text: "Complete after restart"}); err != nil {
				t.Fatal(err)
			}
			admitted, err := service.(ThreadContextCompactor).CompactContext(t.Context(), CompactContextInput{ThreadID: th.ThreadID, RequestKey: "compact", ContinueGoal: true})
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-ready:
			case <-time.After(5 * time.Second):
				t.Fatal("boundary not reached")
			}
			if boundary == "unknown_effect" {
				writer := host.store.repo.(sessiontree.RuntimeJournalRepo)
				if _, err = writer.AppendRuntimeFacts(t.Context(), th.ThreadID.String(), []sessiontree.Entry{{ID: "unknown-call", ThreadID: th.ThreadID.String(), TurnID: admitted.TurnID.String(), RunID: admitted.RunID.String(), Type: sessiontree.EntryToolCall, Message: session.Message{Role: session.Assistant, ToolCallID: "unknown-write", ToolName: "write", ToolArgs: "{}"}}}); err != nil {
					t.Fatal(err)
				}
				effects := host.store.repo.(sessiontree.EffectAttemptRepo)
				prepared, prepareErr := effects.PrepareEffectAttempt(t.Context(), sessiontree.PrepareEffectAttemptRequest{Invocation: sessiontree.EffectInvocationIdentity{ThreadID: th.ThreadID.String(), TurnID: admitted.TurnID.String(), RunID: admitted.RunID.String(), ToolCallID: "unknown-write", ToolName: "write", ArgumentHash: sessiontree.StableHash("{}")}, RequestFingerprint: "unknown", Now: time.Now()})
				if prepareErr != nil {
					t.Fatal(prepareErr)
				}
				if _, err = effects.BeginEffectDispatch(t.Context(), sessiontree.BeginEffectDispatchRequest{EffectAttemptID: prepared.Attempt.EffectAttemptID, RequestFingerprint: "unknown", AuthorizationProofHash: "proof", Now: time.Now()}); err != nil {
					t.Fatal(err)
				}
			}
			snapshotGoalCompactionTestDatabase(t, source, copyPath)
			if _, err = service.Cancel(t.Context(), CancelInput{ThreadID: th.ThreadID, RequestKey: "stop-source"}); err != nil {
				t.Fatal(err)
			}
			close(release)
			stopped := waitGoalCompactionView(t, service, th.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
			if (boundary != "unknown_effect" && *stopped.LastOutcome != TurnOutcomeCancelled) || stopped.Goal.Status != "active" || len(g.Requests()) != len(steps) {
				t.Fatalf("stop after preparation=%+v requests=%d", stopped, len(g.Requests()))
			}
			restoredSteps := []florettest.Step{goalCall("review", `{"operation":"review","expected_version":1,"outcome":"completed","evidence":"Restart result verified."}`), parentEventDone()}
			if boundary == "unknown_effect" {
				restoredSteps = append([]florettest.Step{compactionTestStep("The launch code is CERULEAN-739. Work in /work/first."), parentEventDone()}, restoredSteps...)
			}
			restoredGateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "compact", StateCompatibilityKey: "test:compact"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, restoredSteps...)
			restored, err := Open(t.Context(), Options{Storage: storage.SQLite(copyPath)})
			if err != nil {
				t.Fatal(err)
			}
			defer restored.Shutdown(context.Background())
			resumed, err := restored.ThreadService(AgentFactoryFunc(func(_ context.Context, req AgentRequest) (*Agent, error) {
				if req.RequestKey == "compact" && (req.TurnKind != TurnKindGoalContinuation || req.CanonicalTurnInput.Text != "") {
					t.Errorf("factory=%+v", req)
				}
				if boundary == "factory_failure" {
					return nil, errors.New("recovery factory unavailable")
				}
				return compactionTestAgent(t, restoredGateway, goalOptions...), nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			if _, err = resumed.View(t.Context(), th.ThreadID); err != nil {
				t.Fatal(err)
			}
			final := waitGoalCompactionView(t, resumed, th.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
			if final.TurnID != admitted.TurnID || (boundary == "factory_failure" || boundary == "unknown_effect") != (final.Failure != nil) {
				t.Fatalf("final=%+v failure=%+v", final, final.Failure)
			}
			if boundary == "unknown_effect" && (final.Failure.Code != ThreadTurnFailureEffectOutcomeUnknown || len(restoredGateway.Requests()) != 0 || final.Goal.Status != "active") {
				t.Fatalf("unsafe recovery: failure=%+v requests=%d goal=%+v", final.Failure, len(restoredGateway.Requests()), final.Goal)
			}
			if boundary == "unknown_effect" {
				if _, err = resumed.(ThreadContextCompactor).CompactContext(t.Context(), CompactContextInput{ThreadID: th.ThreadID, RequestKey: "unsafe-compact", ContinueGoal: true}); !errors.Is(err, ErrEffectOutcomeUnknown) {
					t.Fatalf("compaction bypassed inspection: %v", err)
				}
			}
			if boundary != "factory_failure" && boundary != "unknown_effect" && final.Goal.Status != "completed" {
				t.Fatal(final.Goal)
			}
			snapshot, err := resumed.(ThreadContextReader).Context(t.Context(), th.ThreadID)
			if err != nil {
				t.Fatal(err)
			}
			if len(snapshot.Compactions) != 1 {
				t.Fatalf("preparation repeated=%+v", snapshot.Compactions)
			}
			if boundary == "unknown_effect" {
				pure, compactErr := resumed.(ThreadContextCompactor).CompactContext(t.Context(), CompactContextInput{ThreadID: th.ThreadID, RequestKey: "pure-after-unknown"})
				if compactErr != nil {
					t.Fatal(compactErr)
				}
				waitGoalCompactionView(t, resumed, th.ThreadID, func(v ThreadView) bool {
					return v.TurnID == pure.TurnID && v.Activity == ThreadActivityIdle && v.LastOutcome != nil
				})
				if _, err = resumed.(ThreadContextCompactor).CompactContext(t.Context(), CompactContextInput{ThreadID: th.ThreadID, RequestKey: "still-unsafe-compact", ContinueGoal: true}); !errors.Is(err, ErrEffectOutcomeUnknown) {
					t.Fatalf("pure compaction cleared inspection boundary: %v", err)
				}
				recovered, sendErr := resumed.Send(t.Context(), SendInput{ThreadID: th.ThreadID, RequestKey: "inspected-continue", Input: UserInput{Text: "I inspected the tool outcome. Continue without repeating the uncertain action."}})
				if sendErr != nil {
					t.Fatal(sendErr)
				}
				finished := waitGoalCompactionView(t, resumed, th.ThreadID, func(v ThreadView) bool {
					return v.TurnID == recovered.TurnID && v.Activity == ThreadActivityIdle && v.LastOutcome != nil
				})
				if finished.Failure != nil || finished.Goal.Status != "completed" {
					t.Fatalf("explicit recovery stranded: failure=%+v goal=%+v", finished.Failure, finished.Goal)
				}
			}
			if boundary == "confirmed_tool" {
				count := 0
				for _, item := range final.Items {
					if item.Activity != nil && item.Activity.ToolName == "goal" {
						count++
					}
				}
				if count != 2 {
					t.Fatalf("tool activities=%d, expected confirmed result plus review", count)
				}
			}
		})
	}
}

// Snapshot a gated test Host through SQLite's online backup API. Production
// maintenance keeps its stronger no-live-Host ownership contract.
func snapshotGoalCompactionTestDatabase(t *testing.T, source, destination string) {
	t.Helper()
	db, err := sql.Open("sqlite", (&url.URL{Scheme: "file", Path: source, RawQuery: "mode=ro"}).String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	err = conn.Raw(func(raw any) error {
		backup, err := raw.(interface {
			NewBackup(string) (*sqlite.Backup, error)
		}).NewBackup(destination)
		if err != nil {
			return err
		}
		for {
			more, stepErr := backup.Step(-1)
			if stepErr != nil {
				return errors.Join(stepErr, backup.Finish())
			}
			if !more {
				return backup.Finish()
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestGoalCompactionApprovalUsesExistingInteraction(t *testing.T) {
	var calls atomic.Int32
	tool := tools.Define[map[string]any](tools.Definition{Name: "write", InputSchema: tools.StrictObject(nil, nil), Effects: []tools.Effect{tools.EffectWrite}, Permission: tools.PermissionSpec{Mode: tools.PermissionAsk}}, nil, nil, func(context.Context, tools.Invocation[map[string]any]) (tools.Result, error) {
		calls.Add(1)
		return tools.Result{Text: "Written and verified."}, nil
	})
	g := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "compact", StateCompatibilityKey: "test:compact"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "write", Name: "write", Args: "{}"}}}, {Type: provider.EventDone, Reason: "tool_calls"}}}, goalCall("review", `{"operation":"review","expected_version":1,"outcome":"completed","evidence":"Approved write verified."}`), parentEventDone())
	_, service := testThreadServiceWithAgent(t, compactionTestAgent(t, g, WithAgentGoals(), WithAgentTools(tool), WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, req EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
		return effect(ctx, EffectAuthorizationProof{EffectAttemptID: req.EffectAttemptID, RequestFingerprint: req.RequestFingerprint, ThreadID: req.ThreadID, TurnID: req.TurnID, RunID: req.RunID, ToolCallID: req.ToolCallID, PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now()})
	}))))
	th, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.(ThreadGoalController).SetGoal(t.Context(), SetGoalInput{ThreadID: th.ThreadID, RequestKey: "goal", Text: "Write after approval"}); err != nil {
		t.Fatal(err)
	}
	admitted, err := service.(ThreadContextCompactor).CompactContext(t.Context(), CompactContextInput{ThreadID: th.ThreadID, RequestKey: "compact", ContinueGoal: true})
	if err != nil {
		t.Fatal(err)
	}
	waiting := waitGoalCompactionView(t, service, th.ThreadID, func(v ThreadView) bool { return v.Attention.ApprovalCount == 1 })
	if calls.Load() != 0 {
		t.Fatal("effect ran before approval")
	}
	approved := true
	if _, err = service.Respond(t.Context(), RespondInput{ThreadID: th.ThreadID, InteractionID: waiting.Interactions[0].ID, RequestKey: "approve", Answers: []InteractionAnswer{{Approved: &approved}}}); err != nil {
		t.Fatal(err)
	}
	final := waitGoalCompactionView(t, service, th.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	if final.TurnID != admitted.TurnID || final.Failure != nil || final.Goal.Status != "completed" || calls.Load() != 1 {
		t.Fatalf("final=%+v calls=%d", final, calls.Load())
	}
}

func TestGoalCompactionRepeatedRetryRetainsPreparation(t *testing.T) {
	policy := DefaultProviderRetryPolicy()
	policy.MaxRetries = 0
	policy.MaxTimeoutRetries = 0
	g := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "compact", StateCompatibilityKey: "test:compact"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, florettest.Step{ReturnError: errors.New("first request failed")}, florettest.Step{ReturnError: errors.New("retry failed")}, goalCall("review", `{"operation":"review","expected_version":1,"outcome":"completed","evidence":"Verified after repeated Retry."}`), parentEventDone())
	_, svc := testThreadServiceWithAgent(t, compactionTestAgent(t, g, WithAgentGoals(), WithAgentProviderRetryPolicy(policy)))
	th, err := svc.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = svc.(ThreadGoalController).SetGoal(t.Context(), SetGoalInput{ThreadID: th.ThreadID, RequestKey: "goal", Text: "Finish verified work"}); err != nil {
		t.Fatal(err)
	}
	admitted, err := svc.(ThreadContextCompactor).CompactContext(t.Context(), CompactContextInput{ThreadID: th.ThreadID, RequestKey: "compact", ContinueGoal: true})
	if err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		failed := waitGoalCompactionView(t, svc, th.ThreadID, func(v ThreadView) bool {
			return v.TurnID == admitted.TurnID && v.Activity == ThreadActivityIdle && v.Failure != nil
		})
		if failed.Goal.Status != "active" {
			t.Fatal(failed.Goal)
		}
		admitted, err = svc.Retry(t.Context(), RetryInput{ThreadID: th.ThreadID, SourceTurnID: failed.TurnID, RequestKey: RequestKey(fmt.Sprintf("retry-%d", attempt))})
		if err != nil {
			t.Fatal(err)
		}
	}
	final := waitGoalCompactionView(t, svc, th.ThreadID, func(v ThreadView) bool {
		return v.TurnID == admitted.TurnID && v.Activity == ThreadActivityIdle && v.LastOutcome != nil
	})
	snapshot, err := svc.(ThreadContextReader).Context(t.Context(), th.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	if final.Failure != nil || final.Goal.Status != "completed" || len(snapshot.Compactions) != 1 || len(g.Requests()) != 4 {
		t.Fatalf("goal=%+v failure=%+v compactions=%d requests=%d", final.Goal, final.Failure, len(snapshot.Compactions), len(g.Requests()))
	}
}

func waitGoalCompactionView(t *testing.T, service ThreadService, threadID identity.ThreadID, ready func(ThreadView) bool) ThreadView {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		view, err := service.View(t.Context(), threadID)
		if err != nil {
			t.Fatal(err)
		}
		if ready(view) {
			return view
		}
		if time.Now().After(deadline) {
			t.Fatalf("Goal compaction did not settle: activity=%s turn=%s failure=%+v", view.Activity, view.TurnID, view.Failure)
		}
		time.Sleep(time.Millisecond * 5)
	}
}

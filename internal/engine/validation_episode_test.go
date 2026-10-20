package engine_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/internal/engine"
	"github.com/floegence/floret/v7/internal/event"
	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
	"github.com/floegence/floret/v7/internal/testing/harness"
	"github.com/floegence/floret/v7/tools"
)

func TestValidationEpisodeResetsAfterCommittedValidBatch(t *testing.T) {
	for _, businessError := range []bool{false, true} {
		t.Run(fmt.Sprint(businessError), func(t *testing.T) {
			var steps [][]provider.StreamEvent
			for i := 0; i < 3; i++ {
				steps = append(steps,
					harness.Step(harness.Tool(fmt.Sprintf("bad-%d", i), "read", `{"wrong":"x"}`), harness.DoneReason("tool_calls")),
					harness.Step(harness.Tool(fmt.Sprintf("good-%d", i), "read", fmt.Sprintf(`{"value":"%d"}`, i)), harness.DoneReason("tool_calls")),
				)
			}
			steps = append(steps, harness.Step(harness.Text("done"), harness.Done()))
			p := harness.NewScriptedProvider(steps...)
			rec := &event.Recorder{}
			e := newTestEngine(p, rec)
			var executions int
			e.Tools = tools.NewRegistry(stringTool("read", "Read", true, tools.PermissionSpec{}, func(context.Context, string) (string, error) {
				executions++
				if businessError {
					return "", errors.New("not found")
				}
				return "ok", nil
			}))
			got := e.Run(context.Background(), "inspect")
			if got.Status != engine.Completed || executions != 3 || len(p.Requests) != 7 {
				t.Fatalf("result=%+v executions=%d requests=%d", got, executions, len(p.Requests))
			}
			var attempts []int
			for _, ev := range rec.Events {
				if ev.Type == event.StepEnd {
					if n, ok := ev.Metadata.(map[string]any)["validation_correction_attempt"].(int); ok {
						attempts = append(attempts, n)
					}
				}
			}
			if !reflect.DeepEqual(attempts, []int{1, 1, 1}) {
				t.Fatalf("attempts=%v", attempts)
			}
			messages, err := e.Store.Transcript("run")
			if err != nil {
				t.Fatal(err)
			}
			if countMessagesByKind(messages, session.MessageKindToolValidationError) != 6 {
				t.Fatal("validation exchanges lost")
			}
		})
	}
}

func TestValidationEpisodeCountsBatchesAcrossToolKinds(t *testing.T) {
	for _, validControl := range []bool{false, true} {
		t.Run(fmt.Sprint(validControl), func(t *testing.T) {
			steps := [][]provider.StreamEvent{
				harness.Step(harness.Tool("bad-read", "read", `{"wrong":1}`), harness.DoneReason("tool_calls")),
				harness.Step(harness.Tool("bad-control", "checkpoint", `{"wrong":1}`), harness.Tool("bad-read-again", "read", `{}`), harness.DoneReason("tool_calls")),
			}
			if validControl {
				steps = append(steps, harness.Step(harness.Tool("checkpoint", "checkpoint", `{}`), harness.DoneReason("tool_calls")))
			}
			steps = append(steps, harness.Step(harness.Tool("bad-final", "read", `{}`), harness.DoneReason("tool_calls")), harness.Step(harness.Text("done"), harness.Done()))
			rec := &event.Recorder{}
			p := harness.NewScriptedProvider(steps...)
			e := newTestEngine(p, rec)
			e.Tools = tools.NewRegistry(stringTool("read", "Read", true, tools.PermissionSpec{}, func(context.Context, string) (string, error) { t.Fatal("invalid tool executed"); return "", nil }))
			projections := 0
			e.Options.ControlSpec = engine.ControlSpec{Definitions: []tools.ToolDefinition{{Name: "checkpoint", InputSchema: tools.StrictObject(nil, nil)}}, Project: func(call provider.ToolCall) (engine.ControlSignal, bool, error) {
				projections++
				return engine.ControlSignal{Disposition: engine.ControlContinue, Name: call.Name, CallID: call.ID, OutputText: "checkpoint accepted"}, true, nil
			}}
			got := e.Run(t.Context(), "inspect")
			want := engine.Failed
			if validControl {
				want = engine.Completed
			}
			if got.Status != want {
				t.Fatalf("result=%+v", got)
			}
			attempts := []int{}
			for _, ev := range rec.Events {
				if ev.Type == event.StepEnd {
					if n, ok := ev.Metadata.(map[string]any)["validation_correction_attempt"].(int); ok {
						attempts = append(attempts, n)
					}
				}
			}
			expected := []int{1, 2, 3}
			if validControl {
				expected = []int{1, 2, 1}
			}
			if !reflect.DeepEqual(attempts, expected) {
				t.Fatalf("attempts=%v", attempts)
			}
			if projections != map[bool]int{false: 0, true: 1}[validControl] {
				t.Fatalf("projections=%d", projections)
			}
		})
	}
}

func TestValidationEpisodeExhaustionSettlesSiblingsBeforeWaiting(t *testing.T) {
	steps := [][]provider.StreamEvent{}
	for i := 0; i < 3; i++ {
		calls := []provider.StreamEvent{harness.Tool(fmt.Sprint("bad-", i), "read", `{}`)}
		if i == 2 {
			calls = append(calls, harness.Tool("ask-input", "input", `{}`))
		}
		steps = append(steps, harness.Step(append(calls, harness.DoneReason("tool_calls"))...))
	}
	p := harness.NewScriptedProvider(steps...)
	rec := &event.Recorder{}
	e := newTestEngine(p, rec)
	inputs := 0
	e.Tools = tools.NewRegistry(stringTool("read", "Read", true, tools.PermissionSpec{}, func(context.Context, string) (string, error) { t.Fatal("invalid tool executed"); return "", nil }), tools.Define[map[string]any](tools.Definition{Name: "input", ReadOnly: true, InputSchema: tools.StrictObject(nil, nil)}, nil, nil, func(context.Context, tools.Invocation[map[string]any]) (tools.Result, error) {
		inputs++
		return tools.Result{Text: "confirmed result", InputRequired: &tools.InputRequest{Summary: "Continue?", Questions: []tools.InputQuestion{{ID: "answer", Prompt: "Continue?", Kind: "write"}}}}, nil
	}))
	got := e.Run(t.Context(), "inspect")
	if got.Status != engine.Failed || got.Err == nil || got.Err.Error() != "tool argument correction exhausted" || inputs != 1 {
		t.Fatalf("result=%+v inputs=%d", got, inputs)
	}
	messages, err := e.Store.Transcript("run")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, m := range messages {
		if m.Role == session.Tool && m.ToolCallID == "ask-input" {
			found = m.ToolResult != nil && m.ToolResult.InputRequired != nil && strings.Contains(m.Content, "confirmed result")
		}
	}
	if !found {
		t.Fatal("completed sibling result discarded")
	}
	if rec.Events[len(rec.Events)-2].Type != event.StepEnd || rec.Events[len(rec.Events)-1].Type != event.RunEnd {
		t.Fatal("batch not settled before run end")
	}
}

func TestValidationEpisodeDoesNotResetOnTransportRetry(t *testing.T) {
	bad := func(id string) []provider.StreamEvent {
		return harness.Step(harness.Tool(id, "read", `{}`), harness.DoneReason("tool_calls"))
	}
	p := harness.NewScriptedProvider(bad("one"), nil, bad("two"), bad("three"))
	p.Errs[2] = io.EOF
	e := newTestEngine(p, &event.Recorder{})
	e.Options.ProviderRetryPolicy = fastRecoveryPolicy()
	e.Tools = tools.NewRegistry(stringTool("read", "Read", true, tools.PermissionSpec{}, func(context.Context, string) (string, error) { t.Fatal("invalid call executed"); return "", nil }))
	got := e.Run(t.Context(), "inspect")
	if got.Status != engine.Failed || got.Err == nil || got.Err.Error() != "tool argument correction exhausted" || len(p.Requests) != 4 {
		t.Fatalf("result=%+v requests=%d", got, len(p.Requests))
	}
}

func TestValidationEpisodeDoesNotResetOnCompactionOrSteering(t *testing.T) {
	for _, boundary := range []string{"compaction", "steering"} {
		t.Run(boundary, func(t *testing.T) {
			p := harness.NewScriptedProvider(
				harness.Step(harness.Tool("bad-one", "read", `{}`), harness.DoneReason("tool_calls")),
				harness.Step(harness.Tool("bad-two", "read", `{}`), harness.DoneReason("tool_calls")),
				harness.Step(harness.Tool("bad-three", "read", `{}`), harness.DoneReason("tool_calls")))
			e := newTestEngine(p, &event.Recorder{})
			e.Tools = tools.NewRegistry(stringTool("read", "Read", true, tools.PermissionSpec{}, func(context.Context, string) (string, error) { t.Fatal("invalid call executed"); return "", nil }))
			if boundary == "compaction" {
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
			} else {
				begins := 0
				e.Options.Steering = &engine.SteeringControl{Begin: func(ctx context.Context, _ func() error) (context.Context, []session.Message, error) {
					begins++
					if begins == 2 {
						history, err := e.Store.Transcript("run")
						if err != nil {
							return ctx, nil, err
						}
						return ctx, append(history, session.Message{Role: session.User, Content: "Steering instruction", EntryID: "steering"}), nil
					}
					return ctx, nil, nil
				}, CanContinue: func(error) bool { return false }, Finish: func(context.Context) (bool, error) { return true, nil }}
			}
			got := e.Run(t.Context(), "inspect")
			if got.Status != engine.Failed || got.Err == nil || got.Err.Error() != "tool argument correction exhausted" || len(p.Requests) != 3 {
				t.Fatalf("result=%+v requests=%d", got, len(p.Requests))
			}
			if boundary == "compaction" && got.Metrics.Compactions != 1 {
				t.Fatalf("compactions=%d", got.Metrics.Compactions)
			}
		})
	}
}

func TestValidationEpisodeCommitFailureTakesPrecedence(t *testing.T) {
	p := harness.NewScriptedProvider(
		harness.Step(harness.Tool("one", "read", `{}`), harness.DoneReason("tool_calls")),
		harness.Step(harness.Tool("two", "read", `{}`), harness.DoneReason("tool_calls")),
		harness.Step(harness.Tool("three", "read", `{}`), harness.DoneReason("tool_calls")))
	e := newTestEngine(p, &event.Recorder{})
	e.Tools = tools.NewRegistry(stringTool("read", "Read", true, tools.PermissionSpec{}, func(context.Context, string) (string, error) { t.Fatal("invalid call executed"); return "", nil }))
	failure := errors.New("feedback write failed")
	e.Options.CommitMessages = func(_ context.Context, messages []session.Message) ([]session.Message, error) {
		for _, m := range messages {
			if m.Role == session.Tool && m.ToolCallID == "three" {
				return nil, failure
			}
		}
		return messages, nil
	}
	got := e.Run(t.Context(), "inspect")
	if got.Status != engine.Failed || !errors.Is(got.Err, failure) {
		t.Fatalf("result=%+v", got)
	}
}

func TestValidationEpisodeDoesNotResetOnNonToolResponse(t *testing.T) {
	for _, gap := range []string{"text", "empty", "truncated", "stop_hook"} {
		t.Run(gap, func(t *testing.T) {
			var steps [][]provider.StreamEvent
			for i := 0; i < 3; i++ {
				steps = append(steps, harness.Step(harness.Tool(fmt.Sprintf("bad-%d", i), "read", `{"wrong":"x"}`), harness.DoneReason("tool_calls")))
				if i == 2 {
					break
				}
				switch gap {
				case "text":
					steps = append(steps, harness.Step(harness.Text("working"), harness.DoneReason("tool_calls")))
				case "empty":
					steps = append(steps, harness.Step(harness.DoneReason("tool_calls")))
				case "truncated":
					steps = append(steps, []provider.StreamEvent{harness.Text("partial"), {Type: provider.Truncated}})
				case "stop_hook":
					steps = append(steps, harness.Step(harness.Text("done?"), harness.Done()))
				}
			}
			p := harness.NewScriptedProvider(steps...)
			e := newTestEngine(p, &event.Recorder{})
			e.Options.NoProgressLimit = 10
			e.Tools = tools.NewRegistry(stringTool("read", "Read", true, tools.PermissionSpec{}, func(context.Context, string) (string, error) { t.Fatal("invalid call executed"); return "", nil }))
			e.StopHook = func(context.Context, engine.StopHookContext) (engine.StopHookResult, error) {
				return engine.StopHookResult{Continue: true, Prompt: "continue"}, nil
			}
			got := e.Run(context.Background(), "inspect")
			if got.Status != engine.Failed || got.Err == nil || got.Err.Error() != "tool argument correction exhausted" || len(p.Requests) != 5 {
				t.Fatalf("result=%+v requests=%d", got, len(p.Requests))
			}
		})
	}
}

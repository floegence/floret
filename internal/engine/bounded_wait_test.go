package engine_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/floegence/floret/v7/internal/engine"
	"github.com/floegence/floret/v7/internal/event"
	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/testing/harness"
	"github.com/floegence/floret/v7/tools"
	"testing"
	"testing/synctest"
	"time"
)

func TestBoundedWaitRepeatGuard(t *testing.T) {
	// Initialize shared tokenizer workers outside the fake-clock bubble.
	warm := newTestEngine(harness.NewScriptedProvider([]provider.StreamEvent{{Type: provider.Delta, Text: "warm"}, {Type: provider.Done, Reason: "stop"}}), &event.Recorder{})
	warm.Run(context.Background(), "warm")

	for _, tc := range []struct {
		name                    string
		elapsed                 time.Duration
		timedOut, failed, mixed bool
		complete                bool
	}{
		{name: "four full waits", elapsed: time.Second, timedOut: true, complete: true},
		{name: "fast return", timedOut: true},
		{name: "finished target", elapsed: time.Second},
		{name: "failed wait", elapsed: time.Second, timedOut: true, failed: true},
		{name: "mixed side effect", elapsed: time.Second, timedOut: true, mixed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var script [][]provider.StreamEvent
				for i := 0; i < 5; i++ {
					calls := []provider.ToolCall{{ID: fmt.Sprintf("wait-%d", i), Name: "wait", Args: "{\"value\":\"same\"}"}}
					if tc.mixed {
						calls = append(calls, provider.ToolCall{ID: fmt.Sprintf("write-%d", i), Name: "write", Args: "{\"value\":\"same\"}"})
					}
					script = append(script, []provider.StreamEvent{{Type: provider.ToolCalls, ToolCalls: calls}, {Type: provider.Done, Reason: "tool_calls"}})
				}
				script = append(script, []provider.StreamEvent{{Type: provider.Delta, Text: "child report"}, {Type: provider.Done, Reason: "stop"}})
				e := newTestEngine(harness.NewScriptedProvider(script...), &event.Recorder{})
				e.Options.DuplicateToolLimit = 3
				for _, name := range []string{"wait", "write"} {
					mustRegister(t, e.Tools, tools.Define[stringArgs](tools.Definition{Name: name, InputSchema: tools.StrictObject(map[string]any{"value": tools.String("value")}, []string{"value"}), Permission: tools.PermissionSpec{Mode: tools.PermissionAllow}}, nil, nil,
						func(_ context.Context, inv tools.Invocation[stringArgs]) (tools.Result, error) {
							if inv.Name == "write" {
								return tools.Result{Text: "written"}, nil
							}
							time.Sleep(tc.elapsed)
							return tools.Result{Text: "waiting", IsError: tc.failed, BoundedWait: &tools.BoundedWait{Timeout: time.Second, TimedOut: tc.timedOut}}, nil
						}))
				}
				got := e.Run(context.Background(), "wait for child")
				if tc.complete {
					if got.Status != engine.Completed || got.Output != "child report" {
						t.Fatalf("result=%#v", got)
					}
				} else if !errors.Is(got.Err, engine.ErrDuplicateTools) {
					t.Fatalf("err=%v", got.Err)
				}
			})
		})
	}
}

package engine_test

import (
	"context"
	"testing"

	"github.com/floegence/floret/v7/internal/engine"
	"github.com/floegence/floret/v7/internal/event"
	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/testing/harness"
	"github.com/floegence/floret/v7/tools"
)

func TestStreamOutputProgressDoesNotDuplicateArgumentsOrUsage(t *testing.T) {
	for _, streamed := range []bool{false, true} {
		t.Run(map[bool]string{true: "streamed", false: "complete-only"}[streamed], func(t *testing.T) {
			events := []provider.StreamEvent{{Type: provider.Reasoning, Text: "abcd"}, {Type: provider.Delta, Text: "你好"}}
			if streamed {
				events = append(events, provider.StreamEvent{Type: provider.ToolCallStart, ToolCallStream: provider.ToolCallStream{ID: "call", Name: "preview"}}, provider.StreamEvent{Type: provider.ToolCallDelta, Text: "{", ToolCallStream: provider.ToolCallStream{ID: "call", Name: "preview"}}, provider.StreamEvent{Type: provider.ToolCallDelta, Text: "}", ToolCallStream: provider.ToolCallStream{ID: "call", Name: "preview"}}, provider.StreamEvent{Type: provider.ToolCallEnd, ToolCallStream: provider.ToolCallStream{ID: "call", Name: "preview"}})
			}
			events = append(events, provider.StreamEvent{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{{ID: "call", Name: "preview", Args: `{}`}}}, provider.StreamEvent{Type: provider.UsageEvent, Usage: provider.Usage{OutputTokens: 27}}, provider.StreamEvent{Type: provider.Done, Reason: "tool_calls"})
			rec := &event.Recorder{}
			e := newTestEngine(harness.NewScriptedProvider(harness.Step(events...), harness.Step(harness.Text("done"), harness.Done())), rec)
			var received string
			completed, executed := 0, 0
			tool := tools.Define[map[string]any](tools.Definition{Name: "preview", ReadOnly: true, InputSchema: tools.StrictObject(map[string]any{}, nil), NewArgumentPreview: func() tools.ArgumentPreviewer {
				return func(delta string, complete bool) (*tools.ArgumentPreview, error) {
					if executed != 0 {
						t.Fatal("preview ran after effect")
					}
					received += delta
					if complete {
						completed++
					}
					n := len(received)
					return &tools.ArgumentPreview{Additions: &n}, nil
				}
			}}, nil, nil, func(context.Context, tools.Invocation[map[string]any]) (tools.Result, error) {
				executed++
				return tools.Result{Text: "ok"}, nil
			})
			if err := e.Tools.Register(tool); err != nil {
				t.Fatal(err)
			}
			result := e.Run(t.Context(), "edit")
			if result.Status != engine.Completed {
				t.Fatalf("run: %+v", result)
			}
			if received != "{}" || completed != 1 || executed != 1 {
				t.Fatalf("preview=%q completions=%d executions=%d", received, completed, executed)
			}
			found := false
			for _, ev := range rec.Events {
				if ev.Type == event.ProviderToolCallEnd {
					if ev.EstimatedOutputTokens != 5 {
						t.Fatalf("output estimate=%d, want 5", ev.EstimatedOutputTokens)
					}
					found = true
				}
				if ev.Type == event.ProviderUsage && ev.Step == 1 {
					if usage, ok := ev.Metrics.(provider.Usage); ok && usage.OutputTokens != 27 {
						t.Fatalf("usage changed: %+v", usage)
					}
				}
			}
			if !found {
				t.Fatal("missing output progress")
			}
		})
	}
}

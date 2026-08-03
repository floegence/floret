package tools

import (
	"context"
	"errors"
	"testing"

	"github.com/floegence/floret/v3/identity"
)

func TestActivityForCallUsesInvalidActivityOnlyForParseableSchemaFailures(t *testing.T) {
	invalidCalls := 0
	registry := NewRegistry(Define[map[string]any](
		Definition{
			Name: "terminal",
			InputSchema: StrictObject(map[string]any{
				"command":  String("command"),
				"yield_ms": map[string]any{"type": "integer", "maximum": 30_000},
			}, []string{"command"}),
			ReadOnly:   true,
			Permission: PermissionSpec{Mode: PermissionAllow},
			InvalidActivity: func(inv Invocation[map[string]any]) (*ActivityPresentation, error) {
				invalidCalls++
				if inv.CallID != "call-1" || inv.RunID != "run-1" || inv.ThreadID != "thread-1" || inv.TurnID != "turn-1" {
					t.Fatalf("invalid invocation identity = %#v", inv)
				}
				command, _ := inv.Args["command"].(string)
				return &ActivityPresentation{
					Label:    command,
					Renderer: ActivityRendererTerminal,
					Payload:  TerminalActivityPayload{Command: command},
				}, nil
			},
		},
		nil,
		nil,
		func(context.Context, Invocation[map[string]any]) (Result, error) {
			return Result{Text: "unexpected"}, nil
		},
	))
	opts := DispatchOptions{
		RunID: identity.RunID("run-1"), ThreadID: identity.ThreadID("thread-1"),
		TurnID: identity.TurnID("turn-1"), PromptScopeID: identity.PromptScopeID("thread-1"),
	}

	activity, err := registry.ActivityForCall(ToolCall{
		ID: "call-1", Name: "terminal", Args: `{"command":"sleep 30","yield_ms":60000}`,
	}, opts)
	if !errors.Is(err, ErrSchema) {
		t.Fatalf("ActivityForCall error = %v, want schema error", err)
	}
	if activity == nil || activity.Label != "sleep 30" {
		t.Fatalf("invalid activity = %#v", activity)
	}
	if invalidCalls != 1 {
		t.Fatalf("invalid activity calls = %d, want 1", invalidCalls)
	}

	activity, err = registry.ActivityForCall(ToolCall{ID: "call-2", Name: "terminal", Args: `{"command":"pwd"}`}, opts)
	if err != nil || activity != nil || invalidCalls != 1 {
		t.Fatalf("valid call activity = %#v, err = %v, invalid calls = %d", activity, err, invalidCalls)
	}

	activity, err = registry.ActivityForCall(ToolCall{ID: "call-3", Name: "terminal", Args: `{"command":`}, opts)
	if !errors.Is(err, ErrSchema) || activity != nil || invalidCalls != 1 {
		t.Fatalf("malformed call activity = %#v, err = %v, invalid calls = %d", activity, err, invalidCalls)
	}
}

func TestActivityForCallContainsInvalidActivityPanic(t *testing.T) {
	registry := NewRegistry(Define[map[string]any](
		Definition{
			Name:        "read",
			InputSchema: StrictObject(map[string]any{"path": String("path")}, []string{"path"}),
			ReadOnly:    true,
			InvalidActivity: func(Invocation[map[string]any]) (*ActivityPresentation, error) {
				panic("boom")
			},
		},
		nil,
		nil,
		func(context.Context, Invocation[map[string]any]) (Result, error) {
			return Result{Text: "unexpected"}, nil
		},
	))

	activity, err := registry.ActivityForCall(ToolCall{ID: "call-1", Name: "read", Args: `{}`}, DispatchOptions{})
	if !errors.Is(err, ErrSchema) || activity != nil {
		t.Fatalf("panic activity = %#v, err = %v", activity, err)
	}
}

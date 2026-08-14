package tools_test

import (
	"context"
	"testing"

	"github.com/floegence/floret/v4/tools"
)

type effectDispatchArgs struct {
	Command string `json:"command"`
}

func TestEffectDispatchRequestCarriesToolAuthoredActivity(t *testing.T) {
	registry := tools.NewRegistry()
	if err := registry.Register(tools.Define[effectDispatchArgs](
		tools.Definition{
			Name:        "terminal.exec",
			InputSchema: tools.StrictObject(map[string]any{"command": tools.String("command")}, []string{"command"}),
			Effects:     []tools.Effect{tools.EffectShell},
			Permission:  tools.PermissionSpec{Mode: tools.PermissionAsk},
			Activity: func(inv tools.Invocation[any]) (*tools.ActivityPresentation, error) {
				args := inv.Args.(effectDispatchArgs)
				return &tools.ActivityPresentation{
					Label:    args.Command,
					Renderer: tools.ActivityRendererTerminal,
					Payload:  tools.TerminalActivityPayload{Command: args.Command},
				}, nil
			},
		},
		nil,
		nil,
		func(context.Context, tools.Invocation[effectDispatchArgs]) (tools.Result, error) {
			return tools.Result{Text: "done"}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}

	var observed *tools.ActivityPresentation
	result := registry.Dispatch(context.Background(), tools.ToolCall{
		ID: "call", Name: "terminal.exec", Args: `{"command":"printf public"}`,
	}, tools.DispatchOptions{
		EffectBatchPreflight: func(_ context.Context, requests []tools.EffectDispatchRequest) error {
			observed = tools.CloneActivityPresentation(requests[0].Activity)
			return nil
		},
		EffectDispatcher: func(ctx context.Context, _ tools.EffectDispatchRequest, invoke func(context.Context) tools.Result) tools.Result {
			return invoke(ctx)
		},
	})
	if result.IsError {
		t.Fatalf("dispatch result = %#v", result)
	}
	if observed == nil || observed.Label != "printf public" || observed.Renderer != tools.ActivityRendererTerminal {
		t.Fatalf("effect activity = %#v", observed)
	}
	payload, ok := observed.Payload.(tools.TerminalActivityPayload)
	if !ok || payload.Command != "printf public" {
		t.Fatalf("effect activity payload = %#v", observed.Payload)
	}
}

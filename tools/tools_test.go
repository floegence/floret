package tools

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/floegence/floret/observation"
)

type testArgs struct {
	Value string `json:"value"`
}

func testTool(name string, readOnly bool, handler func(context.Context, Invocation[testArgs]) (Result, error)) Tool {
	return Define[testArgs](
		Definition{
			Name:        name,
			Description: name,
			InputSchema: StrictObject(map[string]any{
				"value": String("test value"),
			}, []string{"value"}),
			Effects:    []Effect{EffectRead},
			ReadOnly:   readOnly,
			Permission: PermissionSpec{Mode: PermissionAllow},
		},
		nil,
		nil,
		handler,
	)
}

func TestRegisterRejectsDuplicateName(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Register(testTool("read", false, func(context.Context, Invocation[testArgs]) (Result, error) {
		return Result{}, nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(testTool("read", false, func(context.Context, Invocation[testArgs]) (Result, error) {
		return Result{Text: "other"}, nil
	})); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("err = %v, want duplicate", err)
	}
	got := reg.Run(context.Background(), ToolCall{Name: "read", Args: `{"value":""}`}, nil)
	if got.Text != "" {
		t.Fatalf("duplicate registration overwrote original handler")
	}
}

func TestRegisterRejectsInvalidTool(t *testing.T) {
	if err := NewRegistry().Register(Tool{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("err = %v, want invalid", err)
	}
}

func TestUnknownToolFailsClearly(t *testing.T) {
	got := NewRegistry().Run(context.Background(), ToolCall{Name: "missing"}, nil)
	if !got.IsError || got.Text != `unknown tool "missing"` {
		t.Fatalf("result = %#v, want unknown tool name", got)
	}
}

func TestSchemaErrorDoesNotCallResourceApproverOrHandler(t *testing.T) {
	reg := NewRegistry()
	called := false
	err := reg.Register(Define[testArgs](
		Definition{
			Name:        "read",
			InputSchema: StrictObject(map[string]any{"value": String("")}, []string{"value"}),
			Permission:  PermissionSpec{Mode: PermissionAllow},
		},
		nil,
		func(Invocation[testArgs]) ([]ResourceRef, error) {
			called = true
			return nil, nil
		},
		func(context.Context, Invocation[testArgs]) (Result, error) {
			called = true
			return Result{Text: "bad"}, nil
		},
	))
	if err != nil {
		t.Fatal(err)
	}
	got := reg.Run(context.Background(), ToolCall{Name: "read", Args: `{"extra":1}`}, func(context.Context, ApprovalRequest) (PermissionDecision, error) {
		called = true
		return PermissionDecisionAllow, nil
	})
	if !got.IsError || !contains(got.Text, "invalid arguments") {
		t.Fatalf("result = %#v", got)
	}
	if called {
		t.Fatalf("schema violation should not call resources, approver, or handler")
	}
}

func TestToolPanicRecovered(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Register(testTool("panic", false, func(context.Context, Invocation[testArgs]) (Result, error) {
		panic("boom")
	})); err != nil {
		t.Fatal(err)
	}
	got := reg.Run(context.Background(), ToolCall{Name: "panic", Args: `{"value":"x"}`}, nil)
	if !got.IsError || got.Text != `tool "panic" panicked: boom` {
		t.Fatalf("result = %#v, want recovered panic", got)
	}
}

func TestApprovalGrantedExecutesExactRequest(t *testing.T) {
	reg := NewRegistry()
	var seen string
	if err := reg.Register(Define[testArgs](
		Definition{
			Name:        "write",
			InputSchema: StrictObject(map[string]any{"value": String("")}, []string{"value"}),
			Permission:  PermissionSpec{Mode: PermissionAsk, ResourceKinds: []string{"file"}},
		},
		nil,
		func(inv Invocation[testArgs]) ([]ResourceRef, error) {
			return []ResourceRef{{Kind: "file", Value: inv.Args.Value}}, nil
		},
		func(_ context.Context, inv Invocation[testArgs]) (Result, error) {
			seen = inv.RawArgs
			return Result{Text: "ok"}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}
	got := reg.Run(context.Background(), ToolCall{ID: "1", Name: "write", Args: `{"value":"original"}`}, func(_ context.Context, req ApprovalRequest) (PermissionDecision, error) {
		if req.Args != `{"value":"original"}` || len(req.Resources) != 1 || req.Resources[0].Value != "original" {
			t.Fatalf("approval request = %#v", req)
		}
		return PermissionDecisionAllow, nil
	})
	if got.IsError || got.Text != "ok" {
		t.Fatalf("result = %#v", got)
	}
	if seen != `{"value":"original"}` {
		t.Fatalf("handler saw %q, want exact approved args", seen)
	}
}

func TestApprovalRequestIncludesActivityPresentation(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Register(Define[testArgs](
		Definition{
			Name:        "shell",
			InputSchema: StrictObject(map[string]any{"value": String("")}, []string{"value"}),
			Effects:     []Effect{EffectShell},
			Permission:  PermissionSpec{Mode: PermissionAsk, ResourceKinds: []string{"command"}},
			Activity: func(inv Invocation[any]) (*observation.ActivityPresentation, error) {
				args, ok := inv.Args.(testArgs)
				if !ok {
					t.Fatalf("activity args type = %T", inv.Args)
				}
				return &observation.ActivityPresentation{
					Label:    args.Value,
					Renderer: observation.ActivityRendererTerminal,
					Payload:  map[string]any{"command": args.Value},
				}, nil
			},
		},
		nil,
		func(inv Invocation[testArgs]) ([]ResourceRef, error) {
			return []ResourceRef{{Kind: "command", Value: inv.Args.Value}}, nil
		},
		func(context.Context, Invocation[testArgs]) (Result, error) {
			return Result{Text: "ok"}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}
	got := reg.Run(context.Background(), ToolCall{ID: "call", Name: "shell", Args: `{"value":"curl https://example.test"}`}, func(_ context.Context, req ApprovalRequest) (PermissionDecision, error) {
		if req.Activity == nil ||
			req.Activity.Label != "curl https://example.test" ||
			req.Activity.Renderer != observation.ActivityRendererTerminal ||
			req.Activity.Payload["command"] != "curl https://example.test" {
			t.Fatalf("approval activity = %#v", req.Activity)
		}
		return PermissionDecisionAllow, nil
	})
	if got.IsError || got.Text != "ok" {
		t.Fatalf("result = %#v", got)
	}
}

func TestPermissionResolverCanAllowSafeInvocationWithoutApprover(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Register(Define[testArgs](
		Definition{
			Name:        "shell",
			InputSchema: StrictObject(map[string]any{"value": String("")}, []string{"value"}),
			Effects:     []Effect{EffectShell},
			OpenWorld:   true,
			Permission:  PermissionSpec{Mode: PermissionAsk, ResourceKinds: []string{"command"}},
			PermissionFor: func(req PermissionRequest) (PermissionSpec, error) {
				if req.Name != "shell" || req.RawArgs != `{"value":"ls"}` {
					t.Fatalf("permission request = %#v", req)
				}
				return PermissionSpec{Mode: PermissionAllow, ResourceKinds: []string{"command"}}, nil
			},
		},
		nil,
		nil,
		func(context.Context, Invocation[testArgs]) (Result, error) {
			return Result{Text: "ok"}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}
	calledApprover := false
	got := reg.Run(context.Background(), ToolCall{ID: "call", Name: "shell", Args: `{"value":"ls"}`}, func(context.Context, ApprovalRequest) (PermissionDecision, error) {
		calledApprover = true
		return PermissionDecisionDeny, nil
	})
	if got.IsError || got.Text != "ok" {
		t.Fatalf("result = %#v", got)
	}
	if calledApprover {
		t.Fatalf("allow permission resolver should not call approver")
	}
}

func TestPermissionResolverCanRequireApprovalForRiskyInvocation(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Register(Define[testArgs](
		Definition{
			Name:        "shell",
			InputSchema: StrictObject(map[string]any{"value": String("")}, []string{"value"}),
			Effects:     []Effect{EffectShell},
			OpenWorld:   true,
			Permission:  PermissionSpec{Mode: PermissionAsk, ResourceKinds: []string{"command"}},
			PermissionFor: func(req PermissionRequest) (PermissionSpec, error) {
				if req.Args.(testArgs).Value == "cat README.md" {
					return PermissionSpec{Mode: PermissionAllow, ResourceKinds: []string{"command"}}, nil
				}
				return PermissionSpec{Mode: PermissionAsk, ResourceKinds: []string{"command"}}, nil
			},
		},
		nil,
		func(inv Invocation[testArgs]) ([]ResourceRef, error) {
			return []ResourceRef{{Kind: "command", Value: inv.Args.Value}}, nil
		},
		func(context.Context, Invocation[testArgs]) (Result, error) {
			return Result{Text: "ok"}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}
	var approval ApprovalRequest
	got := reg.RunWithOptions(context.Background(), ToolCall{ID: "call", Name: "shell", Args: `{"value":"rm file"}`}, func(_ context.Context, req ApprovalRequest) (PermissionDecision, error) {
		approval = req
		return PermissionDecisionAllow, nil
	}, RunOptions{RunID: "run", ThreadID: "thread", TurnID: "turn", Step: 2, Labels: map[string]string{"correlation.turn": "turn"}})
	if got.IsError || got.Text != "ok" {
		t.Fatalf("result = %#v", got)
	}
	if approval.ID != "call" ||
		approval.RunID != "run" ||
		approval.ThreadID != "thread" ||
		approval.TurnID != "turn" ||
		approval.Step != 2 ||
		len(approval.Resources) != 1 ||
		approval.Resources[0].Value != "rm file" ||
		approval.Labels["correlation.turn"] != "turn" {
		t.Fatalf("approval = %#v", approval)
	}
}

func TestPermissionDenyDoesNotCallResourcesApproverOrHandler(t *testing.T) {
	reg := NewRegistry()
	called := false
	if err := reg.Register(Define[testArgs](
		Definition{
			Name:        "blocked",
			InputSchema: StrictObject(map[string]any{"value": String("")}, []string{"value"}),
			Permission:  PermissionSpec{Mode: PermissionDeny},
		},
		nil,
		func(Invocation[testArgs]) ([]ResourceRef, error) {
			called = true
			return nil, nil
		},
		func(context.Context, Invocation[testArgs]) (Result, error) {
			called = true
			return Result{Text: "bad"}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}
	got := reg.RunWithOptions(context.Background(), ToolCall{ID: "call", Name: "blocked", Args: `{"value":"x"}`}, func(context.Context, ApprovalRequest) (PermissionDecision, error) {
		called = true
		return PermissionDecisionAllow, nil
	}, RunOptions{RunID: "run", ThreadID: "session", Step: 7})
	if !got.IsError || got.Text != ErrRejected.Error() {
		t.Fatalf("result = %#v", got)
	}
	if called {
		t.Fatalf("deny should not call resource extractor, approver, or handler")
	}
	if defs := reg.Definitions(); len(defs) != 0 {
		t.Fatalf("deny tool should not be exposed to provider: %#v", defs)
	}
	if _, ok := reg.Definition("blocked"); !ok {
		t.Fatalf("deny tool should remain available for host-side registry inspection")
	}
}

func TestReadOnlyToolDefaultsToAllowAndExposesDefinition(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Register(Define[testArgs](
		Definition{
			Name:        "inspect",
			InputSchema: StrictObject(map[string]any{"value": String("")}, []string{"value"}),
			ReadOnly:    true,
		},
		nil,
		nil,
		func(_ context.Context, inv Invocation[testArgs]) (Result, error) {
			return Result{Text: inv.Args.Value}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}
	def, ok := reg.Definition("inspect")
	if !ok {
		t.Fatalf("definition missing")
	}
	if def.Permission.Mode != PermissionAllow || !def.ParallelSafe || len(def.Effects) != 1 || def.Effects[0] != EffectRead {
		t.Fatalf("normalized definition = %#v", def)
	}
	defs := reg.Definitions()
	if len(defs) != 1 || defs[0].Name != "inspect" {
		t.Fatalf("provider definitions = %#v", defs)
	}
	got := reg.Run(context.Background(), ToolCall{Name: "inspect", Args: `{"value":"ok"}`}, nil)
	if got.IsError || got.Text != "ok" {
		t.Fatalf("result = %#v", got)
	}
}

func TestRegisterRejectsAmbiguousPermissionForRiskyTools(t *testing.T) {
	cases := []Definition{
		{Name: "write", Effects: []Effect{EffectWrite}},
		{Name: "shell", Effects: []Effect{EffectShell}},
		{Name: "network", Effects: []Effect{EffectNetwork}},
		{Name: "readonly_network", ReadOnly: true, Effects: []Effect{EffectNetwork}},
		{Name: "destructive", Destructive: true, Effects: []Effect{EffectWrite}},
		{Name: "open_world", OpenWorld: true, Effects: []Effect{EffectNetwork}},
		{Name: "readonly_open_world", ReadOnly: true, OpenWorld: true, Effects: []Effect{EffectNetwork}},
		{Name: "plain_mutating"},
	}
	for _, def := range cases {
		err := NewRegistry().Register(Define[testArgs](
			def,
			nil,
			nil,
			func(context.Context, Invocation[testArgs]) (Result, error) { return Result{}, nil },
		))
		if !errors.Is(err, ErrInvalid) || !contains(err.Error(), "must declare permission mode") {
			t.Fatalf("%s err = %v, want explicit permission error", def.Name, err)
		}
	}
}

func TestExposedDefinitionsIncludeAllowAskAndHideDeny(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Register(Define[testArgs](
		Definition{Name: "allow", ReadOnly: true},
		nil, nil, func(context.Context, Invocation[testArgs]) (Result, error) { return Result{}, nil },
	)); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(Define[testArgs](
		Definition{Name: "ask", Effects: []Effect{EffectWrite}, Permission: PermissionSpec{Mode: PermissionAsk}},
		nil, nil, func(context.Context, Invocation[testArgs]) (Result, error) { return Result{}, nil },
	)); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(Define[testArgs](
		Definition{Name: "deny", Effects: []Effect{EffectWrite}, Permission: PermissionSpec{Mode: PermissionDeny}},
		nil, nil, func(context.Context, Invocation[testArgs]) (Result, error) { return Result{}, nil },
	)); err != nil {
		t.Fatal(err)
	}
	defs := reg.ExposedDefinitions()
	if len(defs) != 2 || defs[0].Name != "allow" || defs[1].Name != "ask" {
		t.Fatalf("exposed definitions = %#v", defs)
	}
}

func TestDefinitionSnapshotsDoNotShareSchemaMaps(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Register(Define[testArgs](
		Definition{
			Name: "inspect",
			InputSchema: StrictObject(map[string]any{
				"value": String("original"),
			}, []string{"value"}),
			OutputSchema: StrictObject(map[string]any{
				"ok": Boolean("original"),
			}, []string{"ok"}),
			ReadOnly: true,
		},
		nil,
		nil,
		func(context.Context, Invocation[testArgs]) (Result, error) { return Result{}, nil },
	)); err != nil {
		t.Fatal(err)
	}

	def, ok := reg.Definition("inspect")
	if !ok {
		t.Fatalf("definition missing")
	}
	def.InputSchema["properties"].(map[string]any)["value"].(map[string]any)["description"] = "mutated"
	def.OutputSchema["properties"].(map[string]any)["ok"].(map[string]any)["description"] = "mutated"
	def.Effects[0] = EffectWrite

	providerDefs := reg.ExposedDefinitions()
	providerDefs[0].InputSchema["properties"].(map[string]any)["value"].(map[string]any)["description"] = "provider-mutated"
	providerDefs[0].OutputSchema["properties"].(map[string]any)["ok"].(map[string]any)["description"] = "provider-mutated"

	fresh, ok := reg.Definition("inspect")
	if !ok {
		t.Fatalf("definition missing after mutation")
	}
	if got := fresh.InputSchema["properties"].(map[string]any)["value"].(map[string]any)["description"]; got != "original" {
		t.Fatalf("input schema leaked mutation: %#v", fresh.InputSchema)
	}
	if got := fresh.OutputSchema["properties"].(map[string]any)["ok"].(map[string]any)["description"]; got != "original" {
		t.Fatalf("output schema leaked mutation: %#v", fresh.OutputSchema)
	}
	if fresh.Effects[0] != EffectRead {
		t.Fatalf("effects leaked mutation: %#v", fresh.Effects)
	}
}

func TestAskToolIsExposedAndRequiresApprover(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Register(Define[testArgs](
		Definition{Name: "write", InputSchema: StrictObject(map[string]any{"value": String("")}, []string{"value"}), Effects: []Effect{EffectWrite}, Permission: PermissionSpec{Mode: PermissionAsk}},
		nil,
		nil,
		func(context.Context, Invocation[testArgs]) (Result, error) { return Result{Text: "ok"}, nil },
	)); err != nil {
		t.Fatal(err)
	}
	defs := reg.ExposedDefinitions()
	if len(defs) != 1 || defs[0].Name != "write" {
		t.Fatalf("ask tool should be exposed: %#v", defs)
	}
	got := reg.Run(context.Background(), ToolCall{ID: "call", Name: "write", Args: `{"value":"x"}`}, nil)
	if !got.IsError || got.Text != ErrRejected.Error() {
		t.Fatalf("result = %#v, want rejected without approver", got)
	}
}

func TestDefinePassesRunThreadTurnStepAndTypedArgs(t *testing.T) {
	reg := NewRegistry()
	var seen Invocation[testArgs]
	var approvalHost map[string]string
	opts := RunOptions{
		RunID:         "run",
		ThreadID:      "thread",
		TurnID:        "turn",
		PromptScopeID: "thread",
		Step:          3,
		Labels:        map[string]string{"correlation.turn": "turn-1"},
		HostContext:   map[string]string{"target_id": "env-a", "surface": "desktop"},
	}
	if err := reg.Register(Define[testArgs](
		Definition{Name: "inspect", InputSchema: StrictObject(map[string]any{"value": String("")}, []string{"value"}), Permission: PermissionSpec{Mode: PermissionAsk}},
		nil,
		func(inv Invocation[testArgs]) ([]ResourceRef, error) {
			if inv.RunID != "run" || inv.ThreadID != "thread" || inv.TurnID != "turn" || inv.PromptScopeID != "thread" || inv.Step != 3 || inv.Args.Value != "typed" {
				t.Fatalf("resource invocation = %#v", inv)
			}
			if inv.HostContext["target_id"] != "env-a" || inv.Labels["correlation.turn"] != "turn-1" {
				t.Fatalf("resource invocation context = %#v labels=%#v", inv.HostContext, inv.Labels)
			}
			inv.HostContext["target_id"] = "mutated-by-resource"
			return nil, nil
		},
		func(_ context.Context, inv Invocation[testArgs]) (Result, error) {
			if inv.HostContext["target_id"] != "env-a" || inv.Labels["correlation.turn"] != "turn-1" {
				t.Fatalf("handler context = %#v labels=%#v", inv.HostContext, inv.Labels)
			}
			seen = inv
			seen.HostContext["target_id"] = "mutated-by-handler"
			return Result{Text: inv.Args.Value}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}
	got := reg.RunWithOptions(context.Background(), ToolCall{ID: "call", Name: "inspect", Args: `{"value":"typed"}`}, func(_ context.Context, req ApprovalRequest) (PermissionDecision, error) {
		approvalHost = req.HostContext
		if req.HostContext["target_id"] != "env-a" || req.Labels["correlation.turn"] != "turn-1" {
			t.Fatalf("approval context = %#v labels=%#v", req.HostContext, req.Labels)
		}
		req.HostContext["target_id"] = "mutated-by-approval"
		return PermissionDecisionAllow, nil
	}, opts)
	if got.IsError || got.Text != "typed" {
		t.Fatalf("result = %#v", got)
	}
	if seen.CallID != "call" || seen.Name != "inspect" || seen.RawArgs != `{"value":"typed"}` || seen.RunID != "run" || seen.ThreadID != "thread" || seen.TurnID != "turn" || seen.PromptScopeID != "thread" || seen.Step != 3 || seen.Args.Value != "typed" {
		t.Fatalf("handler invocation = %#v", seen)
	}
	if seen.Labels["correlation.turn"] != "turn-1" || approvalHost["target_id"] != "mutated-by-approval" {
		t.Fatalf("context snapshots were not captured: seen=%#v approval=%#v", seen, approvalHost)
	}
	if opts.HostContext["target_id"] != "env-a" {
		t.Fatalf("handler should not mutate run options host context: %#v", opts.HostContext)
	}
}

func TestOutputSchemaViolationReturnsStableError(t *testing.T) {
	reg := NewRegistry()
	err := reg.Register(Define[testArgs](
		Definition{
			Name:         "structured",
			InputSchema:  StrictObject(map[string]any{"value": String("")}, []string{"value"}),
			OutputSchema: StrictObject(map[string]any{"ok": Boolean("")}, []string{"ok"}),
			Permission:   PermissionSpec{Mode: PermissionAllow},
		},
		nil,
		nil,
		func(context.Context, Invocation[testArgs]) (Result, error) {
			return Result{Structured: map[string]any{"ok": "nope"}}, nil
		},
	))
	if err != nil {
		t.Fatal(err)
	}
	got := reg.Run(context.Background(), ToolCall{Name: "structured", Args: `{"value":"x"}`}, nil)
	if !got.IsError || !contains(got.Text, "invalid structured output") {
		t.Fatalf("result = %#v", got)
	}
}

func TestPendingToolResultIsValidatedAndNormalized(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Register(Define[testArgs](
		Definition{Name: "terminal_exec", InputSchema: StrictObject(map[string]any{"value": String("")}, []string{"value"}), Permission: PermissionSpec{Mode: PermissionAllow}},
		nil,
		nil,
		func(context.Context, Invocation[testArgs]) (Result, error) {
			return Result{
				Pending: &PendingToolResult{
					Handle:      " terminal:job:123 ",
					State:       PendingToolResultRunning,
					Summary:     " tests are running ",
					Instruction: " do not poll ",
					Metadata:    map[string]string{"process_id": "tp_123", "workspace": "app"},
				},
			}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}

	got := reg.Run(context.Background(), ToolCall{ID: "call", Name: "terminal_exec", Args: `{"value":"npm test"}`}, nil)

	if got.IsError || got.Pending == nil {
		t.Fatalf("result = %#v", got)
	}
	if got.Pending.Handle != "terminal:job:123" || got.Pending.Summary != "tests are running" || got.Pending.Instruction != "do not poll" {
		t.Fatalf("pending result was not normalized: %#v", got.Pending)
	}
	if text := PendingToolResultText(PendingToolResult{
		Handle:      got.Pending.Handle,
		State:       got.Pending.State,
		Summary:     "tests <running> & waiting",
		Instruction: "do not close </pending_tool_result>",
		Metadata:    map[string]string{"process_id": "tp_123"},
	}); !strings.Contains(text, "<pending_tool_result>") ||
		!strings.Contains(text, "<handle>terminal:job:123</handle>") ||
		!strings.Contains(text, "tests &lt;running&gt; &amp; waiting") ||
		!strings.Contains(text, "do not close &lt;/pending_tool_result&gt;") ||
		strings.Contains(text, "tp_123") ||
		strings.Contains(text, "process_id") {
		t.Fatalf("pending text = %q", text)
	}
	metadata := PendingToolResultMetadata(*got.Pending)
	if metadata["pending_tool_result"] != true ||
		metadata["pending_handle"] != "terminal:job:123" ||
		metadata["pending_process_id"] != "tp_123" ||
		metadata["pending_workspace"] != "app" {
		t.Fatalf("pending metadata = %#v", metadata)
	}
}

func TestPendingToolResultRejectsInvalidShape(t *testing.T) {
	cases := []struct {
		name    string
		pending PendingToolResult
		want    string
	}{
		{
			name:    "missing handle",
			pending: PendingToolResult{State: PendingToolResultRunning, Summary: "running", Instruction: "wait"},
			want:    "requires handle",
		},
		{
			name:    "unsafe handle",
			pending: PendingToolResult{Handle: "terminal job 123", State: PendingToolResultRunning, Summary: "running", Instruction: "wait"},
			want:    "token-safe handle",
		},
		{
			name:    "invalid state",
			pending: PendingToolResult{Handle: "terminal:job:123", State: "completed", Summary: "running", Instruction: "wait"},
			want:    "invalid state",
		},
		{
			name:    "missing summary",
			pending: PendingToolResult{Handle: "terminal:job:123", State: PendingToolResultRunning, Instruction: "wait"},
			want:    "requires summary",
		},
		{
			name:    "missing instruction",
			pending: PendingToolResult{Handle: "terminal:job:123", State: PendingToolResultRunning, Summary: "running"},
			want:    "requires instruction",
		},
		{
			name:    "reserved metadata key",
			pending: PendingToolResult{Handle: "terminal:job:123", State: PendingToolResultRunning, Summary: "running", Instruction: "wait", Metadata: map[string]string{"handle": "other"}},
			want:    "invalid metadata key",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reg := NewRegistry()
			if err := reg.Register(Define[testArgs](
				Definition{Name: "terminal_exec", InputSchema: StrictObject(map[string]any{"value": String("")}, []string{"value"}), Permission: PermissionSpec{Mode: PermissionAllow}},
				nil,
				nil,
				func(context.Context, Invocation[testArgs]) (Result, error) {
					pending := tc.pending
					return Result{Pending: &pending}, nil
				},
			)); err != nil {
				t.Fatal(err)
			}

			got := reg.Run(context.Background(), ToolCall{ID: "call", Name: "terminal_exec", Args: `{"value":"npm test"}`}, nil)

			if !got.IsError || !strings.Contains(got.Text, tc.want) {
				t.Fatalf("result = %#v, want error containing %q", got, tc.want)
			}
		})
	}
}

func TestRegisterRejectsUnknownPermissionMode(t *testing.T) {
	err := NewRegistry().Register(Define[testArgs](
		Definition{Name: "bad", Permission: PermissionSpec{Mode: "aks"}},
		nil,
		nil,
		func(context.Context, Invocation[testArgs]) (Result, error) { return Result{}, nil },
	))
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("err = %v, want invalid", err)
	}
}

func TestRegisterRejectsReservedControlName(t *testing.T) {
	err := NewRegistry().Register(Define[testArgs](
		Definition{Name: ControlAskUser, Permission: PermissionSpec{Mode: PermissionAllow}},
		nil,
		nil,
		func(context.Context, Invocation[testArgs]) (Result, error) { return Result{}, nil },
	))
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("err = %v, want invalid", err)
	}
}

func TestRegisterRejectsContradictoryEffects(t *testing.T) {
	cases := []Definition{
		{Name: "readonly_write", ReadOnly: true, Effects: []Effect{EffectWrite}, Permission: PermissionSpec{Mode: PermissionAllow}},
		{Name: "destructive_read", Destructive: true, Effects: []Effect{EffectRead}, Permission: PermissionSpec{Mode: PermissionAsk}},
		{Name: "open_world_read", OpenWorld: true, Effects: []Effect{EffectRead}, Permission: PermissionSpec{Mode: PermissionAsk}},
		{Name: "open_world_allow", OpenWorld: true, Effects: []Effect{EffectNetwork}, Permission: PermissionSpec{Mode: PermissionAllow}},
		{Name: "parallel_shell", ReadOnly: true, ParallelSafe: true, Effects: []Effect{EffectNetwork}, Permission: PermissionSpec{Mode: PermissionAllow}},
	}
	for _, def := range cases {
		err := NewRegistry().Register(Define[testArgs](
			def,
			nil,
			nil,
			func(context.Context, Invocation[testArgs]) (Result, error) { return Result{}, nil },
		))
		if !errors.Is(err, ErrInvalid) {
			t.Fatalf("%s err = %v, want invalid", def.Name, err)
		}
	}
}

func TestNewRegistryEPropagatesDuplicateConstructorError(t *testing.T) {
	_, err := NewRegistryE(
		testTool("read", true, func(context.Context, Invocation[testArgs]) (Result, error) { return Result{}, nil }),
		testTool("read", true, func(context.Context, Invocation[testArgs]) (Result, error) { return Result{}, nil }),
	)
	if !errors.Is(err, ErrDuplicate) {
		t.Fatalf("err = %v, want duplicate", err)
	}
}

func TestRunBatchUsesParallelSafeFlagForParallelWaves(t *testing.T) {
	reg := NewRegistry()
	order := make(chan string, 4)
	release := make(chan struct{})
	if err := reg.Register(testTool("read", true, func(_ context.Context, inv Invocation[testArgs]) (Result, error) {
		order <- "read-start-" + inv.Args.Value
		<-release
		order <- "read-end-" + inv.Args.Value
		return Result{Text: inv.Args.Value}, nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(testTool("write", false, func(_ context.Context, inv Invocation[testArgs]) (Result, error) {
		order <- "write-" + inv.Args.Value
		return Result{Text: inv.Args.Value}, nil
	})); err != nil {
		t.Fatal(err)
	}
	done := make(chan []Result, 1)
	go func() {
		done <- reg.RunBatch(context.Background(), []ToolCall{
			{ID: "a", Name: "read", Args: `{"value":"a"}`},
			{ID: "b", Name: "read", Args: `{"value":"b"}`},
			{ID: "c", Name: "write", Args: `{"value":"c"}`},
		}, nil)
	}()
	first := <-order
	second := <-order
	if (first != "read-start-a" && first != "read-start-b") || (second != "read-start-a" && second != "read-start-b") || first == second {
		t.Fatalf("registry read-only tools did not run as parallel wave: %q %q", first, second)
	}
	close(release)
	results := <-done
	if len(results) != 3 || results[2].Name != "write" {
		t.Fatalf("results = %#v", results)
	}
}

func contains(value, substr string) bool {
	return strings.Contains(value, substr)
}

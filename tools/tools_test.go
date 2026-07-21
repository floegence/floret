package tools

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

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

func TestRegistryRejectsEmptyToolArgsWithoutSubstitution(t *testing.T) {
	called := false
	reg := NewRegistry()
	if err := reg.Register(testTool("read", true, func(context.Context, Invocation[testArgs]) (Result, error) {
		called = true
		return Result{}, nil
	})); err != nil {
		t.Fatal(err)
	}
	got := reg.Run(context.Background(), ToolCall{ID: "call-1", Name: "read"}, nil)
	if !got.IsError || called || !strings.Contains(got.Text, "invalid JSON") {
		t.Fatalf("empty args result = %#v called=%v", got, called)
	}
}

func TestRegistryDispatchRequiresEffectAuthorityBeforeToolSideEffects(t *testing.T) {
	called := false
	reg := NewRegistry()
	if err := reg.Register(Define[testArgs](
		Definition{
			Name:        "write",
			InputSchema: StrictObject(map[string]any{"value": String("")}, []string{"value"}),
			Effects:     []Effect{EffectWrite},
			Permission:  PermissionSpec{Mode: PermissionAllow},
		},
		nil,
		func(Invocation[testArgs]) ([]ResourceRef, error) {
			called = true
			return nil, nil
		},
		func(context.Context, Invocation[testArgs]) (Result, error) {
			called = true
			return Result{Text: "must not run"}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}
	result := reg.Dispatch(context.Background(), ToolCall{ID: "call", Name: "write", Args: `{"value":"x"}`}, DispatchOptions{})
	if !result.IsError || result.Text != ErrEffectDispatcherRequired.Error() || called {
		t.Fatalf("result=%#v called=%v", result, called)
	}
}

func TestRegistryDispatchInvokesHandlerOnlyInsideEffectDispatcher(t *testing.T) {
	type dispatchContextKey struct{}
	handlerCalled := false
	dispatcherCalled := false
	selectedContext := context.WithValue(context.Background(), dispatchContextKey{}, "selected")
	reg := NewRegistry()
	if err := reg.Register(testTool("read", true, func(ctx context.Context, _ Invocation[testArgs]) (Result, error) {
		handlerCalled = true
		if !dispatcherCalled {
			t.Fatal("handler ran outside effect dispatcher")
		}
		if got := ctx.Value(dispatchContextKey{}); got != "selected" {
			t.Fatalf("handler context value = %v, want selected dispatcher context", got)
		}
		return Result{Text: "ok"}, nil
	})); err != nil {
		t.Fatal(err)
	}
	opts := DispatchOptions{
		EffectDispatcher: func(_ context.Context, req EffectDispatchRequest, invoke func(context.Context) Result) Result {
			dispatcherCalled = true
			if req.CallID != "call" || req.Name != "read" || req.Permission.Mode != PermissionAllow {
				t.Fatalf("dispatch request = %#v", req)
			}
			return invoke(selectedContext)
		},
	}
	result := reg.Dispatch(context.Background(), ToolCall{ID: "call", Name: "read", Args: `{"value":"x"}`}, opts)
	if result.IsError || result.Text != "ok" || !dispatcherCalled || !handlerCalled || !result.RequiresEffectFinalization() {
		t.Fatalf("result=%#v dispatcher=%v handler=%v", result, dispatcherCalled, handlerCalled)
	}
	unknown := reg.Dispatch(context.Background(), ToolCall{ID: "missing", Name: "missing", Args: `{}`}, opts)
	if !unknown.IsError || unknown.RequiresEffectFinalization() {
		t.Fatalf("pre-dispatch failure incorrectly requires effect finalization: %#v", unknown)
	}
}

func TestRegisterValidatesRepeatIdentityIgnoredArguments(t *testing.T) {
	t.Parallel()

	valid := testTool("poll", true, func(context.Context, Invocation[testArgs]) (Result, error) {
		return Result{}, nil
	})
	valid.Definition.InputSchema = StrictObject(map[string]any{
		"value":       String("poll identity"),
		"description": String("user-facing activity description"),
	}, []string{"value", "description"})
	valid.Definition.Annotations = map[string]any{
		AnnotationRepeatPolicy:                   RepeatPolicyPolling,
		AnnotationRepeatIdentityIgnoredArguments: []string{"description"},
	}
	if err := NewRegistry().Register(valid); err != nil {
		t.Fatalf("valid polling annotations rejected: %v", err)
	}

	tests := []struct {
		name        string
		annotations map[string]any
	}{
		{name: "requires polling policy", annotations: map[string]any{AnnotationRepeatIdentityIgnoredArguments: []string{"description"}}},
		{name: "requires string array", annotations: map[string]any{AnnotationRepeatPolicy: RepeatPolicyPolling, AnnotationRepeatIdentityIgnoredArguments: "description"}},
		{name: "requires nonempty array", annotations: map[string]any{AnnotationRepeatPolicy: RepeatPolicyPolling, AnnotationRepeatIdentityIgnoredArguments: []string{}}},
		{name: "rejects duplicate fields", annotations: map[string]any{AnnotationRepeatPolicy: RepeatPolicyPolling, AnnotationRepeatIdentityIgnoredArguments: []string{"description", "description"}}},
		{name: "rejects unknown fields", annotations: map[string]any{AnnotationRepeatPolicy: RepeatPolicyPolling, AnnotationRepeatIdentityIgnoredArguments: []string{"missing"}}},
		{name: "rejects unknown policy", annotations: map[string]any{AnnotationRepeatPolicy: "retry"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tool := valid
			tool.Definition.Annotations = tt.annotations
			if err := NewRegistry().Register(tool); !errors.Is(err, ErrInvalid) {
				t.Fatalf("err = %v, want invalid", err)
			}
		})
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
	got := runWithOptionsForTest(reg, context.Background(), ToolCall{ID: "call", Name: "shell", Args: `{"value":"rm file"}`}, func(_ context.Context, req ApprovalRequest) (PermissionDecision, error) {
		approval = req
		return PermissionDecisionAllow, nil
	}, DispatchOptions{RunID: "run", ThreadID: "thread", TurnID: "turn", Step: 2, Labels: map[string]string{"correlation.turn": "turn"}})
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
	got := runWithOptionsForTest(reg, context.Background(), ToolCall{ID: "call", Name: "blocked", Args: `{"value":"x"}`}, func(context.Context, ApprovalRequest) (PermissionDecision, error) {
		called = true
		return PermissionDecisionAllow, nil
	}, DispatchOptions{RunID: "run", ThreadID: "session", Step: 7})
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
	if def.Permission.Mode != PermissionAllow || len(def.Effects) != 1 || def.Effects[0] != EffectRead {
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
	opts := DispatchOptions{
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
	got := runWithOptionsForTest(reg, context.Background(), ToolCall{ID: "call", Name: "inspect", Args: `{"value":"typed"}`}, func(_ context.Context, req ApprovalRequest) (PermissionDecision, error) {
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

func TestRunBatchStartsEveryCallConcurrentlyAndKeepsResultOrder(t *testing.T) {
	reg := NewRegistry()
	started := make(chan string, 3)
	release := make(chan struct{})
	definitions := []Definition{
		{Name: "read", ReadOnly: true, Effects: []Effect{EffectRead}, Permission: PermissionSpec{Mode: PermissionAllow}},
		{Name: "write", Effects: []Effect{EffectWrite}, Permission: PermissionSpec{Mode: PermissionAllow}},
		{Name: "shell", Effects: []Effect{EffectShell}, Permission: PermissionSpec{Mode: PermissionAllow}},
	}
	for _, definition := range definitions {
		definition.InputSchema = StrictObject(map[string]any{"value": String("test value")}, []string{"value"})
		if err := reg.Register(Define[testArgs](definition, nil, nil, func(_ context.Context, inv Invocation[testArgs]) (Result, error) {
			started <- inv.Name
			<-release
			return Result{Text: inv.Args.Value}, nil
		})); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan []Result, 1)
	go func() {
		done <- reg.RunBatch(context.Background(), []ToolCall{
			{ID: "a", Name: "read", Args: `{"value":"a"}`},
			{ID: "b", Name: "write", Args: `{"value":"b"}`},
			{ID: "c", Name: "shell", Args: `{"value":"c"}`},
		}, nil)
	}()
	seen := map[string]bool{}
	for range definitions {
		select {
		case name := <-started:
			seen[name] = true
		case <-time.After(time.Second):
			t.Fatalf("tool batch did not start concurrently: started=%v", seen)
		}
	}
	close(release)
	results := <-done
	if len(results) != 3 || results[0].CallID != "a" || results[1].CallID != "b" || results[2].CallID != "c" {
		t.Fatalf("results = %#v", results)
	}
}

func TestRunBatchApprovalRequestsStartConcurrentlyWithBatchOrder(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Register(Define[testArgs](
		Definition{
			Name:        "approve",
			InputSchema: StrictObject(map[string]any{"value": String("test value")}, []string{"value"}),
			Effects:     []Effect{EffectShell},
			Permission:  PermissionSpec{Mode: PermissionAsk},
		},
		nil,
		nil,
		func(_ context.Context, inv Invocation[testArgs]) (Result, error) {
			return Result{Text: inv.Args.Value}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}
	requests := make(chan ApprovalRequest, 2)
	release := make(chan struct{})
	done := make(chan []Result, 1)
	go func() {
		done <- reg.RunBatch(context.Background(), []ToolCall{
			{ID: "a", Name: "approve", Args: `{"value":"a"}`},
			{ID: "b", Name: "approve", Args: `{"value":"b"}`},
		}, func(_ context.Context, req ApprovalRequest) (PermissionDecision, error) {
			requests <- req
			<-release
			return PermissionDecisionAllow, nil
		})
	}()
	byID := map[string]ApprovalRequest{}
	for range 2 {
		select {
		case req := <-requests:
			byID[req.ID] = req
		case <-time.After(time.Second):
			t.Fatalf("approval requests did not start concurrently: %#v", byID)
		}
	}
	if byID["a"].BatchIndex != 0 || byID["a"].BatchSize != 2 || byID["b"].BatchIndex != 1 || byID["b"].BatchSize != 2 {
		t.Fatalf("approval batch metadata = %#v", byID)
	}
	close(release)
	if results := <-done; len(results) != 2 || results[0].CallID != "a" || results[1].CallID != "b" {
		t.Fatalf("results = %#v", results)
	}
}

func TestDispatchBatchPreflightsOnlyValidDispatchesInModelOrder(t *testing.T) {
	reg := NewRegistry()
	started := make(chan string, 2)
	for _, definition := range []Definition{
		{Name: "ask", Effects: []Effect{EffectWrite}, Permission: PermissionSpec{Mode: PermissionAsk}},
		{Name: "allow", ReadOnly: true, Effects: []Effect{EffectRead}, Permission: PermissionSpec{Mode: PermissionAllow}},
		{Name: "deny", Effects: []Effect{EffectShell}, Permission: PermissionSpec{Mode: PermissionDeny}},
	} {
		definition.InputSchema = StrictObject(map[string]any{"value": String("test value")}, []string{"value"})
		if err := reg.Register(Define[testArgs](definition, nil, nil, func(_ context.Context, inv Invocation[testArgs]) (Result, error) {
			started <- inv.CallID
			return Result{Text: inv.Args.Value}, nil
		})); err != nil {
			t.Fatal(err)
		}
	}
	preflight := make(chan []EffectDispatchRequest, 1)
	release := make(chan struct{})
	done := make(chan []Result, 1)
	go func() {
		done <- reg.DispatchBatch(context.Background(), []ToolCall{
			{ID: "unknown", Name: "missing", Args: `{"value":"unknown"}`},
			{ID: "ask", Name: "ask", Args: `{"value":"ask"}`},
			{ID: "invalid", Name: "allow", Args: `{`},
			{ID: "deny", Name: "deny", Args: `{"value":"deny"}`},
			{ID: "allow", Name: "allow", Args: `{"value":"allow"}`},
		}, DispatchOptions{
			RunID: "run", ThreadID: "thread", TurnID: "turn", Step: 1,
			EffectBatchPreflight: func(_ context.Context, requests []EffectDispatchRequest) error {
				preflight <- requests
				<-release
				return nil
			},
			EffectDispatcher: func(ctx context.Context, _ EffectDispatchRequest, invoke func(context.Context) Result) Result {
				return invoke(ctx)
			},
		})
	}()
	var requests []EffectDispatchRequest
	select {
	case requests = <-preflight:
	case <-time.After(time.Second):
		t.Fatal("batch preflight was not called")
	}
	if len(requests) != 2 || requests[0].CallID != "ask" || requests[0].BatchIndex != 1 || requests[0].BatchSize != 5 ||
		requests[1].CallID != "allow" || requests[1].BatchIndex != 4 || requests[1].BatchSize != 5 {
		t.Fatalf("preflight requests = %#v", requests)
	}
	select {
	case callID := <-started:
		t.Fatalf("handler %q started before batch preflight completed", callID)
	default:
	}
	close(release)
	results := <-done
	if len(results) != 5 || !results[0].IsError || results[1].IsError || !results[2].IsError || !results[3].IsError || results[4].IsError {
		t.Fatalf("batch results = %#v", results)
	}
}

func TestDispatchBatchPreflightFailurePreventsEveryPreparedHandler(t *testing.T) {
	reg := NewRegistry()
	called := 0
	if err := reg.Register(testTool("read", true, func(context.Context, Invocation[testArgs]) (Result, error) {
		called++
		return Result{Text: "unexpected"}, nil
	})); err != nil {
		t.Fatal(err)
	}
	results := reg.DispatchBatch(context.Background(), []ToolCall{
		{ID: "a", Name: "read", Args: `{"value":"a"}`},
		{ID: "b", Name: "read", Args: `{"value":"b"}`},
	}, DispatchOptions{
		EffectBatchPreflight: func(context.Context, []EffectDispatchRequest) error { return errors.New("preflight failed") },
		EffectDispatcher: func(ctx context.Context, _ EffectDispatchRequest, invoke func(context.Context) Result) Result {
			return invoke(ctx)
		},
	})
	if called != 0 || len(results) != 2 || !results[0].IsError || !results[1].IsError ||
		!strings.Contains(results[0].Text, "preflight failed") || !strings.Contains(results[1].Text, "preflight failed") {
		t.Fatalf("called=%d results=%#v", called, results)
	}
}

func contains(value, substr string) bool {
	return strings.Contains(value, substr)
}

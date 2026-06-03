package tools

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/floegence/floret/provider"
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
			ReadOnly: readOnly,
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
	got := reg.Run(context.Background(), provider.ToolCall{Name: "read", Args: `{"value":""}`}, nil)
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
	got := NewRegistry().Run(context.Background(), provider.ToolCall{Name: "missing"}, nil)
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
	got := reg.Run(context.Background(), provider.ToolCall{Name: "read", Args: `{"extra":1}`}, func(context.Context, ApprovalRequest) (PermissionDecision, error) {
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
	got := reg.Run(context.Background(), provider.ToolCall{Name: "panic", Args: `{"value":"x"}`}, nil)
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
	got := reg.Run(context.Background(), provider.ToolCall{ID: "1", Name: "write", Args: `{"value":"original"}`}, func(_ context.Context, req ApprovalRequest) (PermissionDecision, error) {
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
	got := reg.RunWithOptions(context.Background(), provider.ToolCall{ID: "call", Name: "blocked", Args: `{"value":"x"}`}, func(context.Context, ApprovalRequest) (PermissionDecision, error) {
		called = true
		return PermissionDecisionAllow, nil
	}, RunOptions{RunID: "run", SessionID: "session", Step: 7, CWD: "/tmp"})
	if !got.IsError || got.Text != ErrRejected.Error() {
		t.Fatalf("result = %#v", got)
	}
	if called {
		t.Fatalf("deny should not call resource extractor, approver, or handler")
	}
}

func TestDefinePassesRunSessionStepCWDAndTypedArgs(t *testing.T) {
	reg := NewRegistry()
	var seen Invocation[testArgs]
	if err := reg.Register(Define[testArgs](
		Definition{Name: "inspect", InputSchema: StrictObject(map[string]any{"value": String("")}, []string{"value"})},
		nil,
		func(inv Invocation[testArgs]) ([]ResourceRef, error) {
			if inv.RunID != "run" || inv.SessionID != "session" || inv.Step != 3 || inv.CWD != "/repo" || inv.Args.Value != "typed" {
				t.Fatalf("resource invocation = %#v", inv)
			}
			return nil, nil
		},
		func(_ context.Context, inv Invocation[testArgs]) (Result, error) {
			seen = inv
			return Result{Text: inv.Args.Value}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}
	got := reg.RunWithOptions(context.Background(), provider.ToolCall{ID: "call", Name: "inspect", Args: `{"value":"typed"}`}, nil, RunOptions{RunID: "run", SessionID: "session", Step: 3, CWD: "/repo"})
	if got.IsError || got.Text != "typed" {
		t.Fatalf("result = %#v", got)
	}
	if seen.CallID != "call" || seen.Name != "inspect" || seen.RawArgs != `{"value":"typed"}` || seen.RunID != "run" || seen.SessionID != "session" || seen.Step != 3 || seen.CWD != "/repo" || seen.Args.Value != "typed" {
		t.Fatalf("handler invocation = %#v", seen)
	}
}

func TestOutputSchemaViolationReturnsStableError(t *testing.T) {
	reg := NewRegistry()
	err := reg.Register(Define[testArgs](
		Definition{
			Name:         "structured",
			InputSchema:  StrictObject(map[string]any{"value": String("")}, []string{"value"}),
			OutputSchema: StrictObject(map[string]any{"ok": Boolean("")}, []string{"ok"}),
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
	got := reg.Run(context.Background(), provider.ToolCall{Name: "structured", Args: `{"value":"x"}`}, nil)
	if !got.IsError || !contains(got.Text, "invalid structured output") {
		t.Fatalf("result = %#v", got)
	}
}

func TestRunBatchUsesRegistryReadOnlyFlagForParallelWaves(t *testing.T) {
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
		done <- reg.RunBatch(context.Background(), []provider.ToolCall{
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

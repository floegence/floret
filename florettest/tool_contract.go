package florettest

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/tools"
)

// ToolContractInvocation is the product-neutral invocation shape exercised by
// RunToolContract. Factory implementations should preserve every identity and
// pass Value to the supplied callbacks without adding host policy.
type ToolContractInvocation struct {
	CallID        string
	Name          string
	RawArgs       string
	Value         string
	RunID         string
	ThreadID      string
	TurnID        string
	PromptScopeID string
	Step          int
	Labels        map[string]string
	HostContext   map[string]string
}

// ToolContractSpec describes one tool using only public tools package inputs.
type ToolContractSpec struct {
	Name      string
	Resources func(ToolContractInvocation) ([]tools.ResourceRef, error)
	Handler   func(context.Context, ToolContractInvocation) (tools.Result, error)
}

// ToolContractFactory adapts ToolContractSpec through consumer-owned tool
// construction code and returns the registry that exposes the resulting tool.
type ToolContractFactory func(testing.TB, ToolContractSpec) *tools.Registry

// PublicToolContractFactory builds the contract tool directly with tools.Define.
func PublicToolContractFactory(t testing.TB, spec ToolContractSpec) *tools.Registry {
	t.Helper()
	type args struct {
		Value string `json:"value"`
	}
	tool := tools.Define[args](
		tools.Definition{
			Name:        spec.Name,
			Description: "Exercises a consumer tool contract.",
			InputSchema: tools.StrictObject(map[string]any{
				"value": tools.String("contract value"),
			}, []string{"value"}),
			Effects:    []tools.Effect{tools.EffectRead},
			ReadOnly:   true,
			Permission: tools.PermissionSpec{Mode: tools.PermissionAllow},
		},
		nil,
		func(inv tools.Invocation[args]) ([]tools.ResourceRef, error) {
			if spec.Resources == nil {
				return nil, nil
			}
			return spec.Resources(contractToolInvocation(inv, inv.Args.Value))
		},
		func(ctx context.Context, inv tools.Invocation[args]) (tools.Result, error) {
			if spec.Handler == nil {
				return tools.Result{}, errors.New("florettest: tool contract handler is required")
			}
			return spec.Handler(ctx, contractToolInvocation(inv, inv.Args.Value))
		},
	)
	registry, err := tools.NewRegistryE(tool)
	if err != nil {
		t.Fatalf("florettest: construct public contract tool: %v", err)
	}
	return registry
}

// RunToolContract verifies the public behavioral baseline for a consumer tool
// constructor or wrapper.
func RunToolContract(t *testing.T, factory ToolContractFactory) {
	t.Helper()
	if factory == nil {
		t.Fatal("florettest: tool contract factory is required")
	}

	t.Run("schema validation", func(t *testing.T) {
		called := false
		registry := contractRegistry(t, factory, ToolContractSpec{
			Name: "contract_schema",
			Handler: func(context.Context, ToolContractInvocation) (tools.Result, error) {
				called = true
				return tools.Result{Text: "unexpected"}, nil
			},
		})
		result := registry.Dispatch(context.Background(), tools.ToolCall{ID: "schema-call", Name: "contract_schema", Args: `{}`}, contractDispatchOptions())
		if !result.IsError || called || !strings.Contains(result.Text, "invalid arguments") {
			t.Fatalf("invalid schema result=%#v handler_called=%t", result, called)
		}
	})

	t.Run("resource extraction and identity", func(t *testing.T) {
		var got ToolContractInvocation
		registry := contractRegistry(t, factory, ToolContractSpec{
			Name: "contract_resource",
			Resources: func(inv ToolContractInvocation) ([]tools.ResourceRef, error) {
				got = inv
				return []tools.ResourceRef{{Kind: "file", Value: inv.Value}}, nil
			},
			Handler: func(_ context.Context, inv ToolContractInvocation) (tools.Result, error) {
				return tools.Result{Text: inv.Value}, nil
			},
		})
		var request tools.EffectDispatchRequest
		opts := contractDispatchOptions()
		opts.EffectDispatcher = func(ctx context.Context, req tools.EffectDispatchRequest, invoke func(context.Context) tools.Result) tools.Result {
			request = req
			return invoke(ctx)
		}
		result := registry.Dispatch(context.Background(), tools.ToolCall{ID: "resource-call", Name: "contract_resource", Args: `{"value":"notes.md"}`}, opts)
		if result.IsError || result.Text != "notes.md" || got.Value != "notes.md" || got.CallID != "resource-call" ||
			got.RunID != "contract-run" || got.ThreadID != "contract-thread" || got.TurnID != "contract-turn" || got.PromptScopeID != "contract-thread" ||
			len(request.Resources) != 1 || request.Resources[0] != (tools.ResourceRef{Kind: "file", Value: "notes.md"}) {
			t.Fatalf("resource result=%#v invocation=%#v request=%#v", result, got, request)
		}
	})

	t.Run("timeout", func(t *testing.T) {
		registry := contractRegistry(t, factory, ToolContractSpec{
			Name: "contract_timeout",
			Handler: func(ctx context.Context, _ ToolContractInvocation) (tools.Result, error) {
				<-ctx.Done()
				return tools.Result{}, ctx.Err()
			},
		})
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		result := registry.Dispatch(ctx, tools.ToolCall{ID: "timeout-call", Name: "contract_timeout", Args: `{"value":"wait"}`}, contractDispatchOptions())
		if !result.IsError || !strings.Contains(result.Text, context.DeadlineExceeded.Error()) {
			t.Fatalf("timeout result=%#v", result)
		}
	})

	t.Run("panic and error mapping", func(t *testing.T) {
		t.Run("panic", func(t *testing.T) {
			registry := contractRegistry(t, factory, ToolContractSpec{
				Name: "contract_panic",
				Handler: func(context.Context, ToolContractInvocation) (tools.Result, error) {
					panic("contract panic")
				},
			})
			result := registry.Dispatch(context.Background(), tools.ToolCall{ID: "panic-call", Name: "contract_panic", Args: `{"value":"panic"}`}, contractDispatchOptions())
			if !result.IsError || !strings.Contains(result.Text, "contract panic") {
				t.Fatalf("panic result=%#v", result)
			}
		})
		t.Run("error", func(t *testing.T) {
			registry := contractRegistry(t, factory, ToolContractSpec{
				Name: "contract_error",
				Handler: func(context.Context, ToolContractInvocation) (tools.Result, error) {
					return tools.Result{}, errors.New("contract handler error")
				},
			})
			result := registry.Dispatch(context.Background(), tools.ToolCall{ID: "error-call", Name: "contract_error", Args: `{"value":"error"}`}, contractDispatchOptions())
			if !result.IsError || !strings.Contains(result.Text, "contract handler error") {
				t.Fatalf("error result=%#v", result)
			}
		})
	})

	t.Run("parallel ordinary calls", func(t *testing.T) {
		entered := make(chan string, 2)
		release := make(chan struct{})
		registry := contractRegistry(t, factory, ToolContractSpec{
			Name: "contract_parallel",
			Handler: func(ctx context.Context, inv ToolContractInvocation) (tools.Result, error) {
				entered <- inv.Value
				select {
				case <-ctx.Done():
					return tools.Result{}, ctx.Err()
				case <-release:
					return tools.Result{Text: inv.Value}, nil
				}
			},
		})
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var results []tools.Result
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			results = registry.DispatchBatch(ctx, []tools.ToolCall{
				{ID: "parallel-a", Name: "contract_parallel", Args: `{"value":"a"}`},
				{ID: "parallel-b", Name: "contract_parallel", Args: `{"value":"b"}`},
			}, contractDispatchOptions())
		}()
		for index := 0; index < 2; index++ {
			select {
			case <-entered:
			case <-ctx.Done():
				close(release)
				wg.Wait()
				t.Fatal("ordinary calls did not enter concurrently")
			}
		}
		close(release)
		wg.Wait()
		if len(results) != 2 || results[0].IsError || results[1].IsError || results[0].Text != "a" || results[1].Text != "b" {
			t.Fatalf("parallel results=%#v", results)
		}
	})
}

func contractRegistry(t testing.TB, factory ToolContractFactory, spec ToolContractSpec) *tools.Registry {
	t.Helper()
	registry := factory(t, spec)
	if registry == nil {
		t.Fatal("florettest: tool contract factory returned nil")
	}
	return registry
}

func contractDispatchOptions() tools.DispatchOptions {
	return tools.DispatchOptions{
		RunID: "contract-run", ThreadID: "contract-thread", TurnID: "contract-turn", PromptScopeID: "contract-thread", Step: 1,
		Labels: map[string]string{"contract": "tool"}, HostContext: map[string]string{"surface": "florettest"},
		EffectDispatcher: func(ctx context.Context, _ tools.EffectDispatchRequest, invoke func(context.Context) tools.Result) tools.Result {
			return invoke(ctx)
		},
	}
}

func contractToolInvocation[T any](inv tools.Invocation[T], value string) ToolContractInvocation {
	return ToolContractInvocation{
		CallID: inv.CallID, Name: inv.Name, RawArgs: inv.RawArgs, Value: value,
		RunID: inv.RunID, ThreadID: inv.ThreadID, TurnID: inv.TurnID, PromptScopeID: inv.PromptScopeID,
		Step: inv.Step, Labels: cloneStringMap(inv.Labels), HostContext: cloneStringMap(inv.HostContext),
	}
}

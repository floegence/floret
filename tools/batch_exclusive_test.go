package tools

import (
	"context"
	"strings"
	"sync"
	"testing"
)

func TestBatchExclusiveGroupRejectsOnlyConflictingPreparedCalls(t *testing.T) {
	for _, tc := range []struct {
		name       string
		calls      []ToolCall
		wantErrors []bool
	}{
		{"independent", []ToolCall{{ID: "a", Name: "select", Args: "{}"}, {ID: "b", Name: "other", Args: "{}"}}, []bool{false, false}},
		{"conflict", []ToolCall{{ID: "a", Name: "select", Args: "{}"}, {ID: "b", Name: "operate", Args: "{}"}, {ID: "c", Name: "other", Args: "{}"}}, []bool{true, true, false}},
		{"same tool", []ToolCall{{ID: "a", Name: "operate", Args: "{}"}, {ID: "b", Name: "operate", Args: "{}"}}, []bool{true, true}},
		{"invalid sibling", []ToolCall{{ID: "a", Name: "operate", Args: "{}"}, {ID: "b", Name: "select", Args: "{"}}, []bool{false, true}},
		{"distinct groups", []ToolCall{{ID: "a", Name: "select", Args: "{}"}, {ID: "b", Name: "separate", Args: "{}"}}, []bool{false, false}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg := NewRegistry()
			var mu sync.Mutex
			invoked := map[string]bool{}
			authorized := map[string]bool{}
			for name, group := range map[string]string{"select": "control", "operate": "control", "other": "", "separate": "another"} {
				def := Definition{Name: name, InputSchema: StrictObject(map[string]any{}, nil), Permission: PermissionSpec{Mode: PermissionAllow}}
				def.BatchExclusiveGroup = group
				if err := reg.Register(Define[map[string]any](def, nil, nil, func(_ context.Context, inv Invocation[map[string]any]) (Result, error) {
					mu.Lock()
					invoked[inv.CallID] = true
					mu.Unlock()
					return Result{Text: "ok"}, nil
				})); err != nil {
					t.Fatal(err)
				}
			}
			var preflight []EffectDispatchRequest
			got := reg.DispatchBatch(context.Background(), tc.calls, DispatchOptions{
				EffectBatchPreflight: func(_ context.Context, requests []EffectDispatchRequest) error { preflight = requests; return nil },
				EffectDispatcher: func(ctx context.Context, req EffectDispatchRequest, invoke func(context.Context) Result) Result {
					mu.Lock()
					authorized[req.CallID] = true
					mu.Unlock()
					return invoke(ctx)
				},
			})
			accepted := 0
			for i, result := range got {
				if result.IsError != tc.wantErrors[i] || invoked[tc.calls[i].ID] == tc.wantErrors[i] || authorized[tc.calls[i].ID] == tc.wantErrors[i] {
					t.Fatalf("result %d=%+v invoked=%v authorized=%v", i, result, invoked, authorized)
				}
				if !result.IsError {
					accepted++
				} else if tc.calls[i].Args == "{}" && !strings.Contains(result.Text, "no action was executed") {
					t.Fatalf("missing execution boundary: %+v", result)
				}
			}
			if len(preflight) != accepted {
				t.Fatalf("preflight=%+v want %d", preflight, accepted)
			}
		})
	}
}

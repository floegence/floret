package tools

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/floegence/floret/provider"
)

var ErrRejected = errors.New("tool call rejected")
var ErrDuplicate = errors.New("duplicate tool name")
var ErrInvalid = errors.New("invalid tool")

type Handler func(context.Context, string) (string, error)

type Tool struct {
	Name             string
	Description      string
	ReadOnly         bool
	RequiresApproval bool
	Handler          Handler
}

type ApprovalRequest struct {
	ID   string
	Name string
	Args string
}

type Approver func(context.Context, ApprovalRequest) (bool, error)

type Result struct {
	Call provider.ToolCall
	Text string
	Err  error
}

type Registry struct {
	mu    sync.RWMutex
	tools map[string]Tool
}

func NewRegistry(items ...Tool) *Registry {
	r := &Registry{tools: map[string]Tool{}}
	for _, item := range items {
		_ = r.Register(item)
	}
	return r
}

func (r *Registry) Register(t Tool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if t.Name == "" || t.Handler == nil {
		return ErrInvalid
	}
	if _, ok := r.tools[t.Name]; ok {
		return ErrDuplicate
	}
	r.tools[t.Name] = t
	return nil
}

func (r *Registry) Run(ctx context.Context, call provider.ToolCall, approver Approver) Result {
	return r.run(ctx, call, approver)
}

func (r *Registry) run(ctx context.Context, call provider.ToolCall, approver Approver) (result Result) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = Result{Call: call, Err: fmt.Errorf("tool %q panicked: %v", call.Name, recovered)}
		}
	}()
	r.mu.RLock()
	t, ok := r.tools[call.Name]
	r.mu.RUnlock()
	if !ok {
		return Result{Call: call, Err: fmt.Errorf("unknown tool %q", call.Name)}
	}
	if t.RequiresApproval {
		if approver == nil {
			return Result{Call: call, Err: ErrRejected}
		}
		allowed, err := approver(ctx, ApprovalRequest{ID: call.ID, Name: call.Name, Args: call.Args})
		if err != nil {
			return Result{Call: call, Err: err}
		}
		if !allowed {
			return Result{Call: call, Err: ErrRejected}
		}
	}
	text, err := t.Handler(ctx, call.Args)
	return Result{Call: call, Text: text, Err: err}
}

func (r *Registry) RunBatch(ctx context.Context, calls []provider.ToolCall, approver Approver) []Result {
	results := make([]Result, len(calls))
	for i := 0; i < len(calls); {
		j := i
		for j < len(calls) && calls[j].ReadOnly {
			j++
		}
		if j > i {
			var wg sync.WaitGroup
			for k := i; k < j; k++ {
				wg.Add(1)
				go func(idx int) {
					defer wg.Done()
					results[idx] = r.Run(ctx, calls[idx], approver)
				}(k)
			}
			wg.Wait()
			i = j
			continue
		}
		results[i] = r.Run(ctx, calls[i], approver)
		i++
	}
	return results
}

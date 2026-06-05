package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/floegence/floret/provider"
)

var ErrRejected = errors.New("tool call rejected")
var ErrDuplicate = errors.New("duplicate tool name")
var ErrInvalid = errors.New("invalid tool")

const (
	ControlAskUser      = "ask_user"
	ControlTaskComplete = "task_complete"
)

type Definition struct {
	Name         string
	Title        string
	Description  string
	InputSchema  map[string]any
	OutputSchema map[string]any

	Effects      []Effect
	ReadOnly     bool
	Destructive  bool
	OpenWorld    bool
	ParallelSafe bool

	Permission  PermissionSpec
	ResultLimit ResultLimit
}

type RunOptions struct {
	RunID     string
	SessionID string
	Step      int
	CWD       string
}

type erasedInvocation struct {
	CallID    string
	Name      string
	RawArgs   string
	Args      any
	RunID     string
	SessionID string
	Step      int
	CWD       string
}

type Tool struct {
	Definition Definition
	decode     func([]byte) (any, error)
	resources  func(erasedInvocation) ([]ResourceRef, error)
	handler    func(context.Context, erasedInvocation) (Result, error)
}

func Define[T any](
	def Definition,
	decode func([]byte) (T, error),
	resources func(Invocation[T]) ([]ResourceRef, error),
	handler func(context.Context, Invocation[T]) (Result, error),
) Tool {
	return Tool{
		Definition: def,
		decode: func(raw []byte) (any, error) {
			if decode != nil {
				return decode(raw)
			}
			var args T
			if len(strings.TrimSpace(string(raw))) == 0 {
				raw = []byte("{}")
			}
			if err := json.Unmarshal(raw, &args); err != nil {
				return nil, err
			}
			return args, nil
		},
		resources: func(inv erasedInvocation) ([]ResourceRef, error) {
			if resources == nil {
				return nil, nil
			}
			args, ok := inv.Args.(T)
			if !ok {
				return nil, fmt.Errorf("tool %q decoded unexpected args type", inv.Name)
			}
			return resources(Invocation[T]{
				CallID:    inv.CallID,
				Name:      inv.Name,
				RawArgs:   inv.RawArgs,
				Args:      args,
				RunID:     inv.RunID,
				SessionID: inv.SessionID,
				Step:      inv.Step,
				CWD:       inv.CWD,
			})
		},
		handler: func(ctx context.Context, inv erasedInvocation) (Result, error) {
			args, ok := inv.Args.(T)
			if !ok {
				return Result{}, fmt.Errorf("tool %q decoded unexpected args type", inv.Name)
			}
			return handler(ctx, Invocation[T]{
				CallID:    inv.CallID,
				Name:      inv.Name,
				RawArgs:   inv.RawArgs,
				Args:      args,
				RunID:     inv.RunID,
				SessionID: inv.SessionID,
				Step:      inv.Step,
				CWD:       inv.CWD,
			})
		},
	}
}

type Registry struct {
	mu    sync.RWMutex
	tools map[string]Tool
}

func NewRegistry(items ...Tool) *Registry {
	r := &Registry{tools: map[string]Tool{}}
	for _, item := range items {
		if err := r.Register(item); err != nil {
			panic(err)
		}
	}
	return r
}

func NewRegistryE(items ...Tool) (*Registry, error) {
	r := &Registry{tools: map[string]Tool{}}
	for _, item := range items {
		if err := r.Register(item); err != nil {
			return nil, err
		}
	}
	return r, nil
}

func (r *Registry) Register(t Tool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	def := t.Definition
	def.Name = strings.TrimSpace(def.Name)
	if t.handler == nil {
		return ErrInvalid
	}
	schema, err := NormalizeInputSchema(def.InputSchema)
	if err != nil {
		return err
	}
	def.InputSchema = schema
	def, err = ValidateDefinition(def)
	if err != nil {
		return err
	}
	t.Definition = def
	if _, ok := r.tools[def.Name]; ok {
		return ErrDuplicate
	}
	r.tools[def.Name] = t
	return nil
}

func ValidateDefinition(def Definition) (Definition, error) {
	def.Name = strings.TrimSpace(def.Name)
	if def.Name == "" {
		return def, ErrInvalid
	}
	if IsReservedName(def.Name) {
		return def, fmt.Errorf("%w: reserved tool name %q", ErrInvalid, def.Name)
	}
	effects := map[Effect]bool{}
	for _, effect := range def.Effects {
		switch effect {
		case EffectRead, EffectWrite, EffectShell, EffectNetwork:
			effects[effect] = true
		default:
			return def, fmt.Errorf("%w: unknown effect %q", ErrInvalid, effect)
		}
	}
	if def.Permission.Mode == "" {
		if safeReadOnlyDefault(def, effects) {
			def.Permission.Mode = PermissionAllow
		} else {
			return def, fmt.Errorf("%w: tool %q must declare permission mode", ErrInvalid, def.Name)
		}
	}
	switch def.Permission.Mode {
	case PermissionAllow, PermissionAsk, PermissionDeny:
	default:
		return def, fmt.Errorf("%w: unknown permission mode %q", ErrInvalid, def.Permission.Mode)
	}
	if def.ReadOnly {
		if def.Destructive || effects[EffectWrite] || effects[EffectShell] {
			return def, fmt.Errorf("%w: read-only tool %q has mutating effects", ErrInvalid, def.Name)
		}
		if len(def.Effects) == 0 {
			def.Effects = []Effect{EffectRead}
		}
	}
	if def.Destructive && def.ReadOnly {
		return def, fmt.Errorf("%w: destructive tool %q cannot be read-only", ErrInvalid, def.Name)
	}
	if def.Destructive && !(effects[EffectWrite] || effects[EffectShell]) {
		return def, fmt.Errorf("%w: destructive tool %q must declare write or shell effect", ErrInvalid, def.Name)
	}
	if def.OpenWorld && !(effects[EffectNetwork] || effects[EffectShell]) {
		return def, fmt.Errorf("%w: open-world tool %q must declare network or shell effect", ErrInvalid, def.Name)
	}
	if def.OpenWorld && def.Permission.Mode == PermissionAllow {
		return def, fmt.Errorf("%w: open-world tool %q must ask or deny by default", ErrInvalid, def.Name)
	}
	if def.ParallelSafe && !deriveParallelSafe(def) {
		return def, fmt.Errorf("%w: parallel-safe tool %q must be strictly read-only", ErrInvalid, def.Name)
	}
	if def.ParallelSafe == false && deriveParallelSafe(def) {
		def.ParallelSafe = true
	}
	return def, nil
}

func safeReadOnlyDefault(def Definition, effects map[Effect]bool) bool {
	return def.ReadOnly &&
		!def.Destructive &&
		!def.OpenWorld &&
		!effects[EffectWrite] &&
		!effects[EffectShell] &&
		!effects[EffectNetwork]
}

func IsReservedName(name string) bool {
	name = strings.TrimSpace(name)
	return name == ControlAskUser || name == ControlTaskComplete
}

func deriveParallelSafe(def Definition) bool {
	if !def.ReadOnly || def.Destructive || def.OpenWorld {
		return false
	}
	for _, effect := range def.Effects {
		if effect != EffectRead {
			return false
		}
	}
	return true
}

func (r *Registry) IsParallelSafe(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.tools[name]
	return ok && t.Definition.ParallelSafe
}

func (r *Registry) Definition(name string) (Definition, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.tools[name]
	if !ok {
		return Definition{}, false
	}
	return cloneDefinition(t.Definition), true
}

func (r *Registry) Definitions() []provider.ToolDefinition {
	return r.ExposedDefinitions()
}

func (r *Registry) ExposedDefinitions() []provider.ToolDefinition {
	r.mu.RLock()
	defer r.mu.RUnlock()
	defs := make([]provider.ToolDefinition, 0, len(r.tools))
	for _, tool := range r.tools {
		if tool.Definition.Permission.Mode == PermissionDeny {
			continue
		}
		defs = append(defs, provider.ToolDefinition{
			Name:         tool.Definition.Name,
			Title:        tool.Definition.Title,
			Description:  tool.Definition.Description,
			InputSchema:  cloneSchema(tool.Definition.InputSchema),
			OutputSchema: cloneSchema(tool.Definition.OutputSchema),
			Strict:       true,
			Annotations: map[string]any{
				"effects":       effectsAsStrings(tool.Definition.Effects),
				"read_only":     tool.Definition.ReadOnly,
				"destructive":   tool.Definition.Destructive,
				"open_world":    tool.Definition.OpenWorld,
				"parallel_safe": tool.Definition.ParallelSafe,
			},
		})
	}
	slices.SortFunc(defs, func(a, b provider.ToolDefinition) int {
		return strings.Compare(a.Name, b.Name)
	})
	return defs
}

func cloneDefinition(def Definition) Definition {
	def.InputSchema = cloneSchema(def.InputSchema)
	def.OutputSchema = cloneSchema(def.OutputSchema)
	def.Effects = append([]Effect(nil), def.Effects...)
	def.Permission.ResourceKinds = append([]string(nil), def.Permission.ResourceKinds...)
	return def
}

func (r *Registry) Run(ctx context.Context, call provider.ToolCall, approver Approver) Result {
	return r.RunWithOptions(ctx, call, approver, RunOptions{})
}

func (r *Registry) RunWithOptions(ctx context.Context, call provider.ToolCall, approver Approver, opts RunOptions) Result {
	return r.run(ctx, call, approver, opts)
}

func (r *Registry) run(ctx context.Context, call provider.ToolCall, approver Approver, opts RunOptions) (result Result) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = ErrorResult(call.ID, call.Name, fmt.Sprintf("tool %q panicked: %v", call.Name, recovered))
		}
	}()
	r.mu.RLock()
	t, ok := r.tools[call.Name]
	r.mu.RUnlock()
	if !ok {
		return ErrorResult(call.ID, call.Name, fmt.Sprintf("unknown tool %q", call.Name))
	}
	raw := strings.TrimSpace(call.Args)
	if raw == "" {
		raw = "{}"
	}
	if _, err := Validate(t.Definition.InputSchema, []byte(raw)); err != nil {
		return ErrorResult(call.ID, call.Name, InvalidArgumentsText(call.Name, err))
	}
	args, err := t.decode([]byte(raw))
	if err != nil {
		return ErrorResult(call.ID, call.Name, InvalidArgumentsText(call.Name, err))
	}
	inv := erasedInvocation{CallID: call.ID, Name: call.Name, RawArgs: raw, Args: args, RunID: opts.RunID, SessionID: opts.SessionID, Step: opts.Step, CWD: opts.CWD}
	if t.Definition.Permission.Mode == PermissionDeny {
		return ErrorResult(call.ID, call.Name, ErrRejected.Error())
	}
	resources, err := t.resources(inv)
	if err != nil {
		return ErrorResult(call.ID, call.Name, fmt.Sprintf("tool %q resource extraction failed: %v", call.Name, err))
	}
	if denied := r.permissionDenied(ctx, t.Definition, call, args, resources, opts, approver); denied != "" {
		return ErrorResult(call.ID, call.Name, denied)
	}
	result, err = t.handler(ctx, inv)
	if err != nil {
		return ErrorResult(call.ID, call.Name, err.Error())
	}
	result = result.withCall(call.ID, call.Name)
	if t.Definition.OutputSchema != nil && result.Structured != nil {
		if err := ValidateStructured(t.Definition.OutputSchema, result.Structured); err != nil {
			return ErrorResult(call.ID, call.Name, fmt.Sprintf("tool %q returned invalid structured output: %v", call.Name, err))
		}
	}
	return result
}

func (r *Registry) permissionDenied(ctx context.Context, def Definition, call provider.ToolCall, args any, resources []ResourceRef, opts RunOptions, approver Approver) string {
	switch def.Permission.Mode {
	case PermissionDeny:
		return ErrRejected.Error()
	case PermissionAllow:
		return ""
	case PermissionAsk:
		if approver == nil {
			return ErrRejected.Error()
		}
		decision, err := approver(ctx, ApprovalRequest{
			ID:            call.ID,
			Name:          call.Name,
			Args:          call.Args,
			ValidatedArgs: args,
			Resources:     resources,
			Effects:       append([]Effect(nil), def.Effects...),
			ReadOnly:      def.ReadOnly,
			Destructive:   def.Destructive,
			OpenWorld:     def.OpenWorld,
			CWD:           opts.CWD,
		})
		if err != nil {
			return err.Error()
		}
		if decision != PermissionDecisionAllow {
			return ErrRejected.Error()
		}
	default:
		return ErrRejected.Error()
	}
	return ""
}

func (r *Registry) RunBatch(ctx context.Context, calls []provider.ToolCall, approver Approver) []Result {
	return r.RunBatchWithOptions(ctx, calls, approver, RunOptions{})
}

func (r *Registry) RunBatchWithOptions(ctx context.Context, calls []provider.ToolCall, approver Approver, opts RunOptions) []Result {
	results := make([]Result, len(calls))
	for i := 0; i < len(calls); {
		j := i
		for j < len(calls) && r.IsParallelSafe(calls[j].Name) {
			j++
		}
		if j > i {
			var wg sync.WaitGroup
			for k := i; k < j; k++ {
				wg.Add(1)
				go func(idx int) {
					defer wg.Done()
					results[idx] = r.RunWithOptions(ctx, calls[idx], approver, opts)
				}(k)
			}
			wg.Wait()
			i = j
			continue
		}
		results[i] = r.RunWithOptions(ctx, calls[i], approver, opts)
		i++
	}
	return results
}

func (r *Registry) LimitFor(name string) ResultLimit {
	r.mu.RLock()
	defer r.mu.RUnlock()
	tool, ok := r.tools[name]
	if !ok {
		return ResultLimit{}
	}
	return tool.Definition.ResultLimit
}

func effectsAsStrings(effects []Effect) []string {
	out := make([]string, 0, len(effects))
	for _, effect := range effects {
		out = append(out, string(effect))
	}
	return out
}

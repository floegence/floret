package florettest

import (
	"context"
	"errors"
	"sync"

	"github.com/floegence/floret/runtime"
	"github.com/floegence/floret/tools"
)

// ErrModelScriptExhausted reports a ModelGateway request for which no scripted
// step remains.
var ErrModelScriptExhausted = errors.New("florettest: model gateway script exhausted")

// ModelStep describes one StreamModel call. ReturnError takes precedence over
// all other fields. WaitForCancellation keeps the stream open until the request
// context is canceled. BlockUntil keeps it open until release or cancellation.
type ModelStep struct {
	Events              []runtime.ModelEvent
	ReturnError         error
	BlockUntil          <-chan struct{}
	WaitForCancellation bool
}

// ScriptedModelGateway is a deterministic, concurrency-safe ModelGateway. Each
// request consumes exactly one ModelStep in declaration order.
type ScriptedModelGateway struct {
	mu       sync.Mutex
	steps    []ModelStep
	requests []runtime.ModelRequest
	changed  chan struct{}
}

var _ runtime.ModelGateway = (*ScriptedModelGateway)(nil)

// NewScriptedModelGateway constructs a gateway from an ordered script.
func NewScriptedModelGateway(steps ...ModelStep) *ScriptedModelGateway {
	return &ScriptedModelGateway{
		steps:   cloneModelSteps(steps),
		changed: make(chan struct{}),
	}
}

// StreamModel implements runtime.ModelGateway.
func (g *ScriptedModelGateway) StreamModel(ctx context.Context, request runtime.ModelRequest) (<-chan runtime.ModelEvent, error) {
	if g == nil {
		return nil, errors.New("florettest: scripted model gateway is nil")
	}
	g.mu.Lock()
	if g.changed == nil {
		g.changed = make(chan struct{})
	}
	index := len(g.requests)
	g.requests = append(g.requests, cloneModelRequest(request))
	close(g.changed)
	g.changed = make(chan struct{})
	if index >= len(g.steps) {
		g.mu.Unlock()
		return nil, ErrModelScriptExhausted
	}
	step := cloneModelStep(g.steps[index])
	g.mu.Unlock()

	if step.ReturnError != nil {
		return nil, step.ReturnError
	}
	events := make(chan runtime.ModelEvent, len(step.Events)+1)
	go streamModelStep(ctx, events, step)
	return events, nil
}

// Requests returns stable snapshots of all requests observed so far.
func (g *ScriptedModelGateway) Requests() []runtime.ModelRequest {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]runtime.ModelRequest, len(g.requests))
	for index := range g.requests {
		out[index] = cloneModelRequest(g.requests[index])
	}
	return out
}

// WaitForRequests waits until at least count requests have entered StreamModel.
func (g *ScriptedModelGateway) WaitForRequests(ctx context.Context, count int) error {
	if g == nil {
		return errors.New("florettest: scripted model gateway is nil")
	}
	if count <= 0 {
		return nil
	}
	for {
		g.mu.Lock()
		if g.changed == nil {
			g.changed = make(chan struct{})
		}
		if len(g.requests) >= count {
			g.mu.Unlock()
			return nil
		}
		changed := g.changed
		g.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

func streamModelStep(ctx context.Context, out chan<- runtime.ModelEvent, step ModelStep) {
	defer close(out)
	if step.WaitForCancellation {
		<-ctx.Done()
		out <- runtime.ModelEvent{Type: runtime.ModelEventError, Err: ctx.Err()}
		return
	}
	if step.BlockUntil != nil {
		select {
		case <-ctx.Done():
			out <- runtime.ModelEvent{Type: runtime.ModelEventError, Err: ctx.Err()}
			return
		case <-step.BlockUntil:
		}
	}
	for _, event := range step.Events {
		if err := ctx.Err(); err != nil {
			out <- runtime.ModelEvent{Type: runtime.ModelEventError, Err: err}
			return
		}
		out <- cloneModelEvent(event)
	}
}

func cloneModelSteps(in []ModelStep) []ModelStep {
	out := make([]ModelStep, len(in))
	for index := range in {
		out[index] = cloneModelStep(in[index])
	}
	return out
}

func cloneModelStep(in ModelStep) ModelStep {
	out := in
	out.Events = make([]runtime.ModelEvent, len(in.Events))
	for index := range in.Events {
		out.Events[index] = cloneModelEvent(in.Events[index])
	}
	return out
}

func cloneModelEvent(in runtime.ModelEvent) runtime.ModelEvent {
	out := in
	if in.ToolCallStream != nil {
		stream := *in.ToolCallStream
		out.ToolCallStream = &stream
	}
	out.ToolCalls = append([]tools.ToolCall(nil), in.ToolCalls...)
	out.Sources = append([]runtime.SourceRef(nil), in.Sources...)
	out.ResponseState = cloneModelState(in.ResponseState)
	return out
}

func cloneModelRequest(in runtime.ModelRequest) runtime.ModelRequest {
	out := in
	out.Messages = make([]runtime.ModelMessage, len(in.Messages))
	for index := range in.Messages {
		out.Messages[index] = cloneModelMessage(in.Messages[index])
	}
	out.Tools = make([]tools.ToolDefinition, len(in.Tools))
	for index := range in.Tools {
		out.Tools[index] = cloneToolDefinition(in.Tools[index])
	}
	out.HostedTools = make([]runtime.HostedToolDefinition, len(in.HostedTools))
	for index := range in.HostedTools {
		out.HostedTools[index] = cloneHostedToolDefinition(in.HostedTools[index])
	}
	out.PreviousState = cloneModelState(in.PreviousState)
	out.Labels = runtime.RunLabels{
		Correlation: cloneStringMap(in.Labels.Correlation),
		Host:        cloneStringMap(in.Labels.Host),
	}
	return out
}

func cloneModelMessage(in runtime.ModelMessage) runtime.ModelMessage {
	out := in
	out.Attachments = append([]runtime.MessageAttachment(nil), in.Attachments...)
	out.ToolCalls = append([]tools.ToolCall(nil), in.ToolCalls...)
	if in.ToolResult != nil {
		result := *in.ToolResult
		out.ToolResult = &result
	}
	return out
}

func cloneModelState(in *runtime.ModelState) *runtime.ModelState {
	if in == nil {
		return nil
	}
	return &runtime.ModelState{
		Kind:       in.Kind,
		ID:         in.ID,
		Attributes: cloneStringMap(in.Attributes),
	}
}

func cloneToolDefinition(in tools.ToolDefinition) tools.ToolDefinition {
	out := in
	out.InputSchema = cloneAnyMap(in.InputSchema)
	out.OutputSchema = cloneAnyMap(in.OutputSchema)
	out.Annotations = cloneAnyMap(in.Annotations)
	return out
}

func cloneHostedToolDefinition(in runtime.HostedToolDefinition) runtime.HostedToolDefinition {
	out := in
	out.Parameters = cloneAnyMap(in.Parameters)
	out.Options = cloneAnyMap(in.Options)
	return out
}

func cloneStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneAnyMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = cloneAny(value)
	}
	return out
}

func cloneAny(in any) any {
	switch value := in.(type) {
	case map[string]any:
		return cloneAnyMap(value)
	case map[string]string:
		return cloneStringMap(value)
	case []any:
		out := make([]any, len(value))
		for index := range value {
			out[index] = cloneAny(value[index])
		}
		return out
	case []string:
		return append([]string(nil), value...)
	default:
		return value
	}
}

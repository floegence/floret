package florettest

import (
	"context"
	"errors"
	"sync"

	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/tools"
)

// ErrGatewayScriptExhausted reports a request without a remaining Step.
var ErrGatewayScriptExhausted = errors.New("florettest: gateway script exhausted")

// Step describes one Gateway.Stream call.
type Step struct {
	Events              []provider.Event
	ReturnError         error
	BlockUntil          <-chan struct{}
	WaitForCancellation bool
}

// ScriptedGateway is a deterministic, concurrency-safe provider Gateway.
type ScriptedGateway struct {
	identity     provider.Identity
	capabilities provider.Capabilities
	mu           sync.Mutex
	steps        []Step
	requests     []provider.Request
	changed      chan struct{}
}

var _ provider.Gateway = (*ScriptedGateway)(nil)

// NewScriptedGateway constructs a gateway with an explicit provider contract.
func NewScriptedGateway(identity provider.Identity, capabilities provider.Capabilities, steps ...Step) *ScriptedGateway {
	return &ScriptedGateway{
		identity: identity, capabilities: capabilities,
		steps: cloneSteps(steps), changed: make(chan struct{}),
	}
}

// Identity returns the configured provider identity.
func (gateway *ScriptedGateway) Identity() provider.Identity {
	if gateway == nil {
		return provider.Identity{}
	}
	return gateway.identity
}

// Capabilities returns the configured provider capability declaration.
func (gateway *ScriptedGateway) Capabilities() provider.Capabilities {
	if gateway == nil {
		return provider.Capabilities{}
	}
	return gateway.capabilities
}

// Stream consumes exactly one Step in request order.
func (gateway *ScriptedGateway) Stream(ctx context.Context, request provider.Request) (<-chan provider.Event, error) {
	if gateway == nil {
		return nil, errors.New("florettest: scripted gateway is nil")
	}
	gateway.mu.Lock()
	index := len(gateway.requests)
	gateway.requests = append(gateway.requests, cloneProviderRequest(request))
	close(gateway.changed)
	gateway.changed = make(chan struct{})
	if index >= len(gateway.steps) {
		gateway.mu.Unlock()
		return nil, ErrGatewayScriptExhausted
	}
	step := cloneStep(gateway.steps[index])
	gateway.mu.Unlock()
	if step.ReturnError != nil {
		return nil, step.ReturnError
	}
	events := make(chan provider.Event, len(step.Events)+1)
	go streamStep(ctx, events, step)
	return events, nil
}

// Requests returns detached snapshots of all observed requests.
func (gateway *ScriptedGateway) Requests() []provider.Request {
	if gateway == nil {
		return nil
	}
	gateway.mu.Lock()
	defer gateway.mu.Unlock()
	requests := make([]provider.Request, len(gateway.requests))
	for index, request := range gateway.requests {
		requests[index] = cloneProviderRequest(request)
	}
	return requests
}

// WaitForRequests waits until count requests have entered Stream.
func (gateway *ScriptedGateway) WaitForRequests(ctx context.Context, count int) error {
	if gateway == nil {
		return errors.New("florettest: scripted gateway is nil")
	}
	for count > 0 {
		gateway.mu.Lock()
		if len(gateway.requests) >= count {
			gateway.mu.Unlock()
			return nil
		}
		changed := gateway.changed
		gateway.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
	return nil
}

func streamStep(ctx context.Context, output chan<- provider.Event, step Step) {
	defer close(output)
	if step.WaitForCancellation {
		<-ctx.Done()
		output <- provider.Event{Type: provider.EventError, Err: ctx.Err()}
		return
	}
	if step.BlockUntil != nil {
		select {
		case <-ctx.Done():
			output <- provider.Event{Type: provider.EventError, Err: ctx.Err()}
			return
		case <-step.BlockUntil:
		}
	}
	for _, event := range step.Events {
		select {
		case output <- cloneProviderEvent(event):
		case <-ctx.Done():
			output <- provider.Event{Type: provider.EventError, Err: ctx.Err()}
			return
		}
	}
}

func cloneSteps(steps []Step) []Step {
	cloned := make([]Step, len(steps))
	for index, step := range steps {
		cloned[index] = cloneStep(step)
	}
	return cloned
}

func cloneStep(step Step) Step {
	cloned := step
	cloned.Events = make([]provider.Event, len(step.Events))
	for index, event := range step.Events {
		cloned.Events[index] = cloneProviderEvent(event)
	}
	return cloned
}

func cloneProviderEvent(event provider.Event) provider.Event {
	cloned := event
	cloned.ToolCalls = append([]provider.ToolCall(nil), event.ToolCalls...)
	cloned.Sources = append([]provider.Source(nil), event.Sources...)
	if event.ToolCallStream != nil {
		stream := *event.ToolCallStream
		cloned.ToolCallStream = &stream
	}
	cloned.ResponseState = cloneProviderState(event.ResponseState)
	return cloned
}

func cloneProviderRequest(request provider.Request) provider.Request {
	cloned := request
	cloned.Messages = make([]provider.Message, len(request.Messages))
	for index, message := range request.Messages {
		cloned.Messages[index] = message
		cloned.Messages[index].Attachments = append([]provider.Attachment(nil), message.Attachments...)
		cloned.Messages[index].ToolCalls = append([]provider.ToolCall(nil), message.ToolCalls...)
		if message.ToolResult != nil {
			result := *message.ToolResult
			cloned.Messages[index].ToolResult = &result
		}
	}
	cloned.Tools = make([]tools.ToolDefinition, len(request.Tools))
	for index, definition := range request.Tools {
		cloned.Tools[index] = definition
		cloned.Tools[index].InputSchema = cloneAnyMap(definition.InputSchema)
		cloned.Tools[index].OutputSchema = cloneAnyMap(definition.OutputSchema)
		cloned.Tools[index].Annotations = cloneAnyMap(definition.Annotations)
	}
	cloned.HostedTools = append([]provider.HostedToolDefinition(nil), request.HostedTools...)
	cloned.PreviousState = cloneProviderState(request.PreviousState)
	cloned.Labels = provider.Labels{
		Correlation: cloneStringMap(request.Labels.Correlation), Host: cloneStringMap(request.Labels.Host),
	}
	return cloned
}

func cloneProviderState(state *provider.State) *provider.State {
	if state == nil {
		return nil
	}
	return &provider.State{Kind: state.Kind, ID: state.ID, Attributes: cloneStringMap(state.Attributes)}
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func cloneAnyMap(values map[string]any) map[string]any {
	if values == nil {
		return nil
	}
	cloned := make(map[string]any, len(values))
	for key, value := range values {
		switch nested := value.(type) {
		case map[string]any:
			cloned[key] = cloneAnyMap(nested)
		case []any:
			items := make([]any, len(nested))
			for index, item := range nested {
				if object, ok := item.(map[string]any); ok {
					items[index] = cloneAnyMap(object)
				} else {
					items[index] = item
				}
			}
			cloned[key] = items
		default:
			cloned[key] = value
		}
	}
	return cloned
}

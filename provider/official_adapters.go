package provider

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/floegence/floret/v4/internal/configbridge"
	internalprovider "github.com/floegence/floret/v4/internal/provider"
	"github.com/floegence/floret/v4/internal/provider/adapters"
	"github.com/floegence/floret/v4/internal/provider/catalog"
	"github.com/floegence/floret/v4/internal/session"
	"github.com/floegence/floret/v4/tools"
)

// OpenAICompatibleOptions configures an explicit OpenAI chat-completions
// transport. BaseURL is the API root; the gateway sends requests to
// BaseURL/chat/completions.
type OpenAICompatibleOptions struct {
	Provider              string
	Model                 string
	BaseURL               string
	APIKey                string
	StateCompatibilityKey string
	HTTPClient            *http.Client
	Capabilities          Capabilities
}

// AnthropicOptions configures an explicit Anthropic messages transport.
// BaseURL is the API root; the gateway sends requests to BaseURL/messages.
type AnthropicOptions struct {
	Provider              string
	Model                 string
	BaseURL               string
	APIKey                string
	StateCompatibilityKey string
	HTTPClient            *http.Client
	Capabilities          Capabilities
}

type officialGateway struct {
	identity     Identity
	capabilities Capabilities
	transport    internalprovider.Provider
}

var _ Gateway = (*officialGateway)(nil)

// NewOpenAICompatible constructs an explicit OpenAI-compatible Gateway.
func NewOpenAICompatible(options OpenAICompatibleOptions) (Gateway, error) {
	identity, capabilities, baseURL, apiKey, err := validateOfficialOptions(
		options.Provider, options.Model, options.BaseURL, options.APIKey,
		options.StateCompatibilityKey, options.Capabilities,
	)
	if err != nil {
		return nil, fmt.Errorf("openai-compatible gateway: %w", err)
	}
	model, _ := catalog.FindModel(identity.Provider, identity.Model)
	return &officialGateway{
		identity: identity, capabilities: capabilities,
		transport: adapters.OpenAICompatibleProvider{
			Endpoint: baseURL + "/chat/completions", APIKey: apiKey, Model: identity.Model,
			CostModel: model, Cache: catalog.Cache(identity.Provider, identity.Model), HTTPClient: options.HTTPClient,
		},
	}, nil
}

// NewAnthropic constructs an explicit Anthropic messages Gateway.
func NewAnthropic(options AnthropicOptions) (Gateway, error) {
	identity, capabilities, baseURL, apiKey, err := validateOfficialOptions(
		options.Provider, options.Model, options.BaseURL, options.APIKey,
		options.StateCompatibilityKey, options.Capabilities,
	)
	if err != nil {
		return nil, fmt.Errorf("anthropic gateway: %w", err)
	}
	model, _ := catalog.FindModel(identity.Provider, identity.Model)
	return &officialGateway{
		identity: identity, capabilities: capabilities,
		transport: adapters.AnthropicProvider{
			Endpoint: baseURL + "/messages", APIKey: apiKey, Model: identity.Model,
			MaxTokens: model.MaxTokens, CostModel: model, Cache: catalog.Cache(identity.Provider, identity.Model), HTTPClient: options.HTTPClient,
		},
	}, nil
}

func validateOfficialOptions(providerName, model, baseURL, apiKey, stateKey string, capabilities Capabilities) (Identity, Capabilities, string, string, error) {
	identity := Identity{
		Provider: strings.TrimSpace(providerName), Model: strings.TrimSpace(model),
		StateCompatibilityKey: strings.TrimSpace(stateKey),
	}
	if err := identity.Validate(); err != nil {
		return Identity{}, Capabilities{}, "", "", err
	}
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL == "" {
		return Identity{}, Capabilities{}, "", "", errors.New("base URL is required")
	}
	apiKey = strings.TrimSpace(apiKey)
	if apiKey == "" {
		return Identity{}, Capabilities{}, "", "", errors.New("API key is required")
	}
	if err := capabilities.Validate(); err != nil {
		return Identity{}, Capabilities{}, "", "", err
	}
	if capabilities.AttachmentPayload == "" {
		capabilities.AttachmentPayload = AttachmentDescriptors
	}
	if capabilities.AttachmentPayload != AttachmentDescriptors {
		return Identity{}, Capabilities{}, "", "", errors.New("official gateway accepts attachment descriptors only")
	}
	return identity, capabilities, baseURL, apiKey, nil
}

// Identity returns the immutable provider transport identity.
func (gateway *officialGateway) Identity() Identity {
	if gateway == nil {
		return Identity{}
	}
	return gateway.identity
}

// Capabilities returns the immutable provider capability declaration.
func (gateway *officialGateway) Capabilities() Capabilities {
	if gateway == nil {
		return Capabilities{}
	}
	return gateway.capabilities
}

// Stream validates and executes one public provider request.
func (gateway *officialGateway) Stream(ctx context.Context, request Request) (<-chan Event, error) {
	if gateway == nil || gateway.transport == nil {
		return nil, errors.New("provider gateway is nil")
	}
	if err := request.Validate(); err != nil {
		return nil, err
	}
	stream, err := gateway.transport.Stream(ctx, internalRequest(request, gateway.identity))
	if err != nil {
		if errors.Is(err, internalprovider.ErrContextOverflow) {
			return nil, ErrContextOverflow
		}
		return nil, err
	}
	output := make(chan Event)
	go func() {
		defer close(output)
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-stream:
				if !ok {
					return
				}
				select {
				case <-ctx.Done():
					return
				case output <- publicEvent(event):
				}
			}
		}
	}()
	return output, nil
}

func internalRequest(request Request, identity Identity) internalprovider.Request {
	return internalprovider.Request{
		RunID: request.RunID.String(), ThreadID: request.ThreadID.String(), TurnID: request.TurnID.String(),
		TraceID: request.TraceID.String(), PromptScopeID: request.PromptScopeID.String(), Step: request.Step,
		Provider: identity.Provider, Model: identity.Model, Messages: internalMessages(request.Messages),
		Tools: cloneToolDefinitions(request.Tools), HostedTools: internalHostedToolDefinitions(request.HostedTools),
		MaxOutputTokens: request.MaxOutputTokens, Reasoning: configbridge.ReasoningSelection(request.Reasoning),
		PreviousState: internalState(request.PreviousState), Labels: internalprovider.RequestLabels{
			Correlation: cloneStringMap(request.Labels.Correlation), Host: cloneStringMap(request.Labels.Host),
		},
	}
}

func internalMessages(messages []Message) []session.Message {
	output := make([]session.Message, 0, len(messages))
	for _, message := range messages {
		base := session.Message{Role: session.Role(message.Role), Content: message.Text, Reasoning: message.Reasoning}
		if message.ToolResult != nil {
			base.ToolCallID = message.ToolResult.CallID
			base.ToolName = message.ToolResult.ToolName
			base.Content = message.ToolResult.Text
		}
		if len(message.ToolCalls) == 0 {
			output = append(output, base)
			continue
		}
		for index, call := range message.ToolCalls {
			item := base
			if index > 0 {
				item.Content = ""
				item.Reasoning = ""
			}
			item.ToolCallID, item.ToolName, item.ToolArgs = call.ID, call.Name, call.Args
			output = append(output, item)
		}
	}
	return output
}

func cloneToolDefinitions(definitions []tools.ToolDefinition) []tools.ToolDefinition {
	return append([]tools.ToolDefinition(nil), definitions...)
}

func internalHostedToolDefinitions(definitions []HostedToolDefinition) []internalprovider.HostedToolDefinition {
	output := make([]internalprovider.HostedToolDefinition, len(definitions))
	for index, definition := range definitions {
		output[index] = internalprovider.HostedToolDefinition{
			Name: definition.Name, Type: definition.Type, Description: definition.Description,
			Parameters: definition.Parameters, Options: definition.Options,
		}
	}
	return output
}

func internalState(state *State) *internalprovider.State {
	if state == nil {
		return nil
	}
	return &internalprovider.State{Kind: state.Kind, ID: state.ID, Attributes: cloneStringMap(state.Attributes)}
}

func publicEvent(event internalprovider.StreamEvent) Event {
	calls := make([]ToolCall, len(event.ToolCalls))
	for index, call := range event.ToolCalls {
		calls[index] = ToolCall{ID: call.ID, Name: call.Name, Args: call.Args}
	}
	sources := make([]Source, len(event.Sources))
	for index, source := range event.Sources {
		sources[index] = Source{Title: source.Title, URL: source.URL}
	}
	var stream *ToolCallStream
	if event.ToolCallStream.ID != "" || event.ToolCallStream.Name != "" {
		stream = &ToolCallStream{ID: event.ToolCallStream.ID, Name: event.ToolCallStream.Name}
	}
	var hostedCall *ToolCall
	if event.ToolCall.ID != "" || event.ToolCall.Name != "" {
		hostedCall = &ToolCall{ID: event.ToolCall.ID, Name: event.ToolCall.Name, Args: event.ToolCall.Args}
	}
	var hostedResult *HostedToolResult
	if !event.HostedResult.IsZero() {
		hostedResult = publicHostedResult(event.HostedResult)
	}
	return Event{
		Type: EventType(event.Type), Text: event.Text, ToolCallStream: stream, ToolCalls: calls,
		HostedToolCall: hostedCall, HostedResult: hostedResult, Sources: sources, Reason: event.Reason,
		Usage: Usage{
			InputTokens: event.Usage.InputTokens, OutputTokens: event.Usage.OutputTokens,
			ReasoningTokens: event.Usage.ReasoningTokens, CacheReadTokens: event.Usage.CacheReadTokens,
			CacheWriteTokens: event.Usage.CacheWriteTokens, TotalTokens: event.Usage.TotalTokens,
			CostUSD: event.Usage.CostUSD, Source: string(event.Usage.Source), Available: event.Usage.Available,
			WindowInputTokens: event.Usage.WindowInputTokens,
		},
		ResponseID: event.ResponseID, ResponseState: publicState(event.ResponseState), Err: event.Err,
	}
}

func publicHostedResult(result internalprovider.HostedToolResultData) *HostedToolResult {
	items := make([]HostedToolResultItem, len(result.Results))
	for index, item := range result.Results {
		items[index] = HostedToolResultItem{
			Title: item.Title, URL: item.URL, Snippet: item.Snippet, Source: item.Source, Metadata: item.Metadata,
		}
	}
	var resultError *HostedToolResultError
	if result.Error != nil {
		resultError = &HostedToolResultError{Code: result.Error.Code, Message: result.Error.Message}
	}
	return &HostedToolResult{Text: result.Text, Results: items, Error: resultError, Metadata: result.Metadata}
}

func publicState(state *internalprovider.State) *State {
	if state == nil {
		return nil
	}
	return &State{Kind: state.Kind, ID: state.ID, Attributes: cloneStringMap(state.Attributes)}
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

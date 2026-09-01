package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/configbridge"
	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session"
	publicprovider "github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/tools"
)

func projectedModelProvider(_ runtimeConfig, gateway modelGateway, identity modelGatewayIdentity, capabilities modelGatewayCapabilities) (provider.Provider, error) {
	if gateway == nil {
		return nil, errors.New("provider gateway is required")
	}
	direct := modelGatewayProvider{gateway: gateway, identity: identity}
	if capabilities.AttachmentPayload == modelGatewayAttachmentPayloadExpanded {
		return preparedModelGatewayProvider{modelGatewayProvider: direct}, nil
	}
	return direct, nil
}

func resolveModelGatewayHostConfig(cfg runtimeConfig, identity modelGatewayIdentity) (runtimeConfig, error) {
	identity, err := normalizeModelGatewayIdentity(identity)
	if err != nil {
		return runtimeConfig{}, err
	}
	if cfg.ContextPolicy.ContextWindowTokens <= 0 {
		return runtimeConfig{}, errors.New("model gateway host config requires context policy context window tokens")
	}
	cfg.Provider, cfg.Model = identity.Provider, identity.Model
	if cfg.SkillPromptBudgetBytes <= 0 {
		cfg.SkillPromptBudgetBytes = 16 * 1024
	}
	cfg.Reasoning = config.NormalizeReasoningSelection(cfg.Reasoning)
	cfg.ContextPolicy = configbridge.NormalizeContextPolicy(cfg.ContextPolicy)
	return cfg, nil
}

func normalizeModelGatewayIdentity(value modelGatewayIdentity) (modelGatewayIdentity, error) {
	value.Provider = strings.TrimSpace(value.Provider)
	value.Model = strings.TrimSpace(value.Model)
	value.StateCompatibilityKey = strings.TrimSpace(value.StateCompatibilityKey)
	if value.Provider == "" || value.Model == "" || value.StateCompatibilityKey == "" {
		return modelGatewayIdentity{}, errors.New("model gateway identity requires provider, model, and state compatibility key")
	}
	return value, nil
}

func projectedReasoningSelection(requested, fallback config.ReasoningSelection) provider.ReasoningSelection {
	requested = config.NormalizeReasoningSelection(requested)
	if !requested.IsZero() {
		return configbridge.ReasoningSelection(requested)
	}
	return configbridge.ReasoningSelection(fallback)
}

type modelGatewayProvider struct {
	gateway  modelGateway
	identity modelGatewayIdentity
}

type preparedModelGatewayProvider struct{ modelGatewayProvider }

func (adapter preparedModelGatewayProvider) PrepareRequest(ctx context.Context, request provider.Request) (provider.PreparedRequest, error) {
	modelRequest, err := adapter.modelRequest(request)
	if err != nil {
		return nil, err
	}
	preparer, ok := adapter.gateway.(modelGatewayRequestPreparer)
	if !ok {
		return nil, errors.New("expanded model gateway requires prepared request support")
	}
	prepared, err := preparer.PrepareModelRequest(ctx, modelRequest)
	if err != nil {
		return nil, err
	}
	if prepared == nil {
		return nil, errors.New("model gateway returned a nil prepared request")
	}
	return newModelGatewayPreparedRequest(prepared), nil
}

func (adapter modelGatewayProvider) Stream(ctx context.Context, request provider.Request) (<-chan provider.StreamEvent, error) {
	modelRequest, err := adapter.modelRequest(request)
	if err != nil {
		return nil, err
	}
	return streamModelGateway(ctx, func(streamCtx context.Context) (<-chan modelEvent, error) {
		return adapter.gateway.StreamModel(streamCtx, modelRequest)
	})
}

func (adapter modelGatewayProvider) EstimateTokens(_ context.Context, request provider.Request) (provider.TokenEstimate, error) {
	return provider.GenericRequestEstimate(request)
}

func (adapter modelGatewayProvider) modelRequest(request provider.Request) (modelRequest, error) {
	messagesWithEphemeral, err := provider.MessagesWithEphemeralUser(request.Messages, request.EphemeralUser)
	if err != nil {
		return modelRequest{}, err
	}
	messages, err := runtimeModelMessages(messagesWithEphemeral)
	if err != nil {
		return modelRequest{}, err
	}
	return modelRequest{
		RunID: identity.RunID(request.RunID), ThreadID: identity.ThreadID(request.ThreadID),
		TurnID: identity.TurnID(request.TurnID), TraceID: identity.TraceID(request.TraceID),
		PromptScopeID: identity.PromptScopeID(request.PromptScopeID), LogicalRequestID: identity.LogicalRequestID(request.LogicalRequestID),
		AttemptID: request.AttemptID, AttemptEpoch: request.AttemptEpoch, Step: request.Step,
		Provider: adapter.identity.Provider, Model: adapter.identity.Model, Messages: messages,
		Tools: normalizeToolDefinitions(request.Tools), HostedTools: runtimeHostedToolDefinitions(request.HostedTools),
		MaxOutputTokens: request.MaxOutputTokens, Reasoning: configbridge.PublicReasoningSelection(request.Reasoning),
		PreviousState: modelState(request.PreviousState), Labels: providerRequestLabels(request.Labels),
	}, nil
}

func streamModelGateway(ctx context.Context, start func(context.Context) (<-chan modelEvent, error)) (<-chan provider.StreamEvent, error) {
	stream, err := start(ctx)
	if err != nil {
		return nil, err
	}
	out := make(chan provider.StreamEvent)
	go func() {
		defer close(out)
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
				case out <- providerStreamEvent(event):
				}
			}
		}
	}()
	return out, nil
}

type modelGatewayPreparedRequest struct {
	mu        sync.Mutex
	closeOnce sync.Once
	streamed  bool
	closed    bool
	closeErr  error
	prepared  preparedModelRequest
}

func newModelGatewayPreparedRequest(prepared preparedModelRequest) *modelGatewayPreparedRequest {
	return &modelGatewayPreparedRequest{prepared: prepared}
}

func (request *modelGatewayPreparedRequest) Stream(ctx context.Context) (<-chan provider.StreamEvent, error) {
	request.mu.Lock()
	if request.closed || request.streamed {
		request.mu.Unlock()
		return nil, errors.New("prepared model request is closed or already consumed")
	}
	request.streamed = true
	request.mu.Unlock()
	return streamModelGateway(ctx, request.prepared.StreamModel)
}

func (request *modelGatewayPreparedRequest) TokenEstimate() provider.TokenEstimate {
	estimate := request.prepared.TokenEstimate()
	return provider.TokenEstimate{
		PrefixTokens: estimate.PrefixTokens, MessageTokens: estimate.MessageTokens,
		ToolDefinitionTokens: estimate.ToolDefinitionTokens, EstimatedInputTokens: estimate.EstimatedInputTokens,
		Source: estimate.Source, Method: provider.TokenEstimateMethod(estimate.Method),
		Confidence: provider.EstimateConfidence(estimate.Confidence), Coverage: provider.TokenEstimateCoverage(estimate.Coverage),
	}
}

func (request *modelGatewayPreparedRequest) PayloadFingerprint() string {
	return strings.TrimSpace(request.prepared.RenderedPayloadFingerprint())
}

func (*modelGatewayPreparedRequest) EnforceInputTokenLimit() bool { return true }

func (request *modelGatewayPreparedRequest) Close() error {
	request.mu.Lock()
	request.closed = true
	request.mu.Unlock()
	request.closeOnce.Do(func() { request.closeErr = request.prepared.Close() })
	return request.closeErr
}

func runtimeModelMessages(messages []session.Message) ([]modelMessage, error) {
	result := make([]modelMessage, 0, len(messages))
	for index := 0; index < len(messages); {
		message := messages[index]
		switch message.Role {
		case session.System, session.User:
			projected := modelMessage{Role: modelMessageRole(message.Role), Text: message.Content, Attachments: runtimeMessageAttachments(message.Attachments)}
			if err := projected.validateStored(); err != nil {
				return nil, fmt.Errorf("model message %d: %w", index, err)
			}
			result = append(result, projected)
			index++
		case session.Assistant:
			projected := modelMessage{Role: modelMessageRoleAssistant}
			if message.ToolCallID == "" {
				projected.Text, projected.Reasoning = message.Content, message.Reasoning
				index++
			}
			for index < len(messages) && messages[index].Role == session.Assistant && messages[index].ToolCallID != "" {
				part := messages[index]
				projected.ToolCalls = append(projected.ToolCalls, tools.ToolCall{ID: part.ToolCallID, Name: part.ToolName, Args: part.ToolArgs, Reasoning: part.Reasoning})
				index++
			}
			result = append(result, projected)
		case session.Tool:
			result = append(result, modelMessage{Role: modelMessageRoleTool, ToolResult: &modelToolResult{CallID: message.ToolCallID, ToolName: message.ToolName, Text: message.Content}})
			index++
		default:
			return nil, fmt.Errorf("model message %d has unsupported role %q", index, message.Role)
		}
	}
	return result, nil
}

func runtimeHostedToolDefinitions(values []provider.HostedToolDefinition) []publicprovider.HostedToolDefinition {
	result := make([]publicprovider.HostedToolDefinition, 0, len(values))
	for _, value := range values {
		result = append(result, publicprovider.HostedToolDefinition{Name: value.Name, Type: value.Type, Description: value.Description, Parameters: cloneAnyMap(value.Parameters), Options: cloneAnyMap(value.Options)})
	}
	return result
}

func providerStreamEvent(event modelEvent) provider.StreamEvent {
	var hosted provider.ToolCall
	if event.HostedToolCall != nil {
		hosted = provider.ToolCall{ID: event.HostedToolCall.ID, Name: event.HostedToolCall.Name, Args: event.HostedToolCall.Args}
	}
	return provider.StreamEvent{Type: provider.EventType(event.Type), Text: event.Text, ToolCallStream: providerToolCallStream(event.ToolCallStream), ToolCalls: providerToolCalls(event.ToolCalls), ToolCall: hosted, HostedResult: internalHostedToolResult(event.HostedResult), Sources: providerSourceRefs(event.Sources), Reason: event.Reason, Usage: providerUsage(event.Usage), ResponseID: event.ResponseID, ResponseState: providerState(event.ResponseState), Err: event.Err}
}

func providerToolCallStream(value *ToolCallStream) provider.ToolCallStream {
	if value == nil {
		return provider.ToolCallStream{}
	}
	return provider.ToolCallStream{ID: value.ID, Name: value.Name}
}

func providerToolCalls(values []tools.ToolCall) []provider.ToolCall {
	result := make([]provider.ToolCall, 0, len(values))
	for _, value := range values {
		result = append(result, provider.ToolCall{ID: value.ID, Name: value.Name, Args: value.Args, Reasoning: value.Reasoning})
	}
	return result
}

func providerSourceRefs(values []publicprovider.Source) []provider.SourceRef {
	result := make([]provider.SourceRef, 0, len(values))
	for _, value := range values {
		if value.Title != "" || value.URL != "" {
			result = append(result, provider.SourceRef{Title: strings.TrimSpace(value.Title), URL: strings.TrimSpace(value.URL)})
		}
	}
	return result
}

func internalHostedToolResult(value *publicprovider.HostedToolResult) provider.HostedToolResultData {
	if value == nil {
		return provider.HostedToolResultData{}
	}
	items := make([]provider.HostedToolResultItem, len(value.Results))
	for index, item := range value.Results {
		items[index] = provider.HostedToolResultItem{Title: item.Title, URL: item.URL, Snippet: item.Snippet, Source: item.Source, Metadata: cloneAnyMap(item.Metadata)}
	}
	var resultError *provider.HostedToolResultError
	if value.Error != nil {
		resultError = &provider.HostedToolResultError{Code: value.Error.Code, Message: value.Error.Message}
	}
	return provider.HostedToolResultData{Text: value.Text, Results: items, Error: resultError, Metadata: cloneAnyMap(value.Metadata)}
}

func providerUsage(value publicprovider.Usage) provider.Usage {
	return provider.Usage{
		InputTokens: value.InputTokens, OutputTokens: value.OutputTokens, ReasoningTokens: value.ReasoningTokens,
		CacheReadTokens: value.CacheReadTokens, CacheWriteTokens: value.CacheWriteTokens,
		TotalTokens: value.TotalTokens, CostUSD: value.CostUSD, Source: provider.UsageSource(value.Source),
		Available: value.Available, WindowInputTokens: value.WindowInputTokens,
	}
}

func modelState(value *provider.State) *modelStateEnvelope {
	if value == nil {
		return nil
	}
	return &modelStateEnvelope{Kind: value.Kind, ID: value.ID, Attributes: cloneStringMap(value.Attributes)}
}

func providerState(value *modelStateEnvelope) *provider.State {
	if value == nil {
		return nil
	}
	return &provider.State{Kind: value.Kind, ID: value.ID, Attributes: cloneStringMap(value.Attributes)}
}

func providerRequestLabels(value provider.RequestLabels) RunLabels {
	return RunLabels{Correlation: cloneStringMap(value.Correlation), Host: cloneStringMap(value.Host)}
}

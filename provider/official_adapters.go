package provider

import (
	"context"
	"errors"
	"fmt"
	"github.com/floegence/floret/v7/internal/providertransport"
	"github.com/floegence/floret/v7/internal/requestmeasure"
	"net/http"
	"strings"

	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/internal/configbridge"
	internalprovider "github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/provider/adapters"
	"github.com/floegence/floret/v7/internal/provider/cache"
	"github.com/floegence/floret/v7/internal/provider/catalog"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/tools"
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
	// ResolveAttachment resolves authorized content during preparation, never replay.
	ResolveAttachment     func(context.Context, Attachment) (AnthropicAttachment, error)
	MaxRequestBytes       int64
	Temperature           *float64
	TopP                  *float64
	Provider              string
	Model                 string
	BaseURL               string
	APIKey                string
	StateCompatibilityKey string
	HTTPClient            *http.Client
	Capabilities          Capabilities
}

// AnthropicAttachment is authorized model content. Kind is image, document, or
// text. Images accept Data or URL, documents accept Data, and text uses Text
// for an explicit host manifest. Content sources are mutually exclusive.
type AnthropicAttachment struct {
	Kind     string
	MIMEType string
	Data     []byte
	URL      string
	Text     string
}

type officialGateway struct {
	cacheRetention cache.Retention
	identity       Identity
	capabilities   Capabilities
	transport      internalprovider.Provider
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
	if options.MaxRequestBytes < 0 {
		return nil, errors.New("negative Anthropic request byte limit")
	}
	if options.Temperature != nil && !(*options.Temperature >= 0 && *options.Temperature <= 1) {
		return nil, errors.New("invalid Anthropic temperature")
	}
	if options.TopP != nil && !(*options.TopP >= 0 && *options.TopP <= 1) {
		return nil, errors.New("invalid Anthropic top_p")
	}
	model.Reasoning = configbridge.ReasoningCapability(capabilities.ReasoningCapability)
	transport := adapters.AnthropicProvider{
		Endpoint: baseURL + "/messages", APIKey: apiKey, Model: identity.Model,
		MaxTokens: model.MaxTokens, CostModel: model, Cache: catalog.CacheCapability{AnthropicCacheControl: true}, HTTPClient: options.HTTPClient,
		StateIdentity: identity.StateCompatibilityKey, MaxRequestBytes: options.MaxRequestBytes, Temperature: copyAnthropicFloat(options.Temperature), TopP: copyAnthropicFloat(options.TopP),
	}
	if options.ResolveAttachment != nil {
		capabilities.AttachmentPayload = AttachmentExpanded
		transport.ResolveAttachment = func(ctx context.Context, a session.MessageAttachment) (adapters.AnthropicAttachment, error) {
			attachment := Attachment{ResourceRef: a.ResourceRef, Name: a.Name, MIMEType: a.MIMEType, SizeBytes: a.SizeBytes}
			if a.TextStats != nil {
				attachment.TextStats = &AttachmentTextStats{UnicodeCodePointCount: a.TextStats.UnicodeCodePointCount, LogicalLineCount: a.TextStats.LogicalLineCount}
			}
			v, e := options.ResolveAttachment(ctx, attachment)
			return adapters.AnthropicAttachment{Kind: v.Kind, MIMEType: v.MIMEType, Data: v.Data, URL: v.URL, Text: v.Text}, e
		}
	}
	return &officialGateway{identity: identity, capabilities: capabilities, cacheRetention: cache.RetentionShort, transport: transport}, nil
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
	capabilities := gateway.capabilities
	capabilities.OutputControl = OutputControlClient
	return capabilities
}

// Stream validates and executes one public provider request.
func (gateway *officialGateway) Stream(ctx context.Context, request Request) (<-chan Event, error) {
	if gateway == nil || gateway.transport == nil {
		return nil, errors.New("provider gateway is nil")
	}
	if err := request.Validate(); err != nil {
		return nil, err
	}
	stream, err := gateway.transport.Stream(ctx, gateway.request(request))
	if err != nil {
		return nil, officialPreparationError(err)
	}
	return publicOfficialStream(ctx, request, stream), nil
}

func publicOfficialStream(ctx context.Context, request Request, stream <-chan internalprovider.StreamEvent) <-chan Event {
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
				if request.ReusePolicy != nil && !request.ReusePolicy.ProviderStateAllowed {
					event.ResponseState = nil
				}
				select {
				case <-ctx.Done():
					return
				case output <- publicEvent(event):
				}
			}
		}
	}()
	return output
}

func (gateway *officialGateway) request(request Request) internalprovider.Request {
	out := internalRequest(request, gateway.identity)
	if out.Cache.Enabled && gateway.cacheRetention != "" {
		out.Cache.Retention = gateway.cacheRetention
	}
	return out
}

func internalRequest(request Request, identity Identity) internalprovider.Request {
	var policy cache.CachePolicy
	if request.ReusePolicy != nil {
		policy.Enabled = request.ReusePolicy.PromptCacheEnabled
		policy.Namespace = request.ReusePolicy.PromptCacheNamespace
		policy.PreferContinuation = request.ReusePolicy.PreferContinuation
		policy.Retention = cache.RetentionNone
		if policy.Enabled {
			policy.Retention = cache.RetentionInMemory
		}
	}
	return internalprovider.Request{
		DisableProviderState: request.ReusePolicy != nil && !request.ReusePolicy.ProviderStateAllowed,
		AttemptID:            request.AttemptID, AttemptEpoch: request.AttemptEpoch, LogicalRequestID: request.LogicalRequestID.String(),
		Cache: policy,
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
		attachments := message.Attachments
		if message.ToolResult != nil {
			attachments = message.ToolResult.Attachments
		}
		for _, a := range attachments {
			attachment := session.MessageAttachment{ResourceRef: a.ResourceRef, Name: a.Name, MIMEType: a.MIMEType, SizeBytes: a.SizeBytes}
			if a.TextStats != nil {
				attachment.TextStats = &session.MessageAttachmentTextStats{UnicodeCodePointCount: a.TextStats.UnicodeCodePointCount, LogicalLineCount: a.TextStats.LogicalLineCount}
			}
			base.Attachments = append(base.Attachments, attachment)
		}
		if a := message.Attribution; a != nil {
			switch a.Category {
			case "model_context":
				base.Kind = session.MessageKindModelContext
			case "compaction":
				base.Kind = session.MessageKindCompactionSummary
			}
			for _, p := range a.Parts {
				if p.Start >= 0 && p.End >= p.Start && p.End <= len(message.Text) {
					base.ModelContextParts = append(base.ModelContextParts, session.ModelContextPart{Key: p.Key, Text: message.Text[p.Start:p.End]})
				}
			}
		}
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
			ReplaySafe: definition.ReplaySafe, Name: definition.Name, Type: definition.Type, Description: definition.Description,
			Parameters: definition.Parameters, Options: definition.Options,
		}
	}
	return output
}

func internalState(state *State) *internalprovider.State {
	if state == nil {
		return nil
	}
	return &internalprovider.State{ReusePolicy: internalprovider.StateReusePolicy(state.ReusePolicy), Kind: state.Kind, ID: state.ID, Attributes: cloneStringMap(state.Attributes)}
}

func publicEvent(event internalprovider.StreamEvent) Event {
	var part *OutputPart
	if event.OutputPart != nil {
		part = &OutputPart{ItemID: event.OutputPart.ItemID, OutputIndex: event.OutputPart.OutputIndex, ContentIndex: event.OutputPart.ContentIndex}
	}

	calls := make([]ToolCall, len(event.ToolCalls))
	for index, call := range event.ToolCalls {
		calls[index] = ToolCall{ID: call.ID, Name: call.Name, Args: call.Args}
	}
	sources := make([]Source, len(event.Sources))
	for index, source := range event.Sources {
		sources[index] = Source(source)
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
		Transport: providertransport.Clone(event.Transport), OutputPart: part, Type: EventType(event.Type), Text: event.Text, ToolCallStream: stream, ToolCalls: calls,
		HostedToolCall: hostedCall, HostedResult: hostedResult, Sources: sources, Reason: event.Reason,
		Usage: Usage{
			InputTokens: event.Usage.InputTokens, OutputTokens: event.Usage.OutputTokens,
			InputTokensReported: event.Usage.InputTokensReported, InputTokenScope: event.Usage.InputTokenScope,
			OutputTokensReported: event.Usage.OutputTokensReported, Measurement: officialRequestMeasurement(event.Usage.Measurement),
			OutputIncludesReasoning: event.Usage.OutputIncludesReasoning, TotalTokensReported: event.Usage.TotalTokensReported,
			ReasoningTokens: event.Usage.ReasoningTokens, CacheReadTokens: event.Usage.CacheReadTokens,
			CacheWriteTokens: event.Usage.CacheWriteTokens, TotalTokens: event.Usage.TotalTokens,
			CostUSD: event.Usage.CostUSD, Source: string(event.Usage.Source), Available: event.Usage.Available,
			WindowInputTokens: event.Usage.WindowInputTokens,
		},
		ResponseID: event.ResponseID, ResponseState: publicState(event.ResponseState), Err: officialPreparationError(event.Err),
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
	return &HostedToolResult{ResultsProvided: result.ResultsProvided, Text: result.Text, Results: items, Error: resultError, Metadata: result.Metadata}
}

func publicState(state *internalprovider.State) *State {
	if state == nil {
		return nil
	}
	return &State{ReusePolicy: StateReusePolicy(state.ReusePolicy), Kind: state.Kind, ID: state.ID, Attributes: cloneStringMap(state.Attributes)}
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

// Prepare counts and dispatches the same frozen official wire body.
func (gateway *officialGateway) Prepare(ctx context.Context, request Request) (PreparedRequest, error) {
	if err := request.Validate(); err != nil {
		return nil, err
	}
	preparer, ok := gateway.transport.(internalprovider.RequestPreparer)
	if !ok {
		return nil, errors.New("official transport does not prepare requests")
	}
	prepared, err := preparer.PrepareRequest(ctx, gateway.request(request))
	if err != nil {
		return nil, officialPreparationError(err)
	}
	frozen := Request{}
	if request.ReusePolicy != nil {
		policy := *request.ReusePolicy
		frozen.ReusePolicy = &policy
	}
	return &officialPrepared{prepared: prepared, request: frozen}, nil
}

type officialPrepared struct {
	prepared internalprovider.PreparedRequest
	request  Request
}

func (p *officialPrepared) Stream(ctx context.Context) (<-chan Event, error) {
	stream, err := p.prepared.Stream(ctx)
	if err != nil {
		return nil, officialPreparationError(err)
	}
	return publicOfficialStream(ctx, p.request, stream), nil
}
func (p *officialPrepared) TokenEstimate() TokenEstimate {
	v := p.prepared.TokenEstimate()
	return cloneTokenEstimate(TokenEstimate{Characters: v.Characters.Clone(), Breakdown: config.ContextBreakdown(v.Breakdown), AttachmentTokens: v.AttachmentTokens, PrefixTokens: v.PrefixTokens, MessageTokens: v.MessageTokens, ToolDefinitionTokens: v.ToolDefinitionTokens, EstimatedInputTokens: v.EstimatedInputTokens, Source: v.Source, Method: string(v.Method), Confidence: string(v.Confidence), Coverage: string(v.Coverage)})
}
func (p *officialPrepared) RenderedPayloadFingerprint() string {
	return p.prepared.PayloadFingerprint()
}
func (p *officialPrepared) Close() error { return p.prepared.Close() }
func (p *officialPrepared) InputMeasurementPrefix() MeasurementPrefix {
	if m, ok := p.prepared.(interface {
		InputMeasurementPrefix() requestmeasure.Prefix
		MeasurementSuffix(requestmeasure.Prefix) ([]byte, bool)
	}); ok {
		return MeasurementPrefix(m.InputMeasurementPrefix())
	}
	return MeasurementPrefix{}
}
func (p *officialPrepared) MeasurementSuffix(prefix MeasurementPrefix) ([]byte, bool) {
	if m, ok := p.prepared.(interface {
		InputMeasurementPrefix() requestmeasure.Prefix
		MeasurementSuffix(requestmeasure.Prefix) ([]byte, bool)
	}); ok {
		return m.MeasurementSuffix(requestmeasure.Prefix(prefix))
	}
	return nil, false
}
func (p *officialPrepared) PrepareRetry(ctx context.Context, request Request) (PreparedRequest, error) {
	replay, ok := p.prepared.(internalprovider.PreparedRequestReplay)
	if !ok {
		return nil, errors.New("official prepared request cannot replay")
	}
	next, err := replay.PrepareRetry(ctx, internalprovider.Request{AttemptID: request.AttemptID, AttemptEpoch: request.AttemptEpoch, LogicalRequestID: request.LogicalRequestID.String()})
	if err != nil {
		return nil, officialPreparationError(err)
	}
	return &officialPrepared{prepared: next, request: p.request}, nil
}

func (p *officialPrepared) WithMaxOutputTokens(ctx context.Context, limit int64) (PreparedRequest, error) {
	limiter, ok := p.prepared.(internalprovider.PreparedRequestOutputLimiter)
	if !ok {
		return nil, errors.New("official prepared request cannot set output limit")
	}
	next, err := limiter.WithMaxOutputTokens(ctx, limit)
	if err != nil {
		return nil, officialPreparationError(err)
	}
	return &officialPrepared{prepared: next, request: p.request}, nil
}

func officialRequestMeasurement(v *requestmeasure.Evidence) *RequestMeasurement {
	if v == nil {
		return nil
	}
	return &RequestMeasurement{AttemptID: v.AttemptID, CompleteInput: v.CompleteInput, OpaqueInput: v.OpaqueInput, Input: MeasurementPrefix(v.Input), ResponseID: v.ResponseID, ResponseEnd: MeasurementPrefix(v.ResponseEnd)}
}

func copyAnthropicFloat(value *float64) *float64 {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}
func officialPreparationError(err error) error {
	if err == nil {
		return nil
	}
	var wire interface {
		ProviderHTTPDetails() (string, int, string, string)
	}
	if errors.As(err, &wire) {
		name, status, code, message := wire.ProviderHTTPDetails()
		mapped := WithRetryAdvice(&ProviderHTTPError{Provider: name, StatusCode: status, Code: code, Message: message}, RetryAdviceForError(publicRetryError(err)))
		if errors.Is(err, internalprovider.ErrContextOverflow) {
			return errors.Join(ErrContextOverflow, mapped)
		}
		return mapped
	}
	var size interface{ RequestByteLimit() (int64, int64) }
	if errors.As(err, &size) {
		actual, limit := size.RequestByteLimit()
		return &RequestByteLimitError{ActualBytes: actual, LimitBytes: limit}
	}
	if errors.Is(err, internalprovider.ErrContextOverflow) {
		return errors.Join(ErrContextOverflow, publicRetryError(err))
	}
	return publicRetryError(err)
}

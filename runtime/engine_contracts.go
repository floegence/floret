package runtime

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/engine"
	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/session/compaction"
	publicprovider "github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/tools"
)

// IDSource supplies deterministic identities to tests. Production uses the
// cryptographic source installed by Open.
type IDSource interface {
	NewThreadID() (identity.ThreadID, error)
	NewTurnID() (identity.TurnID, error)
	NewRunID() (identity.RunID, error)
}

type randomIDSource struct{}

func (randomIDSource) NewThreadID() (identity.ThreadID, error) {
	value, err := randomIdentity("thread")
	return identity.ThreadID(value), err
}

func (randomIDSource) NewTurnID() (identity.TurnID, error) {
	value, err := randomIdentity("turn")
	return identity.TurnID(value), err
}

func (randomIDSource) NewRunID() (identity.RunID, error) {
	value, err := randomIdentity("run")
	return identity.RunID(value), err
}

func randomIdentity(prefix string) (string, error) {
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return "", err
	}
	return prefix + "-" + hex.EncodeToString(entropy[:]), nil
}

type ManualCompactionPollRequest struct {
	RunID         identity.RunID         `json:"run_id,omitempty"`
	ThreadID      identity.ThreadID      `json:"thread_id,omitempty"`
	TurnID        identity.TurnID        `json:"turn_id,omitempty"`
	TraceID       identity.TraceID       `json:"trace_id,omitempty"`
	PromptScopeID identity.PromptScopeID `json:"prompt_scope_id,omitempty"`
	Step          int                    `json:"step,omitempty"`
}

type ManualCompactionRequest struct {
	RequestID   string    `json:"request_id"`
	Source      string    `json:"source"`
	RequestedAt time.Time `json:"requested_at,omitempty"`
}

func ManualCompactionOperationID(runID identity.RunID, step int, requestID string) string {
	return engine.CompactionOperationID(string(runID), step, compaction.TriggerManual, compaction.ReasonManual, requestID)
}

type ManualCompactionSource interface {
	PollManualCompaction(context.Context, ManualCompactionPollRequest) (ManualCompactionRequest, bool, error)
}

type ToolSurfaceRequest struct {
	RunID         identity.RunID
	ThreadID      identity.ThreadID
	TurnID        identity.TurnID
	TraceID       identity.TraceID
	PromptScopeID identity.PromptScopeID
	Step          int
	Phase         string
	Labels        RunLabels
	HostContext   map[string]string
}

type ToolSurface struct {
	Tools                 *tools.Registry
	ToolDefinitions       []tools.ToolDefinition
	HostedToolDefinitions []publicprovider.HostedToolDefinition
	SystemPrompt          string
	HostContext           map[string]string
	Epoch                 string
	Reason                string
}

type ToolSurfaceProvider func(context.Context, ToolSurfaceRequest) (ToolSurface, error)

// modelGateway is the private effect boundary between ThreadRuntime and a host
// model transport. It does not own thread lifecycle state.
type modelGateway interface {
	StreamModel(context.Context, modelRequest) (<-chan modelEvent, error)
}

type modelGatewayRequestPreparer interface {
	PrepareModelRequest(context.Context, modelRequest) (preparedModelRequest, error)
}

type preparedModelRequest interface {
	StreamModel(context.Context) (<-chan modelEvent, error)
	TokenEstimate() modelRequestTokenEstimate
	RenderedPayloadFingerprint() string
	Close() error
}

type modelRequestTokenEstimateCoverage string

const modelRequestTokenEstimateCoverageComplete modelRequestTokenEstimateCoverage = "complete_request"

type modelRequestTokenEstimate struct {
	PrefixTokens         int64
	MessageTokens        int64
	ToolDefinitionTokens int64
	EstimatedInputTokens int64
	Source               string
	Method               string
	Confidence           string
	Coverage             modelRequestTokenEstimateCoverage
}

type modelGatewayIdentity struct {
	Provider              string
	Model                 string
	StateCompatibilityKey string
}

type modelRequest struct {
	RunID            identity.RunID
	ThreadID         identity.ThreadID
	TurnID           identity.TurnID
	TraceID          identity.TraceID
	PromptScopeID    identity.PromptScopeID
	LogicalRequestID identity.LogicalRequestID
	AttemptID        string
	AttemptEpoch     int
	Step             int
	Provider         string
	Model            string
	Messages         []modelMessage
	Tools            []tools.ToolDefinition
	HostedTools      []publicprovider.HostedToolDefinition
	MaxOutputTokens  int64
	Reasoning        config.ReasoningSelection
	PreviousState    *modelStateEnvelope
	Labels           RunLabels
}

func (request modelRequest) MarshalJSON() ([]byte, error) {
	logicalRequestID := any(nil)
	if request.LogicalRequestID != "" {
		logicalRequestID = request.LogicalRequestID
	}
	return json.Marshal(struct {
		RunID            identity.RunID
		ThreadID         identity.ThreadID
		TurnID           identity.TurnID
		TraceID          identity.TraceID
		PromptScopeID    identity.PromptScopeID
		LogicalRequestID any
		AttemptID        string
		AttemptEpoch     int
		Step             int
		Provider         string
		Model            string
		Messages         []modelMessage
		Tools            []tools.ToolDefinition
		HostedTools      []publicprovider.HostedToolDefinition
		MaxOutputTokens  int64
		Reasoning        config.ReasoningSelection
		PreviousState    *modelStateEnvelope
		Labels           RunLabels
	}{
		RunID: request.RunID, ThreadID: request.ThreadID, TurnID: request.TurnID, TraceID: request.TraceID,
		PromptScopeID: request.PromptScopeID, LogicalRequestID: logicalRequestID, AttemptID: request.AttemptID,
		AttemptEpoch: request.AttemptEpoch, Step: request.Step, Provider: request.Provider, Model: request.Model,
		Messages: request.Messages, Tools: request.Tools, HostedTools: request.HostedTools,
		MaxOutputTokens: request.MaxOutputTokens, Reasoning: request.Reasoning,
		PreviousState: request.PreviousState, Labels: request.Labels,
	})
}

type modelMessageRole string

const (
	modelMessageRoleSystem    modelMessageRole = "system"
	modelMessageRoleUser      modelMessageRole = "user"
	modelMessageRoleAssistant modelMessageRole = "assistant"
	modelMessageRoleTool      modelMessageRole = "tool"
)

func (role modelMessageRole) Valid() bool {
	return role == modelMessageRoleSystem || role == modelMessageRoleUser || role == modelMessageRoleAssistant || role == modelMessageRoleTool
}

type modelToolResult struct {
	CallID   string `json:"call_id"`
	ToolName string `json:"tool_name"`
	Text     string `json:"text,omitempty"`
}

type modelMessage struct {
	Role        modelMessageRole    `json:"role"`
	Text        string              `json:"text,omitempty"`
	Attachments []MessageAttachment `json:"attachments,omitempty"`
	Reasoning   string              `json:"reasoning,omitempty"`
	ToolCalls   []tools.ToolCall    `json:"tool_calls,omitempty"`
	ToolResult  *modelToolResult    `json:"tool_result,omitempty"`
}

func (message modelMessage) Validate() error {
	return message.validateAttachments(session.ValidateMessageAttachments)
}

func (message modelMessage) validateStored() error {
	return message.validateAttachments(session.ValidateStoredMessageAttachments)
}

func (message modelMessage) validateAttachments(validate func([]session.MessageAttachment) error) error {
	if !message.Role.Valid() {
		return fmt.Errorf("unsupported model message role %q", message.Role)
	}
	if message.Role == modelMessageRoleUser {
		if strings.TrimSpace(message.Text) == "" && len(message.Attachments) == 0 {
			return errors.New("user model message requires text or attachments")
		}
		return validate(sessionMessageAttachments(message.Attachments))
	}
	if message.Role == modelMessageRoleTool && (message.ToolResult == nil || strings.TrimSpace(message.ToolResult.CallID) == "" || strings.TrimSpace(message.ToolResult.ToolName) == "") {
		return errors.New("tool model message requires a tool result identity")
	}
	return nil
}

type modelStateEnvelope struct {
	Kind       string            `json:"kind,omitempty"`
	ID         string            `json:"id,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

type modelEventType string

const (
	modelEventDelta            modelEventType = "delta"
	modelEventReasoning        modelEventType = "reasoning"
	modelEventToolCallStart    modelEventType = "tool_call_start"
	modelEventToolCallDelta    modelEventType = "tool_call_delta"
	modelEventToolCallEnd      modelEventType = "tool_call_end"
	modelEventToolCalls        modelEventType = "tool_calls"
	modelEventUsage            modelEventType = "usage"
	modelEventSources          modelEventType = "sources"
	modelEventHostedToolCall   modelEventType = "hosted_tool_call"
	modelEventHostedToolResult modelEventType = "hosted_tool_result"
	modelEventDone             modelEventType = "done"
	modelEventEmpty            modelEventType = "empty"
	modelEventTruncated        modelEventType = "truncated"
	modelEventError            modelEventType = "error"
)

type modelEvent struct {
	Type           modelEventType                   `json:"type"`
	Text           string                           `json:"text,omitempty"`
	ToolCallStream *ToolCallStream                  `json:"tool_call_stream,omitempty"`
	ToolCalls      []tools.ToolCall                 `json:"tool_calls,omitempty"`
	HostedToolCall *publicprovider.ToolCall         `json:"hosted_tool_call,omitempty"`
	HostedResult   *publicprovider.HostedToolResult `json:"hosted_result,omitempty"`
	Sources        []publicprovider.Source          `json:"sources,omitempty"`
	Reason         string                           `json:"reason,omitempty"`
	Usage          publicprovider.Usage             `json:"usage,omitempty"`
	ResponseID     string                           `json:"response_id,omitempty"`
	ResponseState  *modelStateEnvelope              `json:"response_state,omitempty"`
	Err            error                            `json:"-"`
}

type ToolCallStream struct {
	ID   string `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
}

type RunMetrics struct {
	ProviderUsage publicprovider.Usage `json:"provider_usage"`
	Steps         int                  `json:"steps"`
	LLMRequests   int                  `json:"llm_requests"`
	ToolCalls     int                  `json:"tool_calls"`
	Compactions   int                  `json:"compactions"`
	Retries       int                  `json:"retries"`
	WallTimeMS    int64                `json:"wall_time_ms,omitempty"`
}

type TurnLimits struct {
	MaxInputTokens           int64
	MaxTotalTokens           int64
	MaxCostUSD               float64
	MaxToolCalls             int
	MaxLengthContinuations   int
	MaxStopHookContinuations int
}

type SignalDisposition string

const (
	SignalContinue SignalDisposition = "continue"
	SignalWaiting  SignalDisposition = "waiting"
)

type TurnSignal struct {
	Disposition SignalDisposition           `json:"disposition"`
	Name        string                      `json:"name"`
	CallID      string                      `json:"call_id,omitempty"`
	Payload     map[string]any              `json:"payload,omitempty"`
	Activity    *tools.ActivityPresentation `json:"activity,omitempty"`
	OutputText  string                      `json:"output_text,omitempty"`
	ArgsHash    string                      `json:"args_hash,omitempty"`
	Labels      map[string]string           `json:"labels,omitempty"`
}

type TurnSignalSpec struct {
	Definitions []tools.ToolDefinition
	Identity    string
	Project     func(tools.ToolCall) (TurnSignal, bool, error)
}

type manualCompactionSourceAdapter struct {
	source ManualCompactionSource
}

func projectedManualCompactionSource(source ManualCompactionSource) engine.ManualCompactionSource {
	if source == nil {
		return nil
	}
	return manualCompactionSourceAdapter{source: source}
}

func (source manualCompactionSourceAdapter) PollManualCompaction(ctx context.Context, request engine.ManualCompactionPollRequest) (engine.ManualCompactionRequest, bool, error) {
	manual, ok, err := source.source.PollManualCompaction(ctx, ManualCompactionPollRequest{
		RunID: identity.RunID(request.RunID), ThreadID: identity.ThreadID(request.ThreadID),
		TurnID: identity.TurnID(request.TurnID), TraceID: identity.TraceID(request.TraceID),
		PromptScopeID: identity.PromptScopeID(request.PromptScopeID), Step: request.Step,
	})
	if err != nil || !ok {
		return engine.ManualCompactionRequest{}, ok, err
	}
	result := engine.ManualCompactionRequest{RequestID: strings.TrimSpace(manual.RequestID), Source: strings.TrimSpace(manual.Source)}
	if result.RequestID == "" || result.Source == "" {
		return engine.ManualCompactionRequest{}, false, errors.New("manual compaction request id and source are required")
	}
	return result, true, nil
}

func runtimeToolSurfaceProvider(surfaceProvider ToolSurfaceProvider) engine.ToolSurfaceProvider {
	if surfaceProvider == nil {
		return nil
	}
	return func(ctx context.Context, request engine.ToolSurfaceRequest) (engine.ToolSurface, error) {
		surface, err := surfaceProvider(ctx, ToolSurfaceRequest{
			RunID: identity.RunID(request.RunID), ThreadID: identity.ThreadID(request.ThreadID),
			TurnID: identity.TurnID(request.TurnID), TraceID: identity.TraceID(request.TraceID),
			PromptScopeID: identity.PromptScopeID(request.PromptScopeID), Step: request.Step,
			Phase: strings.TrimSpace(request.Phase), Labels: publicRunLabels(request.Labels),
			HostContext: cloneStringMap(request.HostContext),
		})
		if err != nil {
			return engine.ToolSurface{}, err
		}
		return engine.ToolSurface{
			Tools: surface.Tools, ToolDefinitions: normalizeToolDefinitions(surface.ToolDefinitions),
			HostedToolDefinitions: providerHostedToolDefinitions(surface.HostedToolDefinitions),
			SystemPrompt:          surface.SystemPrompt, HostContext: cloneStringMap(surface.HostContext),
			Epoch: strings.TrimSpace(surface.Epoch), Reason: strings.TrimSpace(surface.Reason),
		}, nil
	}
}

func publicRunLabels(labels engine.RunLabels) RunLabels {
	return RunLabels{Correlation: cloneStringMap(labels.Correlation), Host: cloneStringMap(labels.Host)}
}

func agentHarnessSupplementalContext(items []TurnSupplementalContextItem) []engine.TurnSupplementalContextItem {
	if items == nil {
		return nil
	}
	result := make([]engine.TurnSupplementalContextItem, 0, len(items))
	for _, item := range items {
		result = append(result, engine.TurnSupplementalContextItem{
			Kind: item.Kind, Title: item.Title, Text: item.Text, Metadata: cloneStringMap(item.Metadata),
			Sensitive: item.Sensitive, Truncated: item.Truncated,
		})
	}
	return result
}

func runtimeMetrics(metrics engine.RunMetrics) RunMetrics {
	return RunMetrics{
		ProviderUsage: runtimeProviderUsage(metrics.Usage), Steps: metrics.Steps, LLMRequests: metrics.LLMRequests,
		ToolCalls: metrics.ToolCalls, Compactions: metrics.Compactions, Retries: metrics.Retries, WallTimeMS: metrics.WallTimeMS,
	}
}

func runtimeProviderUsage(usage provider.Usage) publicprovider.Usage {
	usage = usage.Normalized()
	return publicprovider.Usage{
		InputTokens: usage.InputTokens, OutputTokens: usage.OutputTokens, ReasoningTokens: usage.ReasoningTokens,
		CacheReadTokens: usage.CacheReadTokens, CacheWriteTokens: usage.CacheWriteTokens, TotalTokens: usage.TotalTokens,
		CostUSD: usage.CostUSD, Source: string(usage.Source), Available: usage.Available, WindowInputTokens: usage.WindowInputTokens,
	}
}

func runtimeTurnSignal(signal *engine.ControlSignal) *TurnSignal {
	if signal == nil {
		return nil
	}
	return &TurnSignal{
		Disposition: SignalDisposition(signal.Disposition), Name: signal.Name, CallID: signal.CallID,
		Payload: cloneAnyMap(signal.Payload), Activity: cloneActivityPresentation(signal.Activity), OutputText: signal.OutputText,
		ArgsHash: signal.ArgsHash, Labels: cloneStringMap(signal.Labels),
	}
}

func normalizeToolDefinitions(definitions []tools.ToolDefinition) []tools.ToolDefinition {
	if definitions == nil {
		return nil
	}
	result := make([]tools.ToolDefinition, 0, len(definitions))
	for _, definition := range definitions {
		if strings.TrimSpace(definition.Name) == "" {
			continue
		}
		definition.Name = strings.TrimSpace(definition.Name)
		definition.Title = strings.TrimSpace(definition.Title)
		definition.Description = strings.TrimSpace(definition.Description)
		definition.InputSchema = cloneAnyMap(definition.InputSchema)
		definition.OutputSchema = cloneAnyMap(definition.OutputSchema)
		definition.Annotations = cloneAnyMap(definition.Annotations)
		result = append(result, definition)
	}
	return result
}

func providerHostedToolDefinitions(definitions []publicprovider.HostedToolDefinition) []provider.HostedToolDefinition {
	if definitions == nil {
		return nil
	}
	result := make([]provider.HostedToolDefinition, 0, len(definitions))
	for _, definition := range definitions {
		if strings.TrimSpace(definition.Type) == "" {
			continue
		}
		result = append(result, provider.HostedToolDefinition{
			Name: strings.TrimSpace(definition.Name), Type: strings.TrimSpace(definition.Type),
			Description: strings.TrimSpace(definition.Description), Parameters: cloneAnyMap(definition.Parameters),
			Options: cloneAnyMap(definition.Options),
		})
	}
	return result
}

func engineTurnSignalSpec(spec TurnSignalSpec) (engine.ControlSpec, error) {
	if len(spec.Definitions) == 0 && spec.Project == nil {
		return engine.ControlSpec{Definitions: []tools.ToolDefinition{}, Project: func(provider.ToolCall) (engine.ControlSignal, bool, error) {
			return engine.ControlSignal{}, false, nil
		}}, nil
	}
	result := engine.ControlSpec{Definitions: normalizeToolDefinitions(spec.Definitions)}
	if spec.Project != nil {
		result.Project = func(call provider.ToolCall) (engine.ControlSignal, bool, error) {
			signal, ok, err := spec.Project(tools.ToolCall{ID: call.ID, Name: call.Name, Args: call.Args, Reasoning: call.Reasoning})
			if err != nil || !ok {
				return engine.ControlSignal{}, ok, err
			}
			return engine.ControlSignal{
				Disposition: engine.ControlDisposition(signal.Disposition), Name: signal.Name, CallID: signal.CallID,
				Payload: cloneAnyMap(signal.Payload), Activity: cloneActivityPresentation(signal.Activity),
				OutputText: signal.OutputText, ArgsHash: signal.ArgsHash, Labels: cloneStringMap(signal.Labels),
			}, true, nil
		}
	}
	if len(result.Definitions) == 0 || result.Project == nil {
		return engine.ControlSpec{}, errors.New("signal spec requires definitions and a projector")
	}
	return result, nil
}

func cloneAnyMap(input map[string]any) map[string]any {
	if input == nil {
		return nil
	}
	result := make(map[string]any, len(input))
	for key, value := range input {
		result[key] = cloneAny(value)
	}
	return result
}

func cloneAny(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneAnyMap(typed)
	case []any:
		result := make([]any, len(typed))
		for index, item := range typed {
			result[index] = cloneAny(item)
		}
		return result
	case []string:
		return append([]string{}, typed...)
	default:
		return value
	}
}

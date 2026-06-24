package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/floegence/floret/config"
	"github.com/floegence/floret/internal/configbridge"
	"github.com/floegence/floret/internal/engine"
	"github.com/floegence/floret/internal/provider"
	"github.com/floegence/floret/internal/provider/adapters"
	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/session/compaction"
	"github.com/floegence/floret/observation"
	"github.com/floegence/floret/tools"
)

// ProjectedTurnOptions configures one Floret-managed run over a host-owned
// transcript projection.
type ProjectedTurnOptions struct {
	Config               config.Config
	ModelGateway         ModelGateway
	Store                *Store
	Tools                *tools.Registry
	Approver             tools.Approver
	Sink                 EventSink
	LoopLimits           LoopLimits
	Capabilities         CapabilityOptions
	CompactionSummarizer ProjectedTurnCompactionSummarizer
	ManualCompactions    ManualCompactionSource
}

// ProjectedTurnRequest is the provider-visible transcript projection for one
// run. Execution identity fields are required and are not inferred from each
// other.
type ProjectedTurnRequest struct {
	RunID                 RunID
	ThreadID              ThreadID
	TurnID                TurnID
	TraceID               TraceID
	PromptScopeID         PromptScopeID
	History               []TranscriptMessage
	Labels                RunLabels
	PreviousProviderState *ModelState
	Completion            TurnCompletionPolicy
	Signals               TurnSignalSpec
	Limits                TurnLimits
	Reasoning             ReasoningSelection
}

// ProjectedTurnResult is the host-safe outcome of RunProjectedTurn.
type ProjectedTurnResult struct {
	RunID              RunID                        `json:"run_id,omitempty"`
	ThreadID           ThreadID                     `json:"thread_id,omitempty"`
	TurnID             TurnID                       `json:"turn_id,omitempty"`
	Status             TurnStatus                   `json:"status"`
	Output             string                       `json:"output,omitempty"`
	Error              string                       `json:"error,omitempty"`
	Metrics            RunMetrics                   `json:"metrics"`
	Transcript         []TranscriptMessage          `json:"transcript,omitempty"`
	CompletionReason   string                       `json:"completion_reason,omitempty"`
	ContinuationReason string                       `json:"continuation_reason,omitempty"`
	FinishReason       string                       `json:"finish_reason,omitempty"`
	RawFinishReason    string                       `json:"raw_finish_reason,omitempty"`
	FinishInferred     bool                         `json:"finish_inferred,omitempty"`
	ProviderState      *ModelState                  `json:"provider_state,omitempty"`
	Signal             *TurnSignal                  `json:"signal,omitempty"`
	ActivityTimeline   observation.ActivityTimeline `json:"activity_timeline"`
}

// TranscriptMessage is the small message shape accepted by RunProjectedTurn.
// Supported roles are user, assistant, and tool. System instructions belong in
// config.Config rather than host-projected transcript history.
type TranscriptMessage struct {
	Role                 string `json:"role"`
	Content              string `json:"content,omitempty"`
	Reasoning            string `json:"reasoning,omitempty"`
	ToolCallID           string `json:"tool_call_id,omitempty"`
	ToolName             string `json:"tool_name,omitempty"`
	ToolArgs             string `json:"tool_args,omitempty"`
	Kind                 string `json:"kind,omitempty"`
	EntryID              string `json:"entry_id,omitempty"`
	ParentEntryID        string `json:"parent_entry_id,omitempty"`
	CompactionID         string `json:"compaction_id,omitempty"`
	CompactionGeneration int    `json:"compaction_generation,omitempty"`
	CompactionWindowID   string `json:"compaction_window_id,omitempty"`
}

const TranscriptMessageKindCompactionSummary = "compaction_summary"

// ProjectedTurnCompactionSummarizer lets a host provide only the summary text
// for a projected-turn compaction. Floret still owns cut-point selection,
// checkpoint message construction, generation/window metadata, and lifecycle
// events.
type ProjectedTurnCompactionSummarizer interface {
	GenerateCompactionSummary(context.Context, ProjectedCompactionSummaryRequest) (ProjectedCompactionSummaryResult, error)
}

type ProjectedCompactionSummaryRequest struct {
	RunID                RunID                `json:"run_id,omitempty"`
	ThreadID             ThreadID             `json:"thread_id,omitempty"`
	TurnID               TurnID               `json:"turn_id,omitempty"`
	TraceID              TraceID              `json:"trace_id,omitempty"`
	PromptScopeID        PromptScopeID        `json:"prompt_scope_id,omitempty"`
	Step                 int                  `json:"step,omitempty"`
	History              []TranscriptMessage  `json:"history,omitempty"`
	CompactedHead        []TranscriptMessage  `json:"compacted_head,omitempty"`
	RetainedTail         []TranscriptMessage  `json:"retained_tail,omitempty"`
	Policy               config.ContextPolicy `json:"policy,omitempty"`
	Trigger              string               `json:"trigger,omitempty"`
	Reason               string               `json:"reason,omitempty"`
	Phase                string               `json:"phase,omitempty"`
	PreviousCompactionID string               `json:"previous_compaction_id,omitempty"`
	PreviousSummary      string               `json:"previous_summary,omitempty"`
	ContextUsage         config.ContextUsage  `json:"context_usage,omitempty"`
	Details              map[string]string    `json:"details,omitempty"`
}

type ProjectedCompactionSummaryResult struct {
	Summary string            `json:"summary"`
	Details map[string]string `json:"details,omitempty"`
}

type ManualCompactionPollRequest struct {
	RunID         RunID         `json:"run_id,omitempty"`
	ThreadID      ThreadID      `json:"thread_id,omitempty"`
	TurnID        TurnID        `json:"turn_id,omitempty"`
	TraceID       TraceID       `json:"trace_id,omitempty"`
	PromptScopeID PromptScopeID `json:"prompt_scope_id,omitempty"`
	Step          int           `json:"step,omitempty"`
}

type ManualCompactionRequest struct {
	RequestID   string    `json:"request_id"`
	Source      string    `json:"source,omitempty"`
	RequestedAt time.Time `json:"requested_at,omitempty"`
}

type ManualCompactionSource interface {
	PollManualCompaction(context.Context, ManualCompactionPollRequest) (ManualCompactionRequest, bool, error)
}

type ProjectedContextCompactionRequest struct {
	RunID                 RunID
	ThreadID              ThreadID
	TurnID                TurnID
	TraceID               TraceID
	PromptScopeID         PromptScopeID
	History               []TranscriptMessage
	Labels                RunLabels
	PreviousProviderState *ModelState
	Reasoning             ReasoningSelection
	ManualCompaction      ManualCompactionRequest
}

type ProjectedContextCompactionResult struct {
	RunID            RunID                        `json:"run_id,omitempty"`
	ThreadID         ThreadID                     `json:"thread_id,omitempty"`
	TurnID           TurnID                       `json:"turn_id,omitempty"`
	Status           string                       `json:"status"`
	Error            string                       `json:"error,omitempty"`
	Metrics          RunMetrics                   `json:"metrics"`
	ActiveTranscript []TranscriptMessage          `json:"active_transcript,omitempty"`
	Compaction       *ProjectedContextCompaction  `json:"compaction,omitempty"`
	ProviderState    *ModelState                  `json:"provider_state,omitempty"`
	ActivityTimeline observation.ActivityTimeline `json:"activity_timeline"`
}

type ProjectedContextCompaction struct {
	OperationID             string              `json:"operation_id,omitempty"`
	RequestID               string              `json:"request_id,omitempty"`
	Source                  string              `json:"source,omitempty"`
	CompactionID            string              `json:"compaction_id,omitempty"`
	PreviousCompactionID    string              `json:"previous_compaction_id,omitempty"`
	CompactionGeneration    int                 `json:"compaction_generation,omitempty"`
	CompactionWindowID      string              `json:"compaction_window_id,omitempty"`
	FirstKeptEntryID        string              `json:"first_kept_entry_id,omitempty"`
	KeptUserEntryIDs        []string            `json:"kept_user_entry_ids,omitempty"`
	CompactedThroughEntryID string              `json:"compacted_through_entry_id,omitempty"`
	Summary                 string              `json:"summary,omitempty"`
	SummarySchemaVersion    string              `json:"summary_schema_version,omitempty"`
	Trigger                 string              `json:"trigger,omitempty"`
	Reason                  string              `json:"reason,omitempty"`
	Phase                   string              `json:"phase,omitempty"`
	TokensBefore            int64               `json:"tokens_before,omitempty"`
	TokensAfterEstimate     int64               `json:"tokens_after_estimate,omitempty"`
	UsageBefore             config.ContextUsage `json:"usage_before,omitempty"`
	UsageAfter              config.ContextUsage `json:"usage_after,omitempty"`
	Details                 map[string]string   `json:"details,omitempty"`
	CreatedAt               time.Time           `json:"created_at,omitempty"`
}

// ModelGateway lets a host supply model access while Floret still owns the
// agent loop, tool dispatch, context pressure, and runtime ledgers.
type ModelGateway interface {
	StreamModel(context.Context, ModelRequest) (<-chan ModelEvent, error)
}

// ModelRequest is the host-safe model request shape passed to ModelGateway.
type ModelRequest struct {
	RunID           RunID
	ThreadID        ThreadID
	TurnID          TurnID
	TraceID         TraceID
	PromptScopeID   PromptScopeID
	Step            int
	Provider        string
	Model           string
	Messages        []ModelMessage
	Tools           []tools.ToolDefinition
	MaxOutputTokens int64
	Reasoning       ReasoningSelection
	PreviousState   *ModelState
	Labels          RunLabels
}

// ModelMessage is a provider-visible message generated by Floret for a
// ModelGateway request.
type ModelMessage struct {
	Role       string `json:"role"`
	Content    string `json:"content,omitempty"`
	Reasoning  string `json:"reasoning,omitempty"`
	ToolCallID string `json:"tool_call_id,omitempty"`
	ToolName   string `json:"tool_name,omitempty"`
	ToolArgs   string `json:"tool_args,omitempty"`
}

// ModelState is opaque provider continuation state. Floret carries it through
// the run lifecycle and returns the latest state; hosts and model gateways own
// provider-specific interpretation and cross-turn persistence.
type ModelState struct {
	Kind       string            `json:"kind,omitempty"`
	ID         string            `json:"id,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

// ModelEventType is a streamed model event kind.
type ModelEventType string

const (
	ModelEventDelta         ModelEventType = "delta"
	ModelEventReasoning     ModelEventType = "reasoning"
	ModelEventToolCallStart ModelEventType = "tool_call_start"
	ModelEventToolCallDelta ModelEventType = "tool_call_delta"
	ModelEventToolCallEnd   ModelEventType = "tool_call_end"
	ModelEventToolCalls     ModelEventType = "tool_calls"
	ModelEventUsage         ModelEventType = "usage"
	ModelEventSources       ModelEventType = "sources"
	ModelEventDone          ModelEventType = "done"
	ModelEventEmpty         ModelEventType = "empty"
	ModelEventTruncated     ModelEventType = "truncated"
	ModelEventError         ModelEventType = "error"
)

// ModelToolCallStream identifies a tool call while the model is still
// generating it. The final executable tool calls are delivered separately by
// ModelEventToolCalls.
type ModelToolCallStream struct {
	ID   string `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
}

// ModelEvent carries streamed model output.
type ModelEvent struct {
	Type           ModelEventType       `json:"type"`
	Text           string               `json:"text,omitempty"`
	ToolCallStream *ModelToolCallStream `json:"tool_call_stream,omitempty"`
	ToolCalls      []tools.ToolCall     `json:"tool_calls,omitempty"`
	Sources        []SourceRef          `json:"sources,omitempty"`
	Reason         string               `json:"reason,omitempty"`
	Usage          ProviderUsage        `json:"usage,omitempty"`
	ResponseID     string               `json:"response_id,omitempty"`
	ResponseState  *ModelState          `json:"response_state,omitempty"`
	Err            error                `json:"-"`
}

type SourceRef struct {
	Title string `json:"title,omitempty"`
	URL   string `json:"url,omitempty"`
}

// RunMetrics summarizes the observable work completed by a run.
type RunMetrics struct {
	ProviderUsage ProviderUsage `json:"provider_usage"`
	Steps         int           `json:"steps"`
	LLMRequests   int           `json:"llm_requests"`
	ToolCalls     int           `json:"tool_calls"`
	Compactions   int           `json:"compactions"`
	Retries       int           `json:"retries"`
	WallTimeMS    int64         `json:"wall_time_ms,omitempty"`
}

// ProviderUsage is normalized provider token and cost usage.
type ProviderUsage struct {
	InputTokens       int64   `json:"input_tokens,omitempty"`
	OutputTokens      int64   `json:"output_tokens,omitempty"`
	ReasoningTokens   int64   `json:"reasoning_tokens,omitempty"`
	CacheReadTokens   int64   `json:"cache_read_tokens,omitempty"`
	CacheWriteTokens  int64   `json:"cache_write_tokens,omitempty"`
	TotalTokens       int64   `json:"total_tokens,omitempty"`
	CostUSD           float64 `json:"cost_usd,omitempty"`
	Source            string  `json:"source,omitempty"`
	Available         bool    `json:"available,omitempty"`
	WindowInputTokens int64   `json:"window_input_tokens,omitempty"`
}

// TurnCompletionPolicy controls how the provider loop may finish. The zero
// value uses natural stops.
type TurnCompletionPolicy string

const (
	// TurnCompletionNaturalStop lets the provider's natural stop finish the run.
	TurnCompletionNaturalStop TurnCompletionPolicy = "natural_stop"
	// TurnCompletionExplicitSignal requires a projected turn signal to finish or
	// pause the run.
	TurnCompletionExplicitSignal TurnCompletionPolicy = "explicit_signal"
)

// TurnLimits contains per-run budget and continuation caps.
type TurnLimits struct {
	MaxTotalTokens           int64
	MaxCostUSD               float64
	MaxToolCalls             int
	MaxLengthContinuations   int
	MaxStopHookContinuations int
}

// SignalDisposition describes how a projected turn signal affects the run.
type SignalDisposition string

const (
	// SignalContinue returns a provider-visible tool result and continues.
	SignalContinue SignalDisposition = "continue"
	// SignalWaiting pauses the run for host or user input.
	SignalWaiting SignalDisposition = "waiting"
	// SignalTerminal completes the run with the projected signal.
	SignalTerminal SignalDisposition = "terminal"
)

// TurnSignal is a host-safe projection of a signal tool call.
type TurnSignal struct {
	Disposition SignalDisposition                 `json:"disposition"`
	Name        string                            `json:"name"`
	CallID      string                            `json:"call_id,omitempty"`
	Payload     map[string]any                    `json:"payload,omitempty"`
	Activity    *observation.ActivityPresentation `json:"activity,omitempty"`
	OutputText  string                            `json:"output_text,omitempty"`
	ArgsHash    string                            `json:"args_hash,omitempty"`
	Labels      map[string]string                 `json:"labels,omitempty"`
}

// TurnSignalSpec lets a host declare provider-visible signal tools without
// importing Floret implementation packages.
type TurnSignalSpec struct {
	Definitions []tools.ToolDefinition
	Project     func(tools.ToolCall) (TurnSignal, bool, error)
}

// RunProjectedTurn executes one Floret-managed provider loop over a transcript
// supplied by the host.
//
// Use this when the product owns durable conversation rows and Floret owns the
// turn execution, prompt scope, provider ledger, tool dispatch, and observable
// runtime events. Use Host for Floret-managed durable conversations.
func RunProjectedTurn(ctx context.Context, opts ProjectedTurnOptions, req ProjectedTurnRequest) (ProjectedTurnResult, error) {
	cfg, err := config.Resolve(opts.Config, nil)
	if err != nil {
		return ProjectedTurnResult{}, err
	}
	ids, err := projectedTurnIdentity(req)
	if err != nil {
		return ProjectedTurnResult{}, err
	}
	completionPolicy, err := engineTurnCompletionPolicy(req.Completion)
	if err != nil {
		return ProjectedTurnResult{}, err
	}
	signalSpec, err := engineTurnSignalSpec(req.Signals, completionPolicy)
	if err != nil {
		return ProjectedTurnResult{}, err
	}
	modelProvider, err := projectedModelProvider(cfg, opts.ModelGateway)
	if err != nil {
		return ProjectedTurnResult{}, err
	}
	store := opts.Store
	if store == nil {
		store = NewMemoryStore()
	}
	if err := store.validate(); err != nil {
		return ProjectedTurnResult{}, err
	}
	registry := opts.Tools
	if registry == nil {
		registry = tools.NewRegistry()
	}
	activityRecorder := &runtimeActivityEventRecorder{sink: runtimeEventSink{sink: opts.Sink}}
	capabilities := mergeCapabilityOptions(cfg, opts.Capabilities)
	effectivePrompt, err := applyCapabilities(registry, cfg.SystemPrompt, capabilities, activityRecorder)
	if err != nil {
		return ProjectedTurnResult{}, err
	}
	cacheRetention, err := config.PromptCacheRetention(cfg)
	if err != nil {
		return ProjectedTurnResult{}, err
	}
	history, err := projectedHistory(req.History)
	if err != nil {
		return ProjectedTurnResult{}, err
	}
	previousProviderState := providerState(req.PreviousProviderState)
	loopLimits := projectedLoopLimits(cfg, opts.LoopLimits, req.Limits)
	eng, err := engine.New(engine.Config{
		Provider:     modelProvider,
		Store:        session.NewMemoryStore(),
		Prompt:       store.prompt,
		Artifacts:    store.artifacts,
		SystemPrompt: effectivePrompt,
		Tools:        registry,
		Sink:         activityRecorder,
		Approver:     opts.Approver,
		Compactor:    projectedCompactionManager(opts.CompactionSummarizer),
		Options: engine.Options{
			RunID:                    ids.runID,
			ThreadID:                 ids.threadID,
			TurnID:                   ids.turnID,
			TraceID:                  ids.traceID,
			PromptScopeID:            ids.promptScopeID,
			ProviderName:             cfg.Provider,
			Model:                    cfg.Model,
			Labels:                   engineLabels(req.Labels),
			CacheRetention:           configbridge.CacheRetention(cacheRetention),
			ContextPolicy:            configbridge.ContextPolicy(cfg.ContextPolicy),
			Reasoning:                projectedReasoningSelection(req.Reasoning, cfg.Reasoning),
			MaxEmptyProviderRetries:  loopLimits.MaxEmptyProviderRetries,
			NoProgressLimit:          loopLimits.NoProgressLimit,
			DuplicateToolLimit:       loopLimits.DuplicateToolLimit,
			WallTime:                 loopLimits.WallTime,
			MaxTotalTokens:           loopLimits.MaxTotalTokens,
			MaxCostUSD:               loopLimits.MaxCostUSD,
			MaxToolCalls:             loopLimits.MaxToolCalls,
			MaxLengthContinuations:   loopLimits.MaxLengthContinuations,
			MaxStopHookContinuations: loopLimits.MaxStopHookContinuations,
			CompletionPolicy:         completionPolicy,
			ControlSpec:              signalSpec,
			PreviousProviderState:    previousProviderState,
			ManualCompactions:        projectedManualCompactionSource(opts.ManualCompactions),
		},
	})
	if err != nil {
		return ProjectedTurnResult{}, err
	}
	result := eng.RunTurn(ctx, engine.RunInput{
		RunID:                 ids.runID,
		ThreadID:              ids.threadID,
		TurnID:                ids.turnID,
		TraceID:               ids.traceID,
		PromptScopeID:         ids.promptScopeID,
		Labels:                engineLabels(req.Labels),
		PreviousProviderState: previousProviderState,
		History:               history,
	})
	return projectedTurnResult(ids, result, activityRecorder.Snapshot(), time.Now().UnixMilli()), result.Err
}

func CompactProjectedContext(ctx context.Context, opts ProjectedTurnOptions, req ProjectedContextCompactionRequest) (ProjectedContextCompactionResult, error) {
	cfg, err := config.Resolve(opts.Config, nil)
	if err != nil {
		return ProjectedContextCompactionResult{}, err
	}
	ids, err := projectedContextCompactionIdentity(req)
	if err != nil {
		return ProjectedContextCompactionResult{}, err
	}
	modelProvider, err := projectedModelProvider(cfg, opts.ModelGateway)
	if err != nil {
		return ProjectedContextCompactionResult{}, err
	}
	store := opts.Store
	if store == nil {
		store = NewMemoryStore()
	}
	if err := store.validate(); err != nil {
		return ProjectedContextCompactionResult{}, err
	}
	registry := opts.Tools
	if registry == nil {
		registry = tools.NewRegistry()
	}
	activityRecorder := &runtimeActivityEventRecorder{sink: runtimeEventSink{sink: opts.Sink}}
	capabilities := mergeCapabilityOptions(cfg, opts.Capabilities)
	effectivePrompt, err := applyCapabilities(registry, cfg.SystemPrompt, capabilities, activityRecorder)
	if err != nil {
		return ProjectedContextCompactionResult{}, err
	}
	cacheRetention, err := config.PromptCacheRetention(cfg)
	if err != nil {
		return ProjectedContextCompactionResult{}, err
	}
	history, err := projectedHistory(req.History)
	if err != nil {
		return ProjectedContextCompactionResult{}, err
	}
	previousProviderState := providerState(req.PreviousProviderState)
	loopLimits := projectedLoopLimits(cfg, opts.LoopLimits, TurnLimits{})
	eng, err := engine.New(engine.Config{
		Provider:     modelProvider,
		Store:        session.NewMemoryStore(),
		Prompt:       store.prompt,
		Artifacts:    store.artifacts,
		SystemPrompt: effectivePrompt,
		Tools:        registry,
		Sink:         activityRecorder,
		Approver:     opts.Approver,
		Compactor:    projectedCompactionManager(opts.CompactionSummarizer),
		Options: engine.Options{
			RunID:                   ids.runID,
			ThreadID:                ids.threadID,
			TurnID:                  ids.turnID,
			TraceID:                 ids.traceID,
			PromptScopeID:           ids.promptScopeID,
			ProviderName:            cfg.Provider,
			Model:                   cfg.Model,
			Labels:                  engineLabels(req.Labels),
			CacheRetention:          configbridge.CacheRetention(cacheRetention),
			ContextPolicy:           configbridge.ContextPolicy(cfg.ContextPolicy),
			Reasoning:               projectedReasoningSelection(req.Reasoning, cfg.Reasoning),
			MaxEmptyProviderRetries: loopLimits.MaxEmptyProviderRetries,
			NoProgressLimit:         loopLimits.NoProgressLimit,
			DuplicateToolLimit:      loopLimits.DuplicateToolLimit,
			WallTime:                loopLimits.WallTime,
			CompletionPolicy:        engine.CompletionNaturalStop,
			ControlSpec: engine.ControlSpec{
				Definitions: []provider.ToolDefinition{},
				Project: func(provider.ToolCall) (engine.ControlSignal, bool, error) {
					return engine.ControlSignal{}, false, nil
				},
			},
			PreviousProviderState: previousProviderState,
		},
	})
	if err != nil {
		return ProjectedContextCompactionResult{}, err
	}
	result := eng.CompactContext(ctx, engine.RunInput{
		RunID:                 ids.runID,
		ThreadID:              ids.threadID,
		TurnID:                ids.turnID,
		TraceID:               ids.traceID,
		PromptScopeID:         ids.promptScopeID,
		Labels:                engineLabels(req.Labels),
		PreviousProviderState: previousProviderState,
		History:               history,
	}, engineManualCompactionRequest(req.ManualCompaction))
	out := projectedContextCompactionResult(ids, result, activityRecorder.Snapshot(), time.Now().UnixMilli())
	return out, result.Err
}

func projectedCompactionManager(summarizer ProjectedTurnCompactionSummarizer) engine.CompactionManager {
	if summarizer == nil {
		return nil
	}
	return engine.LocalCompactionManager{Generator: projectedCompactionSummaryGenerator{summarizer: summarizer}}
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

func (s manualCompactionSourceAdapter) PollManualCompaction(ctx context.Context, req engine.ManualCompactionPollRequest) (engine.ManualCompactionRequest, bool, error) {
	if s.source == nil {
		return engine.ManualCompactionRequest{}, false, nil
	}
	manual, ok, err := s.source.PollManualCompaction(ctx, ManualCompactionPollRequest{
		RunID:         RunID(req.RunID),
		ThreadID:      ThreadID(req.ThreadID),
		TurnID:        TurnID(req.TurnID),
		TraceID:       TraceID(req.TraceID),
		PromptScopeID: PromptScopeID(req.PromptScopeID),
		Step:          req.Step,
	})
	if err != nil || !ok {
		return engine.ManualCompactionRequest{}, ok, err
	}
	return engineManualCompactionRequest(manual), true, nil
}

func engineManualCompactionRequest(manual ManualCompactionRequest) engine.ManualCompactionRequest {
	return engine.ManualCompactionRequest{
		RequestID: strings.TrimSpace(manual.RequestID),
		Source:    strings.TrimSpace(manual.Source),
	}
}

type projectedCompactionSummaryGenerator struct {
	summarizer ProjectedTurnCompactionSummarizer
}

func (g projectedCompactionSummaryGenerator) GenerateSummary(ctx context.Context, prep compaction.Preparation) (string, error) {
	if g.summarizer == nil {
		return "", errors.New("projected compaction summarizer is required")
	}
	result, err := g.summarizer.GenerateCompactionSummary(ctx, ProjectedCompactionSummaryRequest{
		RunID:                RunID(prep.Request.Details["run_id"]),
		ThreadID:             ThreadID(prep.Request.Details["thread_id"]),
		TurnID:               TurnID(prep.Request.Details["turn_id"]),
		TraceID:              TraceID(prep.Request.Details["trace_id"]),
		PromptScopeID:        PromptScopeID(prep.Request.Details["prompt_scope_id"]),
		Step:                 prep.Request.Step,
		History:              runtimeMessages(prep.Request.History),
		CompactedHead:        runtimeMessages(prep.CompactedHead),
		RetainedTail:         runtimeMessages(prep.RetainedTail),
		Policy:               configbridge.PublicContextPolicy(prep.Request.Policy),
		Trigger:              string(prep.Request.Trigger),
		Reason:               string(prep.Request.Reason),
		Phase:                string(prep.Request.Phase),
		PreviousCompactionID: prep.Request.PreviousCompactionID,
		PreviousSummary:      prep.Request.PreviousSummary,
		ContextUsage:         configbridge.PublicContextUsage(prep.Result.UsageBefore),
		Details:              cloneStringMap(prep.Request.Details),
	})
	if err != nil {
		return "", err
	}
	if len(result.Details) > 0 {
		if prep.Result.Details == nil {
			prep.Result.Details = map[string]string{}
		}
		for key, value := range result.Details {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			prep.Result.Details["host."+key] = value
		}
	}
	return strings.TrimSpace(result.Summary), nil
}

func projectedModelProvider(cfg config.Config, gateway ModelGateway) (provider.Provider, error) {
	if gateway != nil {
		return modelGatewayProvider{gateway: gateway}, nil
	}
	return adapters.NewProvider(cfg)
}

func projectedReasoningSelection(requested ReasoningSelection, fallback ReasoningSelection) provider.ReasoningSelection {
	requested = config.NormalizeReasoningSelection(requested)
	if !requested.IsZero() {
		return configbridge.ReasoningSelection(requested)
	}
	return configbridge.ReasoningSelection(fallback)
}

type modelGatewayProvider struct {
	gateway ModelGateway
}

func (p modelGatewayProvider) Stream(ctx context.Context, req provider.Request) (<-chan provider.StreamEvent, error) {
	if p.gateway == nil {
		return nil, errors.New("model gateway is required")
	}
	stream, err := p.gateway.StreamModel(ctx, ModelRequest{
		RunID:           RunID(req.RunID),
		ThreadID:        ThreadID(req.ThreadID),
		TurnID:          TurnID(req.TurnID),
		TraceID:         TraceID(req.TraceID),
		PromptScopeID:   PromptScopeID(req.PromptScopeID),
		Step:            req.Step,
		Provider:        req.Provider,
		Model:           req.Model,
		Messages:        runtimeModelMessages(req.Messages),
		Tools:           runtimeToolDefinitions(req.Tools),
		MaxOutputTokens: req.MaxOutputTokens,
		Reasoning:       configbridge.PublicReasoningSelection(req.Reasoning),
		PreviousState:   modelState(req.PreviousState),
		Labels:          providerRequestLabels(req.Labels),
	})
	if err != nil {
		return nil, err
	}
	out := make(chan provider.StreamEvent)
	go func() {
		defer close(out)
		for ev := range stream {
			select {
			case <-ctx.Done():
				return
			case out <- providerStreamEvent(ev):
			}
		}
	}()
	return out, nil
}

type projectedIDs struct {
	runID         string
	threadID      string
	turnID        string
	traceID       string
	promptScopeID string
}

type projectedLimits struct {
	MaxEmptyProviderRetries  int
	NoProgressLimit          int
	DuplicateToolLimit       int
	WallTime                 time.Duration
	MaxTotalTokens           int64
	MaxCostUSD               float64
	MaxToolCalls             int
	MaxLengthContinuations   int
	MaxStopHookContinuations int
}

func projectedTurnIdentity(req ProjectedTurnRequest) (projectedIDs, error) {
	runID := strings.TrimSpace(string(req.RunID))
	threadID := strings.TrimSpace(string(req.ThreadID))
	turnID := strings.TrimSpace(string(req.TurnID))
	traceID := strings.TrimSpace(string(req.TraceID))
	promptScopeID := strings.TrimSpace(string(req.PromptScopeID))
	switch {
	case runID == "":
		return projectedIDs{}, errors.New("run id is required")
	case threadID == "":
		return projectedIDs{}, errors.New("thread id is required")
	case turnID == "":
		return projectedIDs{}, errors.New("turn id is required")
	case traceID == "":
		return projectedIDs{}, errors.New("trace id is required")
	case promptScopeID == "":
		return projectedIDs{}, errors.New("prompt scope id is required")
	}
	return projectedIDs{
		runID:         runID,
		threadID:      threadID,
		turnID:        turnID,
		traceID:       traceID,
		promptScopeID: promptScopeID,
	}, nil
}

func projectedContextCompactionIdentity(req ProjectedContextCompactionRequest) (projectedIDs, error) {
	runID := strings.TrimSpace(string(req.RunID))
	threadID := strings.TrimSpace(string(req.ThreadID))
	turnID := strings.TrimSpace(string(req.TurnID))
	traceID := strings.TrimSpace(string(req.TraceID))
	promptScopeID := strings.TrimSpace(string(req.PromptScopeID))
	switch {
	case runID == "":
		return projectedIDs{}, errors.New("run id is required")
	case threadID == "":
		return projectedIDs{}, errors.New("thread id is required")
	case turnID == "":
		return projectedIDs{}, errors.New("turn id is required")
	case traceID == "":
		return projectedIDs{}, errors.New("trace id is required")
	case promptScopeID == "":
		return projectedIDs{}, errors.New("prompt scope id is required")
	case strings.TrimSpace(req.ManualCompaction.RequestID) == "":
		return projectedIDs{}, errors.New("manual compaction request id is required")
	}
	return projectedIDs{
		runID:         runID,
		threadID:      threadID,
		turnID:        turnID,
		traceID:       traceID,
		promptScopeID: promptScopeID,
	}, nil
}

func projectedLoopLimits(cfg config.Config, opts LoopLimits, req TurnLimits) projectedLimits {
	out := projectedLimits{
		MaxEmptyProviderRetries: cfg.MaxEmptyProviderRetries,
		NoProgressLimit:         cfg.NoProgressLimit,
		DuplicateToolLimit:      cfg.DuplicateToolLimit,
		WallTime:                cfg.WallTime,
	}
	if opts.MaxEmptyProviderRetries > 0 {
		out.MaxEmptyProviderRetries = opts.MaxEmptyProviderRetries
	}
	if opts.NoProgressLimit > 0 {
		out.NoProgressLimit = opts.NoProgressLimit
	}
	if opts.DuplicateToolLimit > 0 {
		out.DuplicateToolLimit = opts.DuplicateToolLimit
	}
	if opts.WallTime > 0 {
		out.WallTime = opts.WallTime
	}
	out.MaxTotalTokens = req.MaxTotalTokens
	out.MaxCostUSD = req.MaxCostUSD
	out.MaxToolCalls = req.MaxToolCalls
	out.MaxLengthContinuations = req.MaxLengthContinuations
	out.MaxStopHookContinuations = req.MaxStopHookContinuations
	return out
}

func projectedHistory(messages []TranscriptMessage) ([]session.Message, error) {
	out := make([]session.Message, 0, len(messages))
	for i, msg := range messages {
		role := strings.ToLower(strings.TrimSpace(msg.Role))
		var sessionRole session.Role
		switch role {
		case string(session.User):
			sessionRole = session.User
		case string(session.Assistant):
			sessionRole = session.Assistant
		case string(session.Tool):
			sessionRole = session.Tool
		default:
			return nil, fmt.Errorf("transcript message %d has unsupported role %q", i, msg.Role)
		}
		out = append(out, session.Message{
			Role:                 sessionRole,
			Content:              strings.TrimSpace(msg.Content),
			Reasoning:            strings.TrimSpace(msg.Reasoning),
			ToolCallID:           strings.TrimSpace(msg.ToolCallID),
			ToolName:             strings.TrimSpace(msg.ToolName),
			ToolArgs:             strings.TrimSpace(msg.ToolArgs),
			Kind:                 session.MessageKind(strings.TrimSpace(msg.Kind)),
			EntryID:              strings.TrimSpace(msg.EntryID),
			ParentEntryID:        strings.TrimSpace(msg.ParentEntryID),
			CompactionID:         strings.TrimSpace(msg.CompactionID),
			CompactionGeneration: msg.CompactionGeneration,
			CompactionWindowID:   strings.TrimSpace(msg.CompactionWindowID),
		})
	}
	return out, nil
}

func runtimeMessages(messages []session.Message) []TranscriptMessage {
	out := make([]TranscriptMessage, 0, len(messages))
	for _, msg := range messages {
		out = append(out, TranscriptMessage{
			Role:                 string(msg.Role),
			Content:              msg.Content,
			Reasoning:            msg.Reasoning,
			ToolCallID:           msg.ToolCallID,
			ToolName:             msg.ToolName,
			ToolArgs:             msg.ToolArgs,
			Kind:                 string(msg.Kind),
			EntryID:              msg.EntryID,
			ParentEntryID:        msg.ParentEntryID,
			CompactionID:         msg.CompactionID,
			CompactionGeneration: msg.CompactionGeneration,
			CompactionWindowID:   msg.CompactionWindowID,
		})
	}
	return out
}

func runtimeModelMessages(messages []session.Message) []ModelMessage {
	out := make([]ModelMessage, 0, len(messages))
	for _, msg := range messages {
		out = append(out, ModelMessage{
			Role:       string(msg.Role),
			Content:    msg.Content,
			Reasoning:  msg.Reasoning,
			ToolCallID: msg.ToolCallID,
			ToolName:   msg.ToolName,
			ToolArgs:   msg.ToolArgs,
		})
	}
	return out
}

func runtimeToolDefinitions(defs []provider.ToolDefinition) []tools.ToolDefinition {
	out := make([]tools.ToolDefinition, 0, len(defs))
	for _, def := range defs {
		out = append(out, tools.ToolDefinition{
			Name:         def.Name,
			Title:        def.Title,
			Description:  def.Description,
			InputSchema:  cloneAnyMap(def.InputSchema),
			OutputSchema: cloneAnyMap(def.OutputSchema),
			Strict:       def.Strict,
			Annotations:  cloneAnyMap(def.Annotations),
		})
	}
	return out
}

func providerStreamEvent(ev ModelEvent) provider.StreamEvent {
	return provider.StreamEvent{
		Type:           provider.EventType(ev.Type),
		Text:           ev.Text,
		ToolCallStream: providerToolCallStream(ev.ToolCallStream),
		ToolCalls:      providerToolCalls(ev.ToolCalls),
		Sources:        providerSourceRefs(ev.Sources),
		Reason:         ev.Reason,
		Usage:          providerUsage(ev.Usage),
		ResponseID:     ev.ResponseID,
		ResponseState:  providerState(ev.ResponseState),
		Err:            ev.Err,
	}
}

func providerToolCallStream(call *ModelToolCallStream) provider.ToolCallStream {
	if call == nil {
		return provider.ToolCallStream{}
	}
	return provider.ToolCallStream{
		ID:   call.ID,
		Name: call.Name,
	}
}

func providerToolCalls(calls []tools.ToolCall) []provider.ToolCall {
	out := make([]provider.ToolCall, 0, len(calls))
	for _, call := range calls {
		out = append(out, providerToolCall(call))
	}
	return out
}

func providerSourceRefs(in []SourceRef) []provider.SourceRef {
	out := make([]provider.SourceRef, 0, len(in))
	for _, ref := range in {
		if strings.TrimSpace(ref.Title) == "" && strings.TrimSpace(ref.URL) == "" {
			continue
		}
		out = append(out, provider.SourceRef{
			Title: strings.TrimSpace(ref.Title),
			URL:   strings.TrimSpace(ref.URL),
		})
	}
	return out
}

func providerToolCall(call tools.ToolCall) provider.ToolCall {
	return provider.ToolCall{
		ID:        call.ID,
		Name:      call.Name,
		Args:      call.Args,
		Reasoning: call.Reasoning,
	}
}

func modelState(in *provider.State) *ModelState {
	if in == nil {
		return nil
	}
	return &ModelState{
		Kind:       in.Kind,
		ID:         in.ID,
		Attributes: cloneStringMap(in.Attributes),
	}
}

func providerState(in *ModelState) *provider.State {
	if in == nil {
		return nil
	}
	return &provider.State{
		Kind:       in.Kind,
		ID:         in.ID,
		Attributes: cloneStringMap(in.Attributes),
	}
}

func providerUsage(in ProviderUsage) provider.Usage {
	return provider.Usage{
		InputTokens:       in.InputTokens,
		OutputTokens:      in.OutputTokens,
		ReasoningTokens:   in.ReasoningTokens,
		CacheReadTokens:   in.CacheReadTokens,
		CacheWriteTokens:  in.CacheWriteTokens,
		TotalTokens:       in.TotalTokens,
		CostUSD:           in.CostUSD,
		Source:            provider.UsageSource(in.Source),
		Available:         in.Available,
		WindowInputTokens: in.WindowInputTokens,
	}
}

func projectedTurnResult(ids projectedIDs, in engine.Result, events []observation.Event, nowUnixMS int64) ProjectedTurnResult {
	out := ProjectedTurnResult{
		RunID:              RunID(ids.runID),
		ThreadID:           ThreadID(ids.threadID),
		TurnID:             TurnID(ids.turnID),
		Status:             TurnStatus(in.Status),
		Output:             in.Output,
		Metrics:            runtimeMetrics(in.Metrics),
		Transcript:         runtimeMessages(in.Messages),
		CompletionReason:   string(in.CompletionReason),
		ContinuationReason: string(in.ContinuationReason),
		FinishReason:       string(in.FinishReason),
		RawFinishReason:    in.RawFinishReason,
		FinishInferred:     in.FinishInferred,
		ProviderState:      modelState(in.ProviderState),
		Signal:             runtimeTurnSignal(in.ControlSignal),
		ActivityTimeline: observation.BuildActivityTimeline(observation.ActivityRunMeta{
			RunID:    ids.runID,
			ThreadID: ids.threadID,
			TurnID:   ids.turnID,
			TraceID:  ids.traceID,
		}, events, nowUnixMS),
	}
	if in.Err != nil {
		out.Error = in.Err.Error()
	}
	return out
}

func projectedContextCompactionResult(ids projectedIDs, in engine.ContextCompactionResult, events []observation.Event, nowUnixMS int64) ProjectedContextCompactionResult {
	out := ProjectedContextCompactionResult{
		RunID:            RunID(ids.runID),
		ThreadID:         ThreadID(ids.threadID),
		TurnID:           TurnID(ids.turnID),
		Status:           string(in.Status),
		Metrics:          runtimeMetrics(in.Metrics),
		ActiveTranscript: runtimeMessages(in.Messages),
		ProviderState:    modelState(in.ProviderState),
		ActivityTimeline: observation.BuildActivityTimeline(observation.ActivityRunMeta{
			RunID:    ids.runID,
			ThreadID: ids.threadID,
			TurnID:   ids.turnID,
			TraceID:  ids.traceID,
		}, events, nowUnixMS),
	}
	if in.Err != nil {
		out.Error = in.Err.Error()
	}
	out.Compaction = projectedContextCompactionFromResult(in.Compaction)
	if out.Compaction != nil {
		if complete := projectedContextCompactionFromEvents(events); complete != nil {
			out.Compaction.OperationID = complete.OperationID
			out.Compaction.RequestID = complete.RequestID
			out.Compaction.Source = complete.Source
		}
	}
	return out
}

func projectedContextCompactionFromResult(result compaction.Result) *ProjectedContextCompaction {
	if strings.TrimSpace(result.CompactionID) == "" {
		return nil
	}
	return &ProjectedContextCompaction{
		OperationID:             strings.TrimSpace(result.Details["operation_id"]),
		RequestID:               strings.TrimSpace(result.Details["manual_request_id"]),
		Source:                  strings.TrimSpace(result.Details["manual_source"]),
		CompactionID:            strings.TrimSpace(result.CompactionID),
		PreviousCompactionID:    strings.TrimSpace(result.PreviousCompactionID),
		CompactionGeneration:    result.CompactionGeneration,
		CompactionWindowID:      strings.TrimSpace(result.CompactionWindowID),
		FirstKeptEntryID:        strings.TrimSpace(result.FirstKeptEntryID),
		KeptUserEntryIDs:        append([]string(nil), result.KeptUserEntryIDs...),
		CompactedThroughEntryID: strings.TrimSpace(result.CompactedThroughEntryID),
		Summary:                 strings.TrimSpace(result.Summary),
		SummarySchemaVersion:    strings.TrimSpace(result.SummarySchemaVersion),
		Trigger:                 string(result.Trigger),
		Reason:                  string(result.Reason),
		Phase:                   string(result.Phase),
		TokensBefore:            result.TokensBefore,
		TokensAfterEstimate:     result.TokensAfterEstimate,
		UsageBefore:             configbridge.PublicContextUsage(result.UsageBefore),
		UsageAfter:              configbridge.PublicContextUsage(result.UsageAfter),
		Details:                 cloneStringMap(result.Details),
		CreatedAt:               result.CreatedAt,
	}
}

func projectedContextCompactionFromEvents(events []observation.Event) *ProjectedContextCompaction {
	var out *ProjectedContextCompaction
	for _, ev := range events {
		if ev.Type != observation.EventTypeContextCompact {
			continue
		}
		compact, ok := observation.CompactionEventFromEvent(ev)
		if !ok || compact.Phase != observation.CompactionPhaseComplete {
			continue
		}
		meta := ev.Metadata
		out = &ProjectedContextCompaction{
			OperationID:             compact.OperationID,
			RequestID:               compact.RequestID,
			Source:                  compact.Source,
			CompactionID:            compact.CompactionID,
			PreviousCompactionID:    stringFromMetadata(meta, "previous_compaction_id"),
			CompactionGeneration:    compact.CompactionGeneration,
			CompactionWindowID:      compact.CompactionWindowID,
			FirstKeptEntryID:        stringFromMetadata(meta, "first_kept_entry_id"),
			CompactedThroughEntryID: compact.CompactedThroughEntryID,
			Summary:                 strings.TrimSpace(ev.Result),
			SummarySchemaVersion:    stringFromMetadata(meta, "summary_schema_version"),
			Trigger:                 compact.Trigger,
			Reason:                  compact.Reason,
			Phase:                   stringFromMetadata(meta, "compaction_phase"),
			TokensBefore:            compact.TokensBefore,
			TokensAfterEstimate:     compact.TokensAfterEstimate,
			UsageBefore:             compact.ContextBefore,
			UsageAfter:              compact.ContextAfter,
			Details:                 stringDetailsFromMetadata(meta),
			CreatedAt:               compact.ObservedAt,
		}
	}
	return out
}

func stringDetailsFromMetadata(meta map[string]any) map[string]string {
	if len(meta) == 0 {
		return nil
	}
	out := map[string]string{}
	for key, value := range meta {
		text, ok := value.(string)
		if !ok {
			continue
		}
		text = strings.TrimSpace(text)
		if text != "" {
			out[key] = text
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func runtimeMetrics(in engine.RunMetrics) RunMetrics {
	return RunMetrics{
		ProviderUsage: runtimeProviderUsage(in.Usage),
		Steps:         in.Steps,
		LLMRequests:   in.LLMRequests,
		ToolCalls:     in.ToolCalls,
		Compactions:   in.Compactions,
		Retries:       in.Retries,
		WallTimeMS:    in.WallTimeMS,
	}
}

func runtimeProviderUsage(in provider.Usage) ProviderUsage {
	in = in.Normalized()
	return ProviderUsage{
		InputTokens:       in.InputTokens,
		OutputTokens:      in.OutputTokens,
		ReasoningTokens:   in.ReasoningTokens,
		CacheReadTokens:   in.CacheReadTokens,
		CacheWriteTokens:  in.CacheWriteTokens,
		TotalTokens:       in.TotalTokens,
		CostUSD:           in.CostUSD,
		Source:            string(in.Source),
		Available:         in.Available,
		WindowInputTokens: in.WindowInputTokens,
	}
}

func runtimeTurnSignal(in *engine.ControlSignal) *TurnSignal {
	if in == nil {
		return nil
	}
	return &TurnSignal{
		Disposition: SignalDisposition(in.Disposition),
		Name:        in.Name,
		CallID:      in.CallID,
		Payload:     cloneAnyMap(in.Payload),
		OutputText:  in.OutputText,
		ArgsHash:    in.ArgsHash,
		Labels:      cloneStringMap(in.Labels),
	}
}

func engineLabels(in RunLabels) engine.RunLabels {
	return engine.RunLabels{
		Correlation: cloneStringMap(in.Correlation),
		Host:        cloneStringMap(in.Host),
	}
}

func providerRequestLabels(in provider.RequestLabels) RunLabels {
	return RunLabels{
		Correlation: cloneStringMap(in.Correlation),
		Host:        cloneStringMap(in.Host),
	}
}

func engineTurnCompletionPolicy(policy TurnCompletionPolicy) (engine.CompletionPolicy, error) {
	switch policy {
	case "":
		return engine.CompletionNaturalStop, nil
	case TurnCompletionNaturalStop:
		return engine.CompletionNaturalStop, nil
	case TurnCompletionExplicitSignal:
		return engine.CompletionExplicitSignal, nil
	default:
		return "", fmt.Errorf("unsupported completion policy %q", policy)
	}
}

func engineTurnSignalSpec(spec TurnSignalSpec, policy engine.CompletionPolicy) (engine.ControlSpec, error) {
	if len(spec.Definitions) == 0 && spec.Project == nil {
		if policy == engine.CompletionExplicitSignal {
			return engine.ControlSpec{}, errors.New("signal spec is required when completion policy is explicit_signal")
		}
		return engine.ControlSpec{
			Definitions: []provider.ToolDefinition{},
			Project: func(provider.ToolCall) (engine.ControlSignal, bool, error) {
				return engine.ControlSignal{}, false, nil
			},
		}, nil
	}
	out := engine.ControlSpec{
		Definitions: make([]provider.ToolDefinition, 0, len(spec.Definitions)),
	}
	for _, def := range spec.Definitions {
		name := strings.TrimSpace(def.Name)
		if name == "" {
			continue
		}
		out.Definitions = append(out.Definitions, provider.ToolDefinition{
			Name:         name,
			Title:        strings.TrimSpace(def.Title),
			Description:  strings.TrimSpace(def.Description),
			InputSchema:  cloneAnyMap(def.InputSchema),
			OutputSchema: cloneAnyMap(def.OutputSchema),
			Strict:       def.Strict,
			Annotations:  cloneAnyMap(def.Annotations),
		})
	}
	if spec.Project != nil {
		out.Project = func(call provider.ToolCall) (engine.ControlSignal, bool, error) {
			signal, ok, err := spec.Project(tools.ToolCall{
				ID:        call.ID,
				Name:      call.Name,
				Args:      call.Args,
				Reasoning: call.Reasoning,
			})
			if err != nil || !ok {
				return engine.ControlSignal{}, ok, err
			}
			return engine.ControlSignal{
				Disposition: engine.ControlDisposition(signal.Disposition),
				Name:        signal.Name,
				CallID:      signal.CallID,
				Payload:     cloneAnyMap(signal.Payload),
				Activity:    cloneActivityPresentation(signal.Activity),
				OutputText:  signal.OutputText,
				ArgsHash:    signal.ArgsHash,
				Labels:      cloneStringMap(signal.Labels),
			}, true, nil
		}
	}
	if len(out.Definitions) == 0 {
		return engine.ControlSpec{}, errors.New("signal spec must include at least one named definition")
	}
	if out.Project == nil {
		return engine.ControlSpec{}, errors.New("signal spec projector is required")
	}
	return out, nil
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

func cloneAny(value any) any {
	switch v := value.(type) {
	case map[string]any:
		return cloneAnyMap(v)
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = cloneAny(item)
		}
		return out
	case []string:
		out := []string{}
		if len(v) > 0 {
			out = append(out, v...)
		}
		return out
	case map[string]string:
		return cloneStringMap(v)
	default:
		return value
	}
}

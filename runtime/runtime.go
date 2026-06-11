package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/floegence/floret/config"
	"github.com/floegence/floret/internal/agentharness"
	"github.com/floegence/floret/internal/configbridge"
	"github.com/floegence/floret/internal/engine"
	"github.com/floegence/floret/internal/event"
	"github.com/floegence/floret/internal/provider"
	"github.com/floegence/floret/internal/provider/adapters"
	"github.com/floegence/floret/internal/provider/cache"
	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/session/artifact"
	"github.com/floegence/floret/internal/session/compaction"
	"github.com/floegence/floret/internal/sessiontree"
	"github.com/floegence/floret/internal/storage"
	"github.com/floegence/floret/internal/storage/sqlite"
	"github.com/floegence/floret/internal/tools/skills"
	"github.com/floegence/floret/tools"
)

type ThreadID string
type TurnID string
type RunID string
type PromptScopeID string
type TraceID string

type Host interface {
	StartThread(context.Context, StartThreadRequest) (ThreadSnapshot, error)
	ReadThread(context.Context, ThreadID) (ThreadSnapshot, error)
	RunTurn(context.Context, RunTurnRequest) (TurnResult, error)
	RetryTurn(context.Context, RetryTurnRequest) (TurnResult, error)
	DeleteThread(context.Context, ThreadID) error
	Close() error
}

type HostOptions struct {
	Config       config.Config
	Store        *Store
	Tools        *tools.Registry
	Approver     tools.Approver
	Sink         EventSink
	IDGenerator  func(string) string
	LoopLimits   LoopLimits
	Capabilities CapabilityOptions
}

type LoopLimits struct {
	MaxEmptyProviderRetries int
	NoProgressLimit         int
	DuplicateToolLimit      int
	WallTime                time.Duration
}

type CapabilityOptions struct {
	SkillsEnabled          bool
	SkillSources           []string
	SkillPromptBudgetBytes int
}

type StartThreadRequest struct {
	ThreadID ThreadID
}

type RunTurnRequest struct {
	ThreadID ThreadID
	TurnID   TurnID
	Input    string
	Labels   RunLabels
}

type RetryTurnRequest struct {
	ThreadID ThreadID
	Reason   string
	Labels   RunLabels
}

type RunLabels struct {
	Correlation map[string]string
	Host        map[string]string
}

type ThreadSnapshot struct {
	ID               ThreadID        `json:"id"`
	Title            string          `json:"title,omitempty"`
	TitleStatus      string          `json:"title_status,omitempty"`
	TitleSource      string          `json:"title_source,omitempty"`
	TitleUpdatedAt   time.Time       `json:"title_updated_at,omitempty"`
	TitleError       string          `json:"title_error,omitempty"`
	CreatedAt        time.Time       `json:"created_at"`
	UpdatedAt        time.Time       `json:"updated_at"`
	Phase            ThreadPhase     `json:"phase"`
	Status           ThreadStatus    `json:"status"`
	LatestTurnID     TurnID          `json:"latest_turn_id,omitempty"`
	WaitingPrompt    string          `json:"waiting_prompt,omitempty"`
	Recoverable      bool            `json:"recoverable"`
	CanAppendMessage bool            `json:"can_append_message"`
	CanRetry         bool            `json:"can_retry"`
	Messages         []ThreadMessage `json:"messages"`
}

type ThreadMessage struct {
	Role      string    `json:"role"`
	Content   string    `json:"content"`
	TurnID    TurnID    `json:"turn_id,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

type TurnResult struct {
	ID                 TurnID            `json:"id"`
	Status             TurnStatus        `json:"status"`
	Output             string            `json:"output,omitempty"`
	Error              string            `json:"error,omitempty"`
	Diagnostics        map[string]string `json:"diagnostics,omitempty"`
	CompletionReason   string            `json:"completion_reason,omitempty"`
	ContinuationReason string            `json:"continuation_reason,omitempty"`
	FinishReason       string            `json:"finish_reason,omitempty"`
	RawFinishReason    string            `json:"raw_finish_reason,omitempty"`
	FinishInferred     bool              `json:"finish_inferred,omitempty"`
}

type EventSink interface {
	EmitEvent(Event)
}

type Event struct {
	Type         string         `json:"type"`
	TraceID      TraceID        `json:"trace_id,omitempty"`
	RunID        RunID          `json:"run_id,omitempty"`
	ThreadID     ThreadID       `json:"thread_id,omitempty"`
	TurnID       TurnID         `json:"turn_id,omitempty"`
	Step         int            `json:"step,omitempty"`
	Provider     string         `json:"provider,omitempty"`
	Model        string         `json:"model,omitempty"`
	Message      string         `json:"message,omitempty"`
	Result       string         `json:"result,omitempty"`
	Error        string         `json:"error,omitempty"`
	ToolID       string         `json:"tool_id,omitempty"`
	ToolName     string         `json:"tool_name,omitempty"`
	ToolKind     string         `json:"tool_kind,omitempty"`
	ArgsHash     string         `json:"args_hash,omitempty"`
	FinishReason string         `json:"finish_reason,omitempty"`
	Metadata     map[string]any `json:"metadata,omitempty"`
	Timestamp    time.Time      `json:"timestamp,omitempty"`
}

type ThreadStatus string

const (
	ThreadStatusIdle        ThreadStatus = "idle"
	ThreadStatusRunning     ThreadStatus = "running"
	ThreadStatusCompleted   ThreadStatus = "completed"
	ThreadStatusWaiting     ThreadStatus = "waiting"
	ThreadStatusFailed      ThreadStatus = "failed"
	ThreadStatusCancelled   ThreadStatus = "cancelled"
	ThreadStatusInterrupted ThreadStatus = "interrupted"
)

type ThreadPhase string

const (
	ThreadPhaseIdle ThreadPhase = "idle"
	ThreadPhaseTurn ThreadPhase = "turn"
)

type TurnStatus string

const (
	TurnStatusCompleted TurnStatus = "completed"
	TurnStatusWaiting   TurnStatus = "waiting"
	TurnStatusFailed    TurnStatus = "failed"
	TurnStatusCancelled TurnStatus = "cancelled"
)

type Store struct {
	repo       sessiontree.Repo
	prompt     cache.Store
	artifacts  artifact.Store
	deleteData func(context.Context, string) error
	close      func() error
}

func NewMemoryStore() *Store {
	repo := sessiontree.NewMemoryRepo()
	prompt := cache.NewMemoryStore()
	artifacts := artifact.NewMemoryStore()
	return &Store{
		repo:      repo,
		prompt:    prompt,
		artifacts: artifacts,
		deleteData: func(ctx context.Context, threadID string) error {
			if err := repo.DeleteThread(ctx, threadID); err != nil {
				return err
			}
			if err := prompt.DeletePromptScopes(ctx, threadID); err != nil {
				return err
			}
			return artifacts.DeleteThreadArtifacts(ctx, threadID)
		},
	}
}

func OpenSQLiteStore(path string) (*Store, error) {
	sqliteStore, err := sqlite.Open(path)
	if err != nil {
		return nil, err
	}
	return &Store{
		repo:      sqliteStore,
		prompt:    sqliteStore,
		artifacts: sqliteStore,
		deleteData: func(ctx context.Context, threadID string) error {
			return sqliteStore.DeleteThreadData(ctx, storage.DeleteThreadDataRequest{ThreadID: threadID, PromptScopeIDs: []string{threadID}})
		},
		close: sqliteStore.Close,
	}, nil
}

func (s *Store) Close() error {
	if s == nil || s.close == nil {
		return nil
	}
	return s.close()
}

func (s *Store) validate() error {
	if s == nil {
		return errors.New("runtime store is required")
	}
	if s.repo == nil || s.prompt == nil || s.artifacts == nil || s.deleteData == nil {
		return errors.New("runtime store must be created with runtime.NewMemoryStore or runtime.OpenSQLiteStore")
	}
	return nil
}

func (s *Store) deleteThreadData(ctx context.Context, threadID string) error {
	if err := s.validate(); err != nil {
		return err
	}
	return s.deleteData(ctx, threadID)
}

type host struct {
	cfg     config.Config
	store   *Store
	harness *agentharness.AgentHarness
}

func NewHost(opts HostOptions) (Host, error) {
	cfg, err := config.Resolve(opts.Config, nil)
	if err != nil {
		return nil, err
	}
	provider, err := adapters.NewProvider(cfg)
	if err != nil {
		return nil, err
	}
	store := opts.Store
	if store == nil {
		store = NewMemoryStore()
	}
	if err := store.validate(); err != nil {
		return nil, err
	}
	harness, err := newHarnessWithProvider(cfg, provider, harnessOptions{
		Store:        store,
		Tools:        opts.Tools,
		Approver:     opts.Approver,
		Sink:         runtimeEventSink{sink: opts.Sink},
		NewID:        opts.IDGenerator,
		LoopLimits:   opts.LoopLimits,
		Capabilities: opts.Capabilities,
	})
	if err != nil {
		return nil, err
	}
	return &host{cfg: cfg, store: store, harness: harness}, nil
}

func (h *host) StartThread(ctx context.Context, req StartThreadRequest) (ThreadSnapshot, error) {
	thread, err := h.harness.StartThread(ctx, agentharness.StartThreadOptions{ThreadID: string(req.ThreadID)})
	if err != nil {
		return ThreadSnapshot{}, err
	}
	return readThread(ctx, thread)
}

func (h *host) ReadThread(ctx context.Context, threadID ThreadID) (ThreadSnapshot, error) {
	thread, err := h.harness.ResumeThread(ctx, string(threadID), agentharness.ResumeOptions{})
	if err != nil {
		return ThreadSnapshot{}, err
	}
	return readThread(ctx, thread)
}

func (h *host) RunTurn(ctx context.Context, req RunTurnRequest) (TurnResult, error) {
	if strings.TrimSpace(string(req.ThreadID)) == "" {
		return TurnResult{}, errors.New("thread id is required")
	}
	thread, err := h.harness.ResumeThread(ctx, string(req.ThreadID), agentharness.ResumeOptions{})
	if err != nil {
		return TurnResult{}, err
	}
	result, runErr := thread.Run(ctx, req.Input, agentharness.RunOptions{
		TurnID: string(req.TurnID),
		Labels: engine.RunLabels{
			Correlation: cloneStringMap(req.Labels.Correlation),
			Host:        cloneStringMap(req.Labels.Host),
		},
	})
	return turnResult(result), runErr
}

func (h *host) RetryTurn(ctx context.Context, req RetryTurnRequest) (TurnResult, error) {
	if strings.TrimSpace(string(req.ThreadID)) == "" {
		return TurnResult{}, errors.New("thread id is required")
	}
	thread, err := h.harness.ResumeThread(ctx, string(req.ThreadID), agentharness.ResumeOptions{})
	if err != nil {
		return TurnResult{}, err
	}
	result, runErr := thread.Retry(ctx, agentharness.RetryOptions{
		Reason: req.Reason,
		Labels: engine.RunLabels{
			Correlation: cloneStringMap(req.Labels.Correlation),
			Host:        cloneStringMap(req.Labels.Host),
		},
	})
	return turnResult(result), runErr
}

func (h *host) DeleteThread(ctx context.Context, threadID ThreadID) error {
	id := strings.TrimSpace(string(threadID))
	if id == "" {
		return errors.New("thread id is required")
	}
	return h.store.deleteThreadData(ctx, id)
}

func (h *host) Close() error {
	return h.store.Close()
}

func readThread(ctx context.Context, thread *agentharness.Thread) (ThreadSnapshot, error) {
	snapshot, err := thread.Read(ctx)
	if err != nil {
		return ThreadSnapshot{}, err
	}
	return threadSnapshot(snapshot), nil
}

func threadSnapshot(in agentharness.ThreadSnapshot) ThreadSnapshot {
	out := ThreadSnapshot{
		ID:               ThreadID(in.ID),
		Title:            in.Title,
		TitleStatus:      in.TitleStatus,
		TitleSource:      in.TitleSource,
		TitleUpdatedAt:   in.TitleUpdatedAt,
		TitleError:       in.TitleError,
		CreatedAt:        in.CreatedAt,
		UpdatedAt:        in.UpdatedAt,
		Phase:            ThreadPhase(in.Phase),
		Status:           ThreadStatus(in.Status),
		LatestTurnID:     TurnID(in.LatestTurnID),
		WaitingPrompt:    in.WaitingPrompt,
		Recoverable:      in.Recoverable,
		CanAppendMessage: in.CanAppendMessage,
		CanRetry:         in.CanRetry,
		Messages:         make([]ThreadMessage, 0, len(in.Messages)),
	}
	for _, msg := range in.Messages {
		out.Messages = append(out.Messages, ThreadMessage{
			Role:      string(msg.Role),
			Content:   msg.Content,
			TurnID:    TurnID(msg.TurnID),
			CreatedAt: msg.CreatedAt,
		})
	}
	return out
}

func turnResult(in agentharness.TurnResult) TurnResult {
	out := TurnResult{
		ID:                 TurnID(in.ID),
		Status:             TurnStatus(in.Status),
		Output:             in.Output,
		Diagnostics:        cloneStringMap(in.Diagnostics),
		CompletionReason:   string(in.CompletionReason),
		ContinuationReason: string(in.ContinuationReason),
		FinishReason:       string(in.FinishReason),
		RawFinishReason:    in.RawFinishReason,
		FinishInferred:     in.FinishInferred,
	}
	if in.Err != nil {
		out.Error = in.Err.Error()
	}
	return out
}

type harnessOptions struct {
	Store        *Store
	Tools        *tools.Registry
	Approver     tools.Approver
	Sink         event.Sink
	Title        agentharness.TitleGenerator
	NewID        func(string) string
	LoopLimits   LoopLimits
	Capabilities CapabilityOptions
}

func newHarnessWithProvider(cfg config.Config, p provider.Provider, opts harnessOptions) (*agentharness.AgentHarness, error) {
	cfg = config.ResolvePrompt(cfg)
	store := opts.Store
	if store == nil {
		store = NewMemoryStore()
	}
	registry := opts.Tools
	if registry == nil {
		registry = tools.NewRegistry()
	}
	capabilities := mergeCapabilityOptions(cfg, opts.Capabilities)
	effectivePrompt, err := applyCapabilities(registry, cfg.SystemPrompt, capabilities, opts.Sink)
	if err != nil {
		return nil, err
	}
	cacheRetention, err := config.PromptCacheRetention(cfg)
	if err != nil {
		return nil, err
	}
	turnPolicy := agentharness.TurnPolicy{
		ContextPolicy:  configbridge.ContextPolicy(cfg.ContextPolicy),
		CacheRetention: configbridge.CacheRetention(cacheRetention),
	}
	loopLimits := agentharness.LoopLimits{
		MaxEmptyProviderRetries: cfg.MaxEmptyProviderRetries,
		NoProgressLimit:         cfg.NoProgressLimit,
		DuplicateToolLimit:      cfg.DuplicateToolLimit,
		WallTime:                cfg.WallTime,
	}
	if opts.LoopLimits.MaxEmptyProviderRetries > 0 {
		loopLimits.MaxEmptyProviderRetries = opts.LoopLimits.MaxEmptyProviderRetries
	}
	if opts.LoopLimits.NoProgressLimit > 0 {
		loopLimits.NoProgressLimit = opts.LoopLimits.NoProgressLimit
	}
	if opts.LoopLimits.DuplicateToolLimit > 0 {
		loopLimits.DuplicateToolLimit = opts.LoopLimits.DuplicateToolLimit
	}
	if opts.LoopLimits.WallTime > 0 {
		loopLimits.WallTime = opts.LoopLimits.WallTime
	}
	return agentharness.New(agentharness.Options{
		Provider:         p,
		ProviderName:     cfg.Provider,
		Model:            cfg.Model,
		SystemPrompt:     effectivePrompt,
		Tools:            registry,
		PromptStore:      store.prompt,
		Repo:             store.repo,
		Sink:             opts.Sink,
		Approver:         opts.Approver,
		TitleGenerator:   opts.Title,
		CompactionPrompt: compaction.PromptOptions{},
		Artifacts:        store.artifacts,
		TurnPolicy:       turnPolicy,
		LoopLimits:       loopLimits,
		NewID:            opts.NewID,
	}), nil
}

func mergeCapabilityOptions(cfg config.Config, explicit CapabilityOptions) CapabilityOptions {
	out := explicit
	if !out.SkillsEnabled {
		out.SkillsEnabled = cfg.SkillsEnabled
	}
	if out.SkillPromptBudgetBytes <= 0 {
		out.SkillPromptBudgetBytes = cfg.SkillPromptBudgetBytes
	}
	if len(out.SkillSources) == 0 {
		out.SkillSources = append([]string(nil), cfg.SkillSources...)
	}
	return out
}

func applyCapabilities(registry *tools.Registry, basePrompt string, capability CapabilityOptions, sink event.Sink) (string, error) {
	if !capability.SkillsEnabled {
		return basePrompt, nil
	}
	sources := make([]skills.Source, 0, len(capability.SkillSources))
	for _, root := range capability.SkillSources {
		sources = append(sources, skills.Source{Root: root, Kind: skills.SourceConfig, Enabled: true})
	}
	catalog, err := skills.Discover(sources)
	if err != nil {
		return "", err
	}
	emitSkillDiagnostics(sink, catalog.Diagnostics)
	for _, skill := range catalog.Skills {
		emitSkillEvent(sink, event.SkillDetected, map[string]any{
			"skill_id":     skill.Name,
			"source_kind":  string(skill.SourceInfo.Kind),
			"source_label": skill.SourceInfo.DisplayLabel,
			"content_hash": skill.ContentHash,
		})
	}
	prompt, promptDiagnostics := skills.BuildPrompt(catalog.Skills, skills.PromptOptions{MaxBytes: capability.SkillPromptBudgetBytes})
	emitSkillDiagnostics(sink, promptDiagnostics)
	if prompt != "" {
		emitSkillEvent(sink, event.SkillDisclosureApplied, map[string]any{
			"skill_count":   len(catalog.Skills),
			"prompt_bytes":  len(prompt),
			"prompt_sha256": event.StableHash(prompt),
		})
		basePrompt = appendPromptMaterial(basePrompt, prompt)
	}
	if len(catalog.Skills) == 0 {
		return basePrompt, nil
	}
	tool, err := skills.DefineSkillTool(catalog.Skills, skills.ToolOptions{
		OutputPolicy: tools.OutputPolicy{VisibleMaxBytes: 64 * 1024, Strategy: tools.OutputHead, PreserveFull: true},
		OnLoad: func(load skills.SkillLoad) {
			emitSkillEvent(sink, event.SkillLoaded, map[string]any{
				"skill_id":     load.Name,
				"source_kind":  string(load.SourceKind),
				"content_hash": load.ContentHash,
				"bytes":        load.Bytes,
			})
		},
	})
	if err != nil {
		return "", err
	}
	if err := registry.Register(tool); err != nil {
		return "", err
	}
	return basePrompt, nil
}

func appendPromptMaterial(base, addition string) string {
	base = strings.TrimRight(base, "\n")
	addition = strings.TrimSpace(addition)
	if addition == "" {
		return base
	}
	if base == "" {
		return addition
	}
	return base + "\n\n" + addition
}

func emitSkillDiagnostics(sink event.Sink, diagnostics []skills.Diagnostic) {
	for _, diagnostic := range diagnostics {
		emitSkillEvent(sink, event.SkillBlocked, map[string]any{
			"failure_category": diagnostic.Kind,
			"skill_id":         diagnostic.SkillName,
			"source_kind":      string(diagnostic.SourceKind),
			"path":             diagnostic.Path,
			"message":          diagnostic.Message,
			"next_action":      "Fix or remove the downstream skill source entry.",
		})
	}
}

func emitSkillEvent(sink event.Sink, typ event.Type, metadata map[string]any) {
	if sink == nil {
		return
	}
	sink.Emit(event.Event{Type: typ, Metadata: metadata})
}

type runtimeEventSink struct {
	sink EventSink
}

func (s runtimeEventSink) Emit(ev event.Event) {
	if s.sink == nil {
		return
	}
	s.sink.EmitEvent(runtimeEvent(event.Sanitize(ev)))
}

func runtimeEvent(ev event.Event) Event {
	return Event{
		Type:         string(ev.Type),
		TraceID:      TraceID(ev.TraceID),
		RunID:        RunID(ev.RunID),
		ThreadID:     ThreadID(ev.ThreadID),
		TurnID:       TurnID(ev.TurnID),
		Step:         ev.Step,
		Provider:     ev.Provider,
		Model:        ev.Model,
		Message:      ev.Message,
		Result:       ev.Result,
		Error:        ev.Err,
		ToolID:       ev.ToolID,
		ToolName:     ev.ToolName,
		ToolKind:     ev.ToolKind,
		ArgsHash:     ev.ArgsHash,
		FinishReason: ev.FinishReason,
		Metadata:     safeMetadata(ev.Metadata),
		Timestamp:    ev.Timestamp,
	}
}

func safeMetadata(in any) map[string]any {
	values, ok := in.(map[string]any)
	if !ok || len(values) == 0 {
		return nil
	}
	out := make(map[string]any, len(values))
	for key, value := range values {
		out[key] = safeMetadataValue(value)
	}
	return out
}

func safeMetadataValue(value any) any {
	switch v := value.(type) {
	case nil, string, bool, int, int64, float64:
		return v
	case fmt.Stringer:
		return v.String()
	default:
		return fmt.Sprintf("%v", v)
	}
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

type engineHelperOptions struct {
	RunID         string
	PromptScopeID string
	PromptStore   cache.Store
}

func newEngineWithProvider(cfg config.Config, p provider.Provider, store session.TranscriptStore, registry *tools.Registry, opts engineHelperOptions) (*engine.Engine, error) {
	cfg = config.ResolvePrompt(cfg)
	if store == nil {
		store = session.NewMemoryStore()
	}
	if registry == nil {
		registry = tools.NewRegistry()
	}
	promptStore := opts.PromptStore
	if promptStore == nil {
		promptStore = cache.NewMemoryStore()
	}
	cacheRetention, err := config.PromptCacheRetention(cfg)
	if err != nil {
		return nil, err
	}
	return engine.New(engine.Config{
		Provider:     p,
		Store:        store,
		Prompt:       promptStore,
		Artifacts:    artifact.NewMemoryStore(),
		SystemPrompt: cfg.SystemPrompt,
		Tools:        registry,
		Options: engine.Options{
			RunID:                   opts.RunID,
			TraceID:                 opts.RunID,
			PromptScopeID:           opts.PromptScopeID,
			ProviderName:            cfg.Provider,
			Model:                   cfg.Model,
			CacheRetention:          configbridge.CacheRetention(cacheRetention),
			ContextPolicy:           configbridge.ContextPolicy(cfg.ContextPolicy),
			MaxEmptyProviderRetries: cfg.MaxEmptyProviderRetries,
			NoProgressLimit:         cfg.NoProgressLimit,
			DuplicateToolLimit:      cfg.DuplicateToolLimit,
			WallTime:                cfg.WallTime,
		},
	})
}

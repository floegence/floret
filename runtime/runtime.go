package runtime

import (
	"context"
	"strings"

	"github.com/floegence/floret/adapters"
	"github.com/floegence/floret/agentharness"
	"github.com/floegence/floret/config"
	"github.com/floegence/floret/contextpolicy"
	"github.com/floegence/floret/engine"
	"github.com/floegence/floret/event"
	"github.com/floegence/floret/mcpclient"
	"github.com/floegence/floret/memory"
	"github.com/floegence/floret/promptcache"
	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/session"
	"github.com/floegence/floret/sessiontree"
	"github.com/floegence/floret/skills"
	"github.com/floegence/floret/tools"
)

type HarnessOptions struct {
	Store       sessiontree.Repo
	Tools       *tools.Registry
	PromptStore promptcache.Store
	Sink        event.Sink
	Approver    tools.Approver
	StopHook    engine.StopHook
	TurnPolicy  agentharness.TurnPolicy
	LoopLimits  agentharness.LoopLimits
	NewID       func(string) string
	Capability  CapabilityOptions
}

type CapabilityOptions struct {
	MCPServers             []mcpclient.ServerConfig
	SkillSources           []skills.Source
	SkillsEnabled          bool
	SkillPromptBudgetBytes int
	MCPManager             *mcpclient.Manager
}

func NewEngine(cfg config.Config, store session.Store, registry *tools.Registry) (*engine.Engine, error) {
	resolved, err := config.Resolve(cfg, nil)
	if err != nil {
		return nil, err
	}
	p, err := adapters.NewProvider(cfg)
	if err != nil {
		return nil, err
	}
	return NewEngineWithProvider(resolved, p, store, registry)
}

func NewHarness(cfg config.Config, opts HarnessOptions) (*agentharness.AgentHarness, error) {
	resolved, err := config.Resolve(cfg, nil)
	if err != nil {
		return nil, err
	}
	p, err := adapters.NewProvider(resolved)
	if err != nil {
		return nil, err
	}
	return NewHarnessWithProviderE(resolved, p, opts)
}

func NewHarnessWithProvider(cfg config.Config, p provider.Provider, opts HarnessOptions) *agentharness.AgentHarness {
	h, err := NewHarnessWithProviderE(cfg, p, opts)
	if err != nil {
		panic(err)
	}
	return h
}

func NewHarnessWithProviderE(cfg config.Config, p provider.Provider, opts HarnessOptions) (*agentharness.AgentHarness, error) {
	repo := opts.Store
	if repo == nil {
		repo = sessiontree.NewMemoryRepo()
	}
	registry := opts.Tools
	if registry == nil {
		registry = tools.NewRegistry()
	}
	capability := mergeCapabilityOptions(cfg, opts.Capability)
	effectivePrompt, err := applyCapabilities(registry, cfg.SystemPrompt, capability, opts.Sink)
	if err != nil {
		return nil, err
	}
	promptStore := opts.PromptStore
	if promptStore == nil {
		promptStore = promptcache.NewMemoryStore()
		if cfg.PromptCacheDir != "" {
			promptStore = promptcache.NewFileStore(cfg.PromptCacheDir)
		}
	}
	turnPolicy := opts.TurnPolicy
	turnPolicy.ContextPolicy = mergeContextPolicy(turnPolicy.ContextPolicy, cfg.ContextPolicy)
	if turnPolicy.CacheRetention == "" {
		turnPolicy.CacheRetention = config.PromptCacheRetention(cfg)
	}
	loopLimits := opts.LoopLimits
	if loopLimits.MaxEmptyProviderRetries <= 0 {
		loopLimits.MaxEmptyProviderRetries = cfg.MaxEmptyProviderRetries
	}
	if loopLimits.NoProgressLimit <= 0 {
		loopLimits.NoProgressLimit = cfg.NoProgressLimit
	}
	if loopLimits.DuplicateToolLimit <= 0 {
		loopLimits.DuplicateToolLimit = cfg.DuplicateToolLimit
	}
	if loopLimits.WallTime <= 0 {
		loopLimits.WallTime = cfg.WallTime
	}
	return agentharness.New(agentharness.Options{
		Provider:     p,
		ProviderName: cfg.Provider,
		Model:        cfg.Model,
		SystemPrompt: effectivePrompt,
		Tools:        registry,
		PromptStore:  promptStore,
		Repo:         repo,
		Sink:         opts.Sink,
		Approver:     opts.Approver,
		StopHook:     opts.StopHook,
		TurnPolicy:   turnPolicy,
		LoopLimits:   loopLimits,
		NewID:        opts.NewID,
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
	if len(out.SkillSources) == 0 && len(cfg.SkillSources) > 0 {
		for _, root := range cfg.SkillSources {
			out.SkillSources = append(out.SkillSources, skills.Source{Root: root, Kind: skills.SourceConfig, Enabled: true})
		}
	}
	return out
}

func applyCapabilities(registry *tools.Registry, basePrompt string, capability CapabilityOptions, sink event.Sink) (string, error) {
	if capability.MCPManager == nil && len(capability.MCPServers) > 0 {
		capability.MCPManager = mcpclient.NewManager(mcpclient.Options{Sink: mcpEventSink{sink: sink}})
		if err := capability.MCPManager.Start(context.Background(), capability.MCPServers); err != nil {
			return "", err
		}
	}
	if capability.MCPManager != nil {
		if err := capability.MCPManager.RegisterTools(registry); err != nil {
			return "", err
		}
	}
	if !capability.SkillsEnabled {
		return basePrompt, nil
	}
	catalog, err := skills.Discover(capability.SkillSources)
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
	if len(catalog.Skills) > 0 {
		tool, err := skills.DefineSkillTool(catalog.Skills, skills.ToolOptions{
			ResultLimit: tools.ResultLimit{MaxBytes: 64 * 1024, Strategy: "head"},
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
	sink.Emit(event.Event{Type: event.Type(typ), Metadata: metadata})
}

type mcpEventSink struct {
	sink event.Sink
}

func (s mcpEventSink) EmitMCP(diag mcpclient.Diagnostic) {
	if s.sink == nil {
		return
	}
	s.sink.Emit(event.Event{
		Type: event.Type(diag.Type),
		Metadata: map[string]any{
			"server_id":        diag.ServerName,
			"transport":        string(diag.Transport),
			"status":           string(diag.Status),
			"tool_name":        diag.ToolName,
			"tool_count":       diag.ToolCount,
			"protocol_version": diag.ProtocolVersion,
			"failure_category": diag.FailureCategory,
			"next_action":      diag.NextAction,
			"message":          diag.Message,
		},
	})
}

func NewEngineWithProvider(cfg config.Config, p provider.Provider, store session.Store, registry *tools.Registry) (*engine.Engine, error) {
	if store == nil {
		store = session.NewMemoryStore()
	}
	if registry == nil {
		registry = tools.NewRegistry()
	}
	promptStore := promptcache.Store(promptcache.NewMemoryStore())
	if cfg.PromptCacheDir != "" {
		promptStore = promptcache.NewFileStore(cfg.PromptCacheDir)
	}
	return engine.New(engine.Config{
		Provider: p,
		Store:    store,
		Prompt:   promptStore,
		Memory: &memory.Manager{
			SystemPrompt: cfg.SystemPrompt,
		},
		Tools: registry,
		Options: engine.Options{
			RunID:                   cfg.RunID,
			SessionID:               cfg.RunID,
			TraceID:                 cfg.RunID,
			ProviderName:            cfg.Provider,
			Model:                   cfg.Model,
			CacheRetention:          config.PromptCacheRetention(cfg),
			ContextPolicy:           cfg.ContextPolicy,
			MaxEmptyProviderRetries: cfg.MaxEmptyProviderRetries,
			NoProgressLimit:         cfg.NoProgressLimit,
			DuplicateToolLimit:      cfg.DuplicateToolLimit,
			WallTime:                cfg.WallTime,
		},
	})
}

func mergeContextPolicy(primary, fallback contextpolicy.Policy) contextpolicy.Policy {
	if primary.ContextWindowTokens <= 0 {
		primary.ContextWindowTokens = fallback.ContextWindowTokens
	}
	if primary.MaxOutputTokens <= 0 {
		primary.MaxOutputTokens = fallback.MaxOutputTokens
	}
	if primary.ReservedOutputTokens <= 0 {
		primary.ReservedOutputTokens = fallback.ReservedOutputTokens
	}
	if primary.ReservedSummaryTokens <= 0 {
		primary.ReservedSummaryTokens = fallback.ReservedSummaryTokens
	}
	if primary.RecentTailTokens <= 0 {
		primary.RecentTailTokens = fallback.RecentTailTokens
	}
	if primary.EstimatorSource == "" {
		primary.EstimatorSource = fallback.EstimatorSource
	}
	if primary.MaxCompactionFailures <= 0 {
		primary.MaxCompactionFailures = fallback.MaxCompactionFailures
	}
	if primary.MicrocompactToolTokens <= 0 {
		primary.MicrocompactToolTokens = fallback.MicrocompactToolTokens
	}
	return primary
}

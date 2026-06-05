package runtime

import (
	"github.com/floegence/floret/adapters"
	"github.com/floegence/floret/agentharness"
	"github.com/floegence/floret/config"
	"github.com/floegence/floret/contextpolicy"
	"github.com/floegence/floret/engine"
	"github.com/floegence/floret/event"
	"github.com/floegence/floret/memory"
	"github.com/floegence/floret/promptcache"
	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/session"
	"github.com/floegence/floret/sessiontree"
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
	return NewHarnessWithProvider(resolved, p, opts), nil
}

func NewHarnessWithProvider(cfg config.Config, p provider.Provider, opts HarnessOptions) *agentharness.AgentHarness {
	repo := opts.Store
	if repo == nil {
		repo = sessiontree.NewMemoryRepo()
	}
	registry := opts.Tools
	if registry == nil {
		registry = tools.NewRegistry()
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
		SystemPrompt: cfg.SystemPrompt,
		Tools:        registry,
		PromptStore:  promptStore,
		Repo:         repo,
		Sink:         opts.Sink,
		Approver:     opts.Approver,
		StopHook:     opts.StopHook,
		TurnPolicy:   turnPolicy,
		LoopLimits:   loopLimits,
		NewID:        opts.NewID,
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

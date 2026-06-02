package runtime

import (
	"github.com/floegence/floret/adapters"
	"github.com/floegence/floret/config"
	"github.com/floegence/floret/engine"
	"github.com/floegence/floret/memory"
	"github.com/floegence/floret/promptcache"
	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/session"
	"github.com/floegence/floret/tools"
)

func NewEngine(cfg config.Config, store session.Store, registry *tools.Registry) (*engine.Engine, error) {
	resolved, err := config.Resolve(cfg, nil)
	if err != nil {
		return nil, err
	}
	p, err := adapters.NewProvider(cfg)
	if err != nil {
		return nil, err
	}
	return NewEngineWithProvider(resolved, p, store, registry), nil
}

func NewEngineWithProvider(cfg config.Config, p provider.Provider, store session.Store, registry *tools.Registry) *engine.Engine {
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
	return &engine.Engine{
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
	}
}

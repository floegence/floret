package runtime

import (
	"github.com/floegence/floret/config"
	"github.com/floegence/floret/internal/configbridge"
	"github.com/floegence/floret/internal/engine"
	"github.com/floegence/floret/internal/provider"
	"github.com/floegence/floret/internal/provider/cache"
	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/tools"
)

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
		Provider: p, Store: store, Prompt: promptStore,
		SystemPrompt: cfg.SystemPrompt, Tools: registry,
		Options: engine.Options{
			RunID: opts.RunID, TraceID: opts.RunID, PromptScopeID: opts.PromptScopeID,
			ProviderName: cfg.Provider, Model: cfg.Model,
			CacheRetention:          configbridge.CacheRetention(cacheRetention),
			ContextPolicy:           configbridge.ContextPolicy(cfg.ContextPolicy),
			Reasoning:               configbridge.ReasoningSelection(cfg.Reasoning),
			MaxEmptyProviderRetries: cfg.MaxEmptyProviderRetries, NoProgressLimit: cfg.NoProgressLimit,
			DuplicateToolLimit: cfg.DuplicateToolLimit, WallTime: cfg.WallTime,
		},
	})
}

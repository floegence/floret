package runtime

import (
	"github.com/floegence/floret/v2/internal/configbridge"
	"github.com/floegence/floret/v2/internal/engine"
	"github.com/floegence/floret/v2/internal/provider"
	"github.com/floegence/floret/v2/internal/provider/cache"
	"github.com/floegence/floret/v2/internal/session"
	"github.com/floegence/floret/v2/tools"
)

type engineHelperOptions struct {
	RunID         string
	PromptScopeID string
	PromptStore   cache.Store
}

func newEngineWithProvider(cfg runtimeConfig, p provider.Provider, store session.TranscriptStore, registry *tools.Registry, opts engineHelperOptions) (*engine.Engine, error) {
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
	cacheRetention := cfg.PromptCacheRetention
	if cacheRetention == "" {
		cacheRetention = "in_memory"
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

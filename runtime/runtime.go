package runtime

import (
	"github.com/floegence/floret/adapters"
	"github.com/floegence/floret/config"
	"github.com/floegence/floret/engine"
	"github.com/floegence/floret/memory"
	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/session"
	"github.com/floegence/floret/tools"
)

func NewEngine(cfg config.Config, store session.Store, registry *tools.Registry) (*engine.Engine, error) {
	p, err := adapters.NewProvider(cfg)
	if err != nil {
		return nil, err
	}
	return NewEngineWithProvider(cfg, p, store, registry), nil
}

func NewEngineWithProvider(cfg config.Config, p provider.Provider, store session.Store, registry *tools.Registry) *engine.Engine {
	if store == nil {
		store = session.NewMemoryStore()
	}
	if registry == nil {
		registry = tools.NewRegistry()
	}
	return &engine.Engine{
		Provider: p,
		Store:    store,
		Memory: &memory.Manager{
			SystemPrompt: cfg.SystemPrompt,
			MaxMessages:  cfg.MaxContextMessages,
		},
		Tools: registry,
		Options: engine.Options{
			RunID:                   cfg.RunID,
			MaxSteps:                cfg.MaxSteps,
			HardMaxSteps:            cfg.HardMaxSteps,
			MaxEmptyProviderRetries: cfg.MaxEmptyProviderRetries,
			NoProgressLimit:         cfg.NoProgressLimit,
			DuplicateToolLimit:      cfg.DuplicateToolLimit,
			WallTime:                cfg.WallTime,
		},
	}
}

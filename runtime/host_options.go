package runtime

import (
	"errors"
	"fmt"
	"time"

	"github.com/floegence/floret/v2/config"
	"github.com/floegence/floret/v2/tools"
)

// ThreadCompactionOption configures one thread compaction host.
type ThreadCompactionOption struct {
	category string
	apply    func(*threadCompactionOptions) error
}

// SubAgentOption configures one parent-bound SubAgent host.
type SubAgentOption struct {
	category string
	apply    func(*subAgentOptions) error
}

// newThreadCompactionOptions constructs validated options for a compaction host.
func newThreadCompactionOptions(cfg config.Config, options ...ThreadCompactionOption) (threadCompactionOptions, error) {
	out := threadCompactionOptions{config: cfg, initialized: true}
	seen := make(map[string]struct{}, len(options))
	for index, option := range options {
		if option.category == "" || option.apply == nil {
			return threadCompactionOptions{}, fmt.Errorf("thread compaction option %d is invalid", index)
		}
		if _, ok := seen[option.category]; ok {
			return threadCompactionOptions{}, fmt.Errorf("thread compaction option category %q is duplicated", option.category)
		}
		if err := option.apply(&out); err != nil {
			return threadCompactionOptions{}, fmt.Errorf("thread compaction option %d: %w", index, err)
		}
		seen[option.category] = struct{}{}
	}
	if err := out.validate(); err != nil {
		return threadCompactionOptions{}, err
	}
	return out, nil
}

// newSubAgentOptions constructs validated options for a parent-bound SubAgent host.
func newSubAgentOptions(cfg config.Config, options ...SubAgentOption) (subAgentOptions, error) {
	out := subAgentOptions{config: cfg, initialized: true}
	seen := make(map[string]struct{}, len(options))
	for index, option := range options {
		if option.category == "" || option.apply == nil {
			return subAgentOptions{}, fmt.Errorf("subagent option %d is invalid", index)
		}
		if _, ok := seen[option.category]; ok {
			return subAgentOptions{}, fmt.Errorf("subagent option category %q is duplicated", option.category)
		}
		if err := option.apply(&out); err != nil {
			return subAgentOptions{}, fmt.Errorf("subagent option %d: %w", index, err)
		}
		seen[option.category] = struct{}{}
	}
	if err := out.validate(); err != nil {
		return subAgentOptions{}, err
	}
	return out, nil
}

func (options turnExecutionOptions) validate() error {
	if !options.initialized {
		return errors.New("turn execution host options must be constructed with newTurnExecutionOptions")
	}
	return validateProviderHostOptions(options.config, options.modelGateway, options.modelGatewayIdentity, options.modelGatewayCapabilities, options.loopLimits, options.threadTitleMode)
}

func (options threadCompactionOptions) validate() error {
	if !options.initialized {
		return errors.New("thread compaction host options must be constructed with newThreadCompactionOptions")
	}
	return validateProviderHostOptions(options.config, options.modelGateway, options.modelGatewayIdentity, options.modelGatewayCapabilities, options.loopLimits, ThreadTitleModeHostOwned)
}

func (options subAgentOptions) validate() error {
	if !options.initialized {
		return errors.New("subagent host options must be constructed with newSubAgentOptions")
	}
	if options.subAgentRunTimeout < 0 {
		return errors.New("subagent run timeout cannot be negative")
	}
	return validateProviderHostOptions(options.config, options.modelGateway, options.modelGatewayIdentity, options.modelGatewayCapabilities, options.loopLimits, options.threadTitleMode)
}

func validateProviderHostOptions(cfg config.Config, gateway ModelGateway, identity ModelGatewayIdentity, capabilities ModelGatewayCapabilities, limits LoopLimits, titleMode ThreadTitleMode) error {
	if err := capabilities.validate(gateway); err != nil {
		return err
	}
	if gateway != nil {
		if _, err := normalizeModelGatewayIdentity(identity); err != nil {
			return err
		}
	} else if identity != (ModelGatewayIdentity{}) {
		return errors.New("native provider host must not provide model gateway identity")
	}
	if gateway != nil {
		if _, err := resolveModelGatewayHostConfig(cfg, identity); err != nil {
			return err
		}
	} else if _, err := config.Resolve(cfg, nil); err != nil {
		return err
	}
	if limits.MaxEmptyProviderRetries < 0 || limits.NoProgressLimit < 0 || limits.DuplicateToolLimit < 0 || limits.WallTime < 0 {
		return errors.New("loop limits cannot be negative")
	}
	_, err := normalizeThreadTitleMode(titleMode)
	return err
}

// WithThreadCompactionModelGateway atomically configures a custom gateway and
// its declared identity and capabilities.
func WithThreadCompactionModelGateway(gateway ModelGateway, identity ModelGatewayIdentity, capabilities ModelGatewayCapabilities) ThreadCompactionOption {
	return ThreadCompactionOption{category: "model_gateway", apply: func(o *threadCompactionOptions) error {
		if gateway == nil {
			return errors.New("model gateway is required")
		}
		o.modelGateway, o.modelGatewayIdentity, o.modelGatewayCapabilities = gateway, identity, capabilities
		return nil
	}}
}

// WithThreadCompactionEventSink observes the runtime event contract.
func WithThreadCompactionEventSink(sink EventSink) ThreadCompactionOption {
	return ThreadCompactionOption{category: "event_sink", apply: func(o *threadCompactionOptions) error {
		if sink == nil {
			return errors.New("event sink is required")
		}
		o.sink = sink
		return nil
	}}
}

// WithThreadCompactionIDGenerator supplies deterministic correlation
// identifiers. It does not derive ThreadID, TurnID, RunID, or PromptScopeID.
func WithThreadCompactionIDGenerator(generator func(string) string) ThreadCompactionOption {
	return ThreadCompactionOption{category: "id_generator", apply: func(o *threadCompactionOptions) error {
		if generator == nil {
			return errors.New("id generator is required")
		}
		o.idGenerator = generator
		return nil
	}}
}

// WithThreadCompactionLoopLimits configures provider loop limits.
func WithThreadCompactionLoopLimits(limits LoopLimits) ThreadCompactionOption {
	return ThreadCompactionOption{category: "loop_limits", apply: func(o *threadCompactionOptions) error { o.loopLimits = limits; return nil }}
}

// WithSubAgentModelGateway atomically configures a custom gateway and its
// declared identity and capabilities.
func WithSubAgentModelGateway(gateway ModelGateway, identity ModelGatewayIdentity, capabilities ModelGatewayCapabilities) SubAgentOption {
	return SubAgentOption{category: "model_gateway", apply: func(o *subAgentOptions) error {
		if gateway == nil {
			return errors.New("model gateway is required")
		}
		o.modelGateway, o.modelGatewayIdentity, o.modelGatewayCapabilities = gateway, identity, capabilities
		return nil
	}}
}

// WithSubAgentReadOnlyTools configures an immutable registry snapshot after
// proving every tool is locally read-only and statically allowed.
func WithSubAgentReadOnlyTools(items ...tools.Tool) SubAgentOption {
	registry, err := newReadOnlyToolRegistry(items)
	return SubAgentOption{category: "tools", apply: func(o *subAgentOptions) error {
		if err != nil {
			return err
		}
		o.tools = registry
		return nil
	}}
}

// WithSubAgentEffectfulTools configures the explicit effect authorization path.
func WithSubAgentEffectfulTools(registry *tools.Registry, gate EffectAuthorizationGate) SubAgentOption {
	return SubAgentOption{category: "tools", apply: func(o *subAgentOptions) error {
		if registry == nil || gate == nil {
			return errors.New("effectful tools require a registry and authorization gate")
		}
		o.tools, o.effectAuthorizationGate = registry, gate
		return nil
	}}
}

// WithSubAgentEventSink observes the runtime event contract.
func WithSubAgentEventSink(sink EventSink) SubAgentOption {
	return SubAgentOption{category: "event_sink", apply: func(o *subAgentOptions) error {
		if sink == nil {
			return errors.New("event sink is required")
		}
		o.sink = sink
		return nil
	}}
}

// WithSubAgentDynamicToolSurface configures the per-step tool surface owner.
func WithSubAgentDynamicToolSurface(provider ToolSurfaceProvider) SubAgentOption {
	return SubAgentOption{category: "dynamic_tool_surface", apply: func(o *subAgentOptions) error {
		if provider == nil {
			return errors.New("dynamic tool surface provider is required")
		}
		o.toolSurfaceProvider = provider
		return nil
	}}
}

// WithSubAgentIDGenerator supplies deterministic correlation identifiers. It
// does not derive ThreadID, TurnID, RunID, or PromptScopeID.
func WithSubAgentIDGenerator(generator func(string) string) SubAgentOption {
	return SubAgentOption{category: "id_generator", apply: func(o *subAgentOptions) error {
		if generator == nil {
			return errors.New("id generator is required")
		}
		o.idGenerator = generator
		return nil
	}}
}

// WithSubAgentLoopLimits configures provider loop limits.
func WithSubAgentLoopLimits(limits LoopLimits) SubAgentOption {
	return SubAgentOption{category: "loop_limits", apply: func(o *subAgentOptions) error { o.loopLimits = limits; return nil }}
}

// WithSubAgentRunTimeout bounds one child run without changing its identity.
func WithSubAgentRunTimeout(timeout time.Duration) SubAgentOption {
	return SubAgentOption{category: "run_timeout", apply: func(o *subAgentOptions) error { o.subAgentRunTimeout = timeout; return nil }}
}

// WithSubAgentCapabilities configures product-neutral runtime capability sources.
func WithSubAgentCapabilities(capabilities CapabilityOptions) SubAgentOption {
	return SubAgentOption{category: "capabilities", apply: func(o *subAgentOptions) error { o.capabilities = capabilities; return nil }}
}

// WithSubAgentThreadTitleMode selects host-owned or provider-owned child titles.
func WithSubAgentThreadTitleMode(mode ThreadTitleMode) SubAgentOption {
	return SubAgentOption{category: "thread_title_mode", apply: func(o *subAgentOptions) error { o.threadTitleMode = mode; return nil }}
}

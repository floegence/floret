package runtime

import (
	"errors"
	"fmt"

	"github.com/floegence/floret/v2/config"
	"github.com/floegence/floret/v2/tools"
)

// TurnExecutionOption is an opaque option for one turn execution capability.
// Valid values are returned only by this package's With functions.
type TurnExecutionOption struct {
	category string
	apply    func(*turnExecutionOptions) error
}

func (option TurnExecutionOption) applyTurnExecution(builder *turnExecutionOptionsBuilder) error {
	if builder == nil {
		return errors.New("turn execution options builder is required")
	}
	if option.apply == nil || option.category == "" {
		return errors.New("turn execution option is invalid")
	}
	if _, exists := builder.seen[option.category]; exists {
		return fmt.Errorf("turn execution option category %q is duplicated", option.category)
	}
	if err := option.apply(&builder.options); err != nil {
		return err
	}
	builder.seen[option.category] = struct{}{}
	return nil
}

type turnExecutionOptionsBuilder struct {
	options turnExecutionOptions
	seen    map[string]struct{}
}

// newTurnExecutionOptions constructs validated options without changing the
// authority or lifecycle of turnExecutionCapability.
func newTurnExecutionOptions(cfg config.Config, options ...TurnExecutionOption) (turnExecutionOptions, error) {
	builder := turnExecutionOptionsBuilder{
		options: turnExecutionOptions{config: cfg, initialized: true},
		seen:    make(map[string]struct{}, len(options)),
	}
	for index, option := range options {
		if err := option.applyTurnExecution(&builder); err != nil {
			return turnExecutionOptions{}, fmt.Errorf("turn execution option %d: %w", index, err)
		}
	}
	if err := builder.options.validate(); err != nil {
		return turnExecutionOptions{}, err
	}
	return builder.options, nil
}

// WithTurnModelGateway atomically configures a custom gateway and its declared
// identity and capabilities.
func WithTurnModelGateway(gateway ModelGateway, identity ModelGatewayIdentity, capabilities ModelGatewayCapabilities) TurnExecutionOption {
	return TurnExecutionOption{category: "model_gateway", apply: func(options *turnExecutionOptions) error {
		if gateway == nil {
			return errors.New("model gateway is required")
		}
		options.modelGateway = gateway
		options.modelGatewayIdentity = identity
		options.modelGatewayCapabilities = capabilities
		return nil
	}}
}

// WithTurnReadOnlyTools configures an immutable registry snapshot after proving
// every tool is locally read-only and statically allowed.
func WithTurnReadOnlyTools(items ...tools.Tool) TurnExecutionOption {
	registry, snapshotErr := newReadOnlyToolRegistry(items)
	return TurnExecutionOption{category: "tools", apply: func(options *turnExecutionOptions) error {
		if snapshotErr != nil {
			return snapshotErr
		}
		options.tools = registry
		return nil
	}}
}

func newReadOnlyToolRegistry(items []tools.Tool) (*tools.Registry, error) {
	for index, item := range items {
		definition, err := tools.ValidateDefinition(item.Definition)
		if err != nil {
			return nil, fmt.Errorf("read-only tool %d: %w", index, err)
		}
		if err := validateProvablyReadOnlyTool(definition); err != nil {
			return nil, fmt.Errorf("read-only tool %q: %w", definition.Name, err)
		}
	}
	registry, err := tools.NewRegistryE(items...)
	if err != nil {
		return nil, err
	}
	registry.Seal()
	return registry, nil
}

func validateProvablyReadOnlyTool(definition tools.Definition) error {
	if !definition.ReadOnly || definition.Destructive || definition.OpenWorld {
		return errors.New("tool must be read-only, non-destructive, and closed-world")
	}
	if definition.Permission.Mode != tools.PermissionAllow || definition.PermissionFor != nil {
		return errors.New("tool must use static allow permission")
	}
	for _, effect := range definition.Effects {
		if effect != tools.EffectRead {
			return fmt.Errorf("effect %q is not read-only", effect)
		}
	}
	return nil
}

// WithTurnEffectfulTools configures the explicit effect authorization path.
func WithTurnEffectfulTools(registry *tools.Registry, gate EffectAuthorizationGate) TurnExecutionOption {
	return TurnExecutionOption{category: "tools", apply: func(options *turnExecutionOptions) error {
		if registry == nil {
			return errors.New("effectful tool registry is required")
		}
		if gate == nil {
			return errors.New("effect authorization gate is required")
		}
		options.tools = registry
		options.effectAuthorizationGate = gate
		return nil
	}}
}

// WithTurnEventSink observes the existing runtime event contract.
func WithTurnEventSink(sink EventSink) TurnExecutionOption {
	return TurnExecutionOption{category: "event_sink", apply: func(options *turnExecutionOptions) error {
		if sink == nil {
			return errors.New("event sink is required")
		}
		options.sink = sink
		return nil
	}}
}

// WithTurnDynamicToolSurface configures the existing per-step tool surface owner.
func WithTurnDynamicToolSurface(provider ToolSurfaceProvider) TurnExecutionOption {
	return TurnExecutionOption{category: "dynamic_tool_surface", apply: func(options *turnExecutionOptions) error {
		if provider == nil {
			return errors.New("dynamic tool surface provider is required")
		}
		options.toolSurfaceProvider = provider
		return nil
	}}
}

// WithTurnIDGenerator supplies deterministic correlation identifiers. It does
// not derive ThreadID, TurnID, RunID, or PromptScopeID values.
func WithTurnIDGenerator(generator func(string) string) TurnExecutionOption {
	return TurnExecutionOption{category: "id_generator", apply: func(options *turnExecutionOptions) error {
		if generator == nil {
			return errors.New("id generator is required")
		}
		options.idGenerator = generator
		return nil
	}}
}

// WithTurnLoopLimits configures turn loop limits.
func WithTurnLoopLimits(limits LoopLimits) TurnExecutionOption {
	return TurnExecutionOption{category: "loop_limits", apply: func(options *turnExecutionOptions) error {
		if limits.MaxEmptyProviderRetries < 0 {
			return errors.New("max empty provider retries cannot be negative")
		}
		if limits.NoProgressLimit < 0 {
			return errors.New("no progress limit cannot be negative")
		}
		if limits.DuplicateToolLimit < 0 {
			return errors.New("duplicate tool limit cannot be negative")
		}
		if limits.WallTime < 0 {
			return errors.New("loop wall time cannot be negative")
		}
		options.loopLimits = limits
		return nil
	}}
}

// WithTurnCapabilities configures product-neutral runtime capability sources.
func WithTurnCapabilities(capabilities CapabilityOptions) TurnExecutionOption {
	return TurnExecutionOption{category: "capabilities", apply: func(options *turnExecutionOptions) error {
		options.capabilities = capabilities
		return nil
	}}
}

// WithTurnThreadTitleMode selects host-owned or provider-owned title generation.
func WithTurnThreadTitleMode(mode ThreadTitleMode) TurnExecutionOption {
	return TurnExecutionOption{category: "thread_title_mode", apply: func(options *turnExecutionOptions) error {
		normalized, err := normalizeThreadTitleMode(mode)
		if err != nil {
			return err
		}
		options.threadTitleMode = normalized
		return nil
	}}
}

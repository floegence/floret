package runtime

import (
	"errors"
	"fmt"

	"github.com/floegence/floret/config"
	"github.com/floegence/floret/tools"
)

// TurnExecutionOption is an opaque option for one turn execution capability.
// Valid values are returned only by this package's With functions.
type TurnExecutionOption struct {
	category string
	apply    func(*TurnExecutionHostOptions) error
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
	options TurnExecutionHostOptions
	seen    map[string]struct{}
}

// NewTurnExecutionOptions constructs validated options without changing the
// authority or lifecycle of TurnExecutionHost.
func NewTurnExecutionOptions(cfg config.Config, options ...TurnExecutionOption) (TurnExecutionHostOptions, error) {
	builder := turnExecutionOptionsBuilder{
		options: TurnExecutionHostOptions{Config: cfg},
		seen:    make(map[string]struct{}, len(options)),
	}
	for index, option := range options {
		if err := option.applyTurnExecution(&builder); err != nil {
			return TurnExecutionHostOptions{}, fmt.Errorf("turn execution option %d: %w", index, err)
		}
	}
	if err := builder.options.ModelGatewayCapabilities.validate(builder.options.ModelGateway); err != nil {
		return TurnExecutionHostOptions{}, err
	}
	if builder.options.ModelGateway != nil {
		if _, err := normalizeModelGatewayIdentity(builder.options.ModelGatewayIdentity); err != nil {
			return TurnExecutionHostOptions{}, err
		}
	}
	if _, err := normalizeThreadTitleMode(builder.options.ThreadTitleMode); err != nil {
		return TurnExecutionHostOptions{}, err
	}
	return builder.options, nil
}

// WithModelGateway atomically configures a custom gateway and its declared
// identity and capabilities.
func WithModelGateway(gateway ModelGateway, identity ModelGatewayIdentity, capabilities ModelGatewayCapabilities) TurnExecutionOption {
	return TurnExecutionOption{category: "model_gateway", apply: func(options *TurnExecutionHostOptions) error {
		if gateway == nil {
			return errors.New("model gateway is required")
		}
		options.ModelGateway = gateway
		options.ModelGatewayIdentity = identity
		options.ModelGatewayCapabilities = capabilities
		return nil
	}}
}

// WithReadOnlyTools configures an immutable registry snapshot after proving
// every tool is locally read-only and statically allowed.
func WithReadOnlyTools(items ...tools.Tool) TurnExecutionOption {
	registry, snapshotErr := newReadOnlyToolRegistry(items)
	return TurnExecutionOption{category: "tools", apply: func(options *TurnExecutionHostOptions) error {
		if snapshotErr != nil {
			return snapshotErr
		}
		options.Tools = registry
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

// WithEffectfulTools configures the explicit effect authorization path.
func WithEffectfulTools(registry *tools.Registry, gate EffectAuthorizationGate) TurnExecutionOption {
	return TurnExecutionOption{category: "tools", apply: func(options *TurnExecutionHostOptions) error {
		if registry == nil {
			return errors.New("effectful tool registry is required")
		}
		if gate == nil {
			return errors.New("effect authorization gate is required")
		}
		options.Tools = registry
		options.EffectAuthorizationGate = gate
		return nil
	}}
}

// WithEventSink observes the existing runtime event contract.
func WithEventSink(sink EventSink) TurnExecutionOption {
	return TurnExecutionOption{category: "event_sink", apply: func(options *TurnExecutionHostOptions) error {
		if sink == nil {
			return errors.New("event sink is required")
		}
		options.Sink = sink
		return nil
	}}
}

// WithDynamicToolSurface configures the existing per-step tool surface owner.
func WithDynamicToolSurface(provider ToolSurfaceProvider) TurnExecutionOption {
	return TurnExecutionOption{category: "dynamic_tool_surface", apply: func(options *TurnExecutionHostOptions) error {
		if provider == nil {
			return errors.New("dynamic tool surface provider is required")
		}
		options.ToolSurfaceProvider = provider
		return nil
	}}
}

// WithLoopLimits configures turn loop limits.
func WithLoopLimits(limits LoopLimits) TurnExecutionOption {
	return TurnExecutionOption{category: "loop_limits", apply: func(options *TurnExecutionHostOptions) error {
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
		options.LoopLimits = limits
		return nil
	}}
}

// WithThreadTitleMode selects host-owned or provider-owned title generation.
func WithThreadTitleMode(mode ThreadTitleMode) TurnExecutionOption {
	return TurnExecutionOption{category: "thread_title_mode", apply: func(options *TurnExecutionHostOptions) error {
		normalized, err := normalizeThreadTitleMode(mode)
		if err != nil {
			return err
		}
		options.ThreadTitleMode = normalized
		return nil
	}}
}

package runtime

import (
	"errors"
	"fmt"
	"time"

	"github.com/floegence/floret/v4/config"
	"github.com/floegence/floret/v4/provider"
	"github.com/floegence/floret/v4/tools"
)

// Agent is an immutable assistant persona and execution-capability snapshot.
// It contains no durable conversation identity or storage ownership.
type Agent struct {
	configuration       config.AgentConfig
	gateway             provider.Gateway
	tools               *tools.Registry
	effectAuthorization EffectAuthorizationGate
	eventSink           EventSink
	toolSurface         ToolSurfaceProvider
	idGenerator         func(string) string
	loopLimits          LoopLimits
	capabilities        CapabilityOptions
	threadTitleMode     ThreadTitleMode
	subAgentRunTimeout  time.Duration
	manualCompactions   ManualCompactionSource
}

// AgentOption configures one immutable Agent capability at construction time.
type AgentOption struct {
	category string
	apply    func(*agentBuilder) error
}

type agentBuilder struct {
	agent *Agent
	seen  map[string]struct{}
}

// NewAgent validates and snapshots one provider-neutral Agent configuration.
func NewAgent(configuration config.AgentConfig, gateway provider.Gateway, options ...AgentOption) (*Agent, error) {
	if err := configuration.Validate(); err != nil {
		return nil, fmt.Errorf("agent config: %w", err)
	}
	if gateway == nil {
		return nil, errors.New("agent provider gateway is required")
	}
	if err := gateway.Identity().Validate(); err != nil {
		return nil, fmt.Errorf("agent provider identity: %w", err)
	}
	capabilities := gateway.Capabilities()
	if err := capabilities.Validate(); err != nil {
		return nil, fmt.Errorf("agent provider capabilities: %w", err)
	}
	if capabilities.AttachmentPayload == provider.AttachmentExpanded {
		if _, ok := gateway.(provider.RequestPreparer); !ok {
			return nil, errors.New("expanded provider attachments require request preparation")
		}
	}
	builder := agentBuilder{
		agent: &Agent{
			configuration:   cloneAgentConfig(configuration),
			gateway:         gateway,
			tools:           tools.NewRegistry(),
			threadTitleMode: ThreadTitleModeHostOwned,
		},
		seen: make(map[string]struct{}, len(options)),
	}
	for index, option := range options {
		if option.category == "" || option.apply == nil {
			return nil, fmt.Errorf("agent option %d is invalid", index)
		}
		if _, exists := builder.seen[option.category]; exists {
			return nil, fmt.Errorf("agent option category %q is duplicated", option.category)
		}
		if err := option.apply(&builder); err != nil {
			return nil, fmt.Errorf("agent option %d: %w", index, err)
		}
		builder.seen[option.category] = struct{}{}
	}
	if err := validateAgentPolicies(builder.agent); err != nil {
		return nil, err
	}
	builder.agent.tools.Seal()
	return builder.agent, nil
}

// Config returns a detached copy of the Agent's persona and model policy.
func (agent *Agent) Config() config.AgentConfig {
	if agent == nil {
		return config.AgentConfig{}
	}
	return cloneAgentConfig(agent.configuration)
}

// ProviderIdentity returns the immutable identity declared by the Gateway.
func (agent *Agent) ProviderIdentity() provider.Identity {
	if agent == nil || agent.gateway == nil {
		return provider.Identity{}
	}
	return agent.gateway.Identity()
}

// ToolDefinitions returns detached provider-visible definitions for the
// Agent's static tool snapshot.
func (agent *Agent) ToolDefinitions() []tools.ToolDefinition {
	if agent == nil || agent.tools == nil {
		return nil
	}
	return agent.tools.Definitions()
}

// WithAgentTools snapshots static local tools for every execution by the Agent.
func WithAgentTools(items ...tools.Tool) AgentOption {
	return AgentOption{category: "tools", apply: func(builder *agentBuilder) error {
		registry, err := tools.NewRegistryE(items...)
		if err != nil {
			return err
		}
		builder.agent.tools = registry
		return nil
	}}
}

// WithAgentEffectAuthorization configures the host-owned authorization gate
// for effectful local tools.
func WithAgentEffectAuthorization(gate EffectAuthorizationGate) AgentOption {
	return AgentOption{category: "effect_authorization", apply: func(builder *agentBuilder) error {
		if gate == nil {
			return errors.New("agent effect authorization gate is required")
		}
		builder.agent.effectAuthorization = gate
		return nil
	}}
}

// WithAgentEventSink configures runtime event observation.
func WithAgentEventSink(sink EventSink) AgentOption {
	return AgentOption{category: "event_sink", apply: func(builder *agentBuilder) error {
		if sink == nil {
			return errors.New("agent event sink is required")
		}
		builder.agent.eventSink = sink
		return nil
	}}
}

// WithAgentDynamicToolSurface configures the product-neutral per-step tool
// surface source.
func WithAgentDynamicToolSurface(surface ToolSurfaceProvider) AgentOption {
	return AgentOption{category: "dynamic_tool_surface", apply: func(builder *agentBuilder) error {
		if surface == nil {
			return errors.New("agent dynamic tool surface is required")
		}
		builder.agent.toolSurface = surface
		return nil
	}}
}

// WithAgentIDGenerator configures deterministic non-domain correlation IDs.
func WithAgentIDGenerator(generator func(string) string) AgentOption {
	return AgentOption{category: "id_generator", apply: func(builder *agentBuilder) error {
		if generator == nil {
			return errors.New("agent id generator is required")
		}
		builder.agent.idGenerator = generator
		return nil
	}}
}

// WithAgentLoopLimits configures provider-loop safety limits.
func WithAgentLoopLimits(limits LoopLimits) AgentOption {
	return AgentOption{category: "loop_limits", apply: func(builder *agentBuilder) error {
		builder.agent.loopLimits = limits
		return nil
	}}
}

// WithAgentCapabilities configures product-neutral capability sources.
func WithAgentCapabilities(capabilities CapabilityOptions) AgentOption {
	return AgentOption{category: "capabilities", apply: func(builder *agentBuilder) error {
		capabilities.SkillSources = append([]string(nil), capabilities.SkillSources...)
		builder.agent.capabilities = capabilities
		return nil
	}}
}

// WithAgentThreadTitleMode configures host-owned or provider-owned titles.
func WithAgentThreadTitleMode(mode ThreadTitleMode) AgentOption {
	return AgentOption{category: "thread_title_mode", apply: func(builder *agentBuilder) error {
		builder.agent.threadTitleMode = mode
		return nil
	}}
}

// WithAgentSubAgentTimeout bounds one child execution.
func WithAgentSubAgentTimeout(timeout time.Duration) AgentOption {
	return AgentOption{category: "subagent_timeout", apply: func(builder *agentBuilder) error {
		builder.agent.subAgentRunTimeout = timeout
		return nil
	}}
}

// WithAgentManualCompactions configures host-owned manual compaction requests
// that are polled at safe points during each turn executed by the Agent.
func WithAgentManualCompactions(source ManualCompactionSource) AgentOption {
	return AgentOption{category: "manual_compactions", apply: func(builder *agentBuilder) error {
		if source == nil {
			return errors.New("agent manual compaction source is required")
		}
		builder.agent.manualCompactions = source
		return nil
	}}
}

func validateAgentPolicies(agent *Agent) error {
	if agent.loopLimits.MaxEmptyProviderRetries < 0 || agent.loopLimits.NoProgressLimit < 0 || agent.loopLimits.DuplicateToolLimit < 0 || agent.loopLimits.WallTime < 0 {
		return errors.New("agent loop limits cannot be negative")
	}
	if agent.subAgentRunTimeout < 0 {
		return errors.New("agent SubAgent timeout cannot be negative")
	}
	mode, err := normalizeThreadTitleMode(agent.threadTitleMode)
	if err != nil {
		return err
	}
	agent.threadTitleMode = mode
	return nil
}

func cloneAgentConfig(configuration config.AgentConfig) config.AgentConfig {
	return config.AgentConfig{
		Profile:      configuration.Profile,
		SystemPrompt: configuration.SystemPrompt,
		Context:      configuration.Context,
		Reasoning:    configuration.Reasoning,
	}
}

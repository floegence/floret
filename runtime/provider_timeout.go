package runtime

import "github.com/floegence/floret/v7/internal/providerattempt"

// ProviderRequestTimeoutPolicy bounds physical requests, excluding local tools,
// approvals and user input. Zero fields use defaults; negative fields are invalid.
type ProviderRequestTimeoutPolicy providerattempt.RequestTimeoutPolicy

// DefaultProviderRequestTimeoutPolicy allows five minutes for first output or
// hosted tools, two minutes between output, and fifteen minutes per attempt.
func DefaultProviderRequestTimeoutPolicy() ProviderRequestTimeoutPolicy {
	return ProviderRequestTimeoutPolicy(providerattempt.DefaultRequestTimeoutPolicy())
}

// Validate rejects negative request deadlines.
func (policy ProviderRequestTimeoutPolicy) Validate() error {
	return providerattempt.RequestTimeoutPolicy(policy).Validate()
}

// WithAgentProviderRequestTimeoutPolicy sets the single physical-request policy.
func WithAgentProviderRequestTimeoutPolicy(policy ProviderRequestTimeoutPolicy) AgentOption {
	return AgentOption{category: "provider_timeout", apply: func(builder *agentBuilder) error {
		if err := policy.Validate(); err != nil {
			return err
		}
		builder.agent.providerRequestTimeoutPolicy = policy
		return nil
	}}
}

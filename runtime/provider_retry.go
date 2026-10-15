package runtime

import (
	"github.com/floegence/floret/v7/internal/providerretry"
	"github.com/floegence/floret/v7/observation"
)

// ProviderRetryPolicy bounds recovery of each frozen model request. An Agent
// defaults to DefaultProviderRetryPolicy; set Disabled to disable recovery.
type ProviderRetryPolicy providerretry.Policy

// ProviderRetryProgress describes a scheduled attempt replacement in live state.
type ProviderRetryProgress = observation.ProviderRetryProgress

// DefaultProviderRetryPolicy returns four extra attempts, three timeout retries,
// jittered 2/4/8/10-second delays and a five-minute recovery window.
func DefaultProviderRetryPolicy() ProviderRetryPolicy {
	return ProviderRetryPolicy(providerretry.DefaultPolicy())
}

// WithAgentProviderRetryPolicy configures and validates automatic model recovery.
func WithAgentProviderRetryPolicy(policy ProviderRetryPolicy) AgentOption {
	return AgentOption{category: "provider_retry", apply: func(builder *agentBuilder) error {
		if err := policy.Validate(); err != nil {
			return err
		}
		builder.agent.providerRetryPolicy = policy
		return nil
	}}
}

func cloneProviderRetryProgress(value *ProviderRetryProgress) *ProviderRetryProgress {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

// Validate rejects invalid budgets, delays and recovery windows.
func (policy ProviderRetryPolicy) Validate() error { return providerretry.Policy(policy).Validate() }

package engine

import (
	"fmt"

	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
)

func validateContextBudget(policy contextpolicy.Policy) error {
	policy = contextpolicy.Normalize(policy)
	if contextpolicy.RequestSafeLimitTokens(policy) <= 0 {
		return fmt.Errorf("%w: context_window_tokens=%d reserved_output_tokens=%d request_safe_limit=%d", ErrInvalidContextBudget, policy.ContextWindowTokens, contextpolicy.OutputHeadroom(policy), contextpolicy.RequestSafeLimitTokens(policy))
	}
	return nil
}

func fixedContextBudgetError(req provider.Request, validation compactedRequestValidation) error {
	return fmt.Errorf("%w: context_window_tokens=%d reserved_output_tokens=%d projected_input_tokens=%d request_safe_limit=%d fixed_input_tokens=%d", ErrFixedContextOverBudget, req.ContextPolicy.ContextWindowTokens, contextpolicy.OutputHeadroom(req.ContextPolicy), validation.ContextPressure.ProjectedInputTokens, validation.RequestSafeLimit, validation.FixedInputTokens)
}

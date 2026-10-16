package engine

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"

	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
)

func validateContextBudget(policy contextpolicy.Policy) error {
	policy = contextpolicy.Normalize(policy)
	if contextpolicy.ResolveRequestBudget(policy, 0).Invalid {
		return fmt.Errorf("%w: context_window_tokens=%d reserved_output_tokens=%d request_safe_limit=%d", ErrInvalidContextBudget, policy.ContextWindowTokens, contextpolicy.OutputHeadroom(policy), contextpolicy.RequestSafeLimitTokens(policy))
	}
	return nil
}

func fixedContextBudgetError(req provider.Request, validation compactedRequestValidation) error {
	return fmt.Errorf("%w: context_window_tokens=%d reserved_output_tokens=%d projected_input_tokens=%d request_safe_limit=%d fixed_input_tokens=%d", ErrFixedContextOverBudget, req.ContextPolicy.ContextWindowTokens, contextpolicy.OutputHeadroom(req.ContextPolicy), validation.ContextPressure.ProjectedInputTokens, validation.RequestSafeLimit, validation.FixedInputTokens)
}

func (e *Engine) initialRequestOutput(policy contextpolicy.Policy) int64 {
	if policy.ModelMaxOutputTokens > 0 {
		if controller, ok := e.provider.(provider.OutputController); ok && controller.OutputTokenControl() == "server" {
			return 0
		}
	}
	if policy.MaxOutputTokens > 0 {
		return policy.MaxOutputTokens
	}
	return policy.ModelMaxOutputTokens
}

// finalizeRequestOutput changes only generation controls on already frozen input.
func (e *Engine) finalizeRequestOutput(ctx context.Context, req provider.Request) (provider.Request, error) {
	if req.ContextPolicy.ModelMaxOutputTokens <= 0 {
		return req, nil
	}
	control := ""
	if controller, ok := e.provider.(provider.OutputController); ok {
		control = controller.OutputTokenControl()
	}
	if control != "client" && control != "server" {
		return req, fmt.Errorf("%w: provider output control must be explicit", ErrInvalidContextBudget)
	}
	if control == "server" && req.ContextPolicy.MaxOutputTokens > 0 {
		return req, fmt.Errorf("%w: server-managed provider cannot enforce explicit output limit", ErrInvalidContextBudget)
	}
	if req.Reasoning.BudgetTokens == math.MaxInt64 {
		return req, fmt.Errorf("%w: reasoning budget exceeds output capacity", ErrInvalidContextBudget)
	}
	minimum := max(int64(1), req.Reasoning.BudgetTokens+1)
	budget := contextpolicy.ResolveRequestBudget(req.ContextPolicy, req.ContextPressure.ProjectedInputTokens, minimum)
	if budget.Invalid {
		return req, fmt.Errorf("%w: output limit cannot accommodate model or reasoning requirements", ErrInvalidContextBudget)
	}
	req.ContextPressure.RequestSafeLimit = budget.RequestSafeLimit
	req.ContextPressure.ThresholdTokens = budget.ThresholdTokens
	req.ContextPressure.CompactionNeeded = budget.CompactionNeeded
	req.ContextPressure.HardLimitExceeded = budget.HardLimitExceeded
	req.ContextPressure.SafetyMarginTokens = budget.SafetyMarginTokens
	req.ContextPressure.OutputControl = control
	req.ContextPressure.OutputHeadroomTokens = budget.HeadroomTokens
	if budget.HardLimitExceeded {
		return req, nil
	}
	if control == "server" {
		req.MaxOutputTokens = 0
		return req, nil
	}
	limiter, ok := req.Prepared.(provider.PreparedRequestOutputLimiter)
	if !ok {
		return req, fmt.Errorf("%w: provider lacks frozen output limit support", ErrInvalidContextBudget)
	}
	originalEstimate := req.Prepared.TokenEstimate()
	next, err := limiter.WithMaxOutputTokens(ctx, budget.OutputTokens)
	if err != nil {
		return req, withFailureOrigin(err, FailureOriginContract)
	}
	if next == nil {
		return req, errors.New("provider returned nil output-limited request")
	}
	if err := closePreparedRequest(req); err != nil {
		return req, errors.Join(err, next.Close())
	}
	req.Prepared = next
	req.MaxOutputTokens = budget.OutputTokens
	req.ContextPressure.RequestOutputTokens = budget.OutputTokens
	estimate := next.TokenEstimate()
	if err := validatePreparedTokenEstimate(estimate); err != nil {
		return req, err
	}
	if !reflect.DeepEqual(estimate, originalEstimate) {
		return req, errors.New("output control changed prepared input estimate")
	}
	fingerprint := next.PayloadFingerprint()
	if fingerprint == "" || len(fingerprint) > 512 || strings.ContainsAny(fingerprint, "\r\n\x00") {
		return req, errors.New("output-limited request requires payload fingerprint")
	}
	if req.EphemeralUser == nil {
		req.RawPlan.PayloadHash = fingerprint
	}
	return req, nil
}

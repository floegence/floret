package sessiontree

const (
	TurnFailureCodeMetadataKey = "failure_code"

	TurnFailureCancelled                = "cancelled"
	TurnFailureInterrupted              = "interrupted"
	TurnFailureProvider                 = "provider"
	TurnFailureToolDispatch             = "tool_dispatch"
	TurnFailureControlError             = "control_error"
	TurnFailureEffectOutcomeUnknown     = "effect_outcome_unknown"
	TurnFailureAuthorizationUnavailable = "authorization_unavailable"
	TurnFailureAuthorizationContract    = "authorization_contract"
	TurnFailureStorage                  = "storage"
	TurnFailureEngineContract           = "engine_contract"
	TurnFailureContextBudgetInvalid     = "context_budget_invalid"
	TurnFailureContextFixedOverhead     = "context_fixed_overhead"
	TurnFailureContextCompactionLimit   = "context_compaction_limit"
	TurnFailureContextPrefixDrift       = "context_prefix_drift"
	TurnFailureLegacyUnclassified       = "legacy_unclassified"
)

func ValidTurnFailureCode(code string) bool {
	switch code {
	case TurnFailureCancelled,
		TurnFailureInterrupted,
		TurnFailureProvider,
		TurnFailureToolDispatch,
		TurnFailureControlError,
		TurnFailureEffectOutcomeUnknown,
		TurnFailureAuthorizationUnavailable,
		TurnFailureAuthorizationContract,
		TurnFailureStorage,
		TurnFailureEngineContract,
		TurnFailureContextBudgetInvalid,
		TurnFailureContextFixedOverhead,
		TurnFailureContextCompactionLimit,
		TurnFailureContextPrefixDrift,
		TurnFailureLegacyUnclassified:
		return true
	default:
		return false
	}
}

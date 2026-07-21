package sessiontree

const (
	TurnFailureCodeMetadataKey = "failure_code"

	TurnFailureCancelled                = "cancelled"
	TurnFailureInterrupted              = "interrupted"
	TurnFailureProvider                 = "provider"
	TurnFailureToolDispatch             = "tool_dispatch"
	TurnFailureEffectOutcomeUnknown     = "effect_outcome_unknown"
	TurnFailureAuthorizationUnavailable = "authorization_unavailable"
	TurnFailureAuthorizationContract    = "authorization_contract"
	TurnFailureStorage                  = "storage"
	TurnFailureEngineContract           = "engine_contract"
	TurnFailureLegacyUnclassified       = "legacy_unclassified"
)

func ValidTurnFailureCode(code string) bool {
	switch code {
	case TurnFailureCancelled,
		TurnFailureInterrupted,
		TurnFailureProvider,
		TurnFailureToolDispatch,
		TurnFailureEffectOutcomeUnknown,
		TurnFailureAuthorizationUnavailable,
		TurnFailureAuthorizationContract,
		TurnFailureStorage,
		TurnFailureEngineContract,
		TurnFailureLegacyUnclassified:
		return true
	default:
		return false
	}
}

package runtime

import (
	"encoding/json"
	"strings"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/providerattempt"
	"github.com/floegence/floret/v7/internal/sessionlifecycle"
	"github.com/floegence/floret/v7/internal/sessiontree"
	publicprovider "github.com/floegence/floret/v7/provider"
)

// Both complete reads and incremental terminal installation use the existing
// SessionLifecycle authority. Only exact failure/control facts are retained.
type threadLifecycleProjection struct {
	lifecycle sessionlifecycle.Projection
	turns     map[string]*threadLifecycleFacts
}
type threadLifecycleFacts struct {
	runID            identity.RunID
	failureMessage   string
	failureCode      ThreadTurnFailureCode
	timeout          *publicprovider.RequestTimeoutDetails
	cancellation     *ThreadCancellation
	cancellationSeen bool
	kind             TurnKind
	kindSeen         bool
}

func (projection *threadLifecycleProjection) observe(entry sessiontree.Entry) {
	projection.lifecycle.Observe(entry)
	if projection.turns == nil {
		projection.turns = map[string]*threadLifecycleFacts{}
	}
	facts := projection.turns[entry.TurnID]
	if facts == nil {
		facts = &threadLifecycleFacts{}
		projection.turns[entry.TurnID] = facts
	}
	if value := strings.TrimSpace(entry.RunID); entry.Type != sessiontree.EntryCustom && value != "" {
		facts.runID = identity.RunID(value)
	}
	if entry.Type == sessiontree.EntryRunFailure {
		facts.failureMessage = strings.TrimSpace(entry.Error)
	}
	if facts.failureMessage == "" {
		facts.failureMessage = strings.TrimSpace(entry.Metadata["failure_reason"])
	}
	if entry.Type == sessiontree.EntryTurnMarker && entry.Metadata["provider_timeout"] != "" {
		var details publicprovider.RequestTimeoutDetails
		if json.Unmarshal([]byte(entry.Metadata["provider_timeout"]), &details) == nil && details.Validate() == nil {
			facts.timeout = &details
		}
	}
	if value := strings.TrimSpace(entry.Metadata[sessiontree.TurnFailureCodeMetadataKey]); value != "" {
		facts.failureCode = ThreadTurnFailureCode(value)
	}
	if entry.Type == sessiontree.EntryCancelRequested && !facts.cancellationSeen {
		facts.cancellationSeen = true
		facts.cancellation = threadCancellationForTurn([]sessiontree.Entry{entry}, identity.TurnID(entry.TurnID))
	}
	if entry.Type == sessiontree.EntryTurnMarker && entry.TurnStatus == sessiontree.TurnStarted && !facts.kindSeen {
		facts.kindSeen = true
		facts.kind = TurnKind(entry.Metadata[sessiontree.TurnKindMetadataKey])
	}
}
func (projection *threadLifecycleProjection) snapshot() (identity.TurnID, identity.RunID, ThreadActivity, *TurnOutcome, *ThreadTurnFailure) {
	lifecycle := projection.lifecycle.Snapshot(sessionlifecycle.PhaseTurn)
	turnID := identity.TurnID(lifecycle.LatestTurnID())
	facts := projection.turns[turnID.String()]
	if facts == nil {
		facts = &threadLifecycleFacts{}
	}
	activity := ThreadActivityIdle
	var outcome *TurnOutcome
	switch lifecycle.Status() {
	case sessionlifecycle.StatusRunning, sessionlifecycle.StatusWaiting:
		activity = ThreadActivityActive
	case sessionlifecycle.StatusCompleted:
		value := TurnOutcomeCompleted
		outcome = &value
	case sessionlifecycle.StatusFailed:
		value := TurnOutcomeFailed
		outcome = &value
	case sessionlifecycle.StatusInterrupted:
		value := TurnOutcomeInterrupted
		outcome = &value
	case sessionlifecycle.StatusCancelled:
		value := TurnOutcomeCancelled
		outcome = &value
	}
	return turnID, facts.runID, activity, outcome, facts.failure(outcome)
}

func (facts *threadLifecycleFacts) hasFailure(outcome *TurnOutcome) bool {
	return facts != nil && facts.failureMessage != "" && outcome != nil && (*outcome == TurnOutcomeFailed || *outcome == TurnOutcomeInterrupted)
}

func (facts *threadLifecycleFacts) failure(outcome *TurnOutcome) *ThreadTurnFailure {
	var failure *ThreadTurnFailure
	if facts.hasFailure(outcome) {
		code := facts.failureCode
		if !code.Valid() {
			code = ThreadTurnFailureLegacyUnclassified
		}
		failure = &ThreadTurnFailure{Code: code, Message: facts.failureMessage, Reason: classifyThreadTurnFailureReason(code, facts.failureMessage), ToolRepetition: threadToolRepetitionDetails(code, facts.failureMessage)}
		if code == ThreadTurnFailureProvider {
			failure.ProviderTimeout = providerattempt.CloneTimeoutDetails(facts.timeout)
		}
	}
	return failure
}

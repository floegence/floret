package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/floegence/floret/internal/session"
)

const (
	InterruptedTurnRecoveryKindKey                 = "authority_kind"
	InterruptedTurnRecoveryKind                    = "interrupted_turn_recovery"
	InterruptedTurnRecoveryFingerprintKey          = "authority_fingerprint"
	InterruptedTurnRecoveryParentKey               = "authority_parent_thread_id"
	InterruptedTurnRecoveryApprovalRootKey         = "authority_approval_root_thread_id"
	InterruptedTurnRecoveryApprovalGenerationKey   = "authority_approval_queue_generation"
	InterruptedTurnRecoveryApprovalRevisionKey     = "authority_approval_queue_revision"
	InterruptedTurnRecoverySourceFailureEntryKey   = "authority_source_failure_entry_id"
	InterruptedTurnRecoverySourceFailureRawHashKey = "authority_source_failure_raw_hash"
	InterruptedTurnFailureMessage                  = "turn interrupted during previous process"
	InterruptedTurnEffectOutcomeUnknownMessage     = "effect outcome is unknown because the turn was interrupted after dispatch"
	BranchBoundaryTurnFailureMessage               = "turn interrupted by branch boundary"
	interruptedTurnRecoveryCancelledEffectPrefix   = "turn-recovery-cancelled:"
	interruptedTurnRecoveryUnknownEffectPrefix     = "turn-recovery-unknown:"
)

type InterruptedTurnApprovalQueueProof struct {
	RootThreadID string
	Generation   int64
	Revision     int64
}

func AddInterruptedTurnApprovalQueueProof(metadata map[string]string, proof *InterruptedTurnApprovalQueueProof) error {
	if proof == nil {
		return nil
	}
	if strings.TrimSpace(proof.RootThreadID) == "" || proof.Generation <= 0 || proof.Revision <= 0 {
		return ErrAuthorityCorrupt
	}
	metadata[InterruptedTurnRecoveryApprovalRootKey] = proof.RootThreadID
	metadata[InterruptedTurnRecoveryApprovalGenerationKey] = strconv.FormatInt(proof.Generation, 10)
	metadata[InterruptedTurnRecoveryApprovalRevisionKey] = strconv.FormatInt(proof.Revision, 10)
	return nil
}

func InterruptedTurnApprovalQueueProofFromMetadata(metadata map[string]string) (*InterruptedTurnApprovalQueueProof, error) {
	rootID := strings.TrimSpace(metadata[InterruptedTurnRecoveryApprovalRootKey])
	generationRaw := strings.TrimSpace(metadata[InterruptedTurnRecoveryApprovalGenerationKey])
	revisionRaw := strings.TrimSpace(metadata[InterruptedTurnRecoveryApprovalRevisionKey])
	if rootID == "" && generationRaw == "" && revisionRaw == "" {
		return nil, nil
	}
	if rootID == "" || generationRaw == "" || revisionRaw == "" {
		return nil, ErrAuthorityCorrupt
	}
	generation, err := strconv.ParseInt(generationRaw, 10, 64)
	if err != nil || generation <= 0 {
		return nil, ErrAuthorityCorrupt
	}
	revision, err := strconv.ParseInt(revisionRaw, 10, 64)
	if err != nil || revision <= 0 {
		return nil, ErrAuthorityCorrupt
	}
	return &InterruptedTurnApprovalQueueProof{RootThreadID: rootID, Generation: generation, Revision: revision}, nil
}

type RecoverInterruptedTurnRequest struct {
	ExpectedLease  TurnLease
	ParentThreadID string
	Now            time.Time
}

type RecoverInterruptedTurnResult struct {
	RunID              string
	Status             TurnMarkerStatus
	OutcomeFingerprint string
	Failure            *Entry
	ToolResults        []Entry
	Terminal           Entry
	Generation         int64
	Replayed           bool
}

type InterruptedTurnRecoveryPlan struct {
	RunID              string
	Status             TurnMarkerStatus
	FailureCode        string
	FailureMessage     string
	SourceFailure      *InterruptedTurnRecoveryFailureProof
	OutcomeFingerprint string
	TerminalEntryID    string
	Effects            []InterruptedTurnRecoveryEffect
}

type InterruptedTurnRecoveryFailureProof struct {
	EntryID string `json:"entry_id"`
	Message string `json:"message"`
	RawHash string `json:"raw_hash"`
}

func AddInterruptedTurnRecoverySourceFailureProof(metadata map[string]string, proof *InterruptedTurnRecoveryFailureProof) error {
	if proof == nil {
		return nil
	}
	if err := validateInterruptedTurnRecoveryFailureProof(*proof); err != nil {
		return err
	}
	metadata[InterruptedTurnRecoverySourceFailureEntryKey] = proof.EntryID
	metadata[InterruptedTurnRecoverySourceFailureRawHashKey] = proof.RawHash
	return nil
}

func InterruptedTurnRecoverySourceFailureProofFromMetadata(metadata map[string]string) (*InterruptedTurnRecoveryFailureProof, error) {
	entryValue := metadata[InterruptedTurnRecoverySourceFailureEntryKey]
	rawHashValue := metadata[InterruptedTurnRecoverySourceFailureRawHashKey]
	if entryValue == "" && rawHashValue == "" {
		return nil, nil
	}
	entryID := strings.TrimSpace(entryValue)
	rawHash := strings.TrimSpace(rawHashValue)
	if entryID == "" || rawHash == "" || entryValue != entryID || rawHashValue != rawHash {
		return nil, ErrAuthorityCorrupt
	}
	return &InterruptedTurnRecoveryFailureProof{EntryID: entryID, RawHash: rawHash}, nil
}

type InterruptedTurnRecoveryEffect struct {
	EffectAttemptID string             `json:"effect_attempt_id"`
	ToolCallID      string             `json:"tool_call_id"`
	State           EffectAttemptState `json:"state"`
}

type InterruptedTurnRecoveryRepo interface {
	RecoverInterruptedTurn(context.Context, RecoverInterruptedTurnRequest) (RecoverInterruptedTurnResult, error)
}

type InterruptedTurnResolutionValidationRepo interface {
	ValidateInterruptedTurnResolution(context.Context, RecoverInterruptedTurnRequest) error
}

func ValidateThreadAuthorityState(path []Entry, lease *TurnLease, claimOperationID string) error {
	unfinished, err := unfinishedTurnIDs(path)
	if err != nil {
		return err
	}
	if strings.TrimSpace(claimOperationID) != "" && lease != nil {
		return ErrAuthorityCorrupt
	}
	if lease == nil {
		if len(unfinished) != 0 {
			return ErrAuthorityCorrupt
		}
		return nil
	}
	if err := lease.Validate(); err != nil {
		return ErrAuthorityCorrupt
	}
	if lease.Purpose == TurnLeasePurposeMutation {
		if len(unfinished) != 0 {
			return ErrAuthorityCorrupt
		}
		return nil
	}
	if len(unfinished) != 1 || unfinished[0] != lease.TurnID {
		return ErrAuthorityCorrupt
	}
	return nil
}

func ValidateThreadAuthoritySnapshot(meta ThreadMeta, path []Entry, lease *TurnLease, claimOperationID string, leaseGeneration int64) error {
	if err := ValidateThreadMetaAuthority(meta); err != nil {
		return ErrAuthorityCorrupt
	}
	if leaseGeneration < 0 {
		return ErrAuthorityCorrupt
	}
	if lease != nil && lease.Generation != leaseGeneration {
		return ErrAuthorityCorrupt
	}
	if err := ValidateThreadAuthorityState(path, lease, claimOperationID); err != nil {
		return err
	}
	lifecycle, err := normalizeThreadLifecycle(meta)
	if err != nil {
		return ErrAuthorityCorrupt
	}
	switch lifecycle {
	case ThreadLifecycleClosed:
		if lease != nil {
			return ErrAuthorityCorrupt
		}
	case ThreadLifecycleClosing:
		if lease != nil && lease.Purpose != TurnLeasePurposeTurn {
			return ErrAuthorityCorrupt
		}
	}
	return nil
}

func ValidateRecoverInterruptedTurnRequest(req RecoverInterruptedTurnRequest) error {
	if err := req.ExpectedLease.Validate(); err != nil {
		return err
	}
	if req.ExpectedLease.Purpose != TurnLeasePurposeTurn {
		return errors.New("interrupted turn recovery requires a turn lease")
	}
	return nil
}

// ValidateInterruptedTurnLeaseSuccessor verifies that current is a canonical
// monotonic successor of the exact recovery target proof.
func ValidateInterruptedTurnLeaseSuccessor(target, current TurnLease) error {
	if err := target.Validate(); err != nil {
		return ErrAuthorityCorrupt
	}
	if err := current.Validate(); err != nil {
		return ErrAuthorityCorrupt
	}
	if target.Purpose != TurnLeasePurposeTurn || current.ThreadID != target.ThreadID {
		return ErrAuthorityCorrupt
	}
	switch {
	case current.Generation < target.Generation:
		return ErrAuthorityCorrupt
	case current.Generation > target.Generation:
		return nil
	}
	if current.Purpose != target.Purpose || current.TurnID != target.TurnID ||
		current.MutationID != target.MutationID || current.MutationKind != target.MutationKind ||
		current.OwnerID != target.OwnerID || !current.AcquiredAt.Equal(target.AcquiredAt) {
		return ErrAuthorityCorrupt
	}
	switch {
	case current.Heartbeat < target.Heartbeat:
		return ErrAuthorityCorrupt
	case current.Heartbeat == target.Heartbeat:
		if !SameTurnLease(current, target) {
			return ErrAuthorityCorrupt
		}
	case current.RenewedAt.Before(target.RenewedAt), current.ExpiresAt.Before(target.ExpiresAt):
		return ErrAuthorityCorrupt
	}
	return nil
}

func DeriveInterruptedTurnRecoveryPlan(path []Entry, expectedLease TurnLease, parentThreadID string, effects []InterruptedTurnRecoveryEffect) (InterruptedTurnRecoveryPlan, error) {
	runID := interruptedTurnRecoveryRunID(path, expectedLease.TurnID)
	if runID == "" {
		return InterruptedTurnRecoveryPlan{}, ErrAuthorityCorrupt
	}
	info, err := interruptedTurnRecoveryInfoForTurn(path, expectedLease.TurnID)
	if err != nil {
		return InterruptedTurnRecoveryPlan{}, err
	}
	if !info.Started || info.Terminal {
		return InterruptedTurnRecoveryPlan{}, ErrAuthorityCorrupt
	}
	normalizedEffects, hasUnknownOutcome, err := normalizeInterruptedTurnRecoveryEffects(effects)
	if err != nil {
		return InterruptedTurnRecoveryPlan{}, err
	}
	status := TurnAborted
	failureCode := TurnFailureInterrupted
	failureMessage := InterruptedTurnFailureMessage
	var sourceFailure *InterruptedTurnRecoveryFailureProof
	if info.RunFailure {
		failureCode = info.RunFailureCode
		if failureCode == "" {
			failureCode = TurnFailureLegacyUnclassified
		}
		if !ValidTurnFailureCode(failureCode) {
			return InterruptedTurnRecoveryPlan{}, ErrAuthorityCorrupt
		}
		failureMessage = ""
		sourceFailure, err = interruptedTurnRecoveryFailureProofForEntry(info.RunFailureEntry, expectedLease.TurnID)
		if err != nil {
			return InterruptedTurnRecoveryPlan{}, err
		}
		switch failureCode {
		case TurnFailureCancelled, TurnFailureInterrupted:
			status = TurnAborted
		default:
			status = TurnFailed
		}
	}
	if hasUnknownOutcome {
		status = TurnFailed
		failureCode = TurnFailureEffectOutcomeUnknown
		failureMessage = InterruptedTurnEffectOutcomeUnknownMessage
	}
	fingerprint, err := InterruptedTurnRecoveryFingerprint(expectedLease, parentThreadID, runID, status, failureCode, failureMessage, sourceFailure, normalizedEffects)
	if err != nil {
		return InterruptedTurnRecoveryPlan{}, err
	}
	return InterruptedTurnRecoveryPlan{
		RunID: runID, Status: status, FailureCode: failureCode, FailureMessage: failureMessage, SourceFailure: sourceFailure,
		OutcomeFingerprint: fingerprint, TerminalEntryID: "recovery-terminal-" + fingerprint[:24], Effects: normalizedEffects,
	}, nil
}

func InterruptedTurnRecoveryFingerprint(
	expectedLease TurnLease,
	parentThreadID string,
	runID string,
	status TurnMarkerStatus,
	failureCode string,
	failureMessage string,
	sourceFailure *InterruptedTurnRecoveryFailureProof,
	effects []InterruptedTurnRecoveryEffect,
) (string, error) {
	if err := expectedLease.Validate(); err != nil {
		return "", err
	}
	failureCode = strings.TrimSpace(failureCode)
	if !ValidTurnFailureCode(failureCode) {
		return "", ErrAuthorityCorrupt
	}
	switch status {
	case TurnAborted:
		if failureCode != TurnFailureCancelled && failureCode != TurnFailureInterrupted {
			return "", ErrAuthorityCorrupt
		}
	case TurnFailed:
		if failureCode == TurnFailureCancelled || failureCode == TurnFailureInterrupted {
			return "", ErrAuthorityCorrupt
		}
	default:
		return "", ErrAuthorityCorrupt
	}
	if sourceFailure != nil {
		if err := validateInterruptedTurnRecoveryFailureProof(*sourceFailure); err != nil {
			return "", err
		}
	}
	normalizedEffects, _, err := normalizeInterruptedTurnRecoveryEffects(effects)
	if err != nil {
		return "", err
	}
	payload, err := json.Marshal(struct {
		ExpectedLease  TurnLease                            `json:"expected_lease"`
		ParentThreadID string                               `json:"parent_thread_id,omitempty"`
		RunID          string                               `json:"run_id"`
		Status         TurnMarkerStatus                     `json:"status"`
		FailureCode    string                               `json:"failure_code"`
		FailureMessage string                               `json:"failure_message,omitempty"`
		SourceFailure  *InterruptedTurnRecoveryFailureProof `json:"source_failure,omitempty"`
		Effects        []InterruptedTurnRecoveryEffect      `json:"effects,omitempty"`
	}{
		ExpectedLease:  expectedLease,
		ParentThreadID: strings.TrimSpace(parentThreadID),
		RunID:          strings.TrimSpace(runID),
		Status:         status,
		FailureCode:    failureCode,
		FailureMessage: strings.TrimSpace(failureMessage),
		SourceFailure:  sourceFailure,
		Effects:        normalizedEffects,
	})
	if err != nil {
		return "", err
	}
	return StableHash(string(payload)), nil
}

func normalizeInterruptedTurnRecoveryEffects(effects []InterruptedTurnRecoveryEffect) ([]InterruptedTurnRecoveryEffect, bool, error) {
	normalized := append([]InterruptedTurnRecoveryEffect(nil), effects...)
	for index := range normalized {
		normalized[index].EffectAttemptID = strings.TrimSpace(normalized[index].EffectAttemptID)
		normalized[index].ToolCallID = strings.TrimSpace(normalized[index].ToolCallID)
	}
	sort.Slice(normalized, func(i, j int) bool {
		if normalized[i].EffectAttemptID == normalized[j].EffectAttemptID {
			return normalized[i].ToolCallID < normalized[j].ToolCallID
		}
		return normalized[i].EffectAttemptID < normalized[j].EffectAttemptID
	})
	seenAttempts := make(map[string]struct{}, len(normalized))
	seenCalls := make(map[string]struct{}, len(normalized))
	hasUnknownOutcome := false
	for index := range normalized {
		effect := &normalized[index]
		if effect.EffectAttemptID == "" || effect.ToolCallID == "" || !validEffectAttemptState(effect.State) {
			return nil, false, ErrAuthorityCorrupt
		}
		if _, exists := seenAttempts[effect.EffectAttemptID]; exists {
			return nil, false, ErrAuthorityCorrupt
		}
		if _, exists := seenCalls[effect.ToolCallID]; exists {
			return nil, false, ErrAuthorityCorrupt
		}
		seenAttempts[effect.EffectAttemptID] = struct{}{}
		seenCalls[effect.ToolCallID] = struct{}{}
		hasUnknownOutcome = hasUnknownOutcome || effect.State == EffectAttemptDispatching || effect.State == EffectAttemptUnknown
	}
	return normalized, hasUnknownOutcome, nil
}

func validEffectAttemptState(state EffectAttemptState) bool {
	switch state {
	case EffectAttemptPrepared, EffectAttemptDispatching, EffectAttemptCompleted, EffectAttemptFailed,
		EffectAttemptRejected, EffectAttemptUnknown, EffectAttemptCancelled:
		return true
	default:
		return false
	}
}

func InterruptedTurnRecoveryEffects(attempts []EffectAttempt, committedFingerprint string) ([]InterruptedTurnRecoveryEffect, error) {
	effects := make([]InterruptedTurnRecoveryEffect, 0, len(attempts))
	for _, attempt := range attempts {
		state := attempt.State
		terminalFingerprint := strings.TrimSpace(attempt.TerminalFingerprint)
		if committedFingerprint != "" {
			switch {
			case strings.HasPrefix(terminalFingerprint, interruptedTurnRecoveryCancelledEffectPrefix):
				if terminalFingerprint != InterruptedTurnRecoveryCancelledEffectFingerprint(committedFingerprint) || state != EffectAttemptCancelled {
					return nil, ErrAuthorityCorrupt
				}
				state = EffectAttemptPrepared
			case strings.HasPrefix(terminalFingerprint, interruptedTurnRecoveryUnknownEffectPrefix):
				if terminalFingerprint != InterruptedTurnRecoveryUnknownEffectFingerprint(committedFingerprint) || state != EffectAttemptUnknown {
					return nil, ErrAuthorityCorrupt
				}
				state = EffectAttemptDispatching
			}
		}
		effects = append(effects, InterruptedTurnRecoveryEffect{
			EffectAttemptID: attempt.EffectAttemptID,
			ToolCallID:      attempt.Invocation.ToolCallID,
			State:           state,
		})
	}
	normalized, _, err := normalizeInterruptedTurnRecoveryEffects(effects)
	return normalized, err
}

func ValidateInterruptedTurnRecoveryEffectAttempts(attempts []EffectAttempt, threadID, turnID, runID string) error {
	threadID = strings.TrimSpace(threadID)
	turnID = strings.TrimSpace(turnID)
	runID = strings.TrimSpace(runID)
	if threadID == "" || turnID == "" || runID == "" {
		return ErrAuthorityCorrupt
	}
	for _, attempt := range attempts {
		if strings.TrimSpace(attempt.Invocation.ThreadID) != threadID ||
			strings.TrimSpace(attempt.Invocation.TurnID) != turnID ||
			strings.TrimSpace(attempt.Invocation.RunID) != runID {
			return ErrAuthorityCorrupt
		}
	}
	return nil
}

func InterruptedTurnRecoveryCancelledEffectFingerprint(recoveryFingerprint string) string {
	return interruptedTurnRecoveryCancelledEffectPrefix + strings.TrimSpace(recoveryFingerprint)
}

func InterruptedTurnRecoveryUnknownEffectFingerprint(recoveryFingerprint string) string {
	return interruptedTurnRecoveryUnknownEffectPrefix + strings.TrimSpace(recoveryFingerprint)
}

type interruptedTurnRecoveryInfo struct {
	Started         bool
	Terminal        bool
	RunFailure      bool
	RunFailureCode  string
	RunFailureEntry Entry
}

func interruptedTurnRecoveryRunID(path []Entry, turnID string) string {
	for _, entry := range path {
		if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted && entry.TurnID == turnID {
			return strings.TrimSpace(entry.Metadata["run_id"])
		}
	}
	return ""
}

func interruptedTurnRecoveryInfoForTurn(path []Entry, turnID string) (interruptedTurnRecoveryInfo, error) {
	var info interruptedTurnRecoveryInfo
	for _, entry := range path {
		if entry.TurnID != turnID {
			continue
		}
		switch entry.Type {
		case EntryRunFailure:
			if info.RunFailure {
				return interruptedTurnRecoveryInfo{}, ErrAuthorityCorrupt
			}
			info.RunFailure = true
			info.RunFailureEntry = cloneEntry(entry)
			info.RunFailureCode = strings.TrimSpace(entry.Metadata[TurnFailureCodeMetadataKey])
			if info.RunFailureCode != "" && !ValidTurnFailureCode(info.RunFailureCode) {
				return interruptedTurnRecoveryInfo{}, ErrAuthorityCorrupt
			}
		case EntryTurnMarker:
			if entry.TurnStatus == TurnStarted {
				info.Started = true
			}
			if terminalTurnMarker(entry.TurnStatus) {
				info.Terminal = true
			}
		}
	}
	return info, nil
}

func interruptedTurnRecoveryFailureProofForEntry(entry Entry, turnID string) (*InterruptedTurnRecoveryFailureProof, error) {
	if entry.Type != EntryRunFailure || entry.TurnID != strings.TrimSpace(turnID) || ValidateEntryIntegrity(entry) != nil {
		return nil, ErrAuthorityCorrupt
	}
	proof := InterruptedTurnRecoveryFailureProof{
		EntryID: entry.ID,
		Message: entry.Error,
		RawHash: entry.RawHash,
	}
	if err := validateInterruptedTurnRecoveryFailureProof(proof); err != nil {
		return nil, err
	}
	return &proof, nil
}

func validateInterruptedTurnRecoveryFailureProof(proof InterruptedTurnRecoveryFailureProof) error {
	if strings.TrimSpace(proof.EntryID) == "" || proof.EntryID != strings.TrimSpace(proof.EntryID) ||
		strings.TrimSpace(proof.Message) == "" || strings.TrimSpace(proof.RawHash) == "" || proof.RawHash != strings.TrimSpace(proof.RawHash) {
		return ErrAuthorityCorrupt
	}
	return nil
}

func terminalTurnMarker(status TurnMarkerStatus) bool {
	switch status {
	case TurnCompleted, TurnWaiting, TurnFailed, TurnAborted:
		return true
	default:
		return false
	}
}

func (r *MemoryRepo) RecoverInterruptedTurn(_ context.Context, req RecoverInterruptedTurnRequest) (RecoverInterruptedTurnResult, error) {
	if err := ValidateRecoverInterruptedTurnRequest(req); err != nil {
		return RecoverInterruptedTurnResult{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	threadID := strings.TrimSpace(req.ExpectedLease.ThreadID)
	turnID := strings.TrimSpace(req.ExpectedLease.TurnID)
	key := turnAdmissionKey(threadID, turnID)
	if finished, ok := r.turnFinishes[key]; ok {
		resolution, err := r.validateInterruptedTurnResolutionLocked(req, finished)
		if err != nil {
			return RecoverInterruptedTurnResult{}, err
		}
		if resolution.recoveryReplay && resolution.exactProof {
			return resolution.result, nil
		}
		return RecoverInterruptedTurnResult{}, ErrRecoveryTargetResolved
	}
	meta, ok := r.threads[threadID]
	if !ok {
		if _, deleted := r.tombstones[threadID]; deleted {
			return RecoverInterruptedTurnResult{}, ErrThreadDeleted
		}
		return RecoverInterruptedTurnResult{}, ErrThreadNotFound
	}
	if err := validateInterruptedTurnRecoveryMeta(meta, req.ParentThreadID); err != nil {
		return RecoverInterruptedTurnResult{}, err
	}
	if parentThreadID := strings.TrimSpace(req.ParentThreadID); parentThreadID != "" {
		parent, ok := r.threads[parentThreadID]
		if !ok {
			if _, deleted := r.tombstones[parentThreadID]; deleted {
				return RecoverInterruptedTurnResult{}, ErrThreadDeleted
			}
			return RecoverInterruptedTurnResult{}, ErrThreadNotFound
		}
		if err := lifecycleRejectsWrite(parent); err != nil {
			return RecoverInterruptedTurnResult{}, err
		}
	}
	if r.threadAuthorityClaimedLocked(threadID) {
		return RecoverInterruptedTurnResult{}, ErrAuthorityCorrupt
	}
	active, ok := r.leases[threadID]
	if !ok || !SameTurnLease(active, req.ExpectedLease) {
		return RecoverInterruptedTurnResult{}, ErrStaleAuthority
	}
	if !active.TakeoverEligible(r.now().UTC(), r.leasePolicy) {
		return RecoverInterruptedTurnResult{}, ErrThreadAuthorityBusy
	}
	admission, ok := r.turnAdmissions[key]
	if !ok || validateInterruptedTurnAdmission(admission, active) != nil {
		return RecoverInterruptedTurnResult{}, ErrAuthorityCorrupt
	}
	path, err := pathLocked(r.threads, r.entries, threadID, meta.LeafID)
	if err != nil {
		return RecoverInterruptedTurnResult{}, err
	}
	if err := ValidateInterruptedTurnAdmissionPath(path, admission.ThreadID, turnID, admission.RunID, admission.TurnStartedID); err != nil {
		return RecoverInterruptedTurnResult{}, err
	}
	turnAttempts := make([]EffectAttempt, 0)
	for _, attempt := range r.effectAttempts {
		if attempt.Invocation.ThreadID == threadID && attempt.Invocation.TurnID == turnID {
			turnAttempts = append(turnAttempts, attempt)
		}
	}
	if err := ValidateInterruptedTurnRecoveryEffectAttempts(turnAttempts, threadID, turnID, admission.RunID); err != nil {
		return RecoverInterruptedTurnResult{}, err
	}
	effects, err := InterruptedTurnRecoveryEffects(turnAttempts, "")
	if err != nil {
		return RecoverInterruptedTurnResult{}, err
	}
	plan, err := DeriveInterruptedTurnRecoveryPlan(path, req.ExpectedLease, req.ParentThreadID, effects)
	if err != nil {
		return RecoverInterruptedTurnResult{}, err
	}
	terminalEntryID := plan.TerminalEntryID
	if containsEntry(r.entries[threadID], terminalEntryID) {
		return RecoverInterruptedTurnResult{}, ErrRequestConflict
	}
	unresolvedCalls := UnresolvedInterruptedTurnCalls(path, turnID)
	effectStates := make(map[string]EffectAttemptState, len(plan.Effects))
	for _, effect := range plan.Effects {
		effectStates[effect.ToolCallID] = effect.State
	}
	generatedCount := len(unresolvedCalls)
	if strings.TrimSpace(plan.FailureMessage) != "" {
		generatedCount++
	}
	generatedIDs, nextSequence := r.previewNextEntryIDsLocked(threadID, generatedCount)
	for _, id := range generatedIDs {
		if id == terminalEntryID {
			return RecoverInterruptedTurnResult{}, ErrRequestConflict
		}
	}
	now := nonZeroAuthorityTime(req.Now, r.now)
	if r.leaseGeneration[threadID] != active.Generation {
		return RecoverInterruptedTurnResult{}, ErrAuthorityCorrupt
	}
	generation := active.Generation + 1
	approvalQueueProof, err := r.cancelInterruptedTurnApprovalsLocked(active, admission.RunID, plan.OutcomeFingerprint, now)
	if err != nil {
		return RecoverInterruptedTurnResult{}, err
	}
	r.seq = nextSequence
	r.leaseGeneration[threadID] = generation
	delete(r.leases, threadID)
	for attemptID, attempt := range r.effectAttempts {
		if attempt.Invocation.ThreadID != threadID || attempt.Invocation.TurnID != turnID {
			continue
		}
		switch attempt.State {
		case EffectAttemptPrepared:
			attempt.State = EffectAttemptCancelled
			attempt.TerminalFingerprint = InterruptedTurnRecoveryCancelledEffectFingerprint(plan.OutcomeFingerprint)
		case EffectAttemptDispatching:
			attempt.State = EffectAttemptUnknown
			attempt.TerminalFingerprint = InterruptedTurnRecoveryUnknownEffectFingerprint(plan.OutcomeFingerprint)
		}
		attempt.UpdatedAt = now
		r.effectAttempts[attemptID] = attempt
	}
	parentID := meta.LeafID
	result := RecoverInterruptedTurnResult{
		RunID: plan.RunID, Status: plan.Status, OutcomeFingerprint: plan.OutcomeFingerprint, Generation: generation,
	}
	generatedIndex := 0
	for _, call := range unresolvedCalls {
		entry := Entry{
			ID: generatedIDs[generatedIndex], ThreadID: threadID, ParentID: parentID, Type: EntryToolResult, TurnID: turnID, CreatedAt: now,
			Message: InterruptedTurnToolResult(call.Message, effectStates[call.Message.ToolCallID]),
		}
		generatedIndex++
		entry.Raw = rawForEntry(entry)
		entry.RawHash = stableHash(entry.Raw)
		r.appendIndexedEntriesLocked(threadID, entry)
		result.ToolResults = append(result.ToolResults, cloneEntry(entry))
		parentID = entry.ID
	}
	if message := strings.TrimSpace(plan.FailureMessage); message != "" {
		failure := Entry{
			ID: generatedIDs[generatedIndex], ThreadID: threadID, ParentID: parentID, Type: EntryRunFailure,
			TurnID: turnID, Error: message, CreatedAt: now,
		}
		failure.Raw = rawForEntry(failure)
		failure.RawHash = stableHash(failure.Raw)
		r.appendIndexedEntriesLocked(threadID, failure)
		result.Failure = &failure
		parentID = failure.ID
	}
	metadata := map[string]string{
		"recoverable":                    "true",
		TurnFailureCodeMetadataKey:       plan.FailureCode,
		InterruptedTurnRecoveryParentKey: strings.TrimSpace(req.ParentThreadID),
	}
	metadata[InterruptedTurnRecoveryKindKey] = InterruptedTurnRecoveryKind
	metadata[InterruptedTurnRecoveryFingerprintKey] = plan.OutcomeFingerprint
	if err := AddInterruptedTurnRecoverySourceFailureProof(metadata, plan.SourceFailure); err != nil {
		return RecoverInterruptedTurnResult{}, err
	}
	if err := AddInterruptedTurnApprovalQueueProof(metadata, approvalQueueProof); err != nil {
		return RecoverInterruptedTurnResult{}, err
	}
	terminal := Entry{
		ID: terminalEntryID, ThreadID: threadID, ParentID: parentID, Type: EntryTurnMarker,
		TurnID: turnID, TurnStatus: plan.Status, Metadata: metadata, CreatedAt: now,
	}
	terminal.Raw = rawForEntry(terminal)
	terminal.RawHash = stableHash(terminal.Raw)
	r.appendIndexedEntriesLocked(threadID, terminal)
	meta.LeafID = terminal.ID
	meta.UpdatedAt = now
	r.threads[threadID] = meta
	delete(r.providerStates, threadID)
	failureID := ""
	if result.Failure != nil {
		failureID = result.Failure.ID
	}
	r.turnFinishes[key] = turnFinishLedger{
		ThreadID: threadID, TurnID: turnID, RunID: plan.RunID, Generation: generation,
		OutcomeFingerprint: plan.OutcomeFingerprint, FailureEntryID: failureID, TerminalEntryID: terminal.ID,
	}
	result.Terminal = cloneEntry(terminal)
	return result, nil
}

func (r *MemoryRepo) ValidateInterruptedTurnResolution(_ context.Context, req RecoverInterruptedTurnRequest) error {
	if err := ValidateRecoverInterruptedTurnRequest(req); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	threadID := strings.TrimSpace(req.ExpectedLease.ThreadID)
	if _, ok := r.threads[threadID]; !ok {
		if _, deleted := r.tombstones[threadID]; deleted {
			return ErrThreadDeleted
		}
		return ErrAuthorityCorrupt
	}
	finished, ok := r.turnFinishes[turnAdmissionKey(req.ExpectedLease.ThreadID, req.ExpectedLease.TurnID)]
	if !ok {
		return ErrAuthorityCorrupt
	}
	_, err := r.validateInterruptedTurnResolutionLocked(req, finished)
	return err
}

type interruptedTurnResolution struct {
	result         RecoverInterruptedTurnResult
	recoveryReplay bool
	exactProof     bool
}

func (r *MemoryRepo) validateInterruptedTurnResolutionLocked(
	req RecoverInterruptedTurnRequest,
	finished turnFinishLedger,
) (interruptedTurnResolution, error) {
	expectedLease := req.ExpectedLease
	threadID := strings.TrimSpace(expectedLease.ThreadID)
	meta, ok := r.threads[threadID]
	if !ok {
		if _, deleted := r.tombstones[threadID]; deleted {
			return interruptedTurnResolution{}, ErrThreadDeleted
		}
		return interruptedTurnResolution{}, ErrAuthorityCorrupt
	}
	if ValidateThreadMetaAuthority(meta) != nil {
		return interruptedTurnResolution{}, ErrAuthorityCorrupt
	}
	parentThreadID := strings.TrimSpace(req.ParentThreadID)
	if strings.TrimSpace(meta.ParentThreadID) != parentThreadID {
		return interruptedTurnResolution{}, ErrInvalidThreadAuthority
	}
	if parentThreadID != "" {
		parent, ok := r.threads[parentThreadID]
		if !ok || ValidateThreadMetaAuthority(parent) != nil {
			return interruptedTurnResolution{}, ErrAuthorityCorrupt
		}
	}
	leaseGeneration := r.leaseGeneration[threadID]
	var active *TurnLease
	if lease, ok := r.leases[threadID]; ok {
		copy := lease
		active = &copy
	}
	path, err := pathLocked(r.threads, r.entries, threadID, meta.LeafID)
	if err != nil {
		return interruptedTurnResolution{}, err
	}
	if err := ValidateThreadAuthoritySnapshot(meta, path, active, r.authorityClaims[threadID], leaseGeneration); err != nil {
		return interruptedTurnResolution{}, err
	}
	if leaseGeneration < expectedLease.Generation {
		return interruptedTurnResolution{}, ErrAuthorityCorrupt
	}
	if active != nil {
		if ValidateInterruptedTurnLeaseSuccessor(expectedLease, *active) != nil || active.Generation == expectedLease.Generation {
			return interruptedTurnResolution{}, ErrAuthorityCorrupt
		}
	}
	key := turnAdmissionKey(expectedLease.ThreadID, expectedLease.TurnID)
	admission, ok := r.turnAdmissions[key]
	if !ok {
		return interruptedTurnResolution{}, ErrAuthorityCorrupt
	}
	if err := validateInterruptedTurnAdmissionSuccessor(admission, expectedLease); err != nil {
		return interruptedTurnResolution{}, err
	}
	started, ok := findEntry(r.entries[admission.ThreadID], admission.TurnStartedID)
	if !ok || ValidateInterruptedTurnStartedEntry(started, admission.ThreadID, admission.TurnID, admission.RunID, admission.TurnStartedID) != nil {
		return interruptedTurnResolution{}, ErrAuthorityCorrupt
	}
	if leaseGeneration < finished.Generation || (active != nil && active.Generation <= finished.Generation) {
		return interruptedTurnResolution{}, ErrAuthorityCorrupt
	}
	switch finished.Generation {
	case admission.Lease.Generation:
		if err := r.validateNormalInterruptedTurnFinishLocked(finished, admission); err != nil {
			return interruptedTurnResolution{}, err
		}
		return interruptedTurnResolution{}, nil
	case admission.Lease.Generation + 1:
		result, err := r.replayedInterruptedTurnLocked(finished, admission.Lease, parentThreadID, path)
		if err != nil {
			if errors.Is(err, ErrRequestConflict) {
				return interruptedTurnResolution{}, ErrAuthorityCorrupt
			}
			return interruptedTurnResolution{}, err
		}
		return interruptedTurnResolution{
			result: result, recoveryReplay: true, exactProof: SameTurnLease(admission.Lease, expectedLease),
		}, nil
	default:
		return interruptedTurnResolution{}, ErrAuthorityCorrupt
	}
}

func validateInterruptedTurnAdmission(admission turnAdmissionLedger, active TurnLease) error {
	if admission.ThreadID != active.ThreadID || admission.TurnID != active.TurnID ||
		strings.TrimSpace(admission.RunID) == "" || strings.TrimSpace(admission.RequestFingerprint) == "" ||
		strings.TrimSpace(admission.TurnStartedID) == "" || !SameTurnLease(admission.Lease, active) {
		return ErrAuthorityCorrupt
	}
	return nil
}

func validateInterruptedTurnAdmissionSuccessor(admission turnAdmissionLedger, expectedLease TurnLease) error {
	if admission.ThreadID != expectedLease.ThreadID || admission.TurnID != expectedLease.TurnID ||
		strings.TrimSpace(admission.RunID) == "" || strings.TrimSpace(admission.RequestFingerprint) == "" ||
		strings.TrimSpace(admission.TurnStartedID) == "" {
		return ErrAuthorityCorrupt
	}
	if admission.Lease.Generation != expectedLease.Generation ||
		ValidateInterruptedTurnLeaseSuccessor(expectedLease, admission.Lease) != nil {
		return ErrRequestConflict
	}
	return nil
}

func (r *MemoryRepo) validateNormalInterruptedTurnFinishLocked(finished turnFinishLedger, admission turnAdmissionLedger) error {
	if finished.ThreadID != admission.ThreadID || finished.TurnID != admission.TurnID ||
		finished.RunID != admission.RunID || finished.Generation != admission.Lease.Generation ||
		strings.TrimSpace(finished.OutcomeFingerprint) == "" || strings.TrimSpace(finished.TerminalEntryID) == "" {
		return ErrAuthorityCorrupt
	}
	terminal, ok := findEntry(r.entries[finished.ThreadID], finished.TerminalEntryID)
	if !ok || terminal.Type != EntryTurnMarker || terminal.TurnID != admission.TurnID || !terminalTurnMarker(terminal.TurnStatus) {
		return ErrAuthorityCorrupt
	}
	if finished.FailureEntryID != "" {
		failure, ok := findEntry(r.entries[finished.ThreadID], finished.FailureEntryID)
		if !ok || failure.Type != EntryRunFailure || failure.TurnID != admission.TurnID || terminal.ParentID != failure.ID {
			return ErrAuthorityCorrupt
		}
	}
	return nil
}

func (r *MemoryRepo) replayedInterruptedTurnLocked(
	finished turnFinishLedger,
	expectedLease TurnLease,
	parentThreadID string,
	path []Entry,
) (RecoverInterruptedTurnResult, error) {
	if finished.Generation != expectedLease.Generation+1 {
		return RecoverInterruptedTurnResult{}, ErrRequestConflict
	}
	terminal, ok := findEntry(r.entries[finished.ThreadID], finished.TerminalEntryID)
	if !ok || terminal.Type != EntryTurnMarker || terminal.TurnID != finished.TurnID ||
		!terminalTurnMarker(terminal.TurnStatus) ||
		terminal.Metadata[InterruptedTurnRecoveryKindKey] != InterruptedTurnRecoveryKind ||
		terminal.Metadata[InterruptedTurnRecoveryFingerprintKey] != finished.OutcomeFingerprint {
		return RecoverInterruptedTurnResult{}, ErrAuthorityCorrupt
	}
	if terminal.Metadata[InterruptedTurnRecoveryParentKey] != strings.TrimSpace(parentThreadID) {
		return RecoverInterruptedTurnResult{}, ErrInvalidThreadAuthority
	}
	sourceFailure, err := InterruptedTurnRecoverySourceFailureProofFromMetadata(terminal.Metadata)
	if err != nil {
		return RecoverInterruptedTurnResult{}, err
	}
	if sourceFailure != nil {
		entry, ok := findEntry(path, sourceFailure.EntryID)
		if !ok {
			return RecoverInterruptedTurnResult{}, ErrAuthorityCorrupt
		}
		currentProof, proofErr := interruptedTurnRecoveryFailureProofForEntry(entry, finished.TurnID)
		if proofErr != nil || currentProof.EntryID != sourceFailure.EntryID || currentProof.RawHash != sourceFailure.RawHash {
			return RecoverInterruptedTurnResult{}, ErrAuthorityCorrupt
		}
		sourceFailure = currentProof
	}
	failureMessage := ""
	var failure *Entry
	if finished.FailureEntryID != "" {
		entry, ok := findEntry(r.entries[finished.ThreadID], finished.FailureEntryID)
		if !ok || entry.Type != EntryRunFailure || entry.TurnID != finished.TurnID || terminal.ParentID != entry.ID {
			return RecoverInterruptedTurnResult{}, ErrAuthorityCorrupt
		}
		failureMessage = entry.Error
		failure = &entry
	}
	if sourceFailure == nil && failureMessage == "" {
		return RecoverInterruptedTurnResult{}, ErrAuthorityCorrupt
	}
	turnAttempts := make([]EffectAttempt, 0)
	for _, attempt := range r.effectAttempts {
		if attempt.Invocation.ThreadID == finished.ThreadID && attempt.Invocation.TurnID == finished.TurnID {
			turnAttempts = append(turnAttempts, attempt)
		}
	}
	if err := ValidateInterruptedTurnRecoveryEffectAttempts(turnAttempts, finished.ThreadID, finished.TurnID, finished.RunID); err != nil {
		return RecoverInterruptedTurnResult{}, err
	}
	effects, err := InterruptedTurnRecoveryEffects(turnAttempts, finished.OutcomeFingerprint)
	if err != nil {
		return RecoverInterruptedTurnResult{}, err
	}
	approvalQueueProof, err := InterruptedTurnApprovalQueueProofFromMetadata(terminal.Metadata)
	if err != nil {
		return RecoverInterruptedTurnResult{}, err
	}
	if err := r.validateInterruptedTurnApprovalsLocked(expectedLease, finished.RunID, finished.OutcomeFingerprint, terminal.CreatedAt, approvalQueueProof); err != nil {
		return RecoverInterruptedTurnResult{}, err
	}
	fingerprint, err := InterruptedTurnRecoveryFingerprint(
		expectedLease, parentThreadID, finished.RunID, terminal.TurnStatus,
		terminal.Metadata[TurnFailureCodeMetadataKey], failureMessage, sourceFailure, effects,
	)
	if err != nil {
		return RecoverInterruptedTurnResult{}, err
	}
	if fingerprint != finished.OutcomeFingerprint {
		return RecoverInterruptedTurnResult{}, ErrRequestConflict
	}
	if terminal.ID != "recovery-terminal-"+fingerprint[:24] {
		return RecoverInterruptedTurnResult{}, ErrAuthorityCorrupt
	}
	result := RecoverInterruptedTurnResult{
		RunID: finished.RunID, Status: terminal.TurnStatus, OutcomeFingerprint: finished.OutcomeFingerprint,
		Terminal: cloneEntry(terminal), Generation: finished.Generation, Replayed: true,
	}
	if failure != nil {
		cloned := cloneEntry(*failure)
		result.Failure = &cloned
	}
	return result, nil
}

func validateInterruptedTurnRecoveryMeta(meta ThreadMeta, parentThreadID string) error {
	lifecycle, err := canonicalThreadLifecycle(meta)
	if err != nil {
		return err
	}
	if lifecycle != ThreadLifecycleOpen && lifecycle != ThreadLifecycleClosing {
		return ErrThreadClosed
	}
	parentThreadID = strings.TrimSpace(parentThreadID)
	if parentThreadID == "" {
		if strings.TrimSpace(meta.ParentThreadID) != "" {
			return ErrInvalidThreadAuthority
		}
		return nil
	}
	if strings.TrimSpace(meta.ParentThreadID) != parentThreadID {
		return ErrInvalidThreadAuthority
	}
	return nil
}

func ValidateInterruptedTurnRecoveryPath(path []Entry, turnID, runID string) error {
	unfinishedIDs, err := unfinishedTurnIDs(path)
	if err != nil {
		return err
	}
	if len(unfinishedIDs) != 1 || unfinishedIDs[0] != strings.TrimSpace(turnID) {
		return ErrStaleAuthority
	}
	startedRunID := ""
	for _, entry := range path {
		if entry.Type != EntryTurnMarker || strings.TrimSpace(entry.TurnID) == "" {
			continue
		}
		if entry.TurnStatus == TurnStarted {
			if entry.TurnID == turnID {
				startedRunID = strings.TrimSpace(entry.Metadata["run_id"])
			}
		}
	}
	if startedRunID == "" || startedRunID != strings.TrimSpace(runID) {
		return ErrRequestConflict
	}
	return nil
}

func ValidateInterruptedTurnAdmissionPath(path []Entry, threadID, turnID, runID, turnStartedID string) error {
	if err := ValidateInterruptedTurnRecoveryPath(path, turnID, runID); err != nil {
		if errors.Is(err, ErrRequestConflict) {
			return ErrAuthorityCorrupt
		}
		return err
	}
	for _, entry := range path {
		if ValidateInterruptedTurnStartedEntry(entry, threadID, turnID, runID, turnStartedID) == nil {
			return nil
		}
	}
	return ErrAuthorityCorrupt
}

func ValidateInterruptedTurnStartedEntry(entry Entry, threadID, turnID, runID, turnStartedID string) error {
	if entry.ID != strings.TrimSpace(turnStartedID) || entry.ThreadID != strings.TrimSpace(threadID) ||
		entry.Type != EntryTurnMarker || entry.TurnStatus != TurnStarted || entry.TurnID != strings.TrimSpace(turnID) ||
		strings.TrimSpace(entry.Metadata["run_id"]) != strings.TrimSpace(runID) {
		return ErrAuthorityCorrupt
	}
	return nil
}

func unfinishedTurnIDs(path []Entry) ([]string, error) {
	started := map[string]bool{}
	terminal := map[string]bool{}
	var order []string
	for _, entry := range path {
		turnID := strings.TrimSpace(entry.TurnID)
		if entry.Type != EntryTurnMarker || turnID == "" {
			continue
		}
		if entry.TurnStatus == TurnStarted && !started[turnID] {
			started[turnID] = true
			order = append(order, turnID)
		}
		if isTerminalTurnStatus(entry.TurnStatus) {
			terminal[turnID] = true
		}
	}
	var unfinished []string
	for _, turnID := range order {
		if !terminal[turnID] {
			unfinished = append(unfinished, turnID)
		}
	}
	if len(unfinished) > 1 {
		return nil, ErrAuthorityCorrupt
	}
	return unfinished, nil
}

func isTerminalTurnStatus(status TurnMarkerStatus) bool {
	switch status {
	case TurnCompleted, TurnWaiting, TurnFailed, TurnAborted:
		return true
	default:
		return false
	}
}

// PrepareBranchBoundaryEntry closes one copied or rewound unfinished turn so a
// fork or retry path is idle before another turn authority is admitted.
func PrepareBranchBoundaryEntry(path []Entry, threadID, parentEntryID, entryID, reason string, now time.Time) (Entry, error) {
	unfinished, err := unfinishedTurnIDs(path)
	if err != nil {
		return Entry{}, err
	}
	if len(unfinished) == 0 {
		return Entry{}, nil
	}
	threadID = strings.TrimSpace(threadID)
	entryID = strings.TrimSpace(entryID)
	reason = strings.TrimSpace(reason)
	if threadID == "" || entryID == "" || reason == "" {
		return Entry{}, errors.New("branch boundary requires thread, entry, and reason identities")
	}
	entry := Entry{
		ID: entryID, ThreadID: threadID, ParentID: strings.TrimSpace(parentEntryID), Type: EntryTurnMarker,
		TurnID: unfinished[0], TurnStatus: TurnAborted, CreatedAt: now.UTC(),
		Metadata: map[string]string{
			"authority_kind": "branch_boundary", "reason": reason,
			TurnFailureCodeMetadataKey: TurnFailureInterrupted, "failure_reason": BranchBoundaryTurnFailureMessage,
		},
	}
	entry.Raw = rawForEntry(entry)
	entry.RawHash = stableHash(entry.Raw)
	return entry, nil
}

func UnresolvedInterruptedTurnCalls(path []Entry, turnID string) []Entry {
	results := map[string]struct{}{}
	for _, entry := range path {
		if entry.TurnID == turnID && entry.Type == EntryToolResult {
			results[strings.TrimSpace(entry.Message.ToolCallID)] = struct{}{}
		}
	}
	var calls []Entry
	seen := map[string]struct{}{}
	for _, entry := range path {
		callID := strings.TrimSpace(entry.Message.ToolCallID)
		if entry.TurnID != turnID || entry.Type != EntryToolCall || callID == "" {
			continue
		}
		if _, ok := results[callID]; ok {
			continue
		}
		if _, ok := seen[callID]; ok {
			continue
		}
		seen[callID] = struct{}{}
		calls = append(calls, entry)
	}
	return calls
}

func InterruptedTurnToolResult(call session.Message, effectState EffectAttemptState) session.Message {
	if effectState == EffectAttemptDispatching || effectState == EffectAttemptUnknown {
		return session.Message{
			Role: session.Tool, ToolCallID: call.ToolCallID, ToolName: call.ToolName,
			Content:    "Tool call outcome is unknown because the turn was interrupted after dispatch.",
			ToolResult: &session.ToolResultView{Status: "error"},
		}
	}
	return session.Message{
		Role: session.Tool, ToolCallID: call.ToolCallID, ToolName: call.ToolName,
		Content:    "Tool call did not complete because the turn was interrupted.",
		ToolResult: &session.ToolResultView{Status: "cancelled"},
	}
}

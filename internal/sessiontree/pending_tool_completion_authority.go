package sessiontree

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/floegence/floret/internal/session"
)

type AdmitPendingToolCompletionRequest struct {
	CompletionRequestID   string
	RequestFingerprint    string
	SettlementFingerprint string
	Target                PendingToolSettlementTarget
	Settlement            Entry
	ContinuationTurnID    string
	ContinuationRunID     string
	OwnerID               string
	Input                 session.Message
	Now                   time.Time
}

type AdmitPendingToolCompletionResult struct {
	Settlement         Entry
	SettlementReplayed bool
	Admission          AdmitTurnResult
	Replayed           bool
}

type PendingToolCompletionAuthorityRepo interface {
	AdmitPendingToolCompletion(context.Context, AdmitPendingToolCompletionRequest) (AdmitPendingToolCompletionResult, error)
	ReadPendingToolCompletion(context.Context, AdmitPendingToolCompletionRequest) (AdmitPendingToolCompletionResult, bool, error)
}

type pendingToolCompletionLedger struct {
	CompletionRequestID   string
	RequestFingerprint    string
	ThreadID              string
	Target                PendingToolSettlementTarget
	SettlementFingerprint string
	ContinuationTurnID    string
	ContinuationRunID     string
	SettlementEntryID     string
	TurnStartedID         string
	UserMessageID         string
	BaseLeafID            string
}

func ValidatePendingToolCompletionPath(path []Entry) error {
	unfinished, err := unfinishedTurnIDs(path)
	if err != nil {
		return err
	}
	if len(unfinished) != 0 {
		return ErrAuthorityCorrupt
	}
	return nil
}

func ValidateAdmitPendingToolCompletionRequest(req AdmitPendingToolCompletionRequest) error {
	return validateAdmitPendingToolCompletionRequestAttachments(req, session.ValidateMessageAttachments)
}

func ValidateAdmitPendingToolCompletionReplayRequest(req AdmitPendingToolCompletionRequest) error {
	return validateAdmitPendingToolCompletionRequestAttachments(req, session.ValidateStoredMessageAttachments)
}

func ValidateAdmitPendingToolCompletionEnvelope(req AdmitPendingToolCompletionRequest) error {
	if strings.TrimSpace(req.CompletionRequestID) == "" || strings.TrimSpace(req.RequestFingerprint) == "" ||
		strings.TrimSpace(req.SettlementFingerprint) == "" ||
		strings.TrimSpace(req.ContinuationTurnID) == "" || strings.TrimSpace(req.ContinuationRunID) == "" || strings.TrimSpace(req.OwnerID) == "" {
		return errors.New("pending tool completion requires request, completion fingerprint, settlement fingerprint, continuation turn, run, and owner identities")
	}
	return nil
}

func validateAdmitPendingToolCompletionRequestAttachments(req AdmitPendingToolCompletionRequest, validateAttachments func([]session.MessageAttachment) error) error {
	if err := ValidateAdmitPendingToolCompletionEnvelope(req); err != nil {
		return err
	}
	if strings.TrimSpace(req.Input.Content) == "" && len(req.Input.Attachments) == 0 {
		return errors.New("pending tool completion requires structured continuation input")
	}
	if req.Input.Role != session.User {
		return errors.New("pending tool completion continuation input must be a user message")
	}
	if err := validateAttachments(req.Input.Attachments); err != nil {
		return err
	}
	if err := session.ValidateMessageReferences(req.Input.References); err != nil {
		return err
	}
	settlementRequest := SettlePendingToolRecoveryRequest{Target: req.Target, RequestFingerprint: req.SettlementFingerprint, Settlement: req.Settlement}
	return validatePendingToolRecoveryRequest(settlementRequest)
}

func (r *MemoryRepo) ReadPendingToolCompletion(_ context.Context, req AdmitPendingToolCompletionRequest) (AdmitPendingToolCompletionResult, bool, error) {
	if err := ValidateAdmitPendingToolCompletionEnvelope(req); err != nil {
		return AdmitPendingToolCompletionResult{}, false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	existing, ok := r.pendingToolCompletions[strings.TrimSpace(req.CompletionRequestID)]
	if !ok {
		return AdmitPendingToolCompletionResult{}, false, nil
	}
	result, err := r.pendingToolCompletionReplayLocked(existing, req)
	return result, true, err
}

func (r *MemoryRepo) pendingToolCompletionReplayLocked(existing pendingToolCompletionLedger, req AdmitPendingToolCompletionRequest) (AdmitPendingToolCompletionResult, error) {
	if existing.RequestFingerprint != strings.TrimSpace(req.RequestFingerprint) || existing.ThreadID != req.Target.ThreadID ||
		existing.Target != req.Target || existing.SettlementFingerprint != strings.TrimSpace(req.SettlementFingerprint) ||
		existing.ContinuationTurnID != strings.TrimSpace(req.ContinuationTurnID) || existing.ContinuationRunID != strings.TrimSpace(req.ContinuationRunID) {
		return AdmitPendingToolCompletionResult{}, ErrRequestConflict
	}
	if err := ValidateAdmitPendingToolCompletionReplayRequest(req); err != nil {
		return AdmitPendingToolCompletionResult{}, err
	}
	settlement, settlementOK := findEntry(r.entries[existing.ThreadID], existing.SettlementEntryID)
	admissionLedger, admissionOK := r.turnAdmissions[turnAdmissionKey(existing.ThreadID, existing.ContinuationTurnID)]
	if !settlementOK || !admissionOK || admissionLedger.RunID != existing.ContinuationRunID ||
		admissionLedger.TurnStartedID != existing.TurnStartedID || admissionLedger.UserMessageID != existing.UserMessageID {
		return AdmitPendingToolCompletionResult{}, ErrAuthorityCorrupt
	}
	admission, err := r.replayTurnAdmissionLocked(admissionLedger)
	if err != nil {
		return AdmitPendingToolCompletionResult{}, err
	}
	admission.Lease = TurnLease{}
	return AdmitPendingToolCompletionResult{
		Settlement: settlement,
		Admission:  admission,
		Replayed:   true,
	}, nil
}

func (r *MemoryRepo) AdmitPendingToolCompletion(_ context.Context, req AdmitPendingToolCompletionRequest) (AdmitPendingToolCompletionResult, error) {
	if err := ValidateAdmitPendingToolCompletionEnvelope(req); err != nil {
		return AdmitPendingToolCompletionResult{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	requestID := strings.TrimSpace(req.CompletionRequestID)
	fingerprint := strings.TrimSpace(req.RequestFingerprint)
	settlementFingerprint := strings.TrimSpace(req.SettlementFingerprint)
	turnID := strings.TrimSpace(req.ContinuationTurnID)
	runID := strings.TrimSpace(req.ContinuationRunID)
	ownerID := strings.TrimSpace(req.OwnerID)
	if existing, ok := r.pendingToolCompletions[requestID]; ok {
		return r.pendingToolCompletionReplayLocked(existing, req)
	}
	if err := ValidateAdmitPendingToolCompletionRequest(req); err != nil {
		return AdmitPendingToolCompletionResult{}, err
	}
	meta, ok := r.threads[req.Target.ThreadID]
	if !ok {
		if _, deleted := r.tombstones[req.Target.ThreadID]; deleted {
			return AdmitPendingToolCompletionResult{}, ErrThreadDeleted
		}
		return AdmitPendingToolCompletionResult{}, ErrThreadNotFound
	}
	if err := lifecycleRejectsWrite(meta); err != nil {
		return AdmitPendingToolCompletionResult{}, err
	}
	if r.threadAuthorityClaimedLocked(meta.ID) || r.leases[meta.ID].Validate() == nil {
		return AdmitPendingToolCompletionResult{}, ErrThreadAuthorityBusy
	}
	if _, exists := r.turnAdmissions[turnAdmissionKey(meta.ID, turnID)]; exists {
		return AdmitPendingToolCompletionResult{}, ErrRequestConflict
	}
	if r.hasTurnStartedLocked(meta.ID, turnID) {
		return AdmitPendingToolCompletionResult{}, ErrRequestConflict
	}
	path, err := pathLocked(r.threads, r.entries, meta.ID, meta.LeafID)
	if err != nil {
		return AdmitPendingToolCompletionResult{}, err
	}
	if err := ValidatePendingToolCompletionPath(path); err != nil {
		return AdmitPendingToolCompletionResult{}, err
	}
	settlementRequest := SettlePendingToolRecoveryRequest{Target: req.Target, RequestFingerprint: strings.TrimSpace(req.SettlementFingerprint), Settlement: req.Settlement}
	existingSettlement, settlementReplayed, err := pendingToolRecoveryTarget(path, settlementRequest)
	if err != nil {
		return AdmitPendingToolCompletionResult{}, err
	}
	seqBefore := r.seq
	now := nonZeroAuthorityTime(req.Now, r.now)
	settlement := cloneEntry(existingSettlement)
	baseLeafID := meta.LeafID
	if !settlementReplayed {
		settlement = cloneEntry(req.Settlement)
		settlement.ID = r.nextEntryID(meta.ID)
		settlement.ParentID = meta.LeafID
		settlement.CreatedAt = now
		settlement.Raw = rawForEntry(settlement)
		settlement.RawHash = stableHash(settlement.Raw)
		baseLeafID = settlement.ID
	}
	lease, err := r.acquireTurnLeaseLocked(TurnLease{
		ThreadID: meta.ID, Purpose: TurnLeasePurposeTurn, TurnID: turnID, OwnerID: ownerID,
	})
	if err != nil {
		r.seq = seqBefore
		return AdmitPendingToolCompletionResult{}, err
	}
	started := Entry{
		ID: r.nextEntryID(meta.ID), ThreadID: meta.ID, ParentID: baseLeafID, Type: EntryTurnMarker,
		TurnID: turnID, TurnStatus: TurnStarted, CreatedAt: now,
		Metadata: map[string]string{"run_id": runID, "completion_request_id": requestID},
	}
	started.Raw = rawForEntry(started)
	started.RawHash = stableHash(started.Raw)
	user := Entry{
		ID: r.nextEntryID(meta.ID), ThreadID: meta.ID, ParentID: started.ID, Type: EntryUserMessage,
		TurnID: turnID, CreatedAt: now, Message: session.CloneMessage(req.Input),
		Metadata: map[string]string{"completion_request_id": requestID},
	}
	user.Raw = rawForEntry(user)
	user.RawHash = stableHash(user.Raw)
	if !settlementReplayed {
		r.appendIndexedEntriesLocked(meta.ID, settlement)
	}
	r.appendIndexedEntriesLocked(meta.ID, started, user)
	meta.LeafID = user.ID
	meta.UpdatedAt = now
	r.threads[meta.ID] = meta
	r.turnAdmissions[turnAdmissionKey(meta.ID, turnID)] = turnAdmissionLedger{
		ThreadID: meta.ID, TurnID: turnID, RunID: runID, RequestFingerprint: fingerprint,
		Lease: lease, TurnStartedID: started.ID, UserMessageID: user.ID, BaseLeafID: baseLeafID,
	}
	r.pendingToolCompletions[requestID] = pendingToolCompletionLedger{
		CompletionRequestID: requestID, RequestFingerprint: fingerprint, ThreadID: meta.ID,
		Target: req.Target, SettlementFingerprint: settlementFingerprint,
		ContinuationTurnID: turnID, ContinuationRunID: runID,
		SettlementEntryID: settlement.ID, TurnStartedID: started.ID, UserMessageID: user.ID, BaseLeafID: baseLeafID,
	}
	return AdmitPendingToolCompletionResult{
		Settlement:         cloneEntry(settlement),
		SettlementReplayed: settlementReplayed,
		Admission:          AdmitTurnResult{Lease: lease, TurnStarted: cloneEntry(started), UserMessage: cloneEntry(user), BaseLeafID: baseLeafID},
	}, nil
}

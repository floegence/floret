package sessiontree

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/floegence/floret/internal/session"
)

type PublishSubAgentPendingToolCompletionRequest struct {
	InputRequestID        string
	RequestFingerprint    string
	SettlementFingerprint string
	ParentThreadID        string
	ChildThreadID         string
	Target                PendingToolSettlementTarget
	Settlement            Entry
	Message               session.Message
	HostLabels            map[string]string
	CorrelationLabels     map[string]string
	Now                   time.Time
}

type PublishSubAgentPendingToolCompletionResult struct {
	Settlement         Entry
	SettlementReplayed bool
	Input              SubAgentInputRecord
	Replayed           bool
}

type subAgentPendingToolCompletionLedger struct {
	InputRequestID        string
	RequestFingerprint    string
	SettlementFingerprint string
	ParentThreadID        string
	ChildThreadID         string
	Target                PendingToolSettlementTarget
	SettlementEntryID     string
	SubAgentInputID       string
}

func ValidatePublishSubAgentPendingToolCompletionRequest(req PublishSubAgentPendingToolCompletionRequest) error {
	if strings.TrimSpace(req.InputRequestID) == "" || strings.TrimSpace(req.RequestFingerprint) == "" ||
		strings.TrimSpace(req.SettlementFingerprint) == "" || strings.TrimSpace(req.ParentThreadID) == "" || strings.TrimSpace(req.ChildThreadID) == "" {
		return errors.New("subagent pending tool completion requires input request, completion fingerprint, settlement fingerprint, parent, and child identities")
	}
	if req.Target.ThreadID != strings.TrimSpace(req.ChildThreadID) {
		return ErrInvalidThreadAuthority
	}
	if req.Message.Role != session.User || (strings.TrimSpace(req.Message.Content) == "" && len(req.Message.Attachments) == 0) {
		return errors.New("subagent pending tool completion requires structured user input")
	}
	return validatePendingToolRecoveryRequest(SettlePendingToolRecoveryRequest{
		Target: req.Target, RequestFingerprint: req.SettlementFingerprint, Settlement: req.Settlement,
	})
}

func (r *MemoryRepo) PublishSubAgentPendingToolCompletion(_ context.Context, req PublishSubAgentPendingToolCompletionRequest) (PublishSubAgentPendingToolCompletionResult, error) {
	if err := ValidatePublishSubAgentPendingToolCompletionRequest(req); err != nil {
		return PublishSubAgentPendingToolCompletionResult{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	requestID := strings.TrimSpace(req.InputRequestID)
	fingerprint := strings.TrimSpace(req.RequestFingerprint)
	if existing, ok := r.subAgentPendingToolCompletions[requestID]; ok {
		if existing.RequestFingerprint != fingerprint || existing.SettlementFingerprint != strings.TrimSpace(req.SettlementFingerprint) ||
			existing.ParentThreadID != req.ParentThreadID || existing.ChildThreadID != req.ChildThreadID || existing.Target != req.Target {
			return PublishSubAgentPendingToolCompletionResult{}, ErrSubAgentRequestConflict
		}
		settlement, settlementOK := findEntry(r.entries[existing.ChildThreadID], existing.SettlementEntryID)
		input, inputOK := r.subAgentInputByIDLocked(existing.SubAgentInputID)
		if !settlementOK || !inputOK {
			return PublishSubAgentPendingToolCompletionResult{}, ErrAuthorityCorrupt
		}
		return PublishSubAgentPendingToolCompletionResult{Settlement: settlement, SettlementReplayed: true, Input: input, Replayed: true}, nil
	}
	parent, ok := r.threads[req.ParentThreadID]
	if !ok {
		return PublishSubAgentPendingToolCompletionResult{}, ErrThreadNotFound
	}
	if err := lifecycleRejectsWrite(parent); err != nil {
		return PublishSubAgentPendingToolCompletionResult{}, err
	}
	child, ok := r.threads[req.ChildThreadID]
	if !ok {
		return PublishSubAgentPendingToolCompletionResult{}, ErrThreadNotFound
	}
	if child.ParentThreadID != req.ParentThreadID {
		return PublishSubAgentPendingToolCompletionResult{}, ErrInvalidThreadAuthority
	}
	if err := lifecycleRejectsWrite(child); err != nil {
		return PublishSubAgentPendingToolCompletionResult{}, err
	}
	if r.threadAuthorityClaimedLocked(req.ParentThreadID) || r.threadAuthorityClaimedLocked(req.ChildThreadID) {
		return PublishSubAgentPendingToolCompletionResult{}, ErrThreadAuthorityBusy
	}
	if _, active := r.leases[req.ChildThreadID]; active {
		return PublishSubAgentPendingToolCompletionResult{}, ErrActiveTurn
	}
	path, err := pathLocked(r.threads, r.entries, child.ID, child.LeafID)
	if err != nil {
		return PublishSubAgentPendingToolCompletionResult{}, err
	}
	if err := ValidatePendingToolCompletionPath(path); err != nil {
		return PublishSubAgentPendingToolCompletionResult{}, err
	}
	settlementRequest := SettlePendingToolRecoveryRequest{
		Target: req.Target, RequestFingerprint: strings.TrimSpace(req.SettlementFingerprint), Settlement: req.Settlement,
	}
	existingSettlement, settlementReplayed, err := pendingToolRecoveryTarget(path, settlementRequest)
	if err != nil {
		return PublishSubAgentPendingToolCompletionResult{}, err
	}
	now := nonZeroAuthorityTime(req.Now, r.now)
	settlement := cloneEntry(existingSettlement)
	if !settlementReplayed {
		settlement = cloneEntry(req.Settlement)
		settlement.ID = r.nextEntryID(child.ID)
		settlement.ParentID = child.LeafID
		settlement.CreatedAt = now
		settlement.Raw = rawForEntry(settlement)
		settlement.RawHash = stableHash(settlement.Raw)
		r.entries[child.ID] = append(r.entries[child.ID], cloneEntry(settlement))
		child.LeafID = settlement.ID
		child.UpdatedAt = now
		r.threads[child.ID] = child
	}
	input := r.newSubAgentInputLocked(SubAgentRequestPendingToolCompletion, requestID, fingerprint,
		req.ParentThreadID, req.ChildThreadID, req.Message, req.HostLabels, req.CorrelationLabels, now)
	if input.SubAgentInputID == "" {
		return PublishSubAgentPendingToolCompletionResult{}, ErrAuthorityCorrupt
	}
	r.subAgentPendingToolCompletions[requestID] = subAgentPendingToolCompletionLedger{
		InputRequestID: requestID, RequestFingerprint: fingerprint, SettlementFingerprint: strings.TrimSpace(req.SettlementFingerprint),
		ParentThreadID: req.ParentThreadID, ChildThreadID: req.ChildThreadID, Target: req.Target,
		SettlementEntryID: settlement.ID, SubAgentInputID: input.SubAgentInputID,
	}
	return PublishSubAgentPendingToolCompletionResult{
		Settlement: settlement, SettlementReplayed: settlementReplayed, Input: input,
	}, nil
}

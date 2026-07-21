package agentharness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/floegence/floret/internal/event"
	"github.com/floegence/floret/internal/sessiontree"
	"github.com/floegence/floret/tools"
)

type effectApproval struct {
	authority sessiontree.ApprovalAuthorityRepo
	queue     sessiontree.ApprovalQueue
	record    sessiontree.ApprovalRecord
	receipt   sessiontree.ApprovalDecisionReceipt
	now       func() time.Time
}

func (t *Thread) preflightEffectBatch(ctx context.Context, requests []tools.EffectDispatchRequest) error {
	if t == nil || t.harness == nil || len(requests) == 0 {
		return nil
	}
	lease, ok := sessiontree.TurnLeaseFromContext(ctx)
	if !ok || lease.ThreadID != t.id || lease.Purpose != sessiontree.TurnLeasePurposeTurn {
		return sessiontree.ErrStaleAuthority
	}
	local, ok := t.ownedActiveTurnLease(lease.TurnID)
	if !ok || local.OwnerID != lease.OwnerID || local.Generation != lease.Generation {
		return sessiontree.ErrStaleAuthority
	}
	authority, ok := t.harness.options.Repo.(sessiontree.ApprovalAuthorityRepo)
	if !ok {
		return errors.New("session tree repo does not support approval authority")
	}
	type preparedRequest struct {
		request        tools.EffectDispatchRequest
		item           sessiontree.ApprovalPreflightItem
		requestedEvent event.Event
	}
	prepared := make([]preparedRequest, 0, len(requests))
	for _, request := range requests {
		if request.Permission.Mode != tools.PermissionAsk {
			continue
		}
		argumentHash := sessiontree.StableHash(strings.TrimSpace(request.RawArgs))
		effectFingerprint, err := effectRequestFingerprint(request, lease, argumentHash)
		if err != nil {
			return err
		}
		item := approvalPreflightItem(request, effectFingerprint, argumentHash)
		attempt := sessiontree.EffectAttempt{EffectAttemptID: item.EffectAttemptID, Invocation: item.Invocation}
		authorizationRequest := effectAuthorizationRequest(request, lease, attempt, effectFingerprint)
		requestedEvent := t.effectApprovalEvent(event.ToolApprovalRequested, authorizationRequest, "")
		item.RequestedEntry = approvalEventEntry(t.id, request.TurnID, requestedEvent)
		item.RequestedEntry.ID = sessiontree.ApprovalRequestedEntryID(item.EffectAttemptID)
		item.ApprovalRequestFingerprint, err = approvalRequestFingerprint(item)
		if err != nil {
			return err
		}
		prepared = append(prepared, preparedRequest{request: request, item: item, requestedEvent: requestedEvent})
	}
	if len(prepared) == 0 {
		return nil
	}
	slices.SortStableFunc(prepared, func(left, right preparedRequest) int {
		return left.item.BatchIndex - right.item.BatchIndex
	})
	items := make([]sessiontree.ApprovalPreflightItem, 0, len(prepared))
	for _, request := range prepared {
		items = append(items, request.item)
	}
	result, err := authority.PrepareApprovalBatch(ctx, sessiontree.PrepareApprovalBatchRequest{
		Lease: lease, Items: items, Now: t.harness.now(),
	})
	if err != nil {
		return err
	}
	if len(result.Effects) != len(prepared) || len(result.Approvals) != len(prepared) || len(result.RequestedEntries) != len(prepared) {
		return sessiontree.ErrAuthorityCorrupt
	}
	for index, request := range prepared {
		attempt := result.Effects[index]
		record := result.Approvals[index]
		if !sessiontree.ApprovalPreflightMatchesEffect(request.item, lease, attempt) ||
			!sessiontree.ApprovalPreflightMatchesRecord(record, result.Queue.RootThreadID, record.ParentThreadID, request.item, attempt) {
			return sessiontree.ErrAuthorityCorrupt
		}
		if !sessiontree.ApprovalDispatchEntryRequestMatches(result.RequestedEntries[index], request.item.RequestedEntry) {
			return sessiontree.ErrAuthorityCorrupt
		}
		if result.Replayed {
			continue
		}
		t.emitCommittedApprovalEvent(result.RequestedEntries[index], request.request.RunID, request.requestedEvent)
		if t.harness.options.Sink != nil {
			t.harness.options.Sink.Emit(event.SanitizeWithPolicy(request.requestedEvent, t.harness.options.SinkPolicy))
		}
	}
	return nil
}

func approvalPreflightItem(request tools.EffectDispatchRequest, effectFingerprint, argumentHash string) sessiontree.ApprovalPreflightItem {
	resources := make([]sessiontree.ApprovalResource, 0, len(request.Resources))
	for _, resource := range request.Resources {
		resources = append(resources, sessiontree.ApprovalResource{Kind: resource.Kind, Value: resource.Value})
	}
	effects := make([]string, 0, len(request.Effects))
	for _, effect := range request.Effects {
		effects = append(effects, string(effect))
	}
	item := sessiontree.ApprovalPreflightItem{
		EffectRequestFingerprint: effectFingerprint,
		Invocation: sessiontree.EffectInvocationIdentity{
			ThreadID: request.ThreadID, TurnID: request.TurnID, RunID: request.RunID,
			ToolCallID: request.CallID, ToolName: request.Name, ArgumentHash: argumentHash,
		},
		ToolKind: "local", Step: request.Step, BatchIndex: request.BatchIndex, BatchSize: request.BatchSize,
		Resources: resources, Effects: effects, Labels: cloneStringMap(request.Labels), HostContext: cloneStringMap(request.HostContext),
		ReadOnly: request.ReadOnly, Destructive: request.Destructive, OpenWorld: request.OpenWorld,
	}
	item.EffectAttemptID = sessiontree.ApprovalEffectAttemptID(item.Invocation)
	return item
}

func approvalRequestFingerprint(item sessiontree.ApprovalPreflightItem) (string, error) {
	item.ApprovalRequestFingerprint = ""
	payload, err := json.Marshal(item)
	if err != nil {
		return "", err
	}
	return sessiontree.StableHash(string(payload)), nil
}

func effectAuthorizationRequest(request tools.EffectDispatchRequest, lease sessiontree.TurnLease, attempt sessiontree.EffectAttempt, fingerprint string) EffectAuthorizationRequest {
	return EffectAuthorizationRequest{
		EffectAttemptID: attempt.EffectAttemptID, RequestFingerprint: fingerprint,
		ThreadID: request.ThreadID, TurnID: request.TurnID, RunID: request.RunID, ToolCallID: request.CallID,
		ToolName: request.Name, ArgumentHash: attempt.Invocation.ArgumentHash, Resources: append([]tools.ResourceRef(nil), request.Resources...),
		Step: request.Step, BatchIndex: request.BatchIndex, BatchSize: request.BatchSize,
		Labels: cloneStringMap(request.Labels), HostContext: cloneStringMap(request.HostContext),
		Effects: append([]tools.Effect(nil), request.Effects...), Permission: request.Permission,
		ReadOnly: request.ReadOnly, Destructive: request.Destructive, OpenWorld: request.OpenWorld,
		LeaseOwnerID: lease.OwnerID, LeaseGeneration: lease.Generation, ObservedHeartbeat: lease.Heartbeat,
	}
}

func (t *Thread) bindEffectApproval(
	ctx context.Context,
	req EffectAuthorizationRequest,
) (*effectApproval, error) {
	authority, ok := t.harness.options.Repo.(sessiontree.ApprovalAuthorityRepo)
	if !ok {
		return nil, errors.New("session tree repo does not support approval authority")
	}
	lease, ok := sessiontree.TurnLeaseFromContext(ctx)
	if !ok || lease.ThreadID != req.ThreadID || lease.TurnID != req.TurnID || lease.Purpose != sessiontree.TurnLeasePurposeTurn {
		return nil, sessiontree.ErrStaleAuthority
	}
	record, err := authority.Approval(ctx, req.EffectAttemptID)
	if err != nil {
		return nil, err
	}
	queue, err := authority.ReadApprovalQueue(ctx, record.RootThreadID)
	if err != nil {
		return nil, err
	}
	if record.ApprovalID != req.EffectAttemptID || record.EffectAttemptID != req.EffectAttemptID ||
		record.ThreadID != req.ThreadID || record.TurnID != req.TurnID || record.RunID != req.RunID ||
		record.ToolCallID != req.ToolCallID || record.ToolName != req.ToolName || record.ArgsHash != req.ArgumentHash {
		return nil, sessiontree.ErrAuthorityCorrupt
	}
	approval := &effectApproval{
		authority: authority, queue: queue, record: record,
		now: t.harness.now,
	}
	return approval, nil
}

func approvalReceiptFromCanonical(
	receipt sessiontree.ApprovalDecisionReceipt,
	record sessiontree.ApprovalRecord,
	queue sessiontree.ApprovalQueue,
) (sessiontree.ApprovalDecisionReceipt, error) {
	if err := sessiontree.ValidateApprovalDecisionReceiptAuthority(receipt, record, queue); err != nil {
		return sessiontree.ApprovalDecisionReceipt{}, err
	}
	return receipt, nil
}

func (a *effectApproval) wait(ctx context.Context) (sessiontree.ApprovalDecisionReceipt, error) {
	waited, err := a.authority.WaitApprovalDecision(ctx, a.record.ApprovalID)
	if err == nil {
		if waited.Approval.ApprovalID != a.record.ApprovalID || waited.Approval.RootThreadID != a.record.RootThreadID {
			return sessiontree.ApprovalDecisionReceipt{}, sessiontree.ErrAuthorityCorrupt
		}
		receipt, receiptErr := approvalReceiptFromCanonical(waited.Receipt, waited.Approval, waited.Queue)
		if receiptErr != nil {
			return sessiontree.ApprovalDecisionReceipt{}, receiptErr
		}
		if receipt.State == sessiontree.ApprovalCancelled {
			// The cancellation is already canonical. Preserve the caller's
			// cancellation semantics even when the waiter observes the committed
			// receipt before its context cancellation notification.
			if err := ctx.Err(); err != nil {
				return sessiontree.ApprovalDecisionReceipt{}, err
			}
			return sessiontree.ApprovalDecisionReceipt{}, context.Canceled
		}
		a.receipt = receipt
		return receipt, nil
	}
	return sessiontree.ApprovalDecisionReceipt{}, err
}

func approvalBatchCancellationFingerprint(lease sessiontree.TurnLease, runID, reason string) string {
	return sessiontree.StableHash(strings.Join([]string{
		lease.ThreadID, lease.TurnID, strings.TrimSpace(runID), lease.OwnerID,
		fmt.Sprintf("%d", lease.Generation), strings.TrimSpace(reason),
	}, "\x00"))
}

func (t *Thread) cancelApprovalBatchForTurn(ctx context.Context, lease sessiontree.TurnLease, runID string) error {
	authority, ok := t.harness.options.Repo.(sessiontree.ApprovalAuthorityRepo)
	if !ok {
		return errors.New("session tree repo does not support approval authority")
	}
	cause := ctx.Err()
	if cause == nil {
		cause = context.Canceled
	}
	persistCtx, cancel := turnFinalizationContext(ctx)
	defer cancel()
	queue, err := authority.ReadApprovalQueue(persistCtx, lease.ThreadID)
	if err != nil {
		return err
	}
	fingerprint := approvalBatchCancellationFingerprint(lease, runID, "turn_cancelled")
	now := t.harness.now()
	type cancellationCandidate struct {
		entry sessiontree.Entry
		event event.Event
	}
	candidatesByEntryID := make(map[string]cancellationCandidate)
	candidates := make([]sessiontree.ApprovalCancellationEntry, 0, len(queue.Items))
	for _, record := range queue.Items {
		if record.ThreadID != lease.ThreadID || record.TurnID != lease.TurnID || record.RunID != strings.TrimSpace(runID) ||
			!sessiontree.ApprovalQueueVisible(record.State) {
			continue
		}
		cancelledEvent := approvalEventFromRecord(now, event.ToolApprovalCanceled, record, sessiontree.ApprovalReasonCancelled)
		entry := approvalEventEntry(record.ThreadID, record.TurnID, cancelledEvent)
		entry.ID = sessiontree.ApprovalCancellationEntryID(fingerprint, record.ApprovalID)
		candidates = append(candidates, sessiontree.ApprovalCancellationEntry{ApprovalID: record.ApprovalID, Entry: entry})
		candidatesByEntryID[entry.ID] = cancellationCandidate{entry: entry, event: cancelledEvent}
	}
	result, err := authority.CancelApprovalBatch(persistCtx, sessiontree.CancelApprovalBatchRequest{
		Lease: lease, RunID: runID,
		CancellationFingerprint: fingerprint, CancellationEntries: candidates, Now: now,
	})
	if err != nil {
		return err
	}
	for _, record := range result.Approvals {
		if record.ThreadID != lease.ThreadID || record.TurnID != lease.TurnID || record.RunID != strings.TrimSpace(runID) ||
			record.State != sessiontree.ApprovalCancelled || record.Reason != sessiontree.ApprovalReasonCancelled {
			return sessiontree.ErrAuthorityCorrupt
		}
	}
	if !result.Replayed {
		for _, entry := range result.CancellationEntries {
			candidate, ok := candidatesByEntryID[entry.ID]
			if !ok || !sessiontree.ApprovalDispatchEntryRequestMatches(entry, candidate.entry) {
				return sessiontree.ErrAuthorityCorrupt
			}
			t.emitCommittedApprovalEvent(entry, candidate.event.RunID, candidate.event)
			if t.harness.options.Sink != nil {
				t.harness.options.Sink.Emit(event.SanitizeWithPolicy(candidate.event, t.harness.options.SinkPolicy))
			}
		}
	}
	return cause
}

func (a *effectApproval) commitDispatch(
	ctx context.Context,
	lease sessiontree.TurnLease,
	proofHash string,
	approvedEntry sessiontree.Entry,
) (sessiontree.CommitApprovalDispatchResult, error) {
	receipt := a.receipt
	if receipt.State != sessiontree.ApprovalDecisionSubmitted {
		return sessiontree.CommitApprovalDispatchResult{}, sessiontree.ErrRequestConflict
	}
	return a.authority.CommitApprovalDispatch(ctx, sessiontree.CommitApprovalDispatchRequest{
		DecisionID: receipt.DecisionID, ExpectedRootThreadID: receipt.RootThreadID,
		ExpectedGeneration: receipt.QueueGeneration,
		ExpectedCurrent:    a.record.Identity(), ExpectedApprovalRevision: receipt.ApprovalRevision,
		EffectAttemptID: a.record.EffectAttemptID, Lease: lease,
		AuthorizationProofHash: strings.TrimSpace(proofHash), ApprovedEntry: approvedEntry, Now: a.now(),
	})
}

func (a *effectApproval) finalize(ctx context.Context, state sessiontree.ApprovalState, reason, resolutionID string, finalizedEntry sessiontree.Entry) (sessiontree.FinalizeApprovalResult, error) {
	generation := a.queue.Generation
	approvalRevision := a.record.Revision
	if a.receipt.DecisionID != "" {
		generation = a.receipt.QueueGeneration
		approvalRevision = a.receipt.ApprovalRevision
		resolutionID = a.receipt.DecisionID
	}
	persistCtx, cancel := turnFinalizationContext(ctx)
	defer cancel()
	return a.authority.FinalizeApproval(persistCtx, sessiontree.FinalizeApprovalRequest{
		ResolutionID: strings.TrimSpace(resolutionID), ExpectedRootThreadID: a.record.RootThreadID,
		ExpectedGeneration: generation, ExpectedCurrent: a.record.Identity(),
		ExpectedApprovalRevision: approvalRevision, State: state, Reason: reason, FinalizedEntry: finalizedEntry, Now: a.now(),
	})
}

func (t *Thread) finalizeEffectApproval(
	ctx context.Context,
	approval *effectApproval,
	req EffectAuthorizationRequest,
	state sessiontree.ApprovalState,
	reason string,
	resolutionID string,
) error {
	ev := t.effectApprovalEvent(event.ToolApprovalRejected, req, reason)
	entry := approvalEventEntry(req.ThreadID, req.TurnID, ev)
	entry.ID = sessiontree.ApprovalFinalizationEntryID(resolutionID, approval.record.ApprovalID)
	entry.Metadata[subAgentApprovalStateKey] = string(state)
	result, err := approval.finalize(ctx, state, reason, resolutionID, entry)
	if err != nil {
		return err
	}
	if !sessiontree.ApprovalDispatchEntryRequestMatches(result.FinalizedEntry, entry) {
		return sessiontree.ErrAuthorityCorrupt
	}
	if result.Replayed {
		return nil
	}
	t.emitCommittedApprovalEvent(result.FinalizedEntry, req.RunID, ev)
	if t.harness.options.Sink != nil {
		t.harness.options.Sink.Emit(event.SanitizeWithPolicy(ev, t.harness.options.SinkPolicy))
	}
	return nil
}

func approvalFailure(cause error) (sessiontree.ApprovalState, string) {
	switch {
	case errors.Is(cause, ErrEffectUnauthorized), errors.Is(cause, tools.ErrRejected):
		return sessiontree.ApprovalRejected, sessiontree.ApprovalReasonPolicyDenied
	case errors.Is(cause, ErrAuthorizationContract), errors.Is(cause, ErrInvalidAuthorizationProof), errors.Is(cause, ErrEffectDispatchConsumed):
		return sessiontree.ApprovalFailed, sessiontree.ApprovalReasonAuthorizationContract
	default:
		return sessiontree.ApprovalFailed, sessiontree.ApprovalReasonAuthorizationUnavailable
	}
}

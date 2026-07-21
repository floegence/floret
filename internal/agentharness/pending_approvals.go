package agentharness

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/floegence/floret/internal/event"
	"github.com/floegence/floret/internal/sessiontree"
)

type ResolveApprovalOptions struct {
	DecisionID               string
	ExpectedRootThreadID     string
	ExpectedGeneration       int64
	ExpectedRevision         int64
	ExpectedCurrent          sessiontree.ApprovalIdentity
	ExpectedApprovalRevision int64
	Decision                 sessiontree.ApprovalDecision
}

type ResolveApprovalResult struct {
	Receipt  sessiontree.ApprovalDecisionReceipt
	Queue    ApprovalQueueSnapshot
	Approval ApprovalRecord
	Replayed bool
}

func (h *AgentHarness) ReadApprovalQueue(ctx context.Context, opts ReadApprovalQueueOptions) (ApprovalQueueSnapshot, error) {
	if h == nil {
		return ApprovalQueueSnapshot{}, errors.New("agent harness is nil")
	}
	threadID := strings.TrimSpace(opts.ThreadID)
	if threadID == "" {
		return ApprovalQueueSnapshot{}, errors.New("thread id is required")
	}
	authority, ok := h.options.Repo.(sessiontree.ApprovalAuthorityRepo)
	if !ok {
		return ApprovalQueueSnapshot{}, errors.New("session tree repo does not support approval authority")
	}
	queue, err := authority.ReadApprovalQueue(ctx, threadID)
	if err != nil {
		return ApprovalQueueSnapshot{}, err
	}
	return approvalQueueSnapshot(queue), nil
}

func (h *AgentHarness) ResolveApproval(ctx context.Context, opts ResolveApprovalOptions) (ResolveApprovalResult, error) {
	if h == nil {
		return ResolveApprovalResult{}, errors.New("agent harness is nil")
	}
	authority, ok := h.options.Repo.(sessiontree.ApprovalAuthorityRepo)
	if !ok {
		return ResolveApprovalResult{}, errors.New("session tree repo does not support approval authority")
	}
	request := sessiontree.ResolveApprovalRequest{
		DecisionID:               opts.DecisionID,
		ExpectedRootThreadID:     opts.ExpectedRootThreadID,
		ExpectedGeneration:       opts.ExpectedGeneration,
		ExpectedRevision:         opts.ExpectedRevision,
		ExpectedCurrent:          opts.ExpectedCurrent,
		ExpectedApprovalRevision: opts.ExpectedApprovalRevision,
		Decision:                 opts.Decision,
		Now:                      h.now(),
	}
	var rejectedEvent event.Event
	if opts.Decision == sessiontree.ApprovalDecisionReject {
		record, err := authority.Approval(ctx, opts.ExpectedCurrent.ApprovalID)
		if err != nil {
			return ResolveApprovalResult{}, err
		}
		if !sessiontree.SameApprovalIdentity(record.Identity(), opts.ExpectedCurrent) || record.Revision != opts.ExpectedApprovalRevision {
			return ResolveApprovalResult{}, sessiontree.ErrStaleAuthority
		}
		rejectedEvent = approvalEventFromRecord(h.now(), event.ToolApprovalRejected, record, sessiontree.ApprovalReasonUserRejected)
		request.RejectedEntry = approvalEventEntry(record.ThreadID, record.TurnID, rejectedEvent)
		request.RejectedEntry.ID = sessiontree.ApprovalRejectedEntryID(opts.DecisionID, record.ApprovalID)
	}
	resolved, err := authority.ResolveApproval(ctx, request)
	if err != nil {
		return ResolveApprovalResult{}, err
	}
	if opts.Decision == sessiontree.ApprovalDecisionReject {
		if !sessiontree.ApprovalDispatchEntryRequestMatches(resolved.RejectedEntry, request.RejectedEntry) {
			return ResolveApprovalResult{}, sessiontree.ErrAuthorityCorrupt
		}
		if !resolved.Replayed {
			h.emitEntryCommitted(resolved.RejectedEntry, resolved.Approval.RunID)
			h.emit(HarnessEvent{
				Type: EventEntryAppended, RunID: resolved.Approval.RunID, ThreadID: resolved.Approval.ThreadID,
				TurnID: resolved.Approval.TurnID, EntryID: resolved.RejectedEntry.ID, ParentID: resolved.RejectedEntry.ParentID,
				Message: string(rejectedEvent.Type),
			})
			if h.options.Sink != nil {
				h.options.Sink.Emit(event.SanitizeWithPolicy(rejectedEvent, h.options.SinkPolicy))
			}
		}
	}
	return ResolveApprovalResult{
		Receipt: resolved.Receipt, Queue: approvalQueueSnapshot(resolved.Queue),
		Approval: approvalRecordFromCanonical(resolved.Approval), Replayed: resolved.Replayed,
	}, nil
}

func approvalEventFromRecord(now time.Time, typ event.Type, record sessiontree.ApprovalRecord, reason string) event.Event {
	resources := make([]map[string]string, 0, len(record.Resources))
	for _, resource := range record.Resources {
		resources = append(resources, map[string]string{"kind": resource.Kind, "value": resource.Value})
	}
	return event.Event{
		Type: typ, RunID: record.RunID, ThreadID: record.ThreadID, TurnID: record.TurnID, Step: record.Step,
		ToolID: record.ToolCallID, ToolName: record.ToolName, ToolKind: record.ToolKind, ArgsHash: record.ArgsHash,
		Err: strings.TrimSpace(reason), Timestamp: now,
		Metadata: map[string]any{
			"approval_id": record.ApprovalID, "resources": resources, "effects": append([]string(nil), record.Effects...),
			"read_only": record.ReadOnly, "destructive": record.Destructive, "open_world": record.OpenWorld,
			"batch_index": record.BatchIndex, "batch_size": record.BatchSize,
			"labels": cloneStringMap(record.Labels), "host_context": cloneStringMap(record.HostContext),
		},
	}
}

func approvalQueueSnapshot(queue sessiontree.ApprovalQueue) ApprovalQueueSnapshot {
	out := ApprovalQueueSnapshot{
		RootThreadID: queue.RootThreadID, Generation: queue.Generation, Revision: queue.Revision,
		CurrentApprovalID: queue.CurrentApprovalID, GeneratedAt: queue.GeneratedAt,
		Approvals: make([]ApprovalRecord, 0, len(queue.Items)),
	}
	for _, record := range queue.Items {
		out.Approvals = append(out.Approvals, approvalRecordFromCanonical(record))
	}
	return out
}

func approvalRecordFromCanonical(record sessiontree.ApprovalRecord) ApprovalRecord {
	resources := make([]ApprovalResource, 0, len(record.Resources))
	for _, resource := range record.Resources {
		resources = append(resources, ApprovalResource{Kind: resource.Kind, Value: resource.Value})
	}
	return ApprovalRecord{
		ApprovalID: record.ApprovalID, RootThreadID: record.RootThreadID, ParentThreadID: record.ParentThreadID,
		ToolCallID: record.ToolCallID, EffectAttemptID: record.EffectAttemptID, ToolName: record.ToolName, ToolKind: record.ToolKind,
		RunID: record.RunID, ThreadID: record.ThreadID, TurnID: record.TurnID,
		Step: record.Step, BatchIndex: record.BatchIndex, BatchSize: record.BatchSize,
		State: string(record.State), Revision: record.Revision, QueueSequence: record.QueueSequence, DecisionID: record.DecisionID,
		RequestedAt: record.RequestedAt, UpdatedAt: record.UpdatedAt, ResolvedAt: record.ResolvedAt,
		ArgsHash: record.ArgsHash, RequestFingerprint: record.RequestFingerprint, AuthorizationProofHash: record.AuthorizationProofHash,
		Resources: resources, Effects: append([]string(nil), record.Effects...), Labels: cloneStringMap(record.Labels),
		HostContext: cloneStringMap(record.HostContext), ReadOnly: record.ReadOnly, Destructive: record.Destructive,
		OpenWorld: record.OpenWorld, Reason: record.Reason,
	}
}

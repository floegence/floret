package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"time"
)

type SubAgentCloseState string

const (
	SubAgentClosePrepared  SubAgentCloseState = "prepared"
	SubAgentCloseCompleted SubAgentCloseState = "completed"
)

type SubAgentCloseNode struct {
	ThreadID string `json:"thread_id"`
	WasOpen  bool   `json:"was_open"`
}

type SubAgentCloseOperation struct {
	CloseOperationID   string
	ParentThreadID     string
	TargetThreadID     string
	Reason             string
	IntentFingerprint  string
	RequestFingerprint string
	State              SubAgentCloseState
	Nodes              []SubAgentCloseNode
	ResultEntryIDs     []string
	PreparedAt         time.Time
	FinishedAt         time.Time
}

type PrepareSubAgentCloseRequest struct {
	CloseOperationID string
	ParentThreadID   string
	TargetThreadID   string
	Reason           string
	TargetLease      *TurnLease
	Now              time.Time
}

type PrepareSubAgentCloseResult struct {
	Operation SubAgentCloseOperation
	Replayed  bool
}

type FinishSubAgentCloseRequest struct {
	CloseOperationID string
	ParentThreadID   string
	TargetThreadID   string
	Reason           string
	Now              time.Time
}

type FinishSubAgentCloseResult struct {
	Operation         SubAgentCloseOperation
	Threads           []ThreadMeta
	Entries           []Entry
	CancelledInputIDs []string
	Replayed          bool
}

type SubAgentCloseAuthorityRepo interface {
	PrepareSubAgentClose(context.Context, PrepareSubAgentCloseRequest) (PrepareSubAgentCloseResult, error)
	FinishSubAgentClose(context.Context, FinishSubAgentCloseRequest) (FinishSubAgentCloseResult, error)
}

func normalizeSubAgentCloseIntent(operationID, parentThreadID, targetThreadID, reason string) (string, string, string, string, error) {
	operationID = strings.TrimSpace(operationID)
	parentThreadID = strings.TrimSpace(parentThreadID)
	targetThreadID = strings.TrimSpace(targetThreadID)
	reason = strings.TrimSpace(reason)
	if operationID == "" || parentThreadID == "" || targetThreadID == "" {
		return "", "", "", "", errors.New("subagent close requires operation, parent, and target thread identities")
	}
	if reason == "" {
		return "", "", "", "", errors.New("subagent close reason is required")
	}
	if parentThreadID == targetThreadID {
		return "", "", "", "", ErrInvalidThreadAuthority
	}
	return operationID, parentThreadID, targetThreadID, reason, nil
}

func subAgentCloseIntentFingerprint(operationID, parentThreadID, targetThreadID, reason string) string {
	return StableHash(strings.Join([]string{"subagent-close-v1", operationID, parentThreadID, targetThreadID, reason}, "\x00"))
}

// NormalizeSubAgentCloseIntent validates the storage-kernel close identity and
// returns its normalized fields plus immutable intent fingerprint.
func NormalizeSubAgentCloseIntent(operationID, parentThreadID, targetThreadID, reason string) (string, string, string, string, string, error) {
	operationID, parentThreadID, targetThreadID, reason, err := normalizeSubAgentCloseIntent(operationID, parentThreadID, targetThreadID, reason)
	if err != nil {
		return "", "", "", "", "", err
	}
	return operationID, parentThreadID, targetThreadID, reason,
		subAgentCloseIntentFingerprint(operationID, parentThreadID, targetThreadID, reason), nil
}

func subAgentCloseRequestFingerprint(intentFingerprint string, nodes []SubAgentCloseNode) (string, error) {
	payload, err := json.Marshal(struct {
		Intent string              `json:"intent"`
		Nodes  []SubAgentCloseNode `json:"nodes"`
	}{Intent: intentFingerprint, Nodes: nodes})
	if err != nil {
		return "", err
	}
	return StableHash(string(payload)), nil
}

// SubAgentCloseRequestFingerprint binds one close intent to the subtree
// membership derived by the storage transaction.
func SubAgentCloseRequestFingerprint(intentFingerprint string, nodes []SubAgentCloseNode) (string, error) {
	return subAgentCloseRequestFingerprint(intentFingerprint, nodes)
}

func cloneSubAgentCloseOperation(operation SubAgentCloseOperation) SubAgentCloseOperation {
	operation.Nodes = append([]SubAgentCloseNode(nil), operation.Nodes...)
	operation.ResultEntryIDs = append([]string(nil), operation.ResultEntryIDs...)
	return operation
}

func (r *MemoryRepo) PrepareSubAgentClose(_ context.Context, req PrepareSubAgentCloseRequest) (PrepareSubAgentCloseResult, error) {
	operationID, parentThreadID, targetThreadID, reason, err := normalizeSubAgentCloseIntent(req.CloseOperationID, req.ParentThreadID, req.TargetThreadID, req.Reason)
	if err != nil {
		return PrepareSubAgentCloseResult{}, err
	}
	intentFingerprint := subAgentCloseIntentFingerprint(operationID, parentThreadID, targetThreadID, reason)
	r.mu.Lock()
	defer r.mu.Unlock()
	parent, ok := r.threads[parentThreadID]
	if !ok {
		if _, deleted := r.tombstones[parentThreadID]; deleted {
			return PrepareSubAgentCloseResult{}, ErrThreadDeleted
		}
		return PrepareSubAgentCloseResult{}, ErrThreadNotFound
	}
	if err := lifecycleRejectsWrite(parent); err != nil {
		return PrepareSubAgentCloseResult{}, err
	}
	if existing, ok := r.subAgentCloseOperations[operationID]; ok {
		if existing.IntentFingerprint != intentFingerprint {
			return PrepareSubAgentCloseResult{}, ErrRequestConflict
		}
		if err := r.validatePreparedSubAgentCloseLocked(existing, req.TargetLease); err != nil {
			return PrepareSubAgentCloseResult{}, err
		}
		return PrepareSubAgentCloseResult{Operation: cloneSubAgentCloseOperation(existing), Replayed: true}, nil
	}
	if r.threadAuthorityClaimedLocked(parentThreadID) {
		return PrepareSubAgentCloseResult{}, ErrThreadAuthorityBusy
	}
	target, ok := r.threads[targetThreadID]
	if !ok {
		if _, deleted := r.tombstones[targetThreadID]; deleted {
			return PrepareSubAgentCloseResult{}, ErrThreadDeleted
		}
		return PrepareSubAgentCloseResult{}, ErrThreadNotFound
	}
	if target.ParentThreadID != parentThreadID {
		return PrepareSubAgentCloseResult{}, ErrInvalidThreadAuthority
	}
	targetLifecycle, err := canonicalThreadLifecycle(target)
	if err != nil {
		return PrepareSubAgentCloseResult{}, err
	}
	switch targetLifecycle {
	case ThreadLifecycleClosing:
		return PrepareSubAgentCloseResult{}, ErrSubAgentClosing
	case ThreadLifecycleClosed:
		return PrepareSubAgentCloseResult{}, ErrThreadClosed
	case ThreadLifecycleOpen:
	default:
		return PrepareSubAgentCloseResult{}, ErrAuthorityCorrupt
	}
	threadIDs, err := threadAuthorityTreeIDsLocked(r.threads, targetThreadID)
	if err != nil {
		return PrepareSubAgentCloseResult{}, err
	}
	nodes := make([]SubAgentCloseNode, 0, len(threadIDs))
	for _, threadID := range threadIDs {
		meta := r.threads[threadID]
		lifecycle, err := canonicalThreadLifecycle(meta)
		if err != nil {
			return PrepareSubAgentCloseResult{}, err
		}
		if lifecycle != ThreadLifecycleOpen && lifecycle != ThreadLifecycleClosed {
			return PrepareSubAgentCloseResult{}, ErrSubAgentClosing
		}
		if strings.TrimSpace(meta.CloseOperationID) != "" {
			return PrepareSubAgentCloseResult{}, ErrAuthorityCorrupt
		}
		if r.threadAuthorityClaimedLocked(threadID) {
			return PrepareSubAgentCloseResult{}, ErrThreadAuthorityBusy
		}
		if lease, active := r.leases[threadID]; active {
			if threadID != targetThreadID || req.TargetLease == nil || !SameTurnLease(lease, *req.TargetLease) ||
				lease.Purpose != TurnLeasePurposeTurn || !lease.Fresh(r.now().UTC()) {
				return PrepareSubAgentCloseResult{}, ErrThreadAuthorityBusy
			}
		} else if threadID == targetThreadID && req.TargetLease != nil {
			return PrepareSubAgentCloseResult{}, ErrStaleAuthority
		}
		if lifecycle == ThreadLifecycleClosed && r.hasPendingSubAgentInputLocked(threadID) {
			return PrepareSubAgentCloseResult{}, ErrAuthorityCorrupt
		}
		nodes = append(nodes, SubAgentCloseNode{ThreadID: threadID, WasOpen: lifecycle == ThreadLifecycleOpen})
	}
	requestFingerprint, err := subAgentCloseRequestFingerprint(intentFingerprint, nodes)
	if err != nil {
		return PrepareSubAgentCloseResult{}, err
	}
	now := nonZeroAuthorityTime(req.Now, r.now)
	for _, node := range nodes {
		if !node.WasOpen {
			continue
		}
		meta := r.threads[node.ThreadID]
		meta.Lifecycle = ThreadLifecycleClosing
		meta.CloseOperationID = operationID
		meta.UpdatedAt = now
		r.threads[node.ThreadID] = meta
	}
	operation := SubAgentCloseOperation{
		CloseOperationID: operationID, ParentThreadID: parentThreadID, TargetThreadID: targetThreadID, Reason: reason,
		IntentFingerprint: intentFingerprint, RequestFingerprint: requestFingerprint, State: SubAgentClosePrepared,
		Nodes: append([]SubAgentCloseNode(nil), nodes...), PreparedAt: now,
	}
	r.subAgentCloseOperations[operationID] = operation
	return PrepareSubAgentCloseResult{Operation: cloneSubAgentCloseOperation(operation)}, nil
}

func (r *MemoryRepo) FinishSubAgentClose(_ context.Context, req FinishSubAgentCloseRequest) (FinishSubAgentCloseResult, error) {
	operationID, parentThreadID, targetThreadID, reason, err := normalizeSubAgentCloseIntent(req.CloseOperationID, req.ParentThreadID, req.TargetThreadID, req.Reason)
	if err != nil {
		return FinishSubAgentCloseResult{}, err
	}
	intentFingerprint := subAgentCloseIntentFingerprint(operationID, parentThreadID, targetThreadID, reason)
	r.mu.Lock()
	defer r.mu.Unlock()
	operation, ok := r.subAgentCloseOperations[operationID]
	if !ok {
		return FinishSubAgentCloseResult{}, ErrThreadNotFound
	}
	if operation.IntentFingerprint != intentFingerprint {
		return FinishSubAgentCloseResult{}, ErrRequestConflict
	}
	if operation.State == SubAgentCloseCompleted {
		return r.replayedSubAgentCloseLocked(operation)
	}
	parent, ok := r.threads[parentThreadID]
	if !ok {
		if _, deleted := r.tombstones[parentThreadID]; deleted {
			return FinishSubAgentCloseResult{}, ErrThreadDeleted
		}
		return FinishSubAgentCloseResult{}, ErrThreadNotFound
	}
	if err := lifecycleRejectsWrite(parent); err != nil {
		return FinishSubAgentCloseResult{}, err
	}
	if r.threadAuthorityClaimedLocked(parentThreadID) {
		return FinishSubAgentCloseResult{}, ErrThreadAuthorityBusy
	}
	if err := r.validatePreparedSubAgentCloseLocked(operation, nil); err != nil {
		return FinishSubAgentCloseResult{}, err
	}
	now := nonZeroAuthorityTime(req.Now, r.now)
	cancelled := make([]string, 0)
	for _, node := range operation.Nodes {
		if !node.WasOpen {
			continue
		}
		inputs := r.subAgentInputs[node.ThreadID]
		for index := range inputs {
			if inputs[index].State == SubAgentInputPending {
				inputs[index].State = SubAgentInputCancelled
				inputs[index].CancelledAt = now
				cancelled = append(cancelled, inputs[index].SubAgentInputID)
			}
		}
		r.subAgentInputs[node.ThreadID] = inputs
	}
	entries := make([]Entry, 0, len(operation.Nodes))
	threads := make([]ThreadMeta, 0, len(operation.Nodes))
	resultEntryIDs := make([]string, 0, len(operation.Nodes))
	for index := len(operation.Nodes) - 1; index >= 0; index-- {
		node := operation.Nodes[index]
		meta := r.threads[node.ThreadID]
		if node.WasOpen {
			entry := subAgentCloseLifecycleEntry(operation, node.ThreadID, meta.LeafID, r.nextEntryID(node.ThreadID), now)
			r.appendIndexedEntriesLocked(node.ThreadID, entry)
			meta.LeafID = entry.ID
			meta.Lifecycle = ThreadLifecycleClosed
			meta.CloseOperationID = ""
			meta.UpdatedAt = now
			r.threads[node.ThreadID] = meta
			entries = append(entries, cloneEntry(entry))
			resultEntryIDs = append(resultEntryIDs, entry.ID)
		}
		threads = append(threads, meta)
	}
	slices.Sort(cancelled)
	operation.State = SubAgentCloseCompleted
	operation.ResultEntryIDs = resultEntryIDs
	operation.FinishedAt = now
	r.subAgentCloseOperations[operationID] = operation
	return FinishSubAgentCloseResult{
		Operation: cloneSubAgentCloseOperation(operation), Threads: threads, Entries: entries,
		CancelledInputIDs: cancelled,
	}, nil
}

func (r *MemoryRepo) validatePreparedSubAgentCloseLocked(operation SubAgentCloseOperation, targetLease *TurnLease) error {
	if operation.State == SubAgentCloseCompleted {
		return nil
	}
	if operation.State != SubAgentClosePrepared || len(operation.Nodes) == 0 {
		return ErrAuthorityCorrupt
	}
	currentThreadIDs, err := threadAuthorityTreeIDsLocked(r.threads, operation.TargetThreadID)
	if err != nil || len(currentThreadIDs) != len(operation.Nodes) {
		return ErrAuthorityCorrupt
	}
	for index, threadID := range currentThreadIDs {
		if operation.Nodes[index].ThreadID != threadID {
			return ErrAuthorityCorrupt
		}
	}
	for _, node := range operation.Nodes {
		meta, ok := r.threads[node.ThreadID]
		if !ok {
			return ErrAuthorityCorrupt
		}
		lifecycle, err := canonicalThreadLifecycle(meta)
		if err != nil {
			return err
		}
		if node.WasOpen {
			if lifecycle != ThreadLifecycleClosing || meta.CloseOperationID != operation.CloseOperationID {
				return ErrAuthorityCorrupt
			}
		} else if lifecycle != ThreadLifecycleClosed || strings.TrimSpace(meta.CloseOperationID) != "" || r.hasPendingSubAgentInputLocked(node.ThreadID) {
			return ErrAuthorityCorrupt
		}
		if r.threadAuthorityClaimedLocked(node.ThreadID) {
			return ErrThreadAuthorityBusy
		}
		lease, active := r.leases[node.ThreadID]
		if !active {
			if node.ThreadID == operation.TargetThreadID && targetLease != nil {
				return ErrStaleAuthority
			}
			continue
		}
		if node.ThreadID != operation.TargetThreadID || targetLease == nil || !SameTurnLease(lease, *targetLease) || !lease.Fresh(r.now().UTC()) {
			return ErrThreadAuthorityBusy
		}
	}
	return nil
}

func (r *MemoryRepo) replayedSubAgentCloseLocked(operation SubAgentCloseOperation) (FinishSubAgentCloseResult, error) {
	threads := make([]ThreadMeta, 0, len(operation.Nodes))
	for index := len(operation.Nodes) - 1; index >= 0; index-- {
		meta, ok := r.threads[operation.Nodes[index].ThreadID]
		if !ok || !meta.IsClosed() || strings.TrimSpace(meta.CloseOperationID) != "" {
			return FinishSubAgentCloseResult{}, ErrAuthorityCorrupt
		}
		threads = append(threads, meta)
	}
	entries := make([]Entry, 0, len(operation.ResultEntryIDs))
	for _, entryID := range operation.ResultEntryIDs {
		found := false
		for _, threadEntries := range r.entries {
			for _, entry := range threadEntries {
				if entry.ID == entryID && entry.Metadata["close_operation_id"] == operation.CloseOperationID {
					entries = append(entries, cloneEntry(entry))
					found = true
					break
				}
			}
			if found {
				break
			}
		}
		if !found {
			return FinishSubAgentCloseResult{}, ErrAuthorityCorrupt
		}
	}
	return FinishSubAgentCloseResult{Operation: cloneSubAgentCloseOperation(operation), Threads: threads, Entries: entries, Replayed: true}, nil
}

func (r *MemoryRepo) hasPendingSubAgentInputLocked(threadID string) bool {
	for _, input := range r.subAgentInputs[threadID] {
		if input.State == SubAgentInputPending {
			return true
		}
	}
	return false
}

func subAgentCloseLifecycleEntry(operation SubAgentCloseOperation, threadID, parentEntryID, entryID string, now time.Time) Entry {
	entry := Entry{
		ID: entryID, ThreadID: threadID, ParentID: parentEntryID, Type: EntryCustom, CreatedAt: now,
		Metadata: map[string]string{
			"kind": "subagent_lifecycle", "type": "subagent_lifecycle", "action": "closed",
			"reason": operation.Reason, "parent_thread_id": operation.ParentThreadID,
			"close_operation_id": operation.CloseOperationID,
		},
	}
	entry.Raw = rawForEntry(entry)
	entry.RawHash = stableHash(entry.Raw)
	return entry
}

// PrepareSubAgentCloseLifecycleEntry builds the canonical lifecycle entry that
// FinishSubAgentClose persists atomically with terminal child state.
func PrepareSubAgentCloseLifecycleEntry(operation SubAgentCloseOperation, threadID, parentEntryID, entryID string, now time.Time) Entry {
	return subAgentCloseLifecycleEntry(operation, threadID, parentEntryID, entryID, now)
}

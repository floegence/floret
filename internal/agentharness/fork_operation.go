package agentharness

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/floegence/floret/internal/sessiontree"
	"github.com/floegence/floret/internal/storage"
)

const forkOperationPlanVersion = 2

const (
	forkErrorDestinationConflict = "destination_conflict"
)

type forkOperationPlan struct {
	Version            int            `json:"version"`
	OperationID        string         `json:"operation_id"`
	RequestFingerprint string         `json:"request_fingerprint"`
	PreparedAt         time.Time      `json:"prepared_at"`
	Root               forkPlanNode   `json:"root"`
	TerminalChildren   []forkPlanNode `json:"terminal_children,omitempty"`
}

type forkPlanNode struct {
	NodeID              string                           `json:"node_id"`
	SourceThreadID      string                           `json:"source_thread_id"`
	SourceEntryID       string                           `json:"source_entry_id,omitempty"`
	DestinationThreadID string                           `json:"destination_thread_id"`
	TurnIDMap           map[string]string                `json:"turn_id_map,omitempty"`
	RunIDMap            map[string]string                `json:"run_id_map,omitempty"`
	DestinationMeta     *sessiontree.ForkDestinationMeta `json:"destination_meta,omitempty"`
}

type persistedForkResult struct {
	OperationID string        `json:"operation_id"`
	Summary     ThreadSummary `json:"thread"`
}

type forkRequestIdentity struct {
	SourceThreadID        string                   `json:"source_thread_id"`
	SourceEntryID         string                   `json:"source_entry_id,omitempty"`
	Position              sessiontree.ForkPosition `json:"position"`
	DestinationThreadID   string                   `json:"destination_thread_id"`
	RewriteTurnIdentities bool                     `json:"rewrite_turn_identities"`
}

func (h *AgentHarness) forkThreadReplayable(ctx context.Context, opts ForkOptions) (ForkResult, error) {
	if h == nil || h.options.ForkOperations == nil {
		return ForkResult{}, errors.New("fork operation store is required")
	}
	opts.OperationID = strings.TrimSpace(opts.OperationID)
	fingerprint, err := forkRequestFingerprint(opts)
	if err != nil {
		return ForkResult{}, err
	}
	if existing, err := h.options.ForkOperations.ForkOperation(ctx, opts.OperationID); err == nil {
		if existing.RequestFingerprint != fingerprint {
			return ForkResult{}, ErrForkOperationConflict
		}
		plan, err := decodeForkOperationPlan(existing.Plan)
		if err != nil {
			return ForkResult{}, fmt.Errorf("decode fork operation plan: %w", err)
		}
		if err := validateForkOperationPlan(plan, opts.OperationID, fingerprint); err != nil {
			return ForkResult{}, ErrForkOperationConflict
		}
		return h.resumeForkOperation(ctx, existing, plan)
	} else if !errors.Is(err, storage.ErrForkOperationNotFound) {
		return ForkResult{}, err
	}
	plan, err := h.prepareForkOperationPlan(ctx, opts, fingerprint)
	if err != nil {
		return ForkResult{}, err
	}
	planJSON, err := json.Marshal(plan)
	if err != nil {
		return ForkResult{}, err
	}
	record, _, err := h.options.ForkOperations.PrepareForkOperation(ctx, storage.ForkOperationRecord{
		OperationID:        opts.OperationID,
		RequestFingerprint: fingerprint,
		State:              storage.ForkOperationPrepared,
		Plan:               planJSON,
		CreatedAt:          plan.PreparedAt,
		UpdatedAt:          plan.PreparedAt,
	})
	if err != nil {
		return ForkResult{}, err
	}
	if record.RequestFingerprint != fingerprint {
		return ForkResult{}, ErrForkOperationConflict
	}
	plan, err = decodeForkOperationPlan(record.Plan)
	if err != nil {
		return ForkResult{}, fmt.Errorf("decode fork operation plan: %w", err)
	}
	if err := validateForkOperationPlan(plan, opts.OperationID, fingerprint); err != nil {
		return ForkResult{}, ErrForkOperationConflict
	}
	return h.resumeForkOperation(ctx, record, plan)
}

func (h *AgentHarness) prepareForkOperationPlan(ctx context.Context, opts ForkOptions, fingerprint string) (forkOperationPlan, error) {
	preparedAt := h.now()
	root, err := h.prepareForkPlanNode(ctx, "root", opts.SourceThreadID, opts.EntryID, opts.Position, opts.NewThreadID, opts.RewriteTurnIdentities)
	if err != nil {
		return forkOperationPlan{}, err
	}
	reserved := map[string]struct{}{root.DestinationThreadID: {}}
	children, err := h.childThreadMetas(ctx, opts.SourceThreadID)
	if err != nil {
		return forkOperationPlan{}, err
	}
	slices.SortFunc(children, func(left, right sessiontree.ThreadMeta) int {
		return strings.Compare(left.ID, right.ID)
	})
	terminal := make([]forkPlanNode, 0)
	for _, meta := range children {
		snapshot, err := h.subAgentSnapshotFromMeta(ctx, meta)
		if err != nil {
			return forkOperationPlan{}, err
		}
		if !isTerminalSubAgentStatus(snapshot.Status) {
			continue
		}
		destinationID, err := h.nextForkDestinationThreadID(ctx, reserved)
		if err != nil {
			return forkOperationPlan{}, err
		}
		reserved[destinationID] = struct{}{}
		node, err := h.prepareForkPlanNode(ctx, fmt.Sprintf("terminal-child-%d", len(terminal)+1), meta.ID, meta.LeafID, sessiontree.ForkAt, destinationID, true)
		if err != nil {
			return forkOperationPlan{}, err
		}
		node.DestinationMeta = &sessiontree.ForkDestinationMeta{
			ParentThreadID:  root.DestinationThreadID,
			ParentTurnID:    forkMappedID(meta.ParentTurnID, root.TurnIDMap),
			TaskName:        meta.TaskName,
			TaskDescription: meta.TaskDescription,
			AgentPath:       meta.AgentPath,
			HostProfileRef:  meta.HostProfileRef,
			ForkMode:        meta.ForkMode,
			Closed:          meta.Closed,
			Status:          meta.Status,
		}
		terminal = append(terminal, node)
	}
	return forkOperationPlan{
		Version:            forkOperationPlanVersion,
		OperationID:        opts.OperationID,
		RequestFingerprint: fingerprint,
		PreparedAt:         preparedAt,
		Root:               root,
		TerminalChildren:   terminal,
	}, nil
}

func (h *AgentHarness) prepareForkPlanNode(ctx context.Context, nodeID, sourceThreadID, entryID string, position sessiontree.ForkPosition, destinationThreadID string, rewriteIdentities bool) (forkPlanNode, error) {
	path, err := h.forkSourcePath(ctx, ForkOptions{SourceThreadID: sourceThreadID, EntryID: entryID, Position: position})
	if err != nil {
		return forkPlanNode{}, err
	}
	resolvedEntryID := ""
	if len(path) > 0 {
		resolvedEntryID = path[len(path)-1].ID
	}
	var turnIDs map[string]string
	var runIDs map[string]string
	if rewriteIdentities {
		turnIDs, runIDs = h.forkIdentityRewriteFromPath(path)
	}
	return forkPlanNode{
		NodeID:              nodeID,
		SourceThreadID:      strings.TrimSpace(sourceThreadID),
		SourceEntryID:       resolvedEntryID,
		DestinationThreadID: strings.TrimSpace(destinationThreadID),
		TurnIDMap:           turnIDs,
		RunIDMap:            runIDs,
	}, nil
}

func (h *AgentHarness) resumeForkOperation(ctx context.Context, record storage.ForkOperationRecord, plan forkOperationPlan) (ForkResult, error) {
	switch record.State {
	case storage.ForkOperationCompleted:
		if err := h.validateCompletedForkTargets(ctx, plan); err != nil {
			return ForkResult{}, err
		}
		return h.decodeForkResult(record.Result, plan.OperationID, plan.Root.DestinationThreadID)
	case storage.ForkOperationFailed:
		return ForkResult{}, persistedForkError(record.ErrorCode, record.ErrorMessage)
	case storage.ForkOperationPrepared:
	default:
		return ForkResult{}, fmt.Errorf("invalid fork operation state %q", record.State)
	}

	nodes := append([]forkPlanNode{plan.Root}, plan.TerminalChildren...)
	for _, node := range nodes {
		if err := h.executeForkPlanNode(ctx, plan, node); err != nil {
			if errors.Is(err, sessiontree.ErrForkDestinationConflict) {
				return ForkResult{}, h.failForkOperation(ctx, record, forkErrorDestinationConflict, err)
			}
			return ForkResult{}, err
		}
	}
	thread := h.cacheThread(plan.Root.DestinationThreadID)
	summary, err := thread.Summary(ctx)
	if err != nil {
		return ForkResult{}, err
	}
	resultJSON, err := json.Marshal(persistedForkResult{OperationID: plan.OperationID, Summary: summary})
	if err != nil {
		return ForkResult{}, err
	}
	finishedAt := h.now()
	record.State = storage.ForkOperationCompleted
	record.Result = resultJSON
	record.UpdatedAt = finishedAt
	record.FinishedAt = finishedAt
	if err := h.options.ForkOperations.UpdateForkOperation(ctx, record); err != nil {
		if errors.Is(err, storage.ErrForkOperationConflict) {
			current, loadErr := h.options.ForkOperations.ForkOperation(ctx, record.OperationID)
			if loadErr == nil && current.State == storage.ForkOperationCompleted {
				if validateErr := h.validateCompletedForkTargets(ctx, plan); validateErr != nil {
					return ForkResult{}, validateErr
				}
				return h.decodeForkResult(current.Result, plan.OperationID, plan.Root.DestinationThreadID)
			}
		}
		return ForkResult{}, err
	}
	return h.decodeForkResult(resultJSON, plan.OperationID, plan.Root.DestinationThreadID)
}

func (h *AgentHarness) executeForkPlanNode(ctx context.Context, plan forkOperationPlan, node forkPlanNode) error {
	_, err := h.options.Repo.Fork(ctx, sessiontree.ForkOptions{
		SourceThreadID:  node.SourceThreadID,
		EntryID:         node.SourceEntryID,
		EntryIDPinned:   true,
		Position:        sessiontree.ForkAt,
		NewThreadID:     node.DestinationThreadID,
		OperationID:     plan.OperationID,
		OperationNodeID: node.NodeID,
		Now:             plan.PreparedAt,
		TurnIDMap:       node.TurnIDMap,
		RunIDMap:        node.RunIDMap,
		DestinationMeta: node.DestinationMeta,
		RewriteEntry:    rewriteForkContextEntry,
	})
	if err != nil {
		return err
	}
	h.emit(HarnessEvent{Type: EventThreadForked, ThreadID: node.DestinationThreadID, EntryID: node.SourceEntryID, Metadata: map[string]string{"source_thread_id": node.SourceThreadID, "fork_operation_id": plan.OperationID, "fork_operation_node_id": node.NodeID}})
	return nil
}

func (h *AgentHarness) validateCompletedForkTargets(ctx context.Context, plan forkOperationPlan) error {
	nodes := append([]forkPlanNode{plan.Root}, plan.TerminalChildren...)
	for _, node := range nodes {
		meta, err := h.options.Repo.Thread(ctx, node.DestinationThreadID)
		if errors.Is(err, sessiontree.ErrThreadNotFound) {
			return fmt.Errorf("%w: %s", ErrForkOperationTargetMissing, node.DestinationThreadID)
		}
		if err != nil {
			return err
		}
		if meta.ForkOperationID != plan.OperationID || meta.ForkOperationNodeID != node.NodeID || meta.ForkedFromThreadID != node.SourceThreadID || meta.ForkedFromEntryID != node.SourceEntryID || !sessiontree.MatchesForkDestinationMeta(meta, node.DestinationMeta) {
			return sessiontree.ErrForkDestinationConflict
		}
	}
	return nil
}

func (h *AgentHarness) failForkOperation(ctx context.Context, record storage.ForkOperationRecord, code string, cause error) error {
	finishedAt := h.now()
	record.State = storage.ForkOperationFailed
	record.ErrorCode = code
	record.ErrorMessage = cause.Error()
	record.UpdatedAt = finishedAt
	record.FinishedAt = finishedAt
	if err := h.options.ForkOperations.UpdateForkOperation(ctx, record); err != nil && !errors.Is(err, storage.ErrForkOperationConflict) {
		return errors.Join(cause, err)
	}
	return persistedForkError(code, cause.Error())
}

func persistedForkError(code, message string) error {
	switch code {
	case forkErrorDestinationConflict:
		return fmt.Errorf("%w: %s", sessiontree.ErrForkDestinationConflict, message)
	default:
		return fmt.Errorf("unsupported persisted fork error code %q", code)
	}
}

func (h *AgentHarness) decodeForkResult(data json.RawMessage, operationID, destinationThreadID string) (ForkResult, error) {
	var persisted persistedForkResult
	if err := decodeStrictJSON(data, &persisted); err != nil {
		return ForkResult{}, fmt.Errorf("decode fork operation result: %w", err)
	}
	if persisted.OperationID != strings.TrimSpace(operationID) || persisted.Summary.ID != strings.TrimSpace(destinationThreadID) {
		return ForkResult{}, ErrForkOperationConflict
	}
	return ForkResult{
		OperationID: persisted.OperationID,
		Thread:      h.cacheThread(persisted.Summary.ID),
		Summary:     persisted.Summary,
	}, nil
}

func decodeForkOperationPlan(data json.RawMessage) (forkOperationPlan, error) {
	var plan forkOperationPlan
	if err := decodeStrictJSON(data, &plan); err != nil {
		return forkOperationPlan{}, err
	}
	return plan, nil
}

func validateForkOperationPlan(plan forkOperationPlan, operationID, fingerprint string) error {
	if plan.Version != forkOperationPlanVersion ||
		plan.OperationID != strings.TrimSpace(operationID) ||
		plan.RequestFingerprint != strings.TrimSpace(fingerprint) ||
		plan.PreparedAt.IsZero() {
		return ErrForkOperationConflict
	}
	nodes := append([]forkPlanNode{plan.Root}, plan.TerminalChildren...)
	seen := make(map[string]struct{}, len(nodes))
	for index, node := range nodes {
		if strings.TrimSpace(node.NodeID) == "" || strings.TrimSpace(node.SourceThreadID) == "" || strings.TrimSpace(node.DestinationThreadID) == "" {
			return ErrForkOperationConflict
		}
		if _, duplicate := seen[node.DestinationThreadID]; duplicate {
			return ErrForkOperationConflict
		}
		seen[node.DestinationThreadID] = struct{}{}
		if index == 0 {
			if node.NodeID != "root" || node.DestinationMeta != nil {
				return ErrForkOperationConflict
			}
			continue
		}
		meta := node.DestinationMeta
		if meta == nil ||
			strings.TrimSpace(meta.ParentThreadID) != plan.Root.DestinationThreadID ||
			strings.TrimSpace(meta.TaskName) == "" ||
			strings.TrimSpace(meta.AgentPath) == "" {
			return ErrForkOperationConflict
		}
	}
	return nil
}

func decodeStrictJSON(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return errors.New("unexpected trailing JSON value")
		}
		return err
	}
	return nil
}

func forkRequestFingerprint(opts ForkOptions) (string, error) {
	position := opts.Position
	if position == "" {
		position = sessiontree.ForkAt
	}
	data, err := json.Marshal(forkRequestIdentity{
		SourceThreadID:        strings.TrimSpace(opts.SourceThreadID),
		SourceEntryID:         strings.TrimSpace(opts.EntryID),
		Position:              position,
		DestinationThreadID:   strings.TrimSpace(opts.NewThreadID),
		RewriteTurnIdentities: opts.RewriteTurnIdentities,
	})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func (h *AgentHarness) nextForkDestinationThreadID(ctx context.Context, reserved map[string]struct{}) (string, error) {
	for i := 0; i < 100; i++ {
		id := strings.TrimSpace(h.nextID("subagent"))
		if id == "" {
			continue
		}
		if _, exists := reserved[id]; exists {
			continue
		}
		if _, err := h.options.Repo.Thread(ctx, id); errors.Is(err, sessiontree.ErrThreadNotFound) {
			return id, nil
		} else if err != nil {
			return "", err
		}
	}
	return "", errors.New("unable to allocate unique fork destination thread id")
}

func (h *AgentHarness) forkIdentityRewriteFromPath(path []sessiontree.Entry) (map[string]string, map[string]string) {
	turnIDs := map[string]string{}
	runIDs := map[string]string{}
	for _, entry := range path {
		sourceTurnID := strings.TrimSpace(entry.TurnID)
		if sourceTurnID == "" {
			continue
		}
		destinationTurnID := turnIDs[sourceTurnID]
		if destinationTurnID == "" {
			destinationTurnID = h.nextID("turn")
			turnIDs[sourceTurnID] = destinationTurnID
		}
		sourceRunID := strings.TrimSpace(entry.Metadata["run_id"])
		if sourceRunID == "" {
			continue
		}
		destinationRunID := runIDs[sourceRunID]
		if destinationRunID == "" {
			destinationRunID = h.nextID("run")
			runIDs[sourceRunID] = destinationRunID
		}
	}
	return turnIDs, runIDs
}

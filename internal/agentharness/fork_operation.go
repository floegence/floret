package agentharness

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/floegence/floret/internal/sessiontree"
	"github.com/floegence/floret/internal/storage"
)

const forkOperationPlanVersion = 1

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
	NodeID              string                   `json:"node_id"`
	SourceThreadID      string                   `json:"source_thread_id"`
	SourceEntryID       string                   `json:"source_entry_id,omitempty"`
	DestinationThreadID string                   `json:"destination_thread_id"`
	TurnIDMap           map[string]string        `json:"turn_id_map,omitempty"`
	RunIDMap            map[string]string        `json:"run_id_map,omitempty"`
	Turns               []ForkedTurnRef          `json:"turns,omitempty"`
	MetadataPatch       *forkThreadMetadataPatch `json:"metadata_patch,omitempty"`
}

type forkThreadMetadataPatch struct {
	ParentThreadID  string `json:"parent_thread_id"`
	ParentTurnID    string `json:"parent_turn_id,omitempty"`
	TaskName        string `json:"task_name,omitempty"`
	TaskDescription string `json:"task_description,omitempty"`
	AgentPath       string `json:"agent_path,omitempty"`
	HostProfileRef  string `json:"host_profile_ref,omitempty"`
	ForkMode        string `json:"fork_mode,omitempty"`
	Closed          bool   `json:"closed,omitempty"`
	Status          string `json:"status,omitempty"`
}

type persistedForkResult struct {
	OperationID string          `json:"operation_id"`
	Summary     ThreadSummary   `json:"thread"`
	Turns       []ForkedTurnRef `json:"turns,omitempty"`
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
		var plan forkOperationPlan
		if err := json.Unmarshal(existing.Plan, &plan); err != nil {
			return ForkResult{}, fmt.Errorf("decode fork operation plan: %w", err)
		}
		if plan.Version != forkOperationPlanVersion || plan.OperationID != opts.OperationID || plan.RequestFingerprint != fingerprint {
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
	if err := json.Unmarshal(record.Plan, &plan); err != nil {
		return ForkResult{}, fmt.Errorf("decode fork operation plan: %w", err)
	}
	if plan.Version != forkOperationPlanVersion || plan.OperationID != opts.OperationID || plan.RequestFingerprint != fingerprint {
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
		node.MetadataPatch = &forkThreadMetadataPatch{
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
	var turns []ForkedTurnRef
	if rewriteIdentities {
		turnIDs, runIDs, turns = h.forkIdentityRewriteFromPath(path)
	}
	return forkPlanNode{
		NodeID:              nodeID,
		SourceThreadID:      strings.TrimSpace(sourceThreadID),
		SourceEntryID:       resolvedEntryID,
		DestinationThreadID: strings.TrimSpace(destinationThreadID),
		TurnIDMap:           turnIDs,
		RunIDMap:            runIDs,
		Turns:               turns,
	}, nil
}

func (h *AgentHarness) resumeForkOperation(ctx context.Context, record storage.ForkOperationRecord, plan forkOperationPlan) (ForkResult, error) {
	switch record.State {
	case storage.ForkOperationCompleted:
		if err := h.validateCompletedForkTargets(ctx, plan); err != nil {
			return ForkResult{}, err
		}
		return h.decodeForkResult(record.Result)
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
	resultJSON, err := json.Marshal(persistedForkResult{OperationID: plan.OperationID, Summary: summary, Turns: plan.Root.Turns})
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
				return h.decodeForkResult(current.Result)
			}
		}
		return ForkResult{}, err
	}
	return h.decodeForkResult(resultJSON)
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
	})
	if err != nil {
		return err
	}
	if node.MetadataPatch != nil {
		meta, err := h.options.Repo.Thread(ctx, node.DestinationThreadID)
		if err != nil {
			return err
		}
		patch := node.MetadataPatch
		meta.ParentThreadID = patch.ParentThreadID
		meta.ParentTurnID = patch.ParentTurnID
		meta.TaskName = patch.TaskName
		meta.TaskDescription = patch.TaskDescription
		meta.AgentPath = patch.AgentPath
		meta.HostProfileRef = patch.HostProfileRef
		meta.ForkMode = patch.ForkMode
		meta.Closed = patch.Closed
		meta.Status = patch.Status
		meta.UpdatedAt = plan.PreparedAt
		if err := h.options.Repo.UpdateThread(ctx, meta); err != nil {
			return err
		}
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
		if meta.ForkOperationID != plan.OperationID || meta.ForkOperationNodeID != node.NodeID || meta.ForkedFromThreadID != node.SourceThreadID || meta.ForkedFromEntryID != node.SourceEntryID {
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

func (h *AgentHarness) decodeForkResult(data json.RawMessage) (ForkResult, error) {
	var persisted persistedForkResult
	if err := json.Unmarshal(data, &persisted); err != nil {
		return ForkResult{}, fmt.Errorf("decode fork operation result: %w", err)
	}
	return ForkResult{
		OperationID: persisted.OperationID,
		Thread:      h.cacheThread(persisted.Summary.ID),
		Summary:     persisted.Summary,
		Turns:       append([]ForkedTurnRef(nil), persisted.Turns...),
	}, nil
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

func (h *AgentHarness) forkIdentityRewriteFromPath(path []sessiontree.Entry) (map[string]string, map[string]string, []ForkedTurnRef) {
	turnIDs := map[string]string{}
	runIDs := map[string]string{}
	refsByTurn := map[string]*ForkedTurnRef{}
	order := make([]string, 0)
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
		ref := refsByTurn[sourceTurnID]
		if ref == nil {
			ref = &ForkedTurnRef{SourceTurnID: sourceTurnID, DestinationTurnID: destinationTurnID, CreatedAt: entry.CreatedAt}
			refsByTurn[sourceTurnID] = ref
			order = append(order, sourceTurnID)
		}
		if ref.CreatedAt.IsZero() || (!entry.CreatedAt.IsZero() && entry.CreatedAt.Before(ref.CreatedAt)) {
			ref.CreatedAt = entry.CreatedAt
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
		if ref.SourceRunID == "" {
			ref.SourceRunID = sourceRunID
			ref.DestinationRunID = destinationRunID
		}
	}
	refs := make([]ForkedTurnRef, 0, len(order))
	for _, turnID := range order {
		if ref := refsByTurn[turnID]; ref != nil {
			refs = append(refs, *ref)
		}
	}
	return turnIDs, runIDs, refs
}

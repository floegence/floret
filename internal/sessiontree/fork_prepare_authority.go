package sessiontree

import (
	"context"
	"errors"
	"strings"
)

// ForkPrepareThreadState is the transaction-local source state used to validate
// one complete replayable fork plan before any structural claim is published.
type ForkPrepareThreadState struct {
	Meta              ThreadMeta
	Path              []Entry
	PinnedPath        []Entry
	PendingInputCount int
}

// ValidateForkPrepareState rejects a plan whose pinned source snapshot or
// terminal-child set no longer matches the canonical state at claim time.
func ValidateForkPrepareState(rootThreadID string, nodes []ForkOptions, states []ForkPrepareThreadState) error {
	rootThreadID = strings.TrimSpace(rootThreadID)
	if rootThreadID == "" || len(nodes) == 0 || len(states) == 0 {
		return ErrInvalidThreadAuthority
	}
	stateByID := make(map[string]ForkPrepareThreadState, len(states))
	for _, state := range states {
		threadID := strings.TrimSpace(state.Meta.ID)
		if threadID == "" {
			return ErrInvalidThreadAuthority
		}
		if _, duplicate := stateByID[threadID]; duplicate {
			return ErrAuthorityCorrupt
		}
		stateByID[threadID] = state
	}
	rootState, ok := stateByID[rootThreadID]
	if !ok {
		return ErrThreadNotFound
	}
	rootLifecycle, err := canonicalThreadLifecycle(rootState.Meta)
	if err != nil {
		return err
	}
	if rootLifecycle != ThreadLifecycleOpen || strings.TrimSpace(rootState.Meta.ParentThreadID) != "" {
		return ErrInvalidThreadAuthority
	}

	nodeBySource := make(map[string]ForkOptions, len(nodes))
	var rootNode ForkOptions
	for _, node := range nodes {
		sourceID := strings.TrimSpace(node.SourceThreadID)
		if sourceID == "" || strings.TrimSpace(node.NewThreadID) == "" || !node.EntryIDPinned {
			return ErrInvalidThreadAuthority
		}
		if _, duplicate := nodeBySource[sourceID]; duplicate {
			return ErrInvalidThreadAuthority
		}
		nodeBySource[sourceID] = node
		if strings.TrimSpace(node.OperationNodeID) == "root" {
			if rootNode.SourceThreadID != "" || sourceID != rootThreadID || node.DestinationMeta != nil {
				return ErrInvalidThreadAuthority
			}
			rootNode = node
		}
	}
	if rootNode.SourceThreadID == "" {
		return ErrInvalidThreadAuthority
	}
	if err := validateForkPrepareSource(rootNode, rootState); err != nil {
		return err
	}

	terminalChildren := make(map[string]ForkPrepareThreadState)
	for threadID, state := range stateByID {
		if threadID == rootThreadID {
			continue
		}
		if strings.TrimSpace(state.Meta.ParentThreadID) != rootThreadID {
			return ErrInvalidThreadAuthority
		}
		lifecycle, err := canonicalThreadLifecycle(state.Meta)
		if err != nil {
			return err
		}
		if lifecycle == ThreadLifecycleClosing {
			return ErrSubAgentClosing
		}
		if forkPrepareChildTerminal(state, lifecycle) {
			terminalChildren[threadID] = state
		}
	}
	if len(nodeBySource) != len(terminalChildren)+1 {
		return ErrStaleAuthority
	}
	for threadID, state := range terminalChildren {
		node, ok := nodeBySource[threadID]
		if !ok || strings.TrimSpace(node.OperationNodeID) == "root" || node.DestinationMeta == nil {
			return ErrStaleAuthority
		}
		if err := validateForkPrepareSource(node, state); err != nil {
			return err
		}
		if !forkPrepareDestinationMetaMatches(rootNode, state.Meta, node.DestinationMeta) {
			return ErrStaleAuthority
		}
	}
	for sourceID := range nodeBySource {
		if sourceID == rootThreadID {
			continue
		}
		if _, ok := terminalChildren[sourceID]; !ok {
			return ErrStaleAuthority
		}
	}
	return nil
}

func validateForkPrepareSource(node ForkOptions, state ForkPrepareThreadState) error {
	if strings.TrimSpace(node.ExpectedSourceLeafID) != strings.TrimSpace(state.Meta.LeafID) {
		return ErrStaleAuthority
	}
	pathLeafID := ""
	if len(state.PinnedPath) > 0 {
		pathLeafID = strings.TrimSpace(state.PinnedPath[len(state.PinnedPath)-1].ID)
	}
	if pathLeafID != strings.TrimSpace(node.EntryID) {
		return ErrStaleAuthority
	}
	if err := ValidateThreadAuthorityState(state.Path, nil, ""); err != nil {
		return err
	}
	return nil
}

func forkPrepareChildTerminal(state ForkPrepareThreadState, lifecycle ThreadLifecycle) bool {
	if lifecycle == ThreadLifecycleClosed {
		return true
	}
	if lifecycle != ThreadLifecycleOpen || state.PendingInputCount != 0 {
		return false
	}
	latest := TurnMarkerStatus("")
	for _, entry := range state.Path {
		if entry.Type != EntryTurnMarker {
			continue
		}
		switch entry.TurnStatus {
		case TurnStarted, TurnCompleted, TurnWaiting, TurnFailed, TurnAborted:
			latest = entry.TurnStatus
		}
	}
	switch latest {
	case TurnCompleted, TurnFailed, TurnAborted:
		return true
	default:
		return false
	}
}

func forkPrepareDestinationMetaMatches(rootNode ForkOptions, source ThreadMeta, destination *ForkDestinationMeta) bool {
	if destination == nil {
		return false
	}
	return strings.TrimSpace(destination.ParentThreadID) == strings.TrimSpace(rootNode.NewThreadID) &&
		strings.TrimSpace(destination.ParentTurnID) == rewriteForkID(strings.TrimSpace(source.ParentTurnID), rootNode.TurnIDMap) &&
		strings.TrimSpace(destination.TaskName) == strings.TrimSpace(source.TaskName) &&
		strings.TrimSpace(destination.TaskDescription) == strings.TrimSpace(source.TaskDescription) &&
		strings.TrimSpace(destination.AgentPath) == strings.TrimSpace(source.AgentPath) &&
		strings.TrimSpace(destination.HostProfileRef) == strings.TrimSpace(source.HostProfileRef) &&
		strings.TrimSpace(destination.ForkMode) == strings.TrimSpace(source.ForkMode) &&
		destination.Lifecycle == source.Lifecycle
}

// PrepareForkClaim validates and publishes one complete Memory authority claim
// in the same critical section.
func (r *MemoryRepo) PrepareForkClaim(ctx context.Context, operationID, rootThreadID string, nodes []ForkOptions) error {
	_ = ctx
	operationID = strings.TrimSpace(operationID)
	if operationID == "" {
		return errors.New("fork prepare claim operation id is required")
	}
	sources, destinations, authority, err := validateForkBatchNodes(operationID, nodes)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	root, ok := r.threads[strings.TrimSpace(rootThreadID)]
	if !ok {
		return ErrThreadNotFound
	}
	states := make([]ForkPrepareThreadState, 0)
	nodeBySource := make(map[string]ForkOptions, len(nodes))
	for _, node := range nodes {
		nodeBySource[strings.TrimSpace(node.SourceThreadID)] = node
	}
	appendState := func(meta ThreadMeta) error {
		path, err := pathLocked(r.threads, r.entries, meta.ID, meta.LeafID)
		if err != nil {
			return err
		}
		pending := 0
		for _, input := range r.subAgentInputs[meta.ID] {
			if input.State == SubAgentInputPending {
				pending++
			}
		}
		var pinned []Entry
		if node, ok := nodeBySource[meta.ID]; ok {
			pinned, err = pathLocked(r.threads, r.entries, meta.ID, node.EntryID)
			if err != nil {
				return err
			}
		}
		states = append(states, ForkPrepareThreadState{Meta: meta, Path: path, PinnedPath: pinned, PendingInputCount: pending})
		return nil
	}
	if err := appendState(root); err != nil {
		return err
	}
	for _, meta := range r.threads {
		if strings.TrimSpace(meta.ParentThreadID) == strings.TrimSpace(rootThreadID) {
			if err := appendState(meta); err != nil {
				return err
			}
		}
	}
	if err := ValidateForkPrepareState(rootThreadID, nodes, states); err != nil {
		return err
	}
	for _, state := range states {
		node, ok := nodeBySource[state.Meta.ID]
		if !ok {
			continue
		}
		if err := r.validateArtifactClosureLocked(state.Meta.ID, node.NewThreadID, state.PinnedPath, node.ArtifactClosure); err != nil {
			return err
		}
	}
	for _, threadID := range sources {
		if _, leased := r.leases[threadID]; leased {
			return ErrActiveTurn
		}
	}
	sourceSet := stringSet(sources)
	for _, threadID := range destinations {
		if _, exists := r.threads[threadID]; exists {
			return ErrForkDestinationConflict
		}
		if _, deleted := r.tombstones[threadID]; deleted {
			return ErrForkDestinationConflict
		}
	}
	for _, threadID := range authority {
		if owner := strings.TrimSpace(r.authorityClaims[threadID]); owner != "" && owner != operationID {
			return ErrThreadAuthorityBusy
		}
		if _, source := sourceSet[threadID]; !source {
			if _, leased := r.leases[threadID]; leased {
				return ErrThreadAuthorityBusy
			}
		}
	}
	for _, threadID := range authority {
		r.authorityClaims[threadID] = operationID
	}
	return nil
}

package agentharness

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/floegence/floret/v2/internal/sessiontree"
	"github.com/floegence/floret/v2/observation"
)

// ListPendingToolSettlementTargets returns every active pending tool target on
// the thread's canonical path. It intentionally does not use detail-event
// projections, whose pagination and sanitization are presentation concerns.
func (h *AgentHarness) ListPendingToolSettlementTargets(ctx context.Context, threadID string) ([]sessiontree.PendingToolSettlementTarget, error) {
	if h == nil || h.options.Repo == nil {
		return nil, errors.New("agent harness is not initialized")
	}
	return h.listPendingToolSettlementTargets(ctx, strings.TrimSpace(threadID))
}

// ListSubAgentPendingToolSettlementTargets returns canonical pending targets
// for one direct child of the supplied parent.
func (h *AgentHarness) ListSubAgentPendingToolSettlementTargets(ctx context.Context, parentThreadID, childThreadID string) ([]sessiontree.PendingToolSettlementTarget, error) {
	if h == nil || h.options.Repo == nil {
		return nil, errors.New("agent harness is not initialized")
	}
	meta, err := h.resolveSubAgentMeta(ctx, parentThreadID, childThreadID)
	if err != nil {
		return nil, err
	}
	return h.listPendingToolSettlementTargets(ctx, meta.ID)
}

func (h *AgentHarness) listPendingToolSettlementTargets(ctx context.Context, threadID string) ([]sessiontree.PendingToolSettlementTarget, error) {
	if threadID == "" {
		return nil, errors.New("thread id is required")
	}
	meta, err := h.options.Repo.Thread(ctx, threadID)
	if err != nil {
		return nil, err
	}
	if err := sessiontree.ValidateThreadMetaAuthority(meta); err != nil || meta.ID != threadID {
		return nil, sessiontree.ErrAuthorityCorrupt
	}
	path, err := h.options.Repo.Path(ctx, threadID, meta.LeafID)
	if err != nil {
		return nil, err
	}
	return pendingToolSettlementTargets(path, threadID)
}

func pendingToolSettlementTargets(path []sessiontree.Entry, threadID string) ([]sessiontree.PendingToolSettlementTarget, error) {
	runIDs := make(map[string]string)
	active := make(map[string]sessiontree.PendingToolSettlementTarget)
	for _, entry := range path {
		if entry.ThreadID != threadID {
			return nil, sessiontree.ErrAuthorityCorrupt
		}
		turnID := strings.TrimSpace(entry.TurnID)
		if entry.Type == sessiontree.EntryTurnMarker && entry.TurnStatus == sessiontree.TurnStarted {
			runID := strings.TrimSpace(entry.Metadata["run_id"])
			if turnID == "" || runID == "" {
				return nil, sessiontree.ErrAuthorityCorrupt
			}
			if _, duplicate := runIDs[turnID]; duplicate {
				return nil, sessiontree.ErrAuthorityCorrupt
			}
			runIDs[turnID] = runID
		}

		isAuthoritySettlement := entry.Type == sessiontree.EntryCustom && entry.Metadata[sessiontree.PendingToolSettlementKindKey] == sessiontree.PendingToolSettlementKind
		isProjectedSettlement := entry.Type == sessiontree.EntryCustom && entry.Metadata[subAgentDetailKindKey] == pendingToolSettlementEntryKind
		if isProjectedSettlement && !isAuthoritySettlement {
			return nil, sessiontree.ErrAuthorityCorrupt
		}
		if isAuthoritySettlement {
			target, err := pendingToolTargetFromSettlementEntry(entry, threadID)
			if err != nil {
				return nil, err
			}
			key := pendingToolTargetKey(target)
			if _, found := active[key]; !found {
				return nil, sessiontree.ErrAuthorityCorrupt
			}
			delete(active, key)
			continue
		}

		if entry.Type != sessiontree.EntryToolResult || entry.Message.ToolResult == nil ||
			strings.TrimSpace(entry.Message.ToolResult.Status) != string(observation.ActivityStatusRunning) {
			continue
		}
		runID := runIDs[turnID]
		target := sessiontree.PendingToolSettlementTarget{
			ThreadID:        threadID,
			TurnID:          turnID,
			RunID:           runID,
			ToolCallID:      strings.TrimSpace(entry.Message.ToolCallID),
			ToolName:        strings.TrimSpace(entry.Message.ToolName),
			Handle:          pendingHandleFromSessionActivity(entry.Message.Activity),
			EffectAttemptID: strings.TrimSpace(entry.Metadata[sessiontree.PendingToolEffectAttemptIDKey]),
		}
		if err := validateCanonicalPendingToolTarget(target); err != nil {
			return nil, err
		}
		key := pendingToolTargetKey(target)
		if _, duplicate := active[key]; duplicate {
			return nil, sessiontree.ErrAuthorityCorrupt
		}
		active[key] = target
	}

	targets := make([]sessiontree.PendingToolSettlementTarget, 0, len(active))
	for _, target := range active {
		targets = append(targets, target)
	}
	sort.Slice(targets, func(i, j int) bool {
		return pendingToolTargetKey(targets[i]) < pendingToolTargetKey(targets[j])
	})
	return targets, nil
}

func pendingToolTargetFromSettlementEntry(entry sessiontree.Entry, threadID string) (sessiontree.PendingToolSettlementTarget, error) {
	state := PendingToolSettlementStatus(strings.TrimSpace(entry.Metadata[pendingToolSettlementStateKey]))
	switch state {
	case PendingToolSettledCompleted, PendingToolSettledFailed, PendingToolSettledCanceled:
	default:
		return sessiontree.PendingToolSettlementTarget{}, sessiontree.ErrAuthorityCorrupt
	}
	target := sessiontree.PendingToolSettlementTarget{
		ThreadID:        threadID,
		TurnID:          strings.TrimSpace(entry.TurnID),
		RunID:           strings.TrimSpace(entry.Metadata[pendingToolSettlementRunIDKey]),
		ToolCallID:      strings.TrimSpace(entry.Message.ToolCallID),
		ToolName:        strings.TrimSpace(entry.Message.ToolName),
		Handle:          strings.TrimSpace(entry.Metadata[pendingToolSettlementHandleKey]),
		EffectAttemptID: strings.TrimSpace(entry.Metadata[sessiontree.PendingToolEffectAttemptIDKey]),
	}
	if strings.TrimSpace(entry.Metadata[sessiontree.PendingToolSettlementFingerprintKey]) == "" ||
		strings.TrimSpace(entry.Metadata[pendingToolSettlementToolIDKey]) != target.ToolCallID ||
		strings.TrimSpace(entry.Metadata[pendingToolSettlementNameKey]) != target.ToolName {
		return sessiontree.PendingToolSettlementTarget{}, sessiontree.ErrAuthorityCorrupt
	}
	if err := validateCanonicalPendingToolTarget(target); err != nil {
		return sessiontree.PendingToolSettlementTarget{}, err
	}
	return target, nil
}

func validateCanonicalPendingToolTarget(target sessiontree.PendingToolSettlementTarget) error {
	if strings.TrimSpace(target.ThreadID) == "" || strings.TrimSpace(target.TurnID) == "" || strings.TrimSpace(target.RunID) == "" ||
		strings.TrimSpace(target.ToolCallID) == "" || strings.TrimSpace(target.ToolName) == "" || strings.TrimSpace(target.Handle) == "" {
		return fmt.Errorf("%w: pending tool target has incomplete identity", sessiontree.ErrAuthorityCorrupt)
	}
	return nil
}

func pendingToolTargetKey(target sessiontree.PendingToolSettlementTarget) string {
	return strings.Join([]string{target.ThreadID, target.TurnID, target.RunID, target.ToolCallID, target.ToolName, target.Handle, target.EffectAttemptID}, "\x00")
}

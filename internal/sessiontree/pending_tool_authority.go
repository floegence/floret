package sessiontree

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	PendingToolSettlementKindKey        = "authority_kind"
	PendingToolSettlementKind           = "pending_tool_settlement"
	PendingToolSettlementFingerprintKey = "authority_fingerprint"
	PendingToolEffectAttemptIDKey       = "effect_attempt_id"
)

type PendingToolSettlementTarget struct {
	ThreadID        string
	TurnID          string
	RunID           string
	ToolCallID      string
	ToolName        string
	Handle          string
	EffectAttemptID string
}

type SettlePendingToolRecoveryRequest struct {
	Target             PendingToolSettlementTarget
	RequestFingerprint string
	Settlement         Entry
	Now                time.Time
}

type SettlePendingToolRecoveryResult struct {
	Entry    Entry
	Replayed bool
}

type PendingToolRecoveryRepo interface {
	SettlePendingToolRecovery(context.Context, SettlePendingToolRecoveryRequest) (SettlePendingToolRecoveryResult, error)
}

var (
	ErrPendingToolTurnNotFound = errors.New("session tree pending tool turn not found")
	ErrPendingToolRunNotFound  = errors.New("session tree pending tool run not found")
	ErrPendingToolNotFound     = errors.New("session tree pending tool not found")
	ErrPendingToolNotPending   = errors.New("session tree pending tool is not pending")
)

func validatePendingToolRecoveryRequest(req SettlePendingToolRecoveryRequest) error {
	target := req.Target
	if strings.TrimSpace(target.ThreadID) == "" || strings.TrimSpace(target.TurnID) == "" || strings.TrimSpace(target.RunID) == "" ||
		strings.TrimSpace(target.ToolCallID) == "" || strings.TrimSpace(target.ToolName) == "" || strings.TrimSpace(target.Handle) == "" {
		return errors.New("pending tool recovery requires complete target identity")
	}
	if strings.TrimSpace(req.RequestFingerprint) == "" {
		return errors.New("pending tool recovery request fingerprint is required")
	}
	entry := req.Settlement
	if entry.Type != EntryCustom || entry.ThreadID != target.ThreadID || entry.TurnID != target.TurnID ||
		entry.Message.ToolCallID != target.ToolCallID || entry.Message.ToolName != target.ToolName {
		return errors.New("pending tool recovery settlement identity mismatch")
	}
	if entry.Metadata[PendingToolSettlementKindKey] != PendingToolSettlementKind ||
		entry.Metadata[PendingToolSettlementFingerprintKey] != req.RequestFingerprint ||
		strings.TrimSpace(entry.Metadata[PendingToolEffectAttemptIDKey]) != strings.TrimSpace(target.EffectAttemptID) {
		return errors.New("pending tool recovery settlement authority metadata mismatch")
	}
	return nil
}

func pendingToolRecoveryTarget(path []Entry, req SettlePendingToolRecoveryRequest) (Entry, bool, error) {
	target := req.Target
	turnFound := false
	runFound := false
	toolFound := false
	pendingFound := false
	matchingHandle := false
	for _, entry := range path {
		if entry.TurnID != target.TurnID {
			continue
		}
		turnFound = true
		if entry.Type == EntryTurnMarker && entry.TurnStatus == TurnStarted && strings.TrimSpace(entry.Metadata["run_id"]) == target.RunID {
			runFound = true
			continue
		}
		if entry.Type == EntryCustom && entry.Metadata[PendingToolSettlementKindKey] == PendingToolSettlementKind &&
			entry.Message.ToolCallID == target.ToolCallID && entry.Message.ToolName == target.ToolName {
			if entry.Metadata[PendingToolSettlementFingerprintKey] != req.RequestFingerprint ||
				strings.TrimSpace(entry.Metadata[PendingToolEffectAttemptIDKey]) != strings.TrimSpace(target.EffectAttemptID) {
				return Entry{}, false, ErrRequestConflict
			}
			return entry, true, nil
		}
		if (entry.Type != EntryToolCall && entry.Type != EntryToolResult) || entry.Message.ToolCallID != target.ToolCallID || entry.Message.ToolName != target.ToolName {
			continue
		}
		toolFound = true
		if entry.Type != EntryToolResult || entry.Message.ToolResult == nil || strings.TrimSpace(entry.Message.ToolResult.Status) != "running" {
			continue
		}
		pendingFound = true
		if entry.Message.Activity != nil && entry.Message.Activity.Payload != nil &&
			strings.TrimSpace(fmt.Sprint(entry.Message.Activity.Payload["pending_handle"])) == target.Handle &&
			strings.TrimSpace(entry.Metadata[PendingToolEffectAttemptIDKey]) == strings.TrimSpace(target.EffectAttemptID) {
			matchingHandle = true
		}
	}
	switch {
	case !turnFound:
		return Entry{}, false, ErrPendingToolTurnNotFound
	case !runFound:
		return Entry{}, false, ErrPendingToolRunNotFound
	case !toolFound:
		return Entry{}, false, ErrPendingToolNotFound
	case !pendingFound || !matchingHandle:
		return Entry{}, false, ErrPendingToolNotPending
	default:
		return Entry{}, false, nil
	}
}

func (r *MemoryRepo) SettlePendingToolRecovery(_ context.Context, req SettlePendingToolRecoveryRequest) (SettlePendingToolRecoveryResult, error) {
	if err := validatePendingToolRecoveryRequest(req); err != nil {
		return SettlePendingToolRecoveryResult{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	meta, ok := r.threads[req.Target.ThreadID]
	if !ok {
		if _, deleted := r.tombstones[req.Target.ThreadID]; deleted {
			return SettlePendingToolRecoveryResult{}, ErrThreadDeleted
		}
		return SettlePendingToolRecoveryResult{}, ErrThreadNotFound
	}
	if err := lifecycleRejectsWrite(meta); err != nil {
		return SettlePendingToolRecoveryResult{}, err
	}
	if r.threadAuthorityClaimedLocked(meta.ID) || r.leases[meta.ID].Validate() == nil {
		return SettlePendingToolRecoveryResult{}, ErrThreadAuthorityBusy
	}
	path, err := pathLocked(r.threads, r.entries, meta.ID, meta.LeafID)
	if err != nil {
		return SettlePendingToolRecoveryResult{}, err
	}
	if existing, replayed, err := pendingToolRecoveryTarget(path, req); err != nil || replayed {
		return SettlePendingToolRecoveryResult{Entry: cloneEntry(existing), Replayed: replayed}, err
	}
	entry := cloneEntry(req.Settlement)
	entry.ParentID = meta.LeafID
	entry.ID = r.nextEntryID(meta.ID)
	entry.CreatedAt = nonZeroAuthorityTime(req.Now, r.now)
	entry.Raw = rawForEntry(entry)
	entry.RawHash = stableHash(entry.Raw)
	r.appendIndexedEntriesLocked(meta.ID, entry)
	meta.LeafID = entry.ID
	meta.UpdatedAt = entry.CreatedAt
	r.threads[meta.ID] = meta
	return SettlePendingToolRecoveryResult{Entry: cloneEntry(entry)}, nil
}

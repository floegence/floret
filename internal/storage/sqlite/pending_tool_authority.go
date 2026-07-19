package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/floegence/floret/internal/sessiontree"
)

func (s *Store) SettlePendingToolRecovery(ctx context.Context, req sessiontree.SettlePendingToolRecoveryRequest) (sessiontree.SettlePendingToolRecoveryResult, error) {
	if err := validateSQLitePendingToolRecoveryRequest(req); err != nil {
		return sessiontree.SettlePendingToolRecoveryResult{}, err
	}
	threadID := strings.TrimSpace(req.Target.ThreadID)
	var result sessiontree.SettlePendingToolRecoveryResult
	err := s.withImmediate(ctx, func(tx sqlRunner) error {
		meta, err := loadThread(ctx, tx, threadID)
		if errors.Is(err, sessiontree.ErrThreadNotFound) {
			if _, tombstoneErr := loadThreadTombstone(ctx, tx, threadID); tombstoneErr == nil {
				return sessiontree.ErrThreadDeleted
			} else if !errors.Is(tombstoneErr, sql.ErrNoRows) {
				return tombstoneErr
			}
		}
		if err != nil {
			return err
		}
		if err := rejectSQLiteTurnAdmissionLifecycle(meta); err != nil {
			return err
		}
		if _, claimed, err := loadThreadAuthorityClaim(ctx, tx, threadID); err != nil {
			return err
		} else if claimed {
			return sessiontree.ErrThreadAuthorityBusy
		}
		if _, active, err := loadTurnLease(ctx, tx, threadID); err != nil {
			return err
		} else if active {
			return sessiontree.ErrThreadAuthorityBusy
		}
		path, err := pathWithRunner(ctx, tx, threadID, meta.LeafID)
		if err != nil {
			return err
		}
		if existing, replayed, err := sqlitePendingToolRecoveryTarget(path, req); err != nil || replayed {
			result = sessiontree.SettlePendingToolRecoveryResult{Entry: cloneEntry(existing), Replayed: replayed}
			return err
		}
		entry := cloneEntry(req.Settlement)
		entry.ID = ""
		entry.ParentID = meta.LeafID
		entry.CreatedAt = authorityNow(req.Now, s.now)
		entry, err = insertTurnAuthorityEntry(ctx, tx, entry)
		if err != nil {
			return err
		}
		meta.LeafID = entry.ID
		meta.UpdatedAt = entry.CreatedAt
		if err := updateThread(ctx, tx, meta); err != nil {
			return err
		}
		result = sessiontree.SettlePendingToolRecoveryResult{Entry: entry}
		return nil
	})
	return result, err
}

func validateSQLitePendingToolRecoveryRequest(req sessiontree.SettlePendingToolRecoveryRequest) error {
	target := req.Target
	if strings.TrimSpace(target.ThreadID) == "" || strings.TrimSpace(target.TurnID) == "" || strings.TrimSpace(target.RunID) == "" ||
		strings.TrimSpace(target.ToolCallID) == "" || strings.TrimSpace(target.ToolName) == "" || strings.TrimSpace(target.Handle) == "" {
		return errors.New("pending tool recovery requires complete target identity")
	}
	if strings.TrimSpace(req.RequestFingerprint) == "" {
		return errors.New("pending tool recovery request fingerprint is required")
	}
	entry := req.Settlement
	if entry.Type != sessiontree.EntryCustom || entry.ThreadID != target.ThreadID || entry.TurnID != target.TurnID ||
		entry.Message.ToolCallID != target.ToolCallID || entry.Message.ToolName != target.ToolName {
		return errors.New("pending tool recovery settlement identity mismatch")
	}
	if entry.Metadata[sessiontree.PendingToolSettlementKindKey] != sessiontree.PendingToolSettlementKind ||
		entry.Metadata[sessiontree.PendingToolSettlementFingerprintKey] != req.RequestFingerprint ||
		strings.TrimSpace(entry.Metadata[sessiontree.PendingToolEffectAttemptIDKey]) != strings.TrimSpace(target.EffectAttemptID) {
		return errors.New("pending tool recovery settlement authority metadata mismatch")
	}
	return nil
}

func sqlitePendingToolRecoveryTarget(path []sessiontree.Entry, req sessiontree.SettlePendingToolRecoveryRequest) (sessiontree.Entry, bool, error) {
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
		if entry.Type == sessiontree.EntryTurnMarker && entry.TurnStatus == sessiontree.TurnStarted && strings.TrimSpace(entry.Metadata["run_id"]) == target.RunID {
			runFound = true
			continue
		}
		if entry.Type == sessiontree.EntryCustom && entry.Metadata[sessiontree.PendingToolSettlementKindKey] == sessiontree.PendingToolSettlementKind &&
			entry.Message.ToolCallID == target.ToolCallID && entry.Message.ToolName == target.ToolName {
			if entry.Metadata[sessiontree.PendingToolSettlementFingerprintKey] != req.RequestFingerprint ||
				strings.TrimSpace(entry.Metadata[sessiontree.PendingToolEffectAttemptIDKey]) != strings.TrimSpace(target.EffectAttemptID) {
				return sessiontree.Entry{}, false, sessiontree.ErrRequestConflict
			}
			return entry, true, nil
		}
		if (entry.Type != sessiontree.EntryToolCall && entry.Type != sessiontree.EntryToolResult) ||
			entry.Message.ToolCallID != target.ToolCallID || entry.Message.ToolName != target.ToolName {
			continue
		}
		toolFound = true
		if entry.Type != sessiontree.EntryToolResult || entry.Message.ToolResult == nil || strings.TrimSpace(entry.Message.ToolResult.Status) != "running" {
			continue
		}
		pendingFound = true
		if entry.Message.Activity != nil && entry.Message.Activity.Payload != nil &&
			strings.TrimSpace(fmt.Sprint(entry.Message.Activity.Payload["pending_handle"])) == target.Handle &&
			strings.TrimSpace(entry.Metadata[sessiontree.PendingToolEffectAttemptIDKey]) == strings.TrimSpace(target.EffectAttemptID) {
			matchingHandle = true
		}
	}
	switch {
	case !turnFound:
		return sessiontree.Entry{}, false, sessiontree.ErrPendingToolTurnNotFound
	case !runFound:
		return sessiontree.Entry{}, false, sessiontree.ErrPendingToolRunNotFound
	case !toolFound:
		return sessiontree.Entry{}, false, sessiontree.ErrPendingToolNotFound
	case !pendingFound || !matchingHandle:
		return sessiontree.Entry{}, false, sessiontree.ErrPendingToolNotPending
	default:
		return sessiontree.Entry{}, false, nil
	}
}

var _ sessiontree.PendingToolRecoveryRepo = (*Store)(nil)

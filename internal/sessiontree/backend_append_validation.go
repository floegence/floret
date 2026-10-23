package sessiontree

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
	"github.com/floegence/floret/v7/storage/spi"
)

func visitCanonicalTurnEntries(ctx context.Context, tx spi.ReadTx, threadID, turnID string, visit func(Entry) error) error {
	prefix := backendDomainKey("turn_member", threadID, turnID)
	var after []byte
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		page, err := tx.Scan(spi.ScanRequest{Namespace: backendEntryIndexNamespace, Start: prefix, End: backendKeyRangeEnd(prefix), After: after, Limit: backendThreadEntryPageSize})
		if err != nil {
			return err
		}
		for _, row := range page.Records {
			locator, err := readEntryLocator(tx, threadID, string(row.Value))
			if err != nil || !bytes.Equal(row.Key, turnMemberLocatorKey(threadID, turnID, locator.Ordinal)) {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			entry, err := readLocatedEntry(tx, backendDomainV32Format, threadID, locator)
			if err != nil || entry.TurnID != turnID {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			if err := visit(entry); err != nil {
				return err
			}
		}
		if !page.HasMore {
			return nil
		}
		if len(page.Next) == 0 || bytes.Compare(page.Next, after) <= 0 {
			return ErrAuthorityCorrupt
		}
		after = bytes.Clone(page.Next)
	}
}

func visitCanonicalPathMetadata(ctx context.Context, tx spi.ReadTx, threadID, leafID string, visit func(Entry) error) error {
	return visitCanonicalPathMetadataAfter(ctx, tx, threadID, leafID, 0, visit)
}

// afterDepth is an already validated ancestor in this frozen path.
func visitCanonicalPathMetadataAfter(ctx context.Context, tx spi.ReadTx, threadID, leafID string, afterDepth int64, visit func(Entry) error) error {
	if leafID == "" {
		return nil
	}
	leaf, err := readEntryLocator(tx, threadID, leafID)
	if err != nil {
		return err
	}
	// Find one window's newest entry, then walk its parent links once. Seeking
	// every depth from the leaf repeats ancestry reads for the whole history.
	const windowSize = 128
	for first := afterDepth + 1; first <= leaf.Depth; first += windowSize {
		last := min(first+windowSize-1, leaf.Depth)
		node, err := entryAncestor(ctx, tx, threadID, leaf, last)
		if err != nil {
			return err
		}
		window := make([]Entry, int(last-first+1))
		for i := len(window) - 1; i >= 0; i-- {
			if err := ctx.Err(); err != nil {
				return err
			}
			window[i], err = readLocatedEntry(tx, backendDomainV32Format, threadID, node)
			if err != nil {
				return err
			}
			if i != 0 {
				parent, err := readEntryLocator(tx, threadID, node.Parent)
				if err != nil || parent.Depth+1 != node.Depth || parent.Ordinal >= node.Ordinal {
					return errors.Join(ErrAuthorityCorrupt, err)
				}
				node = parent
			}
		}
		for _, entry := range window {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := visit(entry); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateCanonicalAppendedEntry(ctx context.Context, tx spi.ReadTx, meta ThreadMeta, entry Entry) error {
	if err := ValidateEntryIntegrity(entry); err != nil {
		return err
	}
	if presentationEntryRequiresRunIdentity(entry.Type) && strings.TrimSpace(entry.TurnID) != "" && strings.TrimSpace(entry.RunID) == "" {
		return ErrAuthorityCorrupt
	}
	if err := validateThreadContextEntryV9(entry); err != nil {
		return err
	}
	if err := validateEffectOutcomeEntries([]Entry{entry}); err != nil {
		return err
	}
	if err := validateCompletedToolInputEntries([]Entry{entry}, true); err != nil {
		return err
	}
	validation := []Entry{entry}
	if entry.Type == EntryEffectAttempt {
		attempt, err := decodeEffectAttempt(entry)
		if err != nil {
			return err
		}
		if attempt.ResultEntryID != "" {
			result, err := readCanonicalEntryMetadata(tx, meta.ID, attempt.ResultEntryID)
			if err != nil && !errors.Is(err, ErrEntryNotFound) {
				return err
			}
			if err == nil {
				validation = append(validation, result)
			}
		}
	}
	if err := validateCancellationEntries(validation); err != nil {
		return err
	}
	if entry.TurnID != "" {
		kind := entry.Metadata[TurnKindMetadataKey]
		if kind == TurnKindContextCompaction || kind == TurnKindGoalContinuation {
			if err := visitCanonicalTurnEntries(ctx, tx, meta.ID, entry.TurnID, func(candidate Entry) error {
				if candidate.ID == entry.ID {
					return nil
				}
				return validateTurnKindEntries(meta.ID, []Entry{entry, candidate}, true)
			}); err != nil {
				return err
			}
		} else {
			turn, err := readTurnEntryLocator(tx, meta.ID, entry.TurnID)
			if err != nil {
				return err
			}
			if turn.Started != "" && turn.Started != entry.ID {
				started, err := readCanonicalEntryMetadata(tx, meta.ID, turn.Started)
				if err != nil || started.Type != EntryTurnMarker || started.TurnStatus != TurnStarted || started.TurnID != entry.TurnID {
					return errors.Join(ErrAuthorityCorrupt, err)
				}
				validation = append(validation, started)
			}
		}
	}
	if err := validateTurnKindEntries(meta.ID, validation, true); err != nil {
		return err
	}
	switch entry.Type {
	case EntryGoal, EntryGoalReview, EntryGoalContext:
		goal := GoalProjection{}
		if err := visitCanonicalPathMetadata(ctx, tx, meta.ID, meta.LeafID, goal.Observe); err != nil {
			return err
		}
		if entry.Type == EntryGoalReview {
			var review GoalReview
			if json.Unmarshal(entry.Payload, &review) != nil || review.Version < 0 || review.Outcome != "completed" && review.Outcome != "continue" && review.Outcome != "blocked" || strings.TrimSpace(review.Evidence) == "" {
				return ErrAuthorityCorrupt
			}
		}
		if entry.Type == EntryGoalContext && (entry.Message.Kind != session.MessageKindGoalContext && entry.Message.Kind != session.MessageKindGoalReview || entry.Message.Role != session.User) {
			return ErrAuthorityCorrupt
		}
	case EntryParentEvent:
		if err := validateCanonicalParentEvent(ctx, tx, meta, entry); err != nil {
			return err
		}
		return validateCanonicalParentAdmissions(ctx, tx, meta)
	case EntryParentEventsAdmitted:
		if entry.Message.Role != session.User || entry.Message.Kind != session.MessageKindParentEvents || entry.TurnID == "" || entry.RunID == "" || len(entry.Message.Content) > ParentEventBatchMaxBytes || contextpolicy.EstimateTextTokens(entry.Message.Content)+8 > ParentEventBatchMaxTokens {
			return ErrAuthorityCorrupt
		}
		return validateCanonicalParentAdmissions(ctx, tx, meta)
	}
	return nil
}

func canonicalAutomaticParentEvent(ctx context.Context, tx spi.ReadTx, meta ThreadMeta, source Entry) (ParentEvent, bool, error) {
	if meta.ParentThreadID == "" || source.TurnID == "" || source.RunID == "" {
		return ParentEvent{}, false, nil
	}
	turn, err := readTurnEntryLocator(tx, meta.ID, source.TurnID)
	if err != nil {
		return ParentEvent{}, false, err
	}
	if turn.Started != "" {
		started, err := readCanonicalEntryMetadata(tx, meta.ID, turn.Started)
		if err != nil {
			return ParentEvent{}, false, err
		}
		if started.Metadata[TurnKindMetadataKey] == TurnKindContextCompaction {
			return ParentEvent{}, false, nil
		}
	}
	ready := false
	if source.Type == EntryToolResult && source.Message.ToolResult != nil && source.Message.ToolResult.InputRequired != nil {
		projection := ToolInputProjection{}
		err := visitCanonicalTurnEntries(ctx, tx, meta.ID, source.TurnID, func(candidate Entry) error {
			for _, id := range projection.Observe(candidate) {
				ready = ready || id == source.ID
			}
			return nil
		})
		if err != nil {
			return ParentEvent{}, false, err
		}
	}
	event, found := automaticParentEvent(meta, source, ready)
	return event, found, nil
}

func validateCanonicalParentEvent(ctx context.Context, tx spi.ReadTx, meta ThreadMeta, entry Entry) error {
	event, err := decodeParentEvent(entry)
	if err != nil {
		return err
	}
	if entry.TurnID != "" || entry.RunID != "" || entry.Message.Role != "" {
		return ErrAuthorityCorrupt
	}
	seen := map[string]bool{}
	for meta.ID != event.ParentThreadID {
		if meta.ForkedFromThreadID == "" || seen[meta.ID] {
			return ErrAuthorityCorrupt
		}
		seen[meta.ID] = true
		meta, err = readPublishedThreadMeta(tx, backendDomainV32Format, meta.ForkedFromThreadID)
		if err != nil {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
	}
	child, err := readPublishedThreadMeta(tx, backendDomainV32Format, event.SourceThreadID)
	if errors.Is(err, ErrThreadDeleted) {
		return nil
	}
	if err != nil || child.ParentThreadID != event.ParentThreadID {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	source, err := readCanonicalEntryMetadata(tx, child.ID, event.SourceEntryID)
	if err != nil || source.TurnID != event.SourceTurnID || source.RunID != event.SourceRunID {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	if event.Kind == "report" {
		attempt, err := decodeEffectAttempt(source)
		if err != nil || attempt.State != EffectAttemptDispatching {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		return nil
	}
	expected, found, err := canonicalAutomaticParentEvent(ctx, tx, child, source)
	if err != nil {
		return err
	}
	sameTime := expected.ReceivedAt.Equal(event.ReceivedAt)
	expected.ReceivedAt, event.ReceivedAt = time.Time{}, time.Time{}
	if !found || !sameTime || expected != event {
		return ErrAuthorityCorrupt
	}
	return nil
}

func validateCanonicalParentAdmissions(ctx context.Context, tx spi.ReadTx, meta ThreadMeta) error {
	projection := parentEventProjectionState{positions: map[string]int{}}
	return visitCanonicalPathMetadata(ctx, tx, meta.ID, meta.LeafID, projection.observe)
}

package sessiontree

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
	"github.com/floegence/floret/v7/storage/spi"
)

func readCanonicalEntryOrdinal(tx spi.ReadTx, threadID string, ordinal int) (Entry, error) {
	raw, err := tx.Get(backendDomainV32Namespace, backendDomainEntryKey(threadID, ordinal))
	if err != nil {
		return Entry{}, errors.Join(ErrAuthorityCorrupt, err)
	}
	record, err := decodeBackendDomainRecord(backendDomainV32Format, raw)
	if err != nil || record.ThreadID != threadID || record.Ordinal != ordinal {
		return Entry{}, errors.Join(ErrAuthorityCorrupt, err)
	}
	stored, err := decodeCanonicalStoredEntry(backendDomainV32Format, record)
	if err != nil {
		return Entry{}, err
	}
	entry := stored.Entry
	entry.bodyFields = stored.Fields
	return entry, nil
}

// Readers used by preparation release the backend between page/record reads.
// Validation keeps authority identities, never a second repository or journal.
func visitCanonicalJournal(ctx context.Context, tx spi.ReadTx, threadID string, visit func(Entry) error) (int, error) {
	prefix := backendDomainKey(backendDomainRecordEntry, threadID)
	var after []byte
	ordinal := 0
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		page, err := tx.Scan(spi.ScanRequest{Namespace: backendDomainV32Namespace, Start: prefix, End: backendKeyRangeEnd(prefix), After: after, Limit: backendThreadEntryPageSize})
		if err != nil {
			return 0, err
		}
		for _, row := range page.Records {
			record, err := decodeBackendDomainRecord(backendDomainV32Format, row.Value)
			if err != nil || record.ThreadID != threadID || record.Ordinal != ordinal || !bytes.Equal(row.Key, backendDomainEntryKey(threadID, ordinal)) {
				return 0, errors.Join(ErrAuthorityCorrupt, err)
			}
			stored, err := decodeCanonicalStoredEntry(backendDomainV32Format, record)
			if err != nil {
				return 0, err
			}
			entry := stored.Entry
			entry.bodyFields = stored.Fields
			if err := visit(entry); err != nil {
				return 0, err
			}
			ordinal++
		}
		if !page.HasMore {
			return ordinal, nil
		}
		if len(page.Next) == 0 || bytes.Compare(page.Next, after) <= 0 {
			return 0, ErrAuthorityCorrupt
		}
		after = bytes.Clone(page.Next)
	}
}

func validateCanonicalThreadClosure(ctx context.Context, tx spi.ReadTx, threadID string) (map[string]ThreadMeta, error) {
	metas := map[string]ThreadMeta{}
	seen := map[string]bool{}
	queue := []string{threadID}
	for len(queue) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		id := queue[0]
		queue = queue[1:]
		if seen[id] {
			continue
		}
		seen[id] = true
		meta, err := readPublishedThreadMeta(tx, backendDomainV32Format, id)
		if errors.Is(err, ErrThreadDeleted) && id != threadID {
			continue
		}
		if err != nil {
			return nil, err
		}
		metas[id] = meta
		if meta.ParentThreadID != "" {
			queue = append(queue, meta.ParentThreadID)
		}
		steering := steeringProjection{metadataOnly: true}
		count, err := visitCanonicalJournal(ctx, tx, id, func(entry Entry) error {
			if entry.Type == EntryParentEvent {
				event, err := decodeParentEvent(entry)
				if err != nil {
					return err
				}
				queue = append(queue, event.SourceThreadID)
			}
			complete, content, err := canonicalValidationEntry(ctx, tx, entry)
			if err != nil {
				return err
			}
			if err := validateCanonicalHistoricalEntry(ctx, tx, meta, complete); err != nil {
				return fmt.Errorf("%w: thread %q entry %q: %w", ErrAuthorityCorrupt, id, entry.ID, err)
			}
			if _, err := steering.observeValidated(complete, content.nonSpace); err != nil {
				return err
			}
			return validateCanonicalOutputSources(ctx, tx, entry)
		})
		if err != nil {
			return nil, err
		}
		if (count == 0) != (meta.LeafID == "") {
			return nil, ErrAuthorityCorrupt
		}
		if meta.LeafID != "" {
			leaf, err := readEntryLocator(tx, id, meta.LeafID)
			if err != nil || leaf.Ordinal >= count {
				return nil, errors.Join(ErrAuthorityCorrupt, err)
			}
		}
		if err := verifyCanonicalThreadLocators(ctx, tx, id, count); err != nil {
			return nil, err
		}
		if err := validateCanonicalThreadSideRecords(tx, id); err != nil {
			return nil, err
		}
		goal := GoalProjection{}
		parents := parentEventProjectionState{positions: map[string]int{}}
		if err := visitCanonicalPathMetadata(ctx, tx, id, meta.LeafID, func(entry Entry) error {
			if err := goal.Observe(entry); err != nil {
				return err
			}
			return parents.observe(entry)
		}); err != nil {
			return nil, err
		}
	}
	if err := ValidateThreadAuthorityGraph(values(metas)); err != nil {
		return nil, err
	}
	return metas, nil
}

func validateCanonicalHistoricalEntry(ctx context.Context, tx spi.ReadTx, meta ThreadMeta, entry Entry) error {
	if presentationEntryRequiresRunIdentity(entry.Type) && strings.TrimSpace(entry.TurnID) != "" && strings.TrimSpace(entry.RunID) == "" {
		return ErrAuthorityCorrupt
	}
	for _, check := range []func(Entry) error{validateThreadContextEntryV9, validateCanonicalEntryRunIdentity} {
		if err := check(entry); err != nil {
			return err
		}
	}
	for _, check := range []func([]Entry) error{validateEffectOutcomeEntries, func(entries []Entry) error { return validateCompletedToolInputEntries(entries, true) }} {
		if err := check([]Entry{entry}); err != nil {
			return err
		}
	}
	validation := []Entry{entry}
	if entry.Type == EntryEffectAttempt {
		attempt, err := decodeEffectAttempt(entry)
		if err != nil {
			return err
		}
		if attempt.State == EffectAttemptCancelled && attempt.ResultEntryID != "" {
			result, err := readCanonicalEntryMetadata(tx, meta.ID, attempt.ResultEntryID)
			if err != nil {
				return err
			}
			validation = append(validation, result)
		}
	}
	if err := validateCancellationEntries(validation); err != nil {
		return err
	}
	if entry.TurnID != "" {
		turn, err := readTurnEntryLocator(tx, meta.ID, entry.TurnID)
		if err != nil {
			return err
		}
		if turn.Started != "" && turn.Started != entry.ID {
			started, err := readCanonicalEntryMetadata(tx, meta.ID, turn.Started)
			if err != nil {
				return err
			}
			validation = append(validation, started)
		}
	}
	if err := validateTurnKindEntries(meta.ID, validation, true); err != nil {
		return err
	}
	switch entry.Type {
	case EntryGoalReview:
		var review GoalReview
		if json.Unmarshal(entry.Payload, &review) != nil || review.Version < 0 || review.Outcome != "completed" && review.Outcome != "continue" && review.Outcome != "blocked" || strings.TrimSpace(review.Evidence) == "" {
			return ErrAuthorityCorrupt
		}
	case EntryGoalContext:
		if (entry.Message.Kind != session.MessageKindGoalContext && entry.Message.Kind != session.MessageKindGoalReview) || entry.Message.Role != session.User {
			return ErrAuthorityCorrupt
		}
	case EntryParentEvent:
		if err := validateCanonicalParentEvent(ctx, tx, meta, entry); err != nil {
			return err
		}
	case EntryParentEventsAdmitted:
		if entry.Message.Role != session.User || entry.Message.Kind != session.MessageKindParentEvents || entry.TurnID == "" || entry.RunID == "" || len(entry.Message.Content) > ParentEventBatchMaxBytes || contextpolicy.EstimateTextTokens(entry.Message.Content)+8 > ParentEventBatchMaxTokens {
			return ErrAuthorityCorrupt
		}
	case EntryCompaction:
		path, err := canonicalCompletePath(ctx, tx, meta.ID, entry.ID)
		if err != nil {
			return err
		}
		if _, err := BuildContextChecked(path, ContextOptions{}); err != nil {
			return err
		}
	}
	if result := entry.Message.ToolResult; result != nil && result.FullOutput != nil {
		reader := canonicalArtifactReader(tx)
		record, found, err := reader.record(meta.ID, result.FullOutput.ID)
		if err != nil || !found || record.ThreadID != meta.ID || record.Ref.ID != result.FullOutput.ID {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		return reader.validateArtifactRecordLocked(record)
	}
	return nil
}

func validateCanonicalThreadSideRecords(tx spi.ReadTx, threadID string) error {
	for _, kind := range []string{backendDomainRecordTodo, backendDomainRecordProviderState, backendDomainRecordTombstone} {
		record, found, err := readBackendDomainRecord(tx, backendDomainV32Format, backendDomainKey(kind, threadID), kind, threadID, "", 0)
		if err != nil || !found {
			if err != nil {
				return err
			}
			continue
		}
		switch kind {
		case backendDomainRecordTodo:
			var value AgentTodoState
			if err := decodeBackendDomainValue(backendDomainV32Format, record, &value); err != nil || value.ThreadID != threadID || ValidateAgentTodoItems(value.Items) != nil {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
		case backendDomainRecordProviderState:
			var value ProviderStateRecord
			if err := decodeBackendDomainValue(backendDomainV32Format, record, &value); err != nil || value.ThreadID != threadID {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
		case backendDomainRecordTombstone:
			return ErrAuthorityCorrupt
		}
	}
	return nil
}

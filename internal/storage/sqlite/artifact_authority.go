package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/floegence/floret/internal/session/artifact"
	"github.com/floegence/floret/internal/sessiontree"
)

func (s *Store) ReadArtifact(ctx context.Context, req sessiontree.ArtifactReadRequest) (sessiontree.ArtifactContent, error) {
	threadID := strings.TrimSpace(req.ThreadID)
	artifactID := strings.TrimSpace(req.ArtifactID)
	if threadID == "" || artifactID == "" {
		return sessiontree.ArtifactContent{}, errors.New("artifact read requires thread and artifact identities")
	}
	var result sessiontree.ArtifactContent
	err := s.withRead(ctx, func(q sqlRunner) error {
		if err := validateSQLiteArtifactReadAuthority(ctx, q, req.ParentThreadID, threadID); err != nil {
			return err
		}
		record, err := loadSQLiteArtifactRecord(ctx, q, threadID, artifactID)
		if errors.Is(err, sql.ErrNoRows) {
			return sessiontree.ErrArtifactNotFound
		}
		if err != nil {
			return err
		}
		if err := validateSQLiteArtifactRecord(ctx, q, record); err != nil {
			return err
		}
		result = sessiontree.ArtifactContent{Ref: record.Ref, Text: record.Text}
		return nil
	})
	if err != nil {
		return sessiontree.ArtifactContent{}, err
	}
	return result, nil
}

func (s *Store) ArtifactClosure(ctx context.Context, req sessiontree.ArtifactClosureRequest) (artifact.Closure, error) {
	sourceID := strings.TrimSpace(req.SourceThreadID)
	destinationID := strings.TrimSpace(req.DestinationThreadID)
	if sourceID == "" || destinationID == "" {
		return artifact.Closure{}, errors.New("artifact closure requires source and destination")
	}
	var result artifact.Closure
	err := s.withRead(ctx, func(q sqlRunner) error {
		if _, err := loadSQLiteArtifactThread(ctx, q, sourceID); err != nil {
			if errors.Is(err, sessiontree.ErrThreadNotFound) {
				if _, tombstoneErr := loadSQLiteArtifactTombstone(ctx, q, sourceID); tombstoneErr == nil {
					return sessiontree.ErrThreadDeleted
				} else if !errors.Is(tombstoneErr, sql.ErrNoRows) {
					return tombstoneErr
				}
			}
			return err
		}
		entries := make([]sessiontree.Entry, 0, len(req.EntryIDs))
		for _, entryID := range req.EntryIDs {
			entry, err := loadEntry(ctx, q, sourceID, strings.TrimSpace(entryID))
			if err != nil {
				return err
			}
			entries = append(entries, entry)
		}
		closure, err := sqliteArtifactClosure(ctx, q, sourceID, destinationID, entries)
		if err != nil {
			return err
		}
		result = closure
		return nil
	})
	return result, err
}

func (s *Store) withRead(ctx context.Context, fn func(sqlRunner) error) error {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}

func validateSQLiteArtifactReadAuthority(ctx context.Context, q sqlRunner, parentThreadID, threadID string) error {
	parentThreadID = strings.TrimSpace(parentThreadID)
	threadID = strings.TrimSpace(threadID)
	if parentThreadID == "" {
		meta, err := loadSQLiteArtifactThread(ctx, q, threadID)
		if errors.Is(err, sessiontree.ErrThreadNotFound) {
			if _, tombstoneErr := loadSQLiteArtifactTombstone(ctx, q, threadID); tombstoneErr == nil {
				return sessiontree.ErrThreadDeleted
			} else if !errors.Is(tombstoneErr, sql.ErrNoRows) {
				return tombstoneErr
			}
		}
		if err != nil {
			return err
		}
		if strings.TrimSpace(meta.ParentThreadID) != "" {
			return sessiontree.ErrSubAgentParentRequired
		}
		return nil
	}
	if _, err := loadSQLiteArtifactThread(ctx, q, parentThreadID); err != nil {
		if errors.Is(err, sessiontree.ErrThreadNotFound) {
			if _, tombstoneErr := loadSQLiteArtifactTombstone(ctx, q, parentThreadID); tombstoneErr == nil {
				return sessiontree.ErrThreadDeleted
			} else if !errors.Is(tombstoneErr, sql.ErrNoRows) {
				return tombstoneErr
			}
			return sessiontree.ErrSubAgentNotFound
		}
		return err
	}
	if threadID == parentThreadID {
		return sessiontree.ErrSubAgentNotFound
	}
	meta, err := loadSQLiteArtifactThread(ctx, q, threadID)
	if err == nil {
		return validateSQLiteLiveDescendant(ctx, q, parentThreadID, meta)
	}
	if !errors.Is(err, sessiontree.ErrThreadNotFound) {
		return err
	}
	tombstone, tombstoneErr := loadSQLiteArtifactTombstone(ctx, q, threadID)
	if errors.Is(tombstoneErr, sql.ErrNoRows) {
		return sessiontree.ErrSubAgentNotFound
	}
	if tombstoneErr != nil {
		return tombstoneErr
	}
	descendant, err := sqliteTombstoneDescendsFrom(ctx, q, parentThreadID, tombstone)
	if err != nil {
		return err
	}
	if descendant {
		return sessiontree.ErrThreadDeleted
	}
	return sessiontree.ErrSubAgentNotFound
}

func validateSQLiteLiveDescendant(ctx context.Context, q sqlRunner, parentThreadID string, current sessiontree.ThreadMeta) error {
	seen := map[string]struct{}{current.ID: {}}
	for {
		ancestorID := strings.TrimSpace(current.ParentThreadID)
		if ancestorID == "" {
			return sessiontree.ErrSubAgentNotFound
		}
		if ancestorID == parentThreadID {
			return nil
		}
		if _, duplicate := seen[ancestorID]; duplicate {
			return sessiontree.ErrAuthorityCorrupt
		}
		seen[ancestorID] = struct{}{}
		ancestor, err := loadSQLiteArtifactThread(ctx, q, ancestorID)
		if err != nil {
			return sessiontree.ErrAuthorityCorrupt
		}
		current = ancestor
	}
}

func sqliteTombstoneDescendsFrom(ctx context.Context, q sqlRunner, parentThreadID string, current sessiontree.ThreadTombstone) (bool, error) {
	rootThreadID := current.RootThreadID
	seen := map[string]struct{}{current.ThreadID: {}}
	for {
		ancestorID := strings.TrimSpace(current.ParentThreadID)
		if ancestorID == "" {
			if current.ThreadID != rootThreadID {
				return false, sessiontree.ErrAuthorityCorrupt
			}
			return false, nil
		}
		if ancestorID == parentThreadID {
			parentRootThreadID, err := sqliteLiveArtifactRootThreadID(ctx, q, parentThreadID)
			if err != nil {
				return false, err
			}
			if rootThreadID != parentRootThreadID {
				return false, sessiontree.ErrAuthorityCorrupt
			}
			return true, nil
		}
		if _, duplicate := seen[ancestorID]; duplicate {
			return false, sessiontree.ErrAuthorityCorrupt
		}
		seen[ancestorID] = struct{}{}
		if tombstone, err := loadSQLiteArtifactTombstone(ctx, q, ancestorID); err == nil {
			if tombstone.RootThreadID != rootThreadID {
				return false, sessiontree.ErrAuthorityCorrupt
			}
			current = tombstone
			continue
		} else if !errors.Is(err, sql.ErrNoRows) {
			return false, err
		}
		meta, err := loadSQLiteArtifactThread(ctx, q, ancestorID)
		if err != nil {
			return false, sessiontree.ErrAuthorityCorrupt
		}
		current = sessiontree.ThreadTombstone{ThreadID: meta.ID, ParentThreadID: meta.ParentThreadID}
	}
}

func sqliteLiveArtifactRootThreadID(ctx context.Context, q sqlRunner, threadID string) (string, error) {
	seen := map[string]struct{}{}
	for {
		if _, duplicate := seen[threadID]; duplicate {
			return "", sessiontree.ErrAuthorityCorrupt
		}
		seen[threadID] = struct{}{}
		meta, err := loadSQLiteArtifactThread(ctx, q, threadID)
		if err != nil {
			return "", sessiontree.ErrAuthorityCorrupt
		}
		parentThreadID := strings.TrimSpace(meta.ParentThreadID)
		if parentThreadID == "" {
			return threadID, nil
		}
		threadID = parentThreadID
	}
}

func loadSQLiteArtifactThread(ctx context.Context, q sqlRunner, threadID string) (sessiontree.ThreadMeta, error) {
	meta, err := loadThread(ctx, q, strings.TrimSpace(threadID))
	if errors.Is(err, sessiontree.ErrInvalidThreadAuthority) {
		return sessiontree.ThreadMeta{}, sessiontree.ErrAuthorityCorrupt
	}
	if err != nil {
		return sessiontree.ThreadMeta{}, err
	}
	if meta.ID != strings.TrimSpace(threadID) || meta.ID != strings.TrimSpace(meta.ID) {
		return sessiontree.ThreadMeta{}, sessiontree.ErrAuthorityCorrupt
	}
	return meta, nil
}

func loadSQLiteArtifactTombstone(ctx context.Context, q sqlRunner, threadID string) (sessiontree.ThreadTombstone, error) {
	threadID = strings.TrimSpace(threadID)
	tombstone, err := loadThreadTombstone(ctx, q, threadID)
	if err != nil {
		return sessiontree.ThreadTombstone{}, err
	}
	parentThreadID := strings.TrimSpace(tombstone.ParentThreadID)
	rootThreadID := strings.TrimSpace(tombstone.RootThreadID)
	if tombstone.ThreadID != threadID || tombstone.ThreadID != strings.TrimSpace(tombstone.ThreadID) ||
		tombstone.ParentThreadID != parentThreadID || tombstone.RootThreadID != rootThreadID ||
		parentThreadID == threadID || (parentThreadID == "" && rootThreadID != threadID) ||
		(parentThreadID != "" && rootThreadID == threadID) {
		return sessiontree.ThreadTombstone{}, sessiontree.ErrAuthorityCorrupt
	}
	return tombstone, nil
}

func sqliteArtifactClosure(ctx context.Context, q sqlRunner, sourceID, destinationID string, entries []sessiontree.Entry) (artifact.Closure, error) {
	byID := make(map[string]artifact.ManifestItem)
	for _, entry := range entries {
		if entry.ThreadID != sourceID || entry.Message.ToolResult == nil || entry.Message.ToolResult.FullOutput == nil {
			continue
		}
		ref := *entry.Message.ToolResult.FullOutput
		record, err := loadSQLiteArtifactRecord(ctx, q, sourceID, ref.ID)
		if err != nil || record.CanonicalEntryID != entry.ID || !reflect.DeepEqual(record.Ref, ref) {
			return artifact.Closure{}, sessiontree.ErrAuthorityCorrupt
		}
		if err := validateSQLiteArtifactRecord(ctx, q, record); err != nil {
			return artifact.Closure{}, err
		}
		refFingerprint, err := artifact.RefFingerprint(ref)
		if err != nil {
			return artifact.Closure{}, sessiontree.ErrAuthorityCorrupt
		}
		item := artifact.ManifestItem{
			SourceEntryID: record.CanonicalEntryID, ArtifactID: ref.ID, Ref: ref,
			RefFingerprint: refFingerprint, PayloadSHA256: artifact.TextSHA256(record.Text),
		}
		if existing, duplicate := byID[ref.ID]; duplicate && !reflect.DeepEqual(existing, item) {
			return artifact.Closure{}, sessiontree.ErrAuthorityCorrupt
		}
		byID[ref.ID] = item
	}
	items := make([]artifact.ManifestItem, 0, len(byID))
	for _, item := range byID {
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ArtifactID < items[j].ArtifactID })
	fingerprint, err := artifact.ClosureFingerprint(sourceID, destinationID, items)
	if err != nil {
		return artifact.Closure{}, err
	}
	return artifact.Closure{SourceThreadID: sourceID, DestinationThreadID: destinationID, Items: items, Fingerprint: fingerprint}, nil
}

func validateSQLiteArtifactClosure(ctx context.Context, q sqlRunner, sourceID, destinationID string, entries []sessiontree.Entry, expected artifact.Closure) error {
	if err := artifact.ValidateClosure(expected); err != nil || expected.SourceThreadID != sourceID || expected.DestinationThreadID != destinationID {
		return sessiontree.ErrStaleAuthority
	}
	actual, err := sqliteArtifactClosure(ctx, q, sourceID, destinationID, entries)
	if err != nil {
		return err
	}
	if !artifact.EqualClosure(actual, expected) {
		return sessiontree.ErrStaleAuthority
	}
	return nil
}

func copySQLiteArtifactClosure(ctx context.Context, q sqlRunner, closure artifact.Closure, sourceToDestinationEntryID map[string]string, now time.Time) error {
	for _, item := range closure.Items {
		destinationEntryID := strings.TrimSpace(sourceToDestinationEntryID[item.SourceEntryID])
		if destinationEntryID == "" {
			return sessiontree.ErrAuthorityCorrupt
		}
		source, err := loadSQLiteArtifactRecord(ctx, q, closure.SourceThreadID, item.ArtifactID)
		if err != nil || source.CanonicalEntryID != item.SourceEntryID || !reflect.DeepEqual(source.Ref, item.Ref) || artifact.TextSHA256(source.Text) != item.PayloadSHA256 {
			return sessiontree.ErrAuthorityCorrupt
		}
		if err := validateSQLiteArtifactRecord(ctx, q, source); err != nil {
			return err
		}
		destinationEntry, err := loadEntry(ctx, q, closure.DestinationThreadID, destinationEntryID)
		if err != nil || destinationEntry.Message.ToolResult == nil || destinationEntry.Message.ToolResult.FullOutput == nil ||
			!reflect.DeepEqual(*destinationEntry.Message.ToolResult.FullOutput, item.Ref) {
			return sessiontree.ErrAuthorityCorrupt
		}
		if _, err := q.ExecContext(ctx, `INSERT INTO tool_output_artifacts(
			thread_id, id, effect_attempt_id, safe_label, kind, mime, size_bytes, sha256, text, canonical_entry_id, created_at
		) VALUES(?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?)`,
			closure.DestinationThreadID, item.ArtifactID, item.Ref.SafeLabel, item.Ref.Kind, item.Ref.MIME,
			item.Ref.SizeBytes, item.Ref.SHA256, source.Text, destinationEntryID, formatTime(now)); err != nil {
			if isConstraintError(err) {
				return sessiontree.ErrAuthorityCorrupt
			}
			return err
		}
	}
	return nil
}

func (s *Store) ValidateArtifactForkDestination(ctx context.Context, closure artifact.Closure) error {
	return s.withRead(ctx, func(q sqlRunner) error {
		return validateSQLiteArtifactForkDestination(ctx, q, closure)
	})
}

func validateSQLiteArtifactForkDestination(ctx context.Context, q sqlRunner, closure artifact.Closure) error {
	if err := artifact.ValidateClosure(closure); err != nil {
		return sessiontree.ErrAuthorityCorrupt
	}
	if _, err := loadSQLiteArtifactThread(ctx, q, closure.DestinationThreadID); err != nil {
		if errors.Is(err, sessiontree.ErrThreadNotFound) {
			if _, tombstoneErr := loadSQLiteArtifactTombstone(ctx, q, closure.DestinationThreadID); tombstoneErr == nil {
				return sessiontree.ErrThreadDeleted
			} else if !errors.Is(tombstoneErr, sql.ErrNoRows) {
				return tombstoneErr
			}
		}
		return err
	}
	for _, item := range closure.Items {
		record, err := loadSQLiteArtifactRecord(ctx, q, closure.DestinationThreadID, item.ArtifactID)
		if err != nil || !reflect.DeepEqual(record.Ref, item.Ref) || artifact.TextSHA256(record.Text) != item.PayloadSHA256 {
			return sessiontree.ErrAuthorityCorrupt
		}
		if err := validateSQLiteArtifactRecord(ctx, q, record); err != nil {
			return err
		}
	}
	return nil
}

func loadSQLiteArtifactRecord(ctx context.Context, q sqlRunner, threadID, artifactID string) (artifact.Record, error) {
	threadID = strings.TrimSpace(threadID)
	artifactID = strings.TrimSpace(artifactID)
	var record artifact.Record
	var created string
	err := q.QueryRowContext(ctx, `SELECT thread_id, id, safe_label, kind, mime, size_bytes, sha256, text, canonical_entry_id, created_at
		FROM tool_output_artifacts WHERE thread_id = ? AND id = ?`, threadID, artifactID).Scan(
		&record.ThreadID, &record.Ref.ID, &record.Ref.SafeLabel, &record.Ref.Kind, &record.Ref.MIME,
		&record.Ref.SizeBytes, &record.Ref.SHA256, &record.Text, &record.CanonicalEntryID, &created)
	if err != nil {
		return artifact.Record{}, err
	}
	record.CreatedAt = parseTime(created)
	if record.ThreadID != threadID || record.Ref.ID != artifactID {
		return artifact.Record{}, sessiontree.ErrAuthorityCorrupt
	}
	return record, nil
}

func validateSQLiteArtifactRecord(ctx context.Context, q sqlRunner, record artifact.Record) error {
	if record.ThreadID == "" || record.CanonicalEntryID == "" || record.CreatedAt.IsZero() || artifact.ValidateRef(record.Ref) != nil ||
		record.Ref.SHA256 != artifact.TextSHA256(record.Text) || record.Ref.SizeBytes != int64(len(record.Text)) {
		return sessiontree.ErrAuthorityCorrupt
	}
	entry, err := loadEntry(ctx, q, record.ThreadID, record.CanonicalEntryID)
	if err != nil || entry.ThreadID != record.ThreadID || entry.Type != sessiontree.EntryToolResult || entry.Message.ToolResult == nil || entry.Message.ToolResult.FullOutput == nil ||
		!reflect.DeepEqual(*entry.Message.ToolResult.FullOutput, record.Ref) {
		return sessiontree.ErrAuthorityCorrupt
	}
	return nil
}

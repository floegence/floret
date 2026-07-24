package sqlite

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/session/artifact"
	"github.com/floegence/floret/internal/sessiontree"
)

var schemaVersion13Tables = []string{
	"schema_meta",
	"threads",
	"fork_operations",
	"entries",
	"provider_states",
	"agent_todo_states",
	"active_turn_leases",
	"prompt_segments",
	"prompt_toolsets",
	"prompt_requests",
	"prompt_responses",
	"tool_output_artifacts",
	"metadata_records",
}

func migrateQuiescentSchemaVersion13(ctx context.Context, tx sqlRunner, leasePolicy sessiontree.LeasePolicy) error {
	if err := verifySchemaVersion(ctx, tx, schemaVersion13); err != nil {
		return fmt.Errorf("verify sqlite store schema v13 before migration: %w", err)
	}
	artifacts, err := planSchemaVersion13ArtifactMigration(ctx, tx)
	if err != nil {
		return err
	}
	if err := validateQuiescentSchemaVersion13(ctx, tx); err != nil {
		return err
	}
	if err := dropSchemaVersion13NamedIndexes(ctx, tx); err != nil {
		return err
	}
	for _, table := range schemaVersion13Tables {
		if _, err := tx.ExecContext(ctx, `ALTER TABLE `+quoteSchemaName(table)+` RENAME TO `+quoteSchemaName(legacySchemaVersion13Table(table))); err != nil {
			return fmt.Errorf("rename sqlite schema v13 table %q: %w", table, err)
		}
	}
	if _, err := tx.ExecContext(ctx, schemaVersion14SQL); err != nil {
		return fmt.Errorf("create sqlite store schema v14 during migration: %w", err)
	}
	if err := copySchemaVersion13Threads(ctx, tx); err != nil {
		return err
	}
	for _, table := range []string{
		"fork_operations",
		"entries",
		"provider_states",
		"agent_todo_states",
		"prompt_segments",
		"prompt_toolsets",
		"prompt_requests",
		"prompt_responses",
		"metadata_records",
	} {
		if err := copyLegacySharedColumns(ctx, tx, table); err != nil {
			return err
		}
	}
	if err := migrateSchemaVersion13SubAgentTerminalStatus(ctx, tx); err != nil {
		return err
	}
	if err := copySchemaVersion13Artifacts(ctx, tx, artifacts); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO schema_meta(key, value) VALUES
		('schema_version', ?), ('raw_encoder_version', ?), ('schema_fingerprint', ?)`,
		schemaVersion14, rawEncoderVersion, schemaFingerprintVersion14); err != nil {
		return fmt.Errorf("write migrated sqlite store schema v14 metadata: %w", err)
	}
	if err := persistLeasePolicy(ctx, tx, leasePolicy); err != nil {
		return err
	}
	for _, table := range schemaVersion13DropOrder {
		legacyTable := legacySchemaVersion13Table(table)
		if _, err := tx.ExecContext(ctx, `DROP TABLE `+quoteSchemaName(legacyTable)); err != nil {
			return fmt.Errorf("drop migrated sqlite schema v13 table %q: %w", table, err)
		}
	}
	return verifySchemaVersion(ctx, tx, schemaVersion14)
}

func validateQuiescentSchemaVersion13(ctx context.Context, q sqlRunner) error {
	if err := requireLegacyTableEmpty(ctx, q, "active_turn_leases", "active turn leases"); err != nil {
		return err
	}
	var preparedForks int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM fork_operations WHERE state = 'prepared'`).Scan(&preparedForks); err != nil {
		return fmt.Errorf("inspect sqlite schema v13 fork operations: %w", err)
	}
	if preparedForks != 0 {
		return fmt.Errorf("%w: sqlite store schema v13 has %d prepared fork operations", sessiontree.ErrAuthorityCorrupt, preparedForks)
	}
	graph, err := loadSchemaVersion13ThreadAuthorityGraph(ctx, q)
	if err != nil {
		return fmt.Errorf("load sqlite schema v13 thread authority: %w", err)
	}
	if err := sessiontree.ValidateThreadAuthorityGraph(graph); err != nil {
		return fmt.Errorf("validate sqlite schema v13 thread authority: %w", err)
	}
	return validateSchemaVersion13Journals(ctx, q)
}

func loadSchemaVersion13ThreadAuthorityGraph(ctx context.Context, q sqlRunner) ([]sessiontree.ThreadMeta, error) {
	rows, err := q.QueryContext(ctx, `SELECT
		id, leaf_id, parent_thread_id, parent_turn_id, forked_from_thread_id, forked_from_entry_id,
		fork_operation_id, fork_operation_node_id, task_name, task_description, agent_path,
		host_profile_ref, fork_mode, closed, archived, title, title_status, title_source,
		title_updated_at, title_error, created_at, updated_at, last_viewed_at
		FROM threads ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var threads []sessiontree.ThreadMeta
	for rows.Next() {
		var meta sessiontree.ThreadMeta
		var closed, archived int
		var titleStatus, titleSource, titleUpdated, created, updated, lastViewed string
		if err := rows.Scan(
			&meta.ID, &meta.LeafID, &meta.ParentThreadID, &meta.ParentTurnID,
			&meta.ForkedFromThreadID, &meta.ForkedFromEntryID, &meta.ForkOperationID,
			&meta.ForkOperationNodeID, &meta.TaskName, &meta.TaskDescription, &meta.AgentPath,
			&meta.HostProfileRef, &meta.ForkMode, &closed, &archived, &meta.Title, &titleStatus,
			&titleSource, &titleUpdated, &meta.TitleError, &created, &updated, &lastViewed,
		); err != nil {
			return nil, err
		}
		meta.Lifecycle = sessiontree.ThreadLifecycleOpen
		if closed != 0 {
			meta.Lifecycle = sessiontree.ThreadLifecycleClosed
		}
		meta.Archived = archived != 0
		meta.TitleStatus = sessiontree.ThreadTitleStatus(titleStatus)
		meta.TitleSource = sessiontree.ThreadTitleSource(titleSource)
		meta.TitleUpdatedAt = parseTime(titleUpdated)
		meta.CreatedAt = parseTime(created)
		meta.UpdatedAt = parseTime(updated)
		meta.LastViewedAt = parseTime(lastViewed)
		if meta.TitleStatus != "" {
			meta.TitleGeneration = 1
			if meta.TitleSource == sessiontree.ThreadTitleSourceProvider || meta.TitleStatus == sessiontree.ThreadTitleFailed {
				meta.TitleToken = "migrated-v13:" + meta.ID
			}
		}
		threads = append(threads, meta)
	}
	return threads, rows.Err()
}

func requireLegacyTableEmpty(ctx context.Context, q sqlRunner, table, reason string) error {
	var count int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+quoteSchemaName(table)).Scan(&count); err != nil {
		return fmt.Errorf("inspect sqlite schema v13 %s: %w", reason, err)
	}
	if count != 0 {
		return fmt.Errorf("%w: sqlite store schema v13 has %d %s", sessiontree.ErrAuthorityCorrupt, count, reason)
	}
	return nil
}

type schemaVersion13ArtifactMigration struct {
	id, runID, threadID, turnID, promptScopeID string
	step                                       int
	callID, toolName, canonicalEntryID         string
	kind, mime, safeLabel                      string
	sizeBytes                                  int64
	sha256, text, metadataJSON, createdAt      string
}

type schemaVersion13ArtifactRef struct {
	ID        string `json:"id,omitempty"`
	SafeLabel string `json:"safe_label,omitempty"`
	URL       string `json:"url,omitempty"`
	Kind      string `json:"kind,omitempty"`
	MIME      string `json:"mime,omitempty"`
	SizeBytes int64  `json:"size_bytes,omitempty"`
	SHA256    string `json:"sha256,omitempty"`
}

type schemaVersion13ArtifactMessage struct {
	Role       session.Role
	ToolCallID string
	ToolName   string
	ToolResult *struct {
		FullOutput *schemaVersion13ArtifactRef `json:"full_output"`
	}
}

func planSchemaVersion13ArtifactMigration(ctx context.Context, q sqlRunner) ([]schemaVersion13ArtifactMigration, error) {
	rows, err := q.QueryContext(ctx, `SELECT
		id, run_id, thread_id, turn_id, prompt_scope_id, step, call_id, tool_name,
		kind, mime, safe_label, url, size_bytes, sha256, text, metadata_json, created_at
		FROM tool_output_artifacts ORDER BY rowid`)
	if err != nil {
		return nil, fmt.Errorf("read sqlite schema v13 tool output artifacts: %w", err)
	}
	defer rows.Close()
	var out []schemaVersion13ArtifactMigration
	for rows.Next() {
		var item schemaVersion13ArtifactMigration
		var legacyURL string
		if err := rows.Scan(
			&item.id, &item.runID, &item.threadID, &item.turnID, &item.promptScopeID,
			&item.step, &item.callID, &item.toolName, &item.kind, &item.mime, &item.safeLabel,
			&legacyURL, &item.sizeBytes, &item.sha256, &item.text, &item.metadataJSON, &item.createdAt,
		); err != nil {
			return nil, fmt.Errorf("scan sqlite schema v13 tool output artifact: %w", err)
		}
		if legacyURL != "" {
			return nil, fmt.Errorf("%w: sqlite schema v13 artifact %q has a legacy product URL that cannot be migrated without restoring a product route or rewriting canonical entry raw identity", sessiontree.ErrAuthorityCorrupt, item.id)
		}
		if strings.TrimSpace(item.runID) == "" || strings.TrimSpace(item.threadID) == "" ||
			strings.TrimSpace(item.turnID) == "" || strings.TrimSpace(item.promptScopeID) == "" ||
			strings.TrimSpace(item.callID) == "" || strings.TrimSpace(item.toolName) == "" ||
			item.step < 0 || parseTime(item.createdAt).IsZero() || !json.Valid([]byte(item.metadataJSON)) {
			return nil, fmt.Errorf("%w: sqlite schema v13 artifact %q has incomplete durable identity", sessiontree.ErrAuthorityCorrupt, item.id)
		}
		ref := artifact.Ref{
			ID: item.id, SafeLabel: item.safeLabel, Kind: item.kind, MIME: item.mime,
			SizeBytes: item.sizeBytes, SHA256: item.sha256,
		}
		if artifact.ValidateRef(ref) != nil || item.sha256 != artifact.TextSHA256(item.text) || item.sizeBytes != int64(len(item.text)) {
			return nil, fmt.Errorf("%w: sqlite schema v13 artifact %q payload identity is invalid", sessiontree.ErrAuthorityCorrupt, item.id)
		}
		if err := validateSchemaVersion13ArtifactRunIdentity(ctx, q, item); err != nil {
			return nil, err
		}
		entryID, err := schemaVersion13ArtifactCanonicalEntry(ctx, q, item, ref)
		if err != nil {
			return nil, err
		}
		item.canonicalEntryID = entryID
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read sqlite schema v13 tool output artifacts: %w", err)
	}
	return out, nil
}

func validateSchemaVersion13ArtifactRunIdentity(ctx context.Context, q sqlRunner, item schemaVersion13ArtifactMigration) error {
	rows, err := q.QueryContext(ctx, `SELECT metadata_json FROM entries
		WHERE thread_id = ? AND turn_id = ? AND type = 'turn_marker' AND turn_status = 'started'`, item.threadID, item.turnID)
	if err != nil {
		return err
	}
	defer rows.Close()
	matches := 0
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return err
		}
		var metadata map[string]string
		if err := json.Unmarshal([]byte(raw), &metadata); err != nil {
			return fmt.Errorf("%w: sqlite schema v13 artifact %q turn start metadata is invalid", sessiontree.ErrAuthorityCorrupt, item.id)
		}
		if metadata["run_id"] == item.runID {
			matches++
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if matches != 1 {
		return fmt.Errorf("%w: sqlite schema v13 artifact %q has %d matching turn/run authorities", sessiontree.ErrAuthorityCorrupt, item.id, matches)
	}
	return nil
}

func schemaVersion13ArtifactCanonicalEntry(ctx context.Context, q sqlRunner, item schemaVersion13ArtifactMigration, ref artifact.Ref) (string, error) {
	rows, err := q.QueryContext(ctx, `SELECT id, message_json FROM entries
		WHERE thread_id = ? AND turn_id = ? AND type = 'tool_result' ORDER BY ordinal`, item.threadID, item.turnID)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var matches []string
	for rows.Next() {
		var entryID, raw string
		if err := rows.Scan(&entryID, &raw); err != nil {
			return "", err
		}
		var message schemaVersion13ArtifactMessage
		if err := json.Unmarshal([]byte(raw), &message); err != nil || message.ToolResult == nil || message.ToolResult.FullOutput == nil {
			continue
		}
		legacyRef := message.ToolResult.FullOutput
		if message.Role != session.Tool || message.ToolCallID != item.callID || message.ToolName != item.toolName || legacyRef.URL != "" {
			continue
		}
		if legacyRef.ID == ref.ID && legacyRef.SafeLabel == ref.SafeLabel && legacyRef.Kind == ref.Kind &&
			legacyRef.MIME == ref.MIME && legacyRef.SizeBytes == ref.SizeBytes && legacyRef.SHA256 == ref.SHA256 {
			matches = append(matches, entryID)
		}
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	if len(matches) != 1 {
		return "", fmt.Errorf("%w: sqlite schema v13 artifact %q has %d matching canonical tool-result entries", sessiontree.ErrAuthorityCorrupt, item.id, len(matches))
	}
	return matches[0], nil
}

func copySchemaVersion13Artifacts(ctx context.Context, q sqlRunner, artifacts []schemaVersion13ArtifactMigration) error {
	for _, item := range artifacts {
		if _, err := q.ExecContext(ctx, `INSERT INTO tool_output_artifacts(
			id, run_id, thread_id, turn_id, prompt_scope_id, step, call_id, tool_name,
			effect_attempt_id, canonical_entry_id, kind, mime, safe_label, size_bytes,
			sha256, text, metadata_json, created_at
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			item.id, item.runID, item.threadID, item.turnID, item.promptScopeID, item.step,
			item.callID, item.toolName, item.canonicalEntryID, item.kind, item.mime, item.safeLabel,
			item.sizeBytes, item.sha256, item.text, item.metadataJSON, item.createdAt,
		); err != nil {
			return fmt.Errorf("copy sqlite schema v13 artifact %q: %w", item.id, err)
		}
	}
	return nil
}

func migrateSchemaVersion13SubAgentTerminalStatus(ctx context.Context, q sqlRunner) error {
	rows, err := q.QueryContext(ctx, `SELECT id, leaf_id, status FROM legacy_v13_threads
		WHERE parent_thread_id <> '' AND closed = 0 AND status IN ('cancelled', 'interrupted')
		ORDER BY id`)
	if err != nil {
		return fmt.Errorf("read sqlite schema v13 SubAgent terminal statuses: %w", err)
	}
	type terminalStatus struct{ threadID, entryID, status string }
	var items []terminalStatus
	for rows.Next() {
		var item terminalStatus
		if err := rows.Scan(&item.threadID, &item.entryID, &item.status); err != nil {
			_ = rows.Close()
			return err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, item := range items {
		entries, err := loadSchemaVersion13Entries(ctx, q, item.threadID)
		var entry sessiontree.Entry
		for _, candidate := range entries {
			if candidate.ID == item.entryID {
				entry = candidate
				break
			}
		}
		if err != nil || entry.ID == "" || entry.Type != sessiontree.EntryTurnMarker || entry.TurnStatus != sessiontree.TurnAborted {
			return fmt.Errorf("%w: sqlite schema v13 SubAgent %q has invalid terminal status authority at entry %q (%q/%q): %v", sessiontree.ErrAuthorityCorrupt, item.threadID, item.entryID, entry.Type, entry.TurnStatus, err)
		}
		entry.Metadata = cloneStringMapSQLite(entry.Metadata)
		if entry.Metadata == nil {
			entry.Metadata = map[string]string{}
		}
		if item.status == "interrupted" {
			entry.Metadata[sessiontree.TurnFailureCodeMetadataKey] = sessiontree.TurnFailureInterrupted
		} else {
			entry.Metadata[sessiontree.TurnFailureCodeMetadataKey] = sessiontree.TurnFailureCancelled
		}
		entry = sessiontree.PrepareEntry(entry)
		metadataJSON, err := json.Marshal(entry.Metadata)
		if err != nil {
			return err
		}
		result, err := q.ExecContext(ctx, `UPDATE entries SET metadata_json = ?, raw = ?, raw_hash = ?
			WHERE thread_id = ? AND id = ?`, string(metadataJSON), entry.Raw, entry.RawHash, item.threadID, item.entryID)
		if err != nil {
			return fmt.Errorf("migrate sqlite schema v13 SubAgent %q terminal status: %w", item.threadID, err)
		}
		if affected, err := result.RowsAffected(); err != nil || affected != 1 {
			return fmt.Errorf("%w: sqlite schema v13 SubAgent %q terminal status update was not unique", sessiontree.ErrAuthorityCorrupt, item.threadID)
		}
	}
	return nil
}

func validateSchemaVersion13Journals(ctx context.Context, q sqlRunner) error {
	rows, err := q.QueryContext(ctx, `SELECT id, leaf_id, parent_thread_id, closed, status FROM threads ORDER BY id`)
	if err != nil {
		return err
	}
	defer rows.Close()
	type threadLeaf struct {
		id, leaf, parentID, status string
		closed                     bool
	}
	var threads []threadLeaf
	for rows.Next() {
		var thread threadLeaf
		var closed int
		if err := rows.Scan(&thread.id, &thread.leaf, &thread.parentID, &closed, &thread.status); err != nil {
			return err
		}
		thread.closed = closed != 0
		threads = append(threads, thread)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, thread := range threads {
		entries, err := loadSchemaVersion13Entries(ctx, q, thread.id)
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			if thread.leaf != "" {
				return fmt.Errorf("%w: sqlite schema v13 thread %q has a leaf without entries", sessiontree.ErrAuthorityCorrupt, thread.id)
			}
			if err := validateSchemaVersion13ThreadStatus(thread.parentID, thread.status, thread.closed, nil); err != nil {
				return fmt.Errorf("%w: sqlite schema v13 thread %q status: %v", sessiontree.ErrAuthorityCorrupt, thread.id, err)
			}
			continue
		}
		byID := make(map[string]sessiontree.Entry, len(entries))
		started := map[string]int{}
		terminal := map[string]int{}
		roots := 0
		for _, entry := range entries {
			if err := sessiontree.ValidateEntryIntegrity(entry); err != nil {
				return fmt.Errorf("validate sqlite schema v13 entry %q: %w", entry.ID, err)
			}
			byID[entry.ID] = entry
			if entry.ParentID == "" {
				roots++
			}
			if entry.Type == sessiontree.EntryTurnMarker {
				switch entry.TurnStatus {
				case sessiontree.TurnStarted:
					started[entry.TurnID]++
				case sessiontree.TurnCompleted, sessiontree.TurnWaiting, sessiontree.TurnFailed, sessiontree.TurnAborted:
					if entry.Metadata["authority_kind"] != "branch_boundary" {
						terminal[entry.TurnID]++
					}
				}
			}
		}
		if roots != 1 {
			return fmt.Errorf("%w: sqlite schema v13 thread %q has %d journal roots", sessiontree.ErrAuthorityCorrupt, thread.id, roots)
		}
		if _, ok := byID[thread.leaf]; !ok {
			return fmt.Errorf("%w: sqlite schema v13 thread %q leaf %q is missing", sessiontree.ErrAuthorityCorrupt, thread.id, thread.leaf)
		}
		for _, entry := range entries {
			if entry.ParentID != "" {
				if _, ok := byID[entry.ParentID]; !ok {
					return fmt.Errorf("%w: sqlite schema v13 entry %q parent %q is missing", sessiontree.ErrAuthorityCorrupt, entry.ID, entry.ParentID)
				}
			}
			seen := map[string]struct{}{}
			for cursor := entry.ID; cursor != ""; cursor = byID[cursor].ParentID {
				if _, duplicate := seen[cursor]; duplicate {
					return fmt.Errorf("%w: sqlite schema v13 thread %q journal contains a cycle", sessiontree.ErrAuthorityCorrupt, thread.id)
				}
				seen[cursor] = struct{}{}
			}
		}
		for turnID, count := range started {
			if strings.TrimSpace(turnID) == "" || count != 1 || terminal[turnID] != 1 {
				return fmt.Errorf("%w: sqlite schema v13 thread %q turn %q is not quiescent", sessiontree.ErrAuthorityCorrupt, thread.id, turnID)
			}
		}
		for turnID, count := range terminal {
			if strings.TrimSpace(turnID) == "" || count != 1 || started[turnID] != 1 {
				return fmt.Errorf("%w: sqlite schema v13 thread %q terminal turn %q has no exact start", sessiontree.ErrAuthorityCorrupt, thread.id, turnID)
			}
		}
		path, err := schemaVersion13ActivePath(thread.leaf, byID)
		if err != nil {
			return fmt.Errorf("%w: sqlite schema v13 thread %q active path: %v", sessiontree.ErrAuthorityCorrupt, thread.id, err)
		}
		if err := validateSchemaVersion13ThreadStatus(thread.parentID, thread.status, thread.closed, path); err != nil {
			return fmt.Errorf("%w: sqlite schema v13 thread %q status: %v", sessiontree.ErrAuthorityCorrupt, thread.id, err)
		}
	}
	return nil
}

func schemaVersion13ActivePath(leafID string, byID map[string]sessiontree.Entry) ([]sessiontree.Entry, error) {
	if leafID == "" {
		return nil, nil
	}
	var reverse []sessiontree.Entry
	for cursor := leafID; cursor != ""; {
		entry, ok := byID[cursor]
		if !ok {
			return nil, fmt.Errorf("leaf or parent entry %q is missing", cursor)
		}
		reverse = append(reverse, entry)
		cursor = entry.ParentID
	}
	path := make([]sessiontree.Entry, len(reverse))
	for index := range reverse {
		path[len(reverse)-1-index] = reverse[index]
	}
	return path, nil
}

func validateSchemaVersion13ThreadStatus(parentID, legacyStatus string, closed bool, path []sessiontree.Entry) error {
	legacyStatus = strings.TrimSpace(legacyStatus)
	if strings.TrimSpace(parentID) == "" {
		if closed || legacyStatus != "" {
			return fmt.Errorf("root thread contains SubAgent lifecycle state")
		}
		return nil
	}
	if closed {
		if legacyStatus != "closed" {
			return fmt.Errorf("closed child has legacy status %q", legacyStatus)
		}
		return nil
	}
	if legacyStatus == "" {
		return nil
	}
	if legacyStatus == "running" || legacyStatus == "closed" {
		return fmt.Errorf("open child has non-quiescent legacy status %q", legacyStatus)
	}
	derived := schemaVersion13JournalStatus(path)
	switch legacyStatus {
	case "idle", "completed", "waiting", "failed", "cancelled", "interrupted":
		if legacyStatus != derived {
			return fmt.Errorf("legacy status %q conflicts with journal status %q", legacyStatus, derived)
		}
		return nil
	default:
		return fmt.Errorf("legacy status %q is unsupported", legacyStatus)
	}
}

func schemaVersion13JournalStatus(path []sessiontree.Entry) string {
	status := "idle"
	for _, entry := range path {
		if entry.Type != sessiontree.EntryTurnMarker {
			continue
		}
		switch entry.TurnStatus {
		case sessiontree.TurnStarted:
			status = "running"
		case sessiontree.TurnCompleted:
			status = "completed"
		case sessiontree.TurnWaiting:
			status = "waiting"
		case sessiontree.TurnFailed:
			status = "failed"
		case sessiontree.TurnAborted:
			status = "cancelled"
			if entry.Metadata["recoverable"] == "true" {
				status = "interrupted"
			}
		}
	}
	return status
}

func loadSchemaVersion13Entries(ctx context.Context, q sqlRunner, threadID string) ([]sessiontree.Entry, error) {
	rows, err := q.QueryContext(ctx, `SELECT thread_id, id, parent_id, 1 AS path_depth, type, turn_id, created_at,
		message_json, raw, raw_hash, raw_encoder_version,
		turn_status, provider, model, compaction_id, previous_compaction_id,
		compacted_through_entry_id, summary_schema_version, compaction_generation,
		compaction_window_id, first_kept_entry_id, kept_user_entry_ids_json, summary, compaction_trigger,
		compaction_reason, compaction_phase, tokens_before, tokens_after_estimate,
		compaction_operation_id, compaction_request_id, compaction_source,
		context_usage_before_json, context_usage_after_json, error, metadata_json
		FROM entries WHERE thread_id = ? ORDER BY ordinal`, threadID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var entries []sessiontree.Entry
	for rows.Next() {
		entry, err := scanEntry(rows)
		if err != nil {
			return nil, err
		}
		entry.PathDepth = 0
		entries = append(entries, entry)
	}
	return entries, rows.Err()
}

func dropSchemaVersion13NamedIndexes(ctx context.Context, q sqlRunner) error {
	rows, err := q.QueryContext(ctx, `SELECT name FROM sqlite_master
		WHERE type = 'index' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	if err != nil {
		return err
	}
	var indexes []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			_ = rows.Close()
			return err
		}
		indexes = append(indexes, name)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, index := range indexes {
		if _, err := q.ExecContext(ctx, `DROP INDEX `+quoteSchemaName(index)); err != nil {
			return fmt.Errorf("drop sqlite schema v13 index %q: %w", index, err)
		}
	}
	return nil
}

func copySchemaVersion13Threads(ctx context.Context, q sqlRunner) error {
	_, err := q.ExecContext(ctx, `INSERT INTO threads(
		id, leaf_id, parent_thread_id, parent_turn_id, forked_from_thread_id, forked_from_entry_id,
		fork_operation_id, fork_operation_node_id, task_name, task_description, agent_path,
		host_profile_ref, fork_mode, lifecycle, close_operation_id, archived,
		title, title_status, title_source, title_updated_at, title_error,
		created_at, updated_at, last_viewed_at, lease_generation
	) SELECT
		id, leaf_id, parent_thread_id, parent_turn_id, forked_from_thread_id, forked_from_entry_id,
		fork_operation_id, fork_operation_node_id, task_name, task_description, agent_path,
		host_profile_ref, fork_mode, CASE WHEN closed = 1 THEN 'closed' ELSE 'open' END, '', archived,
		title, title_status, title_source, title_updated_at, title_error,
		created_at, updated_at, last_viewed_at, 0
	FROM legacy_v13_threads`)
	if err != nil {
		return fmt.Errorf("copy sqlite schema v13 threads: %w", err)
	}
	return nil
}

func copyLegacySharedColumns(ctx context.Context, q sqlRunner, table string) error {
	columns, err := sharedLegacyTableColumns(ctx, q, table)
	if err != nil {
		return err
	}
	if len(columns) == 0 {
		return fmt.Errorf("sqlite schema v13 table %q has no migratable columns", table)
	}
	quoted := make([]string, len(columns))
	for index, column := range columns {
		quoted[index] = quoteSchemaName(column)
	}
	list := strings.Join(quoted, ", ")
	if _, err := q.ExecContext(ctx, `INSERT INTO `+quoteSchemaName(table)+` (`+list+`) SELECT `+list+` FROM `+quoteSchemaName(legacySchemaVersion13Table(table))); err != nil {
		return fmt.Errorf("copy sqlite schema v13 table %q: %w", table, err)
	}
	return nil
}

func sharedLegacyTableColumns(ctx context.Context, q sqlRunner, table string) ([]string, error) {
	return sharedTableColumns(ctx, q, legacySchemaVersion13Table(table), table)
}

func sharedTableColumns(ctx context.Context, q sqlRunner, legacyTable, currentTable string) ([]string, error) {
	read := func(name string) ([]string, error) {
		rows, err := q.QueryContext(ctx, `PRAGMA table_info(`+quoteSchemaName(name)+`)`)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var columns []string
		for rows.Next() {
			var cid, notNull, primaryKey int
			var columnName, columnType string
			var defaultValue any
			if err := rows.Scan(&cid, &columnName, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
				return nil, err
			}
			columns = append(columns, columnName)
		}
		return columns, rows.Err()
	}
	legacyColumns, err := read(legacyTable)
	if err != nil {
		return nil, err
	}
	currentColumns, err := read(currentTable)
	if err != nil {
		return nil, err
	}
	available := make(map[string]struct{}, len(currentColumns))
	for _, column := range currentColumns {
		available[column] = struct{}{}
	}
	var shared []string
	for _, column := range legacyColumns {
		if _, ok := available[column]; ok {
			shared = append(shared, column)
		}
	}
	return shared, nil
}

func legacySchemaVersion13Table(table string) string {
	return "legacy_v13_" + table
}

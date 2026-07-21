package sqlite

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/floegence/floret/internal/sessiontree"
	"github.com/floegence/floret/internal/storage"
)

var schemaVersion13DropOrder = []string{
	"metadata_records",
	"tool_output_artifacts",
	"prompt_responses",
	"prompt_requests",
	"prompt_toolsets",
	"prompt_segments",
	"active_turn_leases",
	"agent_todo_states",
	"provider_states",
	"entries",
	"fork_operations",
	"threads",
	"schema_meta",
}

func migrateEmptySchemaVersion13(ctx context.Context, tx sqlRunner, leasePolicy sessiontree.LeasePolicy) error {
	if err := verifySchemaVersion(ctx, tx, schemaVersion13); err != nil {
		return fmt.Errorf("verify sqlite store schema v13 before migration: %w", err)
	}
	nonEmpty, err := nonEmptySchemaVersion13Tables(ctx, tx)
	if err != nil {
		return err
	}
	if len(nonEmpty) > 0 {
		return fmt.Errorf("%w: sqlite store schema v13 is not empty: tables %s contain data", unsupportedSchemaError(schemaVersion13, schemaFingerprintVersion13), strings.Join(nonEmpty, ", "))
	}
	for _, tableName := range schemaVersion13DropOrder {
		if _, err := tx.ExecContext(ctx, `DROP TABLE `+quoteSchemaName(tableName)); err != nil {
			return fmt.Errorf("drop sqlite schema v13 table %q: %w", tableName, err)
		}
	}
	if _, err := tx.ExecContext(ctx, schemaSQL); err != nil {
		return fmt.Errorf("create sqlite store schema v16: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO schema_meta(key, value) VALUES
		('schema_version', ?), ('raw_encoder_version', ?), ('schema_fingerprint', ?)`, schemaVersion, rawEncoderVersion, schemaFingerprintVersion16); err != nil {
		return fmt.Errorf("write sqlite store schema v16 metadata: %w", err)
	}
	if err := persistLeasePolicy(ctx, tx, leasePolicy); err != nil {
		return err
	}
	return nil
}

func migrateSchemaVersion14(ctx context.Context, tx sqlRunner) error {
	if err := verifySchemaVersion(ctx, tx, schemaVersion14); err != nil {
		return fmt.Errorf("verify sqlite store schema v14 before migration: %w", err)
	}
	if _, err := tx.ExecContext(ctx, canonicalTurnIndexSQL); err != nil {
		return fmt.Errorf("create sqlite store canonical turn indexes: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE schema_meta SET value = ? WHERE key = 'schema_version'`, schemaVersion15); err != nil {
		return fmt.Errorf("write sqlite store schema v15 version: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE schema_meta SET value = ? WHERE key = 'schema_fingerprint'`, schemaFingerprintVersion15); err != nil {
		return fmt.Errorf("write sqlite store schema v15 fingerprint: %w", err)
	}
	return nil
}

func migrateSchemaVersion15(ctx context.Context, tx sqlRunner) error {
	if err := verifySchemaVersion(ctx, tx, schemaVersion15); err != nil {
		return fmt.Errorf("verify sqlite store schema v15 before migration: %w", err)
	}
	var invalid int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM threads WHERE NOT (
		(title_status = '' AND title = '' AND title_source = '' AND title_updated_at = '' AND title_error = '') OR
		(title_status = 'ready' AND trim(title) <> '' AND title_source IN ('host', 'provider') AND title_updated_at <> '' AND title_error = '') OR
		(title_status = 'failed' AND title = '' AND title_source = '' AND title_updated_at <> '' AND trim(title_error) <> '')
	)`).Scan(&invalid); err != nil {
		return fmt.Errorf("validate sqlite store v15 title state: %w", err)
	}
	if invalid != 0 {
		return fmt.Errorf("%w: sqlite store schema v15 contains %d invalid thread title states", sessiontree.ErrAuthorityCorrupt, invalid)
	}
	if err := validateSchemaVersion15StartedRunIdentities(ctx, tx); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `ALTER TABLE entries ADD COLUMN path_depth INTEGER NOT NULL DEFAULT 1 CHECK (path_depth > 0)`); err != nil {
		return fmt.Errorf("add sqlite store entry path depth: %w", err)
	}
	if err := migrateSchemaVersion15EntryPathDepths(ctx, tx); err != nil {
		return err
	}
	if err := migrateSchemaVersion15TurnFailures(ctx, tx); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `ALTER TABLE threads ADD COLUMN title_generation INTEGER NOT NULL DEFAULT 0 CHECK (title_generation >= 0)`); err != nil {
		return fmt.Errorf("add sqlite store title generation: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `ALTER TABLE threads ADD COLUMN title_token TEXT NOT NULL DEFAULT ''`); err != nil {
		return fmt.Errorf("add sqlite store title token: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE threads SET
		title_generation = 1,
		title_token = CASE WHEN title_source = 'provider' OR title_status = 'failed' THEN 'migrated-v15:' || id ELSE '' END
		WHERE title_status <> ''`); err != nil {
		return fmt.Errorf("migrate sqlite store title authority state: %w", err)
	}
	if err := migrateSchemaVersion15RetrySources(ctx, tx); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, pendingThreadTitleIndexSQL); err != nil {
		return fmt.Errorf("create sqlite store pending title index: %w", err)
	}
	if _, err := tx.ExecContext(ctx, approvalAuthoritySQL); err != nil {
		return fmt.Errorf("create sqlite store approval authority: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE schema_meta SET value = ? WHERE key = 'schema_version'`, schemaVersion); err != nil {
		return fmt.Errorf("write sqlite store schema v16 version: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE schema_meta SET value = ? WHERE key = 'schema_fingerprint'`, schemaFingerprintVersion16); err != nil {
		return fmt.Errorf("write sqlite store schema v16 fingerprint: %w", err)
	}
	return nil
}

func migrateSchemaVersion15EntryPathDepths(ctx context.Context, q sqlRunner) error {
	var invalid int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM (
		SELECT thread_id
		FROM entries
		GROUP BY thread_id
		HAVING SUM(CASE WHEN parent_id = '' THEN 1 ELSE 0 END) <> 1
	)`).Scan(&invalid); err != nil {
		return fmt.Errorf("validate sqlite store v15 journal roots: %w", err)
	}
	if invalid != 0 {
		return fmt.Errorf("%w: sqlite store schema v15 contains %d journals without exactly one root", sessiontree.ErrAuthorityCorrupt, invalid)
	}
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*)
		FROM entries child
		LEFT JOIN entries parent ON parent.thread_id = child.thread_id AND parent.id = child.parent_id
		WHERE child.parent_id <> '' AND parent.id IS NULL`).Scan(&invalid); err != nil {
		return fmt.Errorf("validate sqlite store v15 journal parents: %w", err)
	}
	if invalid != 0 {
		return fmt.Errorf("%w: sqlite store schema v15 contains %d entries with missing parents", sessiontree.ErrAuthorityCorrupt, invalid)
	}

	const depths = `WITH RECURSIVE entry_depths(thread_id, id, path_depth) AS (
		SELECT entry.thread_id, entry.id, 1
		FROM entries entry
		WHERE entry.parent_id = ''
		UNION ALL
		SELECT child.thread_id, child.id, parent.path_depth + 1
		FROM entries child
		JOIN entry_depths parent ON parent.thread_id = child.thread_id AND parent.id = child.parent_id
	)`
	if err := q.QueryRowContext(ctx, depths+`
		SELECT (SELECT COUNT(*) FROM entries) - (SELECT COUNT(*) FROM entry_depths)`).Scan(&invalid); err != nil {
		return fmt.Errorf("validate sqlite store v15 journal reachability: %w", err)
	}
	if invalid != 0 {
		return fmt.Errorf("%w: sqlite store schema v15 contains %d cyclic or unreachable entries", sessiontree.ErrAuthorityCorrupt, invalid)
	}
	if _, err := q.ExecContext(ctx, depths+`
		UPDATE entries
		SET path_depth = (
			SELECT entry_depths.path_depth
			FROM entry_depths
			WHERE entry_depths.thread_id = entries.thread_id AND entry_depths.id = entries.id
		)`); err != nil {
		return fmt.Errorf("backfill sqlite store entry path depths: %w", err)
	}
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*)
		FROM entries child
		LEFT JOIN entries parent ON parent.thread_id = child.thread_id AND parent.id = child.parent_id
		WHERE (child.parent_id = '' AND child.path_depth <> 1)
			OR (child.parent_id <> '' AND child.path_depth <> parent.path_depth + 1)`).Scan(&invalid); err != nil {
		return fmt.Errorf("verify sqlite store entry path depths: %w", err)
	}
	if invalid != 0 {
		return fmt.Errorf("%w: sqlite store schema v15 path depth backfill produced %d invalid entries", sessiontree.ErrAuthorityCorrupt, invalid)
	}
	return nil
}

func migrateSchemaVersion15RetrySources(ctx context.Context, q sqlRunner) error {
	rows, err := q.QueryContext(ctx, `SELECT thread_id, turn_id, run_id, turn_started_id, base_leaf_id
		FROM turn_admissions
		WHERE user_message_id IS NULL
		ORDER BY thread_id, turn_id`)
	if err != nil {
		return fmt.Errorf("read sqlite store v15 retry admissions: %w", err)
	}
	type retryAdmission struct {
		threadID, turnID, runID, startedEntryID, sourceEntryID string
	}
	var admissions []retryAdmission
	for rows.Next() {
		var admission retryAdmission
		if err := rows.Scan(&admission.threadID, &admission.turnID, &admission.runID, &admission.startedEntryID, &admission.sourceEntryID); err != nil {
			_ = rows.Close()
			return fmt.Errorf("read sqlite store v15 retry admission: %w", err)
		}
		admissions = append(admissions, admission)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("read sqlite store v15 retry admissions: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close sqlite store v15 retry admissions: %w", err)
	}

	for _, admission := range admissions {
		admission.threadID = strings.TrimSpace(admission.threadID)
		admission.turnID = strings.TrimSpace(admission.turnID)
		admission.runID = strings.TrimSpace(admission.runID)
		admission.startedEntryID = strings.TrimSpace(admission.startedEntryID)
		admission.sourceEntryID = strings.TrimSpace(admission.sourceEntryID)
		if admission.threadID == "" || admission.turnID == "" || admission.runID == "" || admission.startedEntryID == "" || admission.sourceEntryID == "" {
			return fmt.Errorf("%w: sqlite store schema v15 contains an incomplete retry admission", sessiontree.ErrAuthorityCorrupt)
		}
		started, err := loadEntry(ctx, q, admission.threadID, admission.startedEntryID)
		if err != nil || started.Type != sessiontree.EntryTurnMarker || started.TurnStatus != sessiontree.TurnStarted || started.TurnID != admission.turnID {
			return fmt.Errorf("%w: sqlite store schema v15 retry %q has an invalid started entry", sessiontree.ErrAuthorityCorrupt, admission.turnID)
		}
		if started.Metadata["run_id"] != admission.runID {
			return fmt.Errorf("%w: sqlite store schema v15 retry %q admission run does not match its started entry", sessiontree.ErrAuthorityCorrupt, admission.turnID)
		}
		if started.Metadata[sessiontree.RetrySourceTurnIDMetadataKey] != "" || started.Metadata[sessiontree.RetrySourceEntryIDMetadataKey] != "" {
			return fmt.Errorf("%w: sqlite store schema v15 retry %q has unsupported source metadata", sessiontree.ErrAuthorityCorrupt, admission.turnID)
		}
		source, err := loadEntry(ctx, q, admission.threadID, admission.sourceEntryID)
		if err != nil || strings.TrimSpace(source.TurnID) == "" || strings.TrimSpace(source.TurnID) == admission.turnID {
			return fmt.Errorf("%w: sqlite store schema v15 retry %q has an invalid source entry", sessiontree.ErrAuthorityCorrupt, admission.turnID)
		}
		path, err := pathWithRunner(ctx, q, admission.threadID, admission.startedEntryID)
		if err != nil {
			return fmt.Errorf("%w: sqlite store schema v15 retry %q source path is invalid: %v", sessiontree.ErrAuthorityCorrupt, admission.turnID, err)
		}
		if _, err := sessiontree.ValidateRetrySourcePath(path, source.TurnID, source.ID); err != nil {
			return fmt.Errorf("%w: sqlite store schema v15 retry %q source is not a user or save-point target", sessiontree.ErrAuthorityCorrupt, admission.turnID)
		}
		started.Metadata[sessiontree.RetrySourceTurnIDMetadataKey] = source.TurnID
		started.Metadata[sessiontree.RetrySourceEntryIDMetadataKey] = source.ID
		started = sessiontree.PrepareEntry(started)
		requestFingerprint, err := sessiontree.TurnAdmissionRequestFingerprint(sessiontree.AdmitTurnRequest{
			ThreadID: admission.threadID, TurnID: admission.turnID, RunID: admission.runID,
			RetrySourceTurnID: source.TurnID, RetrySourceEntryID: source.ID,
		})
		if err != nil {
			return fmt.Errorf("encode sqlite store v15 retry request fingerprint: %w", err)
		}
		metadataJSON, err := json.Marshal(started.Metadata)
		if err != nil {
			return fmt.Errorf("encode sqlite store v15 retry source metadata: %w", err)
		}
		result, err := q.ExecContext(ctx, `UPDATE entries SET metadata_json = ?, raw = ?, raw_hash = ?
			WHERE thread_id = ? AND id = ?`, string(metadataJSON), started.Raw, started.RawHash, admission.threadID, admission.startedEntryID)
		if err != nil {
			return fmt.Errorf("write sqlite store v15 retry source metadata: %w", err)
		}
		if affected, err := result.RowsAffected(); err != nil || affected != 1 {
			return fmt.Errorf("%w: sqlite store schema v15 retry %q source update was not unique", sessiontree.ErrAuthorityCorrupt, admission.turnID)
		}
		result, err = q.ExecContext(ctx, `UPDATE turn_admissions SET request_fingerprint = ?
			WHERE thread_id = ? AND turn_id = ?`, requestFingerprint, admission.threadID, admission.turnID)
		if err != nil {
			return fmt.Errorf("write sqlite store v15 retry request fingerprint: %w", err)
		}
		if affected, err := result.RowsAffected(); err != nil || affected != 1 {
			return fmt.Errorf("%w: sqlite store schema v15 retry %q fingerprint update was not unique", sessiontree.ErrAuthorityCorrupt, admission.turnID)
		}
	}
	return nil
}

func validateSchemaVersion15StartedRunIdentities(ctx context.Context, q sqlRunner) error {
	rows, err := q.QueryContext(ctx, `SELECT metadata_json FROM entries WHERE type = 'turn_marker' AND turn_status = 'started'`)
	if err != nil {
		return fmt.Errorf("validate sqlite store v15 started run identities: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var metadataJSON string
		if err := rows.Scan(&metadataJSON); err != nil {
			return fmt.Errorf("validate sqlite store v15 started run identities: %w", err)
		}
		var metadata map[string]string
		if err := json.Unmarshal([]byte(metadataJSON), &metadata); err != nil {
			return fmt.Errorf("%w: sqlite store schema v15 contains invalid started run metadata", sessiontree.ErrAuthorityCorrupt)
		}
		rawRunID := metadata["run_id"]
		runID := strings.TrimSpace(rawRunID)
		if runID == "" || rawRunID != runID {
			return fmt.Errorf("%w: sqlite store schema v15 contains a non-canonical started run identity", sessiontree.ErrAuthorityCorrupt)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("validate sqlite store v15 started run identities: %w", err)
	}
	return nil
}

const schemaVersion15LegacyTurnStatusMetadataKey = "legacy_turn_status"

func migrateSchemaVersion15TurnFailures(ctx context.Context, q sqlRunner) error {
	rows, err := q.QueryContext(ctx, `SELECT thread_id, id FROM entries
		WHERE type = 'turn_marker' AND turn_status IN ('completed', 'waiting', 'failed', 'aborted')
		ORDER BY thread_id, ordinal`)
	if err != nil {
		return fmt.Errorf("read sqlite store v15 terminal turn entries: %w", err)
	}
	type identity struct {
		threadID string
		entryID  string
	}
	var identities []identity
	for rows.Next() {
		var item identity
		if err := rows.Scan(&item.threadID, &item.entryID); err != nil {
			_ = rows.Close()
			return fmt.Errorf("read sqlite store v15 terminal turn entries: %w", err)
		}
		identities = append(identities, item)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("read sqlite store v15 terminal turn entries: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close sqlite store v15 terminal turn entries: %w", err)
	}

	for _, item := range identities {
		terminal, err := loadEntry(ctx, q, item.threadID, item.entryID)
		if err != nil {
			return fmt.Errorf("load sqlite store v15 terminal turn entry: %w", err)
		}
		if err := sessiontree.ValidateEntryIntegrity(terminal); err != nil {
			return fmt.Errorf("%w: sqlite store schema v15 contains an invalid terminal turn entry", sessiontree.ErrAuthorityCorrupt)
		}
		failureMessage := ""
		if terminal.ParentID != "" {
			parent, err := loadEntry(ctx, q, terminal.ThreadID, terminal.ParentID)
			if err != nil {
				return fmt.Errorf("load sqlite store v15 terminal parent entry: %w", err)
			}
			if err := sessiontree.ValidateEntryIntegrity(parent); err != nil {
				return fmt.Errorf("%w: sqlite store schema v15 contains an invalid terminal parent entry", sessiontree.ErrAuthorityCorrupt)
			}
			if parent.Type == sessiontree.EntryRunFailure && parent.TurnID == terminal.TurnID {
				failureMessage = strings.TrimSpace(parent.Error)
			}
		}
		rawExistingCode := terminal.Metadata[sessiontree.TurnFailureCodeMetadataKey]
		existingCode := strings.TrimSpace(rawExistingCode)
		if rawExistingCode != existingCode {
			return fmt.Errorf("%w: sqlite store schema v15 contains a non-canonical terminal failure code", sessiontree.ErrAuthorityCorrupt)
		}
		structuredCode, structured, err := schemaVersion15StructuredTurnFailureCode(ctx, q, terminal)
		if err != nil {
			return err
		}
		if terminal.TurnStatus == sessiontree.TurnAborted && strings.TrimSpace(terminal.Metadata["authority_kind"]) == "branch_boundary" {
			if failureMessage != "" || structured || (existingCode != "" && existingCode != sessiontree.TurnFailureInterrupted) {
				return fmt.Errorf("%w: sqlite store schema v15 contains an invalid branch boundary terminal", sessiontree.ErrAuthorityCorrupt)
			}
			terminal.Metadata = cloneStringMapSQLite(terminal.Metadata)
			if terminal.Metadata == nil {
				terminal.Metadata = map[string]string{}
			}
			terminal.Metadata[sessiontree.TurnFailureCodeMetadataKey] = sessiontree.TurnFailureInterrupted
			terminal.Metadata["failure_reason"] = sessiontree.BranchBoundaryTurnFailureMessage
			if err := updateSchemaVersion15TerminalFailure(ctx, q, terminal); err != nil {
				return err
			}
			continue
		}
		switch terminal.TurnStatus {
		case sessiontree.TurnCompleted, sessiontree.TurnWaiting:
			if failureMessage != "" || existingCode != "" || structured {
				return fmt.Errorf("%w: sqlite store schema v15 contains a successful or waiting turn with failure data", sessiontree.ErrAuthorityCorrupt)
			}
			continue
		case sessiontree.TurnFailed, sessiontree.TurnAborted:
			if failureMessage == "" {
				return fmt.Errorf("%w: sqlite store schema v15 contains a terminal failure without a message", sessiontree.ErrAuthorityCorrupt)
			}
		default:
			return fmt.Errorf("%w: sqlite store schema v15 contains an unsupported terminal turn status", sessiontree.ErrAuthorityCorrupt)
		}

		failureCode := existingCode
		if failureCode != "" {
			if !sessiontree.ValidTurnFailureCode(failureCode) || !schemaVersion15FailureCodeMatchesStatus(terminal.TurnStatus, failureCode) {
				return fmt.Errorf("%w: sqlite store schema v15 contains an invalid terminal failure code", sessiontree.ErrAuthorityCorrupt)
			}
			if structured && failureCode != structuredCode {
				return fmt.Errorf("%w: sqlite store schema v15 contains a conflicting terminal failure code", sessiontree.ErrAuthorityCorrupt)
			}
			continue
		} else if structured {
			failureCode = structuredCode
		} else {
			failureCode = sessiontree.TurnFailureLegacyUnclassified
		}

		terminal.Metadata = cloneStringMapSQLite(terminal.Metadata)
		if terminal.Metadata == nil {
			terminal.Metadata = map[string]string{}
		}
		if terminal.TurnStatus == sessiontree.TurnAborted && existingCode == "" {
			terminal.Metadata[schemaVersion15LegacyTurnStatusMetadataKey] = string(sessiontree.TurnAborted)
			terminal.TurnStatus = sessiontree.TurnFailed
		}
		terminal.Metadata[sessiontree.TurnFailureCodeMetadataKey] = failureCode
		if err := updateSchemaVersion15TerminalFailure(ctx, q, terminal); err != nil {
			return err
		}
	}
	return nil
}

func updateSchemaVersion15TerminalFailure(ctx context.Context, q sqlRunner, terminal sessiontree.Entry) error {
	terminal = sessiontree.PrepareEntry(terminal)
	metadataJSON, err := json.Marshal(terminal.Metadata)
	if err != nil {
		return fmt.Errorf("encode sqlite store v15 terminal failure metadata: %w", err)
	}
	result, err := q.ExecContext(ctx, `UPDATE entries SET turn_status = ?, metadata_json = ?, raw = ?, raw_hash = ?
		WHERE thread_id = ? AND id = ?`, string(terminal.TurnStatus), string(metadataJSON), terminal.Raw, terminal.RawHash, terminal.ThreadID, terminal.ID)
	if err != nil {
		return fmt.Errorf("migrate sqlite store v15 terminal failure: %w", err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return fmt.Errorf("%w: sqlite store schema v15 terminal failure update was not unique", sessiontree.ErrAuthorityCorrupt)
	}
	return nil
}

func schemaVersion15StructuredTurnFailureCode(ctx context.Context, q sqlRunner, terminal sessiontree.Entry) (string, bool, error) {
	var unknownEffects int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM effect_attempts
		WHERE thread_id = ? AND turn_id = ? AND state = 'unknown'`, terminal.ThreadID, terminal.TurnID).Scan(&unknownEffects); err != nil {
		return "", false, fmt.Errorf("read sqlite store v15 unknown effect authority: %w", err)
	}
	if unknownEffects > 0 {
		return sessiontree.TurnFailureEffectOutcomeUnknown, true, nil
	}
	return "", false, nil
}

func schemaVersion15FailureCodeMatchesStatus(status sessiontree.TurnMarkerStatus, failureCode string) bool {
	if status == sessiontree.TurnAborted {
		return failureCode == sessiontree.TurnFailureCancelled || failureCode == sessiontree.TurnFailureInterrupted
	}
	return failureCode != sessiontree.TurnFailureCancelled && failureCode != sessiontree.TurnFailureInterrupted
}

func persistLeasePolicy(ctx context.Context, q sqlRunner, policy sessiontree.LeasePolicy) error {
	if err := policy.Validate(); err != nil {
		return fmt.Errorf("validate sqlite store lease policy: %w", err)
	}
	if _, err := q.ExecContext(ctx, `INSERT INTO authority_lease_policy(
		singleton, lease_ttl_ns, renew_interval_ns, clock_skew_allowance_ns
	) VALUES(1, ?, ?, ?)`, int64(policy.TTL), int64(policy.RenewInterval), int64(policy.ClockSkewAllowance)); err != nil {
		return fmt.Errorf("persist sqlite store lease policy: %w", err)
	}
	return nil
}

func verifyLeasePolicy(ctx context.Context, q sqlRunner, configured sessiontree.LeasePolicy) error {
	var ttl, renew, skew int64
	if err := q.QueryRowContext(ctx, `SELECT lease_ttl_ns, renew_interval_ns, clock_skew_allowance_ns
		FROM authority_lease_policy WHERE singleton = 1`).Scan(&ttl, &renew, &skew); err != nil {
		return fmt.Errorf("read sqlite store lease policy: %w", err)
	}
	persisted := sessiontree.LeasePolicy{
		TTL:                time.Duration(ttl),
		RenewInterval:      time.Duration(renew),
		ClockSkewAllowance: time.Duration(skew),
	}
	if persisted != configured {
		return &storage.StoreLeasePolicyMismatchError{Configured: configured, Persisted: persisted}
	}
	return nil
}

func nonEmptySchemaVersion13Tables(ctx context.Context, q sqlRunner) ([]string, error) {
	tables, err := userTableNames(ctx, q)
	if err != nil {
		return nil, err
	}
	var nonEmpty []string
	for _, tableName := range tables {
		if tableName == "schema_meta" {
			continue
		}
		var count int
		if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+quoteSchemaName(tableName)).Scan(&count); err != nil {
			return nil, fmt.Errorf("count sqlite schema v13 table %q: %w", tableName, err)
		}
		if count > 0 {
			nonEmpty = append(nonEmpty, tableName)
		}
	}
	return nonEmpty, nil
}

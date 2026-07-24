package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

const (
	schemaVersion12            = "12"
	schemaVersion11            = "11"
	schemaFingerprintVersion11 = "200ee3cee291df623ac3cc7d603121cb22601992b4851e13fb859081053f4c85"
	schemaFingerprintVersion12 = "2586cafa8e761a8ed2e6d1227e6eb1a3f706332c590a8e7d1e6045f185520446"
)

func migrateLegacySchemaToVersion13(ctx context.Context, tx sqlRunner, current string) error {
	rawVersion, err := metaValue(ctx, tx, "raw_encoder_version")
	if err != nil {
		return fmt.Errorf("read legacy sqlite store raw encoder version: %w", err)
	}
	if rawVersion != rawEncoderVersion {
		return fmt.Errorf("unsupported legacy sqlite store raw encoder version %q", rawVersion)
	}

	if current == "3" || current == "4" {
		if err := migrateLegacyThreadTitleColumns(ctx, tx, current); err != nil {
			return err
		}
		if err := migrateLegacyPromptScopeColumns(ctx, tx); err != nil {
			return fmt.Errorf("migrate sqlite store v%s prompt scope identity: %w", current, err)
		}
		if err := addLegacyColumnIfMissing(ctx, tx, "entries", "kept_user_entry_ids_json", `ALTER TABLE entries ADD COLUMN kept_user_entry_ids_json TEXT NOT NULL DEFAULT '[]'`); err != nil {
			return fmt.Errorf("migrate sqlite store v%s kept user entry identity: %w", current, err)
		}
		if _, err := tx.ExecContext(ctx, legacyToolOutputArtifactsSQL); err != nil {
			return fmt.Errorf("migrate sqlite store v%s tool output artifacts: %w", current, err)
		}
		current = "5"
	}
	if current == "5" {
		if err := addLegacyColumnIfMissing(ctx, tx, "threads", "status", `ALTER TABLE threads ADD COLUMN status TEXT NOT NULL DEFAULT ''`); err != nil {
			return fmt.Errorf("migrate sqlite store v5 thread status: %w", err)
		}
		if err := addLegacyColumnIfMissing(ctx, tx, "threads", "last_viewed_at", `ALTER TABLE threads ADD COLUMN last_viewed_at TEXT NOT NULL DEFAULT ''`); err != nil {
			return fmt.Errorf("migrate sqlite store v5 last viewed time: %w", err)
		}
		current = "6"
	}
	if current == "6" {
		columns := []struct {
			name string
			stmt string
		}{
			{name: "parent_turn_id", stmt: `ALTER TABLE threads ADD COLUMN parent_turn_id TEXT NOT NULL DEFAULT ''`},
			{name: "task_name", stmt: `ALTER TABLE threads ADD COLUMN task_name TEXT NOT NULL DEFAULT ''`},
			{name: "agent_path", stmt: `ALTER TABLE threads ADD COLUMN agent_path TEXT NOT NULL DEFAULT ''`},
			{name: "host_profile_ref", stmt: `ALTER TABLE threads ADD COLUMN host_profile_ref TEXT NOT NULL DEFAULT ''`},
			{name: "closed", stmt: `ALTER TABLE threads ADD COLUMN closed INTEGER NOT NULL DEFAULT 0`},
		}
		for _, column := range columns {
			if err := addLegacyColumnIfMissing(ctx, tx, "threads", column.name, column.stmt); err != nil {
				return fmt.Errorf("migrate sqlite store v6 thread %s: %w", column.name, err)
			}
		}
		if _, err := tx.ExecContext(ctx, `DROP INDEX IF EXISTS threads_subagent_path_unique`); err != nil {
			return fmt.Errorf("migrate sqlite store v6 subagent index: %w", err)
		}
		current = "7"
	}
	if current == "7" {
		if err := addLegacyColumnIfMissing(ctx, tx, "threads", "fork_mode", `ALTER TABLE threads ADD COLUMN fork_mode TEXT NOT NULL DEFAULT ''`); err != nil {
			return fmt.Errorf("migrate sqlite store v7 fork mode: %w", err)
		}
		current = "8"
	}
	if current == "8" {
		if err := addLegacyColumnIfMissing(ctx, tx, "threads", "task_description", `ALTER TABLE threads ADD COLUMN task_description TEXT NOT NULL DEFAULT ''`); err != nil {
			return fmt.Errorf("migrate sqlite store v8 task description: %w", err)
		}
		current = "9"
	}
	if current == "9" {
		if err := addLegacyColumnIfMissing(ctx, tx, "threads", "fork_operation_id", `ALTER TABLE threads ADD COLUMN fork_operation_id TEXT NOT NULL DEFAULT ''`); err != nil {
			return fmt.Errorf("migrate sqlite store v9 fork operation id: %w", err)
		}
		if err := addLegacyColumnIfMissing(ctx, tx, "threads", "fork_operation_node_id", `ALTER TABLE threads ADD COLUMN fork_operation_node_id TEXT NOT NULL DEFAULT ''`); err != nil {
			return fmt.Errorf("migrate sqlite store v9 fork operation node id: %w", err)
		}
		if _, err := tx.ExecContext(ctx, legacyForkOperationsSQL); err != nil {
			return fmt.Errorf("migrate sqlite store v9 fork operations: %w", err)
		}
		current = "10"
	}
	if current == "10" {
		if err := rebuildLegacySchemaVersion10To11(ctx, tx); err != nil {
			return err
		}
		current = schemaVersion11
	}
	migratedFromVersion11 := false
	if current == schemaVersion11 {
		fingerprint, err := metaValue(ctx, tx, "schema_fingerprint")
		if err != nil {
			return fmt.Errorf("read sqlite store schema v11 fingerprint: %w", err)
		}
		if fingerprint != schemaFingerprintVersion11 {
			return fmt.Errorf("unsupported sqlite store schema v11 fingerprint %q", fingerprint)
		}
		if _, err := tx.ExecContext(ctx, legacyAgentTodoStatesSQL); err != nil {
			return fmt.Errorf("migrate sqlite store v11 agent todo state: %w", err)
		}
		current = schemaVersion12
		migratedFromVersion11 = true
	}
	if current != schemaVersion12 {
		return fmt.Errorf("unsupported legacy sqlite store schema version %q", current)
	}
	if fingerprint, err := metaValue(ctx, tx, "schema_fingerprint"); err != nil || fingerprint != schemaFingerprintVersion12 {
		if migratedFromVersion11 {
			// A v11 migration reaches the exact v12 shape before metadata is advanced.
			if err := putLegacySchemaMeta(ctx, tx, schemaVersion12, schemaFingerprintVersion12); err != nil {
				return err
			}
		} else {
			if err != nil {
				return fmt.Errorf("read sqlite store schema v12 fingerprint: %w", err)
			}
			return fmt.Errorf("unsupported sqlite store schema v12 fingerprint %q", fingerprint)
		}
	}
	if err := migrateSchemaVersion12To13(ctx, tx); err != nil {
		return fmt.Errorf("migrate sqlite store schema v12 to v13: %w", err)
	}
	if err := putLegacySchemaMeta(ctx, tx, schemaVersion13, schemaFingerprintVersion13); err != nil {
		return err
	}
	return verifySchemaVersion(ctx, tx, schemaVersion13)
}

var schemaVersion11SQL = strings.Replace(schemaVersion13SQL, legacyAgentTodoStatesSQL, "", 1)

var schemaVersion10Tables = []string{
	"schema_meta",
	"threads",
	"fork_operations",
	"entries",
	"active_turn_leases",
	"prompt_segments",
	"prompt_toolsets",
	"prompt_requests",
	"prompt_responses",
	"tool_output_artifacts",
	"metadata_records",
}

func rebuildLegacySchemaVersion10To11(ctx context.Context, tx sqlRunner) error {
	if schemaVersion11SQL == schemaVersion13SQL {
		return errors.New("derive sqlite schema v11 without agent todo state")
	}
	if err := dropSchemaVersion13NamedIndexes(ctx, tx); err != nil {
		return err
	}
	for _, table := range schemaVersion10Tables {
		if _, err := tx.ExecContext(ctx, `ALTER TABLE `+quoteSchemaName(table)+` RENAME TO `+quoteSchemaName(legacySchemaVersion10Table(table))); err != nil {
			return fmt.Errorf("rename sqlite schema v10 table %q: %w", table, err)
		}
	}
	if _, err := tx.ExecContext(ctx, schemaVersion11SQL); err != nil {
		return fmt.Errorf("create canonical sqlite schema v11: %w", err)
	}
	for _, table := range []string{
		"threads", "fork_operations", "entries", "active_turn_leases",
		"prompt_segments", "prompt_toolsets", "prompt_requests", "prompt_responses",
		"tool_output_artifacts", "metadata_records",
	} {
		if err := copyLegacyVersion10SharedColumns(ctx, tx, table); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO schema_meta(key, value) VALUES
		('schema_version', ?), ('raw_encoder_version', ?), ('schema_fingerprint', ?)`,
		schemaVersion11, rawEncoderVersion, schemaFingerprintVersion11); err != nil {
		return fmt.Errorf("write canonical sqlite schema v11 metadata: %w", err)
	}
	for index := len(schemaVersion10Tables) - 1; index >= 0; index-- {
		table := schemaVersion10Tables[index]
		if _, err := tx.ExecContext(ctx, `DROP TABLE `+quoteSchemaName(legacySchemaVersion10Table(table))); err != nil {
			return fmt.Errorf("drop migrated sqlite schema v10 table %q: %w", table, err)
		}
	}
	return nil
}

func copyLegacyVersion10SharedColumns(ctx context.Context, q sqlRunner, table string) error {
	columns, err := sharedTableColumns(ctx, q, legacySchemaVersion10Table(table), table)
	if err != nil {
		return err
	}
	quoted := make([]string, len(columns))
	for index, column := range columns {
		quoted[index] = quoteSchemaName(column)
	}
	list := strings.Join(quoted, ", ")
	if _, err := q.ExecContext(ctx, `INSERT INTO `+quoteSchemaName(table)+` (`+list+`) SELECT `+list+` FROM `+quoteSchemaName(legacySchemaVersion10Table(table))); err != nil {
		return fmt.Errorf("copy sqlite schema v10 table %q: %w", table, err)
	}
	return nil
}

func legacySchemaVersion10Table(table string) string {
	return "legacy_v10_" + table
}

func putLegacySchemaMeta(ctx context.Context, tx sqlRunner, version, fingerprint string) error {
	for key, value := range map[string]string{"schema_version": version, "schema_fingerprint": fingerprint} {
		result, err := tx.ExecContext(ctx, `UPDATE schema_meta SET value = ? WHERE key = ?`, value, key)
		if err != nil {
			return fmt.Errorf("update sqlite store %s: %w", key, err)
		}
		updated, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if updated != 1 {
			return fmt.Errorf("update sqlite store %s changed %d rows", key, updated)
		}
	}
	return nil
}

func migrateLegacyThreadTitleColumns(ctx context.Context, q sqlRunner, version string) error {
	for _, column := range []struct {
		name string
		stmt string
	}{
		{name: "title", stmt: `ALTER TABLE threads ADD COLUMN title TEXT NOT NULL DEFAULT ''`},
		{name: "title_status", stmt: `ALTER TABLE threads ADD COLUMN title_status TEXT NOT NULL DEFAULT ''`},
		{name: "title_source", stmt: `ALTER TABLE threads ADD COLUMN title_source TEXT NOT NULL DEFAULT ''`},
		{name: "title_updated_at", stmt: `ALTER TABLE threads ADD COLUMN title_updated_at TEXT NOT NULL DEFAULT ''`},
		{name: "title_error", stmt: `ALTER TABLE threads ADD COLUMN title_error TEXT NOT NULL DEFAULT ''`},
	} {
		if err := addLegacyColumnIfMissing(ctx, q, "threads", column.name, column.stmt); err != nil {
			return fmt.Errorf("migrate sqlite store v%s thread title %s: %w", version, column.name, err)
		}
	}
	return nil
}

func migrateLegacyPromptScopeColumns(ctx context.Context, q sqlRunner) error {
	tables := []struct {
		name, oldIndex, newIndex, createStmt string
	}{
		{name: "prompt_segments", oldIndex: "prompt_segments_lookup_idx", newIndex: "prompt_segments_lookup_idx", createStmt: `CREATE INDEX prompt_segments_lookup_idx ON prompt_segments(prompt_scope_id, provider, model, rowid)`},
		{name: "prompt_toolsets", oldIndex: "prompt_toolsets_lookup_idx", newIndex: "prompt_toolsets_lookup_idx", createStmt: `CREATE INDEX prompt_toolsets_lookup_idx ON prompt_toolsets(prompt_scope_id, provider, model, rowid)`},
		{name: "prompt_requests", oldIndex: "prompt_requests_run_idx", newIndex: "prompt_requests_scope_idx", createStmt: `CREATE INDEX prompt_requests_scope_idx ON prompt_requests(prompt_scope_id, rowid)`},
		{name: "prompt_responses", oldIndex: "prompt_responses_run_idx", newIndex: "prompt_responses_scope_idx", createStmt: `CREATE INDEX prompt_responses_scope_idx ON prompt_responses(prompt_scope_id, rowid)`},
	}
	for _, table := range tables {
		hasScope, err := legacyColumnExists(ctx, q, table.name, "prompt_scope_id")
		if err != nil {
			return err
		}
		if !hasScope {
			if _, err := q.ExecContext(ctx, `ALTER TABLE `+quoteSchemaName(table.name)+` RENAME COLUMN run_id TO prompt_scope_id`); err != nil {
				return err
			}
		}
		for _, index := range []string{table.oldIndex, table.newIndex} {
			if _, err := q.ExecContext(ctx, `DROP INDEX IF EXISTS `+quoteSchemaName(index)); err != nil {
				return err
			}
		}
		if _, err := q.ExecContext(ctx, table.createStmt); err != nil {
			return err
		}
	}
	return nil
}

func addLegacyColumnIfMissing(ctx context.Context, q sqlRunner, table, column, stmt string) error {
	ok, err := legacyColumnExists(ctx, q, table, column)
	if err != nil || ok {
		return err
	}
	_, err = q.ExecContext(ctx, stmt)
	return err
}

func legacyColumnExists(ctx context.Context, q sqlRunner, table, column string) (bool, error) {
	rows, err := q.QueryContext(ctx, `PRAGMA table_info(`+quoteSchemaName(table)+`)`)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, columnType string
		var defaultValue sql.NullString
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			return false, err
		}
		if name == column {
			return true, nil
		}
	}
	return false, rows.Err()
}

const legacyForkOperationsSQL = `
CREATE TABLE fork_operations (
	operation_id TEXT PRIMARY KEY,
	request_fingerprint TEXT NOT NULL,
	state TEXT NOT NULL,
	plan_json TEXT NOT NULL,
	result_json TEXT NOT NULL DEFAULT '',
	error_code TEXT NOT NULL DEFAULT '',
	error_message TEXT NOT NULL DEFAULT '',
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL,
	finished_at TEXT NOT NULL DEFAULT ''
);
CREATE INDEX fork_operations_state_updated_idx ON fork_operations(state, updated_at);
`

const legacyProviderStatesSQL = `
CREATE TABLE provider_states (
	thread_id TEXT PRIMARY KEY,
	leaf_entry_id TEXT NOT NULL,
	compatibility_key TEXT NOT NULL,
	state_json TEXT NOT NULL,
	created_by_run_id TEXT NOT NULL DEFAULT '',
	created_by_turn_id TEXT NOT NULL DEFAULT '',
	updated_at TEXT NOT NULL,
	FOREIGN KEY (thread_id) REFERENCES threads(id) ON DELETE CASCADE
);
`

const legacyToolOutputArtifactsSQL = `
CREATE TABLE tool_output_artifacts (
	rowid INTEGER PRIMARY KEY AUTOINCREMENT,
	id TEXT NOT NULL UNIQUE,
	run_id TEXT NOT NULL DEFAULT '',
	thread_id TEXT NOT NULL,
	turn_id TEXT NOT NULL DEFAULT '',
	prompt_scope_id TEXT NOT NULL DEFAULT '',
	step INTEGER NOT NULL DEFAULT 0,
	call_id TEXT NOT NULL DEFAULT '',
	tool_name TEXT NOT NULL DEFAULT '',
	kind TEXT NOT NULL,
	mime TEXT NOT NULL,
	safe_label TEXT NOT NULL,
	url TEXT NOT NULL,
	size_bytes INTEGER NOT NULL DEFAULT 0,
	sha256 TEXT NOT NULL,
	text TEXT NOT NULL,
	metadata_json TEXT NOT NULL DEFAULT '{}',
	created_at TEXT NOT NULL,
	FOREIGN KEY (thread_id) REFERENCES threads(id) ON DELETE CASCADE
);
CREATE INDEX tool_output_artifacts_thread_idx ON tool_output_artifacts(thread_id, rowid);
`

const legacyAgentTodoStatesSQL = `
CREATE TABLE agent_todo_states (
	thread_id TEXT PRIMARY KEY,
	version INTEGER NOT NULL,
	items_json TEXT NOT NULL,
	updated_at TEXT NOT NULL,
	updated_by_turn_id TEXT NOT NULL DEFAULT '',
	updated_by_run_id TEXT NOT NULL DEFAULT '',
	updated_by_tool_call_id TEXT NOT NULL DEFAULT '',
	FOREIGN KEY (thread_id) REFERENCES threads(id) ON DELETE CASCADE
);
`

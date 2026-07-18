package sqlite

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/floegence/floret/internal/storage"
)

const (
	schemaVersion              = "13"
	schemaVersion12            = "12"
	schemaVersion11            = "11"
	minimumSchemaVersion       = "11"
	schemaFingerprintVersion11 = "200ee3cee291df623ac3cc7d603121cb22601992b4851e13fb859081053f4c85"
	schemaFingerprintVersion12 = "2586cafa8e761a8ed2e6d1227e6eb1a3f706332c590a8e7d1e6045f185520446"
)

func ensureSchema(ctx context.Context, tx sqlRunner) error {
	hasSchema, err := hasUserSchema(ctx, tx)
	if err != nil {
		return err
	}
	hasMeta, err := schemaTableExists(ctx, tx, "schema_meta")
	if err != nil {
		return err
	}
	if !hasMeta {
		if hasSchema {
			return errors.New("unsupported sqlite store without canonical schema metadata")
		}
		if _, err := tx.ExecContext(ctx, schemaSQL); err != nil {
			return fmt.Errorf("create sqlite store schema: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_meta(key, value) VALUES
			('schema_version', ?), ('raw_encoder_version', ?), ('schema_fingerprint', ?)`, schemaVersion, rawEncoderVersion, canonicalSchemaFingerprint()); err != nil {
			return fmt.Errorf("write sqlite store schema metadata: %w", err)
		}
		return verifySchemaVersion(ctx, tx, schemaVersion)
	}

	current, err := metaValue(ctx, tx, "schema_version")
	if errors.Is(err, storage.ErrMetadataNotFound) {
		return errors.New("unsupported sqlite store without canonical schema version")
	}
	if err != nil {
		return err
	}
	if err := migrateSchema(ctx, tx, current); err != nil {
		return err
	}
	return verifySchemaVersion(ctx, tx, schemaVersion)
}

func hasUserSchema(ctx context.Context, q sqlRunner) (bool, error) {
	var count int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

func schemaTableExists(ctx context.Context, q sqlRunner, tableName string) (bool, error) {
	var count int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?`, tableName).Scan(&count); err != nil {
		return false, err
	}
	return count == 1, nil
}

func canonicalSchemaFingerprint() string {
	sum := sha256.Sum256([]byte(schemaSQL))
	return hex.EncodeToString(sum[:])
}

const schemaMetaSQL = `
CREATE TABLE schema_meta (
	key TEXT PRIMARY KEY,
	value TEXT NOT NULL
);
`

const forkOperationsSQL = `
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

const schemaSQL = schemaMetaSQL + `

CREATE TABLE threads (
	id TEXT PRIMARY KEY,
	leaf_id TEXT NOT NULL DEFAULT '',
	parent_thread_id TEXT NOT NULL DEFAULT '',
	parent_turn_id TEXT NOT NULL DEFAULT '',
	forked_from_thread_id TEXT NOT NULL DEFAULT '',
	forked_from_entry_id TEXT NOT NULL DEFAULT '',
	fork_operation_id TEXT NOT NULL DEFAULT '',
	fork_operation_node_id TEXT NOT NULL DEFAULT '',
	task_name TEXT NOT NULL DEFAULT '',
	task_description TEXT NOT NULL DEFAULT '',
	agent_path TEXT NOT NULL DEFAULT '',
	host_profile_ref TEXT NOT NULL DEFAULT '',
	fork_mode TEXT NOT NULL DEFAULT '',
	closed INTEGER NOT NULL DEFAULT 0,
	archived INTEGER NOT NULL DEFAULT 0,
	title TEXT NOT NULL DEFAULT '',
	title_status TEXT NOT NULL DEFAULT '',
	title_source TEXT NOT NULL DEFAULT '',
	title_updated_at TEXT NOT NULL DEFAULT '',
	title_error TEXT NOT NULL DEFAULT '',
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL,
	status TEXT NOT NULL DEFAULT '',
	last_viewed_at TEXT NOT NULL DEFAULT ''
);

` + forkOperationsSQL + `

CREATE TABLE entries (
	thread_id TEXT NOT NULL,
	id TEXT NOT NULL,
	ordinal INTEGER NOT NULL,
	parent_id TEXT NOT NULL DEFAULT '',
	type TEXT NOT NULL,
	turn_id TEXT NOT NULL DEFAULT '',
	created_at TEXT NOT NULL,
	message_json TEXT NOT NULL DEFAULT '{}',
	raw TEXT NOT NULL DEFAULT '',
	raw_hash TEXT NOT NULL DEFAULT '',
	raw_encoder_version INTEGER NOT NULL DEFAULT 1,
	turn_status TEXT NOT NULL DEFAULT '',
	provider TEXT NOT NULL DEFAULT '',
	model TEXT NOT NULL DEFAULT '',
	compaction_id TEXT NOT NULL DEFAULT '',
	previous_compaction_id TEXT NOT NULL DEFAULT '',
	compacted_through_entry_id TEXT NOT NULL DEFAULT '',
	summary_schema_version TEXT NOT NULL DEFAULT '',
	compaction_generation INTEGER NOT NULL DEFAULT 0,
	compaction_window_id TEXT NOT NULL DEFAULT '',
	first_kept_entry_id TEXT NOT NULL DEFAULT '',
	kept_user_entry_ids_json TEXT NOT NULL DEFAULT '[]',
	summary TEXT NOT NULL DEFAULT '',
	compaction_trigger TEXT NOT NULL DEFAULT '',
	compaction_reason TEXT NOT NULL DEFAULT '',
	compaction_phase TEXT NOT NULL DEFAULT '',
	compaction_operation_id TEXT NOT NULL DEFAULT '',
	compaction_request_id TEXT NOT NULL DEFAULT '',
	compaction_source TEXT NOT NULL DEFAULT '',
	tokens_before INTEGER NOT NULL DEFAULT 0,
	tokens_after_estimate INTEGER NOT NULL DEFAULT 0,
	context_usage_before_json TEXT NOT NULL DEFAULT '{}',
	context_usage_after_json TEXT NOT NULL DEFAULT '{}',
	error TEXT NOT NULL DEFAULT '',
	metadata_json TEXT NOT NULL DEFAULT '{}',
	PRIMARY KEY (thread_id, id),
	UNIQUE (thread_id, ordinal),
	FOREIGN KEY (thread_id) REFERENCES threads(id) ON DELETE CASCADE
);

CREATE INDEX entries_parent_idx ON entries(thread_id, parent_id);
CREATE INDEX entries_thread_ordinal_idx ON entries(thread_id, ordinal);
CREATE INDEX threads_updated_at_idx ON threads(updated_at);

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

` + activeTurnLeasesSQL + `

CREATE TABLE prompt_segments (
	rowid INTEGER PRIMARY KEY AUTOINCREMENT,
	id TEXT NOT NULL,
	prompt_scope_id TEXT NOT NULL,
	provider TEXT NOT NULL,
	model TEXT NOT NULL,
	sequence INTEGER NOT NULL DEFAULT 0,
	created_at TEXT NOT NULL,
	data_json TEXT NOT NULL
);

CREATE INDEX prompt_segments_lookup_idx ON prompt_segments(prompt_scope_id, provider, model, rowid);

CREATE TABLE prompt_toolsets (
	rowid INTEGER PRIMARY KEY AUTOINCREMENT,
	id TEXT NOT NULL,
	prompt_scope_id TEXT NOT NULL,
	provider TEXT NOT NULL,
	model TEXT NOT NULL,
	epoch INTEGER NOT NULL DEFAULT 0,
	created_at TEXT NOT NULL,
	data_json TEXT NOT NULL
);

CREATE INDEX prompt_toolsets_lookup_idx ON prompt_toolsets(prompt_scope_id, provider, model, rowid);

CREATE TABLE prompt_requests (
	rowid INTEGER PRIMARY KEY AUTOINCREMENT,
	id TEXT NOT NULL,
	prompt_scope_id TEXT NOT NULL,
	provider TEXT NOT NULL,
	model TEXT NOT NULL,
	created_at TEXT NOT NULL,
	data_json TEXT NOT NULL
);

CREATE INDEX prompt_requests_scope_idx ON prompt_requests(prompt_scope_id, rowid);

CREATE TABLE prompt_responses (
	rowid INTEGER PRIMARY KEY AUTOINCREMENT,
	request_id TEXT NOT NULL,
	prompt_scope_id TEXT NOT NULL,
	created_at TEXT NOT NULL,
	data_json TEXT NOT NULL
);

CREATE INDEX prompt_responses_scope_idx ON prompt_responses(prompt_scope_id, rowid);

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

CREATE TABLE metadata_records (
	namespace TEXT NOT NULL,
	id TEXT NOT NULL,
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL,
	data_json TEXT NOT NULL,
	PRIMARY KEY(namespace, id)
);

CREATE INDEX metadata_records_namespace_updated_idx ON metadata_records(namespace, updated_at, id);
`

const activeTurnLeasesSQL = `
CREATE TABLE active_turn_leases (
	thread_id TEXT PRIMARY KEY,
	turn_id TEXT NOT NULL DEFAULT '',
	owner_id TEXT NOT NULL DEFAULT '',
	created_at TEXT NOT NULL,
	FOREIGN KEY (thread_id) REFERENCES threads(id) ON DELETE CASCADE
);
`

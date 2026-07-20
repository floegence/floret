package sqlite

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/floegence/floret/internal/sessiontree"
	"github.com/floegence/floret/internal/storage"
)

const (
	schemaVersion              = "15"
	schemaVersion14            = "14"
	schemaVersion13            = "13"
	schemaFingerprintVersion13 = "d911a003f57c2e60123ae392164109ab42b20eefa9fdb2111f28d220b8c2e5cb"
	schemaFingerprintVersion14 = "37d856aba09718aab51e3da7ea1e5d66a0b51e6f39d252ad2433caec9e333a07"
	schemaFingerprintVersion15 = "edbfebf6c00fd69b2034b60db905ea6756304299e94903d1868460f314c583ae"
)

func ensureSchema(ctx context.Context, tx sqlRunner, leasePolicy sessiontree.LeasePolicy) error {
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
			return unsupportedSchemaError("", "")
		}
		if _, err := tx.ExecContext(ctx, schemaSQL); err != nil {
			return fmt.Errorf("create sqlite store schema: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_meta(key, value) VALUES
			('schema_version', ?), ('raw_encoder_version', ?), ('schema_fingerprint', ?)`, schemaVersion, rawEncoderVersion, canonicalSchemaFingerprint()); err != nil {
			return fmt.Errorf("write sqlite store schema metadata: %w", err)
		}
		if err := persistLeasePolicy(ctx, tx, leasePolicy); err != nil {
			return err
		}
		return verifySchemaVersion(ctx, tx, schemaVersion)
	}

	current, err := metaValue(ctx, tx, "schema_version")
	if err != nil && !errors.Is(err, storage.ErrMetadataNotFound) {
		return err
	}
	fingerprint, fingerprintErr := metaValue(ctx, tx, "schema_fingerprint")
	if fingerprintErr != nil && !errors.Is(fingerprintErr, storage.ErrMetadataNotFound) {
		return fingerprintErr
	}
	if err != nil || fingerprintErr != nil {
		return unsupportedSchemaError(current, fingerprint)
	}
	switch current {
	case schemaVersion:
		if fingerprint != schemaFingerprintVersion15 {
			return unsupportedSchemaError(current, fingerprint)
		}
		if err := verifySchemaVersion(ctx, tx, schemaVersion); err != nil {
			return err
		}
		return verifyLeasePolicy(ctx, tx, leasePolicy)
	case schemaVersion14:
		if fingerprint != schemaFingerprintVersion14 {
			return unsupportedSchemaError(current, fingerprint)
		}
		if err := migrateSchemaVersion14(ctx, tx); err != nil {
			return err
		}
		if err := verifySchemaVersion(ctx, tx, schemaVersion); err != nil {
			return err
		}
		return verifyLeasePolicy(ctx, tx, leasePolicy)
	case schemaVersion13:
		if fingerprint != schemaFingerprintVersion13 {
			return unsupportedSchemaError(current, fingerprint)
		}
		if err := migrateEmptySchemaVersion13(ctx, tx, leasePolicy); err != nil {
			return err
		}
		return verifySchemaVersion(ctx, tx, schemaVersion)
	default:
		return unsupportedSchemaError(current, fingerprint)
	}
}

func unsupportedSchemaError(version, fingerprint string) error {
	return &storage.UnsupportedStoreSchemaError{
		ObservedVersion:        version,
		ObservedFingerprint:    fingerprint,
		CurrentVersion:         schemaVersion,
		CurrentFingerprint:     schemaFingerprintVersion15,
		PredecessorVersion:     schemaVersion14,
		PredecessorFingerprint: schemaFingerprintVersion14,
	}
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
	return schemaFingerprintVersion15
}

func computedCanonicalSchemaFingerprint() string {
	sum := sha256.Sum256([]byte(schemaSQL))
	return hex.EncodeToString(sum[:])
}

const schemaMetaSQL = `
CREATE TABLE schema_meta (
	key TEXT PRIMARY KEY,
	value TEXT NOT NULL
);
`

const authorityLeasePolicySQL = `
CREATE TABLE authority_lease_policy (
	singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
	lease_ttl_ns INTEGER NOT NULL CHECK (lease_ttl_ns > 0),
	renew_interval_ns INTEGER NOT NULL CHECK (renew_interval_ns > 0),
	clock_skew_allowance_ns INTEGER NOT NULL CHECK (clock_skew_allowance_ns >= 0),
	CHECK (renew_interval_ns <= lease_ttl_ns / 3)
);
`

const threadLeaseGenerationColumnSQL = `
	lease_generation INTEGER NOT NULL DEFAULT 0,
`

const threadLifecycleColumnSQL = `
	lifecycle TEXT NOT NULL DEFAULT 'open' CHECK (lifecycle IN ('open', 'closing', 'closed')),
`

const threadLifecycleColumnSQLVersion13 = `
	closed INTEGER NOT NULL DEFAULT 0,
`

const threadCloseOperationColumnSQL = `
	close_operation_id TEXT NOT NULL DEFAULT '',
`

const threadAuthorityChecksSQL = `
	CHECK (lease_generation >= 0),
	CHECK ((lifecycle = 'closing' AND parent_thread_id <> '' AND close_operation_id <> '') OR
		(lifecycle IN ('open', 'closed') AND close_operation_id = ''))
`

const canonicalTurnIndexSQL = `
CREATE INDEX entries_turn_ordinal_idx ON entries(thread_id, turn_id, ordinal);
CREATE UNIQUE INDEX entries_started_turn_unique_idx ON entries(thread_id, turn_id)
	WHERE type = 'turn_marker' AND turn_status = 'started';
`

const turnAuthoritySQL = `
CREATE TABLE turn_admissions (
	thread_id TEXT NOT NULL,
	turn_id TEXT NOT NULL,
	run_id TEXT NOT NULL,
	request_fingerprint TEXT NOT NULL,
	owner_id TEXT NOT NULL,
	generation INTEGER NOT NULL CHECK (generation > 0),
	heartbeat INTEGER NOT NULL CHECK (heartbeat >= 0),
	acquired_at TEXT NOT NULL,
	renewed_at TEXT NOT NULL,
	expires_at TEXT NOT NULL,
	boundary_terminal_id TEXT,
	turn_started_id TEXT NOT NULL,
	user_message_id TEXT,
	base_leaf_id TEXT NOT NULL DEFAULT '',
	PRIMARY KEY (thread_id, turn_id),
	FOREIGN KEY (thread_id) REFERENCES threads(id) ON DELETE CASCADE,
	FOREIGN KEY (thread_id, turn_started_id) REFERENCES entries(thread_id, id) ON DELETE CASCADE,
	FOREIGN KEY (thread_id, boundary_terminal_id) REFERENCES entries(thread_id, id) ON DELETE CASCADE,
	FOREIGN KEY (thread_id, user_message_id) REFERENCES entries(thread_id, id) ON DELETE CASCADE
);

CREATE INDEX turn_admissions_run_idx ON turn_admissions(thread_id, run_id);

CREATE TABLE turn_finishes (
	thread_id TEXT NOT NULL,
	turn_id TEXT NOT NULL,
	run_id TEXT NOT NULL,
	generation INTEGER NOT NULL CHECK (generation > 0),
	outcome_fingerprint TEXT NOT NULL,
	failure_entry_id TEXT NOT NULL DEFAULT '',
	terminal_entry_id TEXT NOT NULL,
	finished_at TEXT NOT NULL,
	PRIMARY KEY (thread_id, turn_id),
	FOREIGN KEY (thread_id) REFERENCES threads(id) ON DELETE CASCADE,
	FOREIGN KEY (thread_id, terminal_entry_id) REFERENCES entries(thread_id, id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX turn_finishes_terminal_idx ON turn_finishes(thread_id, terminal_entry_id);
`

const pendingToolCompletionAuthoritySQL = `
CREATE TABLE pending_tool_completions (
	completion_request_id TEXT PRIMARY KEY,
	request_fingerprint TEXT NOT NULL,
	thread_id TEXT NOT NULL,
	target_turn_id TEXT NOT NULL,
	target_run_id TEXT NOT NULL,
		target_tool_call_id TEXT NOT NULL,
		target_tool_name TEXT NOT NULL,
		target_handle TEXT NOT NULL,
		target_effect_attempt_id TEXT NOT NULL,
		settlement_fingerprint TEXT NOT NULL,
	settlement_entry_id TEXT NOT NULL,
	continuation_turn_id TEXT NOT NULL,
	continuation_run_id TEXT NOT NULL,
	turn_started_id TEXT NOT NULL,
	user_message_id TEXT NOT NULL,
	base_leaf_id TEXT NOT NULL,
	created_at TEXT NOT NULL,
	UNIQUE (thread_id, continuation_turn_id),
	FOREIGN KEY (thread_id) REFERENCES threads(id) ON DELETE CASCADE,
	FOREIGN KEY (thread_id, settlement_entry_id) REFERENCES entries(thread_id, id) ON DELETE CASCADE,
	FOREIGN KEY (thread_id, turn_started_id) REFERENCES entries(thread_id, id) ON DELETE CASCADE,
	FOREIGN KEY (thread_id, user_message_id) REFERENCES entries(thread_id, id) ON DELETE CASCADE
);

CREATE INDEX pending_tool_completions_target_idx ON pending_tool_completions(
	thread_id, target_turn_id, target_run_id, target_tool_call_id
);
`

const compactionAuthoritySQL = `
CREATE TABLE compaction_operations (
	request_id TEXT PRIMARY KEY,
	thread_id TEXT NOT NULL,
	request_fingerprint TEXT NOT NULL,
	source TEXT NOT NULL,
	source_leaf_id TEXT NOT NULL,
	active_path_hash TEXT NOT NULL,
	summary_schema_version TEXT NOT NULL,
	prompt_identity TEXT NOT NULL,
	request_payload_hash TEXT NOT NULL,
	state TEXT NOT NULL CHECK (state IN ('prepared', 'completed', 'failed')),
	lease_owner_id TEXT NOT NULL DEFAULT '',
	lease_generation INTEGER NOT NULL DEFAULT 0 CHECK (lease_generation >= 0),
	lease_heartbeat INTEGER NOT NULL DEFAULT 0 CHECK (lease_heartbeat >= 0),
	lease_acquired_at TEXT NOT NULL DEFAULT '',
	lease_renewed_at TEXT NOT NULL DEFAULT '',
	lease_expires_at TEXT NOT NULL DEFAULT '',
	result_entry_id TEXT,
	error_code TEXT NOT NULL DEFAULT '',
	error_message TEXT NOT NULL DEFAULT '',
	outcome_fingerprint TEXT NOT NULL DEFAULT '',
	finished_owner_id TEXT NOT NULL DEFAULT '',
	finished_generation INTEGER NOT NULL DEFAULT 0 CHECK (finished_generation >= 0),
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL,
	finished_at TEXT NOT NULL DEFAULT '',
	CHECK (
		(state = 'prepared' AND lease_owner_id <> '' AND lease_generation > 0 AND lease_acquired_at <> '' AND lease_renewed_at <> '' AND lease_expires_at <> '' AND
			result_entry_id IS NULL AND error_code = '' AND error_message = '' AND outcome_fingerprint = '' AND finished_owner_id = '' AND finished_generation = 0 AND finished_at = '') OR
		(state = 'completed' AND lease_owner_id = '' AND lease_generation = 0 AND lease_heartbeat = 0 AND lease_acquired_at = '' AND lease_renewed_at = '' AND lease_expires_at = '' AND
			result_entry_id IS NOT NULL AND error_code = '' AND error_message = '' AND outcome_fingerprint <> '' AND finished_owner_id <> '' AND finished_generation > 0 AND finished_at <> '') OR
		(state = 'failed' AND lease_owner_id = '' AND lease_generation = 0 AND lease_heartbeat = 0 AND lease_acquired_at = '' AND lease_renewed_at = '' AND lease_expires_at = '' AND
			result_entry_id IS NULL AND error_code <> '' AND error_message <> '' AND outcome_fingerprint <> '' AND finished_owner_id <> '' AND finished_generation > 0 AND finished_at <> '')
	),
	FOREIGN KEY (thread_id) REFERENCES threads(id) ON DELETE CASCADE,
	FOREIGN KEY (thread_id, result_entry_id) REFERENCES entries(thread_id, id) ON DELETE CASCADE
);

CREATE INDEX compaction_operations_thread_state_idx ON compaction_operations(thread_id, state, request_id);
`

const effectAuthoritySQL = `
CREATE TABLE effect_attempts (
	effect_attempt_id TEXT PRIMARY KEY,
	thread_id TEXT NOT NULL,
	turn_id TEXT NOT NULL,
	run_id TEXT NOT NULL,
	tool_call_id TEXT NOT NULL,
	tool_name TEXT NOT NULL,
	argument_hash TEXT NOT NULL,
	request_fingerprint TEXT NOT NULL,
	state TEXT NOT NULL CHECK (state IN ('prepared', 'dispatching', 'completed', 'failed', 'rejected', 'unknown', 'cancelled')),
	rejection_code TEXT NOT NULL DEFAULT '',
	terminal_fingerprint TEXT NOT NULL DEFAULT '',
	result_entry_id TEXT NOT NULL DEFAULT '',
	owner_id TEXT NOT NULL,
	generation INTEGER NOT NULL CHECK (generation > 0),
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL,
	UNIQUE (thread_id, turn_id, run_id, tool_call_id),
	CHECK ((state IN ('prepared', 'dispatching') AND terminal_fingerprint = '' AND result_entry_id = '' AND rejection_code = '') OR
		(state IN ('completed', 'failed') AND terminal_fingerprint <> '' AND result_entry_id <> '' AND rejection_code = '') OR
		(state = 'rejected' AND terminal_fingerprint <> '' AND result_entry_id = '' AND rejection_code <> '') OR
		(state IN ('unknown', 'cancelled') AND terminal_fingerprint <> '' AND result_entry_id = '' AND rejection_code = '')),
	FOREIGN KEY (thread_id) REFERENCES threads(id) ON DELETE CASCADE
);

CREATE INDEX effect_attempts_turn_idx ON effect_attempts(thread_id, turn_id, effect_attempt_id);
`

const subAgentInputsSQL = `
CREATE TABLE subagent_publications (
	publication_id TEXT PRIMARY KEY,
	parent_thread_id TEXT NOT NULL,
	child_thread_id TEXT NOT NULL UNIQUE,
	request_fingerprint TEXT NOT NULL,
	artifact_closure_json TEXT NOT NULL DEFAULT '{}',
	subagent_input_id TEXT NOT NULL,
	created_at TEXT NOT NULL
);

CREATE TABLE subagent_input_requests (
	input_request_id TEXT PRIMARY KEY,
	parent_thread_id TEXT NOT NULL,
	child_thread_id TEXT NOT NULL,
	request_fingerprint TEXT NOT NULL,
	subagent_input_id TEXT NOT NULL,
	created_at TEXT NOT NULL
);

CREATE TABLE subagent_pending_tool_completions (
	input_request_id TEXT PRIMARY KEY,
	request_fingerprint TEXT NOT NULL,
	settlement_fingerprint TEXT NOT NULL,
	parent_thread_id TEXT NOT NULL,
	child_thread_id TEXT NOT NULL,
	target_turn_id TEXT NOT NULL,
	target_run_id TEXT NOT NULL,
		target_tool_call_id TEXT NOT NULL,
		target_tool_name TEXT NOT NULL,
		target_handle TEXT NOT NULL,
		target_effect_attempt_id TEXT NOT NULL,
		settlement_entry_id TEXT NOT NULL,
	subagent_input_id TEXT NOT NULL,
	created_at TEXT NOT NULL
);

CREATE INDEX subagent_pending_tool_completions_child_idx ON subagent_pending_tool_completions(child_thread_id, input_request_id);

CREATE TABLE subagent_inputs (
	subagent_input_id TEXT PRIMARY KEY,
	parent_thread_id TEXT NOT NULL,
	child_thread_id TEXT NOT NULL,
	request_kind TEXT NOT NULL CHECK (request_kind IN ('publication', 'input', 'pending_tool_completion')),
	request_id TEXT NOT NULL,
	request_fingerprint TEXT NOT NULL,
	sequence INTEGER NOT NULL CHECK (sequence > 0),
	state TEXT NOT NULL CHECK (state IN ('pending', 'admitted', 'cancelled')),
	message_json TEXT NOT NULL,
	host_labels_json TEXT NOT NULL DEFAULT '{}',
	correlation_labels_json TEXT NOT NULL DEFAULT '{}',
	admitted_turn_id TEXT NOT NULL DEFAULT '',
	admitted_run_id TEXT NOT NULL DEFAULT '',
	created_at TEXT NOT NULL,
	admitted_at TEXT NOT NULL DEFAULT '',
	cancelled_at TEXT NOT NULL DEFAULT '',
	UNIQUE (child_thread_id, sequence),
	UNIQUE (request_kind, request_id),
	CHECK ((state = 'pending' AND admitted_turn_id = '' AND admitted_run_id = '' AND admitted_at = '' AND cancelled_at = '') OR
		(state = 'admitted' AND admitted_turn_id <> '' AND admitted_run_id <> '' AND admitted_at <> '' AND cancelled_at = '') OR
		(state = 'cancelled' AND admitted_turn_id = '' AND admitted_run_id = '' AND admitted_at = '' AND cancelled_at <> '')),
	FOREIGN KEY (child_thread_id) REFERENCES threads(id) ON DELETE CASCADE
);

CREATE INDEX subagent_inputs_pending_idx ON subagent_inputs(child_thread_id, state, sequence, subagent_input_id);
`

const subAgentCloseAuthoritySQL = `
CREATE TABLE subagent_close_operations (
	close_operation_id TEXT PRIMARY KEY,
	parent_thread_id TEXT NOT NULL,
	target_thread_id TEXT NOT NULL,
	reason TEXT NOT NULL,
	intent_fingerprint TEXT NOT NULL,
	request_fingerprint TEXT NOT NULL,
	state TEXT NOT NULL CHECK (state IN ('prepared', 'completed')),
	nodes_json TEXT NOT NULL,
	result_entry_ids_json TEXT NOT NULL DEFAULT '[]',
	prepared_at TEXT NOT NULL,
	finished_at TEXT NOT NULL DEFAULT '',
	CHECK ((state = 'prepared' AND finished_at = '' AND result_entry_ids_json = '[]') OR
		(state = 'completed' AND finished_at <> ''))
);

CREATE INDEX subagent_close_operations_target_idx ON subagent_close_operations(parent_thread_id, target_thread_id, state);
`

const rootAuthoritySQL = `
CREATE TABLE root_create_intents (
	create_intent_id TEXT PRIMARY KEY,
	thread_id TEXT NOT NULL UNIQUE,
	contract_version TEXT NOT NULL,
	request_fingerprint TEXT NOT NULL,
	created_at TEXT NOT NULL
);

CREATE TABLE thread_tombstones (
	thread_id TEXT PRIMARY KEY,
	root_thread_id TEXT NOT NULL,
	parent_thread_id TEXT NOT NULL DEFAULT '',
	create_intent_id TEXT NOT NULL DEFAULT '',
	fork_operation_id TEXT NOT NULL DEFAULT '',
	fork_operation_node_id TEXT NOT NULL DEFAULT '',
	forked_from_thread_id TEXT NOT NULL DEFAULT '',
	forked_from_entry_id TEXT NOT NULL DEFAULT '',
	deleted_at TEXT NOT NULL
);

CREATE INDEX thread_tombstones_root_idx ON thread_tombstones(root_thread_id, thread_id);
`

const forkOperationsSQLVersion13 = `
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
	finished_at TEXT NOT NULL DEFAULT '',
	source_thread_ids_json TEXT NOT NULL DEFAULT '[]',
	authority_thread_ids_json TEXT NOT NULL DEFAULT '[]'
);

CREATE INDEX fork_operations_state_updated_idx ON fork_operations(state, updated_at);

CREATE TABLE thread_authority_claims (
	thread_id TEXT PRIMARY KEY,
	operation_id TEXT NOT NULL,
	created_at TEXT NOT NULL,
	FOREIGN KEY (operation_id) REFERENCES fork_operations(operation_id) ON DELETE CASCADE
);

CREATE INDEX thread_authority_claims_operation_idx ON thread_authority_claims(operation_id, thread_id);
`

const schemaSQL = schemaMetaSQL + authorityLeasePolicySQL + `

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
	` + threadLifecycleColumnSQL + `
	` + threadCloseOperationColumnSQL + `
	archived INTEGER NOT NULL DEFAULT 0,
	title TEXT NOT NULL DEFAULT '',
	title_status TEXT NOT NULL DEFAULT '',
	title_source TEXT NOT NULL DEFAULT '',
	title_updated_at TEXT NOT NULL DEFAULT '',
	title_error TEXT NOT NULL DEFAULT '',
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL,
	last_viewed_at TEXT NOT NULL DEFAULT '',
` + threadLeaseGenerationColumnSQL + `
` + threadAuthorityChecksSQL + `
);

` + forkOperationsSQL + rootAuthoritySQL + `

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
` + canonicalTurnIndexSQL + `
CREATE INDEX threads_updated_at_idx ON threads(updated_at);

` + turnAuthoritySQL + pendingToolCompletionAuthoritySQL + compactionAuthoritySQL + effectAuthoritySQL + `

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

	` + activeTurnLeasesSQL + subAgentInputsSQL + subAgentCloseAuthoritySQL + `

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
	id TEXT NOT NULL,
	run_id TEXT NOT NULL DEFAULT '',
	thread_id TEXT NOT NULL,
	turn_id TEXT NOT NULL DEFAULT '',
	prompt_scope_id TEXT NOT NULL DEFAULT '',
	step INTEGER NOT NULL DEFAULT 0,
	call_id TEXT NOT NULL DEFAULT '',
	tool_name TEXT NOT NULL DEFAULT '',
	effect_attempt_id TEXT,
	canonical_entry_id TEXT NOT NULL,
	kind TEXT NOT NULL,
	mime TEXT NOT NULL,
	safe_label TEXT NOT NULL,
	size_bytes INTEGER NOT NULL DEFAULT 0,
	sha256 TEXT NOT NULL,
	text TEXT NOT NULL,
	metadata_json TEXT NOT NULL DEFAULT '{}',
	created_at TEXT NOT NULL,
	UNIQUE (thread_id, id),
	UNIQUE (effect_attempt_id),
	FOREIGN KEY (thread_id) REFERENCES threads(id) ON DELETE CASCADE,
	FOREIGN KEY (thread_id, canonical_entry_id) REFERENCES entries(thread_id, id) ON DELETE CASCADE,
	FOREIGN KEY (effect_attempt_id) REFERENCES effect_attempts(effect_attempt_id) ON DELETE CASCADE
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
	purpose TEXT NOT NULL CHECK (purpose IN ('turn', 'mutation')),
	turn_id TEXT NOT NULL DEFAULT '',
	mutation_id TEXT NOT NULL DEFAULT '',
	mutation_kind TEXT NOT NULL DEFAULT '',
	owner_id TEXT NOT NULL,
	generation INTEGER NOT NULL CHECK (generation > 0),
	heartbeat INTEGER NOT NULL CHECK (heartbeat >= 0),
	acquired_at TEXT NOT NULL,
	renewed_at TEXT NOT NULL,
	expires_at TEXT NOT NULL,
	CHECK (
		(purpose = 'turn' AND turn_id <> '' AND mutation_id = '' AND mutation_kind = '') OR
		(purpose = 'mutation' AND turn_id = '' AND mutation_id <> '' AND mutation_kind <> '')
	),
	FOREIGN KEY (thread_id) REFERENCES threads(id) ON DELETE CASCADE
);
`

const activeTurnLeasesSQLVersion13 = `
CREATE TABLE active_turn_leases (
	thread_id TEXT PRIMARY KEY,
	turn_id TEXT NOT NULL DEFAULT '',
	owner_id TEXT NOT NULL DEFAULT '',
	created_at TEXT NOT NULL,
	FOREIGN KEY (thread_id) REFERENCES threads(id) ON DELETE CASCADE
);
`

var schemaVersion14SQL = strings.Replace(schemaSQL, canonicalTurnIndexSQL, "", 1)

var schemaVersion13SQL = strings.NewReplacer(
	"\tid TEXT NOT NULL,\n\trun_id TEXT NOT NULL DEFAULT '',\n\tthread_id TEXT NOT NULL,\n", "\tid TEXT NOT NULL UNIQUE,\n\trun_id TEXT NOT NULL DEFAULT '',\n\tthread_id TEXT NOT NULL,\n",
	"\teffect_attempt_id TEXT,\n\tcanonical_entry_id TEXT NOT NULL,\n", "",
	"\tsafe_label TEXT NOT NULL,\n\tsize_bytes INTEGER NOT NULL DEFAULT 0,\n", "\tsafe_label TEXT NOT NULL,\n\turl TEXT NOT NULL,\n\tsize_bytes INTEGER NOT NULL DEFAULT 0,\n",
	"\tcreated_at TEXT NOT NULL,\n\tUNIQUE (thread_id, id),\n\tUNIQUE (effect_attempt_id),\n\tFOREIGN KEY (thread_id) REFERENCES threads(id) ON DELETE CASCADE,\n\tFOREIGN KEY (thread_id, canonical_entry_id) REFERENCES entries(thread_id, id) ON DELETE CASCADE,\n\tFOREIGN KEY (effect_attempt_id) REFERENCES effect_attempts(effect_attempt_id) ON DELETE CASCADE\n);\n\nCREATE INDEX tool_output_artifacts_thread_idx",
	"\tcreated_at TEXT NOT NULL,\n\tFOREIGN KEY (thread_id) REFERENCES threads(id) ON DELETE CASCADE\n);\n\nCREATE INDEX tool_output_artifacts_thread_idx",
	authorityLeasePolicySQL, "",
	turnAuthoritySQL, "",
	pendingToolCompletionAuthoritySQL, "",
	compactionAuthoritySQL, "",
	effectAuthoritySQL, "",
	subAgentInputsSQL, "",
	subAgentCloseAuthoritySQL, "",
	rootAuthoritySQL, "",
	threadLifecycleColumnSQL, threadLifecycleColumnSQLVersion13,
	threadCloseOperationColumnSQL, "",
	threadLeaseGenerationColumnSQL, "",
	threadAuthorityChecksSQL, "",
	"\tlast_viewed_at TEXT NOT NULL DEFAULT '',\n", "\tlast_viewed_at TEXT NOT NULL DEFAULT ''\n",
	forkOperationsSQL, forkOperationsSQLVersion13,
	activeTurnLeasesSQL, activeTurnLeasesSQLVersion13,
).Replace(schemaVersion14SQL)

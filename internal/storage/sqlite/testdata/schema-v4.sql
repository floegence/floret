
CREATE TABLE IF NOT EXISTS schema_meta (
	key TEXT PRIMARY KEY,
	value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS threads (
	id TEXT PRIMARY KEY,
	leaf_id TEXT NOT NULL DEFAULT '',
	parent_thread_id TEXT NOT NULL DEFAULT '',
	forked_from_thread_id TEXT NOT NULL DEFAULT '',
	forked_from_entry_id TEXT NOT NULL DEFAULT '',
	archived INTEGER NOT NULL DEFAULT 0,
	title TEXT NOT NULL DEFAULT '',
	title_status TEXT NOT NULL DEFAULT '',
	title_source TEXT NOT NULL DEFAULT '',
	title_updated_at TEXT NOT NULL DEFAULT '',
	title_error TEXT NOT NULL DEFAULT '',
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS entries (
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

CREATE INDEX IF NOT EXISTS entries_parent_idx ON entries(thread_id, parent_id);
CREATE INDEX IF NOT EXISTS entries_thread_ordinal_idx ON entries(thread_id, ordinal);
CREATE INDEX IF NOT EXISTS threads_updated_at_idx ON threads(updated_at);


CREATE TABLE IF NOT EXISTS active_turn_leases (
	thread_id TEXT PRIMARY KEY,
	turn_id TEXT NOT NULL DEFAULT '',
	owner_id TEXT NOT NULL DEFAULT '',
	created_at TEXT NOT NULL,
	FOREIGN KEY (thread_id) REFERENCES threads(id) ON DELETE CASCADE
);


CREATE TABLE IF NOT EXISTS prompt_segments (
	rowid INTEGER PRIMARY KEY AUTOINCREMENT,
	id TEXT NOT NULL,
	run_id TEXT NOT NULL,
	provider TEXT NOT NULL,
	model TEXT NOT NULL,
	sequence INTEGER NOT NULL DEFAULT 0,
	created_at TEXT NOT NULL,
	data_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS prompt_segments_lookup_idx ON prompt_segments(run_id, provider, model, rowid);

CREATE TABLE IF NOT EXISTS prompt_toolsets (
	rowid INTEGER PRIMARY KEY AUTOINCREMENT,
	id TEXT NOT NULL,
	run_id TEXT NOT NULL,
	provider TEXT NOT NULL,
	model TEXT NOT NULL,
	epoch INTEGER NOT NULL DEFAULT 0,
	created_at TEXT NOT NULL,
	data_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS prompt_toolsets_lookup_idx ON prompt_toolsets(run_id, provider, model, rowid);

CREATE TABLE IF NOT EXISTS prompt_requests (
	rowid INTEGER PRIMARY KEY AUTOINCREMENT,
	id TEXT NOT NULL,
	run_id TEXT NOT NULL,
	provider TEXT NOT NULL,
	model TEXT NOT NULL,
	created_at TEXT NOT NULL,
	data_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS prompt_requests_run_idx ON prompt_requests(run_id, rowid);

CREATE TABLE IF NOT EXISTS prompt_responses (
	rowid INTEGER PRIMARY KEY AUTOINCREMENT,
	request_id TEXT NOT NULL,
	run_id TEXT NOT NULL,
	created_at TEXT NOT NULL,
	data_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS prompt_responses_run_idx ON prompt_responses(run_id, rowid);

CREATE TABLE IF NOT EXISTS metadata_records (
	namespace TEXT NOT NULL,
	id TEXT NOT NULL,
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL,
	data_json TEXT NOT NULL,
	PRIMARY KEY(namespace, id)
);

CREATE INDEX IF NOT EXISTS metadata_records_namespace_updated_idx ON metadata_records(namespace, updated_at, id);

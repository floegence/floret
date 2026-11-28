CREATE TABLE floret_thread_header (
  singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
  store BLOB NOT NULL CHECK (length(store) = 16),
  file BLOB NOT NULL CHECK (length(file) = 16),
  thread TEXT NOT NULL CHECK (length(thread) > 0),
  format INTEGER NOT NULL CHECK (format = 2),
  leaf_seq INTEGER REFERENCES history_events(seq),
  parent_checked_seq INTEGER REFERENCES history_events(seq),
  commit_rev INTEGER NOT NULL DEFAULT 0 CHECK (commit_rev >= 0),
  read_gen INTEGER NOT NULL DEFAULT 1 CHECK (read_gen > 0),
  read_leaf_seq INTEGER REFERENCES history_events(seq),
  read_version INTEGER NOT NULL CHECK (read_version > 0),
  read_rev INTEGER NOT NULL DEFAULT 0 CHECK (read_rev BETWEEN 0 AND commit_rev)
) STRICT;
CREATE TABLE floret_thread_preparation (
  singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
  fingerprint BLOB NOT NULL CHECK (length(fingerprint) = 32)
) STRICT;
CREATE TABLE history_events (
  seq INTEGER PRIMARY KEY,
  event_id TEXT NOT NULL UNIQUE,
  entry_no INTEGER CHECK (entry_no >= 0),
  path_depth INTEGER CHECK (path_depth > 0),
  model_input INTEGER NOT NULL DEFAULT 0 CHECK (model_input IN (0,1)),
  commit_rev INTEGER NOT NULL CHECK (commit_rev > 0),
  kind TEXT NOT NULL, schema_ver INTEGER NOT NULL CHECK (schema_ver > 0),
  at_ns INTEGER, turn_id TEXT, run_id TEXT,
  turn_marker TEXT, content_source TEXT, reasoning_source TEXT,
  recovery_group TEXT CHECK(recovery_group IN ('queue','parent','host_work')),
  parent_seq INTEGER REFERENCES history_events(seq) CHECK (parent_seq < seq),
  subject_seq INTEGER REFERENCES history_events(seq),
  object_seq INTEGER REFERENCES history_events(seq),
  scope_key TEXT, request_key TEXT,
  prompt_scope TEXT, prompt_no INTEGER CHECK (prompt_no > 0),
  attempt_id TEXT, ordinal INTEGER CHECK (ordinal >= 0), state TEXT,
  work_head INTEGER NOT NULL DEFAULT 0 CHECK (work_head IN (0,1)),
  model_prev_seq INTEGER REFERENCES history_events(seq) CHECK (model_prev_seq < seq),
  payload BLOB NOT NULL CHECK (length(payload) <= 16384),
  CHECK ((entry_no IS NULL) = (path_depth IS NULL)),
  CHECK (model_input = 0 OR entry_no IS NOT NULL),
  CHECK ((scope_key IS NULL) = (request_key IS NULL)),
  CHECK ((prompt_scope IS NULL) = (prompt_no IS NULL)),
  CHECK (at_ns IS NOT NULL OR prompt_no IS NOT NULL),
  CHECK ((prompt_no IS NOT NULL) = (kind IN ('provider_segment','provider_tools','provider_request','provider_response') AND entry_no IS NULL)),
  CHECK ((attempt_id IS NULL) = (ordinal IS NULL)),
  CHECK ((attempt_id IS NULL) = (state IS NULL)),
  CHECK (state IS NULL OR state IN ('prepared','dispatching','completed','failed','rejected','unknown','cancelled')),
  CHECK (work_head = 0 OR state IS NULL OR state IN ('prepared','dispatching'))
) STRICT;
CREATE INDEX history_commit ON history_events(commit_rev, seq);
CREATE INDEX history_todo ON history_events(seq) WHERE kind='todo_updated';
CREATE UNIQUE INDEX history_continuation ON history_events(kind) WHERE kind='provider_continuation';
CREATE UNIQUE INDEX history_entry_order ON history_events(entry_no) WHERE entry_no IS NOT NULL;
CREATE INDEX history_parent ON history_events(parent_seq, seq) WHERE parent_seq IS NOT NULL;
CREATE INDEX history_turn ON history_events(turn_id, seq) WHERE turn_id IS NOT NULL;
CREATE INDEX history_content_source ON history_events(content_source,entry_no) WHERE content_source IS NOT NULL;
CREATE INDEX history_reasoning_source ON history_events(reasoning_source,entry_no) WHERE reasoning_source IS NOT NULL;
CREATE UNIQUE INDEX history_active_turn ON history_events(turn_id) WHERE kind='turn_marker' AND work_head=1;
CREATE INDEX history_recovery_group ON history_events(recovery_group,entry_no) WHERE recovery_group IS NOT NULL;
CREATE INDEX history_subject ON history_events(subject_seq, kind, seq) WHERE subject_seq IS NOT NULL;
CREATE UNIQUE INDEX history_request ON history_events(scope_key, request_key) WHERE request_key IS NOT NULL;
CREATE UNIQUE INDEX history_prompt_order ON history_events(prompt_scope,kind,prompt_no) WHERE prompt_no IS NOT NULL;
CREATE INDEX history_prompt_context ON history_events(prompt_scope,coalesce(json_extract(CAST(payload AS TEXT),'$.lookup.context_entry'),''),prompt_no) WHERE kind='provider_request';
CREATE INDEX history_prompt_request ON history_events(prompt_scope,coalesce(json_extract(CAST(payload AS TEXT),'$.lookup.request'),''),prompt_no) WHERE kind='provider_request';
CREATE INDEX history_prompt_turn ON history_events(prompt_scope,turn_id,prompt_no) WHERE kind='provider_request';
CREATE INDEX history_prompt_surface ON history_events(prompt_scope,coalesce(json_extract(CAST(payload AS TEXT),'$.lookup.surface_turn'),''),prompt_no) WHERE kind='provider_request';
CREATE INDEX history_prompt_toolset ON history_events(prompt_scope,coalesce(json_extract(CAST(payload AS TEXT),'$.lookup.provider'),''),coalesce(json_extract(CAST(payload AS TEXT),'$.lookup.model'),''),prompt_no) WHERE kind='provider_tools';
CREATE INDEX history_prompt_pressure ON history_events(prompt_scope,prompt_no) WHERE kind='provider_response' AND json_extract(CAST(payload AS TEXT),'$.lookup.pressure')=1;
CREATE UNIQUE INDEX history_attempt ON history_events(attempt_id, ordinal) WHERE attempt_id IS NOT NULL;
CREATE UNIQUE INDEX history_open_attempt ON history_events(attempt_id) WHERE work_head = 1 AND attempt_id IS NOT NULL;
CREATE INDEX history_pending ON history_events(kind, seq) WHERE work_head = 1;
CREATE INDEX history_boundary ON history_events(object_seq, commit_rev, seq) WHERE kind = 'output_boundary';
CREATE TABLE content_blocks (
  object_seq INTEGER NOT NULL REFERENCES history_events(seq),
  part INTEGER NOT NULL CHECK (part IN (0,1)),
  block_no INTEGER NOT NULL CHECK (block_no >= 0),
  start_byte INTEGER NOT NULL CHECK (start_byte >= 0),
  bytes BLOB NOT NULL CHECK (length(bytes) BETWEEN 1 AND 65536),
  sha256 BLOB NOT NULL CHECK (length(sha256) = 32),
  PRIMARY KEY (object_seq, part, block_no),
  UNIQUE (object_seq, part, start_byte)
) STRICT, WITHOUT ROWID;
CREATE TABLE read_items (
  generation INTEGER NOT NULL CHECK (generation > 0),
  view_kind TEXT NOT NULL CHECK (view_kind IN ('window','message','tool')),
  item_id TEXT NOT NULL, position INTEGER NOT NULL CHECK (position >= 0),
  first_seq INTEGER NOT NULL REFERENCES history_events(seq),
  last_seq INTEGER NOT NULL REFERENCES history_events(seq) CHECK (last_seq >= first_seq),
  source_rev INTEGER NOT NULL CHECK (source_rev > 0),
  turn_id TEXT, run_id TEXT, role TEXT, tool_name TEXT, tool_kind TEXT, status TEXT,
  at_ns INTEGER NOT NULL, completed_ns INTEGER, elapsed_ns INTEGER CHECK (elapsed_ns >= 0),
  fold_flags INTEGER NOT NULL CHECK (fold_flags >= 0),
  preview TEXT NOT NULL CHECK (length(CAST(preview AS BLOB)) <= 4096),
  summary BLOB NOT NULL CHECK (length(summary) <= 8192),
  PRIMARY KEY (generation, view_kind, item_id),
  UNIQUE (generation, view_kind, position)
) STRICT, WITHOUT ROWID;
CREATE INDEX read_time ON read_items(generation, view_kind, at_ns, item_id);
CREATE INDEX read_turn ON read_items(generation, view_kind, turn_id, position);
CREATE INDEX read_unfolded ON read_items(generation, view_kind, position) WHERE fold_flags = 0;
CREATE INDEX read_tool_name ON read_items(generation, tool_name, item_id) WHERE view_kind = 'tool';
CREATE INDEX read_completed ON read_items(generation, completed_ns, item_id) WHERE view_kind = 'tool';
CREATE INDEX read_elapsed ON read_items(generation, elapsed_ns, item_id) WHERE view_kind = 'tool';

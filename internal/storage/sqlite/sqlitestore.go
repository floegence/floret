package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/floegence/floret/internal/provider/cache"
	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/session/artifact"
	"github.com/floegence/floret/internal/sessiontree"
	"github.com/floegence/floret/internal/storage"
	_ "modernc.org/sqlite"
)

const (
	schemaVersion     = "9"
	rawEncoderVersion = "1"
	driverName        = "sqlite"
)

type Option func(*options)

type options struct{}

type Store struct {
	db   *sql.DB
	path string
}

type sqlRunner interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type rowScanner interface {
	Scan(...any) error
}

func DefaultTestUIPath(root string) string {
	return filepath.Join(root, ".floret-test-ui", "floret.db")
}

func Open(path string, opts ...Option) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("sqlite store path is required")
	}
	for _, opt := range opts {
		opt(&options{})
	}
	if path != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return nil, err
		}
	}
	db, err := sql.Open(driverName, path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	store := &Store{db: db, path: path}
	if err := store.init(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) DBPath() string {
	return s.path
}

func (s *Store) SchemaVersion(ctx context.Context) (string, error) {
	return s.metaValue(ctx, "schema_version")
}

func (s *Store) putMetaValue(ctx context.Context, key, value string) error {
	return s.withImmediate(ctx, func(tx sqlRunner) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO schema_meta(key, value) VALUES(?, ?)
			ON CONFLICT(key) DO UPDATE SET value = excluded.value`, key, value)
		return err
	})
}

func (s *Store) init(ctx context.Context) error {
	for _, pragma := range []string{
		"PRAGMA foreign_keys = ON",
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = FULL",
		"PRAGMA busy_timeout = 5000",
	} {
		if _, err := s.db.ExecContext(ctx, pragma); err != nil {
			return err
		}
	}
	if _, err := s.db.ExecContext(ctx, schemaMetaSQL); err != nil {
		return err
	}
	current, err := s.metaValue(ctx, "schema_version")
	if errors.Is(err, storage.ErrMetadataNotFound) {
		if _, err := s.db.ExecContext(ctx, schemaSQL); err != nil {
			return err
		}
		if _, err := s.db.ExecContext(ctx, `INSERT INTO schema_meta(key, value) VALUES
			('schema_version', ?), ('raw_encoder_version', ?)`, schemaVersion, rawEncoderVersion); err != nil {
			return err
		}
		return dropSubAgentPathIndex(ctx, s.db)
	}
	if err != nil {
		return err
	}
	if current != schemaVersion {
		if current != "3" && current != "4" && current != "5" && current != "6" && current != "7" && current != "8" {
			return fmt.Errorf("unsupported sqlite store schema version %q", current)
		}
		if err := s.migrate(ctx, current); err != nil {
			return err
		}
	}
	if _, err := s.db.ExecContext(ctx, schemaSQL); err != nil {
		return err
	}
	if err := dropSubAgentPathIndex(ctx, s.db); err != nil {
		return err
	}
	rawVersion, err := s.metaValue(ctx, "raw_encoder_version")
	if err != nil {
		return err
	}
	if rawVersion != rawEncoderVersion {
		return fmt.Errorf("unsupported sqlite store raw encoder version %q", rawVersion)
	}
	return nil
}

func (s *Store) migrate(ctx context.Context, current string) error {
	return s.withImmediate(ctx, func(tx sqlRunner) error {
		if current == "3" || current == "4" {
			if err := migrateThreadTitleColumns(ctx, tx, current); err != nil {
				return err
			}
			if err := migratePromptCacheScopeColumns(ctx, tx); err != nil {
				return fmt.Errorf("migrate v%s→v5 prompt cache scope columns: %w", current, err)
			}
			if current == "3" {
				if err := addColumnIfMissing(ctx, tx, "entries", "kept_user_entry_ids_json", `ALTER TABLE entries ADD COLUMN kept_user_entry_ids_json TEXT NOT NULL DEFAULT '[]'`); err != nil {
					return fmt.Errorf("migrate v3→v5 add kept user entry ids column: %w", err)
				}
			}
			current = "5"
		}
		if current == "5" {
			if err := addColumnIfMissing(ctx, tx, "threads", "status", `ALTER TABLE threads ADD COLUMN status TEXT NOT NULL DEFAULT ''`); err != nil {
				return fmt.Errorf("migrate v5→v6 add status column: %w", err)
			}
			if err := addColumnIfMissing(ctx, tx, "threads", "last_viewed_at", `ALTER TABLE threads ADD COLUMN last_viewed_at TEXT NOT NULL DEFAULT ''`); err != nil {
				return fmt.Errorf("migrate v5→v6 add last_viewed_at column: %w", err)
			}
			current = "6"
		}
		if current == "6" {
			for _, column := range []struct {
				name string
				stmt string
			}{
				{name: "parent_turn_id", stmt: `ALTER TABLE threads ADD COLUMN parent_turn_id TEXT NOT NULL DEFAULT ''`},
				{name: "task_name", stmt: `ALTER TABLE threads ADD COLUMN task_name TEXT NOT NULL DEFAULT ''`},
				{name: "agent_path", stmt: `ALTER TABLE threads ADD COLUMN agent_path TEXT NOT NULL DEFAULT ''`},
				{name: "host_profile_ref", stmt: `ALTER TABLE threads ADD COLUMN host_profile_ref TEXT NOT NULL DEFAULT ''`},
				{name: "closed", stmt: `ALTER TABLE threads ADD COLUMN closed INTEGER NOT NULL DEFAULT 0`},
			} {
				if err := addColumnIfMissing(ctx, tx, "threads", column.name, column.stmt); err != nil {
					return fmt.Errorf("migrate v6→v7 add %s column: %w", column.name, err)
				}
			}
			if err := dropSubAgentPathIndex(ctx, tx); err != nil {
				return fmt.Errorf("migrate v6→v7 drop subagent path index: %w", err)
			}
			current = "7"
		}
		if current == "7" {
			if err := addColumnIfMissing(ctx, tx, "threads", "fork_mode", `ALTER TABLE threads ADD COLUMN fork_mode TEXT NOT NULL DEFAULT ''`); err != nil {
				return fmt.Errorf("migrate v7→v8 add fork_mode column: %w", err)
			}
			current = "8"
		}
		if current == "8" {
			if err := addColumnIfMissing(ctx, tx, "threads", "task_description", `ALTER TABLE threads ADD COLUMN task_description TEXT NOT NULL DEFAULT ''`); err != nil {
				return fmt.Errorf("migrate v8→v9 add task_description column: %w", err)
			}
			if _, err := tx.ExecContext(ctx, `UPDATE schema_meta SET value = ? WHERE key = 'schema_version'`, schemaVersion); err != nil {
				return fmt.Errorf("migrate v8→v9 update schema_version: %w", err)
			}
			return nil
		}
		return fmt.Errorf("unsupported sqlite store schema version %q", current)
	})
}

func migrateThreadTitleColumns(ctx context.Context, q sqlRunner, current string) error {
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
		if err := addColumnIfMissing(ctx, q, "threads", column.name, column.stmt); err != nil {
			return fmt.Errorf("migrate v%s→v5 add %s column: %w", current, column.name, err)
		}
	}
	return nil
}

func migratePromptCacheScopeColumns(ctx context.Context, q sqlRunner) error {
	tables := []struct {
		name       string
		oldIndex   string
		newIndex   string
		createStmt string
	}{
		{
			name:       "prompt_segments",
			oldIndex:   "prompt_segments_lookup_idx",
			newIndex:   "prompt_segments_lookup_idx",
			createStmt: `CREATE INDEX IF NOT EXISTS prompt_segments_lookup_idx ON prompt_segments(prompt_scope_id, provider, model, rowid)`,
		},
		{
			name:       "prompt_toolsets",
			oldIndex:   "prompt_toolsets_lookup_idx",
			newIndex:   "prompt_toolsets_lookup_idx",
			createStmt: `CREATE INDEX IF NOT EXISTS prompt_toolsets_lookup_idx ON prompt_toolsets(prompt_scope_id, provider, model, rowid)`,
		},
		{
			name:       "prompt_requests",
			oldIndex:   "prompt_requests_run_idx",
			newIndex:   "prompt_requests_scope_idx",
			createStmt: `CREATE INDEX IF NOT EXISTS prompt_requests_scope_idx ON prompt_requests(prompt_scope_id, rowid)`,
		},
		{
			name:       "prompt_responses",
			oldIndex:   "prompt_responses_run_idx",
			newIndex:   "prompt_responses_scope_idx",
			createStmt: `CREATE INDEX IF NOT EXISTS prompt_responses_scope_idx ON prompt_responses(prompt_scope_id, rowid)`,
		},
	}
	for _, table := range tables {
		hasScope, err := columnExists(ctx, q, table.name, "prompt_scope_id")
		if err != nil {
			return err
		}
		if !hasScope {
			hasRunID, err := columnExists(ctx, q, table.name, "run_id")
			if err != nil {
				return err
			}
			if !hasRunID {
				return fmt.Errorf("%s has neither prompt_scope_id nor run_id", table.name)
			}
			if _, err := q.ExecContext(ctx, `ALTER TABLE `+table.name+` RENAME COLUMN run_id TO prompt_scope_id`); err != nil {
				return err
			}
		}
		for _, index := range []string{table.oldIndex, table.newIndex} {
			if _, err := q.ExecContext(ctx, `DROP INDEX IF EXISTS `+index); err != nil {
				return err
			}
		}
		if _, err := q.ExecContext(ctx, table.createStmt); err != nil {
			return err
		}
	}
	return nil
}

func addColumnIfMissing(ctx context.Context, q sqlRunner, table, column, stmt string) error {
	ok, err := columnExists(ctx, q, table, column)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}
	_, err = q.ExecContext(ctx, stmt)
	return err
}

func dropSubAgentPathIndex(ctx context.Context, q sqlRunner) error {
	_, err := q.ExecContext(ctx, `DROP INDEX IF EXISTS threads_subagent_path_unique`)
	return err
}

func columnExists(ctx context.Context, q sqlRunner, table, column string) (bool, error) {
	rows, err := q.QueryContext(ctx, `PRAGMA table_info(`+table+`)`)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var cid, notNull, pk int
		var name, typ string
		var defaultValue sql.NullString
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			return false, err
		}
		if name == column {
			return true, nil
		}
	}
	return false, rows.Err()
}

func (s *Store) metaValue(ctx context.Context, key string) (string, error) {
	var value string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM schema_meta WHERE key = ?`, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", storage.ErrMetadataNotFound
	}
	return value, err
}

func (s *Store) withImmediate(ctx context.Context, fn func(sqlRunner) error) error {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	for _, pragma := range []string{
		"PRAGMA foreign_keys = ON",
		"PRAGMA busy_timeout = 5000",
	} {
		if _, err := conn.ExecContext(ctx, pragma); err != nil {
			return err
		}
	}
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		}
	}()
	if err := fn(conn); err != nil {
		return err
	}
	if _, err := conn.ExecContext(context.Background(), "COMMIT"); err != nil {
		return err
	}
	committed = true
	return nil
}

func (s *Store) CreateThread(ctx context.Context, meta sessiontree.ThreadMeta) (sessiontree.ThreadMeta, error) {
	now := meta.CreatedAt
	if now.IsZero() {
		now = time.Now()
	}
	err := s.withImmediate(ctx, func(tx sqlRunner) error {
		if meta.ID == "" {
			for {
				meta.ID = "thread-" + strconv.FormatInt(time.Now().UnixNano(), 10)
				ok, err := threadExists(ctx, tx, meta.ID)
				if err != nil {
					return err
				}
				if !ok {
					break
				}
			}
		} else if ok, err := threadExists(ctx, tx, meta.ID); err != nil {
			return err
		} else if ok {
			return sessiontree.ErrThreadExists
		}
		meta.CreatedAt = now
		meta.UpdatedAt = now
		return insertThread(ctx, tx, meta)
	})
	return meta, err
}

func (s *Store) AcquireTurnLease(ctx context.Context, lease sessiontree.TurnLease) error {
	if strings.TrimSpace(lease.ThreadID) == "" {
		return sessiontree.ErrThreadNotFound
	}
	if lease.CreatedAt.IsZero() {
		lease.CreatedAt = time.Now()
	}
	return s.withImmediate(ctx, func(tx sqlRunner) error {
		if ok, err := threadExists(ctx, tx, lease.ThreadID); err != nil {
			return err
		} else if !ok {
			return sessiontree.ErrThreadNotFound
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO active_turn_leases(thread_id, turn_id, owner_id, created_at)
			VALUES(?, ?, ?, ?)`, lease.ThreadID, lease.TurnID, lease.OwnerID, formatTime(lease.CreatedAt))
		if isConstraintError(err) {
			return sessiontree.ErrActiveTurn
		}
		return err
	})
}

func (s *Store) ReleaseTurnLease(ctx context.Context, lease sessiontree.TurnLease) error {
	return s.withImmediate(ctx, func(tx sqlRunner) error {
		_, err := tx.ExecContext(ctx, `DELETE FROM active_turn_leases
			WHERE thread_id = ? AND turn_id = ? AND owner_id = ?`, lease.ThreadID, lease.TurnID, lease.OwnerID)
		return err
	})
}

func (s *Store) ActiveTurnLease(ctx context.Context, threadID string) (sessiontree.TurnLease, bool, error) {
	if ok, err := threadExists(ctx, s.db, threadID); err != nil {
		return sessiontree.TurnLease{}, false, err
	} else if !ok {
		return sessiontree.TurnLease{}, false, sessiontree.ErrThreadNotFound
	}
	return loadTurnLease(ctx, s.db, threadID)
}

func (s *Store) ClearExpiredTurnLease(ctx context.Context, threadID string, cutoff time.Time) (sessiontree.TurnLease, bool, error) {
	if cutoff.IsZero() {
		return sessiontree.TurnLease{}, false, nil
	}
	var cleared sessiontree.TurnLease
	ok := false
	err := s.withImmediate(ctx, func(tx sqlRunner) error {
		if exists, err := threadExists(ctx, tx, threadID); err != nil {
			return err
		} else if !exists {
			return sessiontree.ErrThreadNotFound
		}
		lease, found, err := loadTurnLease(ctx, tx, threadID)
		if err != nil || !found {
			return err
		}
		if lease.CreatedAt.IsZero() || !lease.CreatedAt.Before(cutoff) {
			return nil
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM active_turn_leases WHERE thread_id = ?`, threadID); err != nil {
			return err
		}
		cleared = lease
		ok = true
		return nil
	})
	return cleared, ok, err
}

func (s *Store) Thread(ctx context.Context, threadID string) (sessiontree.ThreadMeta, error) {
	return loadThread(ctx, s.db, threadID)
}

func (s *Store) ListThreads(ctx context.Context, opts sessiontree.ListThreadsOptions) ([]sessiontree.ThreadMeta, error) {
	where := []string{}
	args := []any{}
	if !opts.IncludeArchived {
		where = append(where, "archived = 0")
	}
	query := `SELECT
			id, leaf_id, parent_thread_id, parent_turn_id, forked_from_thread_id, forked_from_entry_id,
			task_name, task_description, agent_path, host_profile_ref, fork_mode, closed, archived,
			title, title_status, title_source, title_updated_at, title_error,
			created_at, updated_at, status, last_viewed_at
			FROM threads`
	if len(where) > 0 {
		query += ` WHERE ` + strings.Join(where, ` AND `)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]sessiontree.ThreadMeta, 0)
	for rows.Next() {
		meta, err := scanThreadMeta(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, meta)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return sessiontree.ApplyThreadListOptions(out, opts), nil
}

func (s *Store) UpdateThread(ctx context.Context, meta sessiontree.ThreadMeta) error {
	if meta.UpdatedAt.IsZero() {
		meta.UpdatedAt = time.Now()
	}
	return s.withImmediate(ctx, func(tx sqlRunner) error {
		var exists int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM threads WHERE id = ?`, meta.ID).Scan(&exists); err != nil {
			return err
		}
		if exists == 0 {
			return sessiontree.ErrThreadNotFound
		}
		return updateThread(ctx, tx, meta)
	})
}

func (s *Store) DeleteThread(ctx context.Context, threadID string) error {
	return s.withImmediate(ctx, func(tx sqlRunner) error {
		var exists int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM threads WHERE id = ?`, threadID).Scan(&exists); err != nil {
			return err
		}
		if exists == 0 {
			return sessiontree.ErrThreadNotFound
		}
		_, err := tx.ExecContext(ctx, `DELETE FROM threads WHERE id = ?`, threadID)
		return err
	})
}

func (s *Store) Append(ctx context.Context, entry sessiontree.Entry, opts sessiontree.AppendOptions) (sessiontree.Entry, error) {
	var saved sessiontree.Entry
	err := s.withImmediate(ctx, func(tx sqlRunner) error {
		meta, err := loadThread(ctx, tx, entry.ThreadID)
		if err != nil {
			return err
		}
		if opts.ParentID != "" {
			entry.ParentID = opts.ParentID
		} else if entry.ParentID == "" {
			entry.ParentID = meta.LeafID
		}
		if entry.ParentID != "" {
			ok, err := entryExists(ctx, tx, entry.ThreadID, entry.ParentID)
			if err != nil {
				return err
			}
			if !ok {
				return sessiontree.ErrInvalidParent
			}
		}
		if opts.ID != "" {
			entry.ID = opts.ID
		}
		if entry.ID == "" {
			id, err := nextEntryID(ctx, tx, entry.ThreadID)
			if err != nil {
				return err
			}
			entry.ID = id
		} else if ok, err := entryExists(ctx, tx, entry.ThreadID, entry.ID); err != nil {
			return err
		} else if ok {
			return fmt.Errorf("session tree entry id already exists: %s", entry.ID)
		}
		if entry.CreatedAt.IsZero() {
			entry.CreatedAt = opts.Now
		}
		if entry.CreatedAt.IsZero() {
			entry.CreatedAt = time.Now()
		}
		entry = sessiontree.PrepareEntry(entry)
		ordinal, err := nextOrdinal(ctx, tx, entry.ThreadID)
		if err != nil {
			return err
		}
		if err := insertEntry(ctx, tx, entry, ordinal, false); err != nil {
			return err
		}
		meta.LeafID = entry.ID
		meta.UpdatedAt = entry.CreatedAt
		if err := updateThread(ctx, tx, meta); err != nil {
			return err
		}
		saved = cloneEntry(entry)
		return nil
	})
	return saved, err
}

func (s *Store) Entry(ctx context.Context, threadID, entryID string) (sessiontree.Entry, error) {
	entry, err := loadEntry(ctx, s.db, threadID, entryID)
	if errors.Is(err, sql.ErrNoRows) {
		return sessiontree.Entry{}, sessiontree.ErrEntryNotFound
	}
	return entry, err
}

func (s *Store) Entries(ctx context.Context, threadID string) ([]sessiontree.Entry, error) {
	if _, err := loadThread(ctx, s.db, threadID); err != nil {
		return nil, err
	}
	return loadEntries(ctx, s.db, threadID)
}

func (s *Store) Path(ctx context.Context, threadID, leafID string) ([]sessiontree.Entry, error) {
	meta, err := loadThread(ctx, s.db, threadID)
	if err != nil {
		return nil, err
	}
	if leafID == "" {
		leafID = meta.LeafID
	}
	if leafID == "" {
		return nil, nil
	}
	entries, err := loadEntries(ctx, s.db, threadID)
	if err != nil {
		return nil, err
	}
	byID := map[string]sessiontree.Entry{}
	for _, entry := range entries {
		byID[entry.ID] = entry
	}
	var rev []sessiontree.Entry
	seen := map[string]struct{}{}
	for id := leafID; id != ""; {
		if _, ok := seen[id]; ok {
			return nil, sessiontree.ErrInvalidParent
		}
		seen[id] = struct{}{}
		entry, ok := byID[id]
		if !ok {
			return nil, sessiontree.ErrEntryNotFound
		}
		rev = append(rev, cloneEntry(entry))
		id = entry.ParentID
	}
	slices.Reverse(rev)
	return rev, nil
}

func (s *Store) MoveLeaf(ctx context.Context, threadID, entryID string) error {
	return s.withImmediate(ctx, func(tx sqlRunner) error {
		meta, err := loadThread(ctx, tx, threadID)
		if err != nil {
			return err
		}
		if entryID != "" {
			ok, err := entryExists(ctx, tx, threadID, entryID)
			if err != nil {
				return err
			}
			if !ok {
				return sessiontree.ErrEntryNotFound
			}
		}
		meta.LeafID = entryID
		meta.UpdatedAt = time.Now()
		return updateThread(ctx, tx, meta)
	})
}

func (s *Store) Fork(ctx context.Context, opts sessiontree.ForkOptions) (sessiontree.ThreadMeta, error) {
	var forked sessiontree.ThreadMeta
	err := s.withImmediate(ctx, func(tx sqlRunner) error {
		if opts.Position == "" {
			opts.Position = sessiontree.ForkAt
		}
		sourceMeta, err := loadThread(ctx, tx, opts.SourceThreadID)
		if err != nil {
			return err
		}
		targetID := opts.EntryID
		if targetID == "" {
			targetID = sourceMeta.LeafID
		}
		if opts.Position == sessiontree.ForkBefore {
			entry, err := loadEntry(ctx, tx, opts.SourceThreadID, targetID)
			if errors.Is(err, sql.ErrNoRows) {
				return sessiontree.ErrEntryNotFound
			}
			if err != nil {
				return err
			}
			targetID = entry.ParentID
		}
		path, err := pathWithRunner(ctx, tx, opts.SourceThreadID, targetID)
		if err != nil {
			return err
		}
		now := opts.Now
		if now.IsZero() {
			now = time.Now()
		}
		newID := opts.NewThreadID
		if newID == "" {
			newID, err = nextForkID(ctx, tx, opts.SourceThreadID)
			if err != nil {
				return err
			}
		} else if ok, err := threadExists(ctx, tx, newID); err != nil {
			return err
		} else if ok {
			return sessiontree.ErrThreadExists
		}
		meta := sessiontree.ThreadMeta{ID: newID, ParentThreadID: opts.SourceThreadID, ForkedFromThreadID: opts.SourceThreadID, ForkedFromEntryID: targetID, CreatedAt: now, UpdatedAt: now}
		if err := insertThread(ctx, tx, meta); err != nil {
			return err
		}
		oldToNew := map[string]string{"": ""}
		for _, entry := range path {
			next := cloneEntry(entry)
			nextID, err := nextEntryID(ctx, tx, newID)
			if err != nil {
				return err
			}
			next.ID = nextID
			next.ThreadID = newID
			next.ParentID = oldToNew[entry.ParentID]
			next.TurnID = rewriteForkID(next.TurnID, opts.TurnIDMap)
			next.FirstKeptEntryID = oldToNew[entry.FirstKeptEntryID]
			next.CompactedThroughEntryID = oldToNew[entry.CompactedThroughEntryID]
			next.KeptUserEntryIDs = rewriteEntryIDs(entry.KeptUserEntryIDs, oldToNew)
			next.Metadata = rewriteForkMetadata(next.Metadata, oldToNew, opts.TurnIDMap, opts.RunIDMap)
			next.CreatedAt = now
			next = sessiontree.PrepareEntry(next)
			ordinal, err := nextOrdinal(ctx, tx, newID)
			if err != nil {
				return err
			}
			if err := insertEntry(ctx, tx, next, ordinal, false); err != nil {
				return err
			}
			oldToNew[entry.ID] = next.ID
			meta.LeafID = next.ID
		}
		if err := updateThread(ctx, tx, meta); err != nil {
			return err
		}
		forked = meta
		return nil
	})
	return forked, err
}

func (s *Store) AppendSegment(ctx context.Context, seg cache.Segment) error {
	return s.withImmediate(ctx, func(tx sqlRunner) error {
		return insertSegment(ctx, tx, seg)
	})
}

func (s *Store) Segments(ctx context.Context, promptScopeID, provider, model string) ([]cache.Segment, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT data_json FROM prompt_segments
		WHERE prompt_scope_id = ? AND provider = ? AND model = ? ORDER BY rowid`, promptScopeID, provider, model)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []cache.Segment
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var seg cache.Segment
		if err := json.Unmarshal([]byte(raw), &seg); err != nil {
			return nil, err
		}
		out = append(out, seg)
	}
	return out, rows.Err()
}

func (s *Store) AppendToolset(ctx context.Context, snap cache.ToolsetSnapshot) error {
	return s.withImmediate(ctx, func(tx sqlRunner) error {
		return insertToolset(ctx, tx, snap)
	})
}

func (s *Store) ActiveToolset(ctx context.Context, promptScopeID, provider, model string) (cache.ToolsetSnapshot, bool, error) {
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT data_json FROM prompt_toolsets
		WHERE prompt_scope_id = ? AND provider = ? AND model = ? ORDER BY rowid DESC LIMIT 1`, promptScopeID, provider, model).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return cache.ToolsetSnapshot{}, false, nil
	}
	if err != nil {
		return cache.ToolsetSnapshot{}, false, err
	}
	var snap cache.ToolsetSnapshot
	if err := json.Unmarshal([]byte(raw), &snap); err != nil {
		return cache.ToolsetSnapshot{}, false, err
	}
	return snap, true, nil
}

func (s *Store) AppendProviderRequest(ctx context.Context, req cache.ProviderRequestRecord) error {
	return s.withImmediate(ctx, func(tx sqlRunner) error {
		return insertProviderRequest(ctx, tx, req)
	})
}

func (s *Store) ProviderRequests(ctx context.Context, promptScopeID string) ([]cache.ProviderRequestRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT data_json FROM prompt_requests WHERE prompt_scope_id = ? ORDER BY rowid`, promptScopeID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []cache.ProviderRequestRecord
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var req cache.ProviderRequestRecord
		if err := json.Unmarshal([]byte(raw), &req); err != nil {
			return nil, err
		}
		out = append(out, req)
	}
	return out, rows.Err()
}

func (s *Store) AppendProviderResponse(ctx context.Context, resp cache.ProviderResponseRecord) error {
	return s.withImmediate(ctx, func(tx sqlRunner) error {
		return insertProviderResponse(ctx, tx, resp)
	})
}

func (s *Store) ProviderResponses(ctx context.Context, promptScopeID string) ([]cache.ProviderResponseRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT data_json FROM prompt_responses WHERE prompt_scope_id = ? ORDER BY rowid`, promptScopeID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []cache.ProviderResponseRecord
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var resp cache.ProviderResponseRecord
		if err := json.Unmarshal([]byte(raw), &resp); err != nil {
			return nil, err
		}
		out = append(out, resp)
	}
	return out, rows.Err()
}

func (s *Store) LatestPressureAnchor(ctx context.Context, promptScopeID, providerName, model string) (cache.PressureAnchorState, bool, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT data_json FROM prompt_responses ORDER BY rowid DESC`)
	if err != nil {
		return cache.PressureAnchorState{}, false, err
	}
	defer rows.Close()
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return cache.PressureAnchorState{}, false, err
		}
		var resp cache.ProviderResponseRecord
		if err := json.Unmarshal([]byte(raw), &resp); err != nil {
			return cache.PressureAnchorState{}, false, err
		}
		anchor := resp.PressureAnchor
		if anchor.WindowInputTokens <= 0 {
			continue
		}
		if promptScopeID != "" && anchor.PromptScopeID != promptScopeID {
			continue
		}
		if providerName != "" && anchor.Provider != providerName {
			continue
		}
		if model != "" && anchor.Model != model {
			continue
		}
		return anchor, true, nil
	}
	return cache.PressureAnchorState{}, false, rows.Err()
}

func (s *Store) DeletePromptScopes(ctx context.Context, promptScopeIDs ...string) error {
	clean := cleanIDs(promptScopeIDs)
	if len(clean) == 0 {
		return nil
	}
	return s.withImmediate(ctx, func(tx sqlRunner) error {
		for _, promptScopeID := range clean {
			for _, table := range []string{"prompt_segments", "prompt_toolsets", "prompt_requests", "prompt_responses"} {
				if _, err := tx.ExecContext(ctx, `DELETE FROM `+table+` WHERE prompt_scope_id = ?`, promptScopeID); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (s *Store) PutMetadata(ctx context.Context, rec storage.MetadataRecord) error {
	if rec.Namespace == "" || rec.ID == "" {
		return errors.New("metadata namespace and id are required")
	}
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = time.Now()
	}
	if rec.UpdatedAt.IsZero() {
		rec.UpdatedAt = rec.CreatedAt
	}
	data := strings.TrimSpace(string(rec.Data))
	if data == "" {
		data = "{}"
	}
	return s.withImmediate(ctx, func(tx sqlRunner) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO metadata_records(namespace, id, created_at, updated_at, data_json)
			VALUES(?, ?, ?, ?, ?)
			ON CONFLICT(namespace, id) DO UPDATE SET
				created_at = excluded.created_at,
				updated_at = excluded.updated_at,
				data_json = excluded.data_json`, rec.Namespace, rec.ID, formatTime(rec.CreatedAt), formatTime(rec.UpdatedAt), data)
		return err
	})
}

func (s *Store) Metadata(ctx context.Context, namespace, id string) (storage.MetadataRecord, error) {
	var rec storage.MetadataRecord
	var created, updated, raw string
	err := s.db.QueryRowContext(ctx, `SELECT namespace, id, created_at, updated_at, data_json
		FROM metadata_records WHERE namespace = ? AND id = ?`, namespace, id).Scan(&rec.Namespace, &rec.ID, &created, &updated, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.MetadataRecord{}, storage.ErrMetadataNotFound
	}
	if err != nil {
		return storage.MetadataRecord{}, err
	}
	rec.CreatedAt = parseTime(created)
	rec.UpdatedAt = parseTime(updated)
	rec.Data = json.RawMessage(raw)
	return rec, nil
}

func (s *Store) ListMetadata(ctx context.Context, namespace string) ([]storage.MetadataRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT namespace, id, created_at, updated_at, data_json
		FROM metadata_records WHERE namespace = ? ORDER BY updated_at DESC, id DESC`, namespace)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []storage.MetadataRecord
	for rows.Next() {
		var rec storage.MetadataRecord
		var created, updated, raw string
		if err := rows.Scan(&rec.Namespace, &rec.ID, &created, &updated, &raw); err != nil {
			return nil, err
		}
		rec.CreatedAt = parseTime(created)
		rec.UpdatedAt = parseTime(updated)
		rec.Data = json.RawMessage(raw)
		out = append(out, rec)
	}
	return out, rows.Err()
}

func (s *Store) DeleteMetadata(ctx context.Context, namespace, id string) error {
	return s.withImmediate(ctx, func(tx sqlRunner) error {
		_, err := tx.ExecContext(ctx, `DELETE FROM metadata_records WHERE namespace = ? AND id = ?`, namespace, id)
		return err
	})
}

func (s *Store) DeleteThreadData(ctx context.Context, req storage.DeleteThreadDataRequest) error {
	threadID := strings.TrimSpace(req.ThreadID)
	if threadID == "" {
		return errors.New("thread id is required")
	}
	promptScopeIDs := cleanIDs(append([]string{threadID}, req.PromptScopeIDs...))
	return s.withImmediate(ctx, func(tx sqlRunner) error {
		ok, err := threadExists(ctx, tx, threadID)
		if err != nil {
			return err
		}
		if !ok {
			return sessiontree.ErrThreadNotFound
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM metadata_records WHERE id = ?`, threadID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM tool_output_artifacts WHERE thread_id = ?`, threadID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM threads WHERE id = ?`, threadID); err != nil {
			return err
		}
		for _, promptScopeID := range promptScopeIDs {
			for _, table := range []string{"prompt_segments", "prompt_toolsets", "prompt_requests", "prompt_responses"} {
				if _, err := tx.ExecContext(ctx, `DELETE FROM `+table+` WHERE prompt_scope_id = ?`, promptScopeID); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (s *Store) PutToolOutput(ctx context.Context, output artifact.ToolOutputArtifact) (artifact.Ref, error) {
	if s == nil {
		return artifact.Ref{}, errors.New("sqlite artifact store is nil")
	}
	threadID := strings.TrimSpace(output.ThreadID)
	if threadID == "" {
		return artifact.Ref{}, errors.New("thread id is required")
	}
	if output.MIME == "" {
		output.MIME = artifact.DefaultMIME
	}
	if output.Kind == "" {
		output.Kind = artifact.DefaultKind
	}
	metadata, err := json.Marshal(output.Metadata)
	if err != nil {
		return artifact.Ref{}, err
	}
	if string(metadata) == "null" {
		metadata = []byte("{}")
	}
	sum := sha256.Sum256([]byte(output.Text))
	hash := hex.EncodeToString(sum[:])
	now := time.Now()
	var ref artifact.Ref
	err = s.withImmediate(ctx, func(tx sqlRunner) error {
		if ok, err := threadExists(ctx, tx, threadID); err != nil {
			return err
		} else if !ok {
			return sessiontree.ErrThreadNotFound
		}
		var next int64
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(rowid), 0) + 1 FROM tool_output_artifacts`).Scan(&next); err != nil {
			return err
		}
		toolName := artifact.SafeLabel(output.ToolName, 32)
		ref = artifact.Ref{
			ID:        artifact.SafeLabel(fmt.Sprintf("%s-%06d-%s", toolName, next, hash[:12]), artifact.DefaultSafeLabelMaxChars),
			SafeLabel: artifact.SafeLabel(fmt.Sprintf("%s-output-%06d.log", output.ToolName, next), artifact.DefaultSafeLabelMaxChars),
			URL:       "/api/artifacts/" + artifact.SafeLabel(fmt.Sprintf("%s-%06d-%s", toolName, next, hash[:12]), artifact.DefaultSafeLabelMaxChars),
			Kind:      output.Kind,
			MIME:      output.MIME,
			SizeBytes: int64(len(output.Text)),
			SHA256:    hash,
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO tool_output_artifacts(
			id, run_id, thread_id, turn_id, prompt_scope_id, step, call_id, tool_name,
			kind, mime, safe_label, url, size_bytes, sha256, text, metadata_json, created_at
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			ref.ID, output.RunID, threadID, output.TurnID, output.PromptScopeID, output.Step, output.CallID, output.ToolName,
			ref.Kind, ref.MIME, ref.SafeLabel, ref.URL, ref.SizeBytes, ref.SHA256, output.Text, string(metadata), formatTime(now))
		return err
	})
	if err != nil {
		return artifact.Ref{}, err
	}
	return ref, nil
}

func (s *Store) DeleteThreadArtifacts(ctx context.Context, threadID string) error {
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		return errors.New("thread id is required")
	}
	return s.withImmediate(ctx, func(tx sqlRunner) error {
		_, err := tx.ExecContext(ctx, `DELETE FROM tool_output_artifacts WHERE thread_id = ?`, threadID)
		return err
	})
}

func (s *Store) artifactText(ctx context.Context, id string) (string, bool, error) {
	var text string
	err := s.db.QueryRowContext(ctx, `SELECT text FROM tool_output_artifacts WHERE id = ?`, id).Scan(&text)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return text, err == nil, err
}

func insertThread(ctx context.Context, tx sqlRunner, meta sessiontree.ThreadMeta) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO threads(
		id, leaf_id, parent_thread_id, parent_turn_id, forked_from_thread_id, forked_from_entry_id,
		task_name, task_description, agent_path, host_profile_ref, fork_mode, closed, archived,
		title, title_status, title_source, title_updated_at, title_error,
		created_at, updated_at, status, last_viewed_at
	) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		meta.ID, meta.LeafID, meta.ParentThreadID, meta.ParentTurnID, meta.ForkedFromThreadID, meta.ForkedFromEntryID,
		meta.TaskName, meta.TaskDescription, meta.AgentPath, meta.HostProfileRef, meta.ForkMode, boolInt(meta.Closed), boolInt(meta.Archived),
		meta.Title, string(meta.TitleStatus), string(meta.TitleSource), formatTime(meta.TitleUpdatedAt), meta.TitleError,
		formatTime(meta.CreatedAt), formatTime(meta.UpdatedAt), meta.Status, formatTime(meta.LastViewedAt))
	return err
}

func loadTurnLease(ctx context.Context, q sqlRunner, threadID string) (sessiontree.TurnLease, bool, error) {
	var lease sessiontree.TurnLease
	var created string
	err := q.QueryRowContext(ctx, `SELECT thread_id, turn_id, owner_id, created_at
		FROM active_turn_leases WHERE thread_id = ?`, threadID).Scan(&lease.ThreadID, &lease.TurnID, &lease.OwnerID, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return sessiontree.TurnLease{}, false, nil
	}
	if err != nil {
		return sessiontree.TurnLease{}, false, err
	}
	lease.CreatedAt = parseTime(created)
	return lease, true, nil
}

func updateThread(ctx context.Context, tx sqlRunner, meta sessiontree.ThreadMeta) error {
	_, err := tx.ExecContext(ctx, `UPDATE threads SET
		leaf_id = ?, parent_thread_id = ?, parent_turn_id = ?, forked_from_thread_id = ?, forked_from_entry_id = ?,
		task_name = ?, task_description = ?, agent_path = ?, host_profile_ref = ?, fork_mode = ?, closed = ?, archived = ?,
		title = ?, title_status = ?, title_source = ?, title_updated_at = ?, title_error = ?,
		created_at = ?, updated_at = ?, status = ?, last_viewed_at = ?
		WHERE id = ?`,
		meta.LeafID, meta.ParentThreadID, meta.ParentTurnID, meta.ForkedFromThreadID, meta.ForkedFromEntryID,
		meta.TaskName, meta.TaskDescription, meta.AgentPath, meta.HostProfileRef, meta.ForkMode, boolInt(meta.Closed), boolInt(meta.Archived),
		meta.Title, string(meta.TitleStatus), string(meta.TitleSource), formatTime(meta.TitleUpdatedAt), meta.TitleError,
		formatTime(meta.CreatedAt), formatTime(meta.UpdatedAt), meta.Status, formatTime(meta.LastViewedAt), meta.ID)
	return err
}

func loadThread(ctx context.Context, q sqlRunner, threadID string) (sessiontree.ThreadMeta, error) {
	meta, err := scanThreadMeta(q.QueryRowContext(ctx, `SELECT
		id, leaf_id, parent_thread_id, parent_turn_id, forked_from_thread_id, forked_from_entry_id,
		task_name, task_description, agent_path, host_profile_ref, fork_mode, closed, archived,
		title, title_status, title_source, title_updated_at, title_error,
		created_at, updated_at, status, last_viewed_at
		FROM threads WHERE id = ?`, threadID))
	if errors.Is(err, sql.ErrNoRows) {
		return sessiontree.ThreadMeta{}, sessiontree.ErrThreadNotFound
	}
	return meta, err
}

func scanThreadMeta(scanner rowScanner) (sessiontree.ThreadMeta, error) {
	var meta sessiontree.ThreadMeta
	var archived, closed int
	var titleStatus, titleSource, titleUpdated, created, updated, status, lastViewed string
	err := scanner.Scan(
		&meta.ID, &meta.LeafID, &meta.ParentThreadID, &meta.ParentTurnID, &meta.ForkedFromThreadID, &meta.ForkedFromEntryID,
		&meta.TaskName, &meta.TaskDescription, &meta.AgentPath, &meta.HostProfileRef, &meta.ForkMode, &closed, &archived,
		&meta.Title, &titleStatus, &titleSource, &titleUpdated, &meta.TitleError,
		&created, &updated, &status, &lastViewed,
	)
	if err != nil {
		return sessiontree.ThreadMeta{}, err
	}
	meta.Closed = closed != 0
	meta.Archived = archived != 0
	meta.TitleStatus = sessiontree.ThreadTitleStatus(titleStatus)
	meta.TitleSource = sessiontree.ThreadTitleSource(titleSource)
	meta.TitleUpdatedAt = parseTime(titleUpdated)
	meta.CreatedAt = parseTime(created)
	meta.UpdatedAt = parseTime(updated)
	meta.Status = status
	meta.LastViewedAt = parseTime(lastViewed)
	return meta, nil
}

func insertEntry(ctx context.Context, tx sqlRunner, entry sessiontree.Entry, ordinal int64, preserveRaw bool) error {
	if !preserveRaw {
		entry = sessiontree.PrepareEntry(entry)
	}
	messageJSON, err := json.Marshal(entry.Message)
	if err != nil {
		return err
	}
	beforeJSON, err := json.Marshal(entry.ContextUsageBefore)
	if err != nil {
		return err
	}
	afterJSON, err := json.Marshal(entry.ContextUsageAfter)
	if err != nil {
		return err
	}
	metadataJSON, err := json.Marshal(entry.Metadata)
	if err != nil {
		return err
	}
	keptUserEntryIDsJSON, err := json.Marshal(entry.KeptUserEntryIDs)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO entries(
		thread_id, id, ordinal, parent_id, type, turn_id, created_at,
		message_json, raw, raw_hash, raw_encoder_version,
		turn_status, provider, model, compaction_id, previous_compaction_id,
		compacted_through_entry_id, summary_schema_version, compaction_generation,
		compaction_window_id, first_kept_entry_id, kept_user_entry_ids_json, summary, compaction_trigger,
		compaction_reason, compaction_phase, tokens_before, tokens_after_estimate,
		context_usage_before_json, context_usage_after_json, error, metadata_json
	) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		entry.ThreadID, entry.ID, ordinal, entry.ParentID, string(entry.Type), entry.TurnID, formatTime(entry.CreatedAt),
		string(messageJSON), entry.Raw, entry.RawHash, 1,
		string(entry.TurnStatus), entry.Provider, entry.Model, entry.CompactionID, entry.PreviousCompactionID,
		entry.CompactedThroughEntryID, entry.SummarySchemaVersion, entry.CompactionGeneration,
		entry.CompactionWindowID, entry.FirstKeptEntryID, string(keptUserEntryIDsJSON), entry.Summary, entry.CompactionTrigger,
		entry.CompactionReason, entry.CompactionPhase, entry.TokensBefore, entry.TokensAfterEstimate,
		string(beforeJSON), string(afterJSON), entry.Error, string(metadataJSON))
	return err
}

func insertSegment(ctx context.Context, tx sqlRunner, seg cache.Segment) error {
	data, err := json.Marshal(seg)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO prompt_segments(id, prompt_scope_id, provider, model, sequence, created_at, data_json)
		VALUES(?, ?, ?, ?, ?, ?, ?)`, seg.ID, seg.PromptScopeID, seg.Provider, seg.Model, seg.Sequence, formatTime(seg.CreatedAt), string(data))
	return err
}

func insertToolset(ctx context.Context, tx sqlRunner, snap cache.ToolsetSnapshot) error {
	data, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO prompt_toolsets(id, prompt_scope_id, provider, model, epoch, created_at, data_json)
		VALUES(?, ?, ?, ?, ?, ?, ?)`, snap.ID, snap.PromptScopeID, snap.Provider, snap.Model, snap.Epoch, formatTime(snap.CreatedAt), string(data))
	return err
}

func insertProviderRequest(ctx context.Context, tx sqlRunner, req cache.ProviderRequestRecord) error {
	data, err := json.Marshal(req)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO prompt_requests(id, prompt_scope_id, provider, model, created_at, data_json)
		VALUES(?, ?, ?, ?, ?, ?)`, req.ID, req.PromptScopeID, req.Provider, req.Model, formatTime(req.CreatedAt), string(data))
	return err
}

func insertProviderResponse(ctx context.Context, tx sqlRunner, resp cache.ProviderResponseRecord) error {
	if strings.TrimSpace(resp.PromptScopeID) == "" {
		return errors.New("cache response must include prompt scope id")
	}
	data, err := json.Marshal(resp)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO prompt_responses(request_id, prompt_scope_id, created_at, data_json)
		VALUES(?, ?, ?, ?)`, resp.RequestID, resp.PromptScopeID, formatTime(resp.CreatedAt), string(data))
	return err
}

func loadEntry(ctx context.Context, q sqlRunner, threadID, entryID string) (sessiontree.Entry, error) {
	rows, err := q.QueryContext(ctx, `SELECT `+entryColumns+` FROM entries WHERE thread_id = ? AND id = ?`, threadID, entryID)
	if err != nil {
		return sessiontree.Entry{}, err
	}
	defer rows.Close()
	if !rows.Next() {
		return sessiontree.Entry{}, sql.ErrNoRows
	}
	entry, err := scanEntry(rows)
	if err != nil {
		return sessiontree.Entry{}, err
	}
	return entry, rows.Err()
}

func loadEntries(ctx context.Context, q sqlRunner, threadID string) ([]sessiontree.Entry, error) {
	rows, err := q.QueryContext(ctx, `SELECT `+entryColumns+` FROM entries WHERE thread_id = ? ORDER BY ordinal`, threadID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []sessiontree.Entry
	for rows.Next() {
		entry, err := scanEntry(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, entry)
	}
	return out, rows.Err()
}

func scanEntry(rows *sql.Rows) (sessiontree.Entry, error) {
	var entry sessiontree.Entry
	var typ, turnStatus, created, messageJSON, keptUserEntryIDsJSON, beforeJSON, afterJSON, metadataJSON string
	var rawEncoder int
	err := rows.Scan(
		&entry.ThreadID, &entry.ID, &entry.ParentID, &typ, &entry.TurnID, &created,
		&messageJSON, &entry.Raw, &entry.RawHash, &rawEncoder,
		&turnStatus, &entry.Provider, &entry.Model, &entry.CompactionID, &entry.PreviousCompactionID,
		&entry.CompactedThroughEntryID, &entry.SummarySchemaVersion, &entry.CompactionGeneration,
		&entry.CompactionWindowID, &entry.FirstKeptEntryID, &keptUserEntryIDsJSON, &entry.Summary, &entry.CompactionTrigger,
		&entry.CompactionReason, &entry.CompactionPhase, &entry.TokensBefore, &entry.TokensAfterEstimate,
		&beforeJSON, &afterJSON, &entry.Error, &metadataJSON,
	)
	if err != nil {
		return sessiontree.Entry{}, err
	}
	if strconv.Itoa(rawEncoder) != rawEncoderVersion {
		return sessiontree.Entry{}, fmt.Errorf("unsupported sqlite store entry raw encoder version %d", rawEncoder)
	}
	entry.Type = sessiontree.EntryType(typ)
	entry.TurnStatus = sessiontree.TurnMarkerStatus(turnStatus)
	entry.CreatedAt = parseTime(created)
	if strings.TrimSpace(messageJSON) != "" {
		if err := json.Unmarshal([]byte(messageJSON), &entry.Message); err != nil {
			return sessiontree.Entry{}, err
		}
	}
	if strings.TrimSpace(keptUserEntryIDsJSON) != "" && keptUserEntryIDsJSON != "null" {
		if err := json.Unmarshal([]byte(keptUserEntryIDsJSON), &entry.KeptUserEntryIDs); err != nil {
			return sessiontree.Entry{}, err
		}
	}
	if strings.TrimSpace(beforeJSON) != "" {
		if err := json.Unmarshal([]byte(beforeJSON), &entry.ContextUsageBefore); err != nil {
			return sessiontree.Entry{}, err
		}
	}
	if strings.TrimSpace(afterJSON) != "" {
		if err := json.Unmarshal([]byte(afterJSON), &entry.ContextUsageAfter); err != nil {
			return sessiontree.Entry{}, err
		}
	}
	if strings.TrimSpace(metadataJSON) != "" && metadataJSON != "null" {
		if err := json.Unmarshal([]byte(metadataJSON), &entry.Metadata); err != nil {
			return sessiontree.Entry{}, err
		}
	}
	return cloneEntry(entry), nil
}

func pathWithRunner(ctx context.Context, q sqlRunner, threadID, leafID string) ([]sessiontree.Entry, error) {
	meta, err := loadThread(ctx, q, threadID)
	if err != nil {
		return nil, err
	}
	if leafID == "" {
		leafID = meta.LeafID
	}
	if leafID == "" {
		return nil, nil
	}
	entries, err := loadEntries(ctx, q, threadID)
	if err != nil {
		return nil, err
	}
	byID := map[string]sessiontree.Entry{}
	for _, entry := range entries {
		byID[entry.ID] = entry
	}
	var rev []sessiontree.Entry
	seen := map[string]struct{}{}
	for id := leafID; id != ""; {
		if _, ok := seen[id]; ok {
			return nil, sessiontree.ErrInvalidParent
		}
		seen[id] = struct{}{}
		entry, ok := byID[id]
		if !ok {
			return nil, sessiontree.ErrEntryNotFound
		}
		rev = append(rev, cloneEntry(entry))
		id = entry.ParentID
	}
	slices.Reverse(rev)
	return rev, nil
}

func entryExists(ctx context.Context, q sqlRunner, threadID, entryID string) (bool, error) {
	var count int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM entries WHERE thread_id = ? AND id = ?`, threadID, entryID).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

func threadExists(ctx context.Context, q sqlRunner, threadID string) (bool, error) {
	var count int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM threads WHERE id = ?`, threadID).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

func isConstraintError(err error) bool {
	return err != nil && (strings.Contains(err.Error(), "constraint failed") || strings.Contains(err.Error(), "UNIQUE constraint failed"))
}

func nextEntryID(ctx context.Context, q sqlRunner, threadID string) (string, error) {
	var count int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM entries WHERE thread_id = ?`, threadID).Scan(&count); err != nil {
		return "", err
	}
	for i := count + 1; ; i++ {
		id := fmt.Sprintf("%s-entry-%d", threadID, i)
		ok, err := entryExists(ctx, q, threadID, id)
		if err != nil {
			return "", err
		}
		if !ok {
			return id, nil
		}
	}
}

func nextForkID(ctx context.Context, q sqlRunner, sourceThreadID string) (string, error) {
	for i := 1; ; i++ {
		id := fmt.Sprintf("%s-fork-%d", sourceThreadID, i)
		ok, err := threadExists(ctx, q, id)
		if err != nil {
			return "", err
		}
		if !ok {
			return id, nil
		}
	}
}

func nextOrdinal(ctx context.Context, q sqlRunner, threadID string) (int64, error) {
	var ordinal sql.NullInt64
	if err := q.QueryRowContext(ctx, `SELECT MAX(ordinal) FROM entries WHERE thread_id = ?`, threadID).Scan(&ordinal); err != nil {
		return 0, err
	}
	if !ordinal.Valid {
		return 1, nil
	}
	return ordinal.Int64 + 1, nil
}

func cleanIDs(values []string) []string {
	seen := map[string]struct{}{}
	var out []string
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func cloneEntry(entry sessiontree.Entry) sessiontree.Entry {
	entry.Message = session.CloneMessage(entry.Message)
	if entry.Metadata != nil {
		metadata := make(map[string]string, len(entry.Metadata))
		for key, value := range entry.Metadata {
			metadata[key] = value
		}
		entry.Metadata = metadata
	}
	if entry.KeptUserEntryIDs != nil {
		entry.KeptUserEntryIDs = append([]string(nil), entry.KeptUserEntryIDs...)
	}
	return entry
}

func rewriteEntryIDs(ids []string, oldToNew map[string]string) []string {
	if len(ids) == 0 {
		return nil
	}
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if next := oldToNew[id]; next != "" {
			out = append(out, next)
		}
	}
	return out
}

func rewriteForkID(value string, ids map[string]string) string {
	value = strings.TrimSpace(value)
	if value == "" || len(ids) == 0 {
		return value
	}
	if next := strings.TrimSpace(ids[value]); next != "" {
		return next
	}
	return value
}

func rewriteForkMetadata(metadata map[string]string, entryIDs map[string]string, turnIDs map[string]string, runIDs map[string]string) map[string]string {
	if len(metadata) == 0 {
		return nil
	}
	out := make(map[string]string, len(metadata))
	for key, value := range metadata {
		next := value
		switch strings.TrimSpace(key) {
		case "run_id", "trace_id":
			next = rewriteForkID(value, runIDs)
		case "turn_id":
			next = rewriteForkID(value, turnIDs)
		case "entry_id", "parent_entry_id", "input_entry_id":
			next = rewriteForkID(value, entryIDs)
		}
		out[key] = next
	}
	return out
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func parseTime(value string) time.Time {
	if value == "" {
		return time.Time{}
	}
	t, _ := time.Parse(time.RFC3339Nano, value)
	return t
}

const entryColumns = `thread_id, id, parent_id, type, turn_id, created_at,
	message_json, raw, raw_hash, raw_encoder_version,
	turn_status, provider, model, compaction_id, previous_compaction_id,
	compacted_through_entry_id, summary_schema_version, compaction_generation,
	compaction_window_id, first_kept_entry_id, kept_user_entry_ids_json, summary, compaction_trigger,
	compaction_reason, compaction_phase, tokens_before, tokens_after_estimate,
	context_usage_before_json, context_usage_after_json, error, metadata_json`

const schemaMetaSQL = `
CREATE TABLE IF NOT EXISTS schema_meta (
	key TEXT PRIMARY KEY,
	value TEXT NOT NULL
);
`

const schemaSQL = schemaMetaSQL + `

	CREATE TABLE IF NOT EXISTS threads (
		id TEXT PRIMARY KEY,
		leaf_id TEXT NOT NULL DEFAULT '',
		parent_thread_id TEXT NOT NULL DEFAULT '',
		parent_turn_id TEXT NOT NULL DEFAULT '',
		forked_from_thread_id TEXT NOT NULL DEFAULT '',
		forked_from_entry_id TEXT NOT NULL DEFAULT '',
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

` + activeTurnLeasesSQL + `

CREATE TABLE IF NOT EXISTS prompt_segments (
	rowid INTEGER PRIMARY KEY AUTOINCREMENT,
	id TEXT NOT NULL,
	prompt_scope_id TEXT NOT NULL,
	provider TEXT NOT NULL,
	model TEXT NOT NULL,
	sequence INTEGER NOT NULL DEFAULT 0,
	created_at TEXT NOT NULL,
	data_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS prompt_segments_lookup_idx ON prompt_segments(prompt_scope_id, provider, model, rowid);

CREATE TABLE IF NOT EXISTS prompt_toolsets (
	rowid INTEGER PRIMARY KEY AUTOINCREMENT,
	id TEXT NOT NULL,
	prompt_scope_id TEXT NOT NULL,
	provider TEXT NOT NULL,
	model TEXT NOT NULL,
	epoch INTEGER NOT NULL DEFAULT 0,
	created_at TEXT NOT NULL,
	data_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS prompt_toolsets_lookup_idx ON prompt_toolsets(prompt_scope_id, provider, model, rowid);

CREATE TABLE IF NOT EXISTS prompt_requests (
	rowid INTEGER PRIMARY KEY AUTOINCREMENT,
	id TEXT NOT NULL,
	prompt_scope_id TEXT NOT NULL,
	provider TEXT NOT NULL,
	model TEXT NOT NULL,
	created_at TEXT NOT NULL,
	data_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS prompt_requests_scope_idx ON prompt_requests(prompt_scope_id, rowid);

CREATE TABLE IF NOT EXISTS prompt_responses (
	rowid INTEGER PRIMARY KEY AUTOINCREMENT,
	request_id TEXT NOT NULL,
	prompt_scope_id TEXT NOT NULL,
	created_at TEXT NOT NULL,
	data_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS prompt_responses_scope_idx ON prompt_responses(prompt_scope_id, rowid);

CREATE TABLE IF NOT EXISTS tool_output_artifacts (
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

CREATE INDEX IF NOT EXISTS tool_output_artifacts_thread_idx ON tool_output_artifacts(thread_id, rowid);

CREATE TABLE IF NOT EXISTS metadata_records (
	namespace TEXT NOT NULL,
	id TEXT NOT NULL,
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL,
	data_json TEXT NOT NULL,
	PRIMARY KEY(namespace, id)
);

CREATE INDEX IF NOT EXISTS metadata_records_namespace_updated_idx ON metadata_records(namespace, updated_at, id);
`

const activeTurnLeasesSQL = `
CREATE TABLE IF NOT EXISTS active_turn_leases (
	thread_id TEXT PRIMARY KEY,
	turn_id TEXT NOT NULL DEFAULT '',
	owner_id TEXT NOT NULL DEFAULT '',
	created_at TEXT NOT NULL,
	FOREIGN KEY (thread_id) REFERENCES threads(id) ON DELETE CASCADE
);
`

var _ storage.Store = (*Store)(nil)
var _ artifact.Store = (*Store)(nil)
var _ sessiontree.Repo = (*Store)(nil)
var _ sessiontree.TurnLeaseRepo = (*Store)(nil)

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
	return s.withImmediate(ctx, func(tx sqlRunner) error {
		if err := ensureSchema(ctx, tx); err != nil {
			return err
		}
		threads, err := loadThreadAuthorityGraph(ctx, tx)
		if err != nil {
			return err
		}
		if err := sessiontree.ValidateThreadAuthorityGraph(threads); err != nil {
			return fmt.Errorf("validate sqlite store thread authority: %w", err)
		}
		return nil
	})
}

func (s *Store) metaValue(ctx context.Context, key string) (string, error) {
	return metaValue(ctx, s.db, key)
}

func metaValue(ctx context.Context, q sqlRunner, key string) (string, error) {
	var value string
	err := q.QueryRowContext(ctx, `SELECT value FROM schema_meta WHERE key = ?`, key).Scan(&value)
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
		if err := sessiontree.ValidateThreadMetaAuthority(meta); err != nil {
			return err
		}
		if parentID := strings.TrimSpace(meta.ParentThreadID); parentID != "" {
			if ok, err := threadExists(ctx, tx, parentID); err != nil {
				return err
			} else if !ok {
				return fmt.Errorf("%w: parent thread %q", sessiontree.ErrInvalidThreadAuthority, parentID)
			}
		}
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
			id, leaf_id, parent_thread_id, parent_turn_id, forked_from_thread_id, forked_from_entry_id, fork_operation_id, fork_operation_node_id,
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
		current, err := loadThread(ctx, tx, meta.ID)
		if err != nil {
			return err
		}
		if err := sessiontree.ValidateThreadMetaAuthority(meta); err != nil {
			return err
		}
		if !sessiontree.SameThreadAuthority(current, meta) {
			return fmt.Errorf("%w: thread %q authority is immutable", sessiontree.ErrInvalidThreadAuthority, meta.ID)
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

func (s *Store) PathPage(ctx context.Context, threadID, leafID, beforeEntryID string, limit int) (sessiontree.PathPage, error) {
	if limit <= 0 {
		return sessiontree.PathPage{}, errors.New("path page limit must be positive")
	}
	meta, err := loadThread(ctx, s.db, threadID)
	if err != nil {
		return sessiontree.PathPage{}, err
	}
	if leafID == "" {
		leafID = meta.LeafID
	}
	if beforeEntryID != "" {
		var parentID string
		if err := s.db.QueryRowContext(ctx, `SELECT parent_id FROM entries WHERE thread_id = ? AND id = ?`, threadID, beforeEntryID).Scan(&parentID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return sessiontree.PathPage{}, sessiontree.ErrEntryNotFound
			}
			return sessiontree.PathPage{}, err
		}
		leafID = parentID
	}
	if leafID == "" {
		return sessiontree.PathPage{}, nil
	}
	query := `WITH RECURSIVE path(depth, visited, cycle, ` + entryColumns + `) AS (
SELECT 0, printf('/%d/', e.rowid), 0, ` + qualifiedEntryColumns("e") + `
FROM entries e
WHERE e.thread_id = ? AND e.id = ?
UNION ALL
SELECT path.depth + 1,
	path.visited || printf('%d/', e.rowid),
	instr(path.visited, printf('/%d/', e.rowid)) > 0,
	` + qualifiedEntryColumns("e") + `
FROM entries e
JOIN path ON path.thread_id = e.thread_id AND path.parent_id = e.id
	WHERE path.cycle = 0
)
SELECT ` + entryColumns + `, COUNT(*) OVER(), MAX(cycle) OVER() FROM path ORDER BY depth ASC LIMIT ?`
	rows, err := s.db.QueryContext(ctx, query, threadID, leafID, limit+1)
	if err != nil {
		return sessiontree.PathPage{}, err
	}
	defer rows.Close()
	entries := make([]sessiontree.Entry, 0, limit+1)
	var newestOrdinal int64
	var hasCycle bool
	for rows.Next() {
		var pageNewestOrdinal int64
		var cycle int
		entry, err := scanEntry(rows, &pageNewestOrdinal, &cycle)
		if err != nil {
			return sessiontree.PathPage{}, err
		}
		if newestOrdinal == 0 {
			newestOrdinal = pageNewestOrdinal
		} else if newestOrdinal != pageNewestOrdinal {
			return sessiontree.PathPage{}, errors.New("sqlite store path page ordinal changed during query")
		}
		hasCycle = hasCycle || cycle != 0
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return sessiontree.PathPage{}, err
	}
	if hasCycle {
		return sessiontree.PathPage{}, sessiontree.ErrInvalidParent
	}
	if len(entries) == 0 {
		return sessiontree.PathPage{}, sessiontree.ErrEntryNotFound
	}
	page := sessiontree.PathPage{NewestOrdinal: newestOrdinal}
	if len(entries) > limit {
		page.HasMore = true
		page.NextEntryID = entries[limit-1].ID
		entries = entries[:limit]
	}
	page.Entries = entries
	return page, nil
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
		if targetID == "" && !opts.EntryIDPinned {
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
			existing, err := loadThread(ctx, tx, newID)
			if err != nil {
				return err
			}
			if opts.OperationID != "" && opts.OperationNodeID != "" &&
				existing.ForkOperationID == opts.OperationID &&
				existing.ForkOperationNodeID == opts.OperationNodeID &&
				existing.ForkedFromThreadID == opts.SourceThreadID &&
				existing.ForkedFromEntryID == targetID &&
				sessiontree.MatchesForkDestinationMeta(existing, opts.DestinationMeta) {
				forked = existing
				return nil
			}
			if opts.OperationID != "" || opts.OperationNodeID != "" {
				return sessiontree.ErrForkDestinationConflict
			}
			return sessiontree.ErrThreadExists
		}
		path, err := pathWithRunner(ctx, tx, opts.SourceThreadID, targetID)
		if err != nil {
			return err
		}
		meta := sessiontree.ThreadMeta{ID: newID, ForkedFromThreadID: opts.SourceThreadID, ForkedFromEntryID: targetID, ForkOperationID: opts.OperationID, ForkOperationNodeID: opts.OperationNodeID, CreatedAt: now, UpdatedAt: now}
		applyForkDestinationMeta(&meta, opts.DestinationMeta)
		if err := sessiontree.ValidateThreadMetaAuthority(meta); err != nil {
			return err
		}
		if parentID := strings.TrimSpace(meta.ParentThreadID); parentID != "" {
			if ok, err := threadExists(ctx, tx, parentID); err != nil {
				return err
			} else if !ok {
				return fmt.Errorf("%w: parent thread %q", sessiontree.ErrInvalidThreadAuthority, parentID)
			}
		}
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
			if opts.RewriteEntry != nil {
				next, err = opts.RewriteEntry(next, sessiontree.ForkEntryIdentity{
					SourceThreadID:      opts.SourceThreadID,
					DestinationThreadID: newID,
					TurnIDMap:           opts.TurnIDMap,
					RunIDMap:            opts.RunIDMap,
				})
				if err != nil {
					return err
				}
			}
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
		if todo, ok, err := loadAgentTodoState(ctx, tx, opts.SourceThreadID); err != nil {
			return err
		} else if ok {
			todo.ThreadID = newID
			todo.UpdatedByTurnID = rewriteForkID(todo.UpdatedByTurnID, opts.TurnIDMap)
			todo.UpdatedByRunID = rewriteForkID(todo.UpdatedByRunID, opts.RunIDMap)
			if err := putAgentTodoState(ctx, tx, todo); err != nil {
				return err
			}
		}
		forked = meta
		return nil
	})
	return forked, err
}

func applyForkDestinationMeta(meta *sessiontree.ThreadMeta, destination *sessiontree.ForkDestinationMeta) {
	if meta == nil || destination == nil {
		return
	}
	meta.ParentThreadID = strings.TrimSpace(destination.ParentThreadID)
	meta.ParentTurnID = strings.TrimSpace(destination.ParentTurnID)
	meta.TaskName = strings.TrimSpace(destination.TaskName)
	meta.TaskDescription = strings.TrimSpace(destination.TaskDescription)
	meta.AgentPath = strings.TrimSpace(destination.AgentPath)
	meta.HostProfileRef = strings.TrimSpace(destination.HostProfileRef)
	meta.ForkMode = strings.TrimSpace(destination.ForkMode)
	meta.Closed = destination.Closed
	meta.Status = strings.TrimSpace(destination.Status)
}

func (s *Store) ReadAgentTodoState(ctx context.Context, threadID string) (sessiontree.AgentTodoState, error) {
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		return sessiontree.AgentTodoState{}, errors.New("agent todo state thread id is required")
	}
	if _, err := loadThread(ctx, s.db, threadID); err != nil {
		return sessiontree.AgentTodoState{}, err
	}
	state, ok, err := loadAgentTodoState(ctx, s.db, threadID)
	if err != nil {
		return sessiontree.AgentTodoState{}, err
	}
	if !ok {
		return sessiontree.AgentTodoState{ThreadID: threadID}, nil
	}
	return state, nil
}

func (s *Store) CompareAndSwapAgentTodoState(ctx context.Context, state sessiontree.AgentTodoState, expectedVersion int64) (sessiontree.AgentTodoState, error) {
	state.ThreadID = strings.TrimSpace(state.ThreadID)
	if state.ThreadID == "" {
		return sessiontree.AgentTodoState{}, errors.New("agent todo state thread id is required")
	}
	var updated sessiontree.AgentTodoState
	err := s.withImmediate(ctx, func(tx sqlRunner) error {
		if _, err := loadThread(ctx, tx, state.ThreadID); err != nil {
			return err
		}
		current, ok, err := loadAgentTodoState(ctx, tx, state.ThreadID)
		if err != nil {
			return err
		}
		if !ok {
			current = sessiontree.AgentTodoState{ThreadID: state.ThreadID}
		}
		if current.Version != expectedVersion {
			return sessiontree.ErrAgentTodoVersionConflict
		}
		state.Version = expectedVersion + 1
		if state.UpdatedAt.IsZero() {
			state.UpdatedAt = time.Now()
		}
		if err := putAgentTodoState(ctx, tx, state); err != nil {
			return err
		}
		updated = state
		updated.Items = append([]sessiontree.AgentTodoItem(nil), state.Items...)
		return nil
	})
	return updated, err
}

func loadAgentTodoState(ctx context.Context, q sqlRunner, threadID string) (sessiontree.AgentTodoState, bool, error) {
	var state sessiontree.AgentTodoState
	var itemsJSON, updatedAt string
	err := q.QueryRowContext(ctx, `SELECT thread_id, version, items_json, updated_at,
		updated_by_turn_id, updated_by_run_id, updated_by_tool_call_id
		FROM agent_todo_states WHERE thread_id = ?`, threadID).Scan(
		&state.ThreadID, &state.Version, &itemsJSON, &updatedAt,
		&state.UpdatedByTurnID, &state.UpdatedByRunID, &state.UpdatedByToolCall,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return sessiontree.AgentTodoState{}, false, nil
	}
	if err != nil {
		return sessiontree.AgentTodoState{}, false, err
	}
	if err := json.Unmarshal([]byte(itemsJSON), &state.Items); err != nil {
		return sessiontree.AgentTodoState{}, false, fmt.Errorf("decode agent todo state for thread %q: %w", threadID, err)
	}
	state.UpdatedAt = parseTime(updatedAt)
	return state, true, nil
}

func putAgentTodoState(ctx context.Context, q sqlRunner, state sessiontree.AgentTodoState) error {
	itemsJSON, err := json.Marshal(state.Items)
	if err != nil {
		return err
	}
	_, err = q.ExecContext(ctx, `INSERT INTO agent_todo_states(
		thread_id, version, items_json, updated_at, updated_by_turn_id, updated_by_run_id, updated_by_tool_call_id
	) VALUES(?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(thread_id) DO UPDATE SET
		version = excluded.version,
		items_json = excluded.items_json,
		updated_at = excluded.updated_at,
		updated_by_turn_id = excluded.updated_by_turn_id,
		updated_by_run_id = excluded.updated_by_run_id,
		updated_by_tool_call_id = excluded.updated_by_tool_call_id`,
		state.ThreadID, state.Version, string(itemsJSON), formatTime(state.UpdatedAt),
		state.UpdatedByTurnID, state.UpdatedByRunID, state.UpdatedByToolCall)
	return err
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

func (s *Store) PrepareForkOperation(ctx context.Context, rec storage.ForkOperationRecord) (storage.ForkOperationRecord, bool, error) {
	if err := storage.ValidatePreparedForkOperation(rec); err != nil {
		return storage.ForkOperationRecord{}, false, err
	}
	var existing storage.ForkOperationRecord
	created := false
	err := s.withImmediate(ctx, func(tx sqlRunner) error {
		result, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO fork_operations(
			operation_id, request_fingerprint, state, plan_json, result_json, error_code, error_message,
			created_at, updated_at, finished_at
		) VALUES(?, ?, ?, ?, '', '', '', ?, ?, '')`,
			rec.OperationID, rec.RequestFingerprint, string(rec.State), string(rec.Plan),
			formatTime(rec.CreatedAt), formatTime(rec.UpdatedAt))
		if err != nil {
			return err
		}
		if rows, err := result.RowsAffected(); err != nil {
			return err
		} else {
			created = rows == 1
		}
		existing, err = loadForkOperation(ctx, tx, rec.OperationID)
		return err
	})
	return existing, created, err
}

func (s *Store) ForkOperation(ctx context.Context, operationID string) (storage.ForkOperationRecord, error) {
	return loadForkOperation(ctx, s.db, strings.TrimSpace(operationID))
}

func (s *Store) UpdateForkOperation(ctx context.Context, rec storage.ForkOperationRecord) error {
	if err := storage.ValidateForkOperationRecord(rec); err != nil {
		return err
	}
	return s.withImmediate(ctx, func(tx sqlRunner) error {
		existing, err := loadForkOperation(ctx, tx, rec.OperationID)
		if err != nil {
			return err
		}
		if existing.RequestFingerprint != rec.RequestFingerprint || !jsonRawEqual(existing.Plan, rec.Plan) {
			return storage.ErrForkOperationConflict
		}
		if existing.State != storage.ForkOperationPrepared && !forkOperationRecordEqual(existing, rec) {
			return storage.ErrForkOperationConflict
		}
		_, err = tx.ExecContext(ctx, `UPDATE fork_operations SET
			state = ?, result_json = ?, error_code = ?, error_message = ?, updated_at = ?, finished_at = ?
			WHERE operation_id = ?`,
			string(rec.State), string(rec.Result), rec.ErrorCode, rec.ErrorMessage,
			formatTime(rec.UpdatedAt), formatTime(rec.FinishedAt), rec.OperationID)
		return err
	})
}

func loadForkOperation(ctx context.Context, q sqlRunner, operationID string) (storage.ForkOperationRecord, error) {
	var rec storage.ForkOperationRecord
	var state, plan, result, created, updated, finished string
	err := q.QueryRowContext(ctx, `SELECT operation_id, request_fingerprint, state, plan_json, result_json,
		error_code, error_message, created_at, updated_at, finished_at
		FROM fork_operations WHERE operation_id = ?`, operationID).Scan(
		&rec.OperationID, &rec.RequestFingerprint, &state, &plan, &result,
		&rec.ErrorCode, &rec.ErrorMessage, &created, &updated, &finished,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.ForkOperationRecord{}, storage.ErrForkOperationNotFound
	}
	if err != nil {
		return storage.ForkOperationRecord{}, err
	}
	rec.State = storage.ForkOperationState(state)
	rec.Plan = json.RawMessage(plan)
	if strings.TrimSpace(result) != "" {
		rec.Result = json.RawMessage(result)
	}
	rec.CreatedAt = parseTime(created)
	rec.UpdatedAt = parseTime(updated)
	rec.FinishedAt = parseTime(finished)
	return rec, nil
}

func jsonRawEqual(left, right json.RawMessage) bool {
	if len(left) == 0 || len(right) == 0 {
		return len(left) == len(right)
	}
	var leftValue any
	var rightValue any
	if json.Unmarshal(left, &leftValue) != nil || json.Unmarshal(right, &rightValue) != nil {
		return false
	}
	leftJSON, _ := json.Marshal(leftValue)
	rightJSON, _ := json.Marshal(rightValue)
	return string(leftJSON) == string(rightJSON)
}

func forkOperationRecordEqual(left, right storage.ForkOperationRecord) bool {
	return left.OperationID == right.OperationID &&
		left.RequestFingerprint == right.RequestFingerprint &&
		left.State == right.State &&
		jsonRawEqual(left.Plan, right.Plan) &&
		jsonRawEqual(left.Result, right.Result) &&
		left.ErrorCode == right.ErrorCode &&
		left.ErrorMessage == right.ErrorMessage &&
		left.CreatedAt.Equal(right.CreatedAt) &&
		left.UpdatedAt.Equal(right.UpdatedAt) &&
		left.FinishedAt.Equal(right.FinishedAt)
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

func (s *Store) ProviderState(ctx context.Context, threadID string) (storage.ProviderStateRecord, error) {
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		return storage.ProviderStateRecord{}, errors.New("provider state thread id is required")
	}
	var record storage.ProviderStateRecord
	var rawState, updatedAt string
	err := s.db.QueryRowContext(ctx, `SELECT thread_id, leaf_entry_id, compatibility_key, state_json,
		created_by_run_id, created_by_turn_id, updated_at
		FROM provider_states WHERE thread_id = ?`, threadID).Scan(
		&record.ThreadID, &record.LeafEntryID, &record.CompatibilityKey, &rawState,
		&record.CreatedByRunID, &record.CreatedByTurnID, &updatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.ProviderStateRecord{}, storage.ErrProviderStateNotFound
	}
	if err != nil {
		return storage.ProviderStateRecord{}, err
	}
	if err := json.Unmarshal([]byte(rawState), &record.State); err != nil {
		return storage.ProviderStateRecord{}, fmt.Errorf("decode provider state for thread %q: %w", threadID, err)
	}
	if strings.TrimSpace(record.State.Kind) == "" || strings.TrimSpace(record.State.ID) == "" {
		return storage.ProviderStateRecord{}, fmt.Errorf("provider state for thread %q is incomplete", threadID)
	}
	record.UpdatedAt = parseTime(updatedAt)
	return record, nil
}

func (s *Store) PutProviderState(ctx context.Context, record storage.ProviderStateRecord) error {
	record.ThreadID = strings.TrimSpace(record.ThreadID)
	record.LeafEntryID = strings.TrimSpace(record.LeafEntryID)
	record.CompatibilityKey = strings.TrimSpace(record.CompatibilityKey)
	record.CreatedByRunID = strings.TrimSpace(record.CreatedByRunID)
	record.CreatedByTurnID = strings.TrimSpace(record.CreatedByTurnID)
	if record.ThreadID == "" || record.LeafEntryID == "" || record.CompatibilityKey == "" || strings.TrimSpace(record.State.Kind) == "" || strings.TrimSpace(record.State.ID) == "" {
		return errors.New("provider state record is incomplete")
	}
	if record.UpdatedAt.IsZero() {
		record.UpdatedAt = time.Now()
	}
	rawState, err := json.Marshal(record.State)
	if err != nil {
		return err
	}
	return s.withImmediate(ctx, func(tx sqlRunner) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO provider_states(
			thread_id, leaf_entry_id, compatibility_key, state_json, created_by_run_id, created_by_turn_id, updated_at
		) VALUES(?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(thread_id) DO UPDATE SET
			leaf_entry_id = excluded.leaf_entry_id,
			compatibility_key = excluded.compatibility_key,
			state_json = excluded.state_json,
			created_by_run_id = excluded.created_by_run_id,
			created_by_turn_id = excluded.created_by_turn_id,
			updated_at = excluded.updated_at`,
			record.ThreadID, record.LeafEntryID, record.CompatibilityKey, string(rawState),
			record.CreatedByRunID, record.CreatedByTurnID, formatTime(record.UpdatedAt))
		return err
	})
}

func (s *Store) DeleteProviderState(ctx context.Context, threadID string) error {
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		return errors.New("provider state thread id is required")
	}
	return s.withImmediate(ctx, func(tx sqlRunner) error {
		_, err := tx.ExecContext(ctx, `DELETE FROM provider_states WHERE thread_id = ?`, threadID)
		return err
	})
}

func (s *Store) DeleteThreadTreeData(ctx context.Context, rootThreadID string) ([]string, error) {
	rootThreadID = strings.TrimSpace(rootThreadID)
	if rootThreadID == "" {
		return nil, errors.New("root thread id is required")
	}
	var deletedThreadIDs []string
	err := s.withImmediate(ctx, func(tx sqlRunner) error {
		threads, err := loadThreadAuthorityGraph(ctx, tx)
		if err != nil {
			return err
		}
		threadIDs, err := sessiontree.ThreadAuthorityTreeIDs(threads, rootThreadID)
		if err != nil {
			return err
		}
		deletedThreadIDs = append([]string(nil), threadIDs...)
		for _, threadID := range threadIDs {
			if _, err := tx.ExecContext(ctx, `DELETE FROM metadata_records WHERE id = ?`, threadID); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM tool_output_artifacts WHERE thread_id = ?`, threadID); err != nil {
				return err
			}
		}
		for i := len(threadIDs) - 1; i >= 0; i-- {
			if _, err := tx.ExecContext(ctx, `DELETE FROM threads WHERE id = ?`, threadIDs[i]); err != nil {
				return err
			}
		}
		for _, promptScopeID := range threadIDs {
			for _, table := range []string{"prompt_segments", "prompt_toolsets", "prompt_requests", "prompt_responses"} {
				if _, err := tx.ExecContext(ctx, `DELETE FROM `+table+` WHERE prompt_scope_id = ?`, promptScopeID); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return deletedThreadIDs, nil
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
		id, leaf_id, parent_thread_id, parent_turn_id, forked_from_thread_id, forked_from_entry_id, fork_operation_id, fork_operation_node_id,
		task_name, task_description, agent_path, host_profile_ref, fork_mode, closed, archived,
		title, title_status, title_source, title_updated_at, title_error,
		created_at, updated_at, status, last_viewed_at
	) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		meta.ID, meta.LeafID, meta.ParentThreadID, meta.ParentTurnID, meta.ForkedFromThreadID, meta.ForkedFromEntryID,
		meta.ForkOperationID, meta.ForkOperationNodeID,
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
		leaf_id = ?, parent_thread_id = ?, parent_turn_id = ?, forked_from_thread_id = ?, forked_from_entry_id = ?, fork_operation_id = ?, fork_operation_node_id = ?,
		task_name = ?, task_description = ?, agent_path = ?, host_profile_ref = ?, fork_mode = ?, closed = ?, archived = ?,
		title = ?, title_status = ?, title_source = ?, title_updated_at = ?, title_error = ?,
		created_at = ?, updated_at = ?, status = ?, last_viewed_at = ?
		WHERE id = ?`,
		meta.LeafID, meta.ParentThreadID, meta.ParentTurnID, meta.ForkedFromThreadID, meta.ForkedFromEntryID,
		meta.ForkOperationID, meta.ForkOperationNodeID,
		meta.TaskName, meta.TaskDescription, meta.AgentPath, meta.HostProfileRef, meta.ForkMode, boolInt(meta.Closed), boolInt(meta.Archived),
		meta.Title, string(meta.TitleStatus), string(meta.TitleSource), formatTime(meta.TitleUpdatedAt), meta.TitleError,
		formatTime(meta.CreatedAt), formatTime(meta.UpdatedAt), meta.Status, formatTime(meta.LastViewedAt), meta.ID)
	return err
}

func loadThread(ctx context.Context, q sqlRunner, threadID string) (sessiontree.ThreadMeta, error) {
	meta, err := scanThreadMeta(q.QueryRowContext(ctx, `SELECT
		id, leaf_id, parent_thread_id, parent_turn_id, forked_from_thread_id, forked_from_entry_id, fork_operation_id, fork_operation_node_id,
		task_name, task_description, agent_path, host_profile_ref, fork_mode, closed, archived,
		title, title_status, title_source, title_updated_at, title_error,
		created_at, updated_at, status, last_viewed_at
		FROM threads WHERE id = ?`, threadID))
	if errors.Is(err, sql.ErrNoRows) {
		return sessiontree.ThreadMeta{}, sessiontree.ErrThreadNotFound
	}
	return meta, err
}

func loadThreadAuthorityGraph(ctx context.Context, q sqlRunner) ([]sessiontree.ThreadMeta, error) {
	rows, err := q.QueryContext(ctx, `SELECT
		id, leaf_id, parent_thread_id, parent_turn_id, forked_from_thread_id, forked_from_entry_id, fork_operation_id, fork_operation_node_id,
		task_name, task_description, agent_path, host_profile_ref, fork_mode, closed, archived,
		title, title_status, title_source, title_updated_at, title_error,
		created_at, updated_at, status, last_viewed_at
		FROM threads ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	threads := []sessiontree.ThreadMeta{}
	for rows.Next() {
		meta, err := scanThreadMeta(rows)
		if err != nil {
			return nil, err
		}
		threads = append(threads, meta)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return threads, nil
}

func scanThreadMeta(scanner rowScanner) (sessiontree.ThreadMeta, error) {
	var meta sessiontree.ThreadMeta
	var archived, closed int
	var titleStatus, titleSource, titleUpdated, created, updated, status, lastViewed string
	err := scanner.Scan(
		&meta.ID, &meta.LeafID, &meta.ParentThreadID, &meta.ParentTurnID, &meta.ForkedFromThreadID, &meta.ForkedFromEntryID,
		&meta.ForkOperationID, &meta.ForkOperationNodeID,
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
	if err := sessiontree.ValidateThreadMetaAuthority(meta); err != nil {
		return sessiontree.ThreadMeta{}, err
	}
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
		compaction_operation_id, compaction_request_id, compaction_source,
		context_usage_before_json, context_usage_after_json, error, metadata_json
	) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		entry.ThreadID, entry.ID, ordinal, entry.ParentID, string(entry.Type), entry.TurnID, formatTime(entry.CreatedAt),
		string(messageJSON), entry.Raw, entry.RawHash, 1,
		string(entry.TurnStatus), entry.Provider, entry.Model, entry.CompactionID, entry.PreviousCompactionID,
		entry.CompactedThroughEntryID, entry.SummarySchemaVersion, entry.CompactionGeneration,
		entry.CompactionWindowID, entry.FirstKeptEntryID, string(keptUserEntryIDsJSON), entry.Summary, entry.CompactionTrigger,
		entry.CompactionReason, entry.CompactionPhase, entry.TokensBefore, entry.TokensAfterEstimate,
		entry.CompactionOperationID, entry.CompactionRequestID, entry.CompactionSource,
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

func scanEntry(rows rowScanner, extra ...any) (sessiontree.Entry, error) {
	var entry sessiontree.Entry
	var typ, turnStatus, created, messageJSON, keptUserEntryIDsJSON, beforeJSON, afterJSON, metadataJSON string
	var rawEncoder int
	destinations := []any{
		&entry.ThreadID, &entry.ID, &entry.ParentID, &typ, &entry.TurnID, &created,
		&messageJSON, &entry.Raw, &entry.RawHash, &rawEncoder,
		&turnStatus, &entry.Provider, &entry.Model, &entry.CompactionID, &entry.PreviousCompactionID,
		&entry.CompactedThroughEntryID, &entry.SummarySchemaVersion, &entry.CompactionGeneration,
		&entry.CompactionWindowID, &entry.FirstKeptEntryID, &keptUserEntryIDsJSON, &entry.Summary, &entry.CompactionTrigger,
		&entry.CompactionReason, &entry.CompactionPhase, &entry.TokensBefore, &entry.TokensAfterEstimate,
		&entry.CompactionOperationID, &entry.CompactionRequestID, &entry.CompactionSource,
		&beforeJSON, &afterJSON, &entry.Error, &metadataJSON,
	}
	destinations = append(destinations, extra...)
	err := rows.Scan(destinations...)
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
	compaction_operation_id, compaction_request_id, compaction_source,
	context_usage_before_json, context_usage_after_json, error, metadata_json`

func qualifiedEntryColumns(alias string) string {
	columns := strings.Split(entryColumns, ",")
	for index, column := range columns {
		columns[index] = alias + "." + strings.TrimSpace(column)
	}
	return strings.Join(columns, ", ")
}

var _ storage.Store = (*Store)(nil)
var _ artifact.Store = (*Store)(nil)
var _ sessiontree.Repo = (*Store)(nil)
var _ sessiontree.TurnLeaseRepo = (*Store)(nil)
var _ sessiontree.AgentTodoStateRepo = (*Store)(nil)

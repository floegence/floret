package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/floegence/floret/provider/cache"
	"github.com/floegence/floret/runtime/storage"
	"github.com/floegence/floret/session"
	"github.com/floegence/floret/sessiontree"
	_ "modernc.org/sqlite"
)

const (
	schemaVersion     = "3"
	initialSchema     = "1"
	rawEncoderVersion = "1"
	driverName        = "sqlite"
)

type Option func(*options)

type options struct{}

type Store struct {
	db   *sql.DB
	path string
}

type ImportSummary struct {
	Threads    int
	Metadata   int
	PromptRuns int
	Skipped    int
	Conflicts  int
}

type sqlRunner interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
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

func OpenMemory(opts ...Option) (*Store, error) {
	return Open(":memory:", opts...)
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

func (s *Store) MetaValue(ctx context.Context, key string) (string, error) {
	return s.metaValue(ctx, key)
}

func (s *Store) PutMetaValue(ctx context.Context, key, value string) error {
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
	if _, err := s.db.ExecContext(ctx, schemaSQL); err != nil {
		return err
	}
	current, err := s.metaValue(ctx, "schema_version")
	if errors.Is(err, storage.ErrMetadataNotFound) {
		if _, err := s.db.ExecContext(ctx, `INSERT INTO schema_meta(key, value) VALUES
			('schema_version', ?), ('raw_encoder_version', ?)`, schemaVersion, rawEncoderVersion); err != nil {
			return err
		}
		return nil
	}
	if err != nil {
		return err
	}
	if current == initialSchema {
		if err := s.migrateV1ToV2(ctx); err != nil {
			return err
		}
		current = "2"
	}
	if current == "2" {
		if err := s.migrateV2ToV3(ctx); err != nil {
			return err
		}
		current = schemaVersion
	}
	if current != schemaVersion {
		return fmt.Errorf("unsupported sqlite store schema version %q", current)
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

func (s *Store) migrateV1ToV2(ctx context.Context) error {
	return s.withImmediate(ctx, func(tx sqlRunner) error {
		if _, err := tx.ExecContext(ctx, activeTurnLeasesSQL); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE schema_meta SET value = '2' WHERE key = 'schema_version'`)
		return err
	})
}

func (s *Store) migrateV2ToV3(ctx context.Context) error {
	return s.withImmediate(ctx, func(tx sqlRunner) error {
		if _, err := tx.ExecContext(ctx, `ALTER TABLE entries ADD COLUMN kept_user_entry_ids_json TEXT NOT NULL DEFAULT '[]'`); err != nil {
			if !strings.Contains(err.Error(), "duplicate column name") {
				return err
			}
		}
		_, err := tx.ExecContext(ctx, `UPDATE schema_meta SET value = ? WHERE key = 'schema_version'`, schemaVersion)
		return err
	})
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
			next.FirstKeptEntryID = oldToNew[entry.FirstKeptEntryID]
			next.CompactedThroughEntryID = oldToNew[entry.CompactedThroughEntryID]
			next.KeptUserEntryIDs = rewriteEntryIDs(entry.KeptUserEntryIDs, oldToNew)
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

func (s *Store) Segments(ctx context.Context, runID, provider, model string) ([]cache.Segment, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT data_json FROM prompt_segments
		WHERE run_id = ? AND provider = ? AND model = ? ORDER BY rowid`, runID, provider, model)
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

func (s *Store) ActiveToolset(ctx context.Context, runID, provider, model string) (cache.ToolsetSnapshot, bool, error) {
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT data_json FROM prompt_toolsets
		WHERE run_id = ? AND provider = ? AND model = ? ORDER BY rowid DESC LIMIT 1`, runID, provider, model).Scan(&raw)
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

func (s *Store) ProviderRequests(ctx context.Context, runID string) ([]cache.ProviderRequestRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT data_json FROM prompt_requests WHERE run_id = ? ORDER BY rowid`, runID)
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

func (s *Store) ProviderResponses(ctx context.Context, runID string) ([]cache.ProviderResponseRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT data_json FROM prompt_responses WHERE run_id = ? ORDER BY rowid`, runID)
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

func (s *Store) LatestPressureAnchor(ctx context.Context, sessionID, providerName, model string) (cache.PressureAnchorState, bool, error) {
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
		if sessionID != "" && anchor.SessionID != sessionID && anchor.ThreadID != sessionID {
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

func (s *Store) DeleteRuns(ctx context.Context, runIDs ...string) error {
	clean := cleanIDs(runIDs)
	if len(clean) == 0 {
		return nil
	}
	return s.withImmediate(ctx, func(tx sqlRunner) error {
		for _, runID := range clean {
			for _, table := range []string{"prompt_segments", "prompt_toolsets", "prompt_requests", "prompt_responses"} {
				if _, err := tx.ExecContext(ctx, `DELETE FROM `+table+` WHERE run_id = ?`, runID); err != nil {
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

func (s *Store) DeleteSession(ctx context.Context, req storage.DeleteSessionRequest) error {
	sessionID := strings.TrimSpace(req.SessionID)
	if sessionID == "" {
		return errors.New("session id is required")
	}
	runIDs := cleanIDs(append([]string{sessionID}, req.PromptScopeIDs...))
	namespaces := cleanIDs(req.MetadataNamespaces)
	return s.withImmediate(ctx, func(tx sqlRunner) error {
		if len(namespaces) == 0 {
			if _, err := tx.ExecContext(ctx, `DELETE FROM metadata_records WHERE namespace = '' AND id = ?`, sessionID); err != nil {
				return err
			}
		} else {
			for _, namespace := range namespaces {
				if _, err := tx.ExecContext(ctx, `DELETE FROM metadata_records WHERE namespace = ? AND id = ?`, namespace, sessionID); err != nil {
					return err
				}
			}
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM threads WHERE id = ?`, sessionID); err != nil {
			return err
		}
		for _, runID := range runIDs {
			for _, table := range []string{"prompt_segments", "prompt_toolsets", "prompt_requests", "prompt_responses"} {
				if _, err := tx.ExecContext(ctx, `DELETE FROM `+table+` WHERE run_id = ?`, runID); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (s *Store) ImportSessionTree(ctx context.Context, root string) (ImportSummary, error) {
	var summary ImportSummary
	paths, err := filepath.Glob(filepath.Join(root, "*", "thread.json"))
	if err != nil {
		return summary, err
	}
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			summary.Skipped++
			continue
		}
		var meta sessiontree.ThreadMeta
		if err := json.Unmarshal(data, &meta); err != nil || meta.ID == "" {
			summary.Skipped++
			continue
		}
		entries, err := readLegacyEntries(filepath.Join(filepath.Dir(path), "entries.jsonl"))
		if err != nil {
			summary.Skipped++
			continue
		}
		if len(entries) > 0 {
			last := entries[len(entries)-1]
			if meta.LeafID == "" || !entryIDInSlice(entries, meta.LeafID) {
				if newest, ok := newestRootReachableEntry(entries); ok {
					meta.LeafID = newest.ID
					meta.UpdatedAt = newest.CreatedAt
				}
			} else if meta.LeafID != last.ID {
				if reachable := reachableEntryIDs(entries, meta.LeafID); reachable[last.ParentID] {
					meta.LeafID = last.ID
					meta.UpdatedAt = last.CreatedAt
				}
			}
		}
		if err := s.importThread(ctx, meta, entries); err != nil {
			summary.Conflicts++
			continue
		}
		summary.Threads++
	}
	return summary, nil
}

func (s *Store) ImportPromptCache(ctx context.Context, root string) (ImportSummary, error) {
	var summary ImportSummary
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return summary, nil
	}
	if err != nil {
		return summary, err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		dir := filepath.Join(root, entry.Name())
		if err := s.importPromptRun(ctx, dir); err != nil {
			summary.Skipped++
			continue
		}
		summary.PromptRuns++
	}
	return summary, nil
}

func (s *Store) importPromptRun(ctx context.Context, dir string) error {
	segments, err := readPromptFile[cache.Segment](ctx, dir, "raw_segments.jsonl")
	if err != nil {
		return err
	}
	toolsets, err := readPromptFile[cache.ToolsetSnapshot](ctx, dir, "toolsets.jsonl")
	if err != nil {
		return err
	}
	requests, err := readPromptFile[cache.ProviderRequestRecord](ctx, dir, "requests.jsonl")
	if err != nil {
		return err
	}
	responses, err := readPromptFile[cache.ProviderResponseRecord](ctx, dir, "responses.jsonl")
	if err != nil {
		return err
	}
	return s.withImmediate(ctx, func(tx sqlRunner) error {
		imported, err := promptRunAlreadyImported(ctx, tx, segments, toolsets, requests, responses)
		if err != nil || imported {
			return err
		}
		for _, seg := range segments {
			if err := insertSegment(ctx, tx, seg); err != nil {
				return err
			}
		}
		for _, snap := range toolsets {
			if err := insertToolset(ctx, tx, snap); err != nil {
				return err
			}
		}
		for _, req := range requests {
			if err := insertProviderRequest(ctx, tx, req); err != nil {
				return err
			}
		}
		for _, resp := range responses {
			if err := insertProviderResponse(ctx, tx, resp); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) ImportMetadataDir(ctx context.Context, namespace, root string) (ImportSummary, error) {
	var summary ImportSummary
	paths, err := filepath.Glob(filepath.Join(root, "*.json"))
	if err != nil {
		return summary, err
	}
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			summary.Skipped++
			continue
		}
		var header struct {
			ID        string    `json:"id"`
			CreatedAt time.Time `json:"created_at"`
			UpdatedAt time.Time `json:"updated_at"`
		}
		if err := json.Unmarshal(data, &header); err != nil || header.ID == "" {
			summary.Skipped++
			continue
		}
		if err := s.PutMetadata(ctx, storage.MetadataRecord{
			Namespace: namespace,
			ID:        header.ID,
			CreatedAt: header.CreatedAt,
			UpdatedAt: header.UpdatedAt,
			Data:      append([]byte(nil), data...),
		}); err != nil {
			summary.Conflicts++
			continue
		}
		summary.Metadata++
	}
	return summary, nil
}

func (s *Store) importThread(ctx context.Context, meta sessiontree.ThreadMeta, entries []sessiontree.Entry) error {
	entries = normalizeLegacyEntries(meta.ID, entries)
	return s.withImmediate(ctx, func(tx sqlRunner) error {
		exists, err := threadExists(ctx, tx, meta.ID)
		if err != nil {
			return err
		}
		if exists {
			current, err := loadEntries(ctx, tx, meta.ID)
			if err != nil {
				return err
			}
			if len(current) != len(entries) {
				return errors.New("legacy thread conflicts with existing sqlite thread")
			}
			for i := range current {
				if current[i].ID != entries[i].ID || current[i].RawHash != entries[i].RawHash {
					return errors.New("legacy thread conflicts with existing sqlite thread")
				}
			}
			return nil
		}
		if err := insertThread(ctx, tx, meta); err != nil {
			return err
		}
		for i, entry := range entries {
			if err := insertEntry(ctx, tx, entry, int64(i+1), true); err != nil {
				return err
			}
		}
		return nil
	})
}

func promptRunAlreadyImported(ctx context.Context, tx sqlRunner, segments []cache.Segment, toolsets []cache.ToolsetSnapshot, requests []cache.ProviderRequestRecord, responses []cache.ProviderResponseRecord) (bool, error) {
	runIDs := promptRunIDs(segments, toolsets, requests, responses)
	if len(runIDs) == 0 {
		return false, nil
	}
	checked := false
	matched := false
	check := func(table string, expected []string) error {
		existing, err := promptRowsForRunIDs(ctx, tx, table, runIDs)
		if err != nil {
			return err
		}
		if len(existing) == 0 {
			if len(expected) == 0 {
				return nil
			}
			if checked {
				return errors.New("legacy prompt run conflicts with existing sqlite prompt rows")
			}
			return nil
		}
		checked = true
		if !slices.Equal(existing, expected) {
			return errors.New("legacy prompt run conflicts with existing sqlite prompt rows")
		}
		matched = true
		return nil
	}
	if err := check("prompt_segments", promptJSONRows(segments)); err != nil {
		return false, err
	}
	if err := check("prompt_toolsets", promptJSONRows(toolsets)); err != nil {
		return false, err
	}
	if err := check("prompt_requests", promptJSONRows(requests)); err != nil {
		return false, err
	}
	if err := check("prompt_responses", promptJSONRows(responses)); err != nil {
		return false, err
	}
	return matched, nil
}

func promptRunIDs(segments []cache.Segment, toolsets []cache.ToolsetSnapshot, requests []cache.ProviderRequestRecord, responses []cache.ProviderResponseRecord) []string {
	var ids []string
	for _, seg := range segments {
		ids = append(ids, seg.RunID)
	}
	for _, snap := range toolsets {
		ids = append(ids, snap.RunID)
	}
	for _, req := range requests {
		ids = append(ids, req.RunID)
	}
	for _, resp := range responses {
		runID := resp.RunID
		if runID == "" {
			runID = runIDFromRequest(resp.RequestID)
		}
		ids = append(ids, runID)
	}
	return cleanIDs(ids)
}

func promptJSONRows[T any](values []T) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		data, _ := json.Marshal(value)
		out = append(out, string(data))
	}
	return out
}

func promptRowsForRunIDs(ctx context.Context, tx sqlRunner, table string, runIDs []string) ([]string, error) {
	var out []string
	for _, runID := range runIDs {
		rows, err := tx.QueryContext(ctx, `SELECT data_json FROM `+table+` WHERE run_id = ? ORDER BY rowid`, runID)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var raw string
			if err := rows.Scan(&raw); err != nil {
				_ = rows.Close()
				return nil, err
			}
			out = append(out, raw)
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func normalizeLegacyEntries(threadID string, entries []sessiontree.Entry) []sessiontree.Entry {
	out := make([]sessiontree.Entry, 0, len(entries))
	for _, entry := range entries {
		if entry.ThreadID == "" {
			entry.ThreadID = threadID
		}
		if entry.Raw == "" || entry.RawHash == "" {
			entry = sessiontree.PrepareEntry(entry)
		}
		out = append(out, entry)
	}
	return out
}

func insertThread(ctx context.Context, tx sqlRunner, meta sessiontree.ThreadMeta) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO threads(id, leaf_id, parent_thread_id, forked_from_thread_id, forked_from_entry_id, archived, created_at, updated_at)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?)`,
		meta.ID, meta.LeafID, meta.ParentThreadID, meta.ForkedFromThreadID, meta.ForkedFromEntryID, boolInt(meta.Archived), formatTime(meta.CreatedAt), formatTime(meta.UpdatedAt))
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
	_, err := tx.ExecContext(ctx, `UPDATE threads SET leaf_id = ?, parent_thread_id = ?, forked_from_thread_id = ?, forked_from_entry_id = ?, archived = ?, created_at = ?, updated_at = ? WHERE id = ?`,
		meta.LeafID, meta.ParentThreadID, meta.ForkedFromThreadID, meta.ForkedFromEntryID, boolInt(meta.Archived), formatTime(meta.CreatedAt), formatTime(meta.UpdatedAt), meta.ID)
	return err
}

func loadThread(ctx context.Context, q sqlRunner, threadID string) (sessiontree.ThreadMeta, error) {
	var meta sessiontree.ThreadMeta
	var archived int
	var created, updated string
	err := q.QueryRowContext(ctx, `SELECT id, leaf_id, parent_thread_id, forked_from_thread_id, forked_from_entry_id, archived, created_at, updated_at
		FROM threads WHERE id = ?`, threadID).Scan(&meta.ID, &meta.LeafID, &meta.ParentThreadID, &meta.ForkedFromThreadID, &meta.ForkedFromEntryID, &archived, &created, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return sessiontree.ThreadMeta{}, sessiontree.ErrThreadNotFound
	}
	if err != nil {
		return sessiontree.ThreadMeta{}, err
	}
	meta.Archived = archived != 0
	meta.CreatedAt = parseTime(created)
	meta.UpdatedAt = parseTime(updated)
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
	_, err = tx.ExecContext(ctx, `INSERT INTO prompt_segments(id, run_id, provider, model, sequence, created_at, data_json)
		VALUES(?, ?, ?, ?, ?, ?, ?)`, seg.ID, seg.RunID, seg.Provider, seg.Model, seg.Sequence, formatTime(seg.CreatedAt), string(data))
	return err
}

func insertToolset(ctx context.Context, tx sqlRunner, snap cache.ToolsetSnapshot) error {
	data, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO prompt_toolsets(id, run_id, provider, model, epoch, created_at, data_json)
		VALUES(?, ?, ?, ?, ?, ?, ?)`, snap.ID, snap.RunID, snap.Provider, snap.Model, snap.Epoch, formatTime(snap.CreatedAt), string(data))
	return err
}

func insertProviderRequest(ctx context.Context, tx sqlRunner, req cache.ProviderRequestRecord) error {
	data, err := json.Marshal(req)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO prompt_requests(id, run_id, provider, model, created_at, data_json)
		VALUES(?, ?, ?, ?, ?, ?)`, req.ID, req.RunID, req.Provider, req.Model, formatTime(req.CreatedAt), string(data))
	return err
}

func insertProviderResponse(ctx context.Context, tx sqlRunner, resp cache.ProviderResponseRecord) error {
	runID := resp.RunID
	if runID == "" {
		runID = runIDFromRequest(resp.RequestID)
	}
	if runID == "" {
		return errors.New("cache response must include run id")
	}
	data, err := json.Marshal(resp)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO prompt_responses(request_id, run_id, created_at, data_json)
		VALUES(?, ?, ?, ?)`, resp.RequestID, runID, formatTime(resp.CreatedAt), string(data))
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

func readLegacyEntries(path string) ([]sessiontree.Entry, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var entries []sessiontree.Entry
	dec := json.NewDecoder(f)
	for {
		var entry sessiontree.Entry
		if err := dec.Decode(&entry); errors.Is(err, io.EOF) {
			return entries, nil
		} else if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
}

func readPromptFile[T any](ctx context.Context, dir, name string) ([]T, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(filepath.Join(dir, name))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []T
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var value T
		if err := json.Unmarshal([]byte(line), &value); err != nil {
			return nil, err
		}
		out = append(out, value)
	}
	return out, nil
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

func entryIDInSlice(entries []sessiontree.Entry, id string) bool {
	return slices.ContainsFunc(entries, func(entry sessiontree.Entry) bool { return entry.ID == id })
}

func newestRootReachableEntry(entries []sessiontree.Entry) (sessiontree.Entry, bool) {
	reachable := map[string]bool{"": true}
	var newest sessiontree.Entry
	found := false
	for _, entry := range entries {
		if !reachable[entry.ParentID] {
			continue
		}
		reachable[entry.ID] = true
		newest = entry
		found = true
	}
	return newest, found
}

func reachableEntryIDs(entries []sessiontree.Entry, leafID string) map[string]bool {
	reachable := map[string]bool{"": leafID == ""}
	for _, entry := range entries {
		if leafID == "" || entry.ID == leafID || reachable[entry.ParentID] {
			reachable[entry.ID] = true
		}
	}
	return reachable
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

func runIDFromRequest(requestID string) string {
	if idx := strings.Index(requestID, ":req:"); idx >= 0 {
		return requestID[:idx]
	}
	if idx := strings.Index(requestID, ":request:"); idx >= 0 {
		return requestID[:idx]
	}
	if idx := strings.Index(requestID, ":resp:"); idx >= 0 {
		return requestID[:idx]
	}
	if idx := strings.Index(requestID, ":response:"); idx >= 0 {
		return requestID[:idx]
	}
	return ""
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

const schemaSQL = `
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

` + activeTurnLeasesSQL + `

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
var _ sessiontree.Repo = (*Store)(nil)
var _ sessiontree.TurnLeaseRepo = (*Store)(nil)

package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
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

type options struct {
	leasePolicy sessiontree.LeasePolicy
	now         func() time.Time
}

func WithLeasePolicy(policy sessiontree.LeasePolicy) Option {
	return func(opts *options) {
		opts.leasePolicy = policy
	}
}

func WithAuthorityClock(now func() time.Time) Option {
	return func(opts *options) {
		opts.now = now
	}
}

type Store struct {
	db               *sql.DB
	path             string
	writerAdmission  *WriterAdmission
	leasePolicy      sessiontree.LeasePolicy
	now              func() time.Time
	approvalSignalMu sync.Mutex
	approvalSignals  map[string]chan struct{}
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
	configured := options{leasePolicy: sessiontree.DefaultLeasePolicy, now: time.Now}
	for _, opt := range opts {
		if opt != nil {
			opt(&configured)
		}
	}
	if err := configured.leasePolicy.Validate(); err != nil {
		return nil, err
	}
	if configured.now == nil {
		return nil, errors.New("sqlite store authority clock is required")
	}
	if path != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return nil, err
		}
	}
	writerAdmission, err := NewWriterAdmission(path)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open(driverName, path)
	if err != nil {
		writerAdmission.Close()
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	store := &Store{db: db, path: path, writerAdmission: writerAdmission, leasePolicy: configured.leasePolicy, now: configured.now, approvalSignals: map[string]chan struct{}{}}
	if err := store.init(context.Background()); err != nil {
		_ = db.Close()
		writerAdmission.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	s.approvalSignalMu.Lock()
	for approvalID, waiter := range s.approvalSignals {
		close(waiter)
		delete(s.approvalSignals, approvalID)
	}
	s.approvalSignalMu.Unlock()
	err := s.db.Close()
	s.writerAdmission.Close()
	return err
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
		"PRAGMA busy_timeout = 0",
	} {
		if _, err := s.db.ExecContext(ctx, pragma); err != nil {
			return err
		}
	}
	if err := s.withImmediate(ctx, func(tx sqlRunner) error {
		if err := ensureSchema(ctx, tx, s.leasePolicy); err != nil {
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
	}); err != nil {
		return err
	}
	releaseWriter, err := s.writerAdmission.Reserve(ctx)
	if err != nil {
		return err
	}
	defer releaseWriter()
	for _, pragma := range []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = FULL",
	} {
		if _, err := s.db.ExecContext(ctx, pragma); err != nil {
			return err
		}
	}
	return nil
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
	releaseWriter, err := s.writerAdmission.Reserve(ctx)
	if err != nil {
		return err
	}
	defer releaseWriter()

	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	for _, pragma := range []string{
		"PRAGMA foreign_keys = ON",
		"PRAGMA busy_timeout = 0",
	} {
		if _, err := conn.ExecContext(ctx, pragma); err != nil {
			return err
		}
	}
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		// A cancelled statement can race with a successful BEGIN at the driver
		// boundary. Always reset the connection before returning it to the pool.
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
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
	err := s.withImmediate(ctx, func(tx sqlRunner) error {
		var err error
		meta, err = createThreadWithRunner(ctx, tx, meta)
		return err
	})
	return meta, err
}

func createThreadWithRunner(ctx context.Context, tx sqlRunner, meta sessiontree.ThreadMeta) (sessiontree.ThreadMeta, error) {
	now := meta.CreatedAt
	if now.IsZero() {
		now = time.Now()
	}
	if meta.ID == "" {
		for {
			meta.ID = "thread-" + strconv.FormatInt(time.Now().UnixNano(), 10)
			ok, err := threadExists(ctx, tx, meta.ID)
			if err != nil {
				return sessiontree.ThreadMeta{}, err
			}
			if !ok {
				break
			}
		}
	} else if ok, err := threadExists(ctx, tx, meta.ID); err != nil {
		return sessiontree.ThreadMeta{}, err
	} else if ok {
		return sessiontree.ThreadMeta{}, sessiontree.ErrThreadExists
	}
	if _, err := loadThreadTombstone(ctx, tx, meta.ID); err == nil {
		return sessiontree.ThreadMeta{}, sessiontree.ErrThreadDeleted
	} else if !errors.Is(err, sql.ErrNoRows) && !errors.Is(err, sessiontree.ErrThreadNotFound) {
		return sessiontree.ThreadMeta{}, err
	}
	if err := rejectClaimedThreadAuthorities(ctx, tx, meta.ID, meta.ParentThreadID); err != nil {
		return sessiontree.ThreadMeta{}, err
	}
	meta.CreatedAt = now
	meta.UpdatedAt = now
	if err := sessiontree.ValidateThreadMetaAuthority(meta); err != nil {
		return sessiontree.ThreadMeta{}, err
	}
	if parentID := strings.TrimSpace(meta.ParentThreadID); parentID != "" {
		parent, err := loadThread(ctx, tx, parentID)
		if errors.Is(err, sessiontree.ErrThreadNotFound) {
			return sessiontree.ThreadMeta{}, fmt.Errorf("%w: parent thread %q", sessiontree.ErrInvalidThreadAuthority, parentID)
		}
		if err != nil {
			return sessiontree.ThreadMeta{}, err
		}
		if err := rejectSQLiteThreadWriteLifecycle(parent); err != nil {
			return sessiontree.ThreadMeta{}, err
		}
	}
	if err := insertThread(ctx, tx, meta); err != nil {
		return sessiontree.ThreadMeta{}, err
	}
	return meta, nil
}

func (s *Store) CreateThreadWithInitialEntry(ctx context.Context, meta sessiontree.ThreadMeta, initial sessiontree.Entry) (sessiontree.ThreadMeta, sessiontree.Entry, error) {
	var created sessiontree.ThreadMeta
	var saved sessiontree.Entry
	err := s.withImmediate(ctx, func(tx sqlRunner) error {
		var err error
		created, err = createThreadWithRunner(ctx, tx, meta)
		if err != nil {
			return err
		}
		initial.ThreadID = created.ID
		saved, err = appendWithRunner(ctx, tx, initial, sessiontree.AppendOptions{Now: created.CreatedAt}, s.now)
		if err != nil {
			return err
		}
		created, err = loadThread(ctx, tx, created.ID)
		return err
	})
	return created, saved, err
}

func (s *Store) AcquireTurnLease(ctx context.Context, request sessiontree.TurnLease) (sessiontree.TurnLease, error) {
	purpose, err := request.Purpose.Normalize()
	if err != nil {
		return sessiontree.TurnLease{}, err
	}
	if strings.TrimSpace(request.ThreadID) == "" || strings.TrimSpace(request.OwnerID) == "" {
		return sessiontree.TurnLease{}, errors.New("lease thread and owner are required")
	}
	if purpose == sessiontree.TurnLeasePurposeTurn {
		if strings.TrimSpace(request.TurnID) == "" || strings.TrimSpace(request.MutationID) != "" || strings.TrimSpace(request.MutationKind) != "" {
			return sessiontree.TurnLease{}, errors.New("turn lease request requires only turn identity")
		}
	} else if strings.TrimSpace(request.TurnID) != "" || strings.TrimSpace(request.MutationID) == "" || strings.TrimSpace(request.MutationKind) == "" {
		return sessiontree.TurnLease{}, errors.New("mutation lease request requires only mutation identity and kind")
	}
	var proof sessiontree.TurnLease
	err = s.withImmediate(ctx, func(tx sqlRunner) error {
		meta, err := loadThread(ctx, tx, request.ThreadID)
		if err != nil {
			return err
		}
		if err := rejectSQLiteThreadWriteLifecycle(meta); err != nil {
			return err
		}
		if _, claimed, err := loadThreadAuthorityClaim(ctx, tx, request.ThreadID); err != nil {
			return err
		} else if claimed {
			return sessiontree.ErrThreadAuthorityBusy
		}
		if _, active, err := loadTurnLease(ctx, tx, request.ThreadID); err != nil {
			return err
		} else if active {
			return sessiontree.ErrActiveTurn
		}
		var generation int64
		if err := tx.QueryRowContext(ctx, `SELECT lease_generation FROM threads WHERE id = ?`, request.ThreadID).Scan(&generation); err != nil {
			return err
		}
		generation++
		if _, err := tx.ExecContext(ctx, `UPDATE threads SET lease_generation = ? WHERE id = ?`, generation, request.ThreadID); err != nil {
			return err
		}
		now := s.now().UTC()
		proof = sessiontree.TurnLease{
			ThreadID:     strings.TrimSpace(request.ThreadID),
			Purpose:      purpose,
			TurnID:       strings.TrimSpace(request.TurnID),
			MutationID:   strings.TrimSpace(request.MutationID),
			MutationKind: strings.TrimSpace(request.MutationKind),
			OwnerID:      strings.TrimSpace(request.OwnerID),
			Generation:   generation,
			Heartbeat:    0,
			AcquiredAt:   now,
			RenewedAt:    now,
			ExpiresAt:    now.Add(s.leasePolicy.TTL),
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO active_turn_leases(
			thread_id, purpose, turn_id, mutation_id, mutation_kind, owner_id, generation, heartbeat, acquired_at, renewed_at, expires_at
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			proof.ThreadID, string(proof.Purpose), proof.TurnID, proof.MutationID, proof.MutationKind, proof.OwnerID,
			proof.Generation, proof.Heartbeat, formatTime(proof.AcquiredAt), formatTime(proof.RenewedAt), formatTime(proof.ExpiresAt))
		if isConstraintError(err) {
			return sessiontree.ErrActiveTurn
		}
		return err
	})
	return proof, err
}

func (s *Store) RenewTurnLease(ctx context.Context, proof sessiontree.TurnLease) (sessiontree.TurnLease, error) {
	var renewed sessiontree.TurnLease
	err := s.withImmediate(ctx, func(tx sqlRunner) error {
		active, ok, err := loadTurnLease(ctx, tx, proof.ThreadID)
		if err != nil {
			return err
		}
		if !ok || !sessiontree.SameTurnLease(active, proof) {
			return sessiontree.ErrStaleAuthority
		}
		now := s.now().UTC()
		if !active.Fresh(now) {
			return sessiontree.ErrStaleAuthority
		}
		active.Heartbeat++
		active.RenewedAt = now
		active.ExpiresAt = now.Add(s.leasePolicy.TTL)
		if _, err := tx.ExecContext(ctx, `UPDATE active_turn_leases SET heartbeat = ?, renewed_at = ?, expires_at = ?
			WHERE thread_id = ? AND generation = ?`,
			active.Heartbeat, formatTime(active.RenewedAt), formatTime(active.ExpiresAt), active.ThreadID, active.Generation); err != nil {
			return err
		}
		if active.Purpose == sessiontree.TurnLeasePurposeTurn {
			var admissionCount int
			if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM turn_admissions WHERE thread_id = ? AND turn_id = ?`, proof.ThreadID, proof.TurnID).Scan(&admissionCount); err != nil {
				return err
			}
			updated, err := tx.ExecContext(ctx, `UPDATE turn_admissions SET heartbeat = ?, renewed_at = ?, expires_at = ?
				WHERE thread_id = ? AND turn_id = ? AND owner_id = ? AND generation = ? AND heartbeat = ?`,
				active.Heartbeat, formatTime(active.RenewedAt), formatTime(active.ExpiresAt),
				proof.ThreadID, proof.TurnID, proof.OwnerID, proof.Generation, proof.Heartbeat)
			if err != nil {
				return err
			}
			if count, err := updated.RowsAffected(); err != nil {
				return err
			} else if admissionCount == 1 && count != 1 {
				return sessiontree.ErrAuthorityCorrupt
			}
		}
		if active.Purpose == sessiontree.TurnLeasePurposeMutation && active.MutationKind == sessiontree.CompactionMutationKind {
			updated, err := tx.ExecContext(ctx, `UPDATE compaction_operations SET lease_heartbeat = ?, lease_renewed_at = ?, lease_expires_at = ?, updated_at = ?
				WHERE request_id = ? AND thread_id = ? AND state = 'prepared' AND lease_owner_id = ? AND lease_generation = ? AND lease_heartbeat = ?`,
				active.Heartbeat, formatTime(active.RenewedAt), formatTime(active.ExpiresAt), formatTime(now),
				active.MutationID, active.ThreadID, proof.OwnerID, proof.Generation, proof.Heartbeat)
			if err != nil {
				return err
			}
			if count, err := updated.RowsAffected(); err != nil {
				return err
			} else if count != 1 {
				return sessiontree.ErrAuthorityCorrupt
			}
		}
		renewed = active
		return nil
	})
	return renewed, err
}

func (s *Store) ReleaseTurnLease(ctx context.Context, proof sessiontree.TurnLease) error {
	return s.withImmediate(ctx, func(tx sqlRunner) error {
		active, ok, err := loadTurnLease(ctx, tx, proof.ThreadID)
		if err != nil {
			return err
		}
		if !ok || !sessiontree.SameTurnLease(active, proof) {
			return sessiontree.ErrStaleAuthority
		}
		if active.Purpose == sessiontree.TurnLeasePurposeMutation && active.MutationKind == sessiontree.CompactionMutationKind {
			var count int
			if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM compaction_operations WHERE request_id = ? AND state = 'prepared'`, active.MutationID).Scan(&count); err != nil {
				return err
			}
			if count != 0 {
				return sessiontree.ErrInvalidThreadAuthority
			}
		}
		_, err = tx.ExecContext(ctx, `DELETE FROM active_turn_leases WHERE thread_id = ? AND generation = ?`, proof.ThreadID, proof.Generation)
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

func (s *Store) AuthorityLeasePolicy() sessiontree.LeasePolicy {
	return s.leasePolicy
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
			task_name, task_description, agent_path, host_profile_ref, fork_mode, lifecycle, close_operation_id, archived,
			title, title_status, title_source, title_updated_at, title_error, title_generation, title_token,
			created_at, updated_at, last_viewed_at
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
		if !sessiontree.SameThreadTitleState(current, meta) {
			return sessiontree.ErrRequestConflict
		}
		if err := rejectSQLiteThreadWriteLifecycle(current); err != nil {
			return err
		}
		if err := rejectClaimedThreadAuthorities(ctx, tx, meta.ID); err != nil {
			return err
		}
		active, _, err := loadTurnLease(ctx, tx, meta.ID)
		if err != nil {
			return err
		}
		if err := validateSQLiteTurnLeaseMutation(ctx, meta.ID, "", active, s.now().UTC()); err != nil {
			return err
		}
		return updateThread(ctx, tx, meta)
	})
}

func (s *Store) Append(ctx context.Context, entry sessiontree.Entry, opts sessiontree.AppendOptions) (sessiontree.Entry, error) {
	var saved sessiontree.Entry
	err := s.withImmediate(ctx, func(tx sqlRunner) error {
		var err error
		saved, err = appendWithRunner(ctx, tx, entry, opts, s.now)
		return err
	})
	return saved, err
}

func appendWithRunner(ctx context.Context, tx sqlRunner, entry sessiontree.Entry, opts sessiontree.AppendOptions, now func() time.Time) (sessiontree.Entry, error) {
	meta, err := loadThread(ctx, tx, entry.ThreadID)
	if err != nil {
		return sessiontree.Entry{}, err
	}
	if err := rejectClaimedThreadAuthorities(ctx, tx, entry.ThreadID); err != nil {
		return sessiontree.Entry{}, err
	}
	if err := rejectSQLiteThreadWriteLifecycle(meta); err != nil {
		return sessiontree.Entry{}, err
	}
	activeLease, _, err := loadTurnLease(ctx, tx, entry.ThreadID)
	if err != nil {
		return sessiontree.Entry{}, err
	}
	if err := validateSQLiteTurnLeaseMutation(ctx, entry.ThreadID, entry.TurnID, activeLease, now().UTC()); err != nil {
		return sessiontree.Entry{}, err
	}
	if err := sessiontree.ValidateNewEntryMessageAttachments(entry); err != nil {
		return sessiontree.Entry{}, err
	}
	if err := sessiontree.ValidateEntryMessageReferences(entry); err != nil {
		return sessiontree.Entry{}, err
	}
	if entry.Type == sessiontree.EntryTurnMarker && entry.TurnStatus == sessiontree.TurnStarted {
		runID := strings.TrimSpace(entry.Metadata["run_id"])
		if runID == "" {
			return sessiontree.Entry{}, fmt.Errorf("%w: started turn requires run identity", sessiontree.ErrInvalidThreadAuthority)
		}
		exists, err := turnStartedExists(ctx, tx, entry.ThreadID, entry.TurnID)
		if err != nil {
			return sessiontree.Entry{}, err
		}
		if exists {
			return sessiontree.Entry{}, sessiontree.ErrRequestConflict
		}
		entry = cloneEntry(entry)
		entry.Metadata["run_id"] = runID
	}
	if opts.ParentID != "" {
		entry.ParentID = opts.ParentID
	} else if entry.ParentID == "" {
		entry.ParentID = meta.LeafID
	}
	if entry.ParentID != "" {
		ok, err := entryExists(ctx, tx, entry.ThreadID, entry.ParentID)
		if err != nil {
			return sessiontree.Entry{}, err
		}
		if !ok {
			return sessiontree.Entry{}, sessiontree.ErrInvalidParent
		}
	}
	if opts.ID != "" {
		entry.ID = opts.ID
	}
	if entry.ID == "" {
		id, err := nextEntryID(ctx, tx, entry.ThreadID)
		if err != nil {
			return sessiontree.Entry{}, err
		}
		entry.ID = id
	} else if ok, err := entryExists(ctx, tx, entry.ThreadID, entry.ID); err != nil {
		return sessiontree.Entry{}, err
	} else if ok {
		return sessiontree.Entry{}, fmt.Errorf("session tree entry id already exists: %s", entry.ID)
	}
	if entry.CreatedAt.IsZero() {
		entry.CreatedAt = opts.Now
	}
	if entry.CreatedAt.IsZero() {
		entry.CreatedAt = time.Now()
	}
	entry.PathDepth, err = resolveEntryPathDepth(ctx, tx, entry)
	if err != nil {
		return sessiontree.Entry{}, err
	}
	entry = sessiontree.PrepareEntry(entry)
	ordinal, err := nextOrdinal(ctx, tx, entry.ThreadID)
	if err != nil {
		return sessiontree.Entry{}, err
	}
	if err := insertEntry(ctx, tx, entry, ordinal, false); err != nil {
		return sessiontree.Entry{}, err
	}
	meta.LeafID = entry.ID
	meta.UpdatedAt = entry.CreatedAt
	if err := updateThread(ctx, tx, meta); err != nil {
		return sessiontree.Entry{}, err
	}
	return cloneEntry(entry), nil
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

func (s *Store) CanonicalTurnEntries(ctx context.Context, threadID, turnID, runID string) ([]sessiontree.Entry, bool, error) {
	threadID = strings.TrimSpace(threadID)
	turnID = strings.TrimSpace(turnID)
	runID = strings.TrimSpace(runID)
	var entries []sessiontree.Entry
	found := false
	err := s.withRead(ctx, func(q sqlRunner) error {
		if _, err := loadThread(ctx, q, threadID); err != nil {
			if errors.Is(err, sessiontree.ErrThreadNotFound) {
				if _, tombstoneErr := loadThreadTombstone(ctx, q, threadID); tombstoneErr == nil {
					return sessiontree.ErrThreadDeleted
				} else if !errors.Is(tombstoneErr, sql.ErrNoRows) {
					return tombstoneErr
				}
			}
			return err
		}
		rows, err := q.QueryContext(ctx, `SELECT `+entryColumns+` FROM entries
			WHERE thread_id = ? AND turn_id = ? ORDER BY ordinal`, threadID, turnID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			entry, err := scanEntry(rows)
			if err != nil {
				return err
			}
			found = true
			if err := sessiontree.ValidateEntryIntegrity(entry); err != nil {
				return err
			}
			entries = append(entries, entry)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if len(entries) == 0 {
			return nil
		}
		entries = sessiontree.CanonicalTurnEntriesForRead(entries)
		return sessiontree.ValidateCanonicalTurnEntries(entries, threadID, turnID, runID)
	})
	return entries, found, err
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
		if err := sessiontree.ValidateEntryIntegrity(entry); err != nil {
			return nil, err
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
	var expectedNewestOrdinal int64
	if beforeEntryID != "" {
		cursor, err := loadEntry(ctx, s.db, threadID, beforeEntryID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return sessiontree.PathPage{}, sessiontree.ErrEntryNotFound
			}
			return sessiontree.PathPage{}, err
		}
		if cursor.ThreadID != threadID || cursor.PathDepth <= 0 {
			return sessiontree.PathPage{}, sessiontree.ErrAuthorityCorrupt
		}
		if err := sessiontree.ValidateEntryIntegrity(cursor); err != nil {
			return sessiontree.PathPage{}, err
		}
		leafID = cursor.ParentID
		expectedNewestOrdinal = cursor.PathDepth - 1
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
		if entry.ThreadID != threadID || entry.PathDepth <= 0 {
			return sessiontree.PathPage{}, sessiontree.ErrAuthorityCorrupt
		}
		if err := sessiontree.ValidateEntryIntegrity(entry); err != nil {
			return sessiontree.PathPage{}, err
		}
		if len(entries) > 0 {
			newer := entries[len(entries)-1]
			if newer.ParentID != entry.ID || newer.PathDepth != entry.PathDepth+1 {
				return sessiontree.PathPage{}, sessiontree.ErrInvalidParent
			}
		}
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
	if newestOrdinal != entries[0].PathDepth {
		return sessiontree.PathPage{}, sessiontree.ErrInvalidParent
	}
	if expectedNewestOrdinal != 0 && newestOrdinal != expectedNewestOrdinal {
		return sessiontree.PathPage{}, sessiontree.ErrInvalidParent
	}
	oldest := entries[len(entries)-1]
	if newestOrdinal <= int64(len(entries)) && (oldest.ParentID != "" || oldest.PathDepth != 1) {
		return sessiontree.PathPage{}, sessiontree.ErrInvalidParent
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
		if err := rejectClaimedThreadAuthorities(ctx, tx, threadID); err != nil {
			return err
		}
		if err := rejectSQLiteThreadWriteLifecycle(meta); err != nil {
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
	if strings.TrimSpace(opts.OperationID) != "" || strings.TrimSpace(opts.OperationNodeID) != "" {
		return sessiontree.ThreadMeta{}, sessiontree.ErrInvalidThreadAuthority
	}
	var forked sessiontree.ThreadMeta
	err := s.withImmediate(ctx, func(tx sqlRunner) error {
		var err error
		forked, err = forkWithRunner(ctx, tx, opts)
		return err
	})
	return forked, err
}

func forkWithRunner(ctx context.Context, tx sqlRunner, opts sessiontree.ForkOptions) (sessiontree.ThreadMeta, error) {
	opts = snapshotForkIdentityMaps(opts)
	if opts.Position == "" {
		opts.Position = sessiontree.ForkAt
	}
	sourceMeta, err := loadThread(ctx, tx, opts.SourceThreadID)
	if err != nil {
		return sessiontree.ThreadMeta{}, err
	}
	if err := requireForkAuthorityClaims(ctx, tx, opts.OperationID, opts.SourceThreadID); err != nil {
		return sessiontree.ThreadMeta{}, err
	}
	if sourceMeta.IsClosed() && strings.TrimSpace(opts.OperationID) == "" {
		return sessiontree.ThreadMeta{}, sessiontree.ErrThreadClosed
	}
	targetID := opts.EntryID
	if targetID == "" && !opts.EntryIDPinned {
		targetID = sourceMeta.LeafID
	}
	if opts.Position == sessiontree.ForkBefore {
		entry, err := loadEntry(ctx, tx, opts.SourceThreadID, targetID)
		if errors.Is(err, sql.ErrNoRows) {
			return sessiontree.ThreadMeta{}, sessiontree.ErrEntryNotFound
		}
		if err != nil {
			return sessiontree.ThreadMeta{}, err
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
			return sessiontree.ThreadMeta{}, err
		}
	} else if ok, err := threadExists(ctx, tx, newID); err != nil {
		return sessiontree.ThreadMeta{}, err
	} else if ok {
		if err := requireForkAuthorityClaims(ctx, tx, opts.OperationID, newID); err != nil {
			return sessiontree.ThreadMeta{}, err
		}
		existing, err := loadThread(ctx, tx, newID)
		if err != nil {
			return sessiontree.ThreadMeta{}, err
		}
		if opts.OperationID != "" && opts.OperationNodeID != "" &&
			existing.ForkOperationID == opts.OperationID &&
			existing.ForkOperationNodeID == opts.OperationNodeID &&
			existing.ForkedFromThreadID == opts.SourceThreadID &&
			existing.ForkedFromEntryID == targetID &&
			sessiontree.MatchesForkDestinationMeta(existing, opts.DestinationMeta) {
			return existing, nil
		}
		if opts.OperationID != "" || opts.OperationNodeID != "" {
			return sessiontree.ThreadMeta{}, sessiontree.ErrForkDestinationConflict
		}
		return sessiontree.ThreadMeta{}, sessiontree.ErrThreadExists
	}
	if err := requireForkAuthorityClaims(ctx, tx, opts.OperationID, newID); err != nil {
		return sessiontree.ThreadMeta{}, err
	}
	path, err := pathWithRunner(ctx, tx, opts.SourceThreadID, targetID)
	if err != nil {
		return sessiontree.ThreadMeta{}, err
	}
	if err := validateSQLiteForkRetryAdmissions(ctx, tx, opts.SourceThreadID, path); err != nil {
		return sessiontree.ThreadMeta{}, err
	}
	closure := artifact.CloneClosure(opts.ArtifactClosure)
	if artifact.IsZeroClosure(closure) && strings.TrimSpace(opts.OperationID) == "" {
		closure, err = sqliteArtifactClosure(ctx, tx, opts.SourceThreadID, newID, path)
		if err != nil {
			return sessiontree.ThreadMeta{}, err
		}
	}
	if err := validateSQLiteArtifactClosure(ctx, tx, opts.SourceThreadID, newID, path, closure); err != nil {
		return sessiontree.ThreadMeta{}, err
	}
	meta := sessiontree.ThreadMeta{ID: newID, ForkedFromThreadID: opts.SourceThreadID, ForkedFromEntryID: targetID, ForkOperationID: opts.OperationID, ForkOperationNodeID: opts.OperationNodeID, CreatedAt: now, UpdatedAt: now}
	applyForkDestinationMeta(&meta, opts.DestinationMeta)
	if err := sessiontree.ValidateThreadMetaAuthority(meta); err != nil {
		return sessiontree.ThreadMeta{}, err
	}
	if parentID := strings.TrimSpace(meta.ParentThreadID); parentID != "" {
		if err := requireForkAuthorityClaims(ctx, tx, opts.OperationID, parentID); err != nil {
			return sessiontree.ThreadMeta{}, err
		}
		parent, err := loadThread(ctx, tx, parentID)
		if errors.Is(err, sessiontree.ErrThreadNotFound) {
			return sessiontree.ThreadMeta{}, fmt.Errorf("%w: parent thread %q", sessiontree.ErrInvalidThreadAuthority, parentID)
		}
		if err != nil {
			return sessiontree.ThreadMeta{}, err
		}
		if parent.IsClosed() && strings.TrimSpace(opts.OperationID) == "" {
			return sessiontree.ThreadMeta{}, sessiontree.ErrThreadClosed
		}
	}
	if err := insertThread(ctx, tx, meta); err != nil {
		return sessiontree.ThreadMeta{}, err
	}
	oldToNew := map[string]string{"": ""}
	retryTargetEntryIDs := make(map[string]struct{})
	for _, entry := range path {
		if entry.Type != sessiontree.EntryTurnMarker || entry.TurnStatus != sessiontree.TurnStarted {
			continue
		}
		retrySource, err := sessiontree.CanonicalTurnRetrySourceForStartedEntry(entry)
		if err != nil {
			return sessiontree.ThreadMeta{}, err
		}
		if retrySource != nil {
			retryTargetEntryIDs[retrySource.EntryID] = struct{}{}
		}
	}
	startedTurnIDs := map[string]struct{}{}
	type forkedRetryAdmission struct {
		sourceStarted sessiontree.Entry
		destination   sessiontree.Entry
	}
	var retryAdmissions []forkedRetryAdmission
	for _, entry := range path {
		next := cloneEntry(entry)
		nextID, err := nextEntryID(ctx, tx, newID)
		if err != nil {
			return sessiontree.ThreadMeta{}, err
		}
		next.ID = nextID
		next.ThreadID = newID
		next.ParentID = oldToNew[entry.ParentID]
		next.TurnID = rewriteForkID(next.TurnID, opts.TurnIDMap)
		next.FirstKeptEntryID = oldToNew[entry.FirstKeptEntryID]
		next.CompactedThroughEntryID = oldToNew[entry.CompactedThroughEntryID]
		next.KeptUserEntryIDs = rewriteEntryIDs(entry.KeptUserEntryIDs, oldToNew)
		next.Metadata = rewriteForkMetadata(next.Metadata, oldToNew, opts.TurnIDMap, opts.RunIDMap)
		expectedID := next.ID
		expectedThreadID := next.ThreadID
		expectedParentID := next.ParentID
		expectedTurnID := next.TurnID
		expectedRunID := next.Metadata["run_id"]
		expectedRetrySourceTurnID := next.Metadata[sessiontree.RetrySourceTurnIDMetadataKey]
		expectedRetrySourceEntryID := next.Metadata[sessiontree.RetrySourceEntryIDMetadataKey]
		sourceStarted := entry.Type == sessiontree.EntryTurnMarker && entry.TurnStatus == sessiontree.TurnStarted
		_, sourceTarget := retryTargetEntryIDs[entry.ID]
		if opts.RewriteEntry != nil {
			next, err = opts.RewriteEntry(next, sessiontree.ForkEntryIdentity{
				SourceThreadID:      opts.SourceThreadID,
				DestinationThreadID: newID,
				TurnIDMap:           cloneStringMapSQLite(opts.TurnIDMap),
				RunIDMap:            cloneStringMapSQLite(opts.RunIDMap),
			})
			if err != nil {
				return sessiontree.ThreadMeta{}, err
			}
		}
		destinationStarted := next.Type == sessiontree.EntryTurnMarker && next.TurnStatus == sessiontree.TurnStarted
		if sourceStarted != destinationStarted {
			return sessiontree.ThreadMeta{}, sessiontree.ErrAuthorityCorrupt
		}
		if (sourceStarted || sourceTarget) &&
			(next.ID != expectedID || next.ThreadID != expectedThreadID || next.ParentID != expectedParentID || next.TurnID != expectedTurnID) {
			return sessiontree.ThreadMeta{}, sessiontree.ErrAuthorityCorrupt
		}
		if sourceTarget && (next.Type != entry.Type || next.TurnStatus != entry.TurnStatus ||
			next.Metadata["run_id"] != expectedRunID ||
			next.Metadata[sessiontree.RetrySourceTurnIDMetadataKey] != expectedRetrySourceTurnID ||
			next.Metadata[sessiontree.RetrySourceEntryIDMetadataKey] != expectedRetrySourceEntryID) {
			return sessiontree.ThreadMeta{}, sessiontree.ErrAuthorityCorrupt
		}
		if sourceStarted {
			if next.Type != sessiontree.EntryTurnMarker || next.TurnStatus != sessiontree.TurnStarted {
				return sessiontree.ThreadMeta{}, sessiontree.ErrAuthorityCorrupt
			}
			sourceRetry, err := sessiontree.CanonicalTurnRetrySourceForStartedEntry(entry)
			if err != nil {
				return sessiontree.ThreadMeta{}, err
			}
			destinationRetry, err := sessiontree.CanonicalTurnRetrySourceForStartedEntry(next)
			if err != nil {
				return sessiontree.ThreadMeta{}, err
			}
			if (sourceRetry == nil) != (destinationRetry == nil) {
				return sessiontree.ThreadMeta{}, sessiontree.ErrAuthorityCorrupt
			}
			if sourceRetry != nil && (destinationRetry.EntryID != oldToNew[sourceRetry.EntryID] ||
				destinationRetry.TurnID != rewriteForkID(sourceRetry.TurnID, opts.TurnIDMap)) {
				return sessiontree.ThreadMeta{}, sessiontree.ErrAuthorityCorrupt
			}
			turnID := strings.TrimSpace(next.TurnID)
			if turnID == "" {
				return sessiontree.ThreadMeta{}, sessiontree.ErrAuthorityCorrupt
			}
			if _, duplicate := startedTurnIDs[turnID]; duplicate {
				return sessiontree.ThreadMeta{}, sessiontree.ErrAuthorityCorrupt
			}
			if expectedRunID == "" || next.Metadata["run_id"] != expectedRunID {
				return sessiontree.ThreadMeta{}, sessiontree.ErrAuthorityCorrupt
			}
			startedTurnIDs[turnID] = struct{}{}
		}
		next.CreatedAt = now
		next = sessiontree.PrepareEntry(next)
		ordinal, err := nextOrdinal(ctx, tx, newID)
		if err != nil {
			return sessiontree.ThreadMeta{}, err
		}
		if err := insertEntry(ctx, tx, next, ordinal, false); err != nil {
			return sessiontree.ThreadMeta{}, err
		}
		oldToNew[entry.ID] = next.ID
		if next.Type == sessiontree.EntryTurnMarker && next.TurnStatus == sessiontree.TurnStarted {
			retrySource, err := sessiontree.CanonicalTurnRetrySourceForStartedEntry(next)
			if err != nil {
				return sessiontree.ThreadMeta{}, err
			}
			if retrySource != nil {
				retryAdmissions = append(retryAdmissions, forkedRetryAdmission{sourceStarted: entry, destination: next})
			}
		}
		meta.LeafID = next.ID
	}
	destinationPath, err := pathWithRunner(ctx, tx, newID, meta.LeafID)
	if err != nil {
		return sessiontree.ThreadMeta{}, err
	}
	if err := sessiontree.ValidateForkRetryAuthorityPath(destinationPath, newID); err != nil {
		return sessiontree.ThreadMeta{}, err
	}
	boundaryID, err := nextEntryID(ctx, tx, newID)
	if err != nil {
		return sessiontree.ThreadMeta{}, err
	}
	boundary, err := sessiontree.PrepareBranchBoundaryEntry(destinationPath, newID, meta.LeafID, boundaryID, "fork", now)
	if err != nil {
		return sessiontree.ThreadMeta{}, err
	}
	if boundary.ID != "" {
		ordinal, err := nextOrdinal(ctx, tx, newID)
		if err != nil {
			return sessiontree.ThreadMeta{}, err
		}
		if err := insertEntry(ctx, tx, boundary, ordinal, true); err != nil {
			return sessiontree.ThreadMeta{}, err
		}
		meta.LeafID = boundary.ID
	}
	finalPath, err := pathWithRunner(ctx, tx, newID, meta.LeafID)
	if err != nil {
		return sessiontree.ThreadMeta{}, err
	}
	if err := sessiontree.ValidateForkRetryAuthorityPath(finalPath, newID); err != nil {
		return sessiontree.ThreadMeta{}, err
	}
	for _, item := range retryAdmissions {
		if err := copySQLiteForkRetryAdmission(ctx, tx, opts.SourceThreadID, newID, item.sourceStarted, item.destination, oldToNew, now); err != nil {
			return sessiontree.ThreadMeta{}, err
		}
	}
	if err := copySQLiteArtifactClosure(ctx, tx, closure, oldToNew, now); err != nil {
		return sessiontree.ThreadMeta{}, err
	}
	if err := updateThread(ctx, tx, meta); err != nil {
		return sessiontree.ThreadMeta{}, err
	}
	if todo, ok, err := loadAgentTodoState(ctx, tx, opts.SourceThreadID); err != nil {
		return sessiontree.ThreadMeta{}, err
	} else if ok {
		todo.ThreadID = newID
		todo.UpdatedByTurnID = rewriteForkID(todo.UpdatedByTurnID, opts.TurnIDMap)
		todo.UpdatedByRunID = rewriteForkID(todo.UpdatedByRunID, opts.RunIDMap)
		if err := putAgentTodoState(ctx, tx, todo); err != nil {
			return sessiontree.ThreadMeta{}, err
		}
	}
	return meta, nil
}

func validateSQLiteForkRetryAdmissions(ctx context.Context, q sqlRunner, threadID string, path []sessiontree.Entry) error {
	for _, entry := range path {
		if entry.Type != sessiontree.EntryTurnMarker || entry.TurnStatus != sessiontree.TurnStarted {
			continue
		}
		source, err := sessiontree.CanonicalTurnRetrySourceForStartedEntry(entry)
		if err != nil {
			return err
		}
		if source == nil {
			continue
		}
		eligible, err := sqliteRetrySourceHasRetryEligibleDurableInput(
			ctx, q, threadID, entry.TurnID, strings.TrimSpace(entry.Metadata["run_id"]), entry.ID, *source,
		)
		if err != nil {
			return err
		}
		if !eligible {
			return sessiontree.ErrAuthorityCorrupt
		}
	}
	return nil
}

func copySQLiteForkRetryAdmission(ctx context.Context, tx sqlRunner, sourceThreadID, destinationThreadID string, sourceStarted, destinationStarted sessiontree.Entry, entryIDs map[string]string, now time.Time) error {
	sourceAdmission, found, err := loadSQLiteTurnAdmission(ctx, tx, sourceThreadID, sourceStarted.TurnID)
	if err != nil {
		return err
	}
	sourceRetry, err := sessiontree.CanonicalTurnRetrySourceForStartedEntry(sourceStarted)
	if err != nil {
		return err
	}
	if sourceRetry == nil {
		return sessiontree.ErrAuthorityCorrupt
	}
	destinationRetry, err := sessiontree.CanonicalTurnRetrySourceForStartedEntry(destinationStarted)
	if err != nil {
		return err
	}
	if destinationRetry == nil {
		return sessiontree.ErrAuthorityCorrupt
	}
	mappedSourceEntryID := entryIDs[sourceRetry.EntryID]
	if !found ||
		sourceAdmission.ThreadID != sourceThreadID || sourceAdmission.TurnID != sourceStarted.TurnID ||
		sourceAdmission.RunID != strings.TrimSpace(sourceStarted.Metadata["run_id"]) || sourceAdmission.TurnStartedID != sourceStarted.ID ||
		sourceAdmission.UserMessageID != "" || sourceAdmission.BaseLeafID != sourceRetry.EntryID ||
		mappedSourceEntryID == "" || destinationRetry.EntryID != mappedSourceEntryID {
		return sessiontree.ErrAuthorityCorrupt
	}
	requestFingerprint, err := sessiontree.TurnAdmissionRequestFingerprint(sessiontree.AdmitTurnRequest{
		ThreadID: destinationThreadID, TurnID: destinationStarted.TurnID, RunID: destinationStarted.Metadata["run_id"],
		RetrySourceTurnID: destinationRetry.TurnID, RetrySourceEntryID: destinationRetry.EntryID,
	})
	if err != nil {
		return err
	}
	ownerID := sourceAdmission.Lease.OwnerID
	if strings.TrimSpace(ownerID) == "" {
		ownerID = "fork-history"
	}
	generation := sourceAdmission.Lease.Generation
	if generation <= 0 {
		generation = 1
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO turn_admissions(
		thread_id, turn_id, run_id, request_fingerprint, owner_id, generation, heartbeat,
		acquired_at, renewed_at, expires_at, boundary_terminal_id, turn_started_id, user_message_id, base_leaf_id
	) VALUES(?, ?, ?, ?, ?, ?, 0, ?, ?, ?, NULL, ?, NULL, ?)`,
		destinationThreadID, destinationStarted.TurnID, destinationStarted.Metadata["run_id"], requestFingerprint,
		ownerID, generation, formatTime(now), formatTime(now), formatTime(now.Add(time.Minute)), destinationStarted.ID, destinationRetry.EntryID); err != nil {
		return err
	}
	finish, finished, err := loadSQLiteTurnFinish(ctx, tx, sourceThreadID, sourceStarted.TurnID)
	if err != nil {
		return err
	}
	if !finished {
		return nil
	}
	terminalEntryID := entryIDs[finish.TerminalEntryID]
	if terminalEntryID == "" {
		return sessiontree.ErrAuthorityCorrupt
	}
	failureEntryID := entryIDs[finish.FailureEntryID]
	if finish.FailureEntryID != "" && failureEntryID == "" {
		return sessiontree.ErrAuthorityCorrupt
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO turn_finishes(
		thread_id, turn_id, run_id, generation, outcome_fingerprint, failure_entry_id, terminal_entry_id, finished_at
	) VALUES(?, ?, ?, ?, ?, ?, ?, ?)`, destinationThreadID, destinationStarted.TurnID, destinationStarted.Metadata["run_id"],
		generation, finish.OutcomeFingerprint, failureEntryID, terminalEntryID, formatTime(now))
	return err
}

func (s *Store) ForkWithInitialEntry(ctx context.Context, opts sessiontree.ForkOptions, initial sessiontree.Entry) (sessiontree.ThreadMeta, sessiontree.Entry, error) {
	var forked sessiontree.ThreadMeta
	var saved sessiontree.Entry
	err := s.withImmediate(ctx, func(tx sqlRunner) error {
		var err error
		forked, err = forkWithRunner(ctx, tx, opts)
		if err != nil {
			return err
		}
		initial.ThreadID = forked.ID
		saved, err = appendWithRunner(ctx, tx, initial, sessiontree.AppendOptions{Now: opts.Now}, s.now)
		if err != nil {
			return err
		}
		forked, err = loadThread(ctx, tx, forked.ID)
		return err
	})
	return forked, saved, err
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
	meta.Lifecycle = destination.Lifecycle
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
		meta, err := loadThread(ctx, tx, state.ThreadID)
		if err != nil {
			return err
		}
		if err := rejectSQLiteThreadWriteLifecycle(meta); err != nil {
			return err
		}
		if err := rejectClaimedThreadAuthorities(ctx, tx, state.ThreadID); err != nil {
			return err
		}
		active, ok, err := loadTurnLease(ctx, tx, state.ThreadID)
		if err != nil {
			return err
		}
		if !ok || active.Purpose != sessiontree.TurnLeasePurposeTurn {
			return sessiontree.ErrActiveTurn
		}
		if err := validateSQLiteTurnLeaseMutation(ctx, state.ThreadID, active.TurnID, active, s.now().UTC()); err != nil {
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
	plan, err := storage.DecodeForkOperationPlan(rec.Plan)
	if err != nil {
		return storage.ForkOperationRecord{}, false, err
	}
	nodes := storage.ForkOperationPlanNodes(plan)
	var existing storage.ForkOperationRecord
	created := false
	err = s.withImmediate(ctx, func(tx sqlRunner) error {
		loaded, err := loadForkOperation(ctx, tx, rec.OperationID)
		if err == nil {
			existing = loaded
			return nil
		}
		if !errors.Is(err, storage.ErrForkOperationNotFound) {
			return err
		}
		if err := validateForkOperationPlanAtPrepare(ctx, tx, plan, nodes); err != nil {
			return err
		}
		if err := ensureForkOperationSourcesIdle(ctx, tx, rec.SourceThreadIDs); err != nil {
			return err
		}
		if err := ensureForkOperationDestinationsAbsent(ctx, tx, rec.SourceThreadIDs, rec.AuthorityThreadIDs); err != nil {
			return err
		}
		sourceJSON, err := json.Marshal(rec.SourceThreadIDs)
		if err != nil {
			return err
		}
		authorityJSON, err := json.Marshal(rec.AuthorityThreadIDs)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO fork_operations(
			operation_id, request_fingerprint, source_thread_ids_json, authority_thread_ids_json,
			state, plan_json, result_json, error_code, error_message, created_at, updated_at, finished_at
		) VALUES(?, ?, ?, ?, ?, ?, '', '', '', ?, ?, '')`,
			rec.OperationID, rec.RequestFingerprint, string(sourceJSON), string(authorityJSON),
			string(rec.State), string(rec.Plan), formatTime(rec.CreatedAt), formatTime(rec.UpdatedAt)); err != nil {
			return err
		}
		for _, threadID := range rec.AuthorityThreadIDs {
			if _, err := tx.ExecContext(ctx, `INSERT INTO thread_authority_claims(thread_id, operation_id, created_at)
				VALUES(?, ?, ?)`, threadID, rec.OperationID, formatTime(rec.CreatedAt)); err != nil {
				if isConstraintError(err) {
					return sessiontree.ErrThreadAuthorityBusy
				}
				return err
			}
		}
		created = true
		existing, err = loadForkOperation(ctx, tx, rec.OperationID)
		return err
	})
	return existing, created, err
}

func validateForkOperationPlanAtPrepare(ctx context.Context, q sqlRunner, plan storage.ForkOperationPlan, nodes []sessiontree.ForkOptions) error {
	threads, err := loadThreadAuthorityGraph(ctx, q)
	if err != nil {
		return err
	}
	rootThreadID := strings.TrimSpace(plan.Root.SourceThreadID)
	nodeBySource := make(map[string]sessiontree.ForkOptions, len(nodes))
	for _, node := range nodes {
		nodeBySource[strings.TrimSpace(node.SourceThreadID)] = node
	}
	states := make([]sessiontree.ForkPrepareThreadState, 0)
	for _, meta := range threads {
		if meta.ID != rootThreadID && strings.TrimSpace(meta.ParentThreadID) != rootThreadID {
			continue
		}
		path, err := pathWithRunner(ctx, q, meta.ID, meta.LeafID)
		if err != nil {
			return err
		}
		var pinned []sessiontree.Entry
		if node, ok := nodeBySource[meta.ID]; ok {
			pinned, err = pathWithRunner(ctx, q, meta.ID, node.EntryID)
			if err != nil {
				return err
			}
		}
		pending := 0
		if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM subagent_inputs WHERE child_thread_id = ? AND state = 'pending'`, meta.ID).Scan(&pending); err != nil {
			return err
		}
		states = append(states, sessiontree.ForkPrepareThreadState{Meta: meta, Path: path, PinnedPath: pinned, PendingInputCount: pending})
	}
	if err := sessiontree.ValidateForkPrepareState(rootThreadID, nodes, states); err != nil {
		return err
	}
	for _, state := range states {
		node, ok := nodeBySource[state.Meta.ID]
		if !ok {
			continue
		}
		if err := validateSQLiteArtifactClosure(ctx, q, state.Meta.ID, node.NewThreadID, state.PinnedPath, node.ArtifactClosure); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) ForkOperation(ctx context.Context, operationID string) (storage.ForkOperationRecord, error) {
	var record storage.ForkOperationRecord
	err := s.withRead(ctx, func(q sqlRunner) error {
		var err error
		record, err = loadForkOperation(ctx, q, strings.TrimSpace(operationID))
		if err != nil || record.State != storage.ForkOperationCompleted {
			return err
		}
		plan, err := storage.DecodeForkOperationPlan(record.Plan)
		if err != nil {
			return err
		}
		return validateSQLiteCompletedArtifactClosures(ctx, q, plan)
	})
	return record, err
}

func (s *Store) CommitForkOperation(ctx context.Context, req storage.ForkOperationCommitRequest) (storage.ForkOperationRecord, bool, error) {
	if strings.TrimSpace(req.OperationID) == "" || strings.TrimSpace(req.RequestFingerprint) == "" || len(req.Plan) == 0 || !json.Valid(req.Plan) || len(req.Nodes) == 0 ||
		len(req.Result) == 0 || !json.Valid(req.Result) || req.FinishedAt.IsZero() {
		return storage.ForkOperationRecord{}, false, errors.New("fork commit requires operation, fingerprint, complete nodes, result, and finish time")
	}
	nodes := snapshotForkNodes(req.Nodes)
	var terminal storage.ForkOperationRecord
	replayed := false
	err := s.withImmediate(ctx, func(tx sqlRunner) error {
		existing, err := loadForkOperation(ctx, tx, strings.TrimSpace(req.OperationID))
		if err != nil {
			return err
		}
		if existing.RequestFingerprint != strings.TrimSpace(req.RequestFingerprint) {
			return storage.ErrForkOperationConflict
		}
		if !jsonRawEqual(existing.Plan, req.Plan) {
			return storage.ErrForkOperationConflict
		}
		plan, err := storage.DecodeForkOperationPlan(existing.Plan)
		if err != nil {
			return err
		}
		if err := storage.ValidateForkOperationCommitNodes(plan, nodes); err != nil {
			return err
		}
		if existing.State == storage.ForkOperationCompleted {
			if !jsonRawEqual(existing.Result, req.Result) {
				return storage.ErrForkOperationConflict
			}
			if err := validateSQLiteCompletedArtifactClosures(ctx, tx, plan); err != nil {
				return err
			}
			terminal, replayed = existing, true
			return nil
		}
		if existing.State != storage.ForkOperationPrepared {
			return storage.ErrForkOperationConflict
		}
		if err := validatePreparedForkCommit(ctx, tx, existing, nodes); err != nil {
			return err
		}
		for _, node := range nodes {
			if _, err := forkWithRunner(ctx, tx, node); err != nil {
				return err
			}
		}
		if err := validateSQLiteCompletedArtifactClosures(ctx, tx, plan); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `UPDATE fork_operations SET state = 'completed', result_json = ?,
			error_code = '', error_message = '', updated_at = ?, finished_at = ?
			WHERE operation_id = ? AND state = 'prepared'`, string(req.Result), formatTime(req.FinishedAt), formatTime(req.FinishedAt), existing.OperationID)
		if err != nil {
			return err
		}
		if count, err := result.RowsAffected(); err != nil {
			return err
		} else if count != 1 {
			return storage.ErrForkOperationConflict
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM thread_authority_claims WHERE operation_id = ?`, existing.OperationID); err != nil {
			return err
		}
		terminal, err = loadForkOperation(ctx, tx, existing.OperationID)
		return err
	})
	return terminal, replayed, err
}

func snapshotForkNodes(nodes []sessiontree.ForkOptions) []sessiontree.ForkOptions {
	snapshot := make([]sessiontree.ForkOptions, len(nodes))
	for index, node := range nodes {
		snapshot[index] = snapshotForkIdentityMaps(node)
	}
	return snapshot
}

func snapshotForkIdentityMaps(opts sessiontree.ForkOptions) sessiontree.ForkOptions {
	opts.TurnIDMap = cloneStringMapSQLite(opts.TurnIDMap)
	opts.RunIDMap = cloneStringMapSQLite(opts.RunIDMap)
	return opts
}

func validateSQLiteCompletedArtifactClosures(ctx context.Context, q sqlRunner, plan storage.ForkOperationPlan) error {
	for _, node := range append([]storage.ForkOperationPlanNode{plan.Root}, plan.TerminalChildren...) {
		if err := validateSQLiteArtifactForkDestination(ctx, q, node.ArtifactClosure); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) FailForkOperation(ctx context.Context, req storage.ForkOperationFailureRequest) (storage.ForkOperationRecord, bool, error) {
	if strings.TrimSpace(req.OperationID) == "" || strings.TrimSpace(req.RequestFingerprint) == "" ||
		strings.TrimSpace(req.ErrorCode) == "" || strings.TrimSpace(req.ErrorMessage) == "" || req.FinishedAt.IsZero() {
		return storage.ForkOperationRecord{}, false, errors.New("fork failure requires operation, fingerprint, typed error, and finish time")
	}
	var terminal storage.ForkOperationRecord
	replayed := false
	err := s.withImmediate(ctx, func(tx sqlRunner) error {
		existing, err := loadForkOperation(ctx, tx, strings.TrimSpace(req.OperationID))
		if err != nil {
			return err
		}
		if existing.RequestFingerprint != strings.TrimSpace(req.RequestFingerprint) {
			return storage.ErrForkOperationConflict
		}
		if existing.State == storage.ForkOperationFailed {
			if existing.ErrorCode != strings.TrimSpace(req.ErrorCode) || existing.ErrorMessage != strings.TrimSpace(req.ErrorMessage) {
				return storage.ErrForkOperationConflict
			}
			terminal, replayed = existing, true
			return nil
		}
		if existing.State != storage.ForkOperationPrepared {
			return storage.ErrForkOperationConflict
		}
		if err := validatePreparedForkFailure(ctx, tx, existing); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `UPDATE fork_operations SET state = 'failed', result_json = '',
			error_code = ?, error_message = ?, updated_at = ?, finished_at = ?
			WHERE operation_id = ? AND state = 'prepared'`, strings.TrimSpace(req.ErrorCode), strings.TrimSpace(req.ErrorMessage),
			formatTime(req.FinishedAt), formatTime(req.FinishedAt), existing.OperationID)
		if err != nil {
			return err
		}
		if count, err := result.RowsAffected(); err != nil {
			return err
		} else if count != 1 {
			return storage.ErrForkOperationConflict
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM thread_authority_claims WHERE operation_id = ?`, existing.OperationID); err != nil {
			return err
		}
		terminal, err = loadForkOperation(ctx, tx, existing.OperationID)
		return err
	})
	return terminal, replayed, err
}

func loadForkOperation(ctx context.Context, q sqlRunner, operationID string) (storage.ForkOperationRecord, error) {
	var rec storage.ForkOperationRecord
	var sourceIDs, authorityIDs, state, plan, result, created, updated, finished string
	err := q.QueryRowContext(ctx, `SELECT operation_id, request_fingerprint, source_thread_ids_json, authority_thread_ids_json,
		state, plan_json, result_json,
		error_code, error_message, created_at, updated_at, finished_at
		FROM fork_operations WHERE operation_id = ?`, operationID).Scan(
		&rec.OperationID, &rec.RequestFingerprint, &sourceIDs, &authorityIDs, &state, &plan, &result,
		&rec.ErrorCode, &rec.ErrorMessage, &created, &updated, &finished,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.ForkOperationRecord{}, storage.ErrForkOperationNotFound
	}
	if err != nil {
		return storage.ForkOperationRecord{}, err
	}
	if err := json.Unmarshal([]byte(sourceIDs), &rec.SourceThreadIDs); err != nil {
		return storage.ForkOperationRecord{}, fmt.Errorf("decode fork operation source thread ids: %w", err)
	}
	if err := json.Unmarshal([]byte(authorityIDs), &rec.AuthorityThreadIDs); err != nil {
		return storage.ForkOperationRecord{}, fmt.Errorf("decode fork operation authority thread ids: %w", err)
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
		stringSliceEqual(left.SourceThreadIDs, right.SourceThreadIDs) &&
		stringSliceEqual(left.AuthorityThreadIDs, right.AuthorityThreadIDs) &&
		left.State == right.State &&
		jsonRawEqual(left.Plan, right.Plan) &&
		jsonRawEqual(left.Result, right.Result) &&
		left.ErrorCode == right.ErrorCode &&
		left.ErrorMessage == right.ErrorMessage &&
		left.CreatedAt.Equal(right.CreatedAt) &&
		left.UpdatedAt.Equal(right.UpdatedAt) &&
		left.FinishedAt.Equal(right.FinishedAt)
}

func ensureForkOperationSourcesIdle(ctx context.Context, q sqlRunner, sourceThreadIDs []string) error {
	for _, threadID := range sourceThreadIDs {
		meta, err := loadThread(ctx, q, threadID)
		if err != nil {
			return err
		}
		if meta.Lifecycle == sessiontree.ThreadLifecycleClosing {
			return sessiontree.ErrSubAgentClosing
		}
		if _, active, err := loadTurnLease(ctx, q, threadID); err != nil {
			return err
		} else if active {
			return sessiontree.ErrActiveTurn
		}
	}
	return nil
}

func ensureForkOperationDestinationsAbsent(ctx context.Context, q sqlRunner, sourceThreadIDs, authorityThreadIDs []string) error {
	sources := make(map[string]struct{}, len(sourceThreadIDs))
	for _, threadID := range sourceThreadIDs {
		sources[threadID] = struct{}{}
	}
	for _, threadID := range authorityThreadIDs {
		if _, source := sources[threadID]; source {
			continue
		}
		if exists, err := threadExists(ctx, q, threadID); err != nil {
			return err
		} else if exists {
			return sessiontree.ErrForkDestinationConflict
		}
		if _, err := loadThreadTombstone(ctx, q, threadID); err == nil {
			return sessiontree.ErrForkDestinationConflict
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	}
	return nil
}

func validatePreparedForkCommit(ctx context.Context, q sqlRunner, operation storage.ForkOperationRecord, nodes []sessiontree.ForkOptions) error {
	sources := make(map[string]struct{}, len(operation.SourceThreadIDs))
	authority := make(map[string]struct{}, len(operation.AuthorityThreadIDs))
	destinations := make(map[string]struct{}, len(operation.AuthorityThreadIDs)-len(operation.SourceThreadIDs))
	for _, threadID := range operation.SourceThreadIDs {
		sources[threadID] = struct{}{}
	}
	for _, threadID := range operation.AuthorityThreadIDs {
		authority[threadID] = struct{}{}
		if _, source := sources[threadID]; !source {
			destinations[threadID] = struct{}{}
		}
	}
	nodeSources := make(map[string]struct{}, len(nodes))
	nodeDestinations := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		sourceID := strings.TrimSpace(node.SourceThreadID)
		destinationID := strings.TrimSpace(node.NewThreadID)
		if strings.TrimSpace(node.OperationID) != operation.OperationID || strings.TrimSpace(node.OperationNodeID) == "" {
			return sessiontree.ErrInvalidThreadAuthority
		}
		if _, ok := sources[sourceID]; !ok {
			return sessiontree.ErrInvalidThreadAuthority
		}
		if _, ok := destinations[destinationID]; !ok {
			return sessiontree.ErrInvalidThreadAuthority
		}
		if _, duplicate := nodeDestinations[destinationID]; duplicate {
			return sessiontree.ErrInvalidThreadAuthority
		}
		nodeSources[sourceID] = struct{}{}
		nodeDestinations[destinationID] = struct{}{}
	}
	if !sqliteStringSetEqual(nodeSources, sources) || !sqliteStringSetEqual(nodeDestinations, destinations) {
		return sessiontree.ErrInvalidThreadAuthority
	}
	if err := validateExactForkClaims(ctx, q, operation.OperationID, authority); err != nil {
		return err
	}
	for threadID := range sources {
		if exists, err := threadExists(ctx, q, threadID); err != nil {
			return err
		} else if !exists {
			return sessiontree.ErrAuthorityCorrupt
		}
		if _, active, err := loadTurnLease(ctx, q, threadID); err != nil {
			return err
		} else if active {
			return sessiontree.ErrAuthorityCorrupt
		}
	}
	for threadID := range destinations {
		if exists, err := threadExists(ctx, q, threadID); err != nil {
			return err
		} else if exists {
			return sessiontree.ErrAuthorityCorrupt
		}
		if _, err := loadThreadTombstone(ctx, q, threadID); err == nil {
			return sessiontree.ErrAuthorityCorrupt
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	}
	return nil
}

func validatePreparedForkFailure(ctx context.Context, q sqlRunner, operation storage.ForkOperationRecord) error {
	sources := make(map[string]struct{}, len(operation.SourceThreadIDs))
	authority := make(map[string]struct{}, len(operation.AuthorityThreadIDs))
	for _, threadID := range operation.SourceThreadIDs {
		sources[threadID] = struct{}{}
	}
	for _, threadID := range operation.AuthorityThreadIDs {
		authority[threadID] = struct{}{}
	}
	if err := validateExactForkClaims(ctx, q, operation.OperationID, authority); err != nil {
		return err
	}
	for threadID := range authority {
		if _, source := sources[threadID]; source {
			continue
		}
		if exists, err := threadExists(ctx, q, threadID); err != nil {
			return err
		} else if exists {
			return sessiontree.ErrAuthorityCorrupt
		}
		if _, err := loadThreadTombstone(ctx, q, threadID); err == nil {
			return sessiontree.ErrAuthorityCorrupt
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	}
	return nil
}

func validateExactForkClaims(ctx context.Context, q sqlRunner, operationID string, authority map[string]struct{}) error {
	rows, err := q.QueryContext(ctx, `SELECT thread_id FROM thread_authority_claims WHERE operation_id = ?`, operationID)
	if err != nil {
		return err
	}
	defer rows.Close()
	claimed := make(map[string]struct{}, len(authority))
	for rows.Next() {
		var threadID string
		if err := rows.Scan(&threadID); err != nil {
			return err
		}
		claimed[threadID] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if !sqliteStringSetEqual(claimed, authority) {
		return sessiontree.ErrAuthorityCorrupt
	}
	return nil
}

func sqliteStringSetEqual(left, right map[string]struct{}) bool {
	if len(left) != len(right) {
		return false
	}
	for value := range left {
		if _, ok := right[value]; !ok {
			return false
		}
	}
	return true
}

func stringSliceEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
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

func (s *Store) ProviderState(ctx context.Context, threadID string) (sessiontree.ProviderStateRecord, error) {
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		return sessiontree.ProviderStateRecord{}, errors.New("provider state thread id is required")
	}
	var record sessiontree.ProviderStateRecord
	var rawState, updatedAt string
	err := s.db.QueryRowContext(ctx, `SELECT thread_id, leaf_entry_id, compatibility_key, state_json,
		created_by_run_id, created_by_turn_id, updated_at
		FROM provider_states WHERE thread_id = ?`, threadID).Scan(
		&record.ThreadID, &record.LeafEntryID, &record.CompatibilityKey, &rawState,
		&record.CreatedByRunID, &record.CreatedByTurnID, &updatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return sessiontree.ProviderStateRecord{}, sessiontree.ErrProviderStateNotFound
	}
	if err != nil {
		return sessiontree.ProviderStateRecord{}, err
	}
	if err := json.Unmarshal([]byte(rawState), &record.State); err != nil {
		return sessiontree.ProviderStateRecord{}, fmt.Errorf("decode provider state for thread %q: %w", threadID, err)
	}
	if strings.TrimSpace(record.State.Kind) == "" || strings.TrimSpace(record.State.ID) == "" {
		return sessiontree.ProviderStateRecord{}, fmt.Errorf("provider state for thread %q is incomplete", threadID)
	}
	record.UpdatedAt = parseTime(updatedAt)
	return record, nil
}

func (s *Store) PutProviderState(ctx context.Context, record sessiontree.ProviderStateRecord) error {
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
		meta, err := loadThread(ctx, tx, record.ThreadID)
		if err != nil {
			return err
		}
		if err := rejectSQLiteThreadWriteLifecycle(meta); err != nil {
			return err
		}
		if err := rejectClaimedThreadAuthorities(ctx, tx, record.ThreadID); err != nil {
			return err
		}
		active, ok, err := loadTurnLease(ctx, tx, record.ThreadID)
		if err != nil {
			return err
		}
		if !ok || validateSQLiteTurnLeaseMutation(ctx, record.ThreadID, "", active, s.now().UTC()) != nil {
			return sessiontree.ErrStaleAuthority
		}
		return putProviderStateWithRunner(ctx, tx, record, rawState)
	})
}

func (s *Store) DeleteProviderState(ctx context.Context, threadID string) error {
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		return errors.New("provider state thread id is required")
	}
	return s.withImmediate(ctx, func(tx sqlRunner) error {
		meta, err := loadThread(ctx, tx, threadID)
		if err != nil {
			return err
		}
		if err := rejectSQLiteThreadWriteLifecycle(meta); err != nil {
			return err
		}
		if err := rejectClaimedThreadAuthorities(ctx, tx, threadID); err != nil {
			return err
		}
		active, ok, err := loadTurnLease(ctx, tx, threadID)
		if err != nil {
			return err
		}
		if !ok || validateSQLiteTurnLeaseMutation(ctx, threadID, "", active, s.now().UTC()) != nil {
			return sessiontree.ErrStaleAuthority
		}
		_, err = tx.ExecContext(ctx, `DELETE FROM provider_states WHERE thread_id = ?`, threadID)
		return err
	})
}

func putProviderStateWithRunner(ctx context.Context, q sqlRunner, record sessiontree.ProviderStateRecord, rawState []byte) error {
	_, err := q.ExecContext(ctx, `INSERT INTO provider_states(
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
}

func loadThreadAuthorityClaim(ctx context.Context, q sqlRunner, threadID string) (string, bool, error) {
	var operationID string
	err := q.QueryRowContext(ctx, `SELECT operation_id FROM thread_authority_claims WHERE thread_id = ?`, threadID).Scan(&operationID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return operationID, true, nil
}

func rejectClaimedThreadAuthorities(ctx context.Context, q sqlRunner, threadIDs ...string) error {
	for _, threadID := range threadIDs {
		threadID = strings.TrimSpace(threadID)
		if threadID == "" {
			continue
		}
		if _, claimed, err := loadThreadAuthorityClaim(ctx, q, threadID); err != nil {
			return err
		} else if claimed {
			return sessiontree.ErrThreadAuthorityBusy
		}
	}
	return nil
}

func requireForkAuthorityClaims(ctx context.Context, q sqlRunner, operationID string, threadIDs ...string) error {
	operationID = strings.TrimSpace(operationID)
	for _, threadID := range threadIDs {
		threadID = strings.TrimSpace(threadID)
		if threadID == "" {
			continue
		}
		owner, claimed, err := loadThreadAuthorityClaim(ctx, q, threadID)
		if err != nil {
			return err
		}
		if operationID == "" {
			if claimed {
				return sessiontree.ErrThreadAuthorityBusy
			}
			continue
		}
		if !claimed || owner != operationID {
			return sessiontree.ErrThreadAuthorityBusy
		}
	}
	return nil
}

func validateSQLiteTurnLeaseMutation(ctx context.Context, threadID, turnID string, active sessiontree.TurnLease, now time.Time) error {
	proof, hasProof := sessiontree.TurnLeaseFromContext(ctx)
	relevantProof := hasProof && proof.ThreadID == strings.TrimSpace(threadID)
	activeExists := active.Validate() == nil
	if activeExists {
		if !relevantProof || !sessiontree.SameTurnLease(proof, active) {
			return sessiontree.ErrActiveTurn
		}
		if !active.Fresh(now) {
			return sessiontree.ErrStaleAuthority
		}
		if strings.TrimSpace(turnID) != "" && strings.TrimSpace(turnID) != active.TurnID {
			return sessiontree.ErrActiveTurn
		}
		return nil
	}
	if relevantProof {
		return sessiontree.ErrActiveTurn
	}
	return nil
}

func insertThread(ctx context.Context, tx sqlRunner, meta sessiontree.ThreadMeta) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO threads(
		id, leaf_id, parent_thread_id, parent_turn_id, forked_from_thread_id, forked_from_entry_id, fork_operation_id, fork_operation_node_id,
		task_name, task_description, agent_path, host_profile_ref, fork_mode, lifecycle, close_operation_id, archived,
		title, title_status, title_source, title_updated_at, title_error, title_generation, title_token,
		created_at, updated_at, last_viewed_at
	) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		meta.ID, meta.LeafID, meta.ParentThreadID, meta.ParentTurnID, meta.ForkedFromThreadID, meta.ForkedFromEntryID,
		meta.ForkOperationID, meta.ForkOperationNodeID,
		meta.TaskName, meta.TaskDescription, meta.AgentPath, meta.HostProfileRef, meta.ForkMode, storedThreadLifecycle(meta), meta.CloseOperationID, boolInt(meta.Archived),
		meta.Title, string(meta.TitleStatus), string(meta.TitleSource), formatTime(meta.TitleUpdatedAt), meta.TitleError, meta.TitleGeneration, meta.TitleToken,
		formatTime(meta.CreatedAt), formatTime(meta.UpdatedAt), formatTime(meta.LastViewedAt))
	return err
}

func loadTurnLease(ctx context.Context, q sqlRunner, threadID string) (sessiontree.TurnLease, bool, error) {
	var lease sessiontree.TurnLease
	var acquired, renewed, expires, purpose string
	err := q.QueryRowContext(ctx, `SELECT
		thread_id, purpose, turn_id, mutation_id, mutation_kind, owner_id,
		generation, heartbeat, acquired_at, renewed_at, expires_at
		FROM active_turn_leases WHERE thread_id = ?`, threadID).Scan(
		&lease.ThreadID, &purpose, &lease.TurnID, &lease.MutationID, &lease.MutationKind, &lease.OwnerID,
		&lease.Generation, &lease.Heartbeat, &acquired, &renewed, &expires,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return sessiontree.TurnLease{}, false, nil
	}
	if err != nil {
		return sessiontree.TurnLease{}, false, err
	}
	lease.AcquiredAt = parseTime(acquired)
	lease.RenewedAt = parseTime(renewed)
	lease.ExpiresAt = parseTime(expires)
	lease.Purpose, err = sessiontree.TurnLeasePurpose(purpose).Normalize()
	if err != nil {
		return sessiontree.TurnLease{}, false, err
	}
	if err := lease.Validate(); err != nil {
		return sessiontree.TurnLease{}, false, fmt.Errorf("invalid sqlite turn lease: %w", err)
	}
	return lease, true, nil
}

func updateThread(ctx context.Context, tx sqlRunner, meta sessiontree.ThreadMeta) error {
	_, err := tx.ExecContext(ctx, `UPDATE threads SET
		leaf_id = ?, parent_thread_id = ?, parent_turn_id = ?, forked_from_thread_id = ?, forked_from_entry_id = ?, fork_operation_id = ?, fork_operation_node_id = ?,
		task_name = ?, task_description = ?, agent_path = ?, host_profile_ref = ?, fork_mode = ?, lifecycle = ?, close_operation_id = ?, archived = ?,
		title = ?, title_status = ?, title_source = ?, title_updated_at = ?, title_error = ?, title_generation = ?, title_token = ?,
		created_at = ?, updated_at = ?, last_viewed_at = ?
		WHERE id = ?`,
		meta.LeafID, meta.ParentThreadID, meta.ParentTurnID, meta.ForkedFromThreadID, meta.ForkedFromEntryID,
		meta.ForkOperationID, meta.ForkOperationNodeID,
		meta.TaskName, meta.TaskDescription, meta.AgentPath, meta.HostProfileRef, meta.ForkMode, storedThreadLifecycle(meta), meta.CloseOperationID, boolInt(meta.Archived),
		meta.Title, string(meta.TitleStatus), string(meta.TitleSource), formatTime(meta.TitleUpdatedAt), meta.TitleError, meta.TitleGeneration, meta.TitleToken,
		formatTime(meta.CreatedAt), formatTime(meta.UpdatedAt), formatTime(meta.LastViewedAt), meta.ID)
	return err
}

func loadThread(ctx context.Context, q sqlRunner, threadID string) (sessiontree.ThreadMeta, error) {
	meta, err := scanThreadMeta(q.QueryRowContext(ctx, `SELECT
		id, leaf_id, parent_thread_id, parent_turn_id, forked_from_thread_id, forked_from_entry_id, fork_operation_id, fork_operation_node_id,
		task_name, task_description, agent_path, host_profile_ref, fork_mode, lifecycle, close_operation_id, archived,
		title, title_status, title_source, title_updated_at, title_error, title_generation, title_token,
		created_at, updated_at, last_viewed_at
		FROM threads WHERE id = ?`, threadID))
	if errors.Is(err, sql.ErrNoRows) {
		return sessiontree.ThreadMeta{}, sessiontree.ErrThreadNotFound
	}
	return meta, err
}

func loadThreadAuthorityGraph(ctx context.Context, q sqlRunner) ([]sessiontree.ThreadMeta, error) {
	rows, err := q.QueryContext(ctx, `SELECT
		id, leaf_id, parent_thread_id, parent_turn_id, forked_from_thread_id, forked_from_entry_id, fork_operation_id, fork_operation_node_id,
		task_name, task_description, agent_path, host_profile_ref, fork_mode, lifecycle, close_operation_id, archived,
		title, title_status, title_source, title_updated_at, title_error, title_generation, title_token,
		created_at, updated_at, last_viewed_at
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
	var archived int
	var lifecycle, titleStatus, titleSource, titleUpdated, created, updated, lastViewed string
	err := scanner.Scan(
		&meta.ID, &meta.LeafID, &meta.ParentThreadID, &meta.ParentTurnID, &meta.ForkedFromThreadID, &meta.ForkedFromEntryID,
		&meta.ForkOperationID, &meta.ForkOperationNodeID,
		&meta.TaskName, &meta.TaskDescription, &meta.AgentPath, &meta.HostProfileRef, &meta.ForkMode, &lifecycle, &meta.CloseOperationID, &archived,
		&meta.Title, &titleStatus, &titleSource, &titleUpdated, &meta.TitleError, &meta.TitleGeneration, &meta.TitleToken,
		&created, &updated, &lastViewed,
	)
	if err != nil {
		return sessiontree.ThreadMeta{}, err
	}
	meta.Lifecycle = sessiontree.ThreadLifecycle(lifecycle)
	meta.Archived = archived != 0
	meta.TitleStatus = sessiontree.ThreadTitleStatus(titleStatus)
	meta.TitleSource = sessiontree.ThreadTitleSource(titleSource)
	meta.TitleUpdatedAt = parseTime(titleUpdated)
	meta.CreatedAt = parseTime(created)
	meta.UpdatedAt = parseTime(updated)
	meta.LastViewedAt = parseTime(lastViewed)
	if err := sessiontree.ValidateThreadMetaAuthority(meta); err != nil {
		return sessiontree.ThreadMeta{}, err
	}
	return meta, nil
}

func insertEntry(ctx context.Context, tx sqlRunner, entry sessiontree.Entry, ordinal int64, preserveRaw bool) error {
	pathDepth, err := resolveEntryPathDepth(ctx, tx, entry)
	if err != nil {
		return err
	}
	entry.PathDepth = pathDepth
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
			thread_id, id, ordinal, parent_id, path_depth, type, turn_id, created_at,
		message_json, raw, raw_hash, raw_encoder_version,
		turn_status, provider, model, compaction_id, previous_compaction_id,
		compacted_through_entry_id, summary_schema_version, compaction_generation,
		compaction_window_id, first_kept_entry_id, kept_user_entry_ids_json, summary, compaction_trigger,
		compaction_reason, compaction_phase, tokens_before, tokens_after_estimate,
		compaction_operation_id, compaction_request_id, compaction_source,
		context_usage_before_json, context_usage_after_json, error, metadata_json
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		entry.ThreadID, entry.ID, ordinal, entry.ParentID, entry.PathDepth, string(entry.Type), entry.TurnID, formatTime(entry.CreatedAt),
		string(messageJSON), entry.Raw, entry.RawHash, 1,
		string(entry.TurnStatus), entry.Provider, entry.Model, entry.CompactionID, entry.PreviousCompactionID,
		entry.CompactedThroughEntryID, entry.SummarySchemaVersion, entry.CompactionGeneration,
		entry.CompactionWindowID, entry.FirstKeptEntryID, string(keptUserEntryIDsJSON), entry.Summary, entry.CompactionTrigger,
		entry.CompactionReason, entry.CompactionPhase, entry.TokensBefore, entry.TokensAfterEstimate,
		entry.CompactionOperationID, entry.CompactionRequestID, entry.CompactionSource,
		string(beforeJSON), string(afterJSON), entry.Error, string(metadataJSON))
	return err
}

func resolveEntryPathDepth(ctx context.Context, q sqlRunner, entry sessiontree.Entry) (int64, error) {
	expected := int64(1)
	if entry.ParentID != "" {
		var parentDepth int64
		if err := q.QueryRowContext(ctx, `SELECT path_depth FROM entries WHERE thread_id = ? AND id = ?`,
			entry.ThreadID, entry.ParentID).Scan(&parentDepth); errors.Is(err, sql.ErrNoRows) {
			return 0, sessiontree.ErrInvalidParent
		} else if err != nil {
			return 0, err
		}
		if parentDepth <= 0 {
			return 0, sessiontree.ErrAuthorityCorrupt
		}
		expected = parentDepth + 1
	}
	if entry.PathDepth != 0 && entry.PathDepth != expected {
		return 0, sessiontree.ErrAuthorityCorrupt
	}
	return expected, nil
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
		&entry.ThreadID, &entry.ID, &entry.ParentID, &entry.PathDepth, &typ, &entry.TurnID, &created,
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
		if entry.ThreadID != threadID {
			return nil, sessiontree.ErrAuthorityCorrupt
		}
		if err := sessiontree.ValidateEntryIntegrity(entry); err != nil {
			return nil, err
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
		case sessiontree.RetrySourceTurnIDMetadataKey:
			next = rewriteForkID(value, turnIDs)
		case "entry_id", "parent_entry_id", "input_entry_id", "subagent_input_id":
			next = rewriteForkID(value, entryIDs)
		case sessiontree.RetrySourceEntryIDMetadataKey:
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

func storedThreadLifecycle(meta sessiontree.ThreadMeta) string {
	lifecycle, err := meta.CanonicalLifecycle()
	if err != nil {
		return string(meta.Lifecycle)
	}
	return string(lifecycle)
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

const entryColumns = `thread_id, id, parent_id, path_depth, type, turn_id, created_at,
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
var _ sessiontree.Repo = (*Store)(nil)
var _ sessiontree.TurnLeaseRepo = (*Store)(nil)
var _ sessiontree.AgentTodoStateRepo = (*Store)(nil)

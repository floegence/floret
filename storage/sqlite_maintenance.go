package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
)

const (
	SQLiteMaintenanceActionNone              = "none"
	SQLiteMaintenanceActionVacuum            = "vacuum"
	SQLiteMaintenanceActionIncrementalVacuum = "incremental_vacuum"
)

// SQLiteMaintenancePolicy defines when an idle Floret SQLite backend should
// reclaim free pages before runtime.Open acquires the database.
type SQLiteMaintenancePolicy struct {
	MinimumFileBytes    int64
	MinimumReclaimBytes int64
	MinimumReclaimRatio float64
	RetainedFreeBytes   int64
}

// SQLiteSpaceUsage reports physical SQLite page usage without inspecting
// opaque Floret records.
type SQLiteSpaceUsage struct {
	FileBytes        int64  `json:"file_bytes"`
	PageSize         int64  `json:"page_size"`
	PageCount        int64  `json:"page_count"`
	FreePageCount    int64  `json:"free_page_count"`
	ReclaimableBytes int64  `json:"reclaimable_bytes"`
	AutoVacuum       string `json:"auto_vacuum"`
}

// SQLiteMaintenanceResult describes one pre-open maintenance decision. A
// none action is a safe skip; schema and integrity failures are returned as
// errors because runtime startup must not hide corrupted storage.
type SQLiteMaintenanceResult struct {
	Action string           `json:"action"`
	Before SQLiteSpaceUsage `json:"before"`
	After  SQLiteSpaceUsage `json:"after"`
	Reason string           `json:"reason"`
}

// MaintainSQLite reclaims free pages in an idle Floret SQLite backend. The
// caller must invoke it before runtime.Open and must ensure no live Host owns
// path. It never copies, replaces, or interprets opaque Floret records.
func MaintainSQLite(ctx context.Context, path string, policy SQLiteMaintenancePolicy) (SQLiteMaintenanceResult, error) {
	result := SQLiteMaintenanceResult{Action: SQLiteMaintenanceActionNone}
	if ctx == nil {
		return result, fmt.Errorf("SQLite maintenance context is required")
	}
	path = strings.TrimSpace(path)
	if path == "" || path == ":memory:" {
		return result, fmt.Errorf("SQLite maintenance requires a file path")
	}
	if err := validateSQLiteMaintenancePolicy(policy); err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return result, err
	}
	path = filepath.Clean(absolutePath)
	sqliteOwnership.Lock()
	defer sqliteOwnership.Unlock()
	if sqliteOwnership.open[path] > 0 {
		result.Reason = "runtime_open"
		return result, nil
	}
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		result.Reason = "database_missing"
		return result, nil
	}
	if err != nil {
		return result, err
	}
	if !info.Mode().IsRegular() {
		return result, fmt.Errorf("SQLite maintenance path is not a regular file")
	}

	db, err := sql.Open(sqliteDriverName, path)
	if err != nil {
		return result, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		if sqliteMaintenanceCanSkip(ctx, err) {
			result.Reason = sqliteMaintenanceSkipReason(ctx, err)
			return result, nil
		}
		return result, err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `PRAGMA busy_timeout = 0`); err != nil {
		return sqliteMaintenanceOperationalResult(ctx, result, err)
	}
	if _, err := conn.ExecContext(ctx, `PRAGMA locking_mode = EXCLUSIVE`); err != nil {
		return sqliteMaintenanceOperationalResult(ctx, result, err)
	}
	if _, err := conn.ExecContext(ctx, `BEGIN EXCLUSIVE`); err != nil {
		return sqliteMaintenanceOperationalResult(ctx, result, err)
	}
	locked := true
	defer func() {
		if locked {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()
	if err := validateSQLiteMaintenanceDatabase(ctx, conn); err != nil {
		if ctx.Err() != nil {
			return sqliteMaintenanceOperationalResult(ctx, result, err)
		}
		return result, err
	}
	result.Before, err = readSQLiteSpaceUsage(ctx, conn, path)
	if err != nil {
		if ctx.Err() != nil {
			return sqliteMaintenanceOperationalResult(ctx, result, err)
		}
		return result, err
	}
	result.After = result.Before
	if reason := sqliteMaintenanceThresholdReason(result.Before, policy); reason != "" {
		result.Reason = reason
		if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
			return sqliteMaintenanceOperationalResult(ctx, result, err)
		}
		locked = false
		return result, nil
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return sqliteMaintenanceOperationalResult(ctx, result, err)
	}
	locked = false

	switch result.Before.AutoVacuum {
	case "none":
		available, availableErr := sqliteAvailableBytes(filepath.Dir(path))
		if availableErr != nil {
			result.Reason = "disk_space_unavailable"
			return result, nil
		}
		if available < result.Before.FileBytes {
			result.Reason = "insufficient_disk_space"
			return result, nil
		}
		if _, err := conn.ExecContext(ctx, `PRAGMA auto_vacuum = INCREMENTAL`); err != nil {
			return sqliteMaintenanceOperationalResult(ctx, result, err)
		}
		if _, err := conn.ExecContext(ctx, `VACUUM`); err != nil {
			return sqliteMaintenanceOperationalResult(ctx, result, err)
		}
		result.Action = SQLiteMaintenanceActionVacuum
	case "incremental":
		pages := sqliteIncrementalVacuumPages(result.Before, policy.RetainedFreeBytes)
		if pages <= 0 {
			result.Reason = "retained_free_space"
			return result, nil
		}
		if _, err := conn.ExecContext(ctx, fmt.Sprintf("PRAGMA incremental_vacuum(%d)", pages)); err != nil {
			return sqliteMaintenanceOperationalResult(ctx, result, err)
		}
		result.Action = SQLiteMaintenanceActionIncrementalVacuum
	case "full":
		result.Reason = "full_auto_vacuum"
		return result, nil
	default:
		return result, fmt.Errorf("unsupported SQLite auto_vacuum mode %q", result.Before.AutoVacuum)
	}
	result.After, err = readSQLiteSpaceUsage(ctx, conn, path)
	if err != nil {
		if ctx.Err() != nil {
			return sqliteMaintenanceOperationalResult(ctx, result, err)
		}
		return result, err
	}
	result.Reason = "reclaimed"
	return result, nil
}

type sqliteQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func validateSQLiteMaintenancePolicy(policy SQLiteMaintenancePolicy) error {
	if policy.MinimumFileBytes < 0 || policy.MinimumReclaimBytes < 0 || policy.RetainedFreeBytes < 0 {
		return fmt.Errorf("SQLite maintenance byte thresholds must not be negative")
	}
	if math.IsNaN(policy.MinimumReclaimRatio) || policy.MinimumReclaimRatio < 0 || policy.MinimumReclaimRatio > 1 {
		return fmt.Errorf("SQLite maintenance reclaim ratio must be between zero and one")
	}
	return nil
}

func validateSQLiteMaintenanceDatabase(ctx context.Context, query sqliteQueryer) error {
	var integrity string
	if err := query.QueryRowContext(ctx, `PRAGMA integrity_check(1)`).Scan(&integrity); err != nil {
		return err
	}
	if integrity != "ok" {
		return fmt.Errorf("SQLite integrity check failed: %s", integrity)
	}
	var tableCount, exactTableCount int
	if err := query.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`).Scan(&tableCount); err != nil {
		return err
	}
	if err := query.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name IN ('floret_backend_metadata', 'floret_backend_records')`).Scan(&exactTableCount); err != nil {
		return err
	}
	if tableCount != 2 || exactTableCount != 2 {
		return fmt.Errorf("database is not a Floret backend")
	}
	var physicalSchema []byte
	if err := query.QueryRowContext(ctx, `SELECT value FROM floret_backend_metadata WHERE name = 'physical_schema'`).Scan(&physicalSchema); err != nil {
		return fmt.Errorf("invalid Floret backend metadata: %w", err)
	}
	if !bytesEqualString(physicalSchema, "1") {
		return fmt.Errorf("unsupported Floret backend physical schema %q", physicalSchema)
	}
	return nil
}

func bytesEqualString(value []byte, want string) bool {
	return string(value) == want
}

func readSQLiteSpaceUsage(ctx context.Context, query sqliteQueryer, path string) (SQLiteSpaceUsage, error) {
	usage := SQLiteSpaceUsage{}
	var autoVacuum int64
	for _, item := range []struct {
		query string
		value *int64
	}{
		{`PRAGMA page_size`, &usage.PageSize},
		{`PRAGMA page_count`, &usage.PageCount},
		{`PRAGMA freelist_count`, &usage.FreePageCount},
		{`PRAGMA auto_vacuum`, &autoVacuum},
	} {
		if err := query.QueryRowContext(ctx, item.query).Scan(item.value); err != nil {
			return SQLiteSpaceUsage{}, err
		}
	}
	info, err := os.Stat(path)
	if err != nil {
		return SQLiteSpaceUsage{}, err
	}
	usage.FileBytes = info.Size()
	usage.ReclaimableBytes = usage.PageSize * usage.FreePageCount
	switch autoVacuum {
	case 0:
		usage.AutoVacuum = "none"
	case 1:
		usage.AutoVacuum = "full"
	case 2:
		usage.AutoVacuum = "incremental"
	default:
		usage.AutoVacuum = fmt.Sprintf("unknown:%d", autoVacuum)
	}
	return usage, nil
}

func sqliteMaintenanceThresholdReason(usage SQLiteSpaceUsage, policy SQLiteMaintenancePolicy) string {
	if usage.FileBytes < policy.MinimumFileBytes {
		return "file_below_threshold"
	}
	if usage.ReclaimableBytes < policy.MinimumReclaimBytes {
		return "reclaim_below_threshold"
	}
	if usage.FileBytes == 0 || float64(usage.ReclaimableBytes)/float64(usage.FileBytes) < policy.MinimumReclaimRatio {
		return "ratio_below_threshold"
	}
	return ""
}

func sqliteIncrementalVacuumPages(usage SQLiteSpaceUsage, retainedBytes int64) int64 {
	if usage.PageSize <= 0 || usage.FreePageCount <= 0 {
		return 0
	}
	retainedPages := (retainedBytes + usage.PageSize - 1) / usage.PageSize
	if retainedPages >= usage.FreePageCount {
		return 0
	}
	return usage.FreePageCount - retainedPages
}

func sqliteMaintenanceOperationalResult(ctx context.Context, result SQLiteMaintenanceResult, err error) (SQLiteMaintenanceResult, error) {
	if sqliteMaintenanceCanSkip(ctx, err) {
		result.Action = SQLiteMaintenanceActionNone
		result.Reason = sqliteMaintenanceSkipReason(ctx, err)
		return result, nil
	}
	return result, err
}

func sqliteMaintenanceCanSkip(ctx context.Context, err error) bool {
	if ctx != nil && ctx.Err() != nil {
		return true
	}
	var coded interface{ Code() int }
	if errors.As(err, &coded) {
		switch coded.Code() & 0xff {
		case 5, 6, 13: // SQLITE_BUSY, SQLITE_LOCKED, SQLITE_FULL
			return true
		}
	}
	return false
}

func sqliteMaintenanceSkipReason(ctx context.Context, err error) string {
	if ctx != nil && ctx.Err() != nil {
		return "maintenance_timeout"
	}
	var coded interface{ Code() int }
	if errors.As(err, &coded) {
		switch coded.Code() & 0xff {
		case 5, 6:
			return "database_busy"
		case 13:
			return "insufficient_disk_space"
		}
	}
	return "maintenance_unavailable"
}

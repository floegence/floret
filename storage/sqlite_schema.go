package storage

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/storage/spi"
)

// The physical format owns only table structure; record values remain opaque.
func inspectSQLitePhysicalSchema(ctx context.Context, query sqliteQueryer) (string, error) {
	var count int
	if err := query.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`).Scan(&count); err != nil {
		return "", err
	}
	if count != 2 {
		return "", ErrUnsupportedSQLiteFormat
	}
	var version string
	if err := query.QueryRowContext(ctx, `SELECT value FROM floret_backend_metadata WHERE name='physical_schema'`).Scan(&version); err != nil {
		return "", fmt.Errorf("%w: invalid backend metadata: %v", ErrUnsupportedSQLiteFormat, err)
	}
	if version != "1" && version != "2" {
		if number, err := strconv.Atoi(version); err == nil && number > 2 {
			return "", fmt.Errorf("%w: %w: version %q", ErrUnsupportedSQLiteFormat, ErrSQLiteTooNew, version)
		}
		return "", fmt.Errorf("%w: version %q", ErrUnsupportedSQLiteFormat, version)
	}
	for _, table := range []struct{ name, definition string }{
		{"floret_backend_metadata", "createtablefloret_backend_metadata(nametextprimarykey,valueblobnotnull)withoutrowid"},
		{"floret_backend_records", "createtablefloret_backend_records(namespacetextnotnull,keyblobnotnull,valueblobnotnull,primarykey(namespace,key))"},
	} {
		var definition string
		if err := query.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE type='table' AND name=?`, table.name).Scan(&definition); err != nil {
			return "", fmt.Errorf("%w: %v", ErrUnsupportedSQLiteFormat, err)
		}
		definition = strings.ToLower(strings.Join(strings.Fields(definition), ""))
		// SQLite quotes table names after ALTER TABLE RENAME.
		definition = strings.ReplaceAll(definition, `"`, "")
		want := table.definition
		if table.name == "floret_backend_records" && version == "1" {
			want += "withoutrowid"
		}
		if definition != want {
			return "", fmt.Errorf("%w: table %s layout drift", ErrUnsupportedSQLiteFormat, table.name)
		}
	}
	if err := query.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' OR (type='index' AND sql IS NOT NULL)`).Scan(&count); err != nil {
		return "", err
	}
	if count != 0 {
		return "", fmt.Errorf("%w: unexpected indexes or triggers", ErrUnsupportedSQLiteFormat)
	}
	return version, nil
}

func (tx *sqliteTx) PrepareFloretStorage(access storagebridge.StartupAccess) (bool, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if !tx.active {
		return false, spi.ErrTransactionClosed
	}
	if tx.readOnly {
		return false, spi.ErrInvalidArgument
	}
	version, err := inspectSQLitePhysicalSchema(tx.ctx, tx.tx)
	if err != nil || version == "2" {
		return false, err
	}
	if access.OnMigration != nil {
		access.OnMigration()
	}
	for _, statement := range []string{
		`CREATE TABLE floret_backend_records_next (namespace TEXT NOT NULL, key BLOB NOT NULL, value BLOB NOT NULL, PRIMARY KEY (namespace, key))`,
		`INSERT INTO floret_backend_records_next SELECT namespace, key, value FROM floret_backend_records`,
	} {
		if _, err := tx.tx.ExecContext(tx.ctx, statement); err != nil {
			return false, classifySQLiteError(tx.ctx, err)
		}
	}
	var mismatch int
	if err := tx.tx.QueryRowContext(tx.ctx, `SELECT (SELECT COUNT(*) FROM floret_backend_records) != (SELECT COUNT(*) FROM floret_backend_records_next) OR EXISTS(SELECT 1 FROM floret_backend_records old LEFT JOIN floret_backend_records_next new ON new.namespace=old.namespace AND new.key=old.key WHERE new.value IS NULL OR new.value != old.value)`).Scan(&mismatch); err != nil {
		return false, err
	}
	if mismatch != 0 {
		return false, fmt.Errorf("%w: physical copy mismatch", ErrSQLiteIntegrity)
	}
	for _, statement := range []string{
		`DROP TABLE floret_backend_records`,
		`ALTER TABLE floret_backend_records_next RENAME TO floret_backend_records`,
		`UPDATE floret_backend_metadata SET value=CAST('2' AS BLOB) WHERE name='physical_schema'`,
	} {
		if _, err := tx.tx.ExecContext(tx.ctx, statement); err != nil {
			return false, classifySQLiteError(tx.ctx, err)
		}
	}
	_, err = inspectSQLitePhysicalSchema(tx.ctx, tx.tx)
	return true, err
}

func (tx *sqliteTx) FloretPhysicalMigrationRequired(storagebridge.StartupAccess) (bool, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if !tx.active {
		return false, spi.ErrTransactionClosed
	}
	version, err := inspectSQLitePhysicalSchema(tx.ctx, tx.tx)
	return version == "1", err
}

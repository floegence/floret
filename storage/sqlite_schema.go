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

	var version string
	if err := query.QueryRowContext(ctx, `SELECT value FROM floret_backend_metadata WHERE name='physical_schema'`).Scan(&version); err != nil {
		return "", fmt.Errorf("%w: invalid backend metadata: %v", ErrUnsupportedSQLiteFormat, err)
	}
	if version != "1" && version != "2" && version != "3" {
		if number, err := strconv.Atoi(version); err == nil && number > 3 {
			return "", fmt.Errorf("%w: %w: version %q", ErrUnsupportedSQLiteFormat, ErrSQLiteTooNew, version)
		}
		return "", fmt.Errorf("%w: version %q", ErrUnsupportedSQLiteFormat, version)
	}
	expected := map[string]string{
		"floret_backend_metadata": "CREATE TABLE floret_backend_metadata (name TEXT PRIMARY KEY, value BLOB NOT NULL) WITHOUT ROWID",
		"floret_backend_records":  "CREATE TABLE floret_backend_records (namespace TEXT NOT NULL, key BLOB NOT NULL, value BLOB NOT NULL, PRIMARY KEY (namespace, key))",
	}
	if version == "1" {
		expected["floret_backend_records"] += " WITHOUT ROWID"
	}
	if version == "3" {
		expected["floret_index_documents"] = sqliteIndexDocuments
		expected["floret_index_threads"] = sqliteIndexThreads
		expected["floret_index_terms"] = sqliteIndexTerms
		expected["floret_index_terms_data"] = "CREATE TABLE 'floret_index_terms_data'(id INTEGER PRIMARY KEY, block BLOB)"
		expected["floret_index_terms_idx"] = "CREATE TABLE 'floret_index_terms_idx'(segid, term, pgno, PRIMARY KEY(segid, term)) WITHOUT ROWID"
		expected["floret_index_terms_docsize"] = "CREATE TABLE 'floret_index_terms_docsize'(id INTEGER PRIMARY KEY, sz BLOB, origin INTEGER)"
		expected["floret_index_terms_config"] = "CREATE TABLE 'floret_index_terms_config'(k PRIMARY KEY, v) WITHOUT ROWID"
	}
	rows, err := query.QueryContext(ctx, "SELECT name,sql FROM sqlite_master WHERE (type='table' AND name NOT LIKE 'sqlite_%') OR type='trigger' OR (type='index' AND sql IS NOT NULL)")
	if err != nil {
		return "", err
	}
	defer rows.Close()
	normalize := func(value string) string {
		return strings.NewReplacer("\"", "", "'", "").Replace(strings.ToLower(strings.Join(strings.Fields(value), "")))
	}
	for rows.Next() {
		var name, definition string
		if err := rows.Scan(&name, &definition); err != nil {
			return "", err
		}
		want, ok := expected[name]
		if !ok || normalize(definition) != normalize(want) {
			return "", fmt.Errorf("%w: object %s layout drift: %s", ErrUnsupportedSQLiteFormat, name, definition)
		}
		delete(expected, name)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	if len(expected) != 0 {
		return "", fmt.Errorf("%w: missing physical objects", ErrUnsupportedSQLiteFormat)
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
	if err != nil || version == "3" {
		return false, err
	}
	if access.OnMigration != nil {
		access.OnMigration()
	}
	if version == "1" {
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
	}
	for _, statement := range []string{sqliteIndexDocuments, sqliteIndexThreads, sqliteIndexTerms, "UPDATE floret_backend_metadata SET value=CAST('3' AS BLOB) WHERE name='physical_schema'"} {
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
	return version != "3", err
}

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
	if version != "1" && version != "2" && version != "3" && version != "4" && version != "5" {
		if number, err := strconv.Atoi(version); err == nil && number > 5 {
			return "", fmt.Errorf("%w: %w: version %q", ErrUnsupportedSQLiteFormat, ErrSQLiteTooNew, version)
		}
		return "", fmt.Errorf("%w: version %q", ErrUnsupportedSQLiteFormat, version)
	}
	expected := map[string]string{
		"floret_backend_metadata": "CREATE TABLE floret_backend_metadata (name TEXT PRIMARY KEY, value BLOB NOT NULL) WITHOUT ROWID",
		"floret_backend_records":  "CREATE TABLE floret_backend_records (namespace TEXT NOT NULL, key BLOB NOT NULL, value BLOB NOT NULL, PRIMARY KEY (namespace, key))",
	}
	if version == "3" || version == "4" {
		expected["floret_index_documents"] = sqliteIndexDocuments
		expected["floret_index_threads"] = sqliteIndexThreads
	}
	if version == "4" {
		expected["floret_index_postings"] = sqliteIndexPostings
		expected["floret_index_posting_documents"] = sqliteIndexPostingDocuments
	} else if version == "3" {
		expected["floret_index_terms"] = sqliteIndexTermsV3
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
		if name == "floret_backend_records" {
			rowidLayout := "CREATE TABLE floret_backend_records (namespace TEXT NOT NULL, key BLOB NOT NULL, value BLOB NOT NULL, PRIMARY KEY (namespace, key))"
			withoutRowidLayout := rowidLayout + " WITHOUT ROWID"
			if normalize(definition) == normalize(rowidLayout) || normalize(definition) == normalize(withoutRowidLayout) {
				delete(expected, name)
				continue
			}
		}
		if !ok || normalize(definition) != normalize(want) {
			return "", fmt.Errorf("%w: object %s layout drift: %s", ErrUnsupportedSQLiteFormat, name, definition)
		}
		delete(expected, name)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	if len(expected) != 0 {
		return "", fmt.Errorf("%w: missing physical objects %v", ErrUnsupportedSQLiteFormat, expected)
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
	if err != nil || version == "5" {
		return false, err
	}
	if access.OnMigration != nil {
		access.OnMigration()
	}
	if version == "1" {
		if _, err := tx.tx.ExecContext(tx.ctx, `UPDATE floret_backend_metadata SET value=CAST('2' AS BLOB) WHERE name='physical_schema'`); err != nil {
			return false, classifySQLiteError(tx.ctx, err)
		}
		version = "2"
	}
	if version == "2" {
		for _, statement := range []string{sqliteIndexDocuments, sqliteIndexThreads, sqliteIndexTermsV3, "UPDATE floret_backend_metadata SET value=CAST('3' AS BLOB) WHERE name='physical_schema'"} {
			if _, err := tx.tx.ExecContext(tx.ctx, statement); err != nil {
				return false, classifySQLiteError(tx.ctx, err)
			}
		}
		version = "3"
	}
	// Preserve the released 3 -> 4 edge before retiring its derived state.
	if version != "4" {
		for _, statement := range []string{"DROP TABLE floret_index_terms", "DELETE FROM floret_index_documents", sqliteIndexPostings, sqliteIndexPostingDocuments, "UPDATE floret_backend_metadata SET value=CAST('4' AS BLOB) WHERE name='physical_schema'"} {
			if _, err := tx.tx.ExecContext(tx.ctx, statement); err != nil {
				return false, classifySQLiteError(tx.ctx, err)
			}
		}
	}
	// Format 5 removes only disposable search tables. Canonical bytes stay exact.
	for _, statement := range []string{"DROP TABLE floret_index_postings", "DROP TABLE floret_index_documents", "UPDATE floret_backend_metadata SET value=CAST('5' AS BLOB) WHERE name='physical_schema'"} {
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
	return version != "5", err
}

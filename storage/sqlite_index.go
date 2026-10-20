package storage

import (
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/floegence/floret/v7/storage/spi"
	"strings"
)

const sqliteIndexDocuments = "CREATE TABLE floret_index_documents (rowid INTEGER PRIMARY KEY, document_key TEXT NOT NULL UNIQUE, thread_id TEXT NOT NULL, object TEXT NOT NULL, metadata BLOB NOT NULL)"
const sqliteIndexThreads = "CREATE INDEX floret_index_threads ON floret_index_documents(thread_id, object)"
const sqliteIndexTerms = "CREATE VIRTUAL TABLE floret_index_terms USING fts5(tokens, content='', contentless_delete=1, detail=none, tokenize='ascii')"

func validIndexTerm(term string) bool {
	if term == "" {
		return false
	}
	for _, ch := range term {
		if !(ch >= 'a' && ch <= 'z' || ch >= '0' && ch <= '9') {
			return false
		}
	}
	return true
}

func (tx *sqliteTx) PutIndexDocument(document spi.IndexDocument) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.validateWrite("index", []byte(document.Key)); err != nil {
		return err
	}
	if document.ThreadID == "" || document.Object == "" || len(document.Metadata) == 0 {
		return spi.ErrInvalidArgument
	}
	for _, term := range document.Terms {
		if !validIndexTerm(term) {
			return spi.ErrInvalidArgument
		}
	}
	var rowid int64
	err := tx.tx.QueryRowContext(tx.ctx, "INSERT INTO floret_index_documents(document_key,thread_id,object,metadata) VALUES(?,?,?,?) ON CONFLICT(document_key) DO UPDATE SET thread_id=excluded.thread_id,object=excluded.object,metadata=excluded.metadata RETURNING rowid", document.Key, document.ThreadID, document.Object, document.Metadata).Scan(&rowid)
	if err != nil {
		return classifySQLiteError(tx.ctx, err)
	}
	if _, err = tx.tx.ExecContext(tx.ctx, "DELETE FROM floret_index_terms WHERE rowid=?", rowid); err != nil {
		return classifySQLiteError(tx.ctx, err)
	}
	_, err = tx.tx.ExecContext(tx.ctx, "INSERT INTO floret_index_terms(rowid,tokens) VALUES(?,?)", rowid, strings.Join(document.Terms, " "))
	return classifySQLiteError(tx.ctx, err)
}

func (tx *sqliteTx) DeleteIndexThread(threadID string) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.validateWrite("index", []byte(threadID)); err != nil {
		return err
	}
	if _, err := tx.tx.ExecContext(tx.ctx, "DELETE FROM floret_index_terms WHERE rowid IN (SELECT rowid FROM floret_index_documents WHERE thread_id=?)", threadID); err != nil {
		return classifySQLiteError(tx.ctx, err)
	}
	_, err := tx.tx.ExecContext(tx.ctx, "DELETE FROM floret_index_documents WHERE thread_id=?", threadID)
	return classifySQLiteError(tx.ctx, err)
}

func (tx *sqliteTx) QueryIndex(query spi.IndexQuery) ([]spi.IndexDocument, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.usable(); err != nil {
		return nil, err
	}
	if len(query.ThreadIDs) == 0 {
		return nil, nil
	}
	var groups []string
	for _, group := range query.Terms {
		if len(group) == 0 {
			return nil, nil
		}
		for _, term := range group {
			if !validIndexTerm(term) {
				return nil, spi.ErrInvalidArgument
			}
		}
		groups = append(groups, "("+strings.Join(group, " OR ")+")")
	}
	// json_each avoids SQLite's bind-variable limit for authorized scopes.
	scope, err := json.Marshal(query.ThreadIDs)
	if err != nil {
		return nil, err
	}
	statement := "SELECT d.document_key,d.thread_id,d.object,d.metadata FROM floret_index_documents d WHERE d.thread_id IN (SELECT value FROM json_each(?))"
	args := []any{string(scope)}
	if query.Object != "" {
		statement += " AND d.object=?"
		args = append(args, query.Object)
	}
	if len(groups) > 0 {
		statement += " AND d.rowid IN (SELECT rowid FROM floret_index_terms WHERE floret_index_terms MATCH ?)"
		args = append(args, strings.Join(groups, " AND "))
	}
	statement += " ORDER BY d.document_key"
	rows, err := tx.tx.QueryContext(tx.ctx, statement, args...)
	if err != nil {
		return nil, classifySQLiteError(tx.ctx, err)
	}
	defer rows.Close()
	var result []spi.IndexDocument
	for rows.Next() {
		var document spi.IndexDocument
		if err := rows.Scan(&document.Key, &document.ThreadID, &document.Object, &document.Metadata); err != nil {
			return nil, classifySQLiteError(tx.ctx, err)
		}
		result = append(result, document)
	}
	return result, classifySQLiteError(tx.ctx, rows.Err())
}

var _ spi.IndexWriter = (*sqliteTx)(nil)

func (tx *sqliteTx) CheckIndexIntegrity() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.validateWrite("index", []byte("integrity")); err != nil {
		return err
	}
	if _, err := tx.tx.ExecContext(tx.ctx, "INSERT INTO floret_index_terms(floret_index_terms) VALUES('integrity-check')"); err != nil {
		return classifySQLiteError(tx.ctx, err)
	}
	var mismatch int
	if err := tx.tx.QueryRowContext(tx.ctx, "SELECT EXISTS(SELECT rowid FROM floret_index_documents EXCEPT SELECT id FROM floret_index_terms_docsize) OR EXISTS(SELECT id FROM floret_index_terms_docsize EXCEPT SELECT rowid FROM floret_index_documents)").Scan(&mismatch); err != nil {
		return classifySQLiteError(tx.ctx, err)
	}
	if mismatch != 0 {
		return spi.ErrIndexCorrupt
	}
	return nil
}

func (tx *sqliteTx) ResetIndex() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.validateWrite("index", []byte("reset")); err != nil {
		return err
	}
	for _, statement := range []string{"DROP TABLE floret_index_terms", sqliteIndexTerms, "DELETE FROM floret_index_documents"} {
		if _, err := tx.tx.ExecContext(tx.ctx, statement); err != nil {
			return classifySQLiteError(tx.ctx, err)
		}
	}
	return nil
}

func (tx *sqliteTx) ReadIndexDocument(threadID, key string) (spi.IndexDocument, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.usable(); err != nil {
		return spi.IndexDocument{}, err
	}
	var document spi.IndexDocument
	err := tx.tx.QueryRowContext(tx.ctx, "SELECT document_key,thread_id,object,metadata FROM floret_index_documents WHERE document_key=? AND thread_id=?", key, threadID).Scan(&document.Key, &document.ThreadID, &document.Object, &document.Metadata)
	if errors.Is(err, sql.ErrNoRows) {
		return spi.IndexDocument{}, spi.ErrNotFound
	}
	return document, classifySQLiteError(tx.ctx, err)
}

var _ spi.IndexDocumentReader = (*sqliteTx)(nil)

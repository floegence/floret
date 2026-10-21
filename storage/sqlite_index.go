package storage

import (
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/floegence/floret/v7/storage/spi"
)

const sqliteIndexDocuments = "CREATE TABLE floret_index_documents (rowid INTEGER PRIMARY KEY, document_key TEXT NOT NULL UNIQUE, thread_id TEXT NOT NULL, object TEXT NOT NULL, metadata BLOB NOT NULL)"
const sqliteIndexThreads = "CREATE INDEX floret_index_threads ON floret_index_documents(thread_id, object)"
const sqliteIndexTermsV3 = "CREATE VIRTUAL TABLE floret_index_terms USING fts5(tokens, content='', contentless_delete=1, detail=none, tokenize='ascii')"

const sqliteIndexPostings = "CREATE TABLE floret_index_postings (term TEXT NOT NULL, document_rowid INTEGER NOT NULL, PRIMARY KEY (term, document_rowid)) WITHOUT ROWID"
const sqliteIndexPostingDocuments = "CREATE INDEX floret_index_posting_documents ON floret_index_postings(document_rowid)"

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
	if _, err = tx.tx.ExecContext(tx.ctx, "DELETE FROM floret_index_postings WHERE document_rowid=?", rowid); err != nil {
		return classifySQLiteError(tx.ctx, err)
	}
	return tx.putIndexTerms(rowid, document.Terms)
}

func (tx *sqliteTx) DeleteIndexThread(threadID string) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.validateWrite("index", []byte(threadID)); err != nil {
		return err
	}
	if _, err := tx.tx.ExecContext(tx.ctx, "DELETE FROM floret_index_postings WHERE document_rowid IN (SELECT rowid FROM floret_index_documents WHERE thread_id=?)", threadID); err != nil {
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
	var groups [][]string
	for _, group := range query.Terms {
		if len(group) == 0 {
			return nil, nil
		}
		for _, term := range group {
			if !validIndexTerm(term) {
				return nil, spi.ErrInvalidArgument
			}
		}
		groups = append(groups, group)
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
	for _, group := range groups {
		raw, err := json.Marshal(group)
		if err != nil {
			return nil, err
		}
		statement += " AND d.rowid IN (SELECT document_rowid FROM floret_index_postings WHERE term IN (SELECT value FROM json_each(?)))"
		args = append(args, string(raw))
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
	var mismatch int
	if err := tx.tx.QueryRowContext(tx.ctx, "SELECT EXISTS(SELECT 1 FROM floret_index_postings p LEFT JOIN floret_index_documents d ON d.rowid=p.document_rowid WHERE d.rowid IS NULL) OR EXISTS(SELECT 1 FROM floret_index_documents d WHERE NOT EXISTS(SELECT 1 FROM floret_index_postings p WHERE p.term='' AND p.document_rowid=d.rowid))").Scan(&mismatch); err != nil {
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
	for _, statement := range []string{"DELETE FROM floret_index_postings", "DELETE FROM floret_index_documents"} {
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

func (tx *sqliteTx) putIndexTerms(rowid int64, terms []string) error {
	// One private empty posting preserves document inventory even for empty
	// term sets. Public terms remain nonempty; the sentinel is never queried.
	if _, err := tx.tx.ExecContext(tx.ctx, "INSERT OR IGNORE INTO floret_index_postings(term,document_rowid) VALUES('',?)", rowid); err != nil {
		return classifySQLiteError(tx.ctx, err)
	}
	raw, err := json.Marshal(terms)
	if err != nil {
		return err
	}
	_, err = tx.tx.ExecContext(tx.ctx, "INSERT OR IGNORE INTO floret_index_postings(term,document_rowid) SELECT value,? FROM json_each(?)", rowid, string(raw))
	return classifySQLiteError(tx.ctx, err)
}

func (tx *sqliteTx) AppendIndexDocument(document spi.IndexDocument) error {
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
	err := tx.tx.QueryRowContext(tx.ctx, "UPDATE floret_index_documents SET metadata=? WHERE document_key=? AND thread_id=? AND object=? RETURNING rowid", document.Metadata, document.Key, document.ThreadID, document.Object).Scan(&rowid)
	if errors.Is(err, sql.ErrNoRows) {
		return spi.ErrNotFound
	}
	if err != nil {
		return classifySQLiteError(tx.ctx, err)
	}
	return tx.putIndexTerms(rowid, document.Terms)
}

var _ spi.IndexDocumentAppender = (*sqliteTx)(nil)

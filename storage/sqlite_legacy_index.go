package storage

// Released physical layouts are retained only for transactional migration.
const sqliteIndexDocuments = "CREATE TABLE floret_index_documents (rowid INTEGER PRIMARY KEY, document_key TEXT NOT NULL UNIQUE, thread_id TEXT NOT NULL, object TEXT NOT NULL, metadata BLOB NOT NULL)"
const sqliteIndexThreads = "CREATE INDEX floret_index_threads ON floret_index_documents(thread_id, object)"
const sqliteIndexTermsV3 = "CREATE VIRTUAL TABLE floret_index_terms USING fts5(tokens, content='', contentless_delete=1, detail=none, tokenize='ascii')"

const sqliteIndexPostings = "CREATE TABLE floret_index_postings (term TEXT NOT NULL, document_rowid INTEGER NOT NULL, PRIMARY KEY (term, document_rowid)) WITHOUT ROWID"
const sqliteIndexPostingDocuments = "CREATE INDEX floret_index_posting_documents ON floret_index_postings(document_rowid)"

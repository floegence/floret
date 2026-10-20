package spi

import "errors"

// IndexDocument is an opaque, rebuildable projection of canonical records.
// Metadata contains source locators, never a second canonical transcript.
// Terms are atomic ASCII tokens supplied by the canonical owner.
type IndexDocument struct {
	Key      string
	ThreadID string
	Object   string
	Metadata []byte
	Terms    []string
}

// IndexQuery restricts candidates before canonical exact matching. Each inner
// Terms group is an OR; all groups must match. Empty groups match nothing.
type IndexQuery struct {
	ThreadIDs []string
	Object    string
	Terms     [][]string
}

// IndexReader is an optional transaction-local physical index capability.
// A backend that implements it must not silently substitute another reader
// after an index error. Returned documents need not contain Terms.
type IndexReader interface {
	QueryIndex(IndexQuery) ([]IndexDocument, error)
}

// IndexDocumentReader optionally reads one derived document without scanning a
// thread's index. Both the exact key and ThreadID must match; keys grant no
// access. A missing key or a different scope returns ErrNotFound. Other errors
// remain explicit and must not cause a second read through another reader.
type IndexDocumentReader interface {
	ReadIndexDocument(threadID, key string) (IndexDocument, error)
}

// IndexWriter maintains projections in the same transaction as their canonical
// sources. It is optional; Floret supplies a KV adapter for other backends.
type IndexWriter interface {
	IndexReader
	PutIndexDocument(IndexDocument) error
	DeleteIndexThread(string) error
}

// IndexIntegrityChecker optionally checks derived physical index integrity in
// the active write transaction. Errors must remain explicit to the owner.
type IndexIntegrityChecker interface {
	CheckIndexIntegrity() error
}

// ErrIndexCorrupt identifies damage confirmed to be confined to derived index
// inventory. Canonical or unknown physical corruption must not use this error.
var ErrIndexCorrupt = errors.New("derived storage index is corrupt")

// IndexResetter clears only rebuildable physical index state in the active
// transaction after the canonical owner has validated its complete sources.
type IndexResetter interface{ ResetIndex() error }

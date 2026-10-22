package spi

import "errors"

// IndexDocument is an opaque, rebuildable projection of canonical records.
// Metadata contains source locators, never a second canonical transcript.
// Terms are atomic ASCII tokens supplied by the canonical owner.
// Deprecated: retained for v7 source compatibility. Floret no longer builds or calls physical text indexes.
type IndexDocument struct {
	Key      string
	ThreadID string
	Object   string
	Metadata []byte
	Terms    []string
}

// IndexQuery restricts candidates before canonical exact matching. Each inner
// Terms group is an OR; all groups must match. Empty groups match nothing.
// Deprecated: retained for v7 source compatibility. Floret no longer builds or calls physical text indexes.
type IndexQuery struct {
	ThreadIDs []string
	Object    string
	Terms     [][]string
}

// IndexReader is an optional transaction-local physical index capability.
// A backend that implements it must not silently substitute another reader
// after an index error. Returned documents need not contain Terms.
// Deprecated: retained for v7 source compatibility. Floret no longer builds or calls physical text indexes.
type IndexReader interface {
	QueryIndex(IndexQuery) ([]IndexDocument, error)
}

// IndexDocumentReader optionally reads one derived document without scanning a
// thread's index. Both the exact key and ThreadID must match; keys grant no
// access. A missing key or a different scope returns ErrNotFound. Other errors
// remain explicit and must not cause a second read through another reader.
// Deprecated: retained for v7 source compatibility. Floret no longer builds or calls physical text indexes.
type IndexDocumentReader interface {
	ReadIndexDocument(threadID, key string) (IndexDocument, error)
}

// IndexWriter maintains projections in the same transaction as their canonical
// sources. It was optional in the retired physical indexing implementation.
// Deprecated: retained for v7 source compatibility. Floret no longer builds or calls physical text indexes.
type IndexWriter interface {
	IndexReader
	PutIndexDocument(IndexDocument) error
	DeleteIndexThread(string) error
}

// IndexIntegrityChecker optionally checks derived physical index integrity in
// the active write transaction. Errors must remain explicit to the owner.
// Deprecated: retained for v7 source compatibility. Floret no longer builds or calls physical text indexes.
type IndexIntegrityChecker interface {
	CheckIndexIntegrity() error
}

// ErrIndexCorrupt identifies damage confirmed to be confined to derived index
// inventory. Canonical or unknown physical corruption must not use this error.
// Deprecated: retained for v7 source compatibility; Floret no longer maintains physical text indexes.
var ErrIndexCorrupt = errors.New("derived storage index is corrupt")

// IndexResetter clears only rebuildable physical index state in the active
// transaction after the canonical owner has validated its complete sources.
// Deprecated: retained for v7 source compatibility. Floret no longer builds or calls physical text indexes.
type IndexResetter interface{ ResetIndex() error }

// IndexDocumentAppender optionally adds atomic terms to an existing document
// while replacing its metadata in the same transaction. Existing terms remain.
// The exact Key, ThreadID and Object must match; otherwise return ErrNotFound.
// Terms are a delta, not a complete replacement. Errors must not be retried
// through another index implementation.
// Deprecated: retained for v7 source compatibility. Floret no longer builds or calls physical text indexes.
type IndexDocumentAppender interface {
	AppendIndexDocument(IndexDocument) error
}

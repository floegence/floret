// Package storage defines the transactional persistence boundary used by Floret.
// Backends store opaque namespaced bytes; Floret owns every domain schema and
// index encoded into those bytes.
package storage

import (
	"context"
	"errors"
)

var (
	// ErrNotFound reports that a key does not exist in a namespace.
	ErrNotFound = errors.New("storage record not found")
	// ErrClosed reports an operation attempted on a closed backend.
	ErrClosed = errors.New("storage backend closed")
	// ErrConflict reports that a transaction could not commit because another
	// transaction committed after its snapshot was taken.
	ErrConflict = errors.New("storage transaction conflict")
	// ErrInvalidArgument reports a request outside the storage contract.
	ErrInvalidArgument = errors.New("invalid storage argument")
	// ErrTransactionClosed reports use of a transaction outside its callback.
	ErrTransactionClosed = errors.New("storage transaction closed")
)

// Source opens a fresh Backend. A Source may be retained and opened more than
// once; each returned Backend has an independent lifecycle.
type Source interface {
	Open(context.Context) (Backend, error)
}

// Backend provides snapshot reads and serializable writes over opaque records.
// Implementations must invoke an Update callback exactly once and must never
// retry it implicitly.
type Backend interface {
	View(context.Context, func(ReadTx) error) error
	Update(context.Context, func(WriteTx) error) error
	Close() error
}

// ReadTx is a snapshot that is valid only for the duration of its callback.
type ReadTx interface {
	Get(namespace string, key []byte) ([]byte, error)
	Scan(ScanRequest) (ScanPage, error)
}

// WriteTx is a serializable transaction that is valid only for the duration
// of its callback.
type WriteTx interface {
	ReadTx
	Put(namespace string, key, value []byte) error
	Delete(namespace string, key []byte) error
}

// ScanRequest selects a bounded lexicographic page from one namespace. Start
// is inclusive, End is exclusive, and After is exclusive. Limit must be
// positive.
type ScanRequest struct {
	Namespace string
	Start     []byte
	End       []byte
	After     []byte
	Limit     int
}

// Record is one key and value returned by Scan. Both byte slices are owned by
// the caller.
type Record struct {
	Key   []byte
	Value []byte
}

// ScanPage is one ordered page. Next is the exclusive After cursor for the
// following request and is present only when HasMore is true.
type ScanPage struct {
	Records []Record
	HasMore bool
	Next    []byte
}

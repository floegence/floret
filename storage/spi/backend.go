// Package spi defines the advanced physical storage implementation contract.
// Its opaque records are Floret-owned; hosts must not inspect them to query or
// reconstruct Agent lifecycle state.
package spi

import (
	"context"
	"errors"
)

var (
	ErrNotFound          = errors.New("storage record not found")
	ErrClosed            = errors.New("storage backend closed")
	ErrConflict          = errors.New("storage transaction conflict")
	ErrInvalidArgument   = errors.New("invalid storage argument")
	ErrTransactionClosed = errors.New("storage transaction closed")
	ErrMigrationRequired = errors.New("storage migration required")
)

// Source opens an independently owned Backend. Implementations must return a
// fresh backend on every successful call.
type Source interface {
	Open(context.Context) (Backend, error)
}

type Backend interface {
	View(context.Context, func(ReadTx) error) error
	Update(context.Context, func(WriteTx) error) error
	Close() error
}

// MaintenanceResetter is an optional backend capability used only while no
// runtime Host owns the backend. Reset removes every Floret-owned opaque
// record in one backend transaction and reports whether any record existed.
type MaintenanceResetter interface {
	ResetFloretStorage(context.Context) (bool, error)
}

type ReadTx interface {
	Get(namespace string, key []byte) ([]byte, error)
	Scan(ScanRequest) (ScanPage, error)
}

type WriteTx interface {
	ReadTx
	Put(namespace string, key, value []byte) error
	Delete(namespace string, key []byte) error
}

type ScanRequest struct {
	Namespace string
	Start     []byte
	End       []byte
	After     []byte
	Limit     int
}

type Record struct {
	Key   []byte
	Value []byte
}

type ScanPage struct {
	Records []Record
	HasMore bool
	Next    []byte
}

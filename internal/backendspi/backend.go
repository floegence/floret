// Package backendspi defines the internal transaction port consumed by the
// domain kernel. Runtime adapts the public storage contract at composition.
package backendspi

import (
	"context"
	"errors"
)

var ErrNotFound = errors.New("backend record not found")

type Backend interface {
	View(context.Context, func(ReadTx) error) error
	Update(context.Context, func(WriteTx) error) error
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

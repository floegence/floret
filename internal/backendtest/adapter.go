// Package backendtest adapts public backends for internal domain tests.
package backendtest

import (
	"context"
	"errors"

	"github.com/floegence/floret/v2/internal/backendspi"
	publicstorage "github.com/floegence/floret/v2/storage"
)

// Adapt returns the internal transaction port for a public Backend fixture.
func Adapt(backend publicstorage.Backend) backendspi.Backend {
	return adapter{backend: backend}
}

type adapter struct{ backend publicstorage.Backend }

func (value adapter) View(ctx context.Context, callback func(backendspi.ReadTx) error) error {
	return value.backend.View(ctx, func(tx publicstorage.ReadTx) error { return callback(readTx{tx: tx}) })
}

func (value adapter) Update(ctx context.Context, callback func(backendspi.WriteTx) error) error {
	return value.backend.Update(ctx, func(tx publicstorage.WriteTx) error {
		return callback(writeTx{readTx: readTx{tx: tx}, tx: tx})
	})
}

type readTx struct{ tx publicstorage.ReadTx }

func (value readTx) Get(namespace string, key []byte) ([]byte, error) {
	result, err := value.tx.Get(namespace, key)
	if errors.Is(err, publicstorage.ErrNotFound) {
		return nil, backendspi.ErrNotFound
	}
	return result, err
}

func (value readTx) Scan(request backendspi.ScanRequest) (backendspi.ScanPage, error) {
	page, err := value.tx.Scan(publicstorage.ScanRequest{
		Namespace: request.Namespace, Start: request.Start, End: request.End,
		After: request.After, Limit: request.Limit,
	})
	if err != nil {
		return backendspi.ScanPage{}, err
	}
	records := make([]backendspi.Record, len(page.Records))
	for index, record := range page.Records {
		records[index] = backendspi.Record{Key: record.Key, Value: record.Value}
	}
	return backendspi.ScanPage{Records: records, HasMore: page.HasMore, Next: page.Next}, nil
}

type writeTx struct {
	readTx
	tx publicstorage.WriteTx
}

func (value writeTx) Put(namespace string, key, payload []byte) error {
	return value.tx.Put(namespace, key, payload)
}

func (value writeTx) Delete(namespace string, key []byte) error {
	err := value.tx.Delete(namespace, key)
	if errors.Is(err, publicstorage.ErrNotFound) {
		return backendspi.ErrNotFound
	}
	return err
}

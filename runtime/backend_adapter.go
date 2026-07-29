package runtime

import (
	"context"
	"errors"

	"github.com/floegence/floret/v2/internal/backendspi"
	publicstorage "github.com/floegence/floret/v2/storage"
)

type domainBackendAdapter struct {
	backend publicstorage.Backend
}

func adaptDomainBackend(backend publicstorage.Backend) backendspi.Backend {
	return domainBackendAdapter{backend: backend}
}

func (adapter domainBackendAdapter) View(ctx context.Context, callback func(backendspi.ReadTx) error) error {
	return adapter.backend.View(ctx, func(tx publicstorage.ReadTx) error {
		return callback(domainReadTxAdapter{tx: tx})
	})
}

func (adapter domainBackendAdapter) Update(ctx context.Context, callback func(backendspi.WriteTx) error) error {
	return adapter.backend.Update(ctx, func(tx publicstorage.WriteTx) error {
		return callback(domainWriteTxAdapter{domainReadTxAdapter: domainReadTxAdapter{tx: tx}, tx: tx})
	})
}

type domainReadTxAdapter struct {
	tx publicstorage.ReadTx
}

func (adapter domainReadTxAdapter) Get(namespace string, key []byte) ([]byte, error) {
	value, err := adapter.tx.Get(namespace, key)
	if errors.Is(err, publicstorage.ErrNotFound) {
		return nil, backendspi.ErrNotFound
	}
	return value, err
}

func (adapter domainReadTxAdapter) Scan(request backendspi.ScanRequest) (backendspi.ScanPage, error) {
	page, err := adapter.tx.Scan(publicstorage.ScanRequest{
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

type domainWriteTxAdapter struct {
	domainReadTxAdapter
	tx publicstorage.WriteTx
}

func (adapter domainWriteTxAdapter) Put(namespace string, key, value []byte) error {
	return adapter.tx.Put(namespace, key, value)
}

func (adapter domainWriteTxAdapter) Delete(namespace string, key []byte) error {
	err := adapter.tx.Delete(namespace, key)
	if errors.Is(err, publicstorage.ErrNotFound) {
		return backendspi.ErrNotFound
	}
	return err
}

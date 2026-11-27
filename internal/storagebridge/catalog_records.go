package storagebridge

import (
	"context"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/storage/spi"
)

type CatalogRecord struct {
	Namespace string
	Key       []byte
}

type ThreadCreation struct{ Thread identity.ThreadID }

type ForkWriter func(source spi.ReadTx, target spi.WriteTx, independent bool) error

type TreeDeletion func(spi.WriteTx, func(identity.ThreadID) error) error

func DeleteTree(ctx context.Context, backend spi.Backend, write TreeDeletion) error {
	if scoped, ok := backend.(interface {
		DeleteThreadFiles(context.Context, TreeDeletion) error
	}); ok {
		return scoped.DeleteThreadFiles(ctx, write)
	}
	return backend.Update(ctx, func(tx spi.WriteTx) error { return write(tx, nil) })
}

func ForkThread(ctx context.Context, backend spi.Backend, source, target identity.ThreadID, write ForkWriter) error {
	if scoped, ok := backend.(interface {
		ForkThreadRecords(context.Context, identity.ThreadID, identity.ThreadID, ForkWriter) error
	}); ok {
		return scoped.ForkThreadRecords(ctx, source, target, write)
	}
	return backend.Update(ctx, func(tx spi.WriteTx) error { return write(tx, tx, false) })
}

func CreateThread(ctx context.Context, backend spi.Backend, request ThreadCreation, write func(spi.WriteTx) error) error {
	if scoped, ok := backend.(interface {
		CreateThreadRecords(context.Context, ThreadCreation, func(spi.WriteTx) error) error
	}); ok {
		return scoped.CreateThreadRecords(ctx, request, write)
	}
	return backend.Update(ctx, write)
}

func CatalogReader(tx spi.ReadTx) spi.ReadTx {
	if scoped, ok := tx.(interface{ CatalogReader() spi.ReadTx }); ok {
		return scoped.CatalogReader()
	}
	return tx
}

func CatalogWriter(tx spi.WriteTx) spi.WriteTx {
	if scoped, ok := tx.(interface{ CatalogWriter() spi.WriteTx }); ok {
		return scoped.CatalogWriter()
	}
	return tx
}

func PutIndexedThreadRecord(tx spi.WriteTx, record CatalogRecord, value []byte) error {
	if err := tx.Put(record.Namespace, record.Key, value); err != nil {
		return err
	}
	if scoped, ok := tx.(interface{ CatalogWriter() spi.WriteTx }); ok {
		return scoped.CatalogWriter().Put(record.Namespace, record.Key, value)
	}
	return nil
}

func DeleteIndexedThreadRecord(tx spi.WriteTx, record CatalogRecord) error {
	if err := tx.Delete(record.Namespace, record.Key); err != nil {
		return err
	}
	if scoped, ok := tx.(interface{ CatalogWriter() spi.WriteTx }); ok {
		return scoped.CatalogWriter().Delete(record.Namespace, record.Key)
	}
	return nil
}

func PutCatalogRecord(tx spi.WriteTx, record CatalogRecord, value []byte) error {
	if catalog, ok := tx.(interface {
		PutCatalogRecord(CatalogRecord, []byte) error
	}); ok {
		return catalog.PutCatalogRecord(record, value)
	}
	return tx.Put(record.Namespace, record.Key, value)
}

package storagebridge

import (
	"context"
	"errors"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/storage/spi"
)

type ThreadRead struct{ Thread identity.ThreadID }

var ErrRouteMissing = errors.New("no storage route for thread")
var ErrRouteDeleted = errors.New("storage route for thread deleted")

func ViewThread(ctx context.Context, backend spi.Backend, request ThreadRead, read func(spi.ReadTx) error) error {
	if scoped, ok := backend.(interface {
		ViewThreadRecords(context.Context, ThreadRead, func(spi.ReadTx) error) error
	}); ok {
		return scoped.ViewThreadRecords(ctx, request, read)
	}
	return backend.View(ctx, read)
}

func UpdateThread(ctx context.Context, backend spi.Backend, thread identity.ThreadID, write func(spi.WriteTx) error) error {
	if scoped, ok := backend.(interface {
		UpdateThreadRecords(context.Context, identity.ThreadID, func(spi.WriteTx) error) error
	}); ok {
		return scoped.UpdateThreadRecords(ctx, thread, write)
	}
	return backend.Update(ctx, write)
}

func UpdateRelatedThread(ctx context.Context, backend spi.Backend, thread, related identity.ThreadID, write func(spi.WriteTx) error) error {
	if related != "" {
		if scoped, ok := backend.(interface {
			UpdateRelatedThread(context.Context, identity.ThreadID, identity.ThreadID, func(spi.WriteTx) error) error
		}); ok {
			return scoped.UpdateRelatedThread(ctx, thread, related, write)
		}
	}
	return UpdateThread(ctx, backend, thread, write)
}

func RelatedWriter(tx spi.WriteTx, thread identity.ThreadID) (spi.WriteTx, error) {
	if scoped, ok := tx.(interface {
		RelatedWriter(identity.ThreadID) (spi.WriteTx, error)
	}); ok {
		return scoped.RelatedWriter(thread)
	}
	return tx, nil
}

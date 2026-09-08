// Package storagebridge opens public opaque storage values inside Floret.
package storagebridge

import (
	"context"
	"errors"

	"github.com/floegence/floret/v7/storage/spi"
)

// Source intentionally exposes no methods. The public storage.Source is a
// distinct type with this representation.
type Source struct {
	source spi.Source
}

func New(source spi.Source) Source {
	return Source{source: source}
}

func Open(ctx context.Context, source Source) (spi.Backend, error) {
	if source.source == nil {
		return nil, errors.New("storage source is required")
	}
	return source.source.Open(ctx)
}

// OpenReadOnly never falls back to Source.Open, which could initialize a store.
func OpenReadOnly(ctx context.Context, source Source) (spi.Backend, error) {
	reader, ok := source.source.(interface {
		OpenReadOnly(context.Context) (spi.Backend, error)
	})
	if !ok {
		return nil, errors.New("storage source does not support read-only inspection")
	}
	return reader.OpenReadOnly(ctx)
}

// Package storagebridge opens public opaque storage values inside Floret.
package storagebridge

import (
	"context"
	"errors"

	"github.com/floegence/floret/v3/storage/spi"
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

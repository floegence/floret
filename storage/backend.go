// Package storage provides opaque storage values for ordinary Floret hosts.
// Transactional backend implementation belongs to the advanced storage/spi
// package and cannot be used to query a runtime-owned Source.
package storage

import (
	"github.com/floegence/floret/v4/internal/storagebridge"
	"github.com/floegence/floret/v4/storage/spi"
)

// Source is an opaque storage configuration consumed exclusively by
// runtime.Open. It deliberately has no exported methods.
type Source storagebridge.Source

// NewSource seals an advanced backend implementation for use by runtime.Open.
// Applications that use only official storage constructors do not need this
// function.
func NewSource(source spi.Source) Source {
	return Source(storagebridge.New(source))
}

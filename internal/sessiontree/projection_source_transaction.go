package sessiontree

import (
	"bytes"

	"github.com/floegence/floret/v7/storage/spi"
)

// The dependency closure reaches the same immutable source via several exact
// identities. Share successful reads only within its existing read transaction;
// no data or authority survives a branch change or the end of that transaction.
type projectionSourceTx struct {
	spi.ReadTx
	values map[projectionReadKey][]byte
}
type projectionReadKey struct{ namespace, key string }

func newProjectionSourceTx(tx spi.ReadTx) *projectionSourceTx {
	return &projectionSourceTx{ReadTx: tx, values: map[projectionReadKey][]byte{}}
}

func (tx *projectionSourceTx) Get(namespace string, key []byte) ([]byte, error) {
	// Body blocks have their own bounded reader. Do not duplicate their cache.
	if namespace != backendEntryIndexNamespace && namespace != backendDomainV32Namespace && namespace != projectionSourcesNamespace && namespace != itemMembershipNamespace {
		return tx.ReadTx.Get(namespace, key)
	}
	identity := projectionReadKey{namespace, string(key)}
	if value, found := tx.values[identity]; found {
		return bytes.Clone(value), nil
	}
	value, err := tx.ReadTx.Get(namespace, key)
	if err == nil {
		tx.values[identity] = bytes.Clone(value)
	}
	return value, err
}

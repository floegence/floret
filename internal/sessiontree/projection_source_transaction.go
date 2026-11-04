package sessiontree

import (
	"bytes"

	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/storage/spi"
)

const projectionSourceReadBytes = 4 << 20

// The dependency closure reaches the same immutable source via several exact
// identities. Share successful reads only within its existing read transaction;
// no data or authority survives a branch change or the end of that transaction.
type projectionSourceTx struct {
	spi.ReadTx
	values   map[projectionReadKey][]byte
	locators map[projectionReadKey]entryLocator
	bytes    int
}
type projectionReadKey struct{ namespace, key string }

func newProjectionSourceTx(tx spi.ReadTx) *projectionSourceTx {
	if current, ok := tx.(*projectionSourceTx); ok {
		return current
	}
	return &projectionSourceTx{ReadTx: tx, values: map[projectionReadKey][]byte{}, locators: map[projectionReadKey]entryLocator{}}
}

func cloneEntryLocator(locator entryLocator) entryLocator {
	if locator.TurnPrevious != nil {
		previous := *locator.TurnPrevious
		locator.TurnPrevious = &previous
	}
	return locator
}

func (tx *projectionSourceTx) rememberLocator(identity projectionReadKey, locator entryLocator, rawBytes int) {
	cost := len(identity.namespace) + len(identity.key) + rawBytes + 128
	if cost <= projectionSourceReadBytes-tx.bytes {
		tx.locators[identity] = cloneEntryLocator(locator)
		tx.bytes += cost
	}
}

func (tx *projectionSourceTx) Get(namespace string, key []byte) ([]byte, error) {
	// Body blocks have their own bounded reader. Do not duplicate their cache.
	if !projectionReadNamespace(namespace) {
		return tx.ReadTx.Get(namespace, key)
	}
	identity := projectionReadKey{namespace, string(key)}
	if value, found := tx.values[identity]; found {
		return bytes.Clone(value), nil
	}
	value, err := tx.ReadTx.Get(namespace, key)
	if err == nil {
		tx.remember(identity, value)
	}
	return value, err
}

func projectionReadNamespace(namespace string) bool {
	return namespace == backendEntryIndexNamespace || namespace == backendDomainV32Namespace || namespace == projectionSourcesNamespace || namespace == itemMembershipNamespace || namespace == activityFieldsNamespace || namespace == bodyTextFactsNamespace
}

func (tx *projectionSourceTx) remember(identity projectionReadKey, value []byte) {
	if _, found := tx.values[identity]; found {
		return
	}
	cost := len(identity.namespace) + len(identity.key) + len(value) + 64
	if cost <= projectionSourceReadBytes-tx.bytes {
		tx.values[identity] = bytes.Clone(value)
		tx.bytes += cost
	}
}

// Preserve the physical batch capability across the projection wrapper. Only
// successful records belong to this transaction's bounded memo; errors retain
// the underlying classification and never trigger a second read strategy.
func (tx *projectionSourceTx) ReadFloretRecords(request storagebridge.RecordBatch) ([][]byte, error) {
	if request.Namespace == "" || len(request.Keys) == 0 || len(request.Keys) > storagebridge.MaxRecordBatch {
		return nil, spi.ErrInvalidArgument
	}
	for _, key := range request.Keys {
		if len(key) == 0 {
			return nil, spi.ErrInvalidArgument
		}
	}
	if !projectionReadNamespace(request.Namespace) {
		return storagebridge.ReadRecords(tx.ReadTx, request)
	}
	result := make([][]byte, len(request.Keys))
	var keys [][]byte
	positions := map[string][]int{}
	for i, key := range request.Keys {
		if value, found := tx.values[projectionReadKey{request.Namespace, string(key)}]; found {
			result[i] = bytes.Clone(value)
			continue
		}
		if _, found := positions[string(key)]; !found {
			keys = append(keys, key)
		}
		positions[string(key)] = append(positions[string(key)], i)
	}
	if len(keys) == 0 {
		return result, nil
	}
	values, err := storagebridge.ReadRecords(tx.ReadTx, storagebridge.RecordBatch{Namespace: request.Namespace, Keys: keys})
	if err != nil {
		return nil, err
	}
	for i, key := range keys {
		tx.remember(projectionReadKey{request.Namespace, string(key)}, values[i])
		for _, index := range positions[string(key)] {
			result[index] = bytes.Clone(values[i])
		}
	}
	return result, nil
}

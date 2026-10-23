package storagebridge

import "github.com/floegence/floret/v7/storage/spi"

// RecordBatch is a bounded physical read within one existing transaction.
// It carries no domain meaning and is not part of the host storage SPI.
type RecordBatch struct {
	Namespace string
	Keys      [][]byte
}

const MaxRecordBatch = 256

// ReadRecords preserves key order, detached ownership and the transaction's
// errors. Official SQLite coalesces the reads; other backends use their Get.
func ReadRecords(tx spi.ReadTx, request RecordBatch) ([][]byte, error) {
	if request.Namespace == "" || len(request.Keys) == 0 || len(request.Keys) > MaxRecordBatch {
		return nil, spi.ErrInvalidArgument
	}
	for _, key := range request.Keys {
		if len(key) == 0 {
			return nil, spi.ErrInvalidArgument
		}
	}
	if reader, ok := tx.(interface {
		ReadFloretRecords(RecordBatch) ([][]byte, error)
	}); ok {
		return reader.ReadFloretRecords(request)
	}
	result := make([][]byte, len(request.Keys))
	for i, key := range request.Keys {
		value, err := tx.Get(request.Namespace, key)
		if err != nil {
			return nil, err
		}
		result[i] = value
	}
	return result, nil
}

package storagecodec

import (
	"errors"

	"github.com/floegence/floret/v7/storage/spi"
)

// RecordExists uses metadata-only access when supplied by the physical backend.
// Existing custom v7 SPI implementations retain their Get-based contract.
func RecordExists(tx spi.ReadTx, namespace string, key []byte) (bool, error) {
	var err error
	if reader, ok := tx.(spi.BoundedRecordReader); ok {
		_, err = reader.RecordSize(namespace, key)
	} else {
		_, err = tx.Get(namespace, key)
	}
	if errors.Is(err, spi.ErrNotFound) {
		return false, nil
	}
	return err == nil, err
}

// NamespaceHasRecords probes key presence without loading a value when the
// backend supports bounded reads. Legacy custom SPI implementations use Scan.
func NamespaceHasRecords(tx spi.ReadTx, namespace string) (bool, error) {
	if reader, ok := tx.(spi.BoundedRecordReader); ok {
		return reader.NamespaceHasRecords(namespace)
	}
	page, err := tx.Scan(spi.ScanRequest{Namespace: namespace, Limit: 1})
	return len(page.Records) != 0, err
}

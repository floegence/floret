package storagecodec

import (
	"errors"
	"testing"

	"github.com/floegence/floret/v7/storage/spi"
)

type metadataOnlyProbe struct {
	spi.ReadTx
	err error
}

func (tx metadataOnlyProbe) NamespaceHasRecords(string) (bool, error) { return tx.err == nil, tx.err }
func (tx metadataOnlyProbe) RecordSize(string, []byte) (int64, error) { return 1 << 40, tx.err }
func (tx metadataOnlyProbe) ReadRecordRange(string, []byte, int64, int) ([]byte, error) {
	panic("metadata probe must not read values")
}

func TestMetadataProbesNeverReadValuesOrFallbackAfterCapabilityErrors(t *testing.T) {
	injected := errors.New("metadata access failed")
	for _, err := range []error{nil, injected, spi.ErrNotFound} {
		tx := metadataOnlyProbe{err: err}
		found, got := RecordExists(tx, "namespace", []byte("key"))
		want := err
		if errors.Is(err, spi.ErrNotFound) {
			want = nil
		}
		if found != (err == nil) || !errors.Is(got, want) {
			t.Fatalf("record probe = %v, %v; source error %v", found, got, err)
		}
		found, got = NamespaceHasRecords(tx, "namespace")
		if found != (err == nil) || !errors.Is(got, err) {
			t.Fatalf("namespace probe = %v, %v; source error %v", found, got, err)
		}
	}
}

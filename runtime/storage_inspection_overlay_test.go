package runtime

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/floegence/floret/v7/internal/storagebridge"
	publicstorage "github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestInspectionOverlayScansNamespaceOnceAndInvalidatesAfterWrite(t *testing.T) {
	backend, err := storagebridge.Open(t.Context(), storagebridge.Source(publicstorage.Memory()))
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		for index := 0; index < 600; index++ {
			if err := tx.Put("test", []byte(fmt.Sprintf("%04d", index)), []byte("value")); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := backend.View(t.Context(), func(read spi.ReadTx) error {
		count := &overlayCountingRead{ReadTx: read}
		tx := &inspectionOverlay{ReadTx: count, writes: make(map[string]map[string]*[]byte)}
		if err := tx.Delete("test", []byte("0010")); err != nil {
			return err
		}
		if err := tx.Put("test", []byte("0700"), []byte("extra")); err != nil {
			return err
		}
		var seen [][]byte
		request := spi.ScanRequest{Namespace: "test", Limit: 17, Start: []byte("0005"), End: []byte("0701")}
		for {
			page, err := tx.Scan(request)
			if err != nil {
				return err
			}
			for _, row := range page.Records {
				seen = append(seen, row.Key)
			}
			if !page.HasMore {
				break
			}
			request.After = page.Next
		}
		if count.scans != 3 {
			t.Fatalf("underlying scans=%d, want 3 for all pages", count.scans)
		}
		if len(seen) != 595 || !bytes.Equal(seen[len(seen)-1], []byte("0700")) {
			t.Fatalf("merged bounds/count=%d", len(seen))
		}
		if err := tx.Put("test", []byte("0701"), []byte("new")); err != nil {
			return err
		}
		if _, err := tx.Scan(spi.ScanRequest{Namespace: "test", Limit: 1, After: []byte("0700")}); err != nil {
			return err
		}
		if count.scans != 6 {
			t.Fatalf("invalidated scans=%d, want 6", count.scans)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

type overlayCountingRead struct {
	spi.ReadTx
	scans int
}

func (tx *overlayCountingRead) Scan(request spi.ScanRequest) (spi.ScanPage, error) {
	tx.scans++
	return tx.ReadTx.Scan(request)
}

package sessiontree

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/storage/spi"
)

type bodyWindowCountingBackend struct {
	spi.Backend
	views int
}

func (backend *bodyWindowCountingBackend) View(ctx context.Context, read func(spi.ReadTx) error) error {
	backend.views++
	return backend.Backend.View(ctx, read)
}

func TestCanonicalBodyWindowTransactions(t *testing.T) {
	backend := &bodyWindowCountingBackend{Backend: newMigrationTestBackend()}
	text := strings.Repeat("window-中文-🌸", canonicalBodyBlockBytes)
	var ref canonicalBodyRef
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		var err error
		ref, err = retainCanonicalBody(t.Context(), tx, text)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	reader := migrationSourceReader{ctx: t.Context(), backend: backend}
	for _, offset := range []int{0, canonicalBodyBlockBytes - 1, len(text) - 4, len(text)} {
		backend.views = 0
		data, err := readCanonicalBodyBytes(t.Context(), reader, ref, offset, canonicalBodyReadBytes)
		if err != nil || !bytes.Equal(data, []byte(text[offset:min(len(text), offset+canonicalBodyReadBytes)])) {
			t.Fatalf("offset %d: bytes=%d err=%v", offset, len(data), err)
		}
		if backend.views > 1 || offset == len(text) && backend.views != 0 {
			t.Fatalf("offset %d: got %d read transactions for one window", offset, backend.views)
		}
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		return tx.Delete(canonicalBodyNamespace, canonicalBodyBlockKey(ref.SHA256, 2))
	}); err != nil {
		t.Fatal(err)
	}
	if data, err := readCanonicalBodyBytes(t.Context(), reader, ref, 0, canonicalBodyReadBytes); data != nil || !errors.Is(err, ErrAuthorityCorrupt) || !errors.Is(err, spi.ErrNotFound) {
		t.Fatalf("missing middle block: bytes=%d err=%v", len(data), err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	backend.views = 0
	if data, err := readCanonicalBodyBytes(ctx, reader, ref, 0, canonicalBodyReadBytes); data != nil || !errors.Is(err, context.Canceled) || backend.views != 0 {
		t.Fatalf("canceled window: bytes=%d transactions=%d err=%v", len(data), backend.views, err)
	}
}

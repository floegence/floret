package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/floegence/floret/v3/storage/spi"
)

type blockingSerializedBackend struct {
	entered chan struct{}
	release chan struct{}
}

func (backend *blockingSerializedBackend) View(_ context.Context, read func(spi.ReadTx) error) error {
	close(backend.entered)
	<-backend.release
	return read(nil)
}

func (*blockingSerializedBackend) Update(_ context.Context, mutate func(spi.WriteTx) error) error {
	return mutate(nil)
}

func (*blockingSerializedBackend) Close() error { return nil }

func TestSerializedBackendWaitRespectsOperationContext(t *testing.T) {
	inner := &blockingSerializedBackend{entered: make(chan struct{}), release: make(chan struct{})}
	backend := &serializedBackend{backend: inner}
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- backend.View(context.Background(), func(spi.ReadTx) error { return nil })
	}()
	<-inner.entered

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- backend.Update(ctx, func(spi.WriteTx) error { return nil })
	}()
	select {
	case err := <-secondDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("waiting update error = %v, want context deadline", err)
		}
	case <-time.After(200 * time.Millisecond):
		close(inner.release)
		<-firstDone
		t.Fatal("waiting update ignored its context")
	}

	close(inner.release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first view: %v", err)
	}
}

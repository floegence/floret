package testui

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/internal/provider"
)

type blockingTestProvider struct {
	started chan struct{}
	once    sync.Once
}

func newBlockingTestProvider() *blockingTestProvider {
	return &blockingTestProvider{started: make(chan struct{})}
}

func (p *blockingTestProvider) Stream(ctx context.Context, req provider.Request) (<-chan provider.StreamEvent, error) {
	p.once.Do(func() { close(p.started) })
	ch := make(chan provider.StreamEvent)
	go func() {
		defer close(ch)
		<-ctx.Done()
	}()
	return ch, nil
}

func (p *blockingTestProvider) waitStarted(t *testing.T) {
	t.Helper()
	select {
	case <-p.started:
	case <-time.After(time.Second):
		t.Fatal("provider stream did not start")
	}
}

package testui

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/internal/provider"
	flruntime "github.com/floegence/floret/runtime"
)

type blockingTestProvider struct {
	started chan struct{}
	once    sync.Once
}

type neverClosingTestProvider struct {
	started chan struct{}
	once    sync.Once
	stream  chan provider.StreamEvent
}

func newNeverClosingTestProvider() *neverClosingTestProvider {
	return &neverClosingTestProvider{started: make(chan struct{}), stream: make(chan provider.StreamEvent)}
}

func (p *neverClosingTestProvider) Stream(context.Context, provider.Request) (<-chan provider.StreamEvent, error) {
	p.once.Do(func() { close(p.started) })
	return p.stream, nil
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

func (p *neverClosingTestProvider) waitStarted(t *testing.T) {
	t.Helper()
	select {
	case <-p.started:
	case <-time.After(time.Second):
		t.Fatal("provider stream did not start")
	}
}

func TestTestUIProviderGatewayCancelsNeverClosingProviderStream(t *testing.T) {
	upstream := newNeverClosingTestProvider()
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := (testUIProviderGateway{turn: upstream}).StreamModel(ctx, flruntime.ModelRequest{RunID: "run", ThreadID: "thread", TurnID: "turn"})
	if err != nil {
		t.Fatal(err)
	}
	upstream.waitStarted(t)
	cancel()
	select {
	case _, ok := <-stream:
		if ok {
			t.Fatal("gateway emitted an event after cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("gateway wrapper did not close after cancellation")
	}
}

func TestObservingProviderCancelsNeverClosingProviderStream(t *testing.T) {
	upstream := newNeverClosingTestProvider()
	observed := newObservingProvider(upstream)
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := observed.Stream(ctx, provider.Request{RunID: "run", ThreadID: "thread", TurnID: "turn"})
	if err != nil {
		t.Fatal(err)
	}
	upstream.waitStarted(t)
	cancel()
	select {
	case _, ok := <-stream:
		if ok {
			t.Fatal("observing provider emitted an event after cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("observing provider wrapper did not close after cancellation")
	}
}

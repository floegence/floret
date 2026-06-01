package harness

import (
	"context"
	"sync"

	"github.com/floegence/floret/provider"
)

type ScriptedProvider struct {
	mu       sync.Mutex
	Steps    [][]provider.StreamEvent
	Requests []provider.Request
	Errs     map[int]error
}

func NewScriptedProvider(steps ...[]provider.StreamEvent) *ScriptedProvider {
	return &ScriptedProvider{Steps: steps, Errs: map[int]error{}}
}

func (p *ScriptedProvider) Stream(ctx context.Context, req provider.Request) (<-chan provider.StreamEvent, error) {
	p.mu.Lock()
	p.Requests = append(p.Requests, req)
	err := p.Errs[len(p.Requests)]
	var events []provider.StreamEvent
	if len(p.Requests) <= len(p.Steps) {
		events = append([]provider.StreamEvent(nil), p.Steps[len(p.Requests)-1]...)
	}
	p.mu.Unlock()
	if err != nil {
		return nil, err
	}
	ch := make(chan provider.StreamEvent, len(events))
	for _, ev := range events {
		ch <- ev
	}
	close(ch)
	return ch, nil
}

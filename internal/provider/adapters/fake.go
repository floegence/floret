package adapters

import (
	"context"

	"github.com/floegence/floret/internal/provider"
	"github.com/floegence/floret/internal/provider/cache"
)

type FakeProvider struct {
	Response string
}

func (p FakeProvider) Stream(context.Context, provider.Request) (<-chan provider.StreamEvent, error) {
	response := p.Response
	if response == "" {
		response = "ok"
	}
	ch := make(chan provider.StreamEvent, 2)
	ch <- provider.StreamEvent{Type: provider.Delta, Text: response}
	ch <- provider.StreamEvent{Type: provider.Done, Reason: "stop"}
	close(ch)
	return ch, nil
}

func (p FakeProvider) NormalizeCachePolicy(policy cache.CachePolicy) (cache.CachePolicy, error) {
	return policy, nil
}

func (p FakeProvider) DefaultCacheRetention() cache.Retention {
	return cache.RetentionInMemory
}

func (p FakeProvider) PayloadHash(req provider.Request) (string, error) {
	return req.RawPlan.PayloadHash, nil
}

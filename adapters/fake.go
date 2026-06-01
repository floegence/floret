package adapters

import (
	"context"

	"github.com/floegence/floret/provider"
)

type FakeProvider struct {
	Response string
}

func (p FakeProvider) Stream(context.Context, provider.Request) (<-chan provider.StreamEvent, error) {
	response := p.Response
	if response == "" {
		response = "ok"
	}
	ch := make(chan provider.StreamEvent, 3)
	ch <- provider.StreamEvent{Type: provider.Delta, Text: response}
	ch <- provider.StreamEvent{Type: provider.ToolCalls, ToolCalls: []provider.ToolCall{{ID: "task-complete", Name: "task_complete", Args: response}}}
	ch <- provider.StreamEvent{Type: provider.Done}
	close(ch)
	return ch, nil
}

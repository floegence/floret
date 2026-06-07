package harness

import (
	"context"
	"testing"

	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/session"
)

func TestScriptedProviderRecordsRequestsAndReplaysTranscript(t *testing.T) {
	p := NewScriptedProvider([]provider.StreamEvent{{Type: provider.Delta, Text: "hi"}})
	ch, err := p.Stream(context.Background(), provider.Request{RunID: "r", Step: 1, Messages: []session.Message{{Role: session.User, Content: "hello"}}})
	if err != nil {
		t.Fatal(err)
	}
	ev := <-ch
	if ev.Text != "hi" {
		t.Fatalf("event = %#v", ev)
	}
	if len(p.Requests) != 1 || p.Requests[0].Messages[0].Content != "hello" {
		t.Fatalf("request not recorded: %#v", p.Requests)
	}
}

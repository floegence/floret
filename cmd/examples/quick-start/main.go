package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/floegence/floret/v3/config"
	"github.com/floegence/floret/v3/provider"
	"github.com/floegence/floret/v3/runtime"
	"github.com/floegence/floret/v3/storage"
)

type gateway struct{}

func (gateway) Identity() provider.Identity {
	return provider.Identity{Provider: "example", Model: "deterministic", StateCompatibilityKey: "example:deterministic:v1"}
}

func (gateway) Capabilities() provider.Capabilities {
	return provider.Capabilities{Reasoning: provider.ReasoningUnsupported}
}

func (gateway) Stream(context.Context, provider.Request) (<-chan provider.Event, error) {
	events := make(chan provider.Event, 2)
	events <- provider.Event{Type: provider.EventDelta, Text: "Hello from Floret v3."}
	events <- provider.Event{Type: provider.EventDone, Reason: "stop"}
	close(events)
	return events, nil
}

func main() {
	path := filepath.Join(os.TempDir(), "floret-v3-example.db")
	if err := run(context.Background(), path); err != nil {
		panic(err)
	}
}

func run(ctx context.Context, path string) error {
	host, err := runtime.Open(ctx, runtime.Options{Storage: storage.SQLite(path)})
	if err != nil {
		return err
	}
	defer func() {
		if err := host.Shutdown(context.Background()); err != nil {
			panic(err)
		}
	}()
	agent, err := runtime.NewAgent(config.AgentConfig{
		Profile:      config.AgentProfile{ID: "example", Name: "Example assistant"},
		SystemPrompt: "Answer clearly and concisely.",
		Context:      config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}, gateway{})
	if err != nil {
		return err
	}
	created, err := host.Threads().CreateThread(ctx, runtime.CreateThreadCommand{
		LogicalRequestID: "example-create",
	})
	if err != nil {
		return err
	}
	thread, err := host.Thread(ctx, created.ThreadID)
	if err != nil {
		return err
	}
	turns, err := thread.Turns(agent)
	if err != nil {
		return err
	}
	started, err := turns.StartTurn(ctx, runtime.StartTurnCommand{
		LogicalRequestID: "example-message",
		UserMessage:      runtime.TurnInput{Text: "Hello"},
	})
	if err != nil {
		return err
	}
	fmt.Println(started.ThreadID, started.TurnID, started.RunID)
	return nil
}

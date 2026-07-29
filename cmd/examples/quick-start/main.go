package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/floegence/floret/v2/config"
	"github.com/floegence/floret/v2/provider"
	"github.com/floegence/floret/v2/runtime"
	"github.com/floegence/floret/v2/storage"
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
	events <- provider.Event{Type: provider.EventDelta, Text: "Hello from Floret v2."}
	events <- provider.Event{Type: provider.EventDone, Reason: "stop"}
	close(events)
	return events, nil
}

func main() {
	path := filepath.Join(os.TempDir(), "floret-v2-example.db")
	if err := run(context.Background(), path); err != nil {
		panic(err)
	}
}

func run(ctx context.Context, path string) error {
	host, err := runtime.Open(ctx, runtime.Options{Storage: storage.SQLite(path)})
	if err != nil {
		return err
	}
	defer host.Close()
	agent, err := runtime.NewAgent(config.AgentConfig{
		Profile:      config.AgentProfile{ID: "example", Name: "Example assistant"},
		SystemPrompt: "Answer clearly and concisely.",
		Context:      config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}, gateway{})
	if err != nil {
		return err
	}
	creator, err := host.ThreadCreator("example-thread", "example-create")
	if err != nil {
		return err
	}
	if _, err := creator.Create(ctx); err != nil {
		return err
	}
	runner, err := host.TurnRunner(ctx, "example-thread", agent)
	if err != nil {
		return err
	}
	result, err := runner.Run(ctx, runtime.TurnRequest{
		RunID: "example-run", TurnID: "example-turn", Input: runtime.TurnInput{Text: "Hello"},
	})
	if err != nil {
		return err
	}
	fmt.Println(result.Output)
	return nil
}

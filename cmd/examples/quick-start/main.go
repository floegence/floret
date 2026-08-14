package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/floegence/floret/v4/config"
	"github.com/floegence/floret/v4/provider"
	"github.com/floegence/floret/v4/runtime"
	"github.com/floegence/floret/v4/storage"
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
		return fmt.Errorf("open runtime: %w", err)
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
		return fmt.Errorf("create agent: %w", err)
	}
	service, err := host.ThreadService(runtime.AgentFactoryFunc(func(context.Context, runtime.AgentRequest) (*runtime.Agent, error) { return agent, nil }))
	if err != nil {
		return fmt.Errorf("open thread service: %w", err)
	}
	created, err := service.Create(ctx, runtime.CreateThreadInput{RequestKey: "example-create"})
	if err != nil {
		return fmt.Errorf("create thread: %w", err)
	}
	started, err := service.Send(ctx, runtime.SendInput{ThreadID: created.ThreadID, RequestKey: "example-message", Input: runtime.UserInput{Text: "Hello"}})
	if err != nil {
		return fmt.Errorf("send: %w", err)
	}
	fmt.Println(started.ThreadID, started.TurnID)
	return nil
}

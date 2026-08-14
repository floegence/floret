package main

import (
	"context"
	"fmt"
	"os"

	"github.com/floegence/floret/v4/config"
	"github.com/floegence/floret/v4/provider"
	"github.com/floegence/floret/v4/runtime"
	"github.com/floegence/floret/v4/storage"
)

func main() {
	ctx := context.Background()
	apiKey := os.Getenv("OPENAI_API_KEY")
	if apiKey == "" {
		panic("OPENAI_API_KEY is required")
	}
	model := os.Getenv("OPENAI_MODEL")
	if model == "" {
		model = "gpt-4.1-mini"
	}

	gateway, err := provider.NewOpenAICompatible(provider.OpenAICompatibleOptions{
		Provider: "openai", Model: model, BaseURL: "https://api.openai.com/v1",
		APIKey: apiKey, StateCompatibilityKey: "openai:" + model + ":chat-completions:v1",
		Capabilities: provider.Capabilities{
			Reasoning: provider.ReasoningUnsupported, AttachmentPayload: provider.AttachmentDescriptors,
		},
	})
	if err != nil {
		panic(err)
	}
	agent, err := runtime.NewAgent(config.AgentConfig{
		Profile:      config.AgentProfile{ID: "assistant", Name: "Assistant"},
		SystemPrompt: "Answer clearly and concisely.",
		Context:      config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
	}, gateway)
	if err != nil {
		panic(err)
	}

	host, err := runtime.Open(ctx, runtime.Options{Storage: storage.SQLite("floret.db")})
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := host.Shutdown(context.Background()); err != nil {
			panic(err)
		}
	}()

	service, err := host.ThreadService(runtime.AgentFactoryFunc(func(context.Context, runtime.AgentRequest) (*runtime.Agent, error) { return agent, nil }))
	if err != nil {
		panic(err)
	}
	created, err := service.Create(ctx, runtime.CreateThreadInput{RequestKey: "quickstart-create-thread"})
	if err != nil {
		panic(err)
	}
	started, err := service.Send(ctx, runtime.SendInput{ThreadID: created.ThreadID, RequestKey: "quickstart-first-message", Input: runtime.UserInput{Text: "Explain why durable agent state matters."}})
	if err != nil {
		panic(err)
	}
	fmt.Println(started.ThreadID, started.TurnID)
}

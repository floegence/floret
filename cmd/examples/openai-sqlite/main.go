package main

import (
	"context"
	"fmt"
	"os"

	"github.com/floegence/floret/v3/config"
	"github.com/floegence/floret/v3/provider"
	"github.com/floegence/floret/v3/runtime"
	"github.com/floegence/floret/v3/storage"
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

	created, err := host.Threads().CreateThread(ctx, runtime.CreateThreadCommand{
		LogicalRequestID: "quickstart-create-thread",
	})
	if err != nil {
		panic(err)
	}
	thread, err := host.Thread(ctx, created.ThreadID)
	if err != nil {
		panic(err)
	}
	executor, err := thread.TurnExecutor(agent)
	if err != nil {
		panic(err)
	}
	started, err := executor.StartTurn(ctx, runtime.StartTurnCommand{
		LogicalRequestID: "quickstart-first-message",
		UserMessage:      runtime.TurnInput{Text: "Explain why durable agent state matters."},
	})
	if err != nil {
		panic(err)
	}
	reader, err := thread.Reader()
	if err != nil {
		panic(err)
	}
	projection, err := reader.ReadAuthoritativeProjection(ctx, started.TurnID, started.RunID)
	if err != nil {
		panic(err)
	}
	for _, segment := range projection.Projection.Segments {
		if segment.Kind == runtime.ThreadTurnProjectionSegmentAssistantText {
			fmt.Print(segment.Text)
		}
	}
}

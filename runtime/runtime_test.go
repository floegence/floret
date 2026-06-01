package runtime

import (
	"context"
	"testing"

	"github.com/floegence/floret/config"
	"github.com/floegence/floret/engine"
)

func TestNewEngineFromConfigRunsFakeProvider(t *testing.T) {
	e, err := NewEngine(config.Config{
		Provider:           config.ProviderFake,
		Model:              "fake-model",
		FakeResponse:       "configured",
		RunID:              "run",
		SystemPrompt:       "test",
		MaxContextMessages: 4,
		MaxSteps:           2,
	}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	result := e.Run(context.Background(), "hello")
	if result.Status != engine.Completed || result.Output != "configured" {
		t.Fatalf("result = %#v", result)
	}
}

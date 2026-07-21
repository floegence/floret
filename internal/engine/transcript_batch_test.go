package engine_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/floegence/floret/internal/engine"
	"github.com/floegence/floret/internal/event"
	"github.com/floegence/floret/internal/provider"
	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/testing/harness"
	"github.com/floegence/floret/tools"
)

func TestToolBatchDoesNotReloadTranscriptPerMessage(t *testing.T) {
	const batchSize = 32
	calls := make([]provider.ToolCall, 0, batchSize)
	for index := 0; index < batchSize; index++ {
		calls = append(calls, provider.ToolCall{
			ID: fmt.Sprintf("call-%d", index), Name: "read", Args: fmt.Sprintf(`{"value":"%d"}`, index),
		})
	}
	p := harness.NewScriptedProvider(
		harness.Step(provider.StreamEvent{Type: provider.ToolCalls, ToolCalls: calls}, harness.DoneReason("tool_calls")),
		harness.Step(harness.Text("done"), harness.Done()),
	)
	e := newTestEngine(p, &event.Recorder{})
	e.Options.MaxToolCalls = batchSize
	e.Tools = tools.NewRegistry()
	mustRegister(t, e.Tools, stringTool("read", "Read", true, tools.PermissionSpec{}, func(_ context.Context, value string) (string, error) {
		return value, nil
	}))
	store := &transcriptReadCountingStore{TranscriptStore: session.NewMemoryStore()}
	e.Store = store

	result := e.Run(context.Background(), "run batch")
	if result.Status != engine.Completed {
		t.Fatalf("result = %#v", result)
	}
	if got := store.readCount(); got != 2 {
		t.Fatalf("Transcript calls = %d, want 2 independent of batch size", got)
	}
}

type transcriptReadCountingStore struct {
	session.TranscriptStore
	mu    sync.Mutex
	reads int
}

func (s *transcriptReadCountingStore) Transcript(runID string) ([]session.Message, error) {
	s.mu.Lock()
	s.reads++
	s.mu.Unlock()
	return s.TranscriptStore.Transcript(runID)
}

func (s *transcriptReadCountingStore) readCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reads
}

package runtime

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/internal/sessiontree"
	internalstorage "github.com/floegence/floret/v7/internal/storage"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
)

type unavailableSavePoint struct{ *internalstorage.BackendKernel }

func (kernel unavailableSavePoint) Append(ctx context.Context, entry sessiontree.Entry, opts sessiontree.AppendOptions) (sessiontree.Entry, error) {
	if entry.TurnStatus == sessiontree.TurnSavePoint {
		return sessiontree.Entry{}, errors.New("save point unavailable")
	}
	return kernel.BackendKernel.Append(ctx, entry, opts)
}

func TestRetryCompactionUsesSelectedCheckpointAfterRestart(t *testing.T) {
	ctx := t.Context()
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "compact", StateCompatibilityKey: "test:compact"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		compactionTestStep("ack"), compactionTestStep("ack"), compactionTestStep("ack"), compactionTestStep("FIRST_CHECKPOINT"),
		compactionTestStep("ack"), compactionTestStep("ack"), compactionTestStep("ack"),
		florettest.Step{ReturnError: provider.ErrContextOverflow}, compactionTestStep("SUPERSEDED_CHECKPOINT"), florettest.Step{ReturnError: errors.New("original turn failed")},
		florettest.Step{ReturnError: provider.ErrContextOverflow}, compactionTestStep("RETRY_CHECKPOINT"), compactionTestStep("finished"))
	path := filepath.Join(t.TempDir(), "retry.sqlite")
	open := func() (*Host, ThreadService) {
		t.Helper()
		host, err := Open(ctx, Options{Storage: storage.SQLite(path)})
		if err != nil {
			t.Fatal(err)
		}
		service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) {
			return compactionTestAgent(t, gateway, WithAgentProviderRetryPolicy(ProviderRetryPolicy{Disabled: true})), nil
		}))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
		return host, service
	}
	host, service := open()
	created, err := service.Create(ctx, CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	wait := func(view ThreadView, err error) ThreadView {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return waitThreadView(t, service, created.ThreadID, func(current ThreadView) bool {
			return current.TurnID == view.TurnID && current.Activity == ThreadActivityIdle && current.LastOutcome != nil
		})
	}
	seedCompactionHistory(t, service, created.ThreadID)
	first := wait(service.(ThreadContextCompactor).CompactContext(ctx, CompactContextInput{ThreadID: created.ThreadID, RequestKey: "first"}))
	if first.Failure != nil {
		t.Fatal(first.Failure)
	}
	for _, key := range []RequestKey{"more-1", "more-2", "more-3"} {
		if view := wait(service.Send(ctx, SendInput{ThreadID: created.ThreadID, RequestKey: key, Input: UserInput{Text: strings.Repeat("another durable fact ", 500)}})); view.Failure != nil {
			t.Fatal(view.Failure)
		}
	}
	host.store.repo = unavailableSavePoint{host.store.repo.(*internalstorage.BackendKernel)}
	original := wait(service.Send(ctx, SendInput{ThreadID: created.ThreadID, RequestKey: "original", Input: UserInput{Text: "finish the task"}}))
	if original.Failure == nil || !strings.Contains(original.Failure.Message, "save point unavailable") {
		t.Fatalf("original failure: %+v", original.Failure)
	}
	if err := host.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	host, service = open()
	retry := wait(service.Retry(ctx, RetryInput{ThreadID: created.ThreadID, SourceTurnID: original.TurnID, RequestKey: "retry"}))
	if retry.Failure != nil {
		t.Fatal(retry.Failure)
	}
	requests := gateway.Requests()
	if len(requests) != 13 {
		t.Fatalf("provider calls=%d", len(requests))
	}
	for index, want := range map[int]string{10: "FIRST_CHECKPOINT", 11: "FIRST_CHECKPOINT", 12: "RETRY_CHECKPOINT"} {
		var contents strings.Builder
		for _, message := range requests[index].Messages {
			contents.WriteString(message.Text)
		}
		if !strings.Contains(contents.String(), want) || strings.Contains(contents.String(), "SUPERSEDED_CHECKPOINT") {
			t.Fatalf("provider request %d received the wrong checkpoint; want %s", index, want)
		}
	}
	if err := host.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	host, _ = open()
	entries, err := host.store.repo.Path(ctx, created.ThreadID.String(), "")
	if err != nil {
		t.Fatal(err)
	}
	var checkpoints []sessiontree.Entry
	for _, entry := range entries {
		if entry.Type == sessiontree.EntryCompaction {
			checkpoints = append(checkpoints, entry)
		}
	}
	if len(checkpoints) != 3 || checkpoints[1].PreviousCompactionID != checkpoints[0].CompactionID || checkpoints[2].PreviousCompactionID != checkpoints[0].CompactionID || checkpoints[2].CompactionID == checkpoints[1].CompactionID || checkpoints[2].CompactionGeneration != 2 {
		t.Fatal("retry replaced or inherited the superseded checkpoint")
	}
	history, err := sessiontree.BuildContextChecked(entries, sessiontree.ContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var restored strings.Builder
	for _, message := range history {
		restored.WriteString(message.Content)
	}
	if !strings.Contains(restored.String(), "RETRY_CHECKPOINT") || strings.Contains(restored.String(), "SUPERSEDED_CHECKPOINT") {
		t.Fatal("restart projected the wrong checkpoint")
	}
}

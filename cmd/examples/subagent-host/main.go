package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/floegence/floret/config"
	floretruntime "github.com/floegence/floret/runtime"
)

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	store := floretruntime.NewMemoryStore()
	defer store.Close()

	var createBinder *floretruntime.ThreadCreateHostBinder
	var subAgentBinder *floretruntime.SubAgentHostBinder
	if err := floretruntime.ConfigureHostCapabilities(store, func(bootstrap *floretruntime.HostBootstrap) error {
		var configureErr error
		createBinder, configureErr = floretruntime.NewThreadCreateHostBinder(bootstrap)
		if configureErr != nil {
			return configureErr
		}
		subAgentBinder, configureErr = floretruntime.NewSubAgentHostBinder(bootstrap)
		return configureErr
	}); err != nil {
		return err
	}

	const parentThreadID = floretruntime.ThreadID("parent-thread")
	const childThreadID = floretruntime.ThreadID("review-child")
	const createIntentID = floretruntime.CreateIntentID("create-parent-thread")
	createHost, err := createBinder.Bind(parentThreadID, createIntentID)
	if err != nil {
		return err
	}
	if _, err := createHost.CreateThread(ctx, floretruntime.CreateThreadRequest{
		ThreadID: parentThreadID, CreateIntentID: createIntentID,
	}); err != nil {
		return err
	}

	factory, err := subAgentBinder.Bind(parentThreadID)
	if err != nil {
		return err
	}
	host, err := factory.NewHost(ctx, floretruntime.SubAgentHostOptions{
		Config: config.Config{
			Provider: config.ProviderFake, Model: "fake-model",
			FakeResponse: "The child review is complete.", SystemPrompt: "Return a concise handoff.",
		},
		SubAgentRunTimeout: 2 * time.Second,
	})
	if err != nil {
		return err
	}
	spawned, err := host.SpawnSubAgent(ctx, floretruntime.SpawnSubAgentRequest{
		PublicationID: "publish-review-child", ParentThreadID: parentThreadID,
		ParentTurnID: "parent-turn", ThreadID: childThreadID,
		TaskName: "Review boundary", TaskDescription: "Review the public host boundary.",
		Message:        "Review the public host boundary and return a handoff.",
		HostProfileRef: "local-reviewer", ForkMode: floretruntime.SubAgentForkNone,
	})
	if err != nil {
		return err
	}
	waited, err := host.WaitSubAgents(ctx, floretruntime.WaitSubAgentsRequest{
		ParentThreadID: parentThreadID, ChildThreadIDs: []floretruntime.ThreadID{childThreadID}, Timeout: 2 * time.Second,
	})
	if err != nil {
		return err
	}
	if waited.TimedOut || len(waited.Snapshots) != 1 || waited.Snapshots[0].Status != floretruntime.SubAgentStatusCompleted {
		return fmt.Errorf("unexpected first wait result: %#v", waited)
	}
	if _, err := host.SendSubAgentInput(ctx, floretruntime.SendSubAgentInputRequest{
		InputRequestID: "review-follow-up", ParentThreadID: parentThreadID, ChildThreadID: childThreadID,
		Message: "Confirm the final handoff.",
	}); err != nil {
		return err
	}
	waited, err = host.WaitSubAgents(ctx, floretruntime.WaitSubAgentsRequest{
		ParentThreadID: parentThreadID, ChildThreadIDs: []floretruntime.ThreadID{childThreadID}, Timeout: 2 * time.Second,
	})
	if err != nil {
		return err
	}
	if waited.TimedOut || len(waited.Snapshots) != 1 || waited.Snapshots[0].Status != floretruntime.SubAgentStatusCompleted {
		return fmt.Errorf("unexpected follow-up wait result: %#v", waited)
	}
	handoff := waited.Snapshots[0].LastMessage
	closed, err := host.CloseSubAgent(ctx, floretruntime.CloseSubAgentRequest{
		CloseOperationID: "close-review-child", ParentThreadID: parentThreadID,
		ChildThreadID: childThreadID, Reason: "handoff accepted",
	})
	if err != nil {
		return err
	}
	if closed.Status != floretruntime.SubAgentStatusClosed {
		return fmt.Errorf("child close status=%s", closed.Status)
	}
	fmt.Printf("child=%s path=%s handoff=%q status=%s\n", spawned.ThreadID, spawned.Path, handoff, closed.Status)
	return nil
}

package runtime

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/floegence/floret/internal/testing/tooltest"
	"github.com/floegence/floret/tools"
)

func TestThreadReadHostReadsCanonicalApprovalQueue(t *testing.T) {
	tests := []struct {
		name  string
		store func(*testing.T) *Store
	}{
		{name: "memory", store: func(*testing.T) *Store { return NewMemoryStore() }},
		{name: "sqlite", store: func(t *testing.T) *Store {
			store, err := OpenSQLiteStore(filepath.Join(t.TempDir(), "approval-read.db"))
			if err != nil {
				t.Fatal(err)
			}
			return store
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := tc.store(t)
			t.Cleanup(func() { _ = store.Close() })
			registry := tools.NewRegistry()
			if err := registry.Register(tools.Define[runtimeEchoArgs](
				tools.Definition{
					Name:        "write_note",
					InputSchema: runtimeEchoSchema(),
					Effects:     []tools.Effect{tools.EffectWrite},
					Permission:  tools.PermissionSpec{Mode: tools.PermissionAsk},
				},
				nil,
				nil,
				func(_ context.Context, inv tools.Invocation[runtimeEchoArgs]) (tools.Result, error) {
					return tools.Result{Text: inv.Args.Text}, nil
				},
			)); err != nil {
				t.Fatal(err)
			}
			gateway := runtimeModelGateway(func(_ context.Context, req ModelRequest) (<-chan ModelEvent, error) {
				events := make(chan ModelEvent, 2)
				if req.Step == 1 {
					events <- ModelEvent{Type: ModelEventToolCalls, ToolCalls: []tools.ToolCall{{ID: "call-1", Name: "write_note", Args: `{"text":"notes.md"}`}}}
					events <- ModelEvent{Type: ModelEventDone, Reason: "tool_calls"}
				} else {
					events <- ModelEvent{Type: ModelEventDelta, Text: "done"}
					events <- ModelEvent{Type: ModelEventDone, Reason: "stop"}
				}
				close(events)
				return events, nil
			})
			release := make(chan struct{})
			host, err := newTestHost(t, providerHostOptions{
				Config:               runtimeGatewayConfig("test"),
				ModelGateway:         gateway,
				ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
				Store:                store,
				Tools:                registry,
				EffectAuthorizationGate: allowRuntimeEffectGate{approver: func(ctx context.Context, _ tooltest.ApprovalRequest) (tooltest.PermissionDecision, error) {
					select {
					case <-release:
						return tooltest.PermissionDecisionAllow, nil
					case <-ctx.Done():
						return tooltest.PermissionDecision{}, ctx.Err()
					}
				}},
				IDGenerator: deterministicIDs(),
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
				t.Fatal(err)
			}
			runErr := make(chan error, 1)
			go func() {
				_, runErrValue := host.RunTurn(ctx, RunTurnRequest{
					RunID: "run", ThreadID: "thread", TurnID: "turn", Input: TurnInput{Text: "write"},
				})
				runErr <- runErrValue
			}()

			activeQueue := waitRuntimeApprovalQueue(t, ctx, host, "thread", 1)
			readHost, err := newTestMaintenanceHost(t, store)
			if err != nil {
				t.Fatal(err)
			}
			reader, err := readHost.readHost(ctx, "thread")
			if err != nil {
				t.Fatal(err)
			}
			queue, err := reader.ReadApprovalQueue(ctx, ReadApprovalQueueRequest{ThreadID: "thread"})
			if err != nil {
				t.Fatal(err)
			}
			if queue.RootThreadID != activeQueue.RootThreadID || queue.Generation != activeQueue.Generation ||
				queue.Revision != activeQueue.Revision || len(queue.Items) != 1 ||
				queue.Items[0].ApprovalID != activeQueue.Items[0].ApprovalID {
				t.Fatalf("read queue=%#v, want canonical queue %#v", queue, activeQueue)
			}
			if _, err := reader.ReadApprovalQueue(ctx, ReadApprovalQueueRequest{ThreadID: "other"}); err == nil {
				t.Fatal("thread read host accepted a different root thread")
			}

			resolveRuntimeApproval(t, ctx, host, activeQueue, activeQueue.Items[0], "decision", ApprovalDecisionApprove)
			close(release)
			select {
			case err := <-runErr:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("timed out waiting for approved run")
			}
		})
	}
}

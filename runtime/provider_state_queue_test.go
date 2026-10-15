package runtime

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
)

func TestProviderNativeHistorySurvivesQueuedPromotion(t *testing.T) {
	for _, restart := range []bool{false, true} {
		name := "automatic"
		if restart {
			name = "restart_imported"
		}
		t.Run(name, func(t *testing.T) {
			release := make(chan struct{})
			first := nativeHistoryStep("first-native", false)
			if !restart {
				first.BlockUntil = release
			}
			gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "native", StateCompatibilityKey: "account:model"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, first, nativeHistoryStep("next-native", false))
			path := filepath.Join(t.TempDir(), "threads.sqlite")
			open := func() (*Host, ThreadService) {
				host, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
				service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return testAgent(gateway) }))
				if err != nil {
					t.Fatal(err)
				}
				return host, service
			}
			host, service := open()
			created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
			if err != nil {
				t.Fatal(err)
			}
			if restart {
				sendNativeTurn(t, service, created.ThreadID, "first")
				_, err = service.ImportPendingInputs(t.Context(), ImportPendingInputsInput{ThreadID: created.ThreadID, Items: []ImportedPendingInput{{RequestKey: "queued", Input: UserInput{Text: "queued"}}}})
				if err != nil {
					t.Fatal(err)
				}
				if err = host.Shutdown(context.Background()); err != nil {
					t.Fatal(err)
				}
				_, service = open()
				_, err = service.PromoteQueued(t.Context(), PromoteQueuedInput{ThreadID: created.ThreadID, QueueItemID: "queue:queued", RequestKey: "promote"})
			} else {
				_, err = service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "first", Input: UserInput{Text: "first"}})
				if err != nil {
					t.Fatal(err)
				}
				waitNativeRequests(t, gateway, 1)
				var queued ThreadView
				queued, err = service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "queued", Input: UserInput{Text: "queued"}})
				if err != nil || len(queued.Queue) != 1 {
					t.Fatalf("queue=%+v error=%v", queued.Queue, err)
				}
				close(release)
			}
			if err != nil {
				t.Fatal(err)
			}
			waitNativeRequests(t, gateway, 2)
			state := gateway.Requests()[1].PreviousState
			if state == nil || state.ID != "first-native" {
				t.Fatalf("native history lost: %+v", state)
			}
		})
	}
}

package runtime

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/tools"
)

func TestThreadMediaSurvivesModelRequestActivityAndRestart(t *testing.T) {
	ref := "host-frame://target/" + strings.Repeat("a", 64)
	finish := make(chan struct{})
	gateway := florettest.NewScriptedGateway(
		provider.Identity{Provider: "test", Model: "vision", StateCompatibilityKey: "test:vision"},
		provider.Capabilities{Reasoning: provider.ReasoningUnsupported},
		florettest.Step{Events: []provider.Event{{Type: provider.EventToolCalls, ToolCalls: []provider.ToolCall{{ID: "capture-1", Name: "capture", Args: `{}`}}}, {Type: provider.EventDone, Reason: "tool_calls"}}},
		florettest.Step{BlockUntil: finish, Events: []provider.Event{{Type: provider.EventDelta, Text: "done"}, {Type: provider.EventDone, Reason: "stop"}}},
	)
	capture := tools.Define[map[string]any](tools.Definition{
		Name: "capture", InputSchema: tools.StrictObject(map[string]any{}, []string{}), ReadOnly: true,
		Permission: tools.PermissionSpec{Mode: tools.PermissionAllow},
	}, nil, nil, func(context.Context, tools.Invocation[map[string]any]) (tools.Result, error) {
		return tools.Result{Text: "captured", Attachments: []tools.ArtifactRef{{ID: ref, SafeLabel: "screen.png", Kind: "image", MIME: "image/png", SizeBytes: 100, SHA256: strings.Repeat("a", 64)}},
			Activity: &tools.ActivityPresentation{Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Status: "success"},
				TargetRefs: []tools.ActivityTargetRef{{Kind: "image", Label: "Screenshot", ResourceRef: ref}}}}, nil
	})
	agent, err := testAgent(gateway, WithAgentTools(capture), WithAgentEffectAuthorization(EffectAuthorizationGateFunc(func(ctx context.Context, request EffectAuthorizationRequest, effect AuthorizedEffect) (EffectDispatchResult, error) {
		return effect(ctx, EffectAuthorizationProof{EffectAttemptID: request.EffectAttemptID, RequestFingerprint: request.RequestFingerprint,
			ThreadID: request.ThreadID, TurnID: request.TurnID, RunID: request.RunID, ToolCallID: request.ToolCallID,
			PolicyRevision: "test", AuditReference: "test", AuditHash: "test", AuthorizedAt: time.Now().UTC()})
	})))
	if err != nil {
		t.Fatal(err)
	}
	path := t.TempDir() + "/media.db"
	open := func() (*Host, ThreadService) {
		t.Helper()
		host, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
		if err != nil {
			t.Fatal(err)
		}
		service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
		if err != nil {
			t.Fatal(err)
		}
		return host, service
	}
	host, service := open()
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-media"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "send-media", Input: UserInput{Text: "capture"}}); err != nil {
		t.Fatal(err)
	}
	if err := gateway.WaitForRequests(t.Context(), 2); err != nil {
		t.Fatal(err)
	}
	activeInventory, err := host.MediaInventory(t.Context())
	if err != nil || activeInventory.Idle {
		t.Fatalf("active media inventory must prevent collection: %+v, %v", activeInventory, err)
	}
	queuedRef := "host-queued://" + strings.Repeat("b", 64)
	queued, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "queue-media", Input: UserInput{Text: "queued image", Attachments: []MessageAttachment{{ResourceRef: queuedRef, Name: "queued.png", MIMEType: "image/png", SizeBytes: 1}}}})
	if err != nil || len(queued.Queue) != 1 {
		t.Fatalf("enqueue media: %+v, %v", queued.Queue, err)
	}
	activeInventory, err = host.MediaInventory(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	foundQueued := false
	for _, media := range activeInventory.References {
		foundQueued = foundQueued || media.Attachment.ResourceRef == queuedRef
	}
	if !foundQueued {
		t.Fatal("inventory lost queued media")
	}
	if _, err := service.DeleteQueued(t.Context(), DeleteQueuedInput{ThreadID: created.ThreadID, QueueItemID: queued.Queue[0].ID, RequestKey: "delete-queued-media"}); err != nil {
		t.Fatal(err)
	}
	close(finish)
	view := waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool { return view.Activity == ThreadActivityIdle && view.LastOutcome != nil })
	check := func(view ThreadView) {
		t.Helper()
		for _, item := range view.Items {
			if item.Activity != nil && item.Activity.ToolID == "capture-1" && item.Activity.Presentation != nil {
				refs := item.Activity.Presentation.TargetRefs
				if len(refs) == 1 && refs[0].ResourceRef == ref {
					if len(item.Attachments) != 1 || item.Attachments[0].ResourceRef != ref {
						t.Fatal("canonical view lost confirmed tool attachment")
					}
					locator, err := item.Attachments[0].Locator()
					if err != nil || !strings.HasPrefix(locator, "media:v1:") {
						t.Fatalf("canonical media locator: %q %v", locator, err)
					}
					return
				}
			}
		}
		t.Fatal("runtime view lost media resource reference")
	}
	check(view)
	checkInventory := func(want map[identity.ThreadID]bool) {
		t.Helper()
		inventory, err := host.MediaInventory(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		found := make(map[identity.ThreadID]bool)
		if !inventory.Idle {
			t.Fatal("completed threads must permit media maintenance")
		}
		for _, media := range inventory.References {
			if media.Attachment.ResourceRef == ref {
				found[media.ThreadID] = true
			}
		}
		if len(found) != len(want) {
			t.Fatalf("media owners: %v, want %v", found, want)
		}
		for threadID := range want {
			if !found[threadID] {
				t.Fatalf("missing media owner %s", threadID)
			}
		}
	}
	checkInventory(map[identity.ThreadID]bool{created.ThreadID: true})
	requests := gateway.Requests()
	found := false
	for _, request := range requests {
		for _, message := range request.Messages {
			if message.ToolResult != nil && len(message.ToolResult.Attachments) == 1 && message.ToolResult.Attachments[0].ResourceRef == ref {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("actual runtime model request lost tool-result image")
	}
	if err := host.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	host, service = open()
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	view, err = service.View(t.Context(), created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	check(view)
	forked, err := service.Fork(t.Context(), ForkThreadInput{SourceThreadID: created.ThreadID, RequestKey: "fork-media"})
	if err != nil {
		t.Fatal(err)
	}
	meta, err := host.store.repo.Thread(t.Context(), created.ThreadID.String())
	if err != nil {
		t.Fatal(err)
	}
	meta.Archived = true
	if err := host.store.repo.UpdateThread(t.Context(), meta); err != nil {
		t.Fatal(err)
	}
	checkInventory(map[identity.ThreadID]bool{created.ThreadID: true, forked.ThreadID: true})
	// A retained inactive path is still conversation history, even when the
	// visible leaf precedes the image-producing tool.
	if err := host.store.repo.MoveLeaf(t.Context(), created.ThreadID.String(), "user:send-media"); err != nil {
		t.Fatal(err)
	}
	checkInventory(map[identity.ThreadID]bool{created.ThreadID: true, forked.ThreadID: true})
	if err := service.Delete(t.Context(), DeleteThreadInput{ThreadID: created.ThreadID, RequestKey: "delete-media"}); err != nil {
		t.Fatal(err)
	}
	checkInventory(map[identity.ThreadID]bool{forked.ThreadID: true})
	if err := service.Delete(t.Context(), DeleteThreadInput{ThreadID: forked.ThreadID, RequestKey: "delete-fork-media"}); err != nil {
		t.Fatal(err)
	}
	checkInventory(map[identity.ThreadID]bool{})
}

package runtime

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
)

func assertEmptyParentEventJSON(t *testing.T, view ThreadView) {
	t.Helper()
	if view.ParentEvents == nil || view.ParentEvents.Events == nil || len(view.ParentEvents.Events) != 0 {
		t.Fatalf("empty parent events must be an allocated array: %+v", view.ParentEvents)
	}
	encoded, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &wire); err != nil {
		t.Fatal(err)
	}
	var page map[string]json.RawMessage
	if err := json.Unmarshal(wire["parent_events"], &page); err != nil {
		t.Fatal(err)
	}
	if string(page["events"]) != "[]" {
		t.Fatalf("events=%s, want []", page["events"])
	}
}

func TestCloneThreadViewPreservesParentEventArrays(t *testing.T) {
	for _, events := range [][]ParentEvent{nil, {}} {
		assertEmptyParentEventJSON(t, cloneThreadRuntimeView(ThreadView{ThreadID: "empty-events", ParentEvents: &ParentEventPage{Events: events}}))
	}
	if cloneThreadRuntimeView(ThreadView{}).ParentEvents != nil {
		t.Fatal("disabled events became enabled")
	}
	source := ThreadView{ThreadID: "empty-events", ParentEvents: &ParentEventPage{Events: []ParentEvent{{ID: "original"}}}}
	clone := cloneThreadRuntimeView(source)
	clone.ParentEvents.Events[0].ID = "changed"
	if source.ParentEvents.Events[0].ID != "original" {
		t.Fatal("clone modified the source event")
	}
}

func TestParentEventEmptySnapshotsAcrossLifecycle(t *testing.T) {
	path := t.TempDir() + "/empty-events.db"
	host, service, gateway := parentEventHost(t, true, storage.SQLite(path))
	subscription, err := service.Subscribe(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	assertEmptyParentEventJSON(t, created)
	sent, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "hi", Input: UserInput{Text: "hi"}})
	if err != nil {
		t.Fatal(err)
	}
	assertEmptyParentEventJSON(t, sent)
	final := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
	assertEmptyParentEventJSON(t, final)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for {
		current, err := subscription.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		assertEmptyParentEventJSON(t, current)
		if current.LastOutcome != nil && current.Activity == ThreadActivityIdle {
			break
		}
	}
	page, err := service.(ThreadParentEventService).ListParentEvents(t.Context(), ListParentEventsInput{ThreadID: created.ThreadID})
	if err != nil {
		t.Fatal(err)
	}
	assertEmptyParentEventJSON(t, ThreadView{ThreadID: "empty-events", ParentEvents: &page})
	fork, err := service.Fork(t.Context(), ForkThreadInput{SourceThreadID: created.ThreadID, RequestKey: "fork"})
	if err != nil {
		t.Fatal(err)
	}
	assertEmptyParentEventJSON(t, fork)
	if len(gateway.Requests()) != 1 {
		t.Fatalf("provider requests=%d", len(gateway.Requests()))
	}
	if err := host.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, reopened, _ := parentEventHost(t, true, storage.SQLite(path))
	restored, err := reopened.View(t.Context(), created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	assertEmptyParentEventJSON(t, restored)
	_, disabled, _ := parentEventHost(t, false, storage.Memory())
	off, err := disabled.Create(t.Context(), CreateThreadInput{RequestKey: "disabled"})
	if err != nil {
		t.Fatal(err)
	}
	if off.ParentEvents != nil {
		t.Fatal("disabled parent events must remain absent")
	}
}

func TestParentEventEmptySnapshotsAfterCancellation(t *testing.T) {
	step := parentEventDone()
	step.BlockUntil = make(chan struct{})
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "events", StateCompatibilityKey: "test:events"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, step)
	_, service := parentEventHostWithGateway(t, gateway)
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "send", Input: UserInput{Text: "hi"}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := gateway.WaitForRequests(ctx, 1); err != nil {
		t.Fatal(err)
	}
	stopped, err := service.Cancel(ctx, CancelInput{ThreadID: created.ThreadID, RequestKey: "stop"})
	if err != nil {
		t.Fatal(err)
	}
	assertEmptyParentEventJSON(t, stopped)
	waitThreadExecutionDone(t, service.(*threadRuntimeService), created.ThreadID)
	final, err := service.View(ctx, created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	assertEmptyParentEventJSON(t, final)
}

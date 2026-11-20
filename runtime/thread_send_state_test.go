package runtime

import (
	"context"
	"errors"
	"testing"
)

func TestLookupSendStateTracksOriginalRequest(t *testing.T) {
	host, service := testThreadService(t, newBlockingThreadGateway())
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	assert := func(s ThreadService, key string, want SendState) SendDisposition {
		t.Helper()
		got, found, err := s.(ThreadSendStateReader).LookupSendState(t.Context(), LookupSendInput{ThreadID: created.ThreadID, RequestKey: RequestKey(key)})
		if err != nil || found != (want != "") || got.State != want {
			t.Fatalf("lookup %s: %+v found=%v err=%v", key, got, found, err)
		}
		return got
	}
	assert(service, "unknown", "")
	first, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "first", Input: UserInput{Text: "first"}})
	if err != nil {
		t.Fatal(err)
	}
	if d := assert(service, "first", SendStarted); d.TurnID != first.TurnID || d.RunID != first.RunID {
		t.Fatalf("wrong execution: %+v", d)
	}
	if _, err = service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "next", Input: UserInput{Text: "queued"}}); err != nil {
		t.Fatal(err)
	}
	d := assert(service, "next", SendQueued)
	state, err := service.(ThreadSchedulingReader).SchedulingState(t.Context(), created.ThreadID)
	if err != nil || state.Activity != ThreadActivityActive || state.TurnID != first.TurnID || len(state.QueuedRequestKeys) != 1 || state.QueuedRequestKeys[0] != "next" {
		t.Fatalf("scheduling state: %+v %v", state, err)
	}
	if d.QueueItemID == "" || d.TurnID != "" || d.RunID != "" {
		t.Fatalf("queued request inherited running turn: %+v", d)
	}
	if _, err = service.(ThreadQueueController).EditQueued(t.Context(), EditQueuedInput{ThreadID: created.ThreadID, QueueItemID: d.QueueItemID, ExpectedText: "queued", Text: "edited", RequestKey: "edit"}); err != nil {
		t.Fatal(err)
	}
	assert(service, "next", SendQueued)
	if _, err = service.DeleteQueued(t.Context(), DeleteQueuedInput{ThreadID: created.ThreadID, QueueItemID: d.QueueItemID, RequestKey: "remove"}); err != nil {
		t.Fatal(err)
	}
	assert(service, "next", SendRemoved)
	reopened, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return nil, errors.New("unexpected execution") }))
	if err != nil {
		t.Fatal(err)
	}
	assert(reopened, "next", SendRemoved)
	state, err = reopened.(ThreadSchedulingReader).SchedulingState(t.Context(), created.ThreadID)
	if err != nil || len(state.QueuedRequestKeys) != 0 {
		t.Fatalf("removed queue member: %+v %v", state, err)
	}
	assert(reopened, "remove", "")
	assert(reopened, "first", SendStarted)
}

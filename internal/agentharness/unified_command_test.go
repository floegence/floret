package agentharness

import "testing"

func TestUnifiedCommandActorDeduplicatesSendAndKeepsRetryLogicalTurn(t *testing.T) {
	actor := newUnifiedCommandActor("thread-1")
	first, err := actor.apply(unifiedCommand{RequestID: "req-1", Kind: unifiedCommandSend})
	if err != nil {
		t.Fatal(err)
	}
	replay, err := actor.apply(unifiedCommand{RequestID: "req-1", Kind: unifiedCommandSend})
	if err != nil || replay != first {
		t.Fatalf("replay=%#v err=%v", replay, err)
	}
	retry, err := actor.apply(unifiedCommand{RequestID: "req-retry", Kind: unifiedCommandRetry})
	if err != nil {
		t.Fatal(err)
	}
	if retry.TurnID != first.TurnID || retry.RunID == first.RunID {
		t.Fatalf("retry=%#v first=%#v", retry, first)
	}
	if len(actor.log.events()) != 2 {
		t.Fatalf("events=%d", len(actor.log.events()))
	}
}

func TestUnifiedCommandActorResolveCancelAreIdempotentAndTerminalDoesNotRegress(t *testing.T) {
	actor := newUnifiedCommandActor("thread-1")
	if _, err := actor.apply(unifiedCommand{RequestID: "req-1", Kind: unifiedCommandSend}); err != nil {
		t.Fatal(err)
	}
	actor.state.Interaction = &unifiedPendingInteraction{ID: "interaction-1", ThreadID: "thread-1", TurnID: actor.state.ActiveTurn.ID, Kind: "approval"}
	if _, err := actor.apply(unifiedCommand{RequestID: "req-resolve", Kind: unifiedCommandResolve}); err != nil {
		t.Fatal(err)
	}
	if _, err := actor.apply(unifiedCommand{RequestID: "req-stop", Kind: unifiedCommandCancel}); err != nil {
		t.Fatal(err)
	}
	replay, err := actor.apply(unifiedCommand{RequestID: "req-stop", Kind: unifiedCommandCancel})
	if err != nil || replay.RequestID != "req-stop" {
		t.Fatalf("cancel replay=%#v err=%v", replay, err)
	}
	if actor.state.ActiveTurn.Status != "cancelled" || !actor.state.Terminal {
		t.Fatalf("state=%#v", actor.state)
	}
	if _, err := actor.apply(unifiedCommand{RequestID: "req-late", Kind: unifiedCommandResolve}); err == nil {
		t.Fatal("late resolve revived terminal turn")
	}
}

package runtime

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/provider"
)

func TestMessageEditCompletedAndActive(t *testing.T) {
	for _, active := range []bool{false, true} {
		t.Run(map[bool]string{false: "completed", true: "active"}[active], func(t *testing.T) {
			gateway := newBlockingThreadGateway()
			host, service := testThreadService(t, gateway)
			thread, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
			if err != nil {
				t.Fatal(err)
			}
			input := UserInput{Text: "original", References: []MessageReference{{ReferenceID: "ref", Kind: MessageReferenceText, Label: "note", Text: "retained"}}}
			_, err = service.Send(t.Context(), SendInput{ThreadID: thread.ThreadID, RequestKey: "original", Input: input})
			if err != nil {
				t.Fatal(err)
			}
			<-gateway.started
			if !active {
				close(gateway.release)
				waitThreadView(t, service, thread.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle })
			}
			editor := service.(ThreadMessageEditor)
			target, err := editor.LatestEditableMessage(t.Context(), thread.ThreadID)
			if err != nil {
				t.Fatal(err)
			}
			replacement := target.Input
			replacement.Text = "replacement"
			command := EditLastUserMessageInput{ThreadID: thread.ThreadID, ItemID: target.ItemID, ExpectedInput: target.Input, Input: replacement, RequestKey: "edit", StopActive: active}
			ack, err := editor.EditLastUserMessage(t.Context(), command)
			if err != nil {
				t.Fatal(err)
			}
			if ack.TurnID == target.TurnID || ack.TurnID == "" {
				t.Fatalf("ack %+v", ack)
			}
			if active {
				close(gateway.release)
			}
			view := waitThreadView(t, service, thread.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle })
			users := 0
			for _, item := range view.Items {
				if item.Kind == ThreadItemUser {
					users++
					if item.Text != "replacement" || !reflect.DeepEqual(item.References, target.Input.References) {
						t.Fatalf("user %+v", item)
					}
				}
			}
			if users != 1 {
				t.Fatalf("users %d: %+v", users, view.Items)
			}
			path, err := host.store.repo.Path(t.Context(), thread.ThreadID.String(), "")
			if err != nil {
				t.Fatal(err)
			}
			messages, err := sessiontree.BuildContextChecked(path, sessiontree.ContextOptions{})
			if err != nil {
				t.Fatal(err)
			}
			for _, message := range messages {
				if message.Content == "original" {
					t.Fatal("old input leaked into provider history")
				}
			}
			history, err := editor.ReadMessageEditHistory(t.Context(), LookupMessageEditInput{ThreadID: thread.ThreadID, RequestKey: "edit"})
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, item := range history {
				if item.Kind == ThreadItemUser && item.Text == "original" {
					found = true
				}
			}
			if !found {
				t.Fatal("original history not readable")
			}
			before, _ := host.store.repo.Entries(t.Context(), thread.ThreadID.String())
			replay, err := editor.EditLastUserMessage(t.Context(), command)
			if err != nil || replay != ack {
				t.Fatalf("replay %+v %v", replay, err)
			}
			after, _ := host.store.repo.Entries(t.Context(), thread.ThreadID.String())
			if len(before) != len(after) {
				t.Fatal("replay wrote records")
			}
			command.Input.Text = "different"
			if _, err = editor.EditLastUserMessage(t.Context(), command); !errors.Is(err, ErrRequestConflict) {
				t.Fatalf("mismatched replay %v", err)
			}
			command.RequestKey = "stale"
			if _, err = editor.EditLastUserMessage(t.Context(), command); !errors.Is(err, ErrRequestConflict) {
				t.Fatalf("stale edit %v", err)
			}
			reopened, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return nil, errors.New("unexpected execution") }))
			if err != nil {
				t.Fatal(err)
			}
			command.Input = replacement
			command.RequestKey = "edit"
			got, err := reopened.(ThreadMessageEditor).EditLastUserMessage(t.Context(), command)
			if err != nil || got != ack {
				t.Fatalf("restart replay %+v %v", got, err)
			}
		})
	}
}

func TestMessageEditCompactionRetryAndUsage(t *testing.T) {
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "compact", StateCompatibilityKey: "test:compact"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, compactionTestStep("first"), compactionTestStep("second"), compactionTestStep("third"), compactionTestStep("summary"), compactionTestStep("edited"), compactionTestStep("retry"))
	_, service := testThreadServiceWithAgent(t, compactionTestAgent(t, gateway))
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	seedCompactionHistory(t, service, created.ThreadID)
	compact, err := service.(ThreadContextCompactor).CompactContext(t.Context(), CompactContextInput{ThreadID: created.ThreadID, RequestKey: "compact"})
	if err != nil {
		t.Fatal(err)
	}
	prior := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.TurnID == compact.TurnID && v.Activity == ThreadActivityIdle })
	if prior.Failure != nil {
		t.Fatal(prior.Failure)
	}
	editor := service.(ThreadMessageEditor)
	target, err := editor.LatestEditableMessage(t.Context(), created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	ack, err := editor.EditLastUserMessage(t.Context(), EditLastUserMessageInput{ThreadID: created.ThreadID, ItemID: target.ItemID, ExpectedInput: target.Input, Input: UserInput{Text: "replacement after compaction"}, RequestKey: "edit"})
	if err != nil {
		t.Fatal(err)
	}
	after := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.TurnID == ack.TurnID && v.Activity == ThreadActivityIdle })
	if after.Failure != nil {
		t.Fatal(after.Failure)
	}
	if after.Context.UsageTotals.InputTokens < prior.Context.UsageTotals.InputTokens {
		t.Fatalf("usage lost: before=%+v after=%+v", prior.Context.UsageTotals, after.Context.UsageTotals)
	}
	if _, err = service.Retry(t.Context(), RetryInput{ThreadID: created.ThreadID, SourceTurnID: ack.TurnID, RequestKey: "retry"}); err != nil {
		t.Fatal(err)
	}
	final := waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle })
	if final.Failure != nil {
		t.Fatal(final.Failure)
	}
}

func TestMessageEditRetainsQueueDuringStop(t *testing.T) {
	gateway := newBlockingThreadGateway()
	_, service := testThreadService(t, gateway)
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "original", Input: UserInput{Text: "original"}})
	if err != nil {
		t.Fatal(err)
	}
	<-gateway.started
	_, err = service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "pending", Input: UserInput{Text: "pending"}})
	if err != nil {
		t.Fatal(err)
	}
	editor := service.(ThreadMessageEditor)
	target, err := editor.LatestEditableMessage(t.Context(), created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = editor.EditLastUserMessage(t.Context(), EditLastUserMessageInput{ThreadID: created.ThreadID, ItemID: target.ItemID, ExpectedInput: target.Input, Input: UserInput{Text: "replacement"}, RequestKey: "edit", StopActive: true}); err != nil {
		t.Fatal(err)
	}
	view, err := service.View(t.Context(), created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Queue) != 1 || view.Queue[0].Input.Text != "pending" {
		t.Fatalf("queue %+v", view.Queue)
	}
	if _, err = service.Cancel(t.Context(), CancelInput{ThreadID: created.ThreadID, RequestKey: "cancel"}); err != nil {
		t.Fatal(err)
	}
}

func TestMessageEditAdmissionHonorsHostShutdownFence(t *testing.T) {
	gateway := newBlockingThreadGateway()
	host, service := testThreadService(t, gateway)
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "old", Input: UserInput{Text: "old"}}); err != nil {
		t.Fatal(err)
	}
	<-gateway.started
	close(gateway.release)
	waitThreadView(t, service, created.ThreadID, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle })
	editor := service.(ThreadMessageEditor)
	target, err := editor.LatestEditableMessage(t.Context(), created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	before, err := host.store.repo.Entries(t.Context(), created.ThreadID.String())
	if err != nil {
		t.Fatal(err)
	}
	// Hold shutdown after its public fence, before storage/actor closure. A
	// transition already waiting for Stop must recheck at canonical admission.
	host.closeMu.Lock()
	host.closing = true
	host.closeMu.Unlock()
	defer func() { host.closeMu.Lock(); host.closing = false; host.closeMu.Unlock() }()
	runtime := service.(*threadRuntimeService)
	_, err = runtime.admitMessageEdit(t.Context(), runtime.runtime(created.ThreadID), EditLastUserMessageInput{ThreadID: created.ThreadID, ItemID: target.ItemID, ExpectedInput: target.Input, Input: UserInput{Text: "new"}, RequestKey: "edit"})
	if !errors.Is(err, ErrHostClosed) {
		t.Fatalf("shutdown admitted replacement: %v", err)
	}
	after, err := host.store.repo.Entries(t.Context(), created.ThreadID.String())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("shutdown changed canonical history")
	}
}

package runtime

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestDelegatedInputCannotCrossParentStop(t *testing.T) {
	for _, op := range []string{"send", "steer"} {
		t.Run(op, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			gateway := newBlockingThreadGateway()
			h, svc := testThreadService(t, gateway)
			root, err := svc.Create(ctx, CreateThreadInput{RequestKey: "parent"})
			if err != nil {
				t.Fatal(err)
			}
			root, err = svc.Send(ctx, SendInput{ThreadID: root.ThreadID, RequestKey: "delegate", Input: UserInput{Text: "delegate"}})
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-gateway.started:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			child, err := svc.Create(ctx, CreateThreadInput{ParentThreadID: root.ThreadID, ParentTurnID: root.TurnID, HostProfileRef: "test", TaskName: "child", RequestKey: "create-child"})
			if err != nil {
				t.Fatal(err)
			}
			source := ThreadExecutionTarget{root.ThreadID, root.TurnID, root.RunID}
			h.idMu.Lock()
			pause := &pausedPromotionIDs{IDSource: h.idSource, entered: make(chan struct{}), resume: make(chan struct{})}
			h.idSource = pause
			h.idMu.Unlock()
			resume := sync.OnceFunc(func() { close(pause.resume) })
			defer resume()
			done := make(chan error, 1)
			go func() {
				var err error
				if op == "send" {
					_, err = svc.Send(ctx, SendInput{ThreadID: child.ThreadID, ParentExecution: &source, RequestKey: "start", Input: UserInput{Text: "work"}})
				} else {
					_, err = svc.(ThreadSteeringController).Steer(ctx, SteerInput{ThreadID: child.ThreadID, ParentExecution: &source, RequestKey: "start", Input: UserInput{Text: "work"}})
				}
				done <- err
			}()
			select {
			case <-pause.entered:
			case err := <-done:
				t.Fatalf("did not reach admission: %v", err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			stopper := svc.(ThreadSubtreeStopper)
			frozen, err := stopper.SubtreeExecutions(ctx, root.ThreadID)
			if err != nil {
				t.Fatal(err)
			}
			out, err := stopper.StopSubtree(ctx, frozen)
			if err != nil || len(out.Admitted) != 1 || out.Admitted[0] != source {
				t.Fatalf("stop=%+v err=%v", out, err)
			}
			resume()
			select {
			case err := <-done:
				if !errors.Is(err, ErrRequestConflict) {
					t.Fatalf("late %s: %v", op, err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			v, err := svc.View(ctx, child.ThreadID)
			if err != nil || v.Activity != ThreadActivityIdle || v.TurnID != "" || len(v.Queue) != 0 || gateway.requests.Load() != 1 {
				t.Fatalf("late child: turn=%q queue=%d requests=%d err=%v", v.TurnID, len(v.Queue), gateway.requests.Load(), err)
			}
		})
	}
}

func TestDelegatedInputRequiresDirectLiveParent(t *testing.T) {
	ctx := t.Context()
	_, svc := testThreadService(t, newBlockingThreadGateway())
	start := func(key RequestKey) ThreadView {
		t.Helper()
		v, err := svc.Create(ctx, CreateThreadInput{RequestKey: key})
		if err != nil {
			t.Fatal(err)
		}
		v, err = svc.Send(ctx, SendInput{ThreadID: v.ThreadID, Input: UserInput{Text: "delegate"}, RequestKey: "start"})
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	owner, other := start("owner"), start("other")
	child, err := svc.Create(ctx, CreateThreadInput{ParentThreadID: owner.ThreadID, ParentTurnID: owner.TurnID, HostProfileRef: "test", TaskName: "child", RequestKey: "child"})
	if err != nil {
		t.Fatal(err)
	}
	owned := ThreadExecutionTarget{owner.ThreadID, owner.TurnID, owner.RunID}
	foreign := ThreadExecutionTarget{other.ThreadID, other.TurnID, other.RunID}
	input := SendInput{ThreadID: child.ThreadID, ParentExecution: &owned, RequestKey: "start", Input: UserInput{Text: "work"}}
	active, err := svc.Send(ctx, input)
	if err != nil || active.Activity != ThreadActivityActive {
		t.Fatalf("owned input: %v %v", active.Activity, err)
	}
	for _, source := range []ThreadExecutionTarget{foreign, {owner.ThreadID, owner.TurnID, "stale-run"}} {
		input.ParentExecution, input.RequestKey = &source, "bad-input"
		if _, err := svc.Send(ctx, input); !errors.Is(err, ErrRequestConflict) {
			t.Fatalf("unowned queue: %v", err)
		}
		if _, err := svc.(ThreadSteeringController).Steer(ctx, SteerInput{ThreadID: child.ThreadID, ParentExecution: &source, RequestKey: "bad-steer", Input: input.Input}); !errors.Is(err, ErrRequestConflict) {
			t.Fatalf("unowned steer: %v", err)
		}
	}
	v, err := svc.View(ctx, child.ThreadID)
	if err != nil || v.RunID != active.RunID || len(v.Queue) != 0 {
		t.Fatalf("rejected inputs: run=%q queue=%d err=%v", v.RunID, len(v.Queue), err)
	}
}

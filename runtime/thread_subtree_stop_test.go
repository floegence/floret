package runtime

import (
	"errors"
	"testing"

	"github.com/floegence/floret/v7/identity"
)

func TestExactIdleSubtreeStopPreservesReplacement(t *testing.T) {
	_, svc := testThreadService(t, newBlockingThreadGateway())
	ctx := t.Context()
	create := func(key string, parent identity.ThreadID) identity.ThreadID {
		t.Helper()
		in := CreateThreadInput{RequestKey: RequestKey(key), ParentThreadID: parent}
		if parent != "" {
			in.TaskName, in.HostProfileRef = key, "test"
		}
		v, err := svc.Create(ctx, in)
		if err != nil {
			t.Fatal(err)
		}
		return v.ThreadID
	}
	send := func(id identity.ThreadID, key string) ThreadView {
		t.Helper()
		v, err := svc.Send(ctx, SendInput{ThreadID: id, RequestKey: RequestKey(key), Input: UserInput{Text: key}})
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	root := create("root", "")
	middle := create("middle", root)
	leaf := create("leaf", middle)
	sibling := create("unawaited", root)
	other := create("other", "")
	send(leaf, "research")
	send(sibling, "other-research")
	unrelated := send(other, "unrelated")
	controller := svc.(ThreadSubtreeStopper)
	snapshot, err := controller.SubtreeExecutions(ctx, root)
	if err != nil || len(snapshot) != 2 {
		t.Fatalf("targets=%+v err=%v", snapshot, err)
	}
	in := StopSubtreeInput{ThreadID: root, Targets: snapshot, RequestKey: "click"}
	foreign := ThreadExecutionTarget{other, unrelated.TurnID, unrelated.RunID}
	for _, bad := range [][]ThreadExecutionTarget{snapshot[:1], append(append([]ThreadExecutionTarget{}, snapshot...), foreign)} {
		if out, err := controller.StopSubtree(ctx, StopSubtreeInput{ThreadID: root, Targets: bad, RequestKey: "bad"}); !errors.Is(err, ErrRequestConflict) || len(out.Admitted) > 0 {
			t.Fatalf("unsafe scope: %+v %v", out, err)
		}
		for _, id := range []identity.ThreadID{leaf, sibling, other} {
			v, err := svc.View(ctx, id)
			if err != nil || v.Activity != ThreadActivityActive || v.Cancellation != nil {
				t.Fatalf("invalid snapshot mutated %s: %+v %v", id, v, err)
			}
		}
	}
	if out, err := controller.StopSubtree(ctx, in); err != nil || len(out.Admitted) != 2 {
		t.Fatalf("stop admission=%+v err=%v", out, err)
	}
	for _, id := range []identity.ThreadID{leaf, sibling} {
		v := waitThreadView(t, svc, id, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle })
		if v.LastOutcome == nil || *v.LastOutcome != TurnOutcomeCancelled {
			t.Fatalf("%s did not cancel: %v", id, v.LastOutcome)
		}
	}
	if replay, err := controller.StopSubtree(ctx, in); err != nil || len(replay.Admitted) != 0 || len(replay.AlreadyEnded) != 2 {
		t.Fatalf("settled replay=%+v err=%v", replay, err)
	}
	next := send(leaf, "explicit-new-task")
	_, err = controller.StopSubtree(ctx, in)
	if err != nil && !errors.Is(err, ErrRequestConflict) {
		t.Fatal(err)
	}
	for _, id := range []identity.ThreadID{leaf, other} {
		v, err := svc.View(ctx, id)
		if err != nil || v.Activity != ThreadActivityActive || id == leaf && v.RunID != next.RunID {
			t.Fatalf("stopped another execution: %s %+v %v", id, v, err)
		}
	}
}

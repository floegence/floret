package runtime

import (
	"context"
	"errors"
	"testing"

	"github.com/floegence/floret/v7/identity"
)

func TestCancelIncludesDescendantsOnlyWhenRequested(t *testing.T) {
	for _, include := range []bool{false, true} {
		t.Run(map[bool]string{false: "selected_only", true: "subtree"}[include], func(t *testing.T) {
			gateway := newBlockingThreadGateway()
			_, svc := testThreadService(t, gateway)
			create := func(key RequestKey, parent identity.ThreadID) identity.ThreadID {
				t.Helper()
				input := CreateThreadInput{RequestKey: key, ParentThreadID: parent}
				if parent != "" {
					input.TaskName = string(key)
					input.HostProfileRef = "test"
				}
				v, err := svc.Create(t.Context(), input)
				if err != nil {
					t.Fatal(err)
				}
				return v.ThreadID
			}
			root := create("root", "")
			child := create("child", root)
			grandchild := create("grandchild", child)
			other := create("other", "")
			for _, id := range []identity.ThreadID{root, child, grandchild, other} {
				if _, err := svc.Send(t.Context(), SendInput{ThreadID: id, RequestKey: RequestKey("send:" + id.String()), Input: UserInput{Text: "work"}}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := svc.Send(t.Context(), SendInput{ThreadID: child, RequestKey: "pending", Input: UserInput{Text: "later"}}); err != nil {
				t.Fatal(err)
			}
			if _, err := svc.Cancel(t.Context(), CancelInput{ThreadID: root, RequestKey: "stop", Mode: CancelModeGraceful, IncludeDescendants: include}); err != nil {
				t.Fatal(err)
			}
			waitThreadView(t, svc, root, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle })
			for _, id := range []identity.ThreadID{child, grandchild} {
				if include {
					v := waitThreadView(t, svc, id, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle })
					if v.LastOutcome == nil || *v.LastOutcome != TurnOutcomeCancelled {
						t.Fatalf("descendant outcome=%+v", v)
					}
					if id == child && len(v.Queue) != 1 {
						t.Fatal("subtree stop lost pending input")
					}
				} else {
					v, err := svc.View(t.Context(), id)
					if err != nil || v.Activity != ThreadActivityActive {
						t.Fatalf("default stop changed descendant: %v %+v", err, v)
					}
				}
			}
			v, err := svc.View(t.Context(), other)
			if err != nil || v.Activity != ThreadActivityActive {
				t.Fatal("subtree stop affected independent thread")
			}
			// An idle parent may still own active children from earlier work.
			if !include {
				if _, err := svc.Cancel(t.Context(), CancelInput{ThreadID: root, RequestKey: "stop-remaining", Mode: CancelModeGraceful, IncludeDescendants: true}); err != nil {
					t.Fatal(err)
				}
				waitThreadView(t, svc, child, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle })
			}
		})
	}
}

func TestCanceledChildCreationDoesNotAdmitAfterSubtreeStop(t *testing.T) {
	_, svc := testThreadService(t, newBlockingThreadGateway())
	root, err := svc.Create(t.Context(), CreateThreadInput{RequestKey: "root"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := svc.Create(ctx, CreateThreadInput{ParentThreadID: root.ThreadID, RequestKey: "late-create"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("create=%v", err)
	}
	if _, err := svc.Fork(ctx, ForkThreadInput{SourceThreadID: root.ThreadID, ParentThreadID: root.ThreadID, RequestKey: "late-fork"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("fork=%v", err)
	}
	children, err := svc.List(t.Context(), ThreadScope{ParentID: &root.ThreadID})
	if err != nil || len(children) != 0 {
		t.Fatalf("late children=%+v err=%v", children, err)
	}
}

package runtime

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
	internalstorage "github.com/floegence/floret/v7/internal/storage"
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
	in, err := controller.SubtreeExecutions(ctx, root)
	if err != nil || len(in.Targets) != 2 {
		t.Fatalf("targets=%+v err=%v", in, err)
	}
	snapshot := in.Targets
	foreign := ThreadExecutionTarget{other, unrelated.TurnID, unrelated.RunID}
	for _, bad := range [][]ThreadExecutionTarget{snapshot[:1], append(append([]ThreadExecutionTarget{}, snapshot...), foreign)} {
		key, err := subtreeStopKey(root, bad)
		if err != nil {
			t.Fatal(err)
		}
		if out, err := controller.StopSubtree(ctx, StopSubtreeInput{ThreadID: root, Targets: bad, RequestKey: key}); !errors.Is(err, ErrRequestConflict) || len(out.Admitted) > 0 {
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
	late := create("new-child", root)
	send(late, "new-research")
	changed, err := controller.SubtreeExecutions(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	changed.RequestKey = in.RequestKey
	if out, err := controller.StopSubtree(ctx, changed); !errors.Is(err, ErrRequestConflict) || len(out.Admitted) != 0 {
		t.Fatalf("original request identity stopped different work: %+v %v", out, err)
	}
	next := send(leaf, "explicit-new-task")
	_, err = controller.StopSubtree(ctx, in)
	if err != nil && !errors.Is(err, ErrRequestConflict) {
		t.Fatal(err)
	}
	for _, id := range []identity.ThreadID{leaf, late, other} {
		v, err := svc.View(ctx, id)
		if err != nil || v.Activity != ThreadActivityActive || id == leaf && v.RunID != next.RunID {
			t.Fatalf("stopped another execution: %s %+v %v", id, v, err)
		}
	}
}

type subtreeStopFault struct {
	*internalstorage.BackendKernel
	mu    sync.Mutex
	order []string
	fail  string
	err   error
}

func (r *subtreeStopFault) CancelTurn(ctx context.Context, req sessiontree.CancelTurnRequest) (sessiontree.CancelTurnResult, error) {
	if req.RequestOnly {
		r.mu.Lock()
		r.order = append(r.order, req.ThreadID)
		fail := req.ThreadID == r.fail
		if fail {
			r.fail = ""
		}
		r.mu.Unlock()
		if fail {
			return sessiontree.CancelTurnResult{}, r.err
		}
	}
	return r.BackendKernel.CancelTurn(ctx, req)
}

func TestExactSubtreeStopPartialAdmission(t *testing.T) {
	h, service := testThreadService(t, newBlockingThreadGateway())
	repo := &subtreeStopFault{BackendKernel: h.store.repo.(*internalstorage.BackendKernel), err: errors.New("injected cancel write failure")}
	h.store.repo = repo
	ctx := t.Context()
	start := func(parent identity.ThreadID, key string) identity.ThreadID {
		t.Helper()
		in := CreateThreadInput{ParentThreadID: parent, RequestKey: RequestKey(key)}
		if parent != "" {
			in.TaskName, in.HostProfileRef = key, "test"
		}
		v, err := service.Create(ctx, in)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = service.Send(ctx, SendInput{ThreadID: v.ThreadID, RequestKey: "start", Input: UserInput{Text: key}}); err != nil {
			t.Fatal(err)
		}
		return v.ThreadID
	}
	root := start("", "root")
	sibling := start(root, "sibling")
	branch := start(root, "branch")
	child := start(branch, "child")
	leaf := start(child, "leaf")
	if _, err := service.Send(ctx, SendInput{ThreadID: branch, RequestKey: "queued", Input: UserInput{Text: "later"}}); err != nil {
		t.Fatal(err)
	}
	stopper := service.(ThreadSubtreeStopper)
	snapshot, err := stopper.SubtreeExecutions(ctx, branch)
	if err != nil || len(snapshot.Targets) != 3 {
		t.Fatalf("snapshot=%+v %v", snapshot, err)
	}
	slices.Reverse(snapshot.Targets) // Transport order must not determine admission order.
	repo.fail = child.String()
	out, err := stopper.StopSubtree(ctx, snapshot)
	if !errors.Is(err, repo.err) || len(out.Admitted) != 1 || out.Admitted[0].ThreadID != branch {
		t.Fatalf("partial=%+v %v", out, err)
	}
	waitThreadView(t, service, branch, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle })
	out, err = stopper.StopSubtree(ctx, snapshot)
	if err != nil || len(out.Admitted) != 2 || len(out.AlreadyEnded) != 1 || out.AlreadyEnded[0].ThreadID != branch {
		t.Fatalf("retry=%+v %v", out, err)
	}
	for _, id := range []identity.ThreadID{branch, child, leaf} {
		v := waitThreadView(t, service, id, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle })
		if v.LastOutcome == nil || *v.LastOutcome != TurnOutcomeCancelled || (id == branch && (len(v.Queue) != 1 || v.Queue[0].RequestKey != "queued")) {
			t.Fatalf("unexpected settlement %s: outcome=%v queue=%+v", id, v.LastOutcome, v.Queue)
		}
	}
	for _, id := range []identity.ThreadID{root, sibling} {
		v, err := service.View(ctx, id)
		if err != nil || v.Activity != ThreadActivityActive || v.Cancellation != nil {
			t.Fatalf("unselected %s: cancellation=%v %v", id, v.Cancellation, err)
		}
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if !slices.Equal(repo.order, []string{branch.String(), child.String(), child.String(), leaf.String()}) {
		t.Fatalf("admission order=%v", repo.order)
	}
}

package runtime

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/floegence/floret/v7/identity"
)

func TestAwaitThreadsQuietAndCancelable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		_, threads := testThreadService(t, newBlockingThreadGateway())
		service := threads.(*threadRuntimeService)
		parent, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "parent"})
		if err != nil {
			t.Fatal(err)
		}
		child, err := service.Create(t.Context(), CreateThreadInput{ParentThreadID: parent.ThreadID, TaskName: "child", HostProfileRef: "test", RequestKey: "child"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = service.Send(t.Context(), SendInput{ThreadID: child.ThreadID, RequestKey: "send", Input: UserInput{Text: "work"}}); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() {
			_, err := service.WaitThreads(ctx, WaitThreadsInput{Scope: ThreadScope{ParentID: &parent.ThreadID}, ThreadIDs: []identity.ThreadID{child.ThreadID}, Until: ThreadWaitAll})
			done <- err
		}()
		synctest.Wait()
		time.Sleep(10 * time.Minute)
		select {
		case err := <-done:
			t.Fatalf("quiet wait ended: %v", err)
		default:
		}
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel: %v", err)
		}
	})
}

func TestAwaitExecutionSelection(t *testing.T) {
	completed, failed := TurnOutcomeCompleted, TurnOutcomeFailed
	a := ThreadSummary{ID: "a", TurnID: "at", RunID: "ar", Activity: ThreadActivityIdle, LastOutcome: &completed}
	b := ThreadSummary{ID: "b", TurnID: "bt", RunID: "br", Activity: ThreadActivityActive}
	for _, until := range []ThreadWaitUntil{ThreadWaitAll, ThreadWaitAny} {
		bound := make(map[identity.ThreadID]childWaitExecution)
		selectPair := func(other ThreadSummary) ThreadWaitResult {
			return selectChildExecutions(selectWaitThreads([]ThreadSummary{a, other}, []identity.ThreadID{"a", "b"}), bound, until)
		}
		first := selectPair(b)
		if (first.Reason == ThreadWaitCompleted) != (until == ThreadWaitAny) || len(first.Threads) != 2 {
			t.Fatalf("%s first: %+v", until, first)
		}
		changed := b
		changed.Attention.InputCount = 1
		if got := selectPair(changed); got.Reason != ThreadWaitAttentionRequired {
			t.Fatalf("attention: %+v", got)
		}
		changed = b
		changed.Activity, changed.LastOutcome = ThreadActivityIdle, &failed
		if got := selectPair(changed); got.Reason != ThreadWaitUnsuccessful {
			t.Fatalf("failure: %+v", got)
		}
		changed.LastOutcome = &completed
		if got := selectPair(changed); got.Reason != ThreadWaitCompleted || len(got.Threads) != 2 {
			t.Fatalf("complete: %+v", got)
		}
		changed.RunID = "new-run"
		if got := selectPair(changed); got.Reason != ThreadWaitUnavailable || len(got.Threads) != 1 || len(got.UnavailableThreadIDs) != 1 || got.UnavailableThreadIDs[0] != b.ID {
			t.Fatalf("replacement: %+v", got)
		}
	}
}

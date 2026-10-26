package runtime

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	internalstorage "github.com/floegence/floret/v7/internal/storage"
)

type blockedAdmissionHistory struct {
	*internalstorage.BackendKernel
	entered, release chan struct{}
	once             sync.Once
}

func (repo *blockedAdmissionHistory) ScanPathProjection(ctx context.Context, thread, leaf string, selectSources func([]sessiontree.PathMetadata) error, fields func(sessiontree.PathMetadata) []string, consume func(sessiontree.PathProjectionPage) error) error {
	repo.once.Do(func() { close(repo.entered) })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-repo.release:
	}
	return repo.BackendKernel.ScanPathProjection(ctx, thread, leaf, selectSources, fields, consume)
}
func TestColdSendAckPrecedesCompleteHistoryAndKeepsWindowReadable(t *testing.T) {
	for _, browsing := range []bool{false, true} {
		t.Run(fmt.Sprint("browsing=", browsing), func(t *testing.T) {
			gateway := newBlockingThreadGateway()
			host, public := testThreadService(t, gateway)
			service := public.(*threadRuntimeService)
			id := identity.ThreadID("cold-admission")
			if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
				t.Fatal(err)
			}
			for i := range 300 {
				if _, err := host.store.repo.Append(t.Context(), sessiontree.Entry{ThreadID: id.String(), TurnID: "old-turn", RunID: "old-run", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: fmt.Sprint("retained-", i)}}, sessiontree.AppendOptions{ID: fmt.Sprint("history-", i)}); err != nil {
					t.Fatal(err)
				}
			}
			repo := &blockedAdmissionHistory{BackendKernel: host.store.repo.(*internalstorage.BackendKernel), entered: make(chan struct{}), release: make(chan struct{})}
			host.store.repo = repo
			var release sync.Once
			defer release.Do(func() { close(repo.release) })
			var sub *ThreadUpdateSubscription
			if browsing {
				sub, _ = coldWindowBaseline(t, public, id)
			}
			input := SendInput{ThreadID: id, RequestKey: "immediate", Input: UserInput{Text: "visible before execution history"}}
			deadline, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			ack, err := public.(ThreadCommandService).SendAck(deadline, input)
			if err != nil || ack.TurnID == "" || ack.RunID == "" {
				t.Fatalf("admission waited for historical preparation: %+v %v", ack, err)
			}
			select {
			case <-repo.entered:
			case <-deadline.Done():
				t.Fatal("execution preparation did not start")
			}
			select {
			case <-gateway.started:
				t.Fatal("provider ran before complete preparation")
			default:
			}
			if sub != nil {
				for {
					update, err := sub.Next(deadline)
					if err != nil {
						t.Fatal(err)
					}
					if update.Window != nil {
						t.Fatal("admission replaced accepted history")
					}
					if update.Facts.Activity == ThreadActivityActive {
						found := false
						for _, change := range update.Items {
							if change.ItemID == "user:immediate" && change.Item != nil && change.Item.Text == input.Input.Text {
								found = true
							}
						}
						if !found {
							t.Fatal("admitted user missing from live update")
						}
						break
					}
				}
			}
			replay, err := public.(ThreadCommandService).SendAck(deadline, input)
			if err != nil || replay.TurnID != ack.TurnID || replay.RunID != ack.RunID {
				t.Fatalf("replay=%+v %v", replay, err)
			}
			queued, err := public.(ThreadCommandService).SendAck(deadline, SendInput{ThreadID: id, RequestKey: "queued", Input: UserInput{Text: "later"}})
			if err != nil || queued.TurnID != "" {
				t.Fatalf("queued=%+v %v", queued, err)
			}
			release.Do(func() { close(repo.release) })
			view, err := service.View(t.Context(), id)
			if err != nil || countThreadItems(view.Items, ThreadItemUser) != 301 || len(view.Queue) != 1 {
				t.Fatalf("promotion lost history or queue: items=%d queue=%d %v", len(view.Items), len(view.Queue), err)
			}

		})
	}
}

func TestColdSendAckCancellationDoesNotWaitForHistory(t *testing.T) {
	gateway := newBlockingThreadGateway()
	host, public := testThreadService(t, gateway)
	id := identity.ThreadID("cancel-preparation")
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
		t.Fatal(err)
	}
	repo := &blockedAdmissionHistory{BackendKernel: host.store.repo.(*internalstorage.BackendKernel), entered: make(chan struct{}), release: make(chan struct{})}
	host.store.repo = repo
	defer close(repo.release)
	ack, err := public.(ThreadCommandService).SendAck(t.Context(), SendInput{ThreadID: id, RequestKey: "send", Input: UserInput{Text: "cancel before provider"}})
	if err != nil {
		t.Fatal(err)
	}
	<-repo.entered
	done := make(chan error, 1)
	go func() {
		_, err := public.(ThreadCommandService).CancelAck(t.Context(), CancelInput{ThreadID: id, RequestKey: "stop"})
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Stop waited for complete history")
	}
	current := public.(*threadRuntimeService).currentResult(public.(*threadRuntimeService).runtime(id), threadFullResult)
	if current.Activity != ThreadActivityIdle || current.TurnID != ack.TurnID || current.LastOutcome == nil || *current.LastOutcome != TurnOutcomeCancelled {
		t.Fatalf("cancelled state: %+v", current)
	}
	select {
	case <-gateway.started:
		t.Fatal("cancelled preparation started provider")
	default:
	}
}

func TestColdSendAckLegacySubscriptionKeepsCompleteHistory(t *testing.T) {
	gateway := newBlockingThreadGateway()
	host, public := testThreadService(t, gateway)
	id := identity.ThreadID("legacy-admission")
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
		t.Fatal(err)
	}
	for i := range 12 {
		if _, err := host.store.repo.Append(t.Context(), sessiontree.Entry{ThreadID: id.String(), TurnID: "old-turn", RunID: "old-run", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: fmt.Sprint("history-", i)}}, sessiontree.AppendOptions{ID: fmt.Sprint(i)}); err != nil {
			t.Fatal(err)
		}
	}
	sub, err := public.Subscribe(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if _, err := public.(ThreadCommandService).SendAck(t.Context(), SendInput{ThreadID: id, RequestKey: "send", Input: UserInput{Text: "new input"}}); err != nil {
		t.Fatal(err)
	}
	deadline, cancel := context.WithTimeout(t.Context(), time.Second*3)
	defer cancel()
	for {
		view, err := sub.Next(deadline)
		if err != nil {
			t.Fatal(err)
		}
		if countThreadItems(view.Items, ThreadItemUser) < 12 {
			t.Fatalf("legacy subscriber received partial history: %d", len(view.Items))
		}
		if view.Activity == ThreadActivityActive {
			break
		}
	}
}

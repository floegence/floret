package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/event"
	"github.com/floegence/floret/v7/internal/outputbody"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/threadquery"
	"strings"
	"testing"
	"time"
)

type liveBodyGateway struct {
	unit    string
	count   int
	release chan struct{}
}

func (*liveBodyGateway) Identity() provider.Identity {
	return provider.Identity{Provider: "test", Model: "live-body", StateCompatibilityKey: "live-body:v1"}
}
func (*liveBodyGateway) Capabilities() provider.Capabilities {
	return provider.Capabilities{Reasoning: provider.ReasoningUnsupported}
}
func (gateway *liveBodyGateway) Stream(ctx context.Context, _ provider.Request) (<-chan provider.Event, error) {
	events := make(chan provider.Event)
	go func() {
		defer close(events)
		for range gateway.count {
			select {
			case <-ctx.Done():
				return
			case events <- provider.Event{Type: provider.EventDelta, Text: gateway.unit}:
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-gateway.release:
		}
		select {
		case <-ctx.Done():
			return
		case events <- provider.Event{Type: provider.EventDone, Reason: "stop"}:
		}
	}()
	return events, nil
}

func TestThreadLiveBodySQLiteGiantOutputCommitCancelAndRestart(t *testing.T) {
	for _, cancelRun := range []bool{false, true} {
		t.Run(map[bool]string{false: "complete", true: "cancel"}[cancelRun], func(t *testing.T) {
			gateway := &liveBodyGateway{unit: strings.Repeat("中é🌸\r\n", 1024), count: 4096, release: make(chan struct{})}
			if cancelRun {
				gateway.count = 512
			}
			expected := strings.Repeat(gateway.unit, gateway.count)
			hash := sha256.Sum256([]byte(expected))
			path := t.TempDir() + "/live.sqlite"
			open := func() (*Host, *threadRuntimeService) {
				host, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
				agent, err := testAgent(gateway)
				if err != nil {
					t.Fatal(err)
				}
				public, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
				if err != nil {
					t.Fatal(err)
				}
				return host, public.(*threadRuntimeService)
			}
			host, service := open()
			created, err := service.CreateAck(t.Context(), CreateThreadInput{RequestKey: "create"})
			if err != nil {
				t.Fatal(err)
			}
			admitted, err := service.SendAck(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "send", Input: UserInput{Text: "generate"}})
			if err != nil {
				t.Fatal(err)
			}
			actor := service.runtime(created.ThreadID)
			var live ThreadItem
			deadline := time.NewTimer(120 * time.Second)
			defer deadline.Stop()
			for live.textBytes != len(expected) {
				err := actor.apply(t.Context(), func() error {
					if actor.state.contextErr != nil {
						return actor.state.contextErr
					}
					for _, item := range actor.state.view.Items {
						if item.Kind == ThreadItemAssistant {
							live = item
						}
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
				if live.textBytes == len(expected) {
					break
				}
				select {
				case <-deadline.C:
					t.Fatalf("giant live output unavailable: bytes=%d expected=%d", live.textBytes, len(expected))
				case <-time.After(time.Millisecond):
				}
			}
			if live.liveBody == nil || live.Text != "" || !live.textOmitted || actor.textBuffer.Len() != 0 {
				t.Fatal("actor duplicated live source")
			}
			frozen := cloneThreadRuntimeView(actor.state.view)
			scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{created.ThreadID}}
			sub, err := service.SubscribeWindowUpdates(t.Context(), scope)
			if err != nil {
				t.Fatal(err)
			}
			defer sub.Close()
			if err := sub.SetWindow(t.Context(), scope, ThreadWindowInput{ThreadID: created.ThreadID}); err != nil {
				t.Fatal(err)
			}
			baseline := nextUpdate(t, sub)
			var preview ThreadItem
			for _, item := range baseline.Window.Value.Items {
				if item.ID == live.ID {
					preview = item
				}
			}
			if len(preview.Text) > threadDisplayTextBytes || preview.Body == nil || preview.Body.Bytes != len(expected) || preview.Body.RunID != admitted.RunID || preview.Body.SourceID != live.ID || !strings.HasPrefix(expected, preview.Text) || preview.liveBody != nil {
				t.Fatal("bounded live preview or locator differs")
			}
			input := ThreadLiveBodyInput{ThreadID: created.ThreadID, RunID: admitted.RunID, SourceID: live.ID, ThroughBytes: len(expected)}
			readHash := sha256.New()
			for input.OffsetBytes < input.ThroughBytes {
				part, err := service.ReadLiveBody(t.Context(), scope, input)
				if err != nil || part.Text == "" {
					t.Fatal("live read", err)
				}
				readHash.Write([]byte(part.Text))
				input.OffsetBytes += len(part.Text)
			}
			if string(readHash.Sum(nil)) != string(hash[:]) {
				t.Fatal("live hash differs")
			}
			input.OffsetBytes = 0
			if _, err := service.ReadLiveBody(t.Context(), threadquery.Scope{Authority: "revoked"}, input); !errors.Is(err, threadquery.ErrRecordUnavailable) {
				t.Fatal("revoked read accepted", err)
			}
			wrong := input
			wrong.RunID = "other"
			if _, err := service.ReadLiveBody(t.Context(), scope, wrong); !errors.Is(err, threadquery.ErrRecordUnavailable) {
				t.Fatal("wrong run accepted", err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if _, err := service.ReadLiveBody(ctx, scope, input); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if cancelRun {
				if _, err := service.CancelAck(t.Context(), CancelInput{ThreadID: created.ThreadID, RequestKey: "cancel", Mode: CancelModeGraceful}); err != nil {
					t.Fatal(err)
				}
			} else {
				close(gateway.release)
			}
			// SQLite's race instrumentation scans every canonical byte. This is
			// a correctness deadline; production latency is qualified separately.
			settledCtx, settledCancel := context.WithTimeout(t.Context(), 5*time.Minute)
			defer settledCancel()
			for {
				settled := false
				err := actor.apply(settledCtx, func() error {
					settled = actor.state.view.Activity == ThreadActivityIdle && actor.state.view.LastOutcome != nil
					return actor.state.contextErr
				})
				if err != nil {
					t.Fatalf("settle canonical output: %v", err)
				}
				if settled {
					break
				}
				select {
				case <-settledCtx.Done():
					host.store.lifetimeMu.Lock()
					backgroundErr := host.store.backgroundErr
					host.store.lifetimeMu.Unlock()
					t.Fatalf("canonical installation did not settle: background error=%v", backgroundErr)
				case <-time.After(10 * time.Millisecond):
				}
			}
			completed, err := service.View(t.Context(), created.ThreadID)
			if err != nil {
				t.Fatal(err)
			}
			assert := func(view ThreadView) {
				t.Helper()
				for _, item := range view.Items {
					if item.Kind == ThreadItemAssistant {
						if sha256.Sum256([]byte(item.Text)) != hash {
							t.Fatal("canonical hash differs")
						}
						return
					}
				}
				t.Fatal("canonical output missing")
			}
			assert(completed)
			complete, err := service.completeViewResult(t.Context(), frozen, nil)
			if err != nil {
				t.Fatal("frozen live adapter lost after commit", err)
			}
			assert(complete)
			if _, err := service.ReadLiveBody(t.Context(), scope, input); !errors.Is(err, threadquery.ErrRecordUnavailable) {
				t.Fatal("installed source remained live", err)
			}
			if err := host.Shutdown(t.Context()); err != nil {
				t.Fatal(err)
			}
			_, service = open()
			restored, err := service.View(t.Context(), created.ThreadID)
			if err != nil {
				t.Fatal(err)
			}
			assert(restored)
		})
	}
}

func TestThreadLiveBodySanitizedStreamMatchesFullAdapter(t *testing.T) {
	service, actor, _, full := nativeUpdatesFixture(t)
	full.Close()
	service.runtimes = map[string]*threadRuntimeState{"thread-live": actor}
	owner := outputbody.NewOwner()
	defer owner.Close()
	body := owner.NewBody()
	sink := newRuntimeEventSink(combinedThreadEventSink{runtime: threadRuntimeEventSink{service: service}})
	var expected strings.Builder
	var reader outputbody.Reader
	for _, raw := range []string{"before 中🌸 ", "/private/secret.txt", " after"} {
		if err := body.Append(raw); err != nil {
			t.Fatal(err)
		}
		text := event.SafePathRefsText(raw)
		expected.WriteString(text)
		var err error
		reader, err = body.Present(raw, text)
		if err != nil {
			t.Fatal(err)
		}
		sink.EmitWithOutputBody(event.Event{Type: event.ProviderDelta, ThreadID: "thread-live", TurnID: "turn-live", RunID: "run-live", OutputSourceID: "source", Message: text}, reader, nil)
	}
	complete, err := service.completeViewResult(t.Context(), cloneThreadRuntimeView(actor.state.view), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(complete.Items) != 1 || complete.Items[0].Text != expected.String() || strings.Contains(complete.Items[0].Text, "/private/") {
		t.Fatal("display redaction changed")
	}
}

func TestThreadLiveBodyEncodedBudgetAndRetryRelease(t *testing.T) {
	service, actor, sink, full := nativeUpdatesFixture(t)
	full.Close()
	service.runtimes = map[string]*threadRuntimeState{"thread-live": actor}
	owner := outputbody.NewOwner()
	defer owner.Close()
	body := owner.NewBody()
	text := strings.Repeat("\x00", 16<<10)
	if err := body.Append(text); err != nil {
		t.Fatal(err)
	}
	sink.EmitEvent(Event{ThreadID: "thread-live", TurnID: "turn-live", RunID: "run-live", outputSourceID: "source", outputBody: body, Stream: &StreamObservation{Type: StreamObservationAssistantDelta, Text: text}})
	scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{"thread-live"}}
	input := ThreadLiveBodyInput{ThreadID: "thread-live", RunID: "run-live", SourceID: "source", ThroughBytes: len(text)}
	var out strings.Builder
	for input.OffsetBytes < len(text) {
		chunk, err := service.ReadLiveBody(t.Context(), scope, input)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(chunk)
		if err != nil || len(raw) > 64<<10 || chunk.Text == "" {
			t.Fatal("wire budget exceeded", len(raw), err)
		}
		out.WriteString(chunk.Text)
		input.OffsetBytes += len(chunk.Text)
	}
	if out.String() != text {
		t.Fatal("encoding budget skipped bytes")
	}
	if err := actor.apply(t.Context(), func() error { actor.discardAttemptDraft(); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(actor.state.view.Items) != 0 {
		t.Fatal("retry retained output")
	}
	if capacity := actor.state.view.Items[:cap(actor.state.view.Items)]; len(capacity) > 0 && capacity[0].liveBody != nil {
		t.Fatal("retry backing slice retained body owner")
	}
	input.OffsetBytes = 0
	if _, err := service.ReadLiveBody(t.Context(), scope, input); !errors.Is(err, threadquery.ErrRecordUnavailable) {
		t.Fatal("retry source remained readable", err)
	}
}

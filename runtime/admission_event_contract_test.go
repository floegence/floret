package runtime

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/floegence/floret/observation"
)

type admissionReadEventSink struct {
	mu       sync.Mutex
	readHost *ThreadReadHost
	events   []Event
	page     ThreadTurnsPage
	err      error
}

func (s *admissionReadEventSink) EmitEvent(event Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return
	}
	if err := event.Validate(); err != nil {
		s.err = fmt.Errorf("validate runtime event: %w", err)
		return
	}
	s.events = append(s.events, event)
	if event.Committed == nil || event.Committed.Kind != ThreadDetailEventUserMessage {
		return
	}
	if s.readHost == nil {
		s.err = fmt.Errorf("canonical read host is unavailable at user admission")
		return
	}
	page, err := s.readHost.ListThreadTurns(context.Background(), ListThreadTurnsRequest{
		ThreadID: event.ThreadID,
		Limit:    10,
	})
	if err != nil {
		s.err = fmt.Errorf("read canonical turn during admission event: %w", err)
		return
	}
	s.page = page
}

func (s *admissionReadEventSink) snapshot() ([]Event, ThreadTurnsPage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Event(nil), s.events...), s.page, s.err
}

func TestCanonicalUserAdmissionIsReadableBeforeProviderEvents(t *testing.T) {
	for _, test := range []struct {
		name  string
		store func(*testing.T) *Store
	}{
		{name: "memory", store: func(*testing.T) *Store { return NewMemoryStore() }},
		{name: "sqlite", store: func(t *testing.T) *Store {
			store, err := OpenSQLiteStore(filepath.Join(t.TempDir(), "floret.db"))
			if err != nil {
				t.Fatal(err)
			}
			return store
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store := test.store(t)
			attachment := MessageAttachment{
				ResourceRef: "upload:admission-asset",
				Name:        "admission.txt",
				MIMEType:    "text/plain",
				SizeBytes:   18,
			}
			t.Cleanup(func() {
				if err := store.Close(); err != nil {
					t.Errorf("close store: %v", err)
				}
			})
			sink := &admissionReadEventSink{}
			host, err := newTestHost(t, providerHostOptions{
				Config: runtimeGatewayConfig("admission event contract"),
				ModelGateway: runtimeModelGateway(func(context.Context, ModelRequest) (<-chan ModelEvent, error) {
					return runtimeGatewayEvents("done"), nil
				}),
				ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
				Store:                store,
				Sink:                 sink,
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := host.CreateThread(ctx, testCreateThreadRequest("thread")); err != nil {
				t.Fatal(err)
			}
			readHost, err := mustTestCapabilities(t, store).read.NewHost(ctx, "thread")
			if err != nil {
				t.Fatal(err)
			}
			sink.readHost = readHost

			if _, err := host.RunTurn(ctx, RunTurnRequest{
				ThreadID: "thread",
				TurnID:   "turn-1",
				RunID:    "run-1",
				Input:    TurnInput{Text: "hello", Attachments: []MessageAttachment{attachment}},
			}); err != nil {
				t.Fatal(err)
			}

			events, page, sinkErr := sink.snapshot()
			if sinkErr != nil {
				t.Fatal(sinkErr)
			}
			if len(page.Turns) != 1 {
				t.Fatalf("turn page at admission = %#v", page)
			}
			turn := page.Turns[0]
			if turn.TurnID != "turn-1" || turn.RunID != "run-1" || turn.UserEntryID == "" || turn.UserInput != "hello" ||
				!reflect.DeepEqual(turn.UserAttachments, []MessageAttachment{attachment}) || turn.Status != TurnStatusRunning {
				t.Fatalf("canonical running turn at admission = %#v", turn)
			}
			admissionIndex := -1
			providerIndex := -1
			for index, event := range events {
				if event.Committed != nil && event.Committed.Kind == ThreadDetailEventUserMessage {
					admissionIndex = index
				}
				if providerIndex < 0 && event.Type == observation.EventTypeProviderRequest {
					providerIndex = index
				}
			}
			if admissionIndex < 0 || providerIndex < 0 || admissionIndex >= providerIndex {
				t.Fatalf("event order admission=%d provider=%d events=%#v", admissionIndex, providerIndex, events)
			}
		})
	}
}

package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/threadquery"
)

func TestThreadItemBodyIndependentOfWindowPreviewAndRestart(t *testing.T) {
	path := t.TempDir() + "/locator.sqlite"
	open := func() (*Host, *threadRuntimeService) {
		host, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
		public, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return nil, errors.New("execution not expected") }))
		if err != nil {
			t.Fatal(err)
		}
		return host, public.(*threadRuntimeService)
	}
	host, service := open()
	id := identity.ThreadID("thread-locator")
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
		t.Fatal(err)
	}
	for index := range 149 {
		_, err := host.store.repo.Append(t.Context(), sessiontree.Entry{ThreadID: id.String(), TurnID: fmt.Sprintf("turn-%d", index), RunID: fmt.Sprintf("run-%d", index), Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: "request"}}, sessiontree.AppendOptions{ID: fmt.Sprintf("user-%d", index)})
		if err != nil {
			t.Fatal(err)
		}
	}
	text := strings.Repeat("中文🌸\r\n", 400)
	_, err := host.store.repo.Append(t.Context(), sessiontree.Entry{ThreadID: id.String(), TurnID: "turn-148", RunID: "run-148", Type: sessiontree.EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: text, ContentSourceID: "answer"}}, sessiontree.AppendOptions{ID: "answer-entry"})
	if err != nil {
		t.Fatal(err)
	}
	scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{id}}
	for restart := range 2 {
		sub, err := service.SubscribeWindowUpdates(t.Context(), scope)
		if err != nil {
			t.Fatal(err)
		}
		if err := sub.SetWindow(t.Context(), scope, ThreadWindowInput{ThreadID: id}); err != nil {
			t.Fatal(err)
		}
		readWindow := func() *ThreadWindowSnapshot {
			for {
				update := nextUpdate(t, sub)
				if update.Window != nil {
					return update.Window.Value
				}
			}
		}
		page := readWindow()
		partial := page.Items[len(page.Items)-1]
		if partial.Body == nil || partial.Body.PreviewBytes >= partial.Body.Bytes {
			t.Fatal("fixture must require a page-limited preview")
		}
		if err := sub.SetWindow(t.Context(), scope, ThreadWindowInput{ThreadID: id, AnchorID: "answer", Direction: ThreadWindowAround, Limit: 1}); err != nil {
			t.Fatal(err)
		}
		complete := readWindow().Items[0]
		if complete.Body != nil || complete.Text != text {
			t.Fatal("single-item fixture must be complete")
		}
		sub.Close()
		body, err := service.ItemBody(t.Context(), scope, ThreadItemBodyInput{ThreadID: id, ItemID: "answer"})
		if err != nil {
			t.Fatal(err)
		}
		if body.SourceID != "answer" || body.RunID != "" || body.Field != ThreadBodyContent || body.Bytes != len(text) || body.PreviewBytes != 0 {
			t.Fatalf("locator: %+v", body)
		}
		var ranged strings.Builder
		for ranged.Len() < len(text) {
			chunk, err := service.ReadItemText(t.Context(), scope, ThreadItemTextInput{ThreadID: id, ItemID: "answer", SourceID: body.SourceID, Field: body.Field, ThroughBytes: body.Bytes, OffsetBytes: ranged.Len(), LimitBytes: 1024})
			if err != nil || chunk.OffsetBytes != ranged.Len() || len(chunk.Text) > 1024 || chunk.Bytes != len(text) {
				t.Fatalf("range: %+v %v", chunk, err)
			}
			ranged.WriteString(chunk.Text)
		}
		if ranged.String() != text {
			t.Fatal("ranged presentation differs")
		}
		if _, err := service.ReadItemText(t.Context(), scope, ThreadItemTextInput{ThreadID: id, ItemID: "answer", SourceID: "wrong", Field: body.Field}); !errors.Is(err, threadquery.ErrRecordUnavailable) {
			t.Fatalf("wrong source accepted: %v", err)
		}
		if _, err := service.ReadItemText(t.Context(), threadquery.Scope{Authority: "revoked"}, ThreadItemTextInput{ThreadID: id, ItemID: "answer", SourceID: body.SourceID, Field: body.Field}); !errors.Is(err, threadquery.ErrRecordUnavailable) {
			t.Fatalf("revoked text: %v", err)
		}
		manifest, err := service.BodyManifest(t.Context(), scope, ThreadBodyManifestInput{ThreadID: id, SourceID: body.SourceID, Field: body.Field})
		if err != nil || len(manifest.Parts) != 1 {
			t.Fatalf("manifest: %+v %v", manifest, err)
		}
		part := manifest.Parts[0]
		chunk, err := service.ReadBody(t.Context(), scope, ThreadBodyInput{ThreadID: id, EntryID: part.EntryID, Field: part.Field, SHA256: part.SHA256})
		if err != nil || chunk.Text != text {
			t.Fatalf("canonical source differs: %v", err)
		}
		if _, err := service.ItemBody(t.Context(), threadquery.Scope{Authority: "revoked"}, ThreadItemBodyInput{ThreadID: id, ItemID: "answer"}); !errors.Is(err, threadquery.ErrRecordUnavailable) {
			t.Fatalf("revoked: %v", err)
		}
		if _, err := service.ItemBody(t.Context(), scope, ThreadItemBodyInput{ThreadID: id, ItemID: "missing"}); !errors.Is(err, threadquery.ErrRecordUnavailable) {
			t.Fatalf("missing: %v", err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := service.ItemBody(ctx, scope, ThreadItemBodyInput{ThreadID: id, ItemID: "answer"}); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled: %v", err)
		}
		if restart == 0 {
			if err := host.Shutdown(t.Context()); err != nil {
				t.Fatal(err)
			}
			host, service = open()
		}
	}
}

func TestThreadItemBodyResolvesSmallLiveSource(t *testing.T) {
	gateway := &liveBodyGateway{unit: "small 中文🌸\r\n", count: 1, release: make(chan struct{})}
	host, err := Open(t.Context(), Options{Storage: storage.Memory()})
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
	service := public.(*threadRuntimeService)
	created, err := service.CreateAck(t.Context(), CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := service.SendAck(t.Context(), SendInput{ThreadID: created.ThreadID, RequestKey: "send", Input: UserInput{Text: "generate"}})
	if err != nil {
		t.Fatal(err)
	}
	scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{created.ThreadID}}
	var source string
	deadline := time.Now().Add(5 * time.Second)
	for source == "" && time.Now().Before(deadline) {
		view, err := service.View(t.Context(), created.ThreadID)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range view.Items {
			if item.Kind == ThreadItemAssistant && item.Text == gateway.unit {
				source = item.ID
			}
		}
		if source == "" {
			time.Sleep(time.Millisecond)
		}
	}
	if source == "" {
		t.Fatal("live source unavailable")
	}
	body, err := service.ItemBody(t.Context(), scope, ThreadItemBodyInput{ThreadID: created.ThreadID, ItemID: source})
	if err != nil || body.RunID != admitted.RunID || body.SourceID != source || body.Bytes != len(gateway.unit) {
		t.Fatalf("live locator: %+v %v", body, err)
	}
	part, err := service.ReadLiveBody(t.Context(), scope, ThreadLiveBodyInput{ThreadID: created.ThreadID, RunID: body.RunID, SourceID: body.SourceID, ThroughBytes: body.Bytes})
	if err != nil || part.Text != gateway.unit {
		t.Fatalf("live text: %+v %v", part, err)
	}
	chunk, err := service.ReadItemText(t.Context(), scope, ThreadItemTextInput{ThreadID: created.ThreadID, ItemID: source, RunID: body.RunID, SourceID: body.SourceID, Field: body.Field, ThroughBytes: body.Bytes})
	if err != nil || chunk.Text != gateway.unit {
		t.Fatalf("live item text: %+v %v", chunk, err)
	}
	close(gateway.release)
}

func TestThreadItemTextWireBudgetAndInvalidOffsets(t *testing.T) {
	host, err := Open(t.Context(), Options{Storage: storage.Memory()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	public, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return nil, errors.New("execution not expected") }))
	if err != nil {
		t.Fatal(err)
	}
	service := public.(*threadRuntimeService)
	id := identity.ThreadID("escaped")
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id.String()}); err != nil {
		t.Fatal(err)
	}
	text := "🌸" + strings.Repeat("<>&\x00", 12000)
	_, err = host.store.repo.Append(t.Context(), sessiontree.Entry{ThreadID: id.String(), TurnID: "turn", RunID: "run", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: text}}, sessiontree.AppendOptions{ID: "input"})
	if err != nil {
		t.Fatal(err)
	}
	scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{id}}
	input := ThreadItemTextInput{ThreadID: id, ItemID: "input", EntryID: "input", Field: ThreadBodyContent, ThroughBytes: len(text)}
	var got strings.Builder
	for got.Len() < len(text) {
		input.OffsetBytes = got.Len()
		chunk, err := service.ReadItemText(t.Context(), scope, input)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(chunk)
		if err != nil || len(raw) > 64<<10 || len(chunk.Text) > 16<<10 || chunk.Text == "" {
			t.Fatalf("invalid chunk: bytes=%d err=%v", len(raw), err)
		}
		got.WriteString(chunk.Text)
	}
	if got.String() != text {
		t.Fatal("text differs")
	}
	for _, offset := range []int{-1, 1, len(text) + 1} {
		input.OffsetBytes = offset
		if _, err := service.ReadItemText(t.Context(), scope, input); err == nil {
			t.Fatalf("invalid offset accepted: %d", offset)
		}
	}
	input.OffsetBytes = 0
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := service.ReadItemText(canceled, scope, input); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled read: %v", err)
	}
}

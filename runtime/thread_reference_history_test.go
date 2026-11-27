package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/threadquery"
)

func TestReferenceHistoryForkSeparatesModelEvidenceFromDirectConversation(t *testing.T) {
	answer := func(text string) florettest.Step {
		return florettest.Step{Events: []provider.Event{{Type: provider.EventDelta, Text: text}, {Type: provider.EventDone, Reason: "stop"}}}
	}
	ctx := t.Context()
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "reference", StateCompatibilityKey: "test:reference"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, answer("recorded-A"), answer("recorded-B"), answer("recorded-C"))
	agent, err := testAgent(gateway, WithAgentGoals())
	if err != nil {
		t.Fatal(err)
	}
	db := filepath.Join(t.TempDir(), "history.sqlite")
	open := func() (*Host, ThreadService) {
		t.Helper()
		host, err := Open(ctx, Options{Storage: storage.SQLite(db)})
		if err != nil {
			t.Fatal(err)
		}
		svc, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
		if err != nil {
			t.Fatal(err)
		}
		return host, svc
	}
	host, svc := open()
	t.Cleanup(func() {
		if err := host.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	p, err := svc.Create(ctx, CreateThreadInput{RequestKey: "root"})
	if err != nil {
		t.Fatal(err)
	}
	send := func(id identity.ThreadID, text string, attachments ...MessageAttachment) {
		t.Helper()
		_, err := svc.Send(ctx, SendInput{ThreadID: id, Input: UserInput{Text: text, Attachments: attachments}, RequestKey: RequestKey(text)})
		if err != nil {
			t.Fatal(err)
		}
		v := waitThreadView(t, svc, id, func(v ThreadView) bool { return v.Activity == ThreadActivityIdle && v.LastOutcome != nil })
		if v.Failure != nil {
			t.Fatal(v.Failure)
		}
	}
	send(p.ThreadID, "material-A", MessageAttachment{ResourceRef: "opaque:original-image", Name: "sample.png", MIMEType: "image/png", SizeBytes: 700})
	goals := svc.(ThreadGoalController)
	if _, err := goals.SetGoal(ctx, SetGoalInput{ThreadID: p.ThreadID, RequestKey: "set-parent-goal", Text: "Private parent objective"}); err != nil {
		t.Fatal(err)
	}
	todos := host.store.repo.(sessiontree.AgentTodoStateRepo)
	if _, err := todos.CompareAndSwapAgentTodoState(ctx, sessiontree.AgentTodoState{ThreadID: p.ThreadID.String(), Items: []sessiontree.AgentTodoItem{{ID: "only-parent", Content: "Parent task", Status: sessiontree.AgentTodoPending}}}, 0); err != nil {
		t.Fatal(err)
	}
	req := ForkThreadInput{SourceThreadID: p.ThreadID, RequestKey: "reference", ReferenceHistory: true}
	child, err := svc.Fork(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if len(child.Turns) != 0 || len(child.Items) != 0 || len(child.Interactions) != 0 || len(child.Queue) != 0 || child.Activity != ThreadActivityIdle {
		t.Fatalf("inherited active conversation: %+v", child)
	}
	if child.Goal != nil && child.Goal.Status != "absent" {
		t.Fatalf("inherited goal: %+v", child.Goal)
	}
	if childTodo, err := todos.ReadAgentTodoState(ctx, child.ThreadID.String()); err != nil || len(childTodo.Items) != 0 {
		t.Fatalf("inherited todos: %+v, %v", childTodo, err)
	}
	if _, err := goals.DeleteGoal(ctx, DeleteGoalInput{ThreadID: p.ThreadID, RequestKey: "clear-parent", ExpectedVersion: 1}); err != nil {
		t.Fatal(err)
	}
	send(p.ThreadID, "material-B")
	if err := host.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	host, svc = open()
	origin, found, err := svc.(ThreadForkOriginReader).ReadForkOrigin(ctx, child.ThreadID)
	if err != nil || !found || !origin.ReferenceHistory || origin.SourceThreadID != p.ThreadID {
		t.Fatalf("origin after restart: %+v %t %v", origin, found, err)
	}
	again, err := svc.Fork(ctx, req)
	if err != nil || again.ThreadID != child.ThreadID {
		t.Fatalf("replayed=%q err=%v", again.ThreadID, err)
	}
	other := req
	other.ReferenceHistory = false
	if _, err := svc.Fork(ctx, other); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("fork mode conflict: %v", err)
	}
	send(child.ThreadID, "material-C")
	requests := gateway.Requests()
	if len(requests) != 3 {
		t.Fatalf("requests = %d", len(requests))
	}
	data, err := json.Marshal(requests[2].Messages)
	if err != nil {
		t.Fatal(err)
	}
	model := string(data)
	attachmentCount := 0
	for _, msg := range requests[2].Messages {
		if strings.Contains(msg.Text, "opaque:original-image") {
			t.Fatal("host resource handle entered model text")
		}
		for _, a := range msg.Attachments {
			if a.ResourceRef == "opaque:original-image" {
				attachmentCount++
			}
		}
	}
	if attachmentCount != 1 {
		t.Fatalf("reference attachment count = %d", attachmentCount)
	}
	for _, want := range []string{"Reference history", "material-A", "recorded-A", "material-C"} {
		if !strings.Contains(model, want) {
			t.Fatalf("model missing %q: %s", want, model)
		}
	}
	for _, private := range []string{"material-B", "recorded-B", "Private parent objective", "Parent task"} {
		if strings.Contains(model, private) {
			t.Fatalf("parent-only fact %q entered child request", private)
		}
	}
	page, err := svc.History(ctx, child.ThreadID, "", 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Turns) != 1 {
		t.Fatalf("direct turns: %+v", page.Turns)
	}
	data, err = json.Marshal(page.Items)
	if err != nil {
		t.Fatal(err)
	}
	visible := string(data)
	if !strings.Contains(visible, "material-C") || strings.Contains(visible, "material-A") || strings.Contains(visible, "material-B") {
		t.Fatalf("direct history: %s", visible)
	}
	query := svc.(ThreadQueryReader)
	for _, check := range []struct {
		id   identity.ThreadID
		text string
		want int
	}{{child.ThreadID, "material-A", 0}, {child.ThreadID, "material-C", 1}, {p.ThreadID, "material-C", 0}, {p.ThreadID, "material-A", 1}} {
		result, err := query.Query(ctx, threadquery.Scope{Authority: "test", ThreadIDs: []identity.ThreadID{check.id}}, threadquery.QueryInput{Object: threadquery.Messages, Filter: threadquery.Filter{Text: threadquery.TextMatch{Contains: check.text}}, Limit: 1})
		if err != nil || result.Total != check.want || len(result.Records) != check.want {
			t.Fatalf("query %s %s: %+v %v", check.id, check.text, result, err)
		}
	}
	if err := host.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	host, svc = open()
	admitted, found, err := svc.(ThreadSendReader).LookupSend(ctx, LookupSendInput{ThreadID: child.ThreadID, RequestKey: "material-C"})
	refs, refErr := svc.(ThreadReferenceAttachmentReader).ReadReferenceAttachments(ctx, child.ThreadID)
	if refErr != nil || len(refs) != 1 || refs[0].ResourceRef != "opaque:original-image" {
		t.Fatalf("archived attachments: %+v %v", refs, refErr)
	}
	if direct, err := svc.(ThreadReferenceAttachmentReader).ReadReferenceAttachments(ctx, p.ThreadID); err != nil || len(direct) != 0 {
		t.Fatalf("direct inputs are not archived attachments: %+v %v", direct, err)
	}
	if err != nil || !found || admitted.Text != "material-C" {
		t.Fatalf("uncertain send lookup: %+v %t %v", admitted, found, err)
	}
	if _, found, err := svc.(ThreadSendReader).LookupSend(ctx, LookupSendInput{ThreadID: child.ThreadID, RequestKey: "material-A"}); err != nil || found {
		t.Fatalf("parent admission recovered as child send: %t %v", found, err)
	}
	scope := threadquery.Scope{Authority: "test", ThreadIDs: []identity.ThreadID{child.ThreadID}}
	sub, err := svc.(ThreadWindowSource).SubscribeWindowUpdates(ctx, scope)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	windowCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := sub.SetWindow(windowCtx, scope, ThreadWindowInput{ThreadID: child.ThreadID, Limit: 20, IncludeSummary: true, PreviewBytes: 1024}); err != nil {
		t.Fatal(err)
	}
	for {
		update, err := sub.Next(windowCtx)
		if err != nil {
			t.Fatal(err)
		}
		if update.Window != nil && update.Window.Value != nil {
			w := update.Window.Value
			raw, err := json.Marshal(w)
			if err != nil {
				t.Fatal(err)
			}
			body := string(raw)
			if w.HasBefore || w.HasAfter || !strings.Contains(body, "material-C") || strings.Contains(body, "material-A") || strings.Contains(body, "recorded-A") || strings.Contains(body, "original-image") {
				t.Fatalf("unexpected direct window: %s", body)
			}
			break
		}
	}
}

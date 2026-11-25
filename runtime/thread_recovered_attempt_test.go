package runtime

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/floegence/floret/v7/florettest"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/provider"
	"github.com/floegence/floret/v7/storage"
)

func TestRestartDispatchAfterMaximumRecordedAttempt(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	release := make(chan struct{})
	step := compactionTestStep("recovered answer")
	step.BlockUntil = release
	gateway := florettest.NewScriptedGateway(provider.Identity{Provider: "test", Model: "scripted", StateCompatibilityKey: "test:scripted"}, provider.Capabilities{Reasoning: provider.ReasoningUnsupported}, compactionTestStep("seed"), step, compactionTestStep("fresh"))
	path := filepath.Join(t.TempDir(), "recover.sqlite")
	open := func() *Host {
		host, err := Open(ctx, Options{Storage: storage.SQLite(path)})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
		return host
	}
	connect := func(host *Host) ThreadService {
		service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return testAgent(gateway) }))
		if err != nil {
			t.Fatal(err)
		}
		return service
	}
	host := open()
	service := connect(host)
	created, err := service.Create(ctx, CreateThreadInput{RequestKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Send(ctx, SendInput{ThreadID: created.ThreadID, RequestKey: "seed", Input: UserInput{Text: "seed"}}); err != nil {
		t.Fatal(err)
	}
	waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool { return view.LastOutcome != nil })
	if err := host.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	host = open()
	scope := created.ThreadID.String()
	_, err = host.store.repo.(sessiontree.RuntimeTurnRepo).AcceptTurn(ctx, sessiontree.AcceptTurnRequest{ThreadID: scope, TurnID: "interrupted", RunID: "same-run", LogicalRequestID: "work", RequestFingerprint: "work", InputRequestFingerprint: "input", Input: session.Message{Role: session.User, Content: "continue work"}})
	if err != nil {
		t.Fatal(err)
	}
	requests, err := host.store.prompt.ProviderRequests(ctx, scope)
	if err != nil || len(requests) != 1 {
		t.Fatalf("seed requests: %d %v", len(requests), err)
	}
	for _, epoch := range []int{1, 10, 1, 91} {
		record := requests[0]
		record.RunID, record.TurnID, record.LogicalRequestID = "same-run", "interrupted", "work"
		if epoch == 91 {
			record.RunID = "another-run"
		}
		record.AttemptEpoch, record.AttemptID = epoch, fmt.Sprintf("work:attempt:%d", epoch)
		record.ID = record.RunID + ":req:1"
		if err := host.store.prompt.AppendProviderRequest(ctx, record); err != nil {
			t.Fatal(err)
		}
	}
	if err := host.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	service = connect(open())
	if _, err := service.View(ctx, created.ThreadID); err != nil {
		t.Fatal(err)
	}
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	if err := gateway.WaitForRequests(ctx, 2); err != nil {
		t.Fatal(err)
	}
	actual := gateway.Requests()[1]
	if actual.AttemptEpoch != 11 || actual.AttemptID != "work:attempt:11" {
		t.Fatalf("recovered attempt = %s, epoch %d", actual.AttemptID, actual.AttemptEpoch)
	}
	waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool {
		return view.RunProgress != nil && view.RunProgress.Request != nil && view.RunProgress.Request.AttemptEpoch == 11
	})
	close(release)
	final := waitThreadView(t, service, created.ThreadID, func(view ThreadView) bool { return view.RunID == "same-run" && view.LastOutcome != nil })
	if final.Failure != nil {
		t.Fatal(final.Failure)
	}
	if _, err := service.Send(ctx, SendInput{ThreadID: created.ThreadID, RequestKey: "new", Input: UserInput{Text: "new turn"}}); err != nil {
		t.Fatal(err)
	}
	if err := gateway.WaitForRequests(ctx, 3); err != nil {
		t.Fatal(err)
	}
	if fresh := gateway.Requests()[2]; fresh.AttemptEpoch != 1 {
		t.Fatalf("fresh attempt = %d", fresh.AttemptEpoch)
	}
}

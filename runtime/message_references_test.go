package runtime

import (
	"context"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floegence/floret/config"
	"github.com/floegence/floret/internal/sessiontree"
)

func canonicalReferenceFixture() []MessageReference {
	return []MessageReference{
		{
			ReferenceID: "context:turn-1:0",
			Kind:        MessageReferenceText,
			Label:       "Selected output",
			Text:        "service ready",
		},
		{
			ReferenceID: "context:turn-1:1",
			Kind:        MessageReferenceResource,
			Label:       "config.yaml",
			Text:        "/workspace/config.yaml",
			ResourceRef: "host-resource:v1:config",
		},
	}
}

func renderableSupplementalFixture() []TurnSupplementalContextItem {
	return []TurnSupplementalContextItem{{
		Kind:  "linked_context",
		Title: "Selected output",
		Text:  "service ready",
		Metadata: map[string]string{
			"path": "/workspace/config.yaml",
		},
	}}
}

func TestTurnInputValidatesClosedMessageReferenceUnionAndLimits(t *testing.T) {
	valid := canonicalReferenceFixture()
	if err := (TurnInput{References: valid}).Validate(); err != nil {
		t.Fatalf("valid references rejected: %v", err)
	}

	cases := []struct {
		name string
		refs []MessageReference
	}{
		{name: "invalid kind", refs: []MessageReference{{ReferenceID: "ref", Kind: "other", Label: "label", Text: "text"}}},
		{name: "blank id", refs: []MessageReference{{Kind: MessageReferenceText, Label: "label", Text: "text"}}},
		{name: "unstable id", refs: []MessageReference{{ReferenceID: " ref ", Kind: MessageReferenceText, Label: "label", Text: "text"}}},
		{name: "multiline id", refs: []MessageReference{{ReferenceID: "ref\nnext", Kind: MessageReferenceText, Label: "label", Text: "text"}}},
		{name: "duplicate id", refs: []MessageReference{
			{ReferenceID: "ref", Kind: MessageReferenceText, Label: "one", Text: "one"},
			{ReferenceID: "ref", Kind: MessageReferenceText, Label: "two", Text: "two"},
		}},
		{name: "blank label", refs: []MessageReference{{ReferenceID: "ref", Kind: MessageReferenceText, Text: "text"}}},
		{name: "text with resource", refs: []MessageReference{{ReferenceID: "ref", Kind: MessageReferenceText, Label: "label", Text: "text", ResourceRef: "resource"}}},
		{name: "resource without ref", refs: []MessageReference{{ReferenceID: "ref", Kind: MessageReferenceResource, Label: "label"}}},
		{name: "truncated without text", refs: []MessageReference{{ReferenceID: "ref", Kind: MessageReferenceResource, Label: "label", ResourceRef: "resource", Truncated: true}}},
		{name: "invalid utf8", refs: []MessageReference{{ReferenceID: "ref", Kind: MessageReferenceText, Label: "label", Text: string([]byte{0xff})}}},
		{name: "too many", refs: func() []MessageReference {
			out := make([]MessageReference, MaxMessageReferencesPerTurn+1)
			for index := range out {
				out[index] = MessageReference{ReferenceID: "ref-" + strings.Repeat("x", index%3) + string(rune('a'+index%26)), Kind: MessageReferenceText, Label: "label", Text: "text"}
			}
			return out
		}()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := (TurnInput{References: tc.refs}).Validate(); err == nil {
				t.Fatalf("references %#v succeeded", tc.refs)
			}
		})
	}

	duplicates := []MessageReference{
		{ReferenceID: "ref-1", Kind: MessageReferenceResource, Label: "first", ResourceRef: "same"},
		{ReferenceID: "ref-2", Kind: MessageReferenceResource, Label: "second", ResourceRef: "same"},
	}
	if err := (TurnInput{References: duplicates}).Validate(); err != nil {
		t.Fatalf("duplicate opaque resource refs should preserve user order: %v", err)
	}
}

func TestHostReferenceOnlyTurnUsesCurrentSupplementalWithoutHistoryLeak(t *testing.T) {
	ctx := context.Background()
	var mu sync.Mutex
	var requests []ModelRequest
	gateway := runtimeModelGateway(func(_ context.Context, req ModelRequest) (<-chan ModelEvent, error) {
		mu.Lock()
		requests = append(requests, req)
		index := len(requests)
		mu.Unlock()
		return runtimeGatewayEvents([]string{"first answer", "second answer"}[index-1]), nil
	})
	host, err := newTestHost(t, providerHostOptions{
		Config:               runtimeGatewayConfig("reference-only contract"),
		ModelGateway:         gateway,
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                NewMemoryStore(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	references := canonicalReferenceFixture()
	if _, err := host.RunTurn(ctx, RunTurnRequest{
		ThreadID: "thread", TurnID: "turn-1", RunID: "run-1",
		Input: TurnInput{References: references}, SupplementalContext: renderableSupplementalFixture(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.RunTurn(ctx, RunTurnRequest{
		ThreadID: "thread", TurnID: "turn-2", RunID: "run-2", Input: TurnInput{Text: "continue"},
	}); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(requests) != 2 {
		t.Fatalf("provider requests = %d, want 2", len(requests))
	}
	firstTexts := modelRequestTexts(requests[0])
	if slicesContainBlank(firstTexts) || !strings.Contains(strings.Join(firstTexts, "\n"), "Host-provided supplemental context") {
		t.Fatalf("reference-only provider request = %#v", requests[0].Messages)
	}
	secondText := strings.Join(modelRequestTexts(requests[1]), "\n")
	if strings.Contains(secondText, "Host-provided supplemental context") || strings.Contains(secondText, "service ready") || strings.Contains(secondText, "config.yaml") {
		t.Fatalf("later provider history leaked references or supplemental context: %#v", requests[1].Messages)
	}
	if !strings.Contains(secondText, "continue") {
		t.Fatalf("later provider request lost current input: %#v", requests[1].Messages)
	}
}

func TestHostReferenceOnlyTurnUsesSQLiteAdmissionAuthority(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLiteStore(filepath.Join(t.TempDir(), "reference-only.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	host, err := newTestHost(t, providerHostOptions{
		Config: runtimeGatewayConfig("reference-only sqlite"),
		ModelGateway: runtimeModelGateway(func(context.Context, ModelRequest) (<-chan ModelEvent, error) {
			return runtimeGatewayEvents("done"), nil
		}),
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                store,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	want := canonicalReferenceFixture()
	if _, err := host.RunTurn(ctx, RunTurnRequest{
		ThreadID: "thread", TurnID: "turn", RunID: "run",
		Input: TurnInput{References: want}, SupplementalContext: renderableSupplementalFixture(),
	}); err != nil {
		t.Fatal(err)
	}
	page, err := host.ListThreadTurns(ctx, ListThreadTurnsRequest{ThreadID: "thread", Tail: 1})
	if err != nil || len(page.Turns) != 1 || !reflect.DeepEqual(page.Turns[0].UserReferences, want) {
		t.Fatalf("sqlite reference-only page=%#v err=%v", page, err)
	}
}

func TestHostRejectsReferenceOnlyInvalidSupplementalBeforeAdmission(t *testing.T) {
	cases := []struct {
		name  string
		items []TurnSupplementalContextItem
	}{
		{name: "missing"},
		{name: "blank", items: []TurnSupplementalContextItem{{Kind: " ", Title: " ", Text: " ", Metadata: map[string]string{" ": " "}}}},
		{name: "flags only", items: []TurnSupplementalContextItem{{Sensitive: true, Truncated: true}}},
		{name: "invalid utf8", items: []TurnSupplementalContextItem{{Text: string([]byte{0xff})}}},
		{name: "too many", items: func() []TurnSupplementalContextItem {
			out := make([]TurnSupplementalContextItem, MaxTurnSupplementalContextItems+1)
			for index := range out {
				out[index] = TurnSupplementalContextItem{Kind: "context", Text: "value"}
			}
			return out
		}()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			host, err := newTestHost(t, providerHostOptions{Config: runtimeGatewayConfig("test"), ModelGateway: runtimeModelGateway(func(context.Context, ModelRequest) (<-chan ModelEvent, error) {
				return runtimeGatewayEvents("unexpected"), nil
			}), ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"), Store: NewMemoryStore()})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
				t.Fatal(err)
			}
			if _, err := host.RunTurn(ctx, RunTurnRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", Input: TurnInput{References: canonicalReferenceFixture()}, SupplementalContext: tc.items}); err == nil {
				t.Fatal("RunTurn succeeded")
			}
			overview, err := host.ReadThreadOverview(ctx, "thread")
			if err != nil {
				t.Fatal(err)
			}
			if overview.LatestTurn != nil || overview.Thread.ThroughOrdinal != 0 {
				t.Fatalf("invalid supplemental context mutated thread: %#v", overview)
			}
		})
	}
}

func TestHostRunTurnReplayStartsProviderOnce(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	gateway := runtimeModelGateway(func(ctx context.Context, _ ModelRequest) (<-chan ModelEvent, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return runtimeGatewayEvents("done"), nil
	})
	newHost := func() *testProviderFacade {
		host, err := newTestHost(t, providerHostOptions{Config: runtimeGatewayConfig("test"), ModelGateway: gateway, ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"), Store: store})
		if err != nil {
			t.Fatal(err)
		}
		return host
	}
	firstHost := newHost()
	if _, err := firstHost.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	request := RunTurnRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", Input: TurnInput{References: canonicalReferenceFixture()}, SupplementalContext: renderableSupplementalFixture()}
	firstDone := make(chan error, 1)
	go func() {
		_, err := firstHost.RunTurn(ctx, request)
		firstDone <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first provider did not start")
	}

	replayedRunning, err := newHost().RunTurn(ctx, request)
	if err != nil {
		t.Fatalf("running replay: %v", err)
	}
	if !replayedRunning.Replayed || replayedRunning.Status != TurnStatusRunning || calls.Load() != 1 {
		t.Fatalf("running replay=%#v provider calls=%d", replayedRunning, calls.Load())
	}
	close(release)
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("first turn did not finish")
	}

	retry := request
	retry.SupplementalContext = []TurnSupplementalContextItem{{Kind: "different", Text: "must be ignored"}}
	replayedTerminal, err := newHost().RunTurn(ctx, retry)
	if err != nil {
		t.Fatalf("terminal replay: %v", err)
	}
	if !replayedTerminal.Replayed || replayedTerminal.Status != TurnStatusCompleted || replayedTerminal.Output != "done" || calls.Load() != 1 {
		t.Fatalf("terminal replay=%#v provider calls=%d", replayedTerminal, calls.Load())
	}
}

func TestHostMessageReferencesSurviveSQLiteReopenAndFork(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "floret.db")
	store, err := OpenSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	host, err := newTestHost(t, providerHostOptions{
		Config: runtimeGatewayConfig("test"),
		ModelGateway: runtimeModelGateway(func(context.Context, ModelRequest) (<-chan ModelEvent, error) {
			return runtimeGatewayEvents("done"), nil
		}),
		ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
		Store:                store,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "source"}); err != nil {
		t.Fatal(err)
	}
	want := canonicalReferenceFixture()
	if _, err := host.RunTurn(ctx, RunTurnRequest{ThreadID: "source", TurnID: "turn", RunID: "run", Input: TurnInput{Text: "inspect", References: want}, SupplementalContext: renderableSupplementalFixture()}); err != nil {
		t.Fatal(err)
	}
	entries, err := store.repo.Entries(ctx, "source")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(entries, func(entry sessiontree.Entry) bool {
		return entry.Type == sessiontree.EntryUserMessage && reflect.DeepEqual(entry.Message.References, sessionMessageReferences(want)) && entry.RawHash == sessiontree.StableHash(entry.Raw)
	}) {
		t.Fatalf("canonical entry missing references/raw hash: %#v", entries)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	maintenance, err := newTestMaintenanceHost(t, reopened)
	if err != nil {
		t.Fatal(err)
	}
	page, err := maintenance.ListThreadTurns(ctx, ListThreadTurnsRequest{ThreadID: "source", Tail: 1})
	if err != nil || len(page.Turns) != 1 || !reflect.DeepEqual(page.Turns[0].UserReferences, want) {
		t.Fatalf("reopened references page=%#v err=%v", page, err)
	}
	if _, err := maintenance.ForkThread(ctx, ForkThreadRequest{OperationID: "fork-op", SourceThreadID: "source", DestinationThreadID: "fork"}); err != nil {
		t.Fatal(err)
	}
	forked, err := maintenance.ListThreadTurns(ctx, ListThreadTurnsRequest{ThreadID: "fork", Tail: 1})
	if err != nil || len(forked.Turns) != 1 || !reflect.DeepEqual(forked.Turns[0].UserReferences, want) {
		t.Fatalf("forked references page=%#v err=%v", forked, err)
	}
}

func TestHostSubAgentReferencesProjectInDetailAndRejectReferenceOnly(t *testing.T) {
	ctx := context.Background()
	host, err := newTestHost(t, providerHostOptions{
		Config: config.Config{Provider: config.ProviderFake, Model: "fake-model", FakeResponse: "child done", SystemPrompt: "test"},
		Store:  NewMemoryStore(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "parent"}); err != nil {
		t.Fatal(err)
	}
	want := canonicalReferenceFixture()
	if _, err := host.SpawnSubAgent(ctx, SpawnSubAgentRequest{PublicationID: "publication", ParentThreadID: "parent", ThreadID: "child", TaskName: "worker", Message: "inspect", References: want, ForkMode: SubAgentForkNone}); err != nil {
		t.Fatal(err)
	}
	if waited, err := host.WaitSubAgents(ctx, WaitSubAgentsRequest{ParentThreadID: "parent", ChildThreadIDs: []ThreadID{"child"}, Timeout: time.Second}); err != nil || waited.TimedOut {
		t.Fatalf("waited=%#v err=%v", waited, err)
	}
	detail, err := host.ReadSubAgentDetail(ctx, ReadSubAgentDetailRequest{ParentThreadID: "parent", ChildThreadID: "child", IncludeRaw: true})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(detail.Events, func(event ThreadDetailEvent) bool {
		return event.Kind == ThreadDetailEventUserMessage && event.Message != nil && reflect.DeepEqual(event.Message.References, want)
	}) {
		t.Fatalf("subagent detail missing references: %#v", detail.Events)
	}
	if _, err := host.SpawnSubAgent(ctx, SpawnSubAgentRequest{PublicationID: "reference-only", ParentThreadID: "parent", ThreadID: "child-2", TaskName: "worker", References: want, ForkMode: SubAgentForkNone}); err == nil || !strings.Contains(err.Error(), "reference-only") {
		t.Fatalf("reference-only subagent spawn error=%v", err)
	}
	if _, err := host.CompletePendingTool(ctx, PendingToolCompletionRequest{
		CompletionRequestID: "completion", ContinuationTurnID: "next-turn", ContinuationRunID: "next-run", Status: PendingToolCompletionCompleted,
		Target: PendingToolSettlementTarget{ThreadID: "parent", TurnID: "turn", RunID: "run", ToolCallID: "call", ToolName: "tool", Handle: "handle"},
		Input:  TurnInput{References: want},
	}); err == nil || !strings.Contains(err.Error(), "reference-only") {
		t.Fatalf("reference-only pending completion error=%v", err)
	}
}

func modelRequestTexts(req ModelRequest) []string {
	out := make([]string, 0, len(req.Messages))
	for _, message := range req.Messages {
		if message.Role == ModelMessageRoleUser {
			out = append(out, message.Text)
		}
	}
	return out
}

func slicesContainBlank(values []string) bool {
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			return true
		}
	}
	return false
}

func TestMessageReferencesPreserveValueOrder(t *testing.T) {
	want := canonicalReferenceFixture()
	got := append([]MessageReference(nil), want...)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("references=%#v want=%#v", got, want)
	}
}

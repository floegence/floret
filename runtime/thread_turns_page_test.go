package runtime

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/floegence/floret/v2/internal/agentharness"
	"github.com/floegence/floret/v2/internal/session"
	"github.com/floegence/floret/v2/internal/sessiontree"
)

func TestDurableRetrySourceResetsNextProviderContextAcrossMemoryAndSQLite(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"memory", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			var store *Store
			var path string
			var err error
			if backend == "sqlite" {
				path = filepath.Join(t.TempDir(), "retry-context.db")
				store, err = openSQLiteStoreForTest(path)
			} else {
				store = newMemoryStore()
			}
			if err != nil {
				t.Fatal(err)
			}
			var requestsMu sync.Mutex
			var requests []ModelRequest
			gateway := runtimeModelGateway(func(_ context.Context, req ModelRequest) (<-chan ModelEvent, error) {
				req.Messages = append([]ModelMessage(nil), req.Messages...)
				requestsMu.Lock()
				requests = append(requests, req)
				call := len(requests)
				requestsMu.Unlock()
				switch call {
				case 1:
					return runtimeGatewayEvents("bootstrap answer"), nil
				case 2:
					return runtimeGatewayEvents("retry answer"), nil
				case 3:
					return runtimeGatewayEvents("follow-up answer"), nil
				default:
					return nil, fmt.Errorf("unexpected model request %d", call)
				}
			})
			newHost := func(current *Store) *testProviderFacade {
				host, hostErr := newTestHost(t, providerHostOptions{
					Config: runtimeGatewayConfig("durable retry context"), ModelGateway: gateway,
					ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"), Store: current, IDGenerator: deterministicIDs(),
				})
				if hostErr != nil {
					t.Fatal(hostErr)
				}
				return host
			}
			host := newHost(store)
			if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
				t.Fatal(err)
			}
			if result, err := host.RunTurn(ctx, RunTurnRequest{
				ThreadID: "thread", TurnID: "turn-bootstrap", RunID: "run-bootstrap", Input: TurnInput{Text: "bootstrap question"},
			}); err != nil || result.Status != TurnStatusCompleted {
				t.Fatalf("bootstrap=%#v err=%v", result, err)
			}

			authority := store.repo.(sessiontree.TurnAuthorityRepo)
			failed, err := authority.AdmitTurn(ctx, sessiontree.AdmitTurnRequest{
				ThreadID: "thread", TurnID: "turn-failed", RunID: "run-failed", OwnerID: "owner-failed",
				RequestFingerprint: "request-failed", Input: session.Message{Role: session.User, Content: "retry target question"},
				Now: time.Date(2026, 7, 21, 6, 0, 0, 0, time.UTC),
			})
			if err != nil {
				t.Fatal(err)
			}
			leaseCtx := sessiontree.ContextWithTurnLease(ctx, failed.Lease)
			if _, err := store.repo.Append(leaseCtx, sessiontree.Entry{
				ThreadID: "thread", TurnID: "turn-failed", Type: sessiontree.EntryAssistantMessage,
				Message: session.Message{Role: session.Assistant, Content: "FAILED_ASSISTANT"},
			}, sessiontree.AppendOptions{}); err != nil {
				t.Fatal(err)
			}
			if _, err := store.repo.Append(leaseCtx, sessiontree.Entry{
				ThreadID: "thread", TurnID: "turn-failed", Type: sessiontree.EntryToolCall,
				Message: session.Message{Role: session.Assistant, ToolCallID: "failed-call", ToolName: "failed_tool", ToolArgs: `{"marker":"FAILED_TOOL"}`},
			}, sessiontree.AppendOptions{}); err != nil {
				t.Fatal(err)
			}
			if _, err := authority.FinishTurn(ctx, sessiontree.FinishTurnRequest{
				Lease: failed.Lease, RunID: "run-failed", TerminalEntryID: "failed-terminal", Status: sessiontree.TurnFailed,
				Metadata:       map[string]string{sessiontree.TurnFailureCodeMetadataKey: sessiontree.TurnFailureProvider},
				FailureMessage: "provider failed", OutcomeFingerprint: "outcome-failed",
				Now: time.Date(2026, 7, 21, 6, 0, 1, 0, time.UTC),
			}); err != nil {
				t.Fatal(err)
			}
			if retried, err := host.RetryTurn(ctx, RetryTurnRequest{ThreadID: "thread", Reason: "retry failed branch"}); err != nil || retried.Status != TurnStatusCompleted {
				t.Fatalf("retry=%#v err=%v", retried, err)
			}

			if backend == "sqlite" {
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				store, err = openSQLiteStoreForTest(path)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.Close() })
				host = newHost(store)
			}
			if result, err := host.RunTurn(ctx, RunTurnRequest{
				ThreadID: "thread", TurnID: "turn-follow-up", RunID: "run-follow-up", Input: TurnInput{Text: "follow up"},
			}); err != nil || result.Status != TurnStatusCompleted {
				t.Fatalf("follow-up=%#v err=%v", result, err)
			}

			requestsMu.Lock()
			captured := append([]ModelRequest(nil), requests...)
			requestsMu.Unlock()
			if len(captured) != 3 {
				t.Fatalf("model requests=%#v", captured)
			}
			assertRetryResetModelContext(t, captured[2].Messages)
			entries, err := store.repo.Entries(ctx, "thread")
			if err != nil {
				t.Fatal(err)
			}
			if !physicalFailedBranchPresent(entries) {
				t.Fatalf("failed branch was not retained durably: %#v", entries)
			}
		})
	}
}

func assertRetryResetModelContext(t *testing.T, messages []ModelMessage) {
	t.Helper()
	var text strings.Builder
	for _, message := range messages {
		text.WriteString(message.Text)
		for _, call := range message.ToolCalls {
			text.WriteString(call.ID)
			text.WriteString(call.Name)
			text.WriteString(call.Args)
		}
	}
	contextText := text.String()
	for _, required := range []string{"bootstrap question", "bootstrap answer", "retry target question", "retry answer", "follow up"} {
		if !strings.Contains(contextText, required) {
			t.Fatalf("provider context is missing %q: %#v", required, messages)
		}
	}
	for _, forbidden := range []string{"FAILED_ASSISTANT", "failed-call", "failed_tool", "FAILED_TOOL"} {
		if strings.Contains(contextText, forbidden) {
			t.Fatalf("provider context leaked %q: %#v", forbidden, messages)
		}
	}
}

func physicalFailedBranchPresent(entries []sessiontree.Entry) bool {
	assistant := false
	toolCall := false
	for _, entry := range entries {
		assistant = assistant || entry.Message.Content == "FAILED_ASSISTANT"
		toolCall = toolCall || entry.Message.ToolCallID == "failed-call" || entry.Message.ToolName == "failed_tool"
	}
	return assistant && toolCall
}

func TestRetryTurnIsCanonicalWithoutDuplicatingUserAcrossMemoryAndSQLiteReopen(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"memory", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			var store *Store
			var path string
			var err error
			if backend == "sqlite" {
				path = filepath.Join(t.TempDir(), "retry-turn.db")
				store, err = openSQLiteStoreForTest(path)
			} else {
				store = newMemoryStore()
			}
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			host, err := newTestHost(t, providerHostOptions{
				Config: runtimeGatewayConfig("retry canonical turn"),
				ModelGateway: runtimeModelGateway(func(context.Context, ModelRequest) (<-chan ModelEvent, error) {
					calls++
					if calls == 1 {
						events := make(chan ModelEvent, 1)
						events <- ModelEvent{Type: ModelEventError, Err: errors.New("provider failed")}
						close(events)
						return events, nil
					}
					return runtimeGatewayEvents("retry answer"), nil
				}),
				ModelGatewayIdentity: runtimeGatewayIdentity("fake-model"),
				Store:                store,
				IDGenerator:          deterministicIDs(),
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := host.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
				t.Fatal(err)
			}
			failed, runErr := host.RunTurn(ctx, RunTurnRequest{
				ThreadID: "thread", TurnID: "turn-original", RunID: "run-original", Input: TurnInput{Text: "original question"},
			})
			if runErr == nil || failed.Status != TurnStatusFailed {
				t.Fatalf("failed turn=%#v err=%v", failed, runErr)
			}
			maintenance, err := newTestMaintenanceHost(t, store)
			if err != nil {
				t.Fatal(err)
			}
			beforeRetry, err := maintenance.ListThreadTurns(ctx, ListThreadTurnsRequest{ThreadID: "thread", Tail: 1})
			if err != nil || len(beforeRetry.Turns) != 1 || beforeRetry.Turns[0].UserEntryID == "" {
				t.Fatalf("before retry=%#v err=%v", beforeRetry, err)
			}
			retried, err := host.RetryTurn(ctx, RetryTurnRequest{ThreadID: "thread", Reason: "retry failed provider"})
			if err != nil || retried.Status != TurnStatusCompleted || retried.Output != "retry answer" {
				t.Fatalf("retried=%#v err=%v", retried, err)
			}
			assertRuntimeRetryTurnPage(t, ctx, maintenance, beforeRetry.SinceCursor)
			overview, err := maintenance.ReadThreadOverview(ctx, "thread")
			if err != nil || overview.LatestTurn == nil || overview.LatestTurn.RetrySource == nil || overview.LatestTurn.RetrySource.TurnID != "turn-original" {
				t.Fatalf("retry overview=%#v err=%v", overview, err)
			}
			forkRequest := ForkThreadRequest{OperationID: "fork-retry", SourceThreadID: "thread", DestinationThreadID: "fork"}
			if _, err := maintenance.ForkThread(ctx, forkRequest); err != nil {
				t.Fatal(err)
			}
			if replayed, err := maintenance.ForkThread(ctx, forkRequest); err != nil || replayed.Thread.ID != "fork" {
				t.Fatalf("fork replay=%#v err=%v", replayed, err)
			}
			forkCursor := assertRuntimeForkedRetryTurnPage(t, ctx, maintenance)
			if next, err := host.RunTurn(ctx, RunTurnRequest{
				ThreadID: "fork", TurnID: "fork-next", RunID: "fork-next-run", Input: TurnInput{Text: "continue after retry"},
			}); err != nil || next.Status != TurnStatusCompleted {
				t.Fatalf("fork next=%#v err=%v", next, err)
			}
			forkIncremental, err := maintenance.ListThreadTurns(ctx, ListThreadTurnsRequest{ThreadID: "fork", SinceCursor: &forkCursor, Limit: 1})
			if err != nil || len(forkIncremental.Turns) != 1 || forkIncremental.Turns[0].TurnID != "fork-next" {
				t.Fatalf("fork incremental=%#v err=%v", forkIncremental, err)
			}

			if backend == "sqlite" {
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				store, err = openSQLiteStoreForTest(path)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.Close() })
				maintenance, err = newTestMaintenanceHost(t, store)
				if err != nil {
					t.Fatal(err)
				}
				assertRuntimeRetryTurnPage(t, ctx, maintenance, beforeRetry.SinceCursor)
				forkPage, err := maintenance.ListThreadTurns(ctx, ListThreadTurnsRequest{ThreadID: "fork", Tail: 3})
				if err != nil || len(forkPage.Turns) != 3 || forkPage.Turns[1].RetrySource == nil || forkPage.Turns[1].RetrySource.TurnID != forkPage.Turns[0].TurnID {
					t.Fatalf("reopened fork retry page=%#v err=%v", forkPage, err)
				}
				for _, listed := range forkPage.Turns {
					assertExactThreadTurnMatchesListed(t, ctx, maintenance, "fork", listed)
				}
			}
		})
	}
}

func assertRuntimeForkedRetryTurnPage(t *testing.T, ctx context.Context, maintenance *testMaintenanceFacade) ThreadTurnCursor {
	t.Helper()
	page, err := maintenance.ListThreadTurns(ctx, ListThreadTurnsRequest{ThreadID: "fork", Tail: 2})
	if err != nil || len(page.Turns) != 2 {
		t.Fatalf("fork retry page=%#v err=%v", page, err)
	}
	original, retry := page.Turns[0], page.Turns[1]
	if retry.RetrySource == nil || retry.RetrySource.TurnID != original.TurnID || retry.UserEntryID != "" {
		t.Fatalf("fork retry relation original=%#v retry=%#v", original, retry)
	}
	for _, listed := range page.Turns {
		assertExactThreadTurnMatchesListed(t, ctx, maintenance, "fork", listed)
	}
	if original.TurnID == "turn-original" {
		t.Fatalf("fork did not rewrite original turn identity: %#v", original)
	}
	if _, err := maintenance.ReadThreadTurn(ctx, ReadThreadTurnRequest{ThreadID: "fork", TurnID: "turn-original"}); !errors.Is(err, ErrTurnNotFound) {
		t.Fatalf("fork accepted source turn identity err=%v", err)
	}
	if _, err := maintenance.ReadThreadTurn(ctx, ReadThreadTurnRequest{ThreadID: "thread", TurnID: original.TurnID}); !errors.Is(err, ErrTurnNotFound) {
		t.Fatalf("source accepted mapped fork turn identity err=%v", err)
	}
	latest, err := maintenance.ReadLatestThreadTurn(ctx, "fork")
	if err != nil || latest.TurnID != retry.TurnID || latest.RetrySource == nil || latest.RetrySource.TurnID != original.TurnID {
		t.Fatalf("fork latest retry=%#v err=%v", latest, err)
	}
	overview, err := maintenance.ReadThreadOverview(ctx, "fork")
	if err != nil || overview.LatestTurn == nil || overview.LatestTurn.RetrySource == nil || overview.LatestTurn.RetrySource.TurnID != original.TurnID {
		t.Fatalf("fork retry overview=%#v err=%v", overview, err)
	}
	tail, err := maintenance.ListThreadTurns(ctx, ListThreadTurnsRequest{ThreadID: "fork", Tail: 1})
	if err != nil || tail.BeforeCursor == nil {
		t.Fatalf("fork retry tail=%#v err=%v", tail, err)
	}
	before, err := maintenance.ListThreadTurns(ctx, ListThreadTurnsRequest{ThreadID: "fork", BeforeCursor: tail.BeforeCursor, Limit: 1})
	if err != nil || len(before.Turns) != 1 || before.Turns[0].TurnID != original.TurnID {
		t.Fatalf("fork retry before=%#v err=%v", before, err)
	}
	return page.SinceCursor
}

func assertRuntimeRetryTurnPage(t *testing.T, ctx context.Context, maintenance *testMaintenanceFacade, sinceCursor ThreadTurnCursor) {
	t.Helper()
	page, err := maintenance.ListThreadTurns(ctx, ListThreadTurnsRequest{ThreadID: "thread", Tail: 2})
	if err != nil || len(page.Turns) != 2 {
		t.Fatalf("retry page=%#v err=%v", page, err)
	}
	original, retry := page.Turns[0], page.Turns[1]
	if original.TurnID != "turn-original" || original.Status != TurnStatusFailed || original.UserInput != "original question" || original.RetrySource != nil {
		t.Fatalf("original turn=%#v", original)
	}
	if retry.RetrySource == nil || retry.RetrySource.TurnID != "turn-original" ||
		retry.UserEntryID != "" || retry.UserInput != "" || len(retry.UserAttachments) != 0 || len(retry.UserReferences) != 0 ||
		retry.Status != TurnStatusCompleted || len(retry.Projection.Segments) != 1 || retry.Projection.Segments[0].Text != "retry answer" {
		t.Fatalf("retry turn=%#v", retry)
	}
	for _, listed := range page.Turns {
		assertExactThreadTurnMatchesListed(t, ctx, maintenance, "thread", listed)
	}
	incremental, err := maintenance.ListThreadTurns(ctx, ListThreadTurnsRequest{ThreadID: "thread", SinceCursor: &sinceCursor, Limit: 1})
	if err != nil || len(incremental.Turns) != 1 || incremental.Turns[0].TurnID != retry.TurnID || incremental.SinceCursor != page.SinceCursor {
		t.Fatalf("retry incremental page=%#v err=%v", incremental, err)
	}
	latest, err := maintenance.ReadLatestThreadTurn(ctx, "thread")
	if err != nil || latest.TurnID != retry.TurnID || latest.RetrySource == nil || latest.RetrySource.TurnID != original.TurnID {
		t.Fatalf("retry latest=%#v err=%v", latest, err)
	}
}

func assertExactThreadTurnMatchesListed(t *testing.T, ctx context.Context, maintenance *testMaintenanceFacade, threadID ThreadID, listed ThreadTurnSnapshot) {
	t.Helper()
	exact, err := maintenance.ReadThreadTurn(ctx, ReadThreadTurnRequest{ThreadID: threadID, TurnID: listed.TurnID})
	if err != nil {
		t.Fatalf("ReadThreadTurn(%q, %q): %v", threadID, listed.TurnID, err)
	}
	listed.Projection.ProjectedAt = time.Time{}
	exact.Projection.ProjectedAt = time.Time{}
	if !reflect.DeepEqual(exact, listed) {
		t.Fatalf("exact/list turn mismatch for %q/%q\nexact=%#v\nlisted=%#v", threadID, listed.TurnID, exact, listed)
	}
}

func TestListThreadTurnsRejectsEmptySinceCursor(t *testing.T) {
	store := newMemoryStore()
	maintenance, err := newTestMaintenanceHost(t, store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := maintenance.CreateThread(context.Background(), CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	empty := ThreadTurnCursor("")
	_, err = maintenance.ListThreadTurns(context.Background(), ListThreadTurnsRequest{
		ThreadID: "thread", SinceCursor: &empty, Limit: 1,
	})
	if err == nil || !errors.Is(err, ErrInvalidThreadTurnCursor) {
		t.Fatalf("empty since cursor err=%v", err)
	}
}

func TestThreadTurnCursorIsOpaqueScopedAndTyped(t *testing.T) {
	ctx := context.Background()
	store := newMemoryStore()
	t.Cleanup(func() { _ = store.Close() })
	maintenance, err := newTestMaintenanceHost(t, store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := maintenance.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	since, err := encodeThreadTurnCursor("thread", threadTurnCursorModeSince, "missing-entry")
	if err != nil {
		t.Fatal(err)
	}
	before, err := encodeThreadTurnCursor("thread", threadTurnCursorModeBefore, "missing-entry")
	if err != nil {
		t.Fatal(err)
	}
	otherThread, err := encodeThreadTurnCursor("other", threadTurnCursorModeSince, "missing-entry")
	if err != nil {
		t.Fatal(err)
	}
	wrongVersion := testThreadTurnCursor(t, threadTurnCursorPayload{
		Version: threadTurnCursorVersion + 1, ThreadID: "thread", Mode: threadTurnCursorModeSince, EntryID: "missing-entry",
	})
	for name, req := range map[string]ListThreadTurnsRequest{
		"malformed":     {ThreadID: "thread", SinceCursor: threadTurnCursorPointer("not-base64!"), Limit: 1},
		"tampered":      {ThreadID: "thread", SinceCursor: threadTurnCursorPointer(string(since) + "x"), Limit: 1},
		"wrong mode":    {ThreadID: "thread", SinceCursor: &before, Limit: 1},
		"wrong thread":  {ThreadID: "thread", SinceCursor: &otherThread, Limit: 1},
		"wrong version": {ThreadID: "thread", SinceCursor: &wrongVersion, Limit: 1},
	} {
		t.Run(name, func(t *testing.T) {
			page, err := maintenance.ListThreadTurns(ctx, req)
			if !errors.Is(err, ErrInvalidThreadTurnCursor) || !reflect.DeepEqual(page, ThreadTurnsPage{}) {
				t.Fatalf("page=%#v err=%v", page, err)
			}
		})
	}
	if page, err := maintenance.ListThreadTurns(ctx, ListThreadTurnsRequest{ThreadID: "thread", SinceCursor: &since, Limit: 1}); !errors.Is(err, ErrStaleThreadTurnCursor) || !reflect.DeepEqual(page, ThreadTurnsPage{}) {
		t.Fatalf("stale page=%#v err=%v", page, err)
	}
	raw, err := json.Marshal(ListThreadTurnsRequest{ThreadID: "thread", BeforeCursor: &before, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "entry_id") || !strings.Contains(string(raw), `"before_cursor":"`) {
		t.Fatalf("cursor JSON is not opaque: %s", raw)
	}
}

func TestPublicThreadDetailEventsHideRetryAuthorityMetadata(t *testing.T) {
	events := publicThreadDetailEvents([]agentharness.SubAgentDetailEvent{{
		Kind: agentharness.SubAgentDetailEventTurnMarker,
		TurnMarker: &agentharness.SubAgentDetailTurnMarker{Status: string(sessiontree.TurnStarted), Metadata: map[string]string{
			"run_id": "run", sessiontree.RetrySourceTurnIDMetadataKey: "source-turn", sessiontree.RetrySourceEntryIDMetadataKey: "source-entry",
		}},
	}})
	if len(events) != 1 || events[0].TurnMarker == nil || events[0].TurnMarker.Metadata["run_id"] != "run" {
		t.Fatalf("events=%#v", events)
	}
	if _, found := events[0].TurnMarker.Metadata[sessiontree.RetrySourceTurnIDMetadataKey]; found {
		t.Fatalf("retry source turn leaked: %#v", events[0])
	}
	if _, found := events[0].TurnMarker.Metadata[sessiontree.RetrySourceEntryIDMetadataKey]; found {
		t.Fatalf("retry source entry leaked: %#v", events[0])
	}
}

func TestPublicThreadDetailEventsHideSubAgentInputAuthorityMetadata(t *testing.T) {
	events := publicThreadDetailEvents([]agentharness.SubAgentDetailEvent{{
		Kind: agentharness.SubAgentDetailEventUserMessage,
		Metadata: map[string]string{
			"safe_fact":                                      "kept",
			sessiontree.SubAgentInputIDMetadataKey:           "input-1",
			sessiontree.SubAgentUserMessageOriginMetadataKey: sessiontree.SubAgentUserMessageOriginDelegatedMission,
		},
	}})
	if len(events) != 1 || events[0].Metadata["safe_fact"] != "kept" {
		t.Fatalf("events=%#v", events)
	}
	for _, key := range []string{sessiontree.SubAgentInputIDMetadataKey, sessiontree.SubAgentUserMessageOriginMetadataKey} {
		if _, found := events[0].Metadata[key]; found {
			t.Fatalf("subagent input authority %q leaked: %#v", key, events[0])
		}
	}
}

func TestThreadTurnProjectionRejectsInvalidUserMessageOrigin(t *testing.T) {
	now := time.Now().UTC()
	_, _, err := projectThreadTurnSnapshots("thread", []ThreadDetailEvent{
		{
			ID: "started", Ordinal: 1, ThreadID: "thread", TurnID: "turn", RunID: "run",
			Kind: ThreadDetailEventTurnMarker, CreatedAt: now,
			TurnMarker: &ThreadDetailTurnMarker{Status: string(sessiontree.TurnStarted), Metadata: map[string]string{"run_id": "run"}},
		},
		{
			ID: "user", Ordinal: 2, ThreadID: "thread", TurnID: "turn", RunID: "run",
			Kind: ThreadDetailEventUserMessage, CreatedAt: now,
			Message:  &ThreadDetailMessage{Role: "user", Content: "hello"},
			Metadata: map[string]string{sessiontree.SubAgentUserMessageOriginMetadataKey: "unknown"},
		},
	})
	if !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("projectThreadTurnSnapshots error=%v, want ErrAuthorityCorrupt", err)
	}
}

func threadTurnCursorPointer(raw string) *ThreadTurnCursor {
	cursor := ThreadTurnCursor(raw)
	return &cursor
}

func testThreadTurnCursor(t *testing.T, payload threadTurnCursorPayload) ThreadTurnCursor {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return ThreadTurnCursor(base64.RawURLEncoding.EncodeToString(raw))
}

func TestUnfinishedForkBranchBoundaryHasCanonicalFailureAcrossPublicReads(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"memory", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			var store *Store
			var err error
			if backend == "sqlite" {
				store, err = openSQLiteStoreForTest(filepath.Join(t.TempDir(), "floret.db"))
			} else {
				store = newMemoryStore()
			}
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			if _, err := store.repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: "source"}); err != nil {
				t.Fatal(err)
			}
			if _, err := sessiontree.AppendTurnMarker(ctx, store.repo, "source", "turn", sessiontree.TurnStarted, map[string]string{"run_id": "run"}); err != nil {
				t.Fatal(err)
			}
			user, err := sessiontree.AppendMessage(ctx, store.repo, "source", "turn", session.Message{Role: session.User, Content: "work"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := sessiontree.AppendMessage(ctx, store.repo, "source", "turn", session.Message{Role: session.Assistant, Content: "done"}); err != nil {
				t.Fatal(err)
			}
			if _, err := sessiontree.AppendTurnMarker(ctx, store.repo, "source", "turn", sessiontree.TurnCompleted, map[string]string{"run_id": "run"}); err != nil {
				t.Fatal(err)
			}
			if _, err := store.repo.Fork(ctx, sessiontree.ForkOptions{
				SourceThreadID: "source", EntryID: user.ID, EntryIDPinned: true,
				Position: sessiontree.ForkAt, NewThreadID: "fork",
			}); err != nil {
				t.Fatal(err)
			}
			maintenance, err := newTestMaintenanceHost(t, store)
			if err != nil {
				t.Fatal(err)
			}
			assertFailure := func(name string, turn ThreadTurnSnapshot, err error) {
				t.Helper()
				if err != nil || turn.Status != TurnStatusInterrupted || turn.Failure == nil ||
					turn.Failure.Code != ThreadTurnFailureInterrupted || turn.Failure.Message != sessiontree.BranchBoundaryTurnFailureMessage {
					t.Fatalf("%s turn=%#v err=%v", name, turn, err)
				}
			}
			page, err := maintenance.ListThreadTurns(ctx, ListThreadTurnsRequest{ThreadID: "fork", Tail: 1})
			if err != nil || len(page.Turns) != 1 {
				t.Fatalf("ListThreadTurns page=%#v err=%v", page, err)
			}
			turn := page.Turns[0]
			assertFailure("ListThreadTurns", turn, nil)
			exact, err := maintenance.ReadThreadTurn(ctx, ReadThreadTurnRequest{ThreadID: "fork", TurnID: turn.TurnID})
			assertFailure("ReadThreadTurn", exact, err)
			assertExactThreadTurnMatchesListed(t, ctx, maintenance, "fork", turn)
			latest, err := maintenance.ReadLatestThreadTurn(ctx, "fork")
			assertFailure("ReadLatestThreadTurn", latest, err)
			overview, err := maintenance.ReadThreadOverview(ctx, "fork")
			if err != nil || overview.LatestTurn == nil {
				t.Fatalf("ReadThreadOverview overview=%#v err=%v", overview, err)
			}
			assertFailure("ReadThreadOverview", *overview.LatestTurn, nil)
			projection, err := maintenance.ReadTurnProjection(ctx, ReadTurnProjectionRequest{ThreadID: "fork", TurnID: turn.TurnID, RunID: turn.RunID})
			if err != nil || projection.Status != TurnStatusInterrupted {
				t.Fatalf("ReadTurnProjection projection=%#v err=%v", projection, err)
			}
		})
	}
}

func TestListThreadTurnsUsesCanonicalPageWithoutFullJournalReads(t *testing.T) {
	ctx := context.Background()
	store := newMemoryStore()
	base := store.repo.(*sessiontree.MemoryRepo)
	counting := &canonicalPageCountingRepo{MemoryRepo: base}
	store.repo = counting
	store.agentTodos = counting
	store.rootAuthority = counting
	maintenance, err := newTestMaintenanceHost(t, store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := maintenance.CreateThread(ctx, CreateThreadRequest{ThreadID: "thread"}); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 120; index++ {
		turnID := fmt.Sprintf("turn-%03d", index)
		runID := fmt.Sprintf("run-%03d", index)
		if _, err := sessiontree.AppendTurnMarker(ctx, counting, "thread", turnID, sessiontree.TurnStarted, map[string]string{"run_id": runID}); err != nil {
			t.Fatal(err)
		}
		if _, err := sessiontree.AppendMessage(ctx, counting, "thread", turnID, session.Message{Role: session.User, Content: turnID}); err != nil {
			t.Fatal(err)
		}
		if _, err := sessiontree.AppendMessage(ctx, counting, "thread", turnID, session.Message{Role: session.Assistant, Content: "done"}); err != nil {
			t.Fatal(err)
		}
		if _, err := sessiontree.AppendTurnMarker(ctx, counting, "thread", turnID, sessiontree.TurnCompleted, map[string]string{"run_id": runID}); err != nil {
			t.Fatal(err)
		}
	}

	page, err := maintenance.ListThreadTurns(ctx, ListThreadTurnsRequest{ThreadID: "thread", Tail: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Turns) != 2 || page.Turns[0].TurnID != "turn-118" || page.Turns[1].TurnID != "turn-119" || !page.HasMore {
		t.Fatalf("canonical page = %#v", page)
	}
	if counting.pageCalls != 1 || counting.entriesCalls != 0 || counting.pathCalls != 0 {
		t.Fatalf("repo reads page=%d entries=%d path=%d", counting.pageCalls, counting.entriesCalls, counting.pathCalls)
	}
}

type canonicalPageCountingRepo struct {
	*sessiontree.MemoryRepo
	pageCalls    int
	entriesCalls int
	pathCalls    int
}

func (r *canonicalPageCountingRepo) ListCanonicalTurns(ctx context.Context, opts sessiontree.ListCanonicalTurnsOptions) (sessiontree.CanonicalTurnsPage, error) {
	r.pageCalls++
	return r.MemoryRepo.ListCanonicalTurns(ctx, opts)
}

func (r *canonicalPageCountingRepo) Entries(ctx context.Context, threadID string) ([]sessiontree.Entry, error) {
	r.entriesCalls++
	return r.MemoryRepo.Entries(ctx, threadID)
}

func (r *canonicalPageCountingRepo) Path(ctx context.Context, threadID, leafID string) ([]sessiontree.Entry, error) {
	r.pathCalls++
	return r.MemoryRepo.Path(ctx, threadID, leafID)
}

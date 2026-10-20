package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/controlstate"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	internalstorage "github.com/floegence/floret/v7/internal/storage"
	"github.com/floegence/floret/v7/tools"
)

// completeSummaryOracle freezes the pre-streaming summary algorithm. It must
// remain independent from threadSummaryProjection to detect shared regressions.
func completeSummaryOracle(meta sessiontree.ThreadMeta, path []sessiontree.Entry) (ThreadSummary, error) {
	readyToolInputs := sessiontree.ReadyToolInputResultIDs(path)
	turnID, runID, activity, outcome, failure := threadRuntimeLifecycleFromEntries(meta, path)
	queueCount, err := canonicalQueueCountFromEntries(path)
	if err != nil {
		return ThreadSummary{}, err
	}
	summary := ThreadSummary{
		TurnKind:     turnKindFromEntries(path, turnID),
		Cancellation: threadCancellationForTurn(path, turnID),
		ID:           identity.ThreadID(meta.ID), ParentThreadID: identity.ThreadID(meta.ParentThreadID), ParentTurnID: identity.TurnID(meta.ParentTurnID),
		TaskName: meta.TaskName, TaskDescription: meta.TaskDescription, HostProfileRef: meta.HostProfileRef, ForkMode: meta.ForkMode,
		Title: meta.Title, TitleStatus: ThreadTitleStatus(meta.TitleStatus), TitleGeneration: meta.TitleGeneration, CreatedAt: meta.CreatedAt, UpdatedAt: meta.UpdatedAt,
		Activity: activity, LastOutcome: outcome, TurnID: turnID, RunID: runID, QueueCount: queueCount,
		Failure: cloneThreadTurnFailure(failure),
	}
	interactions := make(map[string]ThreadInteraction)
	interactionOrder := make([]string, 0)
	failedControls := make(map[string]threadControlIdentity)
	for _, entry := range path {
		switch entry.Type {
		case sessiontree.EntryToolCall, sessiontree.EntryToolResult:
			if entry.Message.Kind == session.MessageKindToolValidationError {
				continue
			}
			if input, err := toolInputInteraction(entry, readyToolInputs); err != nil {
				return ThreadSummary{}, err
			} else if input != nil {
				interactionOrder = append(interactionOrder, input.ID)
				interactions[input.ID] = *input
				continue
			}
			if entry.Message.Kind != session.MessageKindControlSignal || entry.Message.ControlSignal == nil {
				continue
			}
			signal := entry.Message.ControlSignal
			input, classification, inputErr := waitingInputFromControlSignal(signal)
			if inputErr != nil {
				return ThreadSummary{}, inputErr
			}
			if classification == controlstate.Failed {
				failedControls[signal.CallID] = threadControlIdentity{turnID: entry.TurnID, runID: entry.RunID}
				delete(interactions, signal.CallID)
				continue
			}
			if classification != controlstate.WaitingInput {
				continue
			}
			if _, found := interactions[signal.CallID]; !found {
				interactionOrder = append(interactionOrder, signal.CallID)
			}
			interactions[signal.CallID] = ThreadInteraction{
				ID: signal.CallID, TurnID: identity.TurnID(entry.TurnID), Kind: ThreadInteractionInput,
				Input: input,
			}
		case sessiontree.EntryInteractionAsked:
			interactionID := strings.TrimPrefix(entry.ID, "interaction-requested:")
			if failed, ok := failedControls[interactionID]; ok && failed.turnID == entry.TurnID && failed.runID == entry.RunID {
				continue
			}
			var interaction ThreadInteraction
			if err := json.Unmarshal(entry.Payload, &interaction); err != nil {
				return ThreadSummary{}, fmt.Errorf("decode interaction %q: %w", entry.ID, err)
			}
			if strings.TrimSpace(interaction.ID) == "" {
				return ThreadSummary{}, ErrAuthorityCorrupt
			}
			if _, found := interactions[interaction.ID]; found {
				// The committed control already owns this interaction and any answer.
				continue
			}
			interactionOrder = append(interactionOrder, interaction.ID)
			interactions[interaction.ID] = interaction
		case sessiontree.EntryInteractionDone:
			interactionID := strings.TrimPrefix(entry.ID, "interaction-resolved:")
			interaction, found := interactions[interactionID]
			if found {
				interaction.Resolved = true
				interactions[interactionID] = interaction
			}
		}
		if entry.Type == sessiontree.EntryUserMessage || entry.Type == sessiontree.EntryAssistantMessage && entry.Message.Kind != "control_signal" {
			if text := strings.TrimSpace(entry.Message.Content); text != "" {
				summary.LastItemAt = entry.CreatedAt
				summary.LastItemPreview = truncateThreadSummaryPreview(text, 160)
			}
		}
	}
	for _, interactionID := range interactionOrder {
		interaction := interactions[interactionID]
		if interaction.Resolved {
			continue
		}
		switch interaction.Kind {
		case ThreadInteractionApproval:
			summary.Attention.ApprovalCount++
		case ThreadInteractionInput:
			summary.Attention.InputCount++
			if interaction.Input != nil {
				summary.PendingInput = cloneInputPresentation(interaction.Input)
			}
		}
	}
	if summary.Activity == ThreadActivityActive && summary.Attention.ApprovalCount == 0 && summary.Attention.InputCount == 0 {
		summary.RunProgress = &ThreadRunProgress{Phase: ThreadRunPhasePreparing}
	}
	return summary, nil
}

func TestThreadSummaryStreamingMatchesFrozenCompleteOracle(t *testing.T) {
	entries := incrementalProjectionFixture(t)
	encode := func(v any) []byte {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	entries = append(entries,
		sessiontree.Entry{ID: "queue-a", Type: sessiontree.EntryQueueAdded, Payload: encode(map[string]string{"id": "a"})},
		sessiontree.Entry{ID: "queue-b", Type: sessiontree.EntryQueueAdded, Payload: encode(map[string]string{"id": "b"})},
		sessiontree.Entry{ID: "queue-reorder", Type: sessiontree.EntryQueueReordered, Payload: encode([]string{"b", "a"})},
		sessiontree.Entry{ID: "queue-promote", Type: sessiontree.EntryQueuePromoted, Payload: encode("b")},
		sessiontree.Entry{ID: "queue-delete", Type: sessiontree.EntryQueueDeleted, Payload: encode("a")},
		sessiontree.Entry{ID: "start-next", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted, Metadata: map[string]string{sessiontree.TurnKindMetadataKey: string(TurnKindContextCompaction)}},
		sessiontree.Entry{ID: "failure", Type: sessiontree.EntryRunFailure, Error: "provider unavailable", Metadata: map[string]string{sessiontree.TurnFailureCodeMetadataKey: string(ThreadTurnFailureProvider)}},
		sessiontree.Entry{ID: "cancel", Type: sessiontree.EntryCancelRequested, Metadata: map[string]string{"cancellation_source": "user_stop", "cancellation_mode": string(CancelModeGraceful)}},
		sessiontree.Entry{ID: "failed", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnFailed},
	)
	for i := range entries {
		entries[i].ThreadID = "thread"
		if entries[i].TurnID == "" {
			entries[i].TurnID = "next"
			entries[i].RunID = "next-run"
		}
		entries[i].CreatedAt = time.Unix(int64(i), 0).UTC()
	}
	meta := sessiontree.ThreadMeta{ID: "thread", Title: "Test"}
	for n := 0; n <= len(entries); n++ {
		got, err := threadSummaryFromCanonicalPath(meta, entries[:n])
		want, wantErr := completeSummaryOracle(meta, entries[:n])
		if err != nil || wantErr != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("prefix %d: got=%+v err=%v want=%+v err=%v", n, got, err, want, wantErr)
		}
	}
	// Uncommitted malformed input cannot become visible or fail a summary until
	// the canonical waiting marker admits the complete batch.
	pending := sessiontree.Entry{ID: "pending-invalid", ThreadID: "thread", TurnID: "pending", RunID: "run", Type: sessiontree.EntryToolResult, Message: session.Message{ToolResult: &session.ToolResultView{InputRequired: &tools.InputRequest{}}}}
	for _, path := range [][]sessiontree.Entry{{pending}, {pending, {Type: sessiontree.EntryTurnMarker, TurnID: "pending", RunID: "run", TurnStatus: sessiontree.TurnWaiting}}} {
		got, err := threadSummaryFromCanonicalPath(meta, path)
		want, wantErr := completeSummaryOracle(meta, path)
		if (err == nil) != (wantErr == nil) || err == nil && !reflect.DeepEqual(got, want) {
			t.Fatalf("pending classification differs got=%+v err=%v want=%+v err=%v", got, err, want, wantErr)
		}
	}
}

type coldSummaryRepo struct {
	*internalstorage.BackendKernel
	paths, pages, reads, readBytes int
	fields                         map[string]int
	afterPage                      func(int)
	failAt                         int
}

func (r *coldSummaryRepo) Path(ctx context.Context, id, leaf string) ([]sessiontree.Entry, error) {
	r.paths++
	return r.BackendKernel.Path(ctx, id, leaf)
}
func (r *coldSummaryRepo) PathMetadataWindow(ctx context.Context, o sessiontree.PathWindowOptions) (sessiontree.PathMetadataWindow, error) {
	r.pages++
	if r.pages == r.failAt {
		return sessiontree.PathMetadataWindow{}, errors.New("injected metadata read failure")
	}
	p, e := r.BackendKernel.PathMetadataWindow(ctx, o)
	if e == nil && r.afterPage != nil {
		r.afterPage(r.pages)
	}
	return p, e
}
func (r *coldSummaryRepo) ReadCanonicalBody(ctx context.Context, id, entry, field string, offset, limit int) (sessiontree.CanonicalBodyChunk, error) {
	r.reads++
	r.fields[field]++
	p, e := r.BackendKernel.ReadCanonicalBody(ctx, id, entry, field, offset, limit)
	r.readBytes += len(p.Text)
	return p, e
}

func TestThreadSummaryColdStreamsFrozenMetadataAndOnlyRequiredBodies(t *testing.T) {
	host, public := testThreadService(t, newBlockingThreadGateway())
	service := public.(*threadRuntimeService)
	const id = "cold-summary"
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: id}); err != nil {
		t.Fatal(err)
	}
	appendEntry := func(e sessiontree.Entry) {
		t.Helper()
		e.ThreadID, e.TurnID, e.RunID = id, "turn", "run"
		if _, err := host.store.repo.Append(t.Context(), e, sessiontree.AppendOptions{ID: e.ID}); err != nil {
			t.Fatal(err)
		}
	}
	appendEntry(sessiontree.Entry{ID: "start", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnStarted})
	for i := range 600 {
		appendEntry(sessiontree.Entry{ID: fmt.Sprint(i), Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: strings.Repeat("large historical body 中文🌸", 100)}})
	}
	appendEntry(sessiontree.Entry{ID: "last", Type: sessiontree.EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: strings.Repeat("界🌸", 1<<18)}})
	appendEntry(sessiontree.Entry{ID: "space", Type: sessiontree.EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: " \r\n "}})
	appendEntry(sessiontree.Entry{ID: "done", Type: sessiontree.EntryTurnMarker, TurnStatus: sessiontree.TurnCompleted})
	meta, err := host.store.repo.Thread(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	path, err := host.store.repo.Path(t.Context(), id, meta.LeafID)
	if err != nil {
		t.Fatal(err)
	}
	want, err := completeSummaryOracle(meta, path)
	if err != nil {
		t.Fatal(err)
	}
	repo := &coldSummaryRepo{BackendKernel: host.store.repo.(*internalstorage.BackendKernel), fields: map[string]int{}}
	host.store.repo = repo
	repo.afterPage = func(n int) {
		if n == 1 {
			if _, err := repo.Append(t.Context(), sessiontree.Entry{ThreadID: id, ID: "late", Type: sessiontree.EntryCustom}, sessiontree.AppendOptions{ID: "late"}); err != nil {
				t.Fatal(err)
			}
		}
	}
	got, err := service.coldThreadSummary(t.Context(), meta)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("frozen summary differs: err=%v got=%+v want=%+v", err, got, want)
	}
	if repo.paths != 0 || repo.pages != 5 || repo.readBytes > 32<<10 || len(repo.fields) != 1 || repo.fields["content"] != 2 {
		t.Fatalf("unexpected source access: %+v", repo)
	}
	repo.afterPage, repo.pages, repo.failAt = nil, 0, 2
	if got, err := service.coldThreadSummary(t.Context(), meta); err == nil || !reflect.DeepEqual(got, ThreadSummary{}) {
		t.Fatalf("partial state returned: %+v %v", got, err)
	}
	repo.pages, repo.failAt = 0, 0
	ctx, cancel := context.WithCancel(t.Context())
	repo.afterPage = func(n int) {
		if n == 1 {
			cancel()
		}
	}
	if got, err := service.coldThreadSummary(ctx, meta); !errors.Is(err, context.Canceled) || !reflect.DeepEqual(got, ThreadSummary{}) {
		t.Fatalf("cancelled summary returned: %+v %v", got, err)
	}
	repo.pages = 0
	repo.afterPage = func(n int) {
		if n == 1 {
			if err := repo.MoveLeaf(t.Context(), id, "0"); err != nil {
				t.Fatal(err)
			}
		}
	}
	if got, err := service.coldThreadSummary(t.Context(), meta); !errors.Is(err, sessiontree.ErrEntryNotFound) || !reflect.DeepEqual(got, ThreadSummary{}) {
		t.Fatalf("changed path accepted: %+v %v", got, err)
	}
}

func TestThreadSummaryPreviewMatchesCompleteTrimAndRuneBoundary(t *testing.T) {
	host, _ := testThreadService(t, newBlockingThreadGateway())
	repo := host.store.repo
	if _, err := repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: "preview"}); err != nil {
		t.Fatal(err)
	}
	cases := []string{"", strings.Repeat(" \r\n", 20000), strings.Repeat(" ", 20000) + strings.Repeat("界🌸", 90) + strings.Repeat(" ", 20000), strings.Repeat("🌸", 159) + " " + "more", strings.Repeat("🌸", 159) + " ", "\r\n 界🌸 \t"}
	for i, body := range cases {
		id := fmt.Sprint(i)
		if _, err := repo.Append(t.Context(), sessiontree.Entry{ThreadID: "preview", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: body}}, sessiontree.AppendOptions{ID: id}); err != nil {
			t.Fatal(err)
		}
		got, err := readSummaryPreview(t.Context(), repo.(sessiontree.CanonicalBodyReader), "preview", id, len(body))
		want := truncateThreadSummaryPreview(strings.TrimSpace(body), 160)
		if err != nil || got != want {
			t.Fatalf("case %d: got=%q err=%v want=%q", i, got, err, want)
		}
	}
}

func TestThreadSummaryControlOrderingMatchesFrozenOracle(t *testing.T) {
	var payload map[string]any
	if err := json.Unmarshal([]byte(cancelHistoryAskArgs), &payload); err != nil {
		t.Fatal(err)
	}
	call := sessiontree.Entry{ID: "ask", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: sessiontree.EntryToolCall, Message: session.Message{Role: session.Assistant, Kind: session.MessageKindControlSignal, ToolCallID: "ask", ToolName: "ask_user", ControlSignal: &session.ControlSignalView{Name: "ask_user", CallID: "ask", Disposition: "waiting", OutputText: "Question", Payload: payload}}}
	input, _, err := waitingInputFromControlSignal(call.Message.ControlSignal)
	if err != nil {
		t.Fatal(err)
	}
	askedJSON, err := json.Marshal(ThreadInteraction{ID: "ask", TurnID: "turn", RunID: "run", Kind: ThreadInteractionInput, Input: input})
	if err != nil {
		t.Fatal(err)
	}
	asked := sessiontree.Entry{ID: "interaction-requested:ask", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: sessiontree.EntryInteractionAsked, Payload: askedJSON}
	done := sessiontree.Entry{ID: "interaction-resolved:ask", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: sessiontree.EntryInteractionDone, Payload: json.RawMessage("{\"accepted\":true}")}
	failed := call
	signal := *call.Message.ControlSignal
	signal.ErrorCode = session.ControlSignalErrorCodeControlError
	failed.Message.ControlSignal = &signal
	paths := [][]sessiontree.Entry{{call}, {call, done}, {call, asked, done}, {call, done, asked}, {failed, asked}, {call, failed, asked}}
	for i, path := range paths {
		got, err := threadSummaryFromCanonicalPath(sessiontree.ThreadMeta{ID: "thread"}, path)
		want, wantErr := completeSummaryOracle(sessiontree.ThreadMeta{ID: "thread"}, path)
		if (err == nil) != (wantErr == nil) || err == nil && !reflect.DeepEqual(got, want) {
			t.Fatalf("path %d differs: got=%+v err=%v want=%+v err=%v", i, got, err, want, wantErr)
		}
	}
}

func TestThreadSummaryColdEmptyLeafStaysFrozen(t *testing.T) {
	host, public := testThreadService(t, newBlockingThreadGateway())
	service := public.(*threadRuntimeService)
	meta, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: "empty-summary"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.store.repo.Append(t.Context(), sessiontree.Entry{ThreadID: meta.ID, Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: "new fact"}}, sessiontree.AppendOptions{ID: "late"}); err != nil {
		t.Fatal(err)
	}
	repo := &coldSummaryRepo{BackendKernel: host.store.repo.(*internalstorage.BackendKernel), fields: map[string]int{}}
	host.store.repo = repo
	got, err := service.coldThreadSummary(t.Context(), meta)
	want, wantErr := completeSummaryOracle(meta, nil)
	if err != nil || wantErr != nil || !reflect.DeepEqual(got, want) || repo.pages != 0 || repo.reads != 0 {
		t.Fatalf("empty snapshot changed: got=%+v err=%v want=%+v", got, err, want)
	}
}

func TestThreadSummaryToolInputInterleavingsMatchFrozenOracle(t *testing.T) {
	input := &tools.InputRequest{Summary: "Verify", Questions: []tools.InputQuestion{{ID: "q", Prompt: "Continue?", Kind: "write"}}}
	result := sessiontree.Entry{ID: "result", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: sessiontree.EntryToolResult, Message: session.Message{ToolCallID: "work", ToolResult: &session.ToolResultView{InputRequired: input}}}
	waiting := sessiontree.Entry{Type: sessiontree.EntryTurnMarker, TurnID: "turn", RunID: "run", TurnStatus: sessiontree.TurnWaiting}
	raw, _ := json.Marshal(ThreadInteraction{ID: sessiontree.ToolInputInteractionID(result.ID), TurnID: "turn", RunID: "run", Kind: ThreadInteractionInput, Input: &InputPresentation{Summary: "Verify"}})
	asked := sessiontree.Entry{ID: "interaction-requested:" + sessiontree.ToolInputInteractionID(result.ID), Type: sessiontree.EntryInteractionAsked, Payload: raw}
	done := sessiontree.Entry{ID: "interaction-resolved:" + sessiontree.ToolInputInteractionID(result.ID), Type: sessiontree.EntryInteractionDone}
	for i, path := range [][]sessiontree.Entry{{result, asked, waiting}, {result, done, asked, waiting}, {result, asked, done, waiting}, {result, waiting, asked}, {waiting, result}, {asked, result, waiting}, {asked, result, done, waiting}} {
		got, err := threadSummaryFromCanonicalPath(sessiontree.ThreadMeta{ID: "thread"}, path)
		want, wantErr := completeSummaryOracle(sessiontree.ThreadMeta{ID: "thread"}, path)
		if (err == nil) != (wantErr == nil) || err == nil && !reflect.DeepEqual(got, want) {
			t.Fatalf("interleaving %d differs: got=%+v err=%v want=%+v err=%v", i, got, err, want, wantErr)
		}
	}
}

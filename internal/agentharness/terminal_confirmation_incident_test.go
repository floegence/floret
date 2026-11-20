package agentharness

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/internal/testing/harness"
)

type uncertainCompletionStore struct {
	*sessiontree.MemoryRepo
	claims []sessiontree.FinishTurnRequest
}

func (store *uncertainCompletionStore) FinishTurn(ctx context.Context, req sessiontree.FinishTurnRequest) (sessiontree.FinishTurnResult, error) {
	store.claims = append(store.claims, req)
	if req.Status == sessiontree.TurnCompleted {
		return sessiontree.FinishTurnResult{}, context.DeadlineExceeded
	}
	return store.MemoryRepo.FinishTurn(ctx, req)
}

func TestUnconfirmedFinishDoesNotReplaceOriginalRequest(t *testing.T) {
	ctx := t.Context()
	store := &uncertainCompletionStore{MemoryRepo: sessiontree.NewMemoryRepo()}
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AcceptTurn(ctx, sessiontree.AcceptTurnRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", RequestFingerprint: "request", InputRequestFingerprint: "input", Input: session.Message{Role: session.User, Content: "work"}}); err != nil {
		t.Fatal(err)
	}
	agent := New(Options{Repo: store, Provider: harness.NewScriptedProvider(harness.Step(harness.Text("finished"), harness.Done())), ProviderName: "test", Model: "scripted", TurnPolicy: TurnPolicy{ContextPolicy: contextpolicy.Policy{ContextWindowTokens: 10000, ReservedOutputTokens: 1000}}})
	thread := &Thread{id: "thread", harness: agent}
	result, err := thread.runAccepted(ctx, "work", RunOptions{TurnID: "turn", RunID: "run", AdmissionCommitted: true}, nil)
	if !errors.Is(err, sessiontree.ErrFinishConfirmationPending) || len(store.claims) != 2 {
		t.Fatalf("unconfirmed completion: claims=%d err=%v", len(store.claims), err)
	}
	persisted, found, err := sessiontree.ReadCompletedTurnIntent(ctx, store, "thread", "turn", "run")
	if err != nil || !found || !reflect.DeepEqual(store.claims[0], store.claims[1]) || !reflect.DeepEqual(store.claims[0], persisted) {
		t.Fatalf("original completion changed across the durable boundary: %v", err)
	}
	entries, err := store.Entries(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Type == sessiontree.EntryTurnMarker && entry.TurnStatus == sessiontree.TurnFailed {
			t.Fatalf("original completion replaced by failed terminal: code=%s execution=%s result=%s", entry.Metadata[sessiontree.TurnFailureCodeMetadataKey], entry.Metadata["execution_status"], result.Status)
		}
	}
}

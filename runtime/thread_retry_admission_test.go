package runtime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/floegence/floret/v7/internal/sessiontree"
	internalstorage "github.com/floegence/floret/v7/internal/storage"
	"github.com/floegence/floret/v7/storage"
)

type rejectingRetryRepo struct {
	*internalstorage.BackendKernel
	err error
}

func (repo *rejectingRetryRepo) AcceptTurn(context.Context, sessiontree.AcceptTurnRequest) (sessiontree.AcceptTurnResult, error) {
	return sessiontree.AcceptTurnResult{}, repo.err
}

func TestRetryAdmissionFailurePreservesPriorState(t *testing.T) {
	raw, err := os.ReadFile("testdata/late-work-v7.129.1/history.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "copy.sqlite")
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	h, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := h.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	var factories atomic.Int32
	svc, err := h.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) {
		factories.Add(1)
		return nil, errors.New("unexpected preparation")
	}))
	if err != nil {
		t.Fatal(err)
	}
	v, err := svc.View(t.Context(), "ab-thread")
	if err != nil || v.Failure == nil {
		t.Fatalf("initial failure: %v %v", v.Failure, err)
	}
	injected := errors.New("rejected retry transaction")
	h.store.repo = &rejectingRetryRepo{h.store.repo.(*internalstorage.BackendKernel), injected}
	for range 2 {
		_, err = svc.Retry(t.Context(), RetryInput{ThreadID: v.ThreadID, SourceTurnID: v.TurnID, RequestKey: "same-key"})
		if !errors.Is(err, injected) {
			t.Fatalf("admission error: %v", err)
		}
		after, err := svc.View(t.Context(), v.ThreadID)
		if err != nil || after.Activity != ThreadActivityIdle || after.TurnID != v.TurnID || after.RunID != v.RunID || after.Failure == nil || *after.Failure != *v.Failure || len(after.Turns) != 3 || factories.Load() != 0 {
			t.Fatalf("rejected retry mutated state: %+v %v", after, err)
		}
	}
}

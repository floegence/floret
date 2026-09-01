package storage

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/floegence/floret/v7/internal/provider/cache"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/internal/storagecodec"
	"github.com/floegence/floret/v7/storage/spi"
)

const backendDomainNamespace = "floret.domain"

var promptStateKey = storagecodec.Tuple(storagecodec.TupleString("prompt"), storagecodec.TupleString("state"))

// BackendKernel is the single domain kernel shared by every public Backend.
type BackendKernel struct {
	*sessiontree.BackendRepo
	promptMu sync.Mutex
	prompt   *cache.MemoryStore
}

type StartupPhase = sessiontree.StartupPhase
type StartupProgress = sessiontree.StartupProgress

const (
	StartupPhaseMigrating = sessiontree.StartupPhaseMigrating
	StartupPhaseVerifying = sessiontree.StartupPhaseVerifying
)

// NewBackendKernel opens all canonical Floret domain state.
func NewBackendKernel(ctx context.Context, backend spi.Backend, now func() time.Time) (*BackendKernel, error) {
	var kernel *BackendKernel
	if err := backend.Update(ctx, func(tx spi.WriteTx) error {
		var err error
		kernel, err = NewBackendKernelInTransaction(ctx, backend, tx, now, nil)
		if err != nil {
			return err
		}
		return kernel.VerifyCurrentStateInTransaction(ctx, tx)
	}); err != nil {
		return nil, err
	}
	return kernel, nil
}

// NewBackendKernelInTransaction opens all canonical Floret domain state in
// the caller's startup transaction.
func NewBackendKernelInTransaction(ctx context.Context, backend spi.Backend, tx spi.WriteTx, now func() time.Time, progress StartupProgress) (*BackendKernel, error) {
	repo, err := sessiontree.NewBackendRepoInTransaction(ctx, backend, tx, now, progress)
	if err != nil {
		return nil, err
	}
	kernel := &BackendKernel{BackendRepo: repo}
	prompt, found, err := loadPromptState(tx)
	if err != nil {
		return nil, err
	}
	if found {
		kernel.prompt = prompt
	} else {
		kernel.prompt = cache.NewMemoryStore()
		if err := savePromptState(tx, kernel.prompt); err != nil {
			return nil, err
		}
	}
	return kernel, nil
}

func loadPromptState(tx spi.ReadTx) (*cache.MemoryStore, bool, error) {
	encoded, err := tx.Get(backendDomainNamespace, promptStateKey)
	if errors.Is(err, spi.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	payload, err := storagecodec.DecodeEnvelope(encoded, "prompt")
	if err != nil {
		return nil, false, err
	}
	state, err := cache.DecodeMemoryState(payload)
	return state, true, err
}

func savePromptState(tx spi.WriteTx, state *cache.MemoryStore) error {
	payload, err := state.EncodeMemoryState()
	if err != nil {
		return err
	}
	encoded, err := storagecodec.EncodeEnvelope("prompt", payload)
	if err != nil {
		return err
	}
	return tx.Put(backendDomainNamespace, promptStateKey, encoded)
}

func (kernel *BackendKernel) updatePrompt(ctx context.Context, mutate func(*cache.MemoryStore) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	kernel.promptMu.Lock()
	defer kernel.promptMu.Unlock()
	if kernel.prompt == nil {
		return errors.New("prompt state is missing")
	}
	return mutate(kernel.prompt)
}

func (kernel *BackendKernel) viewPrompt(ctx context.Context, read func(*cache.MemoryStore) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	kernel.promptMu.Lock()
	defer kernel.promptMu.Unlock()
	if kernel.prompt == nil {
		return errors.New("prompt state is missing")
	}
	return read(kernel.prompt)
}

func clonePromptState(state *cache.MemoryStore) (*cache.MemoryStore, error) {
	if state == nil {
		return nil, errors.New("prompt state is missing")
	}
	payload, err := state.EncodeMemoryState()
	if err != nil {
		return nil, err
	}
	return cache.DecodeMemoryState(payload)
}

// FinishTurn commits the terminal semantic state and accumulated prompt
// observations in one transaction. Provider request checkpoints are already
// durable; this boundary adds response diagnostics and terminal state.
func (kernel *BackendKernel) FinishTurn(ctx context.Context, request sessiontree.FinishTurnRequest) (result sessiontree.FinishTurnResult, err error) {
	kernel.promptMu.Lock()
	defer kernel.promptMu.Unlock()
	if kernel.prompt == nil {
		return sessiontree.FinishTurnResult{}, errors.New("prompt state is missing")
	}
	err = kernel.CheckpointDomainUpdate(ctx, func(memory *sessiontree.MemoryRepo, tx spi.WriteTx) error {
		result, err = memory.FinishTurn(ctx, request)
		if err != nil {
			return err
		}
		return savePromptState(tx, kernel.prompt)
	})
	return result, err
}

// FailUnknownEffectTurn commits the fixed unknown-effect terminal together
// with the current prompt-cache checkpoint. Provider continuation is removed
// by the session-tree mutation in the same transaction.
func (kernel *BackendKernel) FailUnknownEffectTurn(ctx context.Context, request sessiontree.FailUnknownEffectTurnRequest) (result sessiontree.FailUnknownEffectTurnResult, err error) {
	kernel.promptMu.Lock()
	defer kernel.promptMu.Unlock()
	if kernel.prompt == nil {
		return sessiontree.FailUnknownEffectTurnResult{}, errors.New("prompt state is missing")
	}
	err = kernel.CheckpointDomainUpdate(ctx, func(memory *sessiontree.MemoryRepo, tx spi.WriteTx) error {
		result, err = memory.FailUnknownEffectTurn(ctx, request)
		if err != nil {
			return err
		}
		return savePromptState(tx, kernel.prompt)
	})
	return result, err
}

// CancelTurn commits the user Stop terminal and clears prompt continuation in
// the same transaction. It is the cancellation counterpart of FinishTurn.
func (kernel *BackendKernel) CancelTurn(ctx context.Context, request sessiontree.CancelTurnRequest) (result sessiontree.CancelTurnResult, err error) {
	kernel.promptMu.Lock()
	defer kernel.promptMu.Unlock()
	if kernel.prompt == nil {
		return sessiontree.CancelTurnResult{}, errors.New("prompt state is missing")
	}
	err = kernel.CheckpointDomainUpdate(ctx, func(memory *sessiontree.MemoryRepo, tx spi.WriteTx) error {
		result, err = memory.CancelTurn(ctx, request)
		if err != nil {
			return err
		}
		return savePromptState(tx, kernel.prompt)
	})
	return result, err
}

// Checkpoint flushes both canonical domain state and memory-resident prompt
// observations during graceful shutdown or an explicit recovery barrier.
func (kernel *BackendKernel) Checkpoint(ctx context.Context) error {
	kernel.promptMu.Lock()
	defer kernel.promptMu.Unlock()
	if kernel.prompt == nil {
		return errors.New("prompt state is missing")
	}
	return kernel.CheckpointDomain(ctx, func(tx spi.WriteTx) error {
		return savePromptState(tx, kernel.prompt)
	})
}

func (kernel *BackendKernel) AppendSegment(ctx context.Context, value cache.Segment) error {
	return kernel.updatePrompt(ctx, func(state *cache.MemoryStore) error { return state.AppendSegment(ctx, value) })
}

func (kernel *BackendKernel) Segments(ctx context.Context, scopeID, provider, model string) (result []cache.Segment, err error) {
	err = kernel.viewPrompt(ctx, func(state *cache.MemoryStore) error {
		result, err = state.Segments(ctx, scopeID, provider, model)
		return err
	})
	return result, err
}

func (kernel *BackendKernel) AppendToolset(ctx context.Context, value cache.ToolsetSnapshot) error {
	return kernel.updatePrompt(ctx, func(state *cache.MemoryStore) error { return state.AppendToolset(ctx, value) })
}

func (kernel *BackendKernel) ActiveToolset(ctx context.Context, scopeID, provider, model string) (result cache.ToolsetSnapshot, found bool, err error) {
	err = kernel.viewPrompt(ctx, func(state *cache.MemoryStore) error {
		result, found, err = state.ActiveToolset(ctx, scopeID, provider, model)
		return err
	})
	return result, found, err
}

func (kernel *BackendKernel) AppendProviderRequest(ctx context.Context, value cache.ProviderRequestRecord) error {
	return kernel.CheckpointProviderRequest(ctx, nil, nil, value)
}

func (kernel *BackendKernel) CheckpointProviderRequest(ctx context.Context, segments []cache.Segment, toolsets []cache.ToolsetSnapshot, value cache.ProviderRequestRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	kernel.promptMu.Lock()
	defer kernel.promptMu.Unlock()
	nextPrompt, err := clonePromptState(kernel.prompt)
	if err != nil {
		return err
	}
	for _, segment := range segments {
		if err := nextPrompt.AppendSegment(ctx, segment); err != nil {
			return err
		}
	}
	for _, toolset := range toolsets {
		if err := nextPrompt.AppendToolset(ctx, toolset); err != nil {
			return err
		}
	}
	if err := nextPrompt.AppendProviderRequest(ctx, value); err != nil {
		return err
	}
	if err := kernel.CheckpointDomain(ctx, func(tx spi.WriteTx) error {
		return savePromptState(tx, nextPrompt)
	}); err != nil {
		return err
	}
	kernel.prompt = nextPrompt
	return nil
}

func (kernel *BackendKernel) ProviderRequests(ctx context.Context, scopeID string) (result []cache.ProviderRequestRecord, err error) {
	err = kernel.viewPrompt(ctx, func(state *cache.MemoryStore) error {
		result, err = state.ProviderRequests(ctx, scopeID)
		return err
	})
	return result, err
}

func (kernel *BackendKernel) AppendProviderResponse(ctx context.Context, value cache.ProviderResponseRecord) error {
	return kernel.updatePrompt(ctx, func(state *cache.MemoryStore) error { return state.AppendProviderResponse(ctx, value) })
}

func (kernel *BackendKernel) ProviderResponses(ctx context.Context, scopeID string) (result []cache.ProviderResponseRecord, err error) {
	err = kernel.viewPrompt(ctx, func(state *cache.MemoryStore) error {
		result, err = state.ProviderResponses(ctx, scopeID)
		return err
	})
	return result, err
}

func (kernel *BackendKernel) LatestPressureAnchor(ctx context.Context, scopeID, provider, model string) (result cache.PressureAnchorState, found bool, err error) {
	err = kernel.viewPrompt(ctx, func(state *cache.MemoryStore) error {
		result, found, err = state.LatestPressureAnchor(ctx, scopeID, provider, model)
		return err
	})
	return result, found, err
}

func (kernel *BackendKernel) DeletePromptScopes(ctx context.Context, scopeIDs ...string) error {
	return kernel.updatePrompt(ctx, func(state *cache.MemoryStore) error { return state.DeletePromptScopes(ctx, scopeIDs...) })
}

func (kernel *BackendKernel) DeleteRootTree(ctx context.Context, rootThreadID string) (result sessiontree.DeleteRootTreeResult, err error) {
	kernel.promptMu.Lock()
	defer kernel.promptMu.Unlock()
	nextPrompt, err := clonePromptState(kernel.prompt)
	if err != nil {
		return sessiontree.DeleteRootTreeResult{}, err
	}
	err = kernel.UpdateDomain(ctx, func(memory *sessiontree.MemoryRepo, tx spi.WriteTx) error {
		result, err = memory.DeleteRootTree(ctx, rootThreadID)
		if err != nil {
			return err
		}
		if err := nextPrompt.DeletePromptScopes(ctx, result.ThreadIDs...); err != nil {
			return err
		}
		return savePromptState(tx, nextPrompt)
	})
	if err == nil {
		kernel.prompt = nextPrompt
	}
	return result, err
}

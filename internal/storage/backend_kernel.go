package storage

import (
	"context"
	"sync"
	"time"

	"github.com/floegence/floret/v7/internal/provider/cache"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/storage/spi"
)

const backendDomainNamespace = "floret.domain"

// BackendKernel is the single domain kernel shared by every public Backend.
type BackendKernel struct {
	*sessiontree.BackendRepo
	promptMu      sync.Mutex
	promptBackend spi.Backend
	pending       map[string]promptBatch
}

type StartupPhase = sessiontree.StartupPhase
type StartupProgress = sessiontree.StartupProgress

const (
	StartupPhaseMigrating = sessiontree.StartupPhaseMigrating
	StartupPhaseVerifying = sessiontree.StartupPhaseVerifying
)

func NewBackendKernel(ctx context.Context, backend spi.Backend, now func() time.Time) (*BackendKernel, error) {
	var kernel *BackendKernel
	if err := backend.Update(ctx, func(tx spi.WriteTx) error {
		var err error
		kernel, err = NewBackendKernelInTransaction(ctx, backend, tx, now, true, nil)
		if err != nil {
			return err
		}
		return kernel.VerifyCurrentStateInTransaction(ctx, tx)
	}); err != nil {
		return nil, err
	}
	return kernel, nil
}

func NewBackendKernelInTransaction(ctx context.Context, backend spi.Backend, tx spi.WriteTx, now func() time.Time, allowPromptMigration bool, progress StartupProgress) (*BackendKernel, error) {
	if err := preparePromptRecords(ctx, tx, allowPromptMigration, progress); err != nil {
		return nil, err
	}
	repo, err := sessiontree.NewBackendRepoInTransaction(ctx, backend, tx, now, progress)
	if err != nil {
		return nil, err
	}
	return &BackendKernel{BackendRepo: repo, promptBackend: backend, pending: make(map[string]promptBatch)}, nil
}

func (kernel *BackendKernel) VerifyCurrentStateInTransaction(ctx context.Context, tx spi.ReadTx) error {
	if err := kernel.BackendRepo.VerifyCurrentStateInTransaction(ctx, tx); err != nil {
		return err
	}
	return verifyPromptRecords(tx)
}

func (kernel *BackendKernel) FinishTurn(ctx context.Context, request sessiontree.FinishTurnRequest) (result sessiontree.FinishTurnResult, err error) {
	kernel.promptMu.Lock()
	defer kernel.promptMu.Unlock()
	err = kernel.CheckpointDomainUpdate(ctx, func(memory *sessiontree.MemoryRepo, tx spi.WriteTx) error {
		result, err = memory.FinishTurn(ctx, request)
		if err != nil {
			return err
		}
		return kernel.flushPrompt(tx, request.ThreadID)
	})
	if err == nil {
		kernel.clearPending(request.ThreadID)
	}
	return result, err
}

func (kernel *BackendKernel) FailUnknownEffectTurn(ctx context.Context, request sessiontree.FailUnknownEffectTurnRequest) (result sessiontree.FailUnknownEffectTurnResult, err error) {
	kernel.promptMu.Lock()
	defer kernel.promptMu.Unlock()
	err = kernel.CheckpointDomainUpdate(ctx, func(memory *sessiontree.MemoryRepo, tx spi.WriteTx) error {
		result, err = memory.FailUnknownEffectTurn(ctx, request)
		if err != nil {
			return err
		}
		return kernel.flushPrompt(tx, request.ThreadID)
	})
	if err == nil {
		kernel.clearPending(request.ThreadID)
	}
	return result, err
}

func (kernel *BackendKernel) CancelTurn(ctx context.Context, request sessiontree.CancelTurnRequest) (result sessiontree.CancelTurnResult, err error) {
	kernel.promptMu.Lock()
	defer kernel.promptMu.Unlock()
	err = kernel.CheckpointDomainUpdate(ctx, func(memory *sessiontree.MemoryRepo, tx spi.WriteTx) error {
		result, err = memory.CancelTurn(ctx, request)
		if err != nil {
			return err
		}
		return kernel.flushPrompt(tx, request.ThreadID)
	})
	if err == nil {
		kernel.clearPending(request.ThreadID)
	}
	return result, err
}

// Checkpoint flushes only facts not committed by a provider or turn boundary.
func (kernel *BackendKernel) Checkpoint(ctx context.Context) error {
	kernel.promptMu.Lock()
	defer kernel.promptMu.Unlock()
	scopes := kernel.pendingScopes()
	err := kernel.CheckpointDomain(ctx, func(tx spi.WriteTx) error { return kernel.flushPrompt(tx, scopes...) })
	if err == nil {
		kernel.clearPending(scopes...)
	}
	return err
}

func (kernel *BackendKernel) appendPrompt(ctx context.Context, scope string, category int, value any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	record, err := makePromptRecord(scope, category, value)
	if err != nil {
		return err
	}
	kernel.promptMu.Lock()
	defer kernel.promptMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	kernel.pending[scope] = append(kernel.pending[scope], record)
	return nil
}

func (kernel *BackendKernel) AppendSegment(ctx context.Context, value cache.Segment) error {
	return kernel.appendPrompt(ctx, value.PromptScopeID, promptSegment, value)
}
func (kernel *BackendKernel) AppendToolset(ctx context.Context, value cache.ToolsetSnapshot) error {
	return kernel.appendPrompt(ctx, value.PromptScopeID, promptToolset, value)
}
func (kernel *BackendKernel) AppendProviderResponse(ctx context.Context, value cache.ProviderResponseRecord) error {
	return kernel.appendPrompt(ctx, value.PromptScopeID, promptResponse, value)
}
func (kernel *BackendKernel) AppendProviderRequest(ctx context.Context, value cache.ProviderRequestRecord) error {
	return kernel.CheckpointProviderRequest(ctx, nil, nil, value)
}

func (kernel *BackendKernel) Segments(ctx context.Context, scope, provider, model string) ([]cache.Segment, error) {
	kernel.promptMu.Lock()
	defer kernel.promptMu.Unlock()
	batch, err := kernel.readPromptCategory(ctx, scope, promptSegment)
	if err != nil {
		return nil, err
	}
	values, err := decodePromptBatch[cache.Segment](batch)
	if err != nil {
		return nil, err
	}
	var result []cache.Segment
	for _, value := range values {
		if (provider == "" || value.Provider == provider) && (model == "" || value.Model == model) {
			result = append(result, value)
		}
	}
	return result, nil
}

func (kernel *BackendKernel) ActiveToolset(ctx context.Context, scope, provider, model string) (cache.ToolsetSnapshot, bool, error) {
	kernel.promptMu.Lock()
	defer kernel.promptMu.Unlock()
	batch, err := kernel.readPromptCategory(ctx, scope, promptToolset)
	if err != nil {
		return cache.ToolsetSnapshot{}, false, err
	}
	values, err := decodePromptBatch[cache.ToolsetSnapshot](batch)
	if err != nil {
		return cache.ToolsetSnapshot{}, false, err
	}
	for index := len(values) - 1; index >= 0; index-- {
		value := values[index]
		if value.Provider == provider && value.Model == model {
			return value, true, nil
		}
	}
	return cache.ToolsetSnapshot{}, false, nil
}

func (kernel *BackendKernel) ProviderRequests(ctx context.Context, scope string) ([]cache.ProviderRequestRecord, error) {
	kernel.promptMu.Lock()
	defer kernel.promptMu.Unlock()
	batch, err := kernel.readPromptCategory(ctx, scope, promptRequest)
	if err != nil {
		return nil, err
	}
	return decodePromptBatch[cache.ProviderRequestRecord](batch)
}

func (kernel *BackendKernel) ProviderResponses(ctx context.Context, scope string) ([]cache.ProviderResponseRecord, error) {
	kernel.promptMu.Lock()
	defer kernel.promptMu.Unlock()
	batch, err := kernel.readPromptCategory(ctx, scope, promptResponse)
	if err != nil {
		return nil, err
	}
	return decodePromptBatch[cache.ProviderResponseRecord](batch)
}

func (kernel *BackendKernel) LatestPressureAnchor(ctx context.Context, scope, provider, model string) (cache.PressureAnchorState, bool, error) {
	values, err := kernel.ProviderResponses(ctx, scope)
	if err != nil {
		return cache.PressureAnchorState{}, false, err
	}
	for index := len(values) - 1; index >= 0; index-- {
		anchor := values[index].PressureAnchor
		if anchor.WindowInputTokens > 0 && (scope == "" || anchor.PromptScopeID == scope) && (provider == "" || anchor.Provider == provider) && (model == "" || anchor.Model == model) {
			return anchor, true, nil
		}
	}
	return cache.PressureAnchorState{}, false, nil
}

// Candidate facts stay local until the dispatch checkpoint commits. Existing
// pending observations survive a failed checkpoint and are safe to retry.
func (kernel *BackendKernel) CheckpointProviderRequest(ctx context.Context, segments []cache.Segment, toolsets []cache.ToolsetSnapshot, request cache.ProviderRequestRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var candidate promptBatch
	add := func(scope string, category int, value any) error {
		record, err := makePromptRecord(scope, category, value)
		if err == nil {
			candidate = append(candidate, record)
		}
		return err
	}
	for _, value := range segments {
		if err := add(value.PromptScopeID, promptSegment, value); err != nil {
			return err
		}
	}
	for _, value := range toolsets {
		if err := add(value.PromptScopeID, promptToolset, value); err != nil {
			return err
		}
	}
	if err := add(request.PromptScopeID, promptRequest, request); err != nil {
		return err
	}
	kernel.promptMu.Lock()
	defer kernel.promptMu.Unlock()
	selected := map[string]bool{}
	var scopes []string
	for _, record := range candidate {
		if !selected[record.ScopeID] {
			selected[record.ScopeID] = true
			scopes = append(scopes, record.ScopeID)
		}
	}
	err := kernel.CheckpointDomain(ctx, func(tx spi.WriteTx) error {
		var batch promptBatch
		for _, scope := range scopes {
			batch = append(batch, kernel.pending[scope]...)
		}
		batch = append(batch, candidate...)
		return writePromptBatch(tx, batch)
	})
	if err == nil {
		kernel.clearPending(scopes...)
	}
	return err
}

func (kernel *BackendKernel) DeletePromptScopes(ctx context.Context, scopes ...string) error {
	kernel.promptMu.Lock()
	defer kernel.promptMu.Unlock()
	err := kernel.CheckpointDomain(ctx, func(tx spi.WriteTx) error {
		for _, scope := range scopes {
			if err := deletePromptScope(tx, scope); err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil {
		kernel.clearPending(scopes...)
	}
	return err
}

func (kernel *BackendKernel) DeleteRootTree(ctx context.Context, root string) (result sessiontree.DeleteRootTreeResult, err error) {
	kernel.promptMu.Lock()
	defer kernel.promptMu.Unlock()
	err = kernel.UpdateDomain(ctx, func(memory *sessiontree.MemoryRepo, tx spi.WriteTx) error {
		result, err = memory.DeleteRootTree(ctx, root)
		if err != nil {
			return err
		}
		for _, scope := range result.ThreadIDs {
			if err := deletePromptScope(tx, scope); err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil {
		kernel.clearPending(result.ThreadIDs...)
	}
	return result, err
}

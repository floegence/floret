package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/floegence/floret/v3/internal/provider/cache"
	"github.com/floegence/floret/v3/internal/sessiontree"
	"github.com/floegence/floret/v3/internal/storagecodec"
	"github.com/floegence/floret/v3/storage/spi"
)

const backendDomainNamespace = "floret.domain"

var promptStateKey = storagecodec.Tuple(storagecodec.TupleString("prompt"), storagecodec.TupleString("state"))

// BackendKernel is the single domain kernel shared by every public Backend.
type BackendKernel struct {
	*sessiontree.BackendRepo
}

// NewBackendKernel opens all canonical Floret domain state.
func NewBackendKernel(ctx context.Context, backend spi.Backend, policy sessiontree.LeasePolicy, now func() time.Time) (*BackendKernel, error) {
	repo, err := sessiontree.NewBackendRepo(ctx, backend, policy, now)
	if err != nil {
		return nil, err
	}
	kernel := &BackendKernel{BackendRepo: repo}
	if err := repo.UpdateDomain(ctx, func(_ *sessiontree.MemoryRepo, tx spi.WriteTx) error {
		_, found, err := loadPromptState(tx)
		if err != nil {
			return err
		}
		if found {
			return nil
		}
		return savePromptState(tx, cache.NewMemoryStore())
	}); err != nil {
		return nil, err
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
	return kernel.UpdateDomain(ctx, func(_ *sessiontree.MemoryRepo, tx spi.WriteTx) error {
		state, found, err := loadPromptState(tx)
		if err != nil {
			return err
		}
		if !found {
			return errors.New("prompt state is missing")
		}
		if err := mutate(state); err != nil {
			return err
		}
		return savePromptState(tx, state)
	})
}

func (kernel *BackendKernel) viewPrompt(ctx context.Context, read func(*cache.MemoryStore) error) error {
	return kernel.ViewDomain(ctx, func(_ *sessiontree.MemoryRepo, tx spi.ReadTx) error {
		state, found, err := loadPromptState(tx)
		if err != nil {
			return err
		}
		if !found {
			return errors.New("prompt state is missing")
		}
		return read(state)
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
	return kernel.updatePrompt(ctx, func(state *cache.MemoryStore) error { return state.AppendProviderRequest(ctx, value) })
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
	err = kernel.UpdateDomain(ctx, func(memory *sessiontree.MemoryRepo, tx spi.WriteTx) error {
		result, err = memory.DeleteRootTree(ctx, rootThreadID)
		if err != nil {
			return err
		}
		prompt, found, loadErr := loadPromptState(tx)
		if loadErr != nil {
			return loadErr
		}
		if !found {
			return errors.New("prompt state is missing")
		}
		if err := prompt.DeletePromptScopes(ctx, result.ThreadIDs...); err != nil {
			return err
		}
		return savePromptState(tx, prompt)
	})
	return result, err
}

func forkStateKey(operationID string) []byte {
	return storagecodec.Tuple(storagecodec.TupleString("fork"), storagecodec.TupleString(strings.TrimSpace(operationID)))
}

func loadForkOperation(tx spi.ReadTx, operationID string) (ForkOperationRecord, bool, error) {
	encoded, err := tx.Get(backendDomainNamespace, forkStateKey(operationID))
	if errors.Is(err, spi.ErrNotFound) {
		return ForkOperationRecord{}, false, nil
	}
	if err != nil {
		return ForkOperationRecord{}, false, err
	}
	payload, err := storagecodec.DecodeEnvelope(encoded, "fork_operation")
	if err != nil {
		return ForkOperationRecord{}, false, err
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var record ForkOperationRecord
	if err := decoder.Decode(&record); err != nil {
		return ForkOperationRecord{}, false, err
	}
	return record, true, nil
}

func saveForkOperation(tx spi.WriteTx, record ForkOperationRecord) error {
	payload, err := json.Marshal(record)
	if err != nil {
		return err
	}
	encoded, err := storagecodec.EncodeEnvelope("fork_operation", payload)
	if err != nil {
		return err
	}
	return tx.Put(backendDomainNamespace, forkStateKey(record.OperationID), encoded)
}

func (kernel *BackendKernel) PrepareForkOperation(ctx context.Context, record ForkOperationRecord) (result ForkOperationRecord, replayed bool, err error) {
	if err := ValidatePreparedForkOperation(record); err != nil {
		return ForkOperationRecord{}, false, err
	}
	err = kernel.UpdateDomain(ctx, func(memory *sessiontree.MemoryRepo, tx spi.WriteTx) error {
		existing, found, loadErr := loadForkOperation(tx, record.OperationID)
		if loadErr != nil {
			return loadErr
		}
		if found {
			if existing.RequestFingerprint != record.RequestFingerprint || !jsonEqual(existing.Plan, record.Plan) ||
				!slices.Equal(existing.SourceThreadIDs, record.SourceThreadIDs) || !slices.Equal(existing.AuthorityThreadIDs, record.AuthorityThreadIDs) {
				return ErrForkOperationConflict
			}
			result, replayed = existing, true
			return nil
		}
		plan, decodeErr := DecodeForkOperationPlan(record.Plan)
		if decodeErr != nil {
			return decodeErr
		}
		if err := validateForkOperationPlanRecord(record, plan); err != nil {
			return err
		}
		if err := memory.PrepareForkClaim(ctx, record.OperationID, plan.Root.SourceThreadID, ForkOperationPlanNodes(plan)); err != nil {
			return err
		}
		result = cloneForkOperationRecord(record)
		return saveForkOperation(tx, result)
	})
	return result, replayed, err
}

func (kernel *BackendKernel) ForkOperation(ctx context.Context, operationID string) (result ForkOperationRecord, err error) {
	err = kernel.ViewDomain(ctx, func(memory *sessiontree.MemoryRepo, tx spi.ReadTx) error {
		var found bool
		result, found, err = loadForkOperation(tx, operationID)
		if err != nil {
			return err
		}
		if !found {
			return ErrForkOperationNotFound
		}
		if result.State == ForkOperationCompleted {
			plan, decodeErr := DecodeForkOperationPlan(result.Plan)
			if decodeErr != nil {
				return decodeErr
			}
			for _, node := range append([]ForkOperationPlanNode{plan.Root}, plan.TerminalChildren...) {
				if validateErr := memory.ValidateArtifactForkDestination(ctx, node.ArtifactClosure); validateErr != nil {
					return validateErr
				}
			}
		}
		return nil
	})
	return result, err
}

func (kernel *BackendKernel) CommitForkOperation(ctx context.Context, request ForkOperationCommitRequest) (result ForkOperationRecord, replayed bool, err error) {
	if strings.TrimSpace(request.OperationID) == "" || strings.TrimSpace(request.RequestFingerprint) == "" || len(request.Plan) == 0 || !json.Valid(request.Plan) || len(request.Nodes) == 0 || len(request.Result) == 0 || !json.Valid(request.Result) || request.FinishedAt.IsZero() {
		return ForkOperationRecord{}, false, errors.New("fork commit requires operation, fingerprint, complete nodes, result, and finish time")
	}
	err = kernel.UpdateDomain(ctx, func(memory *sessiontree.MemoryRepo, tx spi.WriteTx) error {
		existing, found, loadErr := loadForkOperation(tx, request.OperationID)
		if loadErr != nil {
			return loadErr
		}
		if !found {
			return ErrForkOperationNotFound
		}
		if existing.RequestFingerprint != strings.TrimSpace(request.RequestFingerprint) || !jsonEqual(existing.Plan, request.Plan) {
			return ErrForkOperationConflict
		}
		plan, decodeErr := DecodeForkOperationPlan(existing.Plan)
		if decodeErr != nil {
			return decodeErr
		}
		if err := ValidateForkOperationCommitNodes(plan, request.Nodes); err != nil {
			return err
		}
		if existing.State == ForkOperationCompleted {
			if !jsonEqual(existing.Result, request.Result) {
				return ErrForkOperationConflict
			}
			result, replayed = cloneForkOperationRecord(existing), true
			return nil
		}
		if existing.State != ForkOperationPrepared {
			return ErrForkOperationConflict
		}
		terminal := cloneForkOperationRecord(existing)
		terminal.State, terminal.Result = ForkOperationCompleted, append(json.RawMessage(nil), request.Result...)
		terminal.UpdatedAt, terminal.FinishedAt = request.FinishedAt, request.FinishedAt
		_, err = memory.CommitForkBatch(ctx, existing.OperationID, request.Nodes, func() error { return saveForkOperation(tx, terminal) })
		result = terminal
		return err
	})
	return result, replayed, err
}

func (kernel *BackendKernel) FailForkOperation(ctx context.Context, request ForkOperationFailureRequest) (result ForkOperationRecord, replayed bool, err error) {
	if strings.TrimSpace(request.OperationID) == "" || strings.TrimSpace(request.RequestFingerprint) == "" || strings.TrimSpace(request.ErrorCode) == "" || strings.TrimSpace(request.ErrorMessage) == "" || request.FinishedAt.IsZero() {
		return ForkOperationRecord{}, false, errors.New("fork failure requires operation, fingerprint, typed error, and finish time")
	}
	err = kernel.UpdateDomain(ctx, func(memory *sessiontree.MemoryRepo, tx spi.WriteTx) error {
		existing, found, loadErr := loadForkOperation(tx, request.OperationID)
		if loadErr != nil {
			return loadErr
		}
		if !found {
			return ErrForkOperationNotFound
		}
		if existing.RequestFingerprint != strings.TrimSpace(request.RequestFingerprint) {
			return ErrForkOperationConflict
		}
		if existing.State == ForkOperationFailed {
			if existing.ErrorCode != strings.TrimSpace(request.ErrorCode) || existing.ErrorMessage != strings.TrimSpace(request.ErrorMessage) {
				return ErrForkOperationConflict
			}
			result, replayed = cloneForkOperationRecord(existing), true
			return nil
		}
		if existing.State != ForkOperationPrepared {
			return ErrForkOperationConflict
		}
		terminal := cloneForkOperationRecord(existing)
		terminal.State, terminal.ErrorCode, terminal.ErrorMessage = ForkOperationFailed, strings.TrimSpace(request.ErrorCode), strings.TrimSpace(request.ErrorMessage)
		terminal.UpdatedAt, terminal.FinishedAt = request.FinishedAt, request.FinishedAt
		err = memory.FailForkClaim(existing.OperationID, existing.SourceThreadIDs, existing.AuthorityThreadIDs, func() error { return saveForkOperation(tx, terminal) })
		result = terminal
		return err
	})
	return result, replayed, err
}

package runtime

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	internalstorage "github.com/floegence/floret/v5/internal/storagebridge"
	publicstorage "github.com/floegence/floret/v5/storage"
	"github.com/floegence/floret/v5/storage/spi"
)

var (
	// ErrStorageResetUnsupported reports a backend that cannot atomically clear
	// all Floret-owned opaque records.
	ErrStorageResetUnsupported = errors.New("Floret storage reset is unsupported")
	// ErrStorageResetRequest reports incomplete reset ownership metadata.
	ErrStorageResetRequest = errors.New("invalid Floret storage reset request")
)

// StorageResetRequest binds one destructive maintenance operation to the
// exact environment and host ownership manifest that selected the store.
type StorageResetRequest struct {
	Storage                 publicstorage.Source
	EnvironmentID           string
	OperationID             string
	OwnershipManifestSHA256 string
}

// StorageResetPreflight is safe to present before destructive confirmation.
type StorageResetPreflight struct {
	EnvironmentID           string
	OperationID             string
	OwnershipManifestSHA256 string
	Supported               bool
}

// StorageResetResult reports a newly initialized current-schema store.
type StorageResetResult struct {
	EnvironmentID           string
	OperationID             string
	OwnershipManifestSHA256 string
	PreviousDataRemoved     bool
}

// PreflightStorageReset verifies request ownership metadata and backend reset
// support without changing Floret records. The caller must keep every runtime
// Host for the selected store stopped until reset completes.
func PreflightStorageReset(ctx context.Context, request StorageResetRequest) (StorageResetPreflight, error) {
	normalized, err := normalizeStorageResetRequest(ctx, request)
	if err != nil {
		return StorageResetPreflight{}, err
	}
	backend, err := internalstorage.Open(ctx, internalstorage.Source(normalized.Storage))
	if err != nil {
		return StorageResetPreflight{}, err
	}
	defer backend.Close()
	if _, ok := backend.(spi.MaintenanceResetter); !ok {
		return StorageResetPreflight{}, ErrStorageResetUnsupported
	}
	return StorageResetPreflight{
		EnvironmentID: normalized.EnvironmentID, OperationID: normalized.OperationID,
		OwnershipManifestSHA256: normalized.OwnershipManifestSHA256, Supported: true,
	}, nil
}

// ResetStorage removes all Floret records through the backend owner, then
// initializes and verifies one fresh current-schema store. Repeating the same
// request is idempotent and never imports previous records.
func ResetStorage(ctx context.Context, request StorageResetRequest) (result StorageResetResult, retErr error) {
	normalized, err := normalizeStorageResetRequest(ctx, request)
	if err != nil {
		return StorageResetResult{}, err
	}
	backend, err := internalstorage.Open(ctx, internalstorage.Source(normalized.Storage))
	if err != nil {
		return StorageResetResult{}, err
	}
	resetter, ok := backend.(spi.MaintenanceResetter)
	if !ok {
		_ = backend.Close()
		return StorageResetResult{}, ErrStorageResetUnsupported
	}
	removed, err := resetter.ResetFloretStorage(ctx)
	closeErr := backend.Close()
	if err != nil || closeErr != nil {
		return StorageResetResult{}, errors.Join(err, closeErr)
	}
	fresh, err := Open(ctx, Options{Storage: normalized.Storage})
	if err != nil {
		return StorageResetResult{}, fmt.Errorf("initialize reset Floret storage: %w", err)
	}
	if err := fresh.Shutdown(ctx); err != nil {
		return StorageResetResult{}, fmt.Errorf("verify reset Floret storage: %w", err)
	}
	return StorageResetResult{
		EnvironmentID: normalized.EnvironmentID, OperationID: normalized.OperationID,
		OwnershipManifestSHA256: normalized.OwnershipManifestSHA256,
		PreviousDataRemoved:     removed,
	}, nil
}

func normalizeStorageResetRequest(ctx context.Context, request StorageResetRequest) (StorageResetRequest, error) {
	if ctx == nil {
		return StorageResetRequest{}, fmt.Errorf("%w: context is required", ErrStorageResetRequest)
	}
	if err := ctx.Err(); err != nil {
		return StorageResetRequest{}, err
	}
	request.EnvironmentID = strings.TrimSpace(request.EnvironmentID)
	request.OperationID = strings.TrimSpace(request.OperationID)
	request.OwnershipManifestSHA256 = strings.ToLower(strings.TrimSpace(request.OwnershipManifestSHA256))
	if request.EnvironmentID == "" || request.OperationID == "" || len(request.EnvironmentID) > 256 || len(request.OperationID) > 256 {
		return StorageResetRequest{}, fmt.Errorf("%w: environment_id and operation_id are required", ErrStorageResetRequest)
	}
	digest, ok := strings.CutPrefix(request.OwnershipManifestSHA256, "sha256:")
	if !ok || len(digest) != 64 {
		return StorageResetRequest{}, fmt.Errorf("%w: ownership manifest SHA-256 is required", ErrStorageResetRequest)
	}
	if _, err := hex.DecodeString(digest); err != nil {
		return StorageResetRequest{}, fmt.Errorf("%w: ownership manifest SHA-256 is invalid", ErrStorageResetRequest)
	}
	return request, nil
}

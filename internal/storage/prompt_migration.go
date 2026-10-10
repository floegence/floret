package storage

import (
	"context"
	"errors"

	"github.com/floegence/floret/v7/internal/provider/cache"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/internal/storagecodec"
	"github.com/floegence/floret/v7/storage/spi"
)

// The released v1 global snapshot is read only during startup migration.
type legacyPromptSnapshot struct {
	Version   int                            `json:"version"`
	Segments  []cache.Segment                `json:"segments"`
	Toolsets  []cache.ToolsetSnapshot        `json:"toolsets"`
	Requests  []cache.ProviderRequestRecord  `json:"requests"`
	Responses []cache.ProviderResponseRecord `json:"responses"`
}

func preparePromptRecords(ctx context.Context, tx spi.WriteTx, allowMigration bool, progress StartupProgress) error {
	legacy, legacyErr := tx.Get(backendDomainNamespace, promptStateKey)
	if legacyErr != nil && !errors.Is(legacyErr, spi.ErrNotFound) {
		return legacyErr
	}
	page, err := tx.Scan(spi.ScanRequest{Namespace: promptRecordsNamespace, Limit: 1})
	if err != nil {
		return err
	}
	if len(page.Records) != 0 {
		if legacyErr == nil {
			return errors.Join(sessiontree.ErrAuthorityCorrupt, errors.New("mixed prompt storage formats"))
		}
		return nil
	}
	if !allowMigration {
		return errors.Join(sessiontree.ErrAuthorityCorrupt, errors.New("current prompt storage is missing"))
	}
	if legacyErr == nil {
		if progress != nil {
			progress(StartupPhaseMigrating)
		}
		payload, err := storagecodec.DecodeEnvelope(legacy, "prompt")
		if err != nil {
			return err
		}
		var state legacyPromptSnapshot
		if err := decodePromptJSON(payload, &state); err != nil {
			return err
		}
		if state.Version != 1 {
			return errors.New("unsupported legacy prompt state version")
		}
		batch := make(promptBatch, 0, 256)
		appendRecord := func(scope string, category int, value any) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			record, err := makePromptRecord(scope, category, value)
			if err != nil {
				return err
			}
			batch = append(batch, record)
			if len(batch) < 256 {
				return nil
			}
			err = writePromptBatch(tx, batch)
			batch = batch[:0]
			return err
		}
		for _, value := range state.Segments {
			if err := appendRecord(value.PromptScopeID, promptSegment, value); err != nil {
				return err
			}
		}
		for _, value := range state.Toolsets {
			if err := appendRecord(value.PromptScopeID, promptToolset, value); err != nil {
				return err
			}
		}
		for _, value := range state.Requests {
			if err := appendRecord(value.PromptScopeID, promptRequest, value); err != nil {
				return err
			}
		}
		for _, value := range state.Responses {
			if err := appendRecord(value.PromptScopeID, promptResponse, value); err != nil {
				return err
			}
		}
		if err := writePromptBatch(tx, batch); err != nil {
			return err
		}
	}
	if err := putPromptValue(tx, promptSchemaKey, "prompt_schema", 2); err != nil {
		return err
	}
	if legacyErr == nil {
		return tx.Delete(backendDomainNamespace, promptStateKey)
	}
	return nil
}

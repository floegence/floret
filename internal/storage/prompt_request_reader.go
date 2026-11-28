package storage

import (
	"context"
	"errors"
	"github.com/floegence/floret/v7/internal/provider/cache"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/storage/spi"
)

func (kernel *BackendKernel) LatestProviderRequest(ctx context.Context, scope string) (cache.ProviderRequestRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return cache.ProviderRequestRecord{}, false, err
	}
	kernel.promptMu.Lock()
	defer kernel.promptMu.Unlock()
	pending := kernel.pending[scope]
	for index := len(pending) - 1; index >= 0; index-- {
		if pending[index].Category == promptRequest {
			var result cache.ProviderRequestRecord
			if err := decodePromptJSON(pending[index].Value, &result); err != nil {
				return result, false, err
			}
			if result.PromptScopeID != scope {
				return result, false, sessiontree.ErrAuthorityCorrupt
			}
			return result, true, nil
		}
	}
	var result cache.ProviderRequestRecord
	found := false
	err := kernel.viewPrompt(ctx, scope, func(tx spi.ReadTx) error {
		counters, err := readPromptCounters(tx, scope)
		if err != nil {
			return err
		}
		ordinal := counters.Counts[promptRequest]
		if ordinal == 0 {
			return nil
		}
		result, err = readPromptOrdinal[cache.ProviderRequestRecord](tx, scope, promptRequest, ordinal)
		if err != nil {
			return err
		}
		if result.PromptScopeID != scope {
			return sessiontree.ErrAuthorityCorrupt
		}
		found = true
		return nil
	})
	if errors.Is(err, spi.ErrNotFound) {
		err = errors.Join(sessiontree.ErrAuthorityCorrupt, err)
	}
	return result, found, err
}

//go:build floret_layout_cutover

package storage

import (
	"context"

	"github.com/floegence/floret/v7/internal/provider/cache"
	"github.com/floegence/floret/v7/storage/spi"
)

type OfflineRequestLedger struct {
	cache.Store
	Records spi.ReadTx
}

func (ledger OfflineRequestLedger) LatestModelContextRequest(ctx context.Context, scope, thread, entry string) (cache.ProviderRequestRecord, bool, error) {
	counters, err := readPromptCounters(ledger.Records, scope)
	if err != nil {
		return cache.ProviderRequestRecord{}, false, err
	}
	for ordinal := counters.Counts[promptRequest]; ordinal > 0; ordinal-- {
		if err := ctx.Err(); err != nil {
			return cache.ProviderRequestRecord{}, false, err
		}
		request, err := readRequestOrdinal(ledger.Records, scope, ordinal)
		if err != nil {
			return cache.ProviderRequestRecord{}, false, err
		}
		if request.ThreadID == thread && request.ModelContextEntryID == entry {
			return request, true, nil
		}
	}
	return cache.ProviderRequestRecord{}, false, nil
}

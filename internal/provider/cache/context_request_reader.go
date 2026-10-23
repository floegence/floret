package cache

import "context"

// ModelContextRequestReader locates the last append using an exact managed
// context identity. The request ledger remains the only request authority.
type ModelContextRequestReader interface {
	LatestModelContextRequest(context.Context, string, string, string) (ProviderRequestRecord, bool, error)
}

func LatestModelContextRequest(ctx context.Context, store Store, scope, thread, entry string) (ProviderRequestRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return ProviderRequestRecord{}, false, err
	}
	if reader, ok := store.(ModelContextRequestReader); ok {
		return reader.LatestModelContextRequest(ctx, scope, thread, entry)
	}
	// Released custom stores need only the original Store contract.
	records, err := store.ProviderRequests(ctx, scope)
	if err != nil {
		return ProviderRequestRecord{}, false, err
	}
	for i := len(records) - 1; i >= 0; i-- {
		if records[i].ThreadID == thread && records[i].ModelContextEntryID == entry {
			return records[i], true, nil
		}
	}
	return ProviderRequestRecord{}, false, nil
}

func (s *PreparedStore) LatestModelContextRequest(ctx context.Context, scope, thread, entry string) (ProviderRequestRecord, bool, error) {
	return LatestModelContextRequest(ctx, s.base, scope, thread, entry)
}

func (s *MemoryStore) LatestModelContextRequest(ctx context.Context, scope, thread, entry string) (ProviderRequestRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return ProviderRequestRecord{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	index, found := s.contextRequests[contextRequestIdentity{scope, thread, entry}]
	if !found {
		return ProviderRequestRecord{}, false, nil
	}
	return cloneProviderRequestRecord(s.requests[index]), true, nil
}

type contextRequestIdentity struct{ scope, thread, entry string }

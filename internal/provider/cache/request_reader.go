package cache

import "context"

// LatestRequestReader is an optional internal fast path for current context
// attribution. It returns the last append, preserving transport-attempt order.
type LatestRequestReader interface {
	LatestProviderRequest(context.Context, string) (ProviderRequestRecord, bool, error)
}

func (s *MemoryStore) LatestProviderRequest(ctx context.Context, scope string) (ProviderRequestRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return ProviderRequestRecord{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if index, found := s.latestRequests[scope]; found {
		return cloneProviderRequestRecord(s.requests[index]), true, nil
	}
	return ProviderRequestRecord{}, false, nil
}

// appendRequest runs under the memory store mutex; the index borrows the
// existing immutable record rather than retaining another request copy.
func (s *MemoryStore) appendRequest(request ProviderRequestRecord) {
	if s.latestRequests == nil {
		s.latestRequests = map[string]int{}
	}
	if s.contextRequests == nil {
		s.contextRequests = map[contextRequestIdentity]int{}
	}
	s.contextRequests[contextRequestIdentity{request.PromptScopeID, request.ThreadID, request.ModelContextEntryID}] = len(s.requests)
	s.latestRequests[request.PromptScopeID] = len(s.requests)
	s.requests = append(s.requests, cloneProviderRequestRecord(request))
}

func (s *PreparedStore) LatestProviderRequest(ctx context.Context, scope string) (ProviderRequestRecord, bool, error) {
	return LatestProviderRequest(ctx, s.base, scope)
}
func LatestProviderRequest(ctx context.Context, store Store, scope string) (ProviderRequestRecord, bool, error) {
	if reader, ok := store.(LatestRequestReader); ok {
		return reader.LatestProviderRequest(ctx, scope)
	}
	requests, err := store.ProviderRequests(ctx, scope)
	if err != nil || len(requests) == 0 {
		return ProviderRequestRecord{}, false, err
	}
	return requests[len(requests)-1], true, nil
}

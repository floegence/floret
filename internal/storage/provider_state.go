package storage

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/floegence/floret/internal/provider"
)

var ErrProviderStateNotFound = errors.New("provider state not found")

type ProviderStateRecord struct {
	ThreadID         string
	LeafEntryID      string
	CompatibilityKey string
	State            provider.State
	CreatedByRunID   string
	CreatedByTurnID  string
	UpdatedAt        time.Time
}

type ProviderStateStore interface {
	ProviderState(context.Context, string) (ProviderStateRecord, error)
	PutProviderState(context.Context, ProviderStateRecord) error
	DeleteProviderState(context.Context, string) error
}

type MemoryProviderStateStore struct {
	mu      sync.Mutex
	records map[string]ProviderStateRecord
}

func NewMemoryProviderStateStore() *MemoryProviderStateStore {
	return &MemoryProviderStateStore{records: make(map[string]ProviderStateRecord)}
}

func (s *MemoryProviderStateStore) ProviderState(_ context.Context, threadID string) (ProviderStateRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[threadID]
	if !ok {
		return ProviderStateRecord{}, ErrProviderStateNotFound
	}
	record.State = *provider.CloneState(&record.State)
	return record, nil
}

func (s *MemoryProviderStateStore) PutProviderState(_ context.Context, record ProviderStateRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	record.State = *provider.CloneState(&record.State)
	s.records[record.ThreadID] = record
	return nil
}

func (s *MemoryProviderStateStore) DeleteProviderState(_ context.Context, threadID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, threadID)
	return nil
}

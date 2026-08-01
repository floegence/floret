package florettest

import (
	"errors"
	"sync"

	"github.com/floegence/floret/v3/identity"
)

// IDSourceOptions configures exact deterministic lifecycle identity sequences.
type IDSourceOptions struct {
	ThreadIDs []identity.ThreadID
	TurnIDs   []identity.TurnID
	RunIDs    []identity.RunID
}

// IDSource is a deterministic, concurrency-safe runtime identity source for
// tests. Production hosts must use the cryptographic source installed by
// runtime.Open.
type IDSource struct {
	mu      sync.Mutex
	threads []identity.ThreadID
	turns   []identity.TurnID
	runs    []identity.RunID
}

// NewIDSource returns a test-only identity source. Callers must provide every
// identity their test will use.
func NewIDSource(options IDSourceOptions) *IDSource {
	return &IDSource{
		threads: append([]identity.ThreadID(nil), options.ThreadIDs...),
		turns:   append([]identity.TurnID(nil), options.TurnIDs...),
		runs:    append([]identity.RunID(nil), options.RunIDs...),
	}
}

// NewThreadID returns the next configured Thread identity.
func (source *IDSource) NewThreadID() (identity.ThreadID, error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	if len(source.threads) == 0 {
		return "", errors.New("florettest thread identity sequence is exhausted")
	}
	value := source.threads[0]
	source.threads = source.threads[1:]
	return value, nil
}

// NewTurnID returns the next configured Turn identity.
func (source *IDSource) NewTurnID() (identity.TurnID, error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	if len(source.turns) == 0 {
		return "", errors.New("florettest turn identity sequence is exhausted")
	}
	value := source.turns[0]
	source.turns = source.turns[1:]
	return value, nil
}

// NewRunID returns the next configured Run identity.
func (source *IDSource) NewRunID() (identity.RunID, error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	if len(source.runs) == 0 {
		return "", errors.New("florettest run identity sequence is exhausted")
	}
	value := source.runs[0]
	source.runs = source.runs[1:]
	return value, nil
}

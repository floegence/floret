package cache

import (
	"context"
	"strings"
)

// executionStore belongs to one sequential Engine execution. It retains only
// the first surface and model invariant for that execution's turn, not history.
// A new execution (including restart and retry) reads canonical facts afresh.
type executionStore struct {
	Store
	scope, turn string
	loaded      bool
	facts       turnRequestFacts
}

func NewExecutionStore(base Store, scope, turn string) Store {
	return &executionStore{Store: base, scope: scope, turn: turn}
}

type turnRequestFacts struct {
	surface                TurnSurfaceSnapshot
	surfaceFound           bool
	provider, model        string
	modelFound, modelDrift bool
}

func (facts *turnRequestFacts) observe(request ProviderRequestRecord, turn string) {
	surfaceID := strings.TrimSpace(request.TurnID)
	if surfaceID == "" {
		surfaceID = strings.TrimSpace(request.RunID)
	}
	if !facts.surfaceFound && surfaceID == turn {
		facts.surface, facts.surfaceFound = cloneTurnSurface(request.TurnSurface), true
	}
	if request.TurnID == turn {
		if facts.modelFound && (facts.provider != request.Provider || facts.model != request.Model) {
			facts.modelDrift = true
		}
		if !facts.modelFound {
			facts.provider, facts.model, facts.modelFound = request.Provider, request.Model, true
		}
	}
}

type turnRequestReader interface {
	turnRequestFacts(context.Context, string, string) (turnRequestFacts, error)
}

func readTurnRequestFacts(ctx context.Context, store Store, scope, turn string) (turnRequestFacts, error) {
	if err := ctx.Err(); err != nil {
		return turnRequestFacts{}, err
	}
	if reader, ok := store.(turnRequestReader); ok {
		return reader.turnRequestFacts(ctx, scope, turn)
	}
	requests, err := store.ProviderRequests(ctx, scope)
	if err != nil {
		return turnRequestFacts{}, err
	}
	var facts turnRequestFacts
	for _, request := range requests {
		if err := ctx.Err(); err != nil {
			return turnRequestFacts{}, err
		}
		facts.observe(request, turn)
	}
	return facts, nil
}

func (s *executionStore) turnRequestFacts(ctx context.Context, scope, turn string) (turnRequestFacts, error) {
	if scope != s.scope || turn != s.turn {
		return readTurnRequestFacts(ctx, s.Store, scope, turn)
	}
	if !s.loaded {
		facts, err := readTurnRequestFacts(ctx, s.Store, scope, turn)
		if err != nil {
			return turnRequestFacts{}, err
		}
		s.facts, s.loaded = facts, true
	}
	return s.facts, nil
}

func (s *PreparedStore) turnRequestFacts(ctx context.Context, scope, turn string) (turnRequestFacts, error) {
	return readTurnRequestFacts(ctx, s.base, scope, turn)
}

func (s *executionStore) LatestProviderRequest(ctx context.Context, scope string) (ProviderRequestRecord, bool, error) {
	return LatestProviderRequest(ctx, s.Store, scope)
}

func (s *executionStore) observe(request ProviderRequestRecord) {
	if s.loaded && request.PromptScopeID == s.scope {
		s.facts.observe(request, s.turn)
	}
}

func (s *executionStore) AppendProviderRequest(ctx context.Context, request ProviderRequestRecord) error {
	if err := s.Store.AppendProviderRequest(ctx, request); err != nil {
		return err
	}
	s.observe(request)
	return nil
}

func (s *executionStore) CheckpointProviderRequest(ctx context.Context, segments []Segment, toolsets []ToolsetSnapshot, request ProviderRequestRecord) error {
	// PreparedStore already owns the Store-only adapter for test/custom stores;
	// production stores retain their atomic checkpoint boundary.
	prepared := &PreparedStore{base: s.Store, segments: segments, toolsets: toolsets}
	if err := prepared.AppendProviderRequest(ctx, request); err != nil {
		return err
	}
	s.observe(request)
	return nil
}

func (s *executionStore) DeletePromptScopes(ctx context.Context, scopes ...string) error {
	if deleter, ok := s.Store.(Deleter); ok {
		if err := deleter.DeletePromptScopes(ctx, scopes...); err != nil {
			return err
		}
	}
	for _, scope := range scopes {
		if strings.TrimSpace(scope) == s.scope {
			s.loaded, s.facts = false, turnRequestFacts{}
		}
	}
	return nil
}

// The latest append usually supplies the render boundary too. Returning to an
// older surface must still locate its last matching generation and lineage;
// the intervening request cannot prove that surface's segment-prefix invariant.
func latestRenderRequest(ctx context.Context, store Store, scope string, latest ProviderRequestRecord, generation int, lineage string) (ProviderRequestRecord, bool, error) {
	if latest.CompactionGeneration == generation && latest.RenderLineageKey == lineage {
		return latest, true, nil
	}
	requests, err := store.ProviderRequests(ctx, scope)
	if err != nil {
		return ProviderRequestRecord{}, false, err
	}
	for index := len(requests) - 1; index >= 0; index-- {
		request := requests[index]
		if request.CompactionGeneration == generation && request.RenderLineageKey == lineage {
			return request, true, nil
		}
	}
	return ProviderRequestRecord{}, false, nil
}

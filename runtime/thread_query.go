package runtime

import (
	"context"
	"errors"
	"github.com/floegence/floret/v7/threadquery"
)

// ThreadQueryReader is an optional canonical index capability. Hosts authorize
// Scope before each query, page, read and aggregate. Tool arguments and confirmed
// output are private host facts; do not add them to ordinary presentation DTOs.
type ThreadQueryReader interface {
	Query(context.Context, threadquery.Scope, threadquery.QueryInput) (threadquery.Page, error)
	Read(context.Context, threadquery.Scope, threadquery.ReadInput) (threadquery.ReadPage, error)
	Aggregate(context.Context, threadquery.Scope, threadquery.AggregateInput) (threadquery.AggregatePage, error)
}

var _ ThreadQueryReader = (*threadRuntimeService)(nil)

func (s *threadRuntimeService) queryRepo() (ThreadQueryReader, error) {
	if err := s.host.available(); err != nil {
		return nil, err
	}
	repo, ok := s.host.store.repo.(ThreadQueryReader)
	if !ok {
		return nil, errors.New("canonical thread index is unavailable for this repository")
	}
	return repo, nil
}

func (s *threadRuntimeService) Query(ctx context.Context, scope threadquery.Scope, in threadquery.QueryInput) (threadquery.Page, error) {
	repo, err := s.queryRepo()
	if err != nil {
		return threadquery.Page{}, err
	}
	return repo.Query(ctx, scope, in)
}
func (s *threadRuntimeService) Read(ctx context.Context, scope threadquery.Scope, in threadquery.ReadInput) (threadquery.ReadPage, error) {
	repo, err := s.queryRepo()
	if err != nil {
		return threadquery.ReadPage{}, err
	}
	return repo.Read(ctx, scope, in)
}
func (s *threadRuntimeService) Aggregate(ctx context.Context, scope threadquery.Scope, in threadquery.AggregateInput) (threadquery.AggregatePage, error) {
	repo, err := s.queryRepo()
	if err != nil {
		return threadquery.AggregatePage{}, err
	}
	return repo.Aggregate(ctx, scope, in)
}

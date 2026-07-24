package runtime

import (
	"fmt"

	"github.com/floegence/floret/internal/sessiontree"
	"github.com/floegence/floret/internal/storage"
)

type SQLiteStoreOption func(*sqliteStoreOptions)

type sqliteStoreOptions struct {
	leasePolicy sessiontree.LeasePolicy
}

func WithSQLiteStoreLeasePolicy(policy StoreLeasePolicy) SQLiteStoreOption {
	return func(options *sqliteStoreOptions) {
		options.leasePolicy = policy.internal()
	}
}

func (p StoreLeasePolicy) Validate() error {
	if err := p.internal().Validate(); err != nil {
		return fmt.Errorf("invalid floret store lease policy: %w", err)
	}
	return nil
}

func (p StoreLeasePolicy) internal() sessiontree.LeasePolicy {
	return sessiontree.LeasePolicy{
		TTL:                p.TTL,
		RenewInterval:      p.RenewInterval,
		ClockSkewAllowance: p.ClockSkewAllowance,
	}
}

func publicStoreLeasePolicy(policy sessiontree.LeasePolicy) StoreLeasePolicy {
	return StoreLeasePolicy{
		TTL:                policy.TTL,
		RenewInterval:      policy.RenewInterval,
		ClockSkewAllowance: policy.ClockSkewAllowance,
	}
}

func resolveSQLiteStoreOptions(options []SQLiteStoreOption) (sqliteStoreOptions, error) {
	configured := sqliteStoreOptions{leasePolicy: sessiontree.DefaultLeasePolicy}
	for _, option := range options {
		if option != nil {
			option(&configured)
		}
	}
	if err := configured.leasePolicy.Validate(); err != nil {
		return sqliteStoreOptions{}, fmt.Errorf("invalid floret sqlite store options: %w", err)
	}
	return configured, nil
}

func mapStoreSchemaMigrationSources(sources []storage.StoreSchemaMigrationSource) []StoreSchemaMigrationSource {
	mapped := make([]StoreSchemaMigrationSource, len(sources))
	for index, source := range sources {
		mapped[index] = StoreSchemaMigrationSource{
			Identity: StoreSchemaIdentity{
				Version:     source.Identity.Version,
				Fingerprint: source.Identity.Fingerprint,
			},
			Requirement: StoreSchemaMigrationRequirement(source.Requirement),
		}
	}
	return mapped
}

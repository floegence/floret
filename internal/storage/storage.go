package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/floegence/floret/internal/provider/cache"
	"github.com/floegence/floret/internal/sessiontree"
)

var ErrMetadataNotFound = errors.New("storage metadata not found")

type StoreSchemaIdentity struct {
	Version     string
	Fingerprint string
}

type StoreSchemaMigrationRequirement string

const (
	StoreSchemaMigrationRequirementNone               StoreSchemaMigrationRequirement = "none"
	StoreSchemaMigrationRequirementQuiescentAuthority StoreSchemaMigrationRequirement = "quiescent_authority"
)

type StoreSchemaMigrationSource struct {
	Identity    StoreSchemaIdentity
	Requirement StoreSchemaMigrationRequirement
}

// UnsupportedStoreSchemaError reports an exact schema contract mismatch. Store
// open must return this error without modifying the observed database.
type UnsupportedStoreSchemaError struct {
	Observed   StoreSchemaIdentity
	Current    StoreSchemaIdentity
	Migratable []StoreSchemaMigrationSource
}

func (e *UnsupportedStoreSchemaError) Error() string {
	if e == nil {
		return "unsupported store schema"
	}
	sources := make([]string, 0, len(e.Migratable))
	for _, source := range e.Migratable {
		sources = append(sources, fmt.Sprintf(
			"version %q fingerprint %q requirement %q",
			source.Identity.Version,
			source.Identity.Fingerprint,
			source.Requirement,
		))
	}
	return fmt.Sprintf(
		"unsupported store schema version %q fingerprint %q; current is version %q fingerprint %q; migratable sources are [%s]",
		e.Observed.Version,
		e.Observed.Fingerprint,
		e.Current.Version,
		e.Current.Fingerprint,
		strings.Join(sources, ", "),
	)
}

type StoreLeasePolicyMismatchError struct {
	Configured sessiontree.LeasePolicy
	Persisted  sessiontree.LeasePolicy
}

func (e *StoreLeasePolicyMismatchError) Error() string {
	if e == nil {
		return "store lease policy mismatch"
	}
	return fmt.Sprintf("store lease policy mismatch: configured=%+v persisted=%+v", e.Configured, e.Persisted)
}

type MetadataRecord struct {
	Namespace string
	ID        string
	CreatedAt time.Time
	UpdatedAt time.Time
	Data      json.RawMessage
}

type MetadataStore interface {
	PutMetadata(context.Context, MetadataRecord) error
	Metadata(context.Context, string, string) (MetadataRecord, error)
	ListMetadata(context.Context, string) ([]MetadataRecord, error)
	DeleteMetadata(context.Context, string, string) error
}

type Store interface {
	sessiontree.Repo
	cache.Store
	MetadataStore
	ForkOperationStore
	sessiontree.ProviderStateStore
	Close() error
}

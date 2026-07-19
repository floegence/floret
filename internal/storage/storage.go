package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/floegence/floret/internal/provider/cache"
	"github.com/floegence/floret/internal/sessiontree"
)

var ErrMetadataNotFound = errors.New("storage metadata not found")

// UnsupportedStoreSchemaError reports an exact schema contract mismatch. Store
// open must return this error without modifying the observed database.
type UnsupportedStoreSchemaError struct {
	ObservedVersion        string
	ObservedFingerprint    string
	CurrentVersion         string
	CurrentFingerprint     string
	PredecessorVersion     string
	PredecessorFingerprint string
}

func (e *UnsupportedStoreSchemaError) Error() string {
	if e == nil {
		return "unsupported store schema"
	}
	return fmt.Sprintf(
		"unsupported store schema version %q fingerprint %q; accepted current is version %q fingerprint %q and empty predecessor is version %q fingerprint %q",
		e.ObservedVersion,
		e.ObservedFingerprint,
		e.CurrentVersion,
		e.CurrentFingerprint,
		e.PredecessorVersion,
		e.PredecessorFingerprint,
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

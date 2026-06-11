package storage

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/floegence/floret/provider/cache"
	"github.com/floegence/floret/sessiontree"
)

var ErrMetadataNotFound = errors.New("storage metadata not found")

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

type DeleteThreadDataRequest struct {
	ThreadID           string
	PromptScopeIDs     []string
	MetadataNamespaces []string
}

type Store interface {
	sessiontree.Repo
	cache.Store
	MetadataStore
	DeleteThreadData(context.Context, DeleteThreadDataRequest) error
	Close() error
}

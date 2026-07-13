package storage

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/floegence/floret/internal/provider/cache"
	"github.com/floegence/floret/internal/sessiontree"
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

type DeleteThreadTreeDataRequest struct {
	RootThreadID   string
	ThreadIDs      []string
	PromptScopeIDs []string
}

type Store interface {
	sessiontree.Repo
	cache.Store
	MetadataStore
	DeleteThreadTreeData(context.Context, DeleteThreadTreeDataRequest) error
	Close() error
}

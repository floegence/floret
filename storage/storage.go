package storage

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/floegence/floret/promptcache"
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

type DeleteSessionRequest struct {
	SessionID          string
	PromptScopeIDs     []string
	MetadataNamespaces []string
}

type Store interface {
	sessiontree.Repo
	promptcache.Store
	MetadataStore
	DeleteSession(context.Context, DeleteSessionRequest) error
	Close() error
}

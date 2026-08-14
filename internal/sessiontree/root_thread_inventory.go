package sessiontree

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/floegence/floret/v4/internal/storagecodec"
	"github.com/floegence/floret/v4/storage/spi"
)

const rootThreadInventoryVersion = 1

var backendRootThreadInventoryKey = storagecodec.Tuple(
	storagecodec.TupleString("sessiontree"), storagecodec.TupleString("root_thread_inventory"),
)

type persistedRootThreadInventory struct {
	Version int                       `json:"version"`
	Items   []RootThreadInventoryItem `json:"items"`
}

// RootThreadInventoryItem is one canonical root thread and its active path.
type RootThreadInventoryItem struct {
	Meta ThreadMeta
	Path []Entry
	// ProjectionFingerprint validates the cached inventory payload; it is not a
	// lifecycle revision or host replay cursor.
	ProjectionFingerprint [32]byte `json:"-"`
}

// ListRootThreadInventory reads a bounded root-thread page and the lifecycle
// facts needed to project each item inside one backend snapshot.
func (repo *BackendRepo) ListRootThreadInventory(ctx context.Context, opts ListThreadsOptions) ([]RootThreadInventoryItem, error) {
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if repo.domainDirty && repo.domainMemory != nil {
		items, err := repo.domainMemory.rootThreadInventoryLocked()
		if err != nil {
			return nil, err
		}
		if err := attachRootThreadInventoryProjectionFingerprints(items); err != nil {
			return nil, err
		}
		repo.rootInventoryItems = cloneRootThreadInventoryItems(items)
		return applyRootThreadInventoryOptions(cloneRootThreadInventoryItems(items), opts), nil
	}
	var out []RootThreadInventoryItem
	err := repo.backend.View(ctx, func(tx spi.ReadTx) error {
		encoded, err := tx.Get(backendDomainNamespace, backendRootThreadInventoryKey)
		if err != nil {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
		if bytes.Equal(encoded, repo.rootInventoryEncoded) && repo.rootInventoryItems != nil {
			out = applyRootThreadInventoryOptions(cloneRootThreadInventoryItems(repo.rootInventoryItems), opts)
			return nil
		}
		items, err := decodeRootThreadInventory(encoded)
		if err != nil {
			return err
		}
		repo.rootInventoryEncoded = bytes.Clone(encoded)
		repo.rootInventoryItems = cloneRootThreadInventoryItems(items)
		out = applyRootThreadInventoryOptions(cloneRootThreadInventoryItems(items), opts)
		return nil
	})
	return out, err
}

func encodeRootThreadInventory(memory *MemoryRepo) ([]byte, error) {
	memory.mu.Lock()
	items, err := memory.rootThreadInventoryLocked()
	memory.mu.Unlock()
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(persistedRootThreadInventory{Version: rootThreadInventoryVersion, Items: items})
	if err != nil {
		return nil, err
	}
	return storagecodec.EncodeEnvelope("sessiontree-root-thread-inventory", payload)
}

// EncodeBackendRootThreadInventoryRecord derives the backend record committed
// beside one canonical session-tree state. The package is internal; this
// helper exists so the explicit physical migration can write the same record
// as BackendRepo without duplicating its storage identity or encoding.
func EncodeBackendRootThreadInventoryRecord(memory *MemoryRepo) (key, value []byte, err error) {
	value, err = encodeRootThreadInventory(memory)
	if err != nil {
		return nil, nil, err
	}
	return bytes.Clone(backendRootThreadInventoryKey), value, nil
}

func (r *MemoryRepo) rootThreadInventoryLocked() ([]RootThreadInventoryItem, error) {
	metas := make([]ThreadMeta, 0, len(r.threads))
	for _, meta := range r.threads {
		if strings.TrimSpace(meta.ParentThreadID) == "" {
			metas = append(metas, meta)
		}
	}
	metas = ApplyThreadListOptions(metas, ListThreadsOptions{IncludeArchived: true, RootOnly: true})
	items := make([]RootThreadInventoryItem, 0, len(metas))
	for _, meta := range metas {
		path, err := pathLocked(r.threads, r.entries, meta.ID, meta.LeafID)
		if err != nil {
			return nil, err
		}
		items = append(items, RootThreadInventoryItem{Meta: meta, Path: path})
	}
	return items, nil
}

func decodeRootThreadInventory(encoded []byte) ([]RootThreadInventoryItem, error) {
	payload, err := storagecodec.DecodeEnvelope(encoded, "sessiontree-root-thread-inventory")
	if err != nil {
		return nil, errors.Join(ErrAuthorityCorrupt, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var inventory persistedRootThreadInventory
	if err := decoder.Decode(&inventory); err != nil {
		return nil, errors.Join(ErrAuthorityCorrupt, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("root thread inventory contains trailing data")
		}
		return nil, errors.Join(ErrAuthorityCorrupt, err)
	}
	if inventory.Version != rootThreadInventoryVersion || inventory.Items == nil {
		return nil, errors.Join(ErrAuthorityCorrupt, errors.New("unsupported root thread inventory"))
	}
	if err := validateRootThreadInventory(inventory.Items); err != nil {
		return nil, errors.Join(ErrAuthorityCorrupt, err)
	}
	if err := attachRootThreadInventoryProjectionFingerprints(inventory.Items); err != nil {
		return nil, err
	}
	return inventory.Items, nil
}

func attachRootThreadInventoryProjectionFingerprints(items []RootThreadInventoryItem) error {
	for index := range items {
		fingerprint, err := rootThreadInventoryProjectionFingerprint(items[index])
		if err != nil {
			return err
		}
		items[index].ProjectionFingerprint = fingerprint
	}
	return nil
}

func rootThreadInventoryProjectionFingerprint(item RootThreadInventoryItem) ([32]byte, error) {
	encoded, err := json.Marshal(item)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(encoded), nil
}

func validateRootThreadInventory(items []RootThreadInventoryItem) error {
	seen := make(map[string]struct{}, len(items))
	for index, item := range items {
		threadID := strings.TrimSpace(item.Meta.ID)
		if threadID == "" || item.Meta.ParentThreadID != "" {
			return fmt.Errorf("root thread inventory item %d has invalid identity", index)
		}
		if _, duplicate := seen[threadID]; duplicate {
			return fmt.Errorf("root thread inventory contains duplicate thread %q", threadID)
		}
		seen[threadID] = struct{}{}
		for entryIndex, entry := range item.Path {
			if entry.ThreadID != threadID || ValidateEntryIntegrity(entry) != nil ||
				(entryIndex == 0 && entry.ParentID != "") ||
				(entryIndex > 0 && entry.ParentID != item.Path[entryIndex-1].ID) {
				return fmt.Errorf("root thread inventory path for %q is invalid", threadID)
			}
		}
		if (len(item.Path) == 0) != (strings.TrimSpace(item.Meta.LeafID) == "") ||
			(len(item.Path) > 0 && item.Path[len(item.Path)-1].ID != item.Meta.LeafID) {
			return fmt.Errorf("root thread inventory leaf for %q is invalid", threadID)
		}
		if index > 0 {
			previous := items[index-1].Meta
			if item.Meta.CreatedAt.After(previous.CreatedAt) ||
				(item.Meta.CreatedAt.Equal(previous.CreatedAt) && item.Meta.ID < previous.ID) {
				return errors.New("root thread inventory order is invalid")
			}
		}
	}
	return nil
}

func applyRootThreadInventoryOptions(items []RootThreadInventoryItem, opts ListThreadsOptions) []RootThreadInventoryItem {
	items = slices.Clone(items)
	out := items[:0]
	for _, item := range items {
		if item.Meta.Archived && !opts.IncludeArchived {
			continue
		}
		if !threadAfterListCursor(item.Meta, opts) {
			continue
		}
		out = append(out, item)
		if opts.Limit > 0 && len(out) >= opts.Limit {
			break
		}
	}
	return out
}

func cloneRootThreadInventoryItems(items []RootThreadInventoryItem) []RootThreadInventoryItem {
	if items == nil {
		return nil
	}
	out := make([]RootThreadInventoryItem, len(items))
	for index, item := range items {
		item.Path = cloneEntries(item.Path)
		out[index] = item
	}
	return out
}

func (r *MemoryRepo) ListRootThreadInventory(_ context.Context, opts ListThreadsOptions) ([]RootThreadInventoryItem, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out, err := r.rootThreadInventoryLocked()
	if err != nil {
		return nil, err
	}
	out = applyRootThreadInventoryOptions(out, opts)
	if err := attachRootThreadInventoryProjectionFingerprints(out); err != nil {
		return nil, err
	}
	return out, nil
}

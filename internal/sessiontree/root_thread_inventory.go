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

	"github.com/floegence/floret/v6/internal/storagecodec"
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
	if repo == nil || ctx == nil {
		return nil, errors.New("root thread inventory requires context and repository")
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if repo.domainMemory == nil {
		return nil, errors.New("session-tree state is missing")
	}
	items, err := repo.domainMemory.rootThreadInventoryLocked()
	if err != nil {
		return nil, err
	}
	if err := attachRootThreadInventoryProjectionFingerprints(items); err != nil {
		return nil, err
	}
	return applyRootThreadInventoryOptions(items, opts), nil
}

func encodeRootThreadInventory(memory *MemoryRepo) ([]byte, error) {
	memory.mu.Lock()
	items, err := memory.rootThreadInventoryLocked()
	memory.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return encodeRootThreadInventoryItems(items)
}

func encodeRootThreadInventoryItems(items []RootThreadInventoryItem) ([]byte, error) {
	payload, err := json.Marshal(persistedRootThreadInventory{Version: rootThreadInventoryVersion, Items: items})
	if err != nil {
		return nil, err
	}
	return storagecodec.EncodeEnvelope("sessiontree-root-thread-inventory", payload)
}

func verifyLegacyUTF8RootThreadInventory(encoded []byte, memory *MemoryRepo) error {
	payload, err := storagecodec.DecodeEnvelope(encoded, "sessiontree-root-thread-inventory")
	if err != nil {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var inventory persistedRootThreadInventory
	if err := decoder.Decode(&inventory); err != nil {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("root thread inventory contains trailing data")
		}
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	if inventory.Version != rootThreadInventoryVersion || inventory.Items == nil {
		return errors.Join(ErrAuthorityCorrupt, errors.New("unsupported root thread inventory"))
	}
	for itemIndex := range inventory.Items {
		for entryIndex := range inventory.Items[itemIndex].Path {
			if _, err := repairLegacyUTF8EntryProjection(&inventory.Items[itemIndex].Path[entryIndex]); err != nil {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
		}
	}
	if err := validateRootThreadInventory(inventory.Items); err != nil {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	got, err := encodeRootThreadInventoryItems(inventory.Items)
	if err != nil {
		return err
	}
	want, err := encodeRootThreadInventory(memory)
	if err != nil {
		return err
	}
	if !bytes.Equal(got, want) {
		return errors.Join(ErrAuthorityCorrupt, errors.New("legacy root thread inventory does not match canonical domain"))
	}
	return nil
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

package sessiontree

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/floegence/floret/internal/session/artifact"
)

var (
	ErrArtifactNotFound           = errors.New("session tree artifact not found")
	ErrSubAgentNotFound           = errors.New("session tree subagent not found")
	ErrSubAgentParentRequired     = errors.New("session tree subagent requires parent authority")
	ErrUnsupportedStoreCapability = errors.New("session tree store capability is unsupported")
)

type ArtifactReadRequest struct {
	ParentThreadID string
	ThreadID       string
	ArtifactID     string
}

type ArtifactContent struct {
	Ref  artifact.Ref
	Text string
}

type ArtifactClosureRequest struct {
	SourceThreadID      string
	DestinationThreadID string
	EntryIDs            []string
}

type ArtifactAuthorityRepo interface {
	ReadArtifact(context.Context, ArtifactReadRequest) (ArtifactContent, error)
	ArtifactClosure(context.Context, ArtifactClosureRequest) (artifact.Closure, error)
}

func artifactRecordKey(threadID, artifactID string) string {
	return strings.TrimSpace(threadID) + "\x00" + strings.TrimSpace(artifactID)
}

func (r *MemoryRepo) ReadArtifact(_ context.Context, req ArtifactReadRequest) (ArtifactContent, error) {
	threadID := strings.TrimSpace(req.ThreadID)
	artifactID := strings.TrimSpace(req.ArtifactID)
	if threadID == "" || artifactID == "" {
		return ArtifactContent{}, errors.New("artifact read requires thread and artifact identities")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.validateArtifactReadAuthorityLocked(req.ParentThreadID, threadID); err != nil {
		return ArtifactContent{}, err
	}
	record, ok := r.artifacts[artifactRecordKey(threadID, artifactID)]
	if !ok {
		return ArtifactContent{}, ErrArtifactNotFound
	}
	if record.ThreadID != threadID || record.Ref.ID != artifactID {
		return ArtifactContent{}, ErrAuthorityCorrupt
	}
	if err := r.validateArtifactRecordLocked(record); err != nil {
		return ArtifactContent{}, err
	}
	return ArtifactContent{Ref: record.Ref, Text: record.Text}, nil
}

func (r *MemoryRepo) ArtifactClosure(_ context.Context, req ArtifactClosureRequest) (artifact.Closure, error) {
	sourceID := strings.TrimSpace(req.SourceThreadID)
	destinationID := strings.TrimSpace(req.DestinationThreadID)
	if sourceID == "" || destinationID == "" {
		return artifact.Closure{}, errors.New("artifact closure requires source and destination")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	meta, ok := r.threads[sourceID]
	if !ok {
		if tombstone, deleted := r.tombstones[sourceID]; deleted {
			if err := validateMemoryArtifactTombstone(tombstone, sourceID); err != nil {
				return artifact.Closure{}, err
			}
			return artifact.Closure{}, ErrThreadDeleted
		}
		return artifact.Closure{}, ErrThreadNotFound
	}
	if err := validateMemoryArtifactThread(meta, sourceID); err != nil {
		return artifact.Closure{}, err
	}
	entries := make([]Entry, 0, len(req.EntryIDs))
	for _, entryID := range req.EntryIDs {
		entry, ok := memoryEntryByID(r.entries[sourceID], strings.TrimSpace(entryID))
		if !ok {
			return artifact.Closure{}, ErrEntryNotFound
		}
		entries = append(entries, entry)
	}
	return r.artifactClosureLocked(sourceID, destinationID, entries)
}

func (r *MemoryRepo) validateArtifactReadAuthorityLocked(parentThreadID, threadID string) error {
	parentThreadID = strings.TrimSpace(parentThreadID)
	threadID = strings.TrimSpace(threadID)
	if parentThreadID == "" {
		meta, ok := r.threads[threadID]
		if !ok {
			if tombstone, deleted := r.tombstones[threadID]; deleted {
				if err := validateMemoryArtifactTombstone(tombstone, threadID); err != nil {
					return err
				}
				return ErrThreadDeleted
			}
			return ErrThreadNotFound
		}
		if err := validateMemoryArtifactThread(meta, threadID); err != nil {
			return err
		}
		if strings.TrimSpace(meta.ParentThreadID) != "" {
			return ErrSubAgentParentRequired
		}
		return nil
	}
	parent, ok := r.threads[parentThreadID]
	if !ok {
		if tombstone, deleted := r.tombstones[parentThreadID]; deleted {
			if err := validateMemoryArtifactTombstone(tombstone, parentThreadID); err != nil {
				return err
			}
			return ErrThreadDeleted
		}
		return ErrSubAgentNotFound
	}
	if err := validateMemoryArtifactThread(parent, parentThreadID); err != nil {
		return err
	}
	if threadID == parentThreadID {
		return ErrSubAgentNotFound
	}
	if meta, ok := r.threads[threadID]; ok {
		if err := validateMemoryArtifactThread(meta, threadID); err != nil {
			return err
		}
		return r.validateLiveDescendantLocked(parentThreadID, meta)
	}
	tombstone, deleted := r.tombstones[threadID]
	if !deleted {
		return ErrSubAgentNotFound
	}
	if err := validateMemoryArtifactTombstone(tombstone, threadID); err != nil {
		return err
	}
	descendant, err := r.tombstoneDescendsFromLocked(parentThreadID, tombstone)
	if err != nil {
		return err
	}
	if descendant {
		return ErrThreadDeleted
	}
	return ErrSubAgentNotFound
}

func (r *MemoryRepo) validateLiveDescendantLocked(parentThreadID string, current ThreadMeta) error {
	seen := map[string]struct{}{current.ID: {}}
	for {
		ancestorID := strings.TrimSpace(current.ParentThreadID)
		if ancestorID == "" {
			return ErrSubAgentNotFound
		}
		if ancestorID == parentThreadID {
			return nil
		}
		if _, duplicate := seen[ancestorID]; duplicate {
			return ErrAuthorityCorrupt
		}
		seen[ancestorID] = struct{}{}
		ancestor, ok := r.threads[ancestorID]
		if !ok {
			return ErrAuthorityCorrupt
		}
		if err := validateMemoryArtifactThread(ancestor, ancestorID); err != nil {
			return err
		}
		current = ancestor
	}
}

func (r *MemoryRepo) tombstoneDescendsFromLocked(parentThreadID string, current ThreadTombstone) (bool, error) {
	rootThreadID := current.RootThreadID
	seen := map[string]struct{}{current.ThreadID: {}}
	for {
		ancestorID := strings.TrimSpace(current.ParentThreadID)
		if ancestorID == "" {
			if current.ThreadID != rootThreadID {
				return false, ErrAuthorityCorrupt
			}
			return false, nil
		}
		if ancestorID == parentThreadID {
			parentRootThreadID, err := r.liveArtifactRootThreadIDLocked(parentThreadID)
			if err != nil {
				return false, err
			}
			if rootThreadID != parentRootThreadID {
				return false, ErrAuthorityCorrupt
			}
			return true, nil
		}
		if _, duplicate := seen[ancestorID]; duplicate {
			return false, ErrAuthorityCorrupt
		}
		seen[ancestorID] = struct{}{}
		if tombstone, ok := r.tombstones[ancestorID]; ok {
			if err := validateMemoryArtifactTombstone(tombstone, ancestorID); err != nil || tombstone.RootThreadID != rootThreadID {
				return false, ErrAuthorityCorrupt
			}
			current = tombstone
			continue
		}
		meta, ok := r.threads[ancestorID]
		if !ok {
			return false, ErrAuthorityCorrupt
		}
		if err := validateMemoryArtifactThread(meta, ancestorID); err != nil {
			return false, err
		}
		current = ThreadTombstone{ThreadID: meta.ID, ParentThreadID: meta.ParentThreadID}
	}
}

func (r *MemoryRepo) liveArtifactRootThreadIDLocked(threadID string) (string, error) {
	seen := map[string]struct{}{}
	for {
		if _, duplicate := seen[threadID]; duplicate {
			return "", ErrAuthorityCorrupt
		}
		seen[threadID] = struct{}{}
		meta, ok := r.threads[threadID]
		if !ok {
			return "", ErrAuthorityCorrupt
		}
		if err := validateMemoryArtifactThread(meta, threadID); err != nil {
			return "", err
		}
		parentThreadID := strings.TrimSpace(meta.ParentThreadID)
		if parentThreadID == "" {
			return threadID, nil
		}
		threadID = parentThreadID
	}
}

func validateMemoryArtifactThread(meta ThreadMeta, expectedThreadID string) error {
	if meta.ID != strings.TrimSpace(expectedThreadID) || meta.ID != strings.TrimSpace(meta.ID) || ValidateThreadMetaAuthority(meta) != nil {
		return ErrAuthorityCorrupt
	}
	return nil
}

func validateMemoryArtifactTombstone(tombstone ThreadTombstone, expectedThreadID string) error {
	threadID := strings.TrimSpace(expectedThreadID)
	parentThreadID := strings.TrimSpace(tombstone.ParentThreadID)
	rootThreadID := strings.TrimSpace(tombstone.RootThreadID)
	if tombstone.ThreadID != threadID || tombstone.ThreadID != strings.TrimSpace(tombstone.ThreadID) ||
		tombstone.ParentThreadID != parentThreadID || tombstone.RootThreadID != rootThreadID ||
		rootThreadID == "" || tombstone.DeletedAt.IsZero() || parentThreadID == threadID ||
		(parentThreadID == "" && rootThreadID != threadID) || (parentThreadID != "" && rootThreadID == threadID) {
		return ErrAuthorityCorrupt
	}
	return nil
}

func (r *MemoryRepo) artifactClosureLocked(sourceID, destinationID string, entries []Entry) (artifact.Closure, error) {
	byID := make(map[string]artifact.ManifestItem)
	for _, entry := range entries {
		if entry.ThreadID != sourceID || entry.Message.ToolResult == nil || entry.Message.ToolResult.FullOutput == nil {
			continue
		}
		ref := *entry.Message.ToolResult.FullOutput
		if err := artifact.ValidateRef(ref); err != nil {
			return artifact.Closure{}, ErrAuthorityCorrupt
		}
		record, ok := r.artifacts[artifactRecordKey(sourceID, ref.ID)]
		if !ok || record.ThreadID != sourceID || record.Ref.ID != ref.ID || record.CanonicalEntryID != entry.ID || !reflect.DeepEqual(record.Ref, ref) {
			return artifact.Closure{}, ErrAuthorityCorrupt
		}
		if err := r.validateArtifactRecordLocked(record); err != nil {
			return artifact.Closure{}, err
		}
		refFingerprint, err := artifact.RefFingerprint(ref)
		if err != nil {
			return artifact.Closure{}, ErrAuthorityCorrupt
		}
		item := artifact.ManifestItem{
			SourceEntryID: record.CanonicalEntryID, ArtifactID: ref.ID, Ref: ref,
			RefFingerprint: refFingerprint, PayloadSHA256: artifact.TextSHA256(record.Text),
		}
		if existing, duplicate := byID[ref.ID]; duplicate && !reflect.DeepEqual(existing, item) {
			return artifact.Closure{}, ErrAuthorityCorrupt
		}
		byID[ref.ID] = item
	}
	items := make([]artifact.ManifestItem, 0, len(byID))
	for _, item := range byID {
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ArtifactID < items[j].ArtifactID })
	fingerprint, err := artifact.ClosureFingerprint(sourceID, destinationID, items)
	if err != nil {
		return artifact.Closure{}, err
	}
	return artifact.Closure{SourceThreadID: sourceID, DestinationThreadID: destinationID, Items: items, Fingerprint: fingerprint}, nil
}

func (r *MemoryRepo) validateArtifactClosureLocked(sourceID, destinationID string, entries []Entry, expected artifact.Closure) error {
	if err := artifact.ValidateClosure(expected); err != nil || expected.SourceThreadID != sourceID || expected.DestinationThreadID != destinationID {
		return ErrStaleAuthority
	}
	actual, err := r.artifactClosureLocked(sourceID, destinationID, entries)
	if err != nil {
		return err
	}
	if !artifact.EqualClosure(actual, expected) {
		return ErrStaleAuthority
	}
	return nil
}

func (r *MemoryRepo) stageArtifactForkLocked(closure artifact.Closure, sourceToDestinationEntryID map[string]string, now time.Time) (map[string]artifact.Record, error) {
	staged := make(map[string]artifact.Record, len(closure.Items))
	for _, item := range closure.Items {
		destinationEntryID := strings.TrimSpace(sourceToDestinationEntryID[item.SourceEntryID])
		if destinationEntryID == "" {
			return nil, ErrAuthorityCorrupt
		}
		source, ok := r.artifacts[artifactRecordKey(closure.SourceThreadID, item.ArtifactID)]
		if !ok || source.ThreadID != closure.SourceThreadID || source.Ref.ID != item.ArtifactID || source.CanonicalEntryID != item.SourceEntryID || !reflect.DeepEqual(source.Ref, item.Ref) || artifact.TextSHA256(source.Text) != item.PayloadSHA256 {
			return nil, ErrAuthorityCorrupt
		}
		if err := r.validateArtifactRecordLocked(source); err != nil {
			return nil, err
		}
		key := artifactRecordKey(closure.DestinationThreadID, item.ArtifactID)
		if _, collision := r.artifacts[key]; collision {
			return nil, ErrAuthorityCorrupt
		}
		staged[key] = artifact.Record{
			ThreadID: closure.DestinationThreadID, Ref: item.Ref, Text: source.Text,
			CanonicalEntryID: destinationEntryID, CreatedAt: now,
		}
	}
	return staged, nil
}

// ValidateArtifactForkDestination verifies the complete copied artifact set
// without relying on the source thread still being live.
func (r *MemoryRepo) ValidateArtifactForkDestination(_ context.Context, closure artifact.Closure) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.validateArtifactForkDestinationLocked(closure)
}

func (r *MemoryRepo) validateArtifactForkDestinationLocked(closure artifact.Closure) error {
	if err := artifact.ValidateClosure(closure); err != nil {
		return ErrAuthorityCorrupt
	}
	if meta, ok := r.threads[closure.DestinationThreadID]; !ok {
		if tombstone, deleted := r.tombstones[closure.DestinationThreadID]; deleted {
			if err := validateMemoryArtifactTombstone(tombstone, closure.DestinationThreadID); err != nil {
				return err
			}
			return ErrThreadDeleted
		}
		return ErrThreadNotFound
	} else if err := validateMemoryArtifactThread(meta, closure.DestinationThreadID); err != nil {
		return err
	}
	for _, item := range closure.Items {
		record, ok := r.artifacts[artifactRecordKey(closure.DestinationThreadID, item.ArtifactID)]
		if !ok || record.ThreadID != closure.DestinationThreadID || record.Ref.ID != item.ArtifactID || !reflect.DeepEqual(record.Ref, item.Ref) || artifact.TextSHA256(record.Text) != item.PayloadSHA256 {
			return ErrAuthorityCorrupt
		}
		if err := r.validateArtifactRecordLocked(record); err != nil {
			return err
		}
	}
	return nil
}

func (r *MemoryRepo) validateArtifactRecordLocked(record artifact.Record) error {
	if record.ThreadID == "" || record.CanonicalEntryID == "" || record.CreatedAt.IsZero() || artifact.ValidateRef(record.Ref) != nil || record.Ref.SHA256 != artifact.TextSHA256(record.Text) ||
		record.Ref.SizeBytes != int64(len(record.Text)) {
		return ErrAuthorityCorrupt
	}
	entry, ok := memoryEntryByID(r.entries[record.ThreadID], record.CanonicalEntryID)
	if !ok || entry.ThreadID != record.ThreadID || entry.Type != EntryToolResult || entry.Message.ToolResult == nil || entry.Message.ToolResult.FullOutput == nil ||
		!reflect.DeepEqual(*entry.Message.ToolResult.FullOutput, record.Ref) {
		return ErrAuthorityCorrupt
	}
	return nil
}

func memoryEntryByID(entries []Entry, entryID string) (Entry, bool) {
	for _, entry := range entries {
		if entry.ID == entryID {
			return entry, true
		}
	}
	return Entry{}, false
}

func (r *FileRepo) ReadArtifact(context.Context, ArtifactReadRequest) (ArtifactContent, error) {
	return ArtifactContent{}, ErrUnsupportedStoreCapability
}

func (r *FileRepo) ArtifactClosure(context.Context, ArtifactClosureRequest) (artifact.Closure, error) {
	return artifact.Closure{}, ErrUnsupportedStoreCapability
}

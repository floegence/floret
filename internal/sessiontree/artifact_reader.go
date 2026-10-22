package sessiontree

import (
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/floegence/floret/v7/internal/session/artifact"
)

// Read errors remain distinct from missing records and authorization outcomes.
// All accessors refer to one immutable snapshot or transaction.
type artifactAuthorityReader struct {
	thread    func(string) (ThreadMeta, bool, error)
	tombstone func(string) (ThreadTombstone, bool, error)
	entry     func(string, string) (Entry, bool, error)
	record    func(string, string) (artifact.Record, bool, error)
}

func (r *MemoryRepo) artifactReaderLocked() artifactAuthorityReader {
	return artifactAuthorityReader{
		thread:    func(id string) (ThreadMeta, bool, error) { value, ok := r.threads[id]; return value, ok, nil },
		tombstone: func(id string) (ThreadTombstone, bool, error) { value, ok := r.tombstones[id]; return value, ok, nil },
		entry: func(threadID, entryID string) (Entry, bool, error) {
			value, ok := memoryEntryByID(r.entries[threadID], entryID)
			return value, ok, nil
		},
		record: func(threadID, artifactID string) (artifact.Record, bool, error) {
			value, ok := r.artifacts[artifactRecordKey(threadID, artifactID)]
			return value, ok, nil
		},
	}
}

func (r artifactAuthorityReader) validateArtifactReadAuthorityLocked(parentThreadID, threadID string) error {
	parentThreadID = strings.TrimSpace(parentThreadID)
	threadID = strings.TrimSpace(threadID)
	if parentThreadID == "" {
		meta, ok, err := r.thread(threadID)
		if err != nil {
			return err
		}
		if !ok {
			if tombstone, deleted, err := r.tombstone(threadID); err != nil {
				return err
			} else if deleted {
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
	parent, ok, err := r.thread(parentThreadID)
	if err != nil {
		return err
	}
	if !ok {
		if tombstone, deleted, err := r.tombstone(parentThreadID); err != nil {
			return err
		} else if deleted {
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
	if meta, ok, err := r.thread(threadID); err != nil {
		return err
	} else if ok {
		if err := validateMemoryArtifactThread(meta, threadID); err != nil {
			return err
		}
		return r.validateLiveDescendantLocked(parentThreadID, meta)
	}
	tombstone, deleted, err := r.tombstone(threadID)
	if err != nil {
		return err
	}
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

func (r artifactAuthorityReader) validateLiveDescendantLocked(parentThreadID string, current ThreadMeta) error {
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
		ancestor, ok, err := r.thread(ancestorID)
		if err != nil {
			return err
		}
		if !ok {
			return ErrAuthorityCorrupt
		}
		if err := validateMemoryArtifactThread(ancestor, ancestorID); err != nil {
			return err
		}
		current = ancestor
	}
}

func (r artifactAuthorityReader) tombstoneDescendsFromLocked(parentThreadID string, current ThreadTombstone) (bool, error) {
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
		if tombstone, ok, err := r.tombstone(ancestorID); err != nil {
			return false, err
		} else if ok {
			if err := validateMemoryArtifactTombstone(tombstone, ancestorID); err != nil || tombstone.RootThreadID != rootThreadID {
				return false, ErrAuthorityCorrupt
			}
			current = tombstone
			continue
		}
		meta, ok, err := r.thread(ancestorID)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, ErrAuthorityCorrupt
		}
		if err := validateMemoryArtifactThread(meta, ancestorID); err != nil {
			return false, err
		}
		current = ThreadTombstone{ThreadID: meta.ID, ParentThreadID: meta.ParentThreadID}
	}
}

func (r artifactAuthorityReader) liveArtifactRootThreadIDLocked(threadID string) (string, error) {
	seen := map[string]struct{}{}
	for {
		if _, duplicate := seen[threadID]; duplicate {
			return "", ErrAuthorityCorrupt
		}
		seen[threadID] = struct{}{}
		meta, ok, err := r.thread(threadID)
		if err != nil {
			return "", err
		}
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

func (r artifactAuthorityReader) artifactClosureLocked(sourceID, destinationID string, entries []Entry) (artifact.Closure, error) {
	byID := make(map[string]artifact.ManifestItem)
	for _, entry := range entries {
		if entry.ThreadID != sourceID || entry.Message.ToolResult == nil || entry.Message.ToolResult.FullOutput == nil {
			continue
		}
		ref := *entry.Message.ToolResult.FullOutput
		if err := artifact.ValidateRef(ref); err != nil {
			return artifact.Closure{}, ErrAuthorityCorrupt
		}
		record, ok, err := r.record(sourceID, ref.ID)
		if err != nil {
			return artifact.Closure{}, err
		}
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

func (r artifactAuthorityReader) validateArtifactForkDestinationLocked(closure artifact.Closure) error {
	if err := artifact.ValidateClosure(closure); err != nil {
		return ErrAuthorityCorrupt
	}
	if meta, ok, err := r.thread(closure.DestinationThreadID); err != nil {
		return err
	} else if !ok {
		if tombstone, deleted, err := r.tombstone(closure.DestinationThreadID); err != nil {
			return err
		} else if deleted {
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
		record, ok, err := r.record(closure.DestinationThreadID, item.ArtifactID)
		if err != nil {
			return err
		}
		if !ok || record.ThreadID != closure.DestinationThreadID || record.Ref.ID != item.ArtifactID || !reflect.DeepEqual(record.Ref, item.Ref) || artifact.TextSHA256(record.Text) != item.PayloadSHA256 {
			return ErrAuthorityCorrupt
		}
		if err := r.validateArtifactRecordLocked(record); err != nil {
			return err
		}
	}
	return nil
}

func (r artifactAuthorityReader) validateArtifactRecordLocked(record artifact.Record) error {
	if record.ThreadID == "" || record.CanonicalEntryID == "" || record.CreatedAt.IsZero() || artifact.ValidateRef(record.Ref) != nil || record.Ref.SHA256 != artifact.TextSHA256(record.Text) ||
		record.Ref.SizeBytes != int64(len(record.Text)) {
		return ErrAuthorityCorrupt
	}
	entry, ok, err := r.entry(record.ThreadID, record.CanonicalEntryID)
	if err != nil {
		return err
	}
	if !ok || entry.ThreadID != record.ThreadID || entry.Type != EntryToolResult || entry.Message.ToolResult == nil || entry.Message.ToolResult.FullOutput == nil ||
		!reflect.DeepEqual(*entry.Message.ToolResult.FullOutput, record.Ref) {
		return ErrAuthorityCorrupt
	}
	return nil
}

func (r artifactAuthorityReader) stageArtifactForkLocked(closure artifact.Closure, sourceToDestinationEntryID map[string]string, now time.Time) (map[string]artifact.Record, error) {
	staged := make(map[string]artifact.Record, len(closure.Items))
	for _, item := range closure.Items {
		destinationEntryID := strings.TrimSpace(sourceToDestinationEntryID[item.SourceEntryID])
		if destinationEntryID == "" {
			return nil, ErrAuthorityCorrupt
		}
		source, ok, err := r.record(closure.SourceThreadID, item.ArtifactID)
		if err != nil {
			return nil, err
		}
		if !ok || source.ThreadID != closure.SourceThreadID || source.Ref.ID != item.ArtifactID || source.CanonicalEntryID != item.SourceEntryID || !reflect.DeepEqual(source.Ref, item.Ref) || artifact.TextSHA256(source.Text) != item.PayloadSHA256 {
			return nil, ErrAuthorityCorrupt
		}
		if err := r.validateArtifactRecordLocked(source); err != nil {
			return nil, err
		}
		key := artifactRecordKey(closure.DestinationThreadID, item.ArtifactID)
		if _, collision, err := r.record(closure.DestinationThreadID, item.ArtifactID); err != nil {
			return nil, err
		} else if collision {
			return nil, ErrAuthorityCorrupt
		}
		staged[key] = artifact.Record{
			ThreadID: closure.DestinationThreadID, Ref: item.Ref, Text: source.Text,
			CanonicalEntryID: destinationEntryID, CreatedAt: now,
		}
	}
	return staged, nil
}

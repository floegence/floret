package sessiontree

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/floegence/floret/v7/internal/session/artifact"
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
	return r.artifactReaderLocked().validateArtifactReadAuthorityLocked(parentThreadID, threadID)
}

func (r *MemoryRepo) validateLiveDescendantLocked(parentThreadID string, current ThreadMeta) error {
	return r.artifactReaderLocked().validateLiveDescendantLocked(parentThreadID, current)
}

func (r *MemoryRepo) tombstoneDescendsFromLocked(parentThreadID string, current ThreadTombstone) (bool, error) {
	return r.artifactReaderLocked().tombstoneDescendsFromLocked(parentThreadID, current)
}

func (r *MemoryRepo) liveArtifactRootThreadIDLocked(threadID string) (string, error) {
	return r.artifactReaderLocked().liveArtifactRootThreadIDLocked(threadID)
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
	return r.artifactReaderLocked().artifactClosureLocked(sourceID, destinationID, entries)
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
	return r.artifactReaderLocked().stageArtifactForkLocked(closure, sourceToDestinationEntryID, now)
}

// ValidateArtifactForkDestination verifies the complete copied artifact set
// without relying on the source thread still being live.
func (r *MemoryRepo) ValidateArtifactForkDestination(_ context.Context, closure artifact.Closure) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.validateArtifactForkDestinationLocked(closure)
}

func (r *MemoryRepo) validateArtifactForkDestinationLocked(closure artifact.Closure) error {
	return r.artifactReaderLocked().validateArtifactForkDestinationLocked(closure)
}

func (r *MemoryRepo) validateArtifactRecordLocked(record artifact.Record) error {
	return r.artifactReaderLocked().validateArtifactRecordLocked(record)
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

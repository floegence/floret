package sessiontree

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

// ThreadLifecycle is the durable canonical lifecycle of a thread identity.
// A deleted identity is represented by a tombstone rather than by absence.
type ThreadLifecycle string

const (
	ThreadLifecycleOpen    ThreadLifecycle = "open"
	ThreadLifecycleClosing ThreadLifecycle = "closing"
	ThreadLifecycleClosed  ThreadLifecycle = "closed"
	ThreadLifecycleDeleted ThreadLifecycle = "deleted"
)

func (l ThreadLifecycle) Valid() bool {
	switch l {
	case ThreadLifecycleOpen, ThreadLifecycleClosing, ThreadLifecycleClosed, ThreadLifecycleDeleted:
		return true
	default:
		return false
	}
}

func normalizeThreadLifecycle(meta ThreadMeta) (ThreadLifecycle, error) {
	lifecycle := meta.Lifecycle
	if lifecycle == "" {
		return ThreadLifecycleOpen, nil
	}
	if !lifecycle.Valid() {
		return "", fmt.Errorf("invalid thread lifecycle %q", lifecycle)
	}
	return lifecycle, nil
}

func (m ThreadMeta) CanonicalLifecycle() (ThreadLifecycle, error) {
	return normalizeThreadLifecycle(m)
}

func (m ThreadMeta) IsClosed() bool {
	lifecycle, err := normalizeThreadLifecycle(m)
	return err == nil && lifecycle == ThreadLifecycleClosed
}

func (m ThreadMeta) IsClosing() bool {
	lifecycle, err := normalizeThreadLifecycle(m)
	return err == nil && lifecycle == ThreadLifecycleClosing
}

func canonicalThreadLifecycle(meta ThreadMeta) (ThreadLifecycle, error) {
	lifecycle, err := normalizeThreadLifecycle(meta)
	if err != nil {
		return "", err
	}
	if lifecycle == ThreadLifecycleDeleted {
		return "", ErrThreadDeleted
	}
	return lifecycle, nil
}

func lifecycleRejectsWrite(meta ThreadMeta) error {
	lifecycle, err := canonicalThreadLifecycle(meta)
	if err != nil {
		return err
	}
	switch lifecycle {
	case ThreadLifecycleClosing:
		return ErrSubAgentClosing
	case ThreadLifecycleClosed:
		return ErrThreadClosed
	default:
		return nil
	}
}

// ThreadTombstone retains identity provenance after queryable Agent state is
// deleted. It is intentionally not a ThreadMeta and is never returned as a
// normal thread read.
type ThreadTombstone struct {
	ThreadID            string
	RootThreadID        string
	ParentThreadID      string
	OriginRequestKey    string
	OriginFingerprint   string
	DeleteRequestKey    string
	DeleteFingerprint   string
	ForkedFromThreadID  string
	ForkedFromEntryID   string
	LegacyCreateIntent  string `json:"create_intent_id,omitempty"`
	LegacyForkRequestID string `json:"fork_operation_id,omitempty"`
	LegacyForkNodeID    string `json:"fork_operation_node_id,omitempty"`
	DeletedAt           time.Time
}

type DeleteRootTreeResult struct {
	ThreadIDs []string
	Replayed  bool
}

type ThreadTombstoneRepo interface {
	ThreadTombstone(context.Context, string) (ThreadTombstone, error)
}

type ThreadOrigin struct {
	Thread    *ThreadMeta
	Tombstone *ThreadTombstone
}

// ThreadOriginRepo resolves canonical create/fork identity without a receipt
// ledger. A tombstone match prevents a deleted thread from being recreated.
type ThreadOriginRepo interface {
	ThreadOrigin(context.Context, string) (ThreadOrigin, error)
}

// ThreadDeleteRepo is the v4 canonical tombstone boundary. The request key is
// stored on the tombstone itself rather than in a separate receipt ledger.
type ThreadDeleteRepo interface {
	DeleteRootTreeWithRequest(context.Context, string, string, string) (DeleteRootTreeResult, error)
}

func (r *MemoryRepo) ThreadTombstone(_ context.Context, threadID string) (ThreadTombstone, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	tombstone, ok := r.tombstones[strings.TrimSpace(threadID)]
	if !ok {
		return ThreadTombstone{}, ErrThreadNotFound
	}
	return tombstone, nil
}

func (r *MemoryRepo) ThreadOrigin(_ context.Context, requestKey string) (ThreadOrigin, error) {
	requestKey = strings.TrimSpace(requestKey)
	if requestKey == "" {
		return ThreadOrigin{}, errors.New("thread origin request key is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var result ThreadOrigin
	for _, meta := range r.threads {
		if meta.OriginRequestKey != requestKey {
			continue
		}
		if result.Thread != nil || result.Tombstone != nil {
			return ThreadOrigin{}, ErrAuthorityCorrupt
		}
		copy := meta
		result.Thread = &copy
	}
	for _, tombstone := range r.tombstones {
		if tombstone.OriginRequestKey != requestKey {
			continue
		}
		if result.Thread != nil || result.Tombstone != nil {
			return ThreadOrigin{}, ErrAuthorityCorrupt
		}
		copy := tombstone
		result.Tombstone = &copy
	}
	if result.Thread == nil && result.Tombstone == nil {
		return ThreadOrigin{}, ErrThreadNotFound
	}
	return result, nil
}

func (r *MemoryRepo) DeleteRootTree(_ context.Context, rootThreadID string) (DeleteRootTreeResult, error) {
	return r.deleteRootTree(rootThreadID, "", "")
}

func (r *MemoryRepo) DeleteRootTreeWithRequest(_ context.Context, rootThreadID, requestKey, fingerprint string) (DeleteRootTreeResult, error) {
	requestKey = strings.TrimSpace(requestKey)
	fingerprint = strings.TrimSpace(fingerprint)
	if requestKey == "" || fingerprint == "" {
		return DeleteRootTreeResult{}, errors.New("delete request key and fingerprint are required")
	}
	return r.deleteRootTree(rootThreadID, requestKey, fingerprint)
}

func (r *MemoryRepo) deleteRootTree(rootThreadID, requestKey, fingerprint string) (DeleteRootTreeResult, error) {
	rootThreadID = strings.TrimSpace(rootThreadID)
	if rootThreadID == "" {
		return DeleteRootTreeResult{}, errors.New("root thread id is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if tombstone, ok := r.tombstones[rootThreadID]; ok && tombstone.ThreadID == rootThreadID && tombstone.RootThreadID == rootThreadID {
		if requestKey != "" && (tombstone.DeleteRequestKey != requestKey || tombstone.DeleteFingerprint != fingerprint) {
			return DeleteRootTreeResult{}, ErrRequestConflict
		}
		threadIDs := make([]string, 0)
		for threadID, candidate := range r.tombstones {
			if candidate.RootThreadID == rootThreadID {
				threadIDs = append(threadIDs, threadID)
			}
		}
		slices.Sort(threadIDs)
		return DeleteRootTreeResult{ThreadIDs: threadIDs, Replayed: true}, nil
	}
	if _, ok := r.threads[rootThreadID]; !ok {
		return DeleteRootTreeResult{}, ErrThreadNotFound
	}
	threadIDs, err := threadTreeIDsLocked(r.threads, rootThreadID)
	if err != nil {
		return DeleteRootTreeResult{}, err
	}
	for _, threadID := range threadIDs {
		if _, ok := r.threads[threadID]; !ok {
			return DeleteRootTreeResult{}, ErrThreadNotFound
		}
	}
	now := r.now().UTC()
	deletedSet := make(map[string]struct{}, len(threadIDs))
	for _, threadID := range threadIDs {
		deletedSet[threadID] = struct{}{}
	}
	for _, threadID := range threadIDs {
		meta := r.threads[threadID]
		rootID := rootThreadID
		r.tombstones[threadID] = ThreadTombstone{
			ThreadID: threadID, RootThreadID: rootID, ParentThreadID: meta.ParentThreadID,
			OriginRequestKey: meta.OriginRequestKey, OriginFingerprint: meta.OriginFingerprint,
			DeleteRequestKey: requestKey, DeleteFingerprint: fingerprint,
			ForkedFromThreadID: meta.ForkedFromThreadID, ForkedFromEntryID: meta.ForkedFromEntryID,
			DeletedAt: now,
		}
		delete(r.threads, threadID)
		r.deleteIndexedEntriesLocked(threadID)
		delete(r.todos, threadID)
		delete(r.providerStates, threadID)
		for key, record := range r.artifacts {
			if record.ThreadID == threadID {
				delete(r.artifacts, key)
			}
		}
	}
	for attemptID, attempt := range r.effectAttempts {
		if _, deleted := deletedSet[attempt.Invocation.ThreadID]; deleted {
			delete(r.effectAttemptByInvocation, effectInvocationKey(attempt.Invocation))
			delete(r.effectAttempts, attemptID)
		}
	}
	return DeleteRootTreeResult{ThreadIDs: append([]string(nil), threadIDs...)}, nil
}

func threadTreeIDsLocked(threads map[string]ThreadMeta, rootThreadID string) ([]string, error) {
	list := make([]ThreadMeta, 0, len(threads))
	for _, meta := range threads {
		list = append(list, meta)
	}
	if err := ValidateThreadAuthorityGraph(list); err != nil {
		return nil, err
	}
	children := map[string][]string{}
	for _, meta := range list {
		if parent := strings.TrimSpace(meta.ParentThreadID); parent != "" {
			children[parent] = append(children[parent], meta.ID)
		}
	}
	if _, ok := threads[rootThreadID]; !ok {
		return nil, ErrThreadNotFound
	}
	for parent := range children {
		slices.Sort(children[parent])
	}
	ids := make([]string, 0, len(list))
	var walk func(string)
	walk = func(id string) {
		ids = append(ids, id)
		for _, child := range children[id] {
			walk(child)
		}
	}
	walk(rootThreadID)
	return ids, nil
}

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
	CreateIntentID      string
	ForkOperationID     string
	ForkOperationNodeID string
	ForkedFromThreadID  string
	ForkedFromEntryID   string
	DeletedAt           time.Time
}

type CreateRootRequest struct {
	ThreadID        string
	CreateIntentID  string
	ContractVersion string
	Meta            ThreadMeta
}

type CreateRootResult struct {
	Thread   ThreadMeta
	Replayed bool
}

type DeleteRootTreeResult struct {
	ThreadIDs []string
	Replayed  bool
}

type ThreadTombstoneRepo interface {
	ThreadTombstone(context.Context, string) (ThreadTombstone, error)
}

type RootAuthorityRepo interface {
	ThreadTombstoneRepo
	CreateRoot(context.Context, CreateRootRequest) (CreateRootResult, error)
	DeleteRootTree(context.Context, string) (DeleteRootTreeResult, error)
}

type ThreadAuthoritySnapshot struct {
	Thread           ThreadMeta
	Lease            *TurnLease
	ClaimOperationID string
	LeaseGeneration  int64
}

type ThreadAuthorityInspectionRepo interface {
	InspectThreadAuthority(context.Context, string) (ThreadAuthoritySnapshot, error)
}

type SubAgentThreadAuthoritySnapshot struct {
	Parent ThreadMeta
	Child  ThreadAuthoritySnapshot
}

type SubAgentThreadAuthorityInspectionRepo interface {
	InspectSubAgentThreadAuthority(context.Context, string, string) (SubAgentThreadAuthoritySnapshot, error)
}

type rootCreateLedger struct {
	ThreadID        string
	CreateIntentID  string
	Fingerprint     string
	ContractVersion string
}

func validateCreateRootRequest(req CreateRootRequest) error {
	if strings.TrimSpace(req.ThreadID) == "" || strings.TrimSpace(req.CreateIntentID) == "" {
		return errors.New("root create requires thread and create intent identities")
	}
	if strings.TrimSpace(req.ContractVersion) == "" {
		return errors.New("root create contract version is required")
	}
	if strings.TrimSpace(req.Meta.ID) != strings.TrimSpace(req.ThreadID) {
		return fmt.Errorf("root create thread identity mismatch")
	}
	if strings.TrimSpace(req.Meta.ParentThreadID) != "" || strings.TrimSpace(req.Meta.ParentTurnID) != "" ||
		strings.TrimSpace(req.Meta.ForkedFromThreadID) != "" || strings.TrimSpace(req.Meta.ForkedFromEntryID) != "" ||
		strings.TrimSpace(req.Meta.ForkOperationID) != "" || strings.TrimSpace(req.Meta.ForkOperationNodeID) != "" {
		return fmt.Errorf("root create requires an independent root thread")
	}
	return ValidateThreadMetaAuthority(req.Meta)
}

// ValidateCreateRootRequest validates the exact root-create contract.
func ValidateCreateRootRequest(req CreateRootRequest) error {
	return validateCreateRootRequest(req)
}

func createRootFingerprint(req CreateRootRequest) string {
	return StableHash(strings.Join([]string{
		strings.TrimSpace(req.ThreadID), strings.TrimSpace(req.CreateIntentID), strings.TrimSpace(req.ContractVersion),
	}, "\x00"))
}

// CreateRootFingerprint is the stable identity used by root-create replay.
func CreateRootFingerprint(req CreateRootRequest) string {
	return createRootFingerprint(req)
}

func (r *MemoryRepo) CreateRoot(_ context.Context, req CreateRootRequest) (CreateRootResult, error) {
	if err := validateCreateRootRequest(req); err != nil {
		return CreateRootResult{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	fingerprint := createRootFingerprint(req)
	intentID := strings.TrimSpace(req.CreateIntentID)
	threadID := strings.TrimSpace(req.ThreadID)
	if existing, ok := r.rootCreateIntents[intentID]; ok {
		if existing.ThreadID != threadID || existing.Fingerprint != fingerprint {
			return CreateRootResult{}, ErrRequestConflict
		}
		if meta, ok := r.threads[threadID]; ok {
			if !createRootReplayMatches(meta, threadID) {
				return CreateRootResult{}, ErrRequestConflict
			}
			return CreateRootResult{Thread: meta, Replayed: true}, nil
		}
		if _, ok := r.tombstones[threadID]; ok {
			return CreateRootResult{}, ErrThreadDeleted
		}
		return CreateRootResult{}, ErrAuthorityCorrupt
	}
	if _, ok := r.tombstones[threadID]; ok {
		return CreateRootResult{}, ErrThreadDeleted
	}
	for _, existing := range r.rootCreateIntents {
		if existing.ThreadID == threadID {
			return CreateRootResult{}, ErrRequestConflict
		}
	}
	if _, exists := r.threads[threadID]; exists {
		return CreateRootResult{}, ErrRequestConflict
	}
	meta := req.Meta
	meta.ID = threadID
	meta.Lifecycle = ThreadLifecycleOpen
	created, err := r.createThreadLocked(meta)
	if err != nil {
		return CreateRootResult{}, err
	}
	r.rootCreateIntents[intentID] = rootCreateLedger{
		ThreadID: threadID, CreateIntentID: intentID, Fingerprint: fingerprint,
		ContractVersion: strings.TrimSpace(req.ContractVersion),
	}
	return CreateRootResult{Thread: created}, nil
}

func createRootReplayMatches(meta ThreadMeta, threadID string) bool {
	if strings.TrimSpace(meta.ID) != strings.TrimSpace(threadID) ||
		strings.TrimSpace(meta.ParentThreadID) != "" || strings.TrimSpace(meta.ParentTurnID) != "" ||
		strings.TrimSpace(meta.ForkedFromThreadID) != "" || strings.TrimSpace(meta.ForkedFromEntryID) != "" ||
		strings.TrimSpace(meta.ForkOperationID) != "" || strings.TrimSpace(meta.ForkOperationNodeID) != "" {
		return false
	}
	lifecycle, err := canonicalThreadLifecycle(meta)
	if err != nil || lifecycle != ThreadLifecycleOpen {
		return false
	}
	return ValidateThreadMetaAuthority(meta) == nil
}

// CreateRootReplayMatches reports whether a live row is the exact canonical
// root shape eligible for root-create replay.
func CreateRootReplayMatches(meta ThreadMeta, threadID string) bool {
	return createRootReplayMatches(meta, threadID)
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

func (r *MemoryRepo) InspectThreadAuthority(_ context.Context, threadID string) (ThreadAuthoritySnapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	threadID = strings.TrimSpace(threadID)
	meta, ok := r.threads[threadID]
	if !ok {
		if _, deleted := r.tombstones[threadID]; deleted {
			return ThreadAuthoritySnapshot{}, ErrThreadDeleted
		}
		return ThreadAuthoritySnapshot{}, ErrThreadNotFound
	}
	snapshot := ThreadAuthoritySnapshot{
		Thread: meta, ClaimOperationID: r.authorityClaims[threadID], LeaseGeneration: r.leaseGeneration[threadID],
	}
	if lease, active := r.leases[threadID]; active {
		copy := lease
		snapshot.Lease = &copy
	}
	path, err := pathLocked(r.threads, r.entries, threadID, meta.LeafID)
	if err != nil {
		return ThreadAuthoritySnapshot{}, err
	}
	if err := ValidateThreadAuthoritySnapshot(snapshot.Thread, path, snapshot.Lease, snapshot.ClaimOperationID, snapshot.LeaseGeneration); err != nil {
		return ThreadAuthoritySnapshot{}, err
	}
	return snapshot, nil
}

func (r *MemoryRepo) InspectSubAgentThreadAuthority(_ context.Context, parentThreadID, childThreadID string) (SubAgentThreadAuthoritySnapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	parentThreadID = strings.TrimSpace(parentThreadID)
	childThreadID = strings.TrimSpace(childThreadID)
	parent, ok := r.threads[parentThreadID]
	if !ok {
		if _, deleted := r.tombstones[parentThreadID]; deleted {
			return SubAgentThreadAuthoritySnapshot{}, ErrThreadDeleted
		}
		return SubAgentThreadAuthoritySnapshot{}, ErrThreadNotFound
	}
	if err := ValidateThreadMetaAuthority(parent); err != nil {
		return SubAgentThreadAuthoritySnapshot{}, ErrAuthorityCorrupt
	}
	child, ok := r.threads[childThreadID]
	if !ok || strings.TrimSpace(child.ParentThreadID) != parentThreadID {
		return SubAgentThreadAuthoritySnapshot{}, ErrSubAgentNotFound
	}
	snapshot := ThreadAuthoritySnapshot{
		Thread: child, ClaimOperationID: r.authorityClaims[childThreadID], LeaseGeneration: r.leaseGeneration[childThreadID],
	}
	if lease, active := r.leases[childThreadID]; active {
		copy := lease
		snapshot.Lease = &copy
	}
	path, err := pathLocked(r.threads, r.entries, childThreadID, child.LeafID)
	if err != nil {
		return SubAgentThreadAuthoritySnapshot{}, err
	}
	if err := ValidateThreadAuthoritySnapshot(snapshot.Thread, path, snapshot.Lease, snapshot.ClaimOperationID, snapshot.LeaseGeneration); err != nil {
		return SubAgentThreadAuthoritySnapshot{}, err
	}
	return SubAgentThreadAuthoritySnapshot{Parent: parent, Child: snapshot}, nil
}

func (r *MemoryRepo) DeleteRootTree(_ context.Context, rootThreadID string) (DeleteRootTreeResult, error) {
	rootThreadID = strings.TrimSpace(rootThreadID)
	if rootThreadID == "" {
		return DeleteRootTreeResult{}, errors.New("root thread id is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if tombstone, ok := r.tombstones[rootThreadID]; ok && tombstone.ThreadID == rootThreadID && tombstone.RootThreadID == rootThreadID {
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
	threadIDs, err := threadAuthorityTreeIDsLocked(r.threads, rootThreadID)
	if err != nil {
		return DeleteRootTreeResult{}, err
	}
	for _, threadID := range threadIDs {
		lifecycle, err := canonicalThreadLifecycle(r.threads[threadID])
		if err != nil {
			return DeleteRootTreeResult{}, err
		}
		if lifecycle == ThreadLifecycleClosing {
			return DeleteRootTreeResult{}, ErrSubAgentClosing
		}
		if _, active := r.leases[threadID]; active {
			return DeleteRootTreeResult{}, ErrThreadAuthorityBusy
		}
		if r.threadAuthorityClaimedLocked(threadID) {
			return DeleteRootTreeResult{}, ErrThreadAuthorityBusy
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
		createIntentID := ""
		for intentID, intent := range r.rootCreateIntents {
			if intent.ThreadID == threadID {
				createIntentID = intentID
				break
			}
		}
		r.tombstones[threadID] = ThreadTombstone{
			ThreadID: threadID, RootThreadID: rootID, ParentThreadID: meta.ParentThreadID,
			CreateIntentID:  createIntentID,
			ForkOperationID: meta.ForkOperationID, ForkOperationNodeID: meta.ForkOperationNodeID,
			ForkedFromThreadID: meta.ForkedFromThreadID, ForkedFromEntryID: meta.ForkedFromEntryID,
			DeletedAt: now,
		}
		delete(r.threads, threadID)
		delete(r.entries, threadID)
		delete(r.todos, threadID)
		delete(r.providerStates, threadID)
		delete(r.leases, threadID)
		delete(r.authorityClaims, threadID)
		delete(r.subAgentInputs, threadID)
		delete(r.subAgentInputSequence, threadID)
		delete(r.leaseGeneration, threadID)
		for key, record := range r.artifacts {
			if record.ThreadID == threadID {
				delete(r.artifacts, key)
			}
		}
		for key, admission := range r.turnAdmissions {
			if admission.ThreadID == threadID {
				delete(r.turnAdmissions, key)
			}
		}
		for key, finish := range r.turnFinishes {
			if finish.ThreadID == threadID {
				delete(r.turnFinishes, key)
			}
		}
	}
	for attemptID, attempt := range r.effectAttempts {
		if _, deleted := deletedSet[attempt.Invocation.ThreadID]; deleted {
			delete(r.effectAttemptByInvocation, effectInvocationKey(attempt.Invocation))
			delete(r.effectAttempts, attemptID)
		}
	}
	for requestID, completion := range r.pendingToolCompletions {
		if _, deleted := deletedSet[completion.ThreadID]; deleted {
			delete(r.pendingToolCompletions, requestID)
		}
	}
	for requestID, compaction := range r.compactionOperations {
		if _, deleted := deletedSet[compaction.ThreadID]; deleted {
			delete(r.compactionOperations, requestID)
		}
	}
	for operationID, closeOperation := range r.subAgentCloseOperations {
		_, parentDeleted := deletedSet[closeOperation.ParentThreadID]
		_, targetDeleted := deletedSet[closeOperation.TargetThreadID]
		if parentDeleted || targetDeleted {
			delete(r.subAgentCloseOperations, operationID)
		}
	}
	return DeleteRootTreeResult{ThreadIDs: append([]string(nil), threadIDs...)}, nil
}

func threadAuthorityTreeIDsLocked(threads map[string]ThreadMeta, rootThreadID string) ([]string, error) {
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

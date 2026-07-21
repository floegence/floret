package sessiontree

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/session/artifact"
)

var (
	ErrRequestConflict         = errors.New("session tree authority request conflicts with persisted request")
	ErrSubAgentRequestConflict = fmt.Errorf("subagent request identity conflicts with persisted request: %w", ErrRequestConflict)
	ErrSubAgentInputNotFound   = errors.New("session tree subagent input not found")
	ErrEffectAttemptNotFound   = errors.New("session tree effect attempt not found")
	ErrEffectOutcomeUnknown    = errors.New("session tree effect outcome is unknown")
)

type SubAgentInputState string

const (
	SubAgentInputPending   SubAgentInputState = "pending"
	SubAgentInputAdmitted  SubAgentInputState = "admitted"
	SubAgentInputCancelled SubAgentInputState = "cancelled"
)

type SubAgentRequestKind string

const (
	SubAgentRequestPublication           SubAgentRequestKind = "publication"
	SubAgentRequestInput                 SubAgentRequestKind = "input"
	SubAgentRequestPendingToolCompletion SubAgentRequestKind = "pending_tool_completion"
)

type SubAgentInputRecord struct {
	SubAgentInputID    string
	ParentThreadID     string
	ChildThreadID      string
	RequestKind        SubAgentRequestKind
	RequestID          string
	RequestFingerprint string
	Sequence           int64
	State              SubAgentInputState
	Message            session.Message
	HostLabels         map[string]string
	CorrelationLabels  map[string]string
	AdmittedTurnID     string
	AdmittedRunID      string
	CreatedAt          time.Time
	AdmittedAt         time.Time
	CancelledAt        time.Time
}

type PublishSubAgentRequest struct {
	PublicationID      string
	RequestFingerprint string
	ParentThreadID     string
	ChildMeta          ThreadMeta
	ForkOptions        *ForkOptions
	ArtifactClosure    artifact.Closure
	Message            session.Message
	HostLabels         map[string]string
	CorrelationLabels  map[string]string
	Now                time.Time
}

type PublishSubAgentResult struct {
	Thread   ThreadMeta
	Input    SubAgentInputRecord
	Replayed bool
}

type PublishSubAgentInputRequest struct {
	InputRequestID     string
	RequestFingerprint string
	ParentThreadID     string
	ChildThreadID      string
	Message            session.Message
	HostLabels         map[string]string
	CorrelationLabels  map[string]string
	Interrupt          bool
	Now                time.Time
}

type AdmitSubAgentInputRequest struct {
	ParentThreadID string
	ChildThreadID  string
	TurnID         string
	RunID          string
	OwnerID        string
	Now            time.Time
}

type AdmitSubAgentInputResult struct {
	Input       SubAgentInputRecord
	Lease       TurnLease
	TurnStarted Entry
	UserMessage Entry
	Replayed    bool
}

type SubAgentInputAuthorityRepo interface {
	PublishSubAgent(context.Context, PublishSubAgentRequest) (PublishSubAgentResult, error)
	PublishSubAgentInput(context.Context, PublishSubAgentInputRequest) (SubAgentInputRecord, bool, error)
	PublishSubAgentPendingToolCompletion(context.Context, PublishSubAgentPendingToolCompletionRequest) (PublishSubAgentPendingToolCompletionResult, error)
	AdmitSubAgentInput(context.Context, AdmitSubAgentInputRequest) (AdmitSubAgentInputResult, error)
	ListSubAgentInputs(context.Context, string, SubAgentInputState) ([]SubAgentInputRecord, error)
}

type subAgentRequestLedger struct {
	ParentThreadID     string
	ChildThreadID      string
	RequestFingerprint string
	SubAgentInputID    string
	ArtifactClosure    artifact.Closure
}

func (r *MemoryRepo) PublishSubAgent(_ context.Context, req PublishSubAgentRequest) (PublishSubAgentResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := validateSubAgentPublicationIdentity(req); err != nil {
		return PublishSubAgentResult{}, err
	}
	parent, ok := r.threads[req.ParentThreadID]
	if !ok {
		return PublishSubAgentResult{}, ErrThreadNotFound
	}
	if parent.ID != req.ParentThreadID || ValidateThreadMetaAuthority(parent) != nil {
		return PublishSubAgentResult{}, ErrAuthorityCorrupt
	}
	if err := lifecycleRejectsWrite(parent); err != nil {
		return PublishSubAgentResult{}, err
	}
	if existing, ok := r.subAgentPublications[req.PublicationID]; ok {
		if existing.ParentThreadID != req.ParentThreadID || existing.ChildThreadID != req.ChildMeta.ID || existing.RequestFingerprint != req.RequestFingerprint ||
			!artifact.EqualClosure(existing.ArtifactClosure, req.ArtifactClosure) {
			return PublishSubAgentResult{}, ErrRequestConflict
		}
		if err := validateSubAgentPublicationRequest(req); err != nil {
			return PublishSubAgentResult{}, err
		}
		input, ok := r.subAgentInputByIDLocked(existing.SubAgentInputID)
		if !ok {
			return PublishSubAgentResult{}, fmt.Errorf("subagent publication %q input is missing", req.PublicationID)
		}
		child, ok := r.threads[req.ChildMeta.ID]
		if !ok {
			return PublishSubAgentResult{}, fmt.Errorf("subagent publication %q child is missing", req.PublicationID)
		}
		if !PublishSubAgentChildMatches(req, child) || input.ParentThreadID != req.ParentThreadID || input.ChildThreadID != req.ChildMeta.ID ||
			input.RequestID != req.PublicationID || input.RequestFingerprint != req.RequestFingerprint {
			return PublishSubAgentResult{}, ErrAuthorityCorrupt
		}
		if err := r.validateSubAgentPublicationArtifactsLocked(req.ArtifactClosure); err != nil {
			return PublishSubAgentResult{}, err
		}
		return PublishSubAgentResult{Thread: child, Input: input, Replayed: true}, nil
	}
	if err := validateSubAgentPublicationRequest(req); err != nil {
		return PublishSubAgentResult{}, err
	}
	if req.ForkOptions != nil && parent.LeafID != strings.TrimSpace(req.ForkOptions.ExpectedSourceLeafID) {
		return PublishSubAgentResult{}, ErrStaleAuthority
	}
	if r.threadAuthorityClaimedLocked(req.ParentThreadID) || r.threadAuthorityClaimedLocked(req.ChildMeta.ID) {
		return PublishSubAgentResult{}, ErrThreadAuthorityBusy
	}
	if _, exists := r.threads[req.ChildMeta.ID]; exists {
		return PublishSubAgentResult{}, ErrThreadExists
	}
	seqBefore := r.seq
	var child ThreadMeta
	var err error
	if req.ForkOptions == nil {
		child, err = r.createThreadLocked(req.ChildMeta)
	} else {
		opts := *req.ForkOptions
		opts.NewThreadID = req.ChildMeta.ID
		opts.ArtifactClosure = artifact.CloneClosure(req.ArtifactClosure)
		child, err = r.forkLocked(context.Background(), opts)
	}
	if err != nil {
		r.seq = seqBefore
		return PublishSubAgentResult{}, err
	}
	if !PublishSubAgentChildMatches(req, child) {
		r.rollbackSubAgentPublicationChildLocked(child.ID)
		r.seq = seqBefore
		return PublishSubAgentResult{}, ErrAuthorityCorrupt
	}
	input := r.newSubAgentInputLocked(SubAgentRequestPublication, req.PublicationID, req.RequestFingerprint, req.ParentThreadID, child.ID, req.Message, req.HostLabels, req.CorrelationLabels, req.Now)
	r.subAgentPublications[req.PublicationID] = subAgentRequestLedger{
		ParentThreadID: req.ParentThreadID, ChildThreadID: child.ID, RequestFingerprint: req.RequestFingerprint, SubAgentInputID: input.SubAgentInputID,
		ArtifactClosure: artifact.CloneClosure(req.ArtifactClosure),
	}
	if input.SubAgentInputID == "" {
		r.rollbackSubAgentPublicationChildLocked(child.ID)
		r.seq = seqBefore
		return PublishSubAgentResult{}, errors.New("failed to allocate subagent input identity")
	}
	return PublishSubAgentResult{Thread: child, Input: input}, nil
}

func (r *MemoryRepo) rollbackSubAgentPublicationChildLocked(childThreadID string) {
	delete(r.threads, childThreadID)
	r.deleteIndexedEntriesLocked(childThreadID)
	delete(r.todos, childThreadID)
	delete(r.providerStates, childThreadID)
	delete(r.leases, childThreadID)
	delete(r.authorityClaims, childThreadID)
	delete(r.subAgentInputs, childThreadID)
	delete(r.subAgentInputSequence, childThreadID)
	r.deleteApprovalAuthorityForThreadsLocked(map[string]struct{}{childThreadID: {}})
	for key, record := range r.artifacts {
		if record.ThreadID == childThreadID {
			delete(r.artifacts, key)
		}
	}
}

func (r *MemoryRepo) PublishSubAgentInput(_ context.Context, req PublishSubAgentInputRequest) (SubAgentInputRecord, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ValidatePublishSubAgentInputRequest(req); err != nil {
		return SubAgentInputRecord{}, false, err
	}
	parent, ok := r.threads[req.ParentThreadID]
	if !ok {
		return SubAgentInputRecord{}, false, ErrThreadNotFound
	}
	if err := lifecycleRejectsWrite(parent); err != nil {
		return SubAgentInputRecord{}, false, err
	}
	if existing, ok := r.subAgentInputRequests[req.InputRequestID]; ok {
		if existing.ParentThreadID != req.ParentThreadID || existing.ChildThreadID != req.ChildThreadID || existing.RequestFingerprint != req.RequestFingerprint {
			return SubAgentInputRecord{}, false, ErrSubAgentRequestConflict
		}
		input, ok := r.subAgentInputByIDLocked(existing.SubAgentInputID)
		if !ok {
			return SubAgentInputRecord{}, false, fmt.Errorf("subagent input request %q input is missing", req.InputRequestID)
		}
		return input, true, nil
	}
	child, ok := r.threads[req.ChildThreadID]
	if !ok {
		return SubAgentInputRecord{}, false, ErrThreadNotFound
	}
	if child.ParentThreadID != req.ParentThreadID {
		return SubAgentInputRecord{}, false, ErrInvalidThreadAuthority
	}
	if err := lifecycleRejectsWrite(child); err != nil {
		return SubAgentInputRecord{}, false, err
	}
	if r.threadAuthorityClaimedLocked(req.ParentThreadID) || r.threadAuthorityClaimedLocked(req.ChildThreadID) {
		return SubAgentInputRecord{}, false, ErrThreadAuthorityBusy
	}
	if active, ok := r.leases[req.ChildThreadID]; ok && active.Purpose == TurnLeasePurposeMutation {
		return SubAgentInputRecord{}, false, ErrActiveTurn
	}
	if req.Interrupt {
		now := nonZeroAuthorityTime(req.Now, r.now)
		inputs := r.subAgentInputs[req.ChildThreadID]
		for index := range inputs {
			if inputs[index].State == SubAgentInputPending {
				inputs[index].State = SubAgentInputCancelled
				inputs[index].CancelledAt = now
			}
		}
		r.subAgentInputs[req.ChildThreadID] = inputs
	}
	input := r.newSubAgentInputLocked(SubAgentRequestInput, req.InputRequestID, req.RequestFingerprint, req.ParentThreadID, req.ChildThreadID, req.Message, req.HostLabels, req.CorrelationLabels, req.Now)
	r.subAgentInputRequests[req.InputRequestID] = subAgentRequestLedger{
		ParentThreadID: req.ParentThreadID, ChildThreadID: req.ChildThreadID, RequestFingerprint: req.RequestFingerprint, SubAgentInputID: input.SubAgentInputID,
	}
	return input, false, nil
}

func (r *MemoryRepo) AdmitSubAgentInput(ctx context.Context, req AdmitSubAgentInputRequest) (AdmitSubAgentInputResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if strings.TrimSpace(req.ParentThreadID) == "" || strings.TrimSpace(req.ChildThreadID) == "" || strings.TrimSpace(req.TurnID) == "" || strings.TrimSpace(req.RunID) == "" || strings.TrimSpace(req.OwnerID) == "" {
		return AdmitSubAgentInputResult{}, errors.New("subagent admission requires parent, child, turn, run, and owner identities")
	}
	req.ParentThreadID = strings.TrimSpace(req.ParentThreadID)
	req.ChildThreadID = strings.TrimSpace(req.ChildThreadID)
	req.TurnID = strings.TrimSpace(req.TurnID)
	req.RunID = strings.TrimSpace(req.RunID)
	req.OwnerID = strings.TrimSpace(req.OwnerID)
	parent, ok := r.threads[req.ParentThreadID]
	if !ok {
		return AdmitSubAgentInputResult{}, ErrThreadNotFound
	}
	child, ok := r.threads[req.ChildThreadID]
	if !ok {
		return AdmitSubAgentInputResult{}, ErrThreadNotFound
	}
	if err := lifecycleRejectsWrite(parent); err != nil {
		return AdmitSubAgentInputResult{}, err
	}
	if err := lifecycleRejectsWrite(child); err != nil {
		return AdmitSubAgentInputResult{}, err
	}
	if child.ParentThreadID != req.ParentThreadID {
		return AdmitSubAgentInputResult{}, ErrInvalidThreadAuthority
	}
	for _, input := range r.subAgentInputs[req.ChildThreadID] {
		if input.State != SubAgentInputAdmitted || (input.AdmittedTurnID != req.TurnID && input.AdmittedRunID != req.RunID) {
			continue
		}
		if input.AdmittedTurnID != req.TurnID || input.AdmittedRunID != req.RunID {
			return AdmitSubAgentInputResult{}, ErrRequestConflict
		}
		return AdmitSubAgentInputResult{Input: cloneSubAgentInputRecord(input), Replayed: true}, nil
	}
	if r.threadAuthorityClaimedLocked(req.ParentThreadID) || r.threadAuthorityClaimedLocked(req.ChildThreadID) {
		return AdmitSubAgentInputResult{}, ErrThreadAuthorityBusy
	}
	if _, active := r.leases[req.ChildThreadID]; active {
		return AdmitSubAgentInputResult{}, ErrActiveTurn
	}
	if r.hasTurnStartedLocked(req.ChildThreadID, req.TurnID) {
		return AdmitSubAgentInputResult{}, ErrRequestConflict
	}
	inputIndex := -1
	for index, input := range r.subAgentInputs[req.ChildThreadID] {
		if input.State == SubAgentInputPending && (inputIndex < 0 || input.Sequence < r.subAgentInputs[req.ChildThreadID][inputIndex].Sequence ||
			(input.Sequence == r.subAgentInputs[req.ChildThreadID][inputIndex].Sequence && input.SubAgentInputID < r.subAgentInputs[req.ChildThreadID][inputIndex].SubAgentInputID)) {
			inputIndex = index
		}
	}
	if inputIndex < 0 {
		return AdmitSubAgentInputResult{}, ErrSubAgentInputNotFound
	}
	lease, err := r.acquireTurnLeaseLocked(TurnLease{ThreadID: req.ChildThreadID, TurnID: req.TurnID, OwnerID: req.OwnerID, Purpose: TurnLeasePurposeTurn})
	if err != nil {
		return AdmitSubAgentInputResult{}, err
	}
	input := r.subAgentInputs[req.ChildThreadID][inputIndex]
	_ = ctx
	now := nonZeroAuthorityTime(req.Now, r.now)
	started := Entry{
		ID:       r.nextEntryID(req.ChildThreadID),
		ThreadID: req.ChildThreadID, TurnID: req.TurnID, Type: EntryTurnMarker, TurnStatus: TurnStarted, Metadata: map[string]string{"run_id": req.RunID},
	}
	started.ParentID = child.LeafID
	started.CreatedAt = now
	started.Raw = rawForEntry(started)
	started.RawHash = stableHash(started.Raw)
	user := Entry{
		ID:       r.nextEntryID(req.ChildThreadID),
		ThreadID: req.ChildThreadID,
		TurnID:   req.TurnID,
		Type:     EntryUserMessage,
		Metadata: map[string]string{"subagent_input_id": input.SubAgentInputID},
		Message:  session.CloneMessage(input.Message),
	}
	user.ParentID = started.ID
	user.CreatedAt = now
	user.Raw = rawForEntry(user)
	user.RawHash = stableHash(user.Raw)
	r.appendIndexedEntriesLocked(req.ChildThreadID, started, user)
	child.LeafID = user.ID
	child.UpdatedAt = now
	r.threads[req.ChildThreadID] = child
	input.State = SubAgentInputAdmitted
	input.AdmittedTurnID = req.TurnID
	input.AdmittedRunID = req.RunID
	input.AdmittedAt = now
	r.subAgentInputs[req.ChildThreadID][inputIndex] = input
	return AdmitSubAgentInputResult{Input: cloneSubAgentInputRecord(input), Lease: lease, TurnStarted: started, UserMessage: user}, nil
}

func (r *MemoryRepo) ListSubAgentInputs(_ context.Context, childThreadID string, state SubAgentInputState) ([]SubAgentInputRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.threads[childThreadID]; !ok {
		return nil, ErrThreadNotFound
	}
	out := make([]SubAgentInputRecord, 0)
	for _, input := range r.subAgentInputs[childThreadID] {
		if state == "" || input.State == state {
			out = append(out, cloneSubAgentInputRecord(input))
		}
	}
	slices.SortFunc(out, func(left, right SubAgentInputRecord) int {
		if left.Sequence < right.Sequence {
			return -1
		}
		if left.Sequence > right.Sequence {
			return 1
		}
		return strings.Compare(left.SubAgentInputID, right.SubAgentInputID)
	})
	return out, nil
}

func (r *MemoryRepo) acquireTurnLeaseLocked(request TurnLease) (TurnLease, error) {
	if _, ok := r.threads[request.ThreadID]; !ok {
		return TurnLease{}, ErrThreadNotFound
	}
	if _, ok := r.leases[request.ThreadID]; ok {
		return TurnLease{}, ErrActiveTurn
	}
	purpose, err := validateLeaseRequest(request)
	if err != nil {
		return TurnLease{}, err
	}
	now := r.now().UTC()
	r.leaseGeneration[request.ThreadID]++
	proof := TurnLease{
		ThreadID: request.ThreadID, Purpose: purpose, TurnID: request.TurnID, MutationID: request.MutationID, MutationKind: request.MutationKind,
		OwnerID: request.OwnerID, Generation: r.leaseGeneration[request.ThreadID], AcquiredAt: now, RenewedAt: now, ExpiresAt: now.Add(r.leasePolicy.TTL),
	}
	r.leases[request.ThreadID] = proof
	return proof, nil
}

func (r *MemoryRepo) newSubAgentInputLocked(kind SubAgentRequestKind, requestID, fingerprint, parentThreadID, childThreadID string, message session.Message, hostLabels, correlationLabels map[string]string, now time.Time) SubAgentInputRecord {
	r.subAgentInputSequence[childThreadID]++
	sequence := r.subAgentInputSequence[childThreadID]
	input := SubAgentInputRecord{
		SubAgentInputID: fmt.Sprintf("%s-input-%d", childThreadID, sequence), ParentThreadID: parentThreadID, ChildThreadID: childThreadID,
		RequestKind: kind, RequestID: requestID, RequestFingerprint: fingerprint, Sequence: sequence, State: SubAgentInputPending,
		Message: session.CloneMessage(message), HostLabels: cloneStringMap(hostLabels), CorrelationLabels: cloneStringMap(correlationLabels), CreatedAt: nonZeroAuthorityTime(now, r.now),
	}
	r.subAgentInputs[childThreadID] = append(r.subAgentInputs[childThreadID], input)
	return cloneSubAgentInputRecord(input)
}

func (r *MemoryRepo) subAgentInputByIDLocked(inputID string) (SubAgentInputRecord, bool) {
	for _, inputs := range r.subAgentInputs {
		for _, input := range inputs {
			if input.SubAgentInputID == inputID {
				return cloneSubAgentInputRecord(input), true
			}
		}
	}
	return SubAgentInputRecord{}, false
}

func validateSubAgentPublicationRequest(req PublishSubAgentRequest) error {
	if err := validateSubAgentPublicationIdentity(req); err != nil {
		return err
	}
	publicationID := strings.TrimSpace(req.PublicationID)
	fingerprint := strings.TrimSpace(req.RequestFingerprint)
	parentThreadID := strings.TrimSpace(req.ParentThreadID)
	childThreadID := strings.TrimSpace(req.ChildMeta.ID)
	if req.PublicationID != publicationID || req.RequestFingerprint != fingerprint || req.ParentThreadID != parentThreadID || req.ChildMeta.ID != childThreadID ||
		req.ChildMeta.ParentThreadID != parentThreadID || sessiontreeMetaInvalid(req.ChildMeta) {
		return ErrInvalidThreadAuthority
	}
	childLifecycle, err := req.ChildMeta.CanonicalLifecycle()
	if err != nil || childLifecycle != ThreadLifecycleOpen || strings.TrimSpace(req.ChildMeta.LeafID) != "" ||
		strings.TrimSpace(req.ChildMeta.CloseOperationID) != "" || strings.TrimSpace(req.ChildMeta.ForkedFromThreadID) != "" ||
		strings.TrimSpace(req.ChildMeta.ForkedFromEntryID) != "" || strings.TrimSpace(req.ChildMeta.ForkOperationID) != "" ||
		strings.TrimSpace(req.ChildMeta.ForkOperationNodeID) != "" {
		return ErrInvalidThreadAuthority
	}
	if strings.TrimSpace(req.Message.Content) == "" && len(req.Message.Attachments) == 0 {
		return errors.New("subagent publication requires a message")
	}
	if err := session.ValidateMessageReferences(req.Message.References); err != nil {
		return err
	}
	if req.ForkOptions == nil {
		if !artifact.IsZeroClosure(req.ArtifactClosure) {
			return ErrRequestConflict
		}
		return nil
	}
	options := req.ForkOptions
	if strings.TrimSpace(options.SourceThreadID) != parentThreadID || options.SourceThreadID != parentThreadID ||
		strings.TrimSpace(options.NewThreadID) != childThreadID || options.NewThreadID != childThreadID ||
		strings.TrimSpace(options.EntryID) == "" || !options.EntryIDPinned || strings.TrimSpace(options.ExpectedSourceLeafID) != strings.TrimSpace(options.EntryID) ||
		(options.Position != "" && options.Position != ForkAt) ||
		strings.TrimSpace(options.OperationID) != "" || strings.TrimSpace(options.OperationNodeID) != "" ||
		!subAgentDestinationMetaMatches(req.ChildMeta, options.DestinationMeta) {
		return ErrInvalidThreadAuthority
	}
	if err := artifact.ValidateClosure(req.ArtifactClosure); err != nil ||
		req.ArtifactClosure.SourceThreadID != parentThreadID || req.ArtifactClosure.DestinationThreadID != childThreadID {
		return ErrRequestConflict
	}
	return nil
}

func validateSubAgentPublicationIdentity(req PublishSubAgentRequest) error {
	if strings.TrimSpace(req.PublicationID) == "" || strings.TrimSpace(req.RequestFingerprint) == "" ||
		strings.TrimSpace(req.ParentThreadID) == "" || strings.TrimSpace(req.ChildMeta.ID) == "" {
		return errors.New("subagent publication requires publication, fingerprint, parent, and child identities")
	}
	return nil
}

// ValidatePublishSubAgentIdentity validates the durable request-ledger key and
// conflict identity before a backend interprets the requested child shape.
func ValidatePublishSubAgentIdentity(req PublishSubAgentRequest) error {
	return validateSubAgentPublicationIdentity(req)
}

// ValidatePublishSubAgentRequest enforces the exact parent, child, fork, and
// first-input identity before a backend starts the atomic publication.
func ValidatePublishSubAgentRequest(req PublishSubAgentRequest) error {
	return validateSubAgentPublicationRequest(req)
}

// PublishSubAgentChildMatches verifies the durable child authority produced by
// or replayed for one exact publication request.
func PublishSubAgentChildMatches(req PublishSubAgentRequest, child ThreadMeta) bool {
	if child.ID != req.ChildMeta.ID || !subAgentDestinationMetaMatches(child, forkDestinationMetaForChild(req.ChildMeta)) {
		return false
	}
	if req.ForkOptions == nil {
		return strings.TrimSpace(child.ForkedFromThreadID) == "" && strings.TrimSpace(child.ForkedFromEntryID) == "" &&
			strings.TrimSpace(child.ForkOperationID) == "" && strings.TrimSpace(child.ForkOperationNodeID) == ""
	}
	return child.ForkedFromThreadID == req.ForkOptions.SourceThreadID && child.ForkedFromEntryID == req.ForkOptions.EntryID &&
		strings.TrimSpace(child.ForkOperationID) == "" && strings.TrimSpace(child.ForkOperationNodeID) == ""
}

func sessiontreeMetaInvalid(meta ThreadMeta) bool {
	return ValidateThreadMetaAuthority(meta) != nil
}

func forkDestinationMetaForChild(meta ThreadMeta) *ForkDestinationMeta {
	return &ForkDestinationMeta{
		ParentThreadID: meta.ParentThreadID, ParentTurnID: meta.ParentTurnID,
		TaskName: meta.TaskName, TaskDescription: meta.TaskDescription, AgentPath: meta.AgentPath,
		HostProfileRef: meta.HostProfileRef, ForkMode: meta.ForkMode, Lifecycle: meta.Lifecycle,
	}
}

func subAgentDestinationMetaMatches(meta ThreadMeta, destination *ForkDestinationMeta) bool {
	if destination == nil || !MatchesForkDestinationMeta(meta, destination) {
		return false
	}
	metaLifecycle, metaErr := meta.CanonicalLifecycle()
	want := ThreadMeta{ID: meta.ID}
	applyForkDestinationMeta(&want, destination)
	wantLifecycle, wantErr := want.CanonicalLifecycle()
	return metaErr == nil && wantErr == nil && metaLifecycle == wantLifecycle
}

func (r *MemoryRepo) validateSubAgentPublicationArtifactsLocked(closure artifact.Closure) error {
	if artifact.IsZeroClosure(closure) {
		return nil
	}
	return r.validateArtifactForkDestinationLocked(closure)
}

func ValidatePublishSubAgentInputRequest(req PublishSubAgentInputRequest) error {
	if strings.TrimSpace(req.InputRequestID) == "" || strings.TrimSpace(req.RequestFingerprint) == "" || strings.TrimSpace(req.ParentThreadID) == "" || strings.TrimSpace(req.ChildThreadID) == "" {
		return errors.New("subagent input requires request, fingerprint, parent, and child identities")
	}
	if strings.TrimSpace(req.Message.Content) == "" && len(req.Message.Attachments) == 0 {
		return errors.New("subagent input requires a message")
	}
	if err := session.ValidateMessageReferences(req.Message.References); err != nil {
		return err
	}
	return nil
}

func nonZeroAuthorityTime(value time.Time, now func() time.Time) time.Time {
	if !value.IsZero() {
		return value.UTC()
	}
	return now().UTC()
}

func cloneSubAgentInputRecord(input SubAgentInputRecord) SubAgentInputRecord {
	input.Message = session.CloneMessage(input.Message)
	input.HostLabels = cloneStringMap(input.HostLabels)
	input.CorrelationLabels = cloneStringMap(input.CorrelationLabels)
	return input
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	out := make(map[string]string, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}

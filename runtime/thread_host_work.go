package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session/artifact"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/tools"
)

// ThreadHostWorkService records host-owned work bound to an authorized local
// invocation. These operations are host integration, never model tool schemas.
type ThreadHostWorkService interface {
	BindHostWork(context.Context, BindHostWorkInput) error
	CompleteHostWork(context.Context, CompleteHostWorkInput) error
	ListHostWork(context.Context, identity.ThreadID) ([]HostWork, error)
	ReadHostWorkOutput(context.Context, identity.ThreadID, string) (string, error)
}
type BindHostWorkInput struct {
	EffectAttemptID string
	ThreadID        identity.ThreadID
	TurnID          identity.TurnID
	RunID           identity.RunID
	ToolCallID      string
	WorkID          string
}
type CompleteHostWorkInput struct {
	// Activity is the final host-sanitized presentation for the original tool item.
	Activity *tools.ActivityPresentation
	// Result retains bounded host-defined facts (valid JSON, at most 16 KiB),
	// independently from the optional output snapshot.
	Result   json.RawMessage
	ThreadID identity.ThreadID
	WorkID   string
	// Notice is complete UTF-8 text of at most 2048 bytes. The host provides a
	// bounded status and read reference when output does not fit.
	Notice string
	// Notify permits automatic continuation after natural completion.
	// Every unobserved final notice is admitted at the next provider preparation.
	// Stop, deletion and unknown execution outcomes suppress continuation.
	Notify bool
	// Output is the final bounded snapshot, at most 8 MiB. Nil means unavailable.
	Output *string
}
type HostWork struct {
	Result          json.RawMessage `json:"result,omitempty"`
	Inherited       bool            `json:"inherited,omitempty"`
	WorkID          string          `json:"work_id"`
	TurnID          identity.TurnID `json:"turn_id"`
	RunID           identity.RunID  `json:"run_id"`
	ToolCallID      string          `json:"tool_call_id"`
	State           string          `json:"state"`
	Notice          string          `json:"notice,omitempty"`
	OutputAvailable bool            `json:"output_available"`
}

func (s *threadRuntimeService) BindHostWork(ctx context.Context, in BindHostWorkInput) error {
	if err := s.host.requireExecution(); err != nil {
		return err
	}
	if strings.TrimSpace(in.WorkID) == "" || len(in.WorkID) > 240 {
		return errors.New("host work requires a bounded identity")
	}
	_, err := s.ensureThread(ctx, in.ThreadID)
	if err != nil {
		return err
	}
	effects, ok := s.host.store.repo.(sessiontree.EffectAttemptReader)
	if !ok {
		return ErrUnsupportedStoreCapability
	}
	attempt, err := effects.EffectAttempt(ctx, in.ThreadID.String(), in.EffectAttemptID)
	if err != nil {
		return runtimeHostError(err)
	}
	inv := attempt.Invocation
	if inv.TurnID != in.TurnID.String() || inv.RunID != in.RunID.String() || inv.ToolCallID != in.ToolCallID || attempt.State != sessiontree.EffectAttemptDispatching {
		return ErrPendingToolNotFound
	}
	repo, ok := s.host.store.repo.(sessiontree.HostWorkRepo)
	if !ok {
		return ErrUnsupportedStoreCapability
	}
	_, err = repo.BindHostWork(ctx, sessiontree.HostWork{ID: in.WorkID, ExecutorID: s.host.executorID, Invocation: inv, State: "bound"})
	return runtimeHostError(err)
}
func (s *threadRuntimeService) CompleteHostWork(ctx context.Context, in CompleteHostWorkInput) error {
	if err := s.host.available(); err != nil {
		return err
	}
	repo, ok := s.host.store.repo.(sessiontree.HostWorkRepo)
	if !ok {
		return ErrUnsupportedStoreCapability
	}
	req := sessiontree.HostWorkSettlement{ThreadID: in.ThreadID.String(), ID: in.WorkID, ExecutorID: s.host.executorID, State: "completed", Notice: in.Notice, Notify: in.Notify, OutputUnavailable: in.Output == nil}
	req.Result = append(json.RawMessage(nil), in.Result...)
	req.Activity = tools.CloneActivityPresentation(in.Activity)
	if in.Output != nil {
		req.Output = &artifact.FullOutput{Text: *in.Output}
	}
	_, err := repo.SettleHostWork(ctx, req)
	if errors.Is(err, sessiontree.ErrHostWorkOutputStorage) {
		// The failed transaction published neither fact nor reference. Preserve
		// the observed completion without claiming its output was retained.
		req.Output = nil
		req.OutputUnavailable = true
		_, err = repo.SettleHostWork(ctx, req)
	}
	if err != nil {
		return runtimeHostError(err)
	}
	s.refreshCanonical(in.ThreadID, "")
	if in.Notify {
		go s.continueHostWork(in.ThreadID)
	}
	return nil
}
func (s *threadRuntimeService) ListHostWork(ctx context.Context, id identity.ThreadID) ([]HostWork, error) {
	if _, err := s.ensureThread(ctx, id); err != nil {
		return nil, err
	}
	repo, ok := s.host.store.repo.(sessiontree.HostWorkRepo)
	if !ok {
		return nil, ErrUnsupportedStoreCapability
	}
	works, err := repo.HostWorks(ctx, id.String())
	if err != nil {
		return nil, runtimeHostError(err)
	}
	out := make([]HostWork, 0, len(works))
	for _, w := range works {
		out = append(out, HostWork{Result: append(json.RawMessage(nil), w.Result...), Inherited: w.Inherited, WorkID: w.ID, TurnID: identity.TurnID(w.Invocation.TurnID), RunID: identity.RunID(w.Invocation.RunID), ToolCallID: w.Invocation.ToolCallID, State: w.State, Notice: w.Notice, OutputAvailable: w.Output != nil})
	}
	return out, nil
}
func (s *threadRuntimeService) ReadHostWorkOutput(ctx context.Context, id identity.ThreadID, workID string) (string, error) {
	if _, err := s.ensureThread(ctx, id); err != nil {
		return "", err
	}
	repo, ok := s.host.store.repo.(sessiontree.HostWorkRepo)
	if !ok {
		return "", ErrUnsupportedStoreCapability
	}
	works, err := repo.HostWorks(ctx, id.String())
	if err != nil {
		return "", runtimeHostError(err)
	}
	reader, ok := s.host.store.repo.(sessiontree.ArtifactAuthorityRepo)
	if !ok {
		return "", ErrUnsupportedStoreCapability
	}
	for _, w := range works {
		if w.ID == workID && w.Output != nil {
			content, err := reader.ReadArtifact(ctx, sessiontree.ArtifactReadRequest{ThreadID: id.String(), ArtifactID: w.Output.ID})
			return content.Text, runtimeHostError(err)
		}
	}
	return "", ErrArtifactNotFound
}

type hostWorkRecovery struct {
	mu   sync.Mutex
	done bool
}

func (s *threadRuntimeService) reconcileHostWork(ctx context.Context, id identity.ThreadID) error {
	value, _ := s.host.workRecovery.LoadOrStore(id, &hostWorkRecovery{})
	recovery := value.(*hostWorkRecovery)
	recovery.mu.Lock()
	defer recovery.mu.Unlock()
	if recovery.done {
		return nil
	}
	if err := s.reconcileHostWorkOnce(ctx, id); err != nil {
		return err
	}
	recovery.done = true
	return nil
}
func (s *threadRuntimeService) reconcileHostWorkOnce(ctx context.Context, id identity.ThreadID) error {
	repo, ok := s.host.store.repo.(sessiontree.HostWorkRepo)
	if !ok {
		return nil
	}
	works, err := repo.HostWorks(ctx, id.String())
	if err != nil {
		return runtimeHostError(err)
	}
	for _, w := range works {
		if w.State != "bound" || w.Inherited || w.ExecutorID == s.host.executorID {
			continue
		}
		var activity *tools.ActivityPresentation
		if reader, ok := s.host.store.repo.(sessiontree.CanonicalToolExchangeReader); ok {
			_, result, found, err := reader.CanonicalToolExchange(ctx, id.String(), w.Invocation.TurnID, w.Invocation.RunID, w.Invocation.ToolCallID)
			if err != nil {
				return runtimeHostError(err)
			}
			if found {
				activity = tools.FinalizeActivityPresentation(result.Message.Activity, "unknown")
			}
		}
		_, err := repo.SettleHostWork(ctx, sessiontree.HostWorkSettlement{Activity: activity, ThreadID: id.String(), ID: w.ID, ExecutorID: w.ExecutorID, State: "unknown", Notice: "Work " + w.ID + ": execution observation is unavailable after restart; the outcome and any unsealed output are unknown. Effects may have occurred. Do not automatically replay it.", OutputUnavailable: true})
		if err != nil {
			return runtimeHostError(err)
		}
	}
	return nil
}

func (s *threadRuntimeService) continueHostWork(id identity.ThreadID) {
	ctx := context.Background()
	if s.host.requireExecution() != nil {
		return
	}
	repo, ok := s.host.store.repo.(sessiontree.HostWorkRepo)
	if !ok {
		return
	}
	key, eligible, err := repo.HostWorkContinuationKey(ctx, id.String())
	if err != nil || !eligible {
		return
	}
	if _, err := s.viewResult(ctx, id, threadAckResult); err != nil {
		return
	}
	_, err = s.admitThreadExecutionResult(ctx, id, UserInput{}, nil, key, TurnKindHostWorkContinuation, threadAckResult)
	if err != nil && !errors.Is(err, ErrThreadBusy) && !errors.Is(err, sessiontree.ErrStaleAuthority) {
		s.host.store.reportBackgroundError(err)
	}
}

var _ ThreadHostWorkService = (*threadRuntimeService)(nil)

func (s *threadRuntimeService) hostWorkSourceTurn(ctx context.Context, id identity.ThreadID, key string, retry identity.TurnID) (identity.TurnID, error) {
	seen := map[identity.TurnID]bool{}
	for retry != "" {
		if seen[retry] {
			return "", ErrAuthorityCorrupt
		}
		seen[retry] = true
		reader, ok := s.host.store.repo.(sessiontree.CanonicalTurnReadRepo)
		if !ok {
			return "", ErrUnsupportedStoreCapability
		}
		read, err := reader.ReadCanonicalTurn(ctx, id.String(), retry.String())
		if err != nil {
			return "", runtimeHostError(err)
		}
		if len(read.Turn.Entries) == 0 {
			return "", ErrAuthorityCorrupt
		}
		key = read.Turn.Entries[0].Entry.RequestKey
		retry = ""
		if read.Turn.RetrySource != nil {
			retry = identity.TurnID(read.Turn.RetrySource.TurnID)
		}
	}
	repo, ok := s.host.store.repo.(sessiontree.HostWorkRepo)
	if !ok {
		return "", ErrUnsupportedStoreCapability
	}
	works, err := repo.HostWorks(ctx, id.String())
	if err != nil {
		return "", runtimeHostError(err)
	}
	for _, w := range works {
		if !w.Inherited && key == "host-work-continuation:"+sessiontree.StableHash(w.ID)[:32] {
			return identity.TurnID(w.Invocation.TurnID), nil
		}
	}
	return "", ErrPendingToolNotFound
}

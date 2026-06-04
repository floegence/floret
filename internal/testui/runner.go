package testui

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/floegence/floret/adapters"
	"github.com/floegence/floret/agentharness"
	"github.com/floegence/floret/builtintools"
	"github.com/floegence/floret/config"
	"github.com/floegence/floret/contextpolicy"
	"github.com/floegence/floret/engine"
	"github.com/floegence/floret/eval"
	"github.com/floegence/floret/event"
	"github.com/floegence/floret/harness"
	"github.com/floegence/floret/internal/sessionlifecycle"
	"github.com/floegence/floret/memory"
	"github.com/floegence/floret/modelcatalog"
	"github.com/floegence/floret/promptcache"
	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/session"
	"github.com/floegence/floret/sessiontree"
	"github.com/floegence/floret/tools"
)

const (
	TargetUnit          = "unit"
	TargetRace          = "race"
	TargetEvalDemo      = "eval-demo"
	TargetProviderSmoke = "provider-smoke"
	TargetAll           = "all"
)

var (
	errAgentSessionBusy  = errors.New("agent session is running")
	errAgentSessionInput = errors.New("agent session input error")
)

type Runner struct {
	Root            string
	EnvFile         string
	Now             func() time.Time
	Exec            func(context.Context, string, []string, string, []string) ([]byte, int)
	ProviderFactory func(config.Config) (provider.Provider, error)
	Sessions        *agentSessionRegistry
}

func NewRunner(root string) Runner {
	return Runner{
		Root:            root,
		EnvFile:         filepath.Join(root, config.DefaultEnvFile),
		Now:             time.Now,
		Exec:            execCommand,
		ProviderFactory: adapters.NewProvider,
		Sessions:        newAgentSessionRegistry(),
	}
}

type agentSessionRegistry struct {
	mu       sync.Mutex
	order    []string
	sessions map[string]*agentSession
}

type agentSession struct {
	mu                      sync.Mutex
	id                      string
	transient               bool
	profile                 ProviderProfile
	systemPrompt            string
	selectedTools           []string
	hostedTools             []provider.HostedToolDefinition
	unavailableCapabilities []string
	contextPolicy           contextpolicy.Policy
	cfg                     config.Config
	provider                *observingProvider
	recorder                agentEventRecorder
	harnessRecorder         agentHarnessRecorder
	repo                    sessiontree.Repo
	promptStore             promptcache.Store
	registry                *tools.Registry
	harness                 *agentharness.AgentHarness
	thread                  *agentharness.Thread
	turns                   []AgentTurnSummary
	snapshotMu              sync.Mutex
	lastSnapshot            AgentSessionSnapshot
	createdAt               time.Time
	updatedAt               time.Time
}

type agentSessionRuntime struct {
	provider                *observingProvider
	recorder                agentEventRecorder
	harnessRecorder         agentHarnessRecorder
	registry                *tools.Registry
	hostedTools             []provider.HostedToolDefinition
	unavailableCapabilities []string
	harness                 *agentharness.AgentHarness
	thread                  *agentharness.Thread
}

type agentEventRecorder interface {
	event.Sink
	Snapshot() []event.Event
}

type agentHarnessRecorder interface {
	agentharness.HarnessSink
	Snapshot() []agentharness.HarnessEvent
}

func newAgentSessionRegistry() *agentSessionRegistry {
	return &agentSessionRegistry{sessions: map[string]*agentSession{}}
}

func (r *Runner) sessionRegistry() *agentSessionRegistry {
	if r.Sessions != nil {
		return r.Sessions
	}
	r.Sessions = newAgentSessionRegistry()
	return r.Sessions
}

func (r *Runner) providerFactory() func(config.Config) (provider.Provider, error) {
	if r.ProviderFactory != nil {
		return r.ProviderFactory
	}
	return adapters.NewProvider
}

func (r Runner) ConfigInfo() ConfigInfo {
	info := ConfigInfo{EnvFile: r.EnvFile}
	if _, err := os.Stat(r.EnvFile); err == nil {
		info.EnvFileFound = true
	}
	cfg, err := config.Load(config.WithPath(r.EnvFile))
	if err != nil {
		info.Provider = "invalid"
		info.Model = err.Error()
		return info
	}
	info.Provider = cfg.Provider
	info.Model = cfg.Model
	info.LiveProvider = cfg.Provider != "" && cfg.Provider != config.ProviderFake
	info.BaseURL = redactURL(cfg.BaseURL)
	return info
}

func (r Runner) Catalog() []CatalogProvider {
	return modelcatalog.Providers()
}

func (r *Runner) RunAgent(ctx context.Context, req AgentRunRequest) AgentRunResponse {
	return r.CreateAgentSession(ctx, req)
}

func (r *Runner) CreateIdleAgentSession(ctx context.Context, req AgentRunRequest) (AgentSessionSnapshot, error) {
	started := r.now()
	profile, err := r.profileForRun(req)
	if err != nil {
		return AgentSessionSnapshot{}, err
	}
	cfg := config.Config{
		Provider:                profile.Provider,
		Model:                   profile.Model,
		BaseURL:                 profile.BaseURL,
		APIKey:                  profile.APIKey,
		FakeResponse:            profile.FakeResponse,
		RunID:                   fmt.Sprintf("testui-agent-%d", started.UnixNano()),
		SystemPrompt:            strings.TrimSpace(req.SystemPrompt),
		ContextPolicy:           req.ContextPolicy,
		MaxEmptyProviderRetries: 1,
		NoProgressLimit:         2,
		DuplicateToolLimit:      3,
		WallTime:                60 * time.Second,
	}
	if cfg.SystemPrompt == "" {
		cfg.SystemPrompt = "You are Floret. Answer naturally when the user's request is complete, or call ask_user if you need missing information."
	}
	cfg, err = config.Resolve(cfg, nil)
	if err != nil {
		return AgentSessionSnapshot{}, err
	}
	sessionID := fmt.Sprintf("testui-session-%d", started.UnixNano())
	cfg.RunID = sessionID
	resolvedProfile := resolvedProfileFromConfig(profile, cfg, cfg.APIKey != "" || profile.APIKeySet)
	selectedTools, err := normalizeAgentSessionToolsForProfile(req.SelectedTools, req.ToolMode, resolvedProfile, r.EnvFile)
	if err != nil {
		return AgentSessionSnapshot{}, fmt.Errorf("%w: %v", errAgentSessionInput, err)
	}
	sess, err := r.buildAgentSession(ctx, agentSessionBuildOptions{
		ID:            sessionID,
		CreatedAt:     started,
		UpdatedAt:     started,
		Profile:       resolvedProfile,
		SystemPrompt:  cfg.SystemPrompt,
		SelectedTools: selectedTools,
		ContextPolicy: cfg.ContextPolicy,
		Config:        cfg,
		Start:         true,
	})
	if err != nil {
		return AgentSessionSnapshot{}, err
	}
	r.sessionRegistry().put(sess)
	if err := r.saveAgentSessionMetadata(r.metadataFromSession(sess)); err != nil {
		return AgentSessionSnapshot{}, err
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return r.sessionSnapshotLocked(ctx, sess)
}

func (r *Runner) RunInterfaceProbe(ctx context.Context, req AgentInterfaceProbeRequest) AgentRunResponse {
	started := r.now()
	resp := AgentRunResponse{
		ID:        fmt.Sprintf("%d", started.UnixNano()),
		Probe:     true,
		StartedAt: started,
	}
	profile := ProviderProfile{ID: "tool-contract-probe", Name: "Tool Contract Probe", Provider: config.ProviderFake, Model: "tool-contract-probe"}
	selectedTools, err := normalizeAgentSessionToolsForProfile(req.SelectedTools, "", profile, r.EnvFile)
	if err != nil {
		return r.failAgentRunWithStatus(resp, http.StatusBadRequest, err)
	}
	probe := *r
	probe.ProviderFactory = func(config.Config) (provider.Provider, error) {
		if slices.Contains(selectedTools, builtintools.ToolList) {
			return harness.NewScriptedProvider(
				harness.Step(
					provider.StreamEvent{Type: provider.Reasoning, Text: "Inspect selected tool contract."},
					harness.Text("Checking selected tool definitions before running a low-risk read probe."),
					harness.Tool("probe-list", builtintools.ToolList, `{"path":null,"limit":5}`),
					harness.DoneReason("tool_calls"),
				),
				harness.Step(
					provider.StreamEvent{Type: provider.Reasoning, Text: "Confirm tool result handoff."},
					harness.Text("Tool contract probe passed: provider request exposed the selected toolset and the list tool result reached the follow-up request."),
					harness.Done(),
				),
			), nil
		}
		return harness.NewScriptedProvider(
			harness.Step(
				provider.StreamEvent{Type: provider.Reasoning, Text: "Inspect selected tool contract."},
				harness.Text("Tool contract probe passed: provider request exposed the selected toolset. No low-risk list tool was selected, so no local tool was executed."),
				harness.Done(),
			),
		), nil
	}
	cfg := config.Config{
		Provider:                config.ProviderFake,
		Model:                   "tool-contract-probe",
		RunID:                   "testui-probe-" + resp.ID,
		SystemPrompt:            "You are Floret's deterministic test UI interface probe. Exercise only the scripted low-risk probe behavior.",
		ContextPolicy:           req.ContextPolicy,
		MaxEmptyProviderRetries: 1,
		NoProgressLimit:         2,
		DuplicateToolLimit:      3,
		WallTime:                30 * time.Second,
	}
	cfg, err = config.Resolve(cfg, nil)
	if err != nil {
		return r.failAgentRun(resp, err)
	}
	sessionID := "testui-probe-" + resp.ID
	cfg.RunID = sessionID
	profile.Provider = cfg.Provider
	profile.Model = cfg.Model
	sess, err := probe.buildAgentSession(ctx, agentSessionBuildOptions{
		ID:            sessionID,
		Transient:     true,
		CreatedAt:     started,
		UpdatedAt:     started,
		Profile:       profile,
		SystemPrompt:  cfg.SystemPrompt,
		SelectedTools: selectedTools,
		ContextPolicy: cfg.ContextPolicy,
		Config:        cfg,
		Start:         true,
	})
	if err != nil {
		return r.failAgentRun(resp, err)
	}
	resp.Profile = profile
	result := probe.runAgentTurn(ctx, sess, resp, "Run the test UI tool contract probe for the selected tools.")
	result.Probe = true
	if result.Status == string(engine.Completed) {
		result.Summary = "Interface probe passed: selected tools were bound to a transient session and captured in the provider request."
	}
	return result
}

func (r *Runner) CreateAgentSession(ctx context.Context, req AgentRunRequest) AgentRunResponse {
	started := r.now()
	resp := AgentRunResponse{
		ID:        fmt.Sprintf("%d", started.UnixNano()),
		StartedAt: started,
	}
	if strings.TrimSpace(req.Message) == "" {
		resp.Status = "error"
		resp.StatusCode = http.StatusBadRequest
		resp.Error = "message is required"
		resp.Summary = resp.Error
		resp.FinishedAt = r.now()
		resp.DurationMS = resp.FinishedAt.Sub(resp.StartedAt).Milliseconds()
		return resp
	}
	profile, err := r.profileForRun(req)
	if err != nil {
		return r.failAgentRun(resp, err)
	}
	resp.Profile = stripProfileSecret(profile)
	cfg := config.Config{
		Provider:                profile.Provider,
		Model:                   profile.Model,
		BaseURL:                 profile.BaseURL,
		APIKey:                  profile.APIKey,
		FakeResponse:            profile.FakeResponse,
		RunID:                   "testui-agent-" + resp.ID,
		SystemPrompt:            strings.TrimSpace(req.SystemPrompt),
		ContextPolicy:           req.ContextPolicy,
		MaxEmptyProviderRetries: 1,
		NoProgressLimit:         2,
		DuplicateToolLimit:      3,
		WallTime:                60 * time.Second,
	}
	if cfg.SystemPrompt == "" {
		cfg.SystemPrompt = "You are Floret. Answer naturally when the user's request is complete, or call ask_user if you need missing information."
	}
	cfg, err = config.Resolve(cfg, nil)
	if err != nil {
		return r.failAgentRun(resp, err)
	}
	sessionID := "testui-session-" + resp.ID
	cfg.RunID = sessionID
	resolvedProfile := resolvedProfileFromConfig(profile, cfg, cfg.APIKey != "" || profile.APIKeySet)
	resp.Profile = resolvedProfile
	selectedTools, err := normalizeAgentSessionToolsForProfile(req.SelectedTools, req.ToolMode, resolvedProfile, r.EnvFile)
	if err != nil {
		return r.failAgentRunWithStatus(resp, http.StatusBadRequest, err)
	}
	sess, err := r.buildAgentSession(ctx, agentSessionBuildOptions{
		ID:            sessionID,
		CreatedAt:     started,
		UpdatedAt:     started,
		Profile:       resolvedProfile,
		SystemPrompt:  cfg.SystemPrompt,
		SelectedTools: selectedTools,
		ContextPolicy: cfg.ContextPolicy,
		Config:        cfg,
		Start:         true,
	})
	if err != nil {
		return r.failAgentRun(resp, err)
	}
	r.sessionRegistry().put(sess)
	if err := r.saveAgentSessionMetadata(r.metadataFromSession(sess)); err != nil {
		return r.failAgentRun(resp, err)
	}
	return r.runAgentTurn(ctx, sess, resp, req.Message)
}

func (r *Runner) RunAgentTurn(ctx context.Context, sessionID string, req AgentTurnRequest) AgentRunResponse {
	started := r.now()
	resp := AgentRunResponse{ID: fmt.Sprintf("%d", started.UnixNano()), SessionID: sessionID, StartedAt: started}
	return r.runAgentTurnResponse(ctx, sessionID, req, resp, nil)
}

func (r *Runner) RunAgentTurnStream(ctx context.Context, sessionID string, req AgentTurnRequest, sink AgentStreamSink) AgentRunResponse {
	started := r.now()
	resp := AgentRunResponse{ID: fmt.Sprintf("%d", started.UnixNano()), SessionID: sessionID, StartedAt: started}
	return r.runAgentTurnResponse(ctx, sessionID, req, resp, sink)
}

func (r *Runner) runAgentTurnResponse(ctx context.Context, sessionID string, req AgentTurnRequest, resp AgentRunResponse, sink AgentStreamSink) AgentRunResponse {
	if strings.TrimSpace(req.Message) == "" {
		resp.Status = "error"
		resp.StatusCode = http.StatusBadRequest
		resp.Error = "message is required"
		resp.Summary = resp.Error
		resp.FinishedAt = r.now()
		resp.DurationMS = resp.FinishedAt.Sub(resp.StartedAt).Milliseconds()
		return resp
	}
	sess, ok := r.sessionRegistry().get(sessionID)
	if !ok {
		var err error
		sess, err = r.restoreAgentSession(ctx, sessionID)
		if err != nil {
			status := http.StatusInternalServerError
			if isMissingAgentSessionError(err) {
				status = http.StatusNotFound
			}
			return r.failAgentRunWithStatus(resp, status, err)
		}
	}
	resp.Profile = sess.profile
	if !sess.mu.TryLock() {
		return r.failAgentRunWithStatus(resp, http.StatusConflict, fmt.Errorf("agent session %q is already running", sessionID))
	}
	defer sess.mu.Unlock()
	snapshot, err := r.sessionSnapshotLocked(ctx, sess)
	if err != nil {
		return r.failAgentRun(resp, err)
	}
	if !snapshot.CanAppendMessage {
		return r.failAgentRunWithStatus(resp, http.StatusConflict, fmt.Errorf("agent session %q is %s and cannot accept a new message", sessionID, snapshot.Status))
	}
	r.markAgentSessionRunningLocked(sess, resp.ID)
	if sink != nil {
		setAgentSessionStreamSink(sess, sink)
		defer setAgentSessionStreamSink(sess, nil)
	}
	result := r.runAgentTurnLocked(ctx, sess, resp, req.Message)
	if sink != nil {
		if result.Session.ID != "" {
			snapshotCopy := result.Session
			sink.EmitAgentStream(AgentStreamEvent{
				Type:      AgentStreamSessionSnapshot,
				SessionID: sessionID,
				TurnID:    result.TurnID,
				At:        r.now(),
				Snapshot:  &snapshotCopy,
			})
		}
		resultCopy := result
		sink.EmitAgentStream(AgentStreamEvent{
			Type:      agentStreamEventForResult(result),
			SessionID: sessionID,
			TurnID:    result.TurnID,
			At:        result.FinishedAt,
			Result:    &resultCopy,
			Message:   result.Summary,
			Error:     result.Error,
		})
	}
	return result
}

func (r *Runner) UpdateAgentSessionTools(ctx context.Context, sessionID string, req AgentToolsUpdateRequest) (AgentSessionSnapshot, error) {
	if req.SelectedTools == nil {
		return AgentSessionSnapshot{}, fmt.Errorf("%w: selected_tools is required", errAgentSessionInput)
	}
	var err error
	sess, ok := r.sessionRegistry().get(sessionID)
	if !ok {
		sess, err = r.restoreAgentSession(ctx, sessionID)
		if err != nil {
			return AgentSessionSnapshot{}, err
		}
	}
	selectedTools, err := normalizeAgentSessionToolsForProfile(*req.SelectedTools, "", sess.profile, r.EnvFile)
	if err != nil {
		return AgentSessionSnapshot{}, fmt.Errorf("%w: %v", errAgentSessionInput, err)
	}
	if !sess.mu.TryLock() {
		return AgentSessionSnapshot{}, fmt.Errorf("%w: %s", errAgentSessionBusy, sessionID)
	}
	defer sess.mu.Unlock()
	current, err := r.sessionSnapshotLocked(ctx, sess)
	if err != nil {
		return AgentSessionSnapshot{}, err
	}
	if snapshotIsRunning(current) {
		return AgentSessionSnapshot{}, fmt.Errorf("%w: %s", errAgentSessionBusy, sessionID)
	}
	if slices.Equal(sess.selectedTools, selectedTools) {
		return current, nil
	}
	previous := cloneSelectedTools(sess.selectedTools)
	nextRuntime, err := sess.prepareRuntime(ctx, r, selectedTools)
	if err != nil {
		return AgentSessionSnapshot{}, err
	}
	now := r.now()
	reason := strings.TrimSpace(req.Reason)
	metadata := map[string]string{
		"source":         "test-ui",
		"previous_tools": strings.Join(previous, ","),
		"selected_tools": strings.Join(selectedTools, ","),
	}
	if reason != "" {
		metadata["reason"] = reason
	}
	if _, err := sessiontree.AppendActiveTools(ctx, sess.repo, sess.id, metadata); err != nil {
		return AgentSessionSnapshot{}, err
	}
	currentTools := sess.selectedTools
	currentUpdatedAt := sess.updatedAt
	sess.selectedTools = selectedTools
	sess.updatedAt = now
	if err := r.saveAgentSessionMetadata(r.metadataFromSession(sess)); err != nil {
		sess.selectedTools = currentTools
		sess.updatedAt = currentUpdatedAt
		return AgentSessionSnapshot{}, err
	}
	sess.applyRuntime(nextRuntime)
	return r.sessionSnapshotLocked(ctx, sess)
}

func (r *Runner) DeleteAgentSession(ctx context.Context, sessionID string) error {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return fmt.Errorf("%w: session id is required", errAgentSessionInput)
	}
	registry := r.sessionRegistry()
	runIDs := []string{sessionID}
	registry.mu.Lock()
	sess, inMemory := registry.sessions[sessionID]
	if inMemory && !sess.mu.TryLock() {
		registry.mu.Unlock()
		return fmt.Errorf("%w: %s", errAgentSessionBusy, sessionID)
	}
	if inMemory {
		snap, err := r.sessionSnapshotLocked(ctx, sess)
		if err != nil {
			sess.mu.Unlock()
			registry.mu.Unlock()
			return err
		}
		if snapshotIsRunning(snap) {
			sess.mu.Unlock()
			registry.mu.Unlock()
			return fmt.Errorf("%w: %s", errAgentSessionBusy, sessionID)
		}
		runIDs = agentSessionPromptCacheRunIDs(sessionID, sess.turns, snap.PathEntries)
		delete(registry.sessions, sessionID)
		registry.order = removeSessionID(registry.order, sessionID)
		sess.mu.Unlock()
	}
	registry.mu.Unlock()
	if !inMemory {
		meta, err := r.loadAgentSessionMetadata(sessionID)
		if err != nil {
			return fmt.Errorf("agent session %q not found", sessionID)
		}
		if meta.ID == "" {
			return fmt.Errorf("agent session %q not found", sessionID)
		}
		snap, err := r.sessionSnapshotFromMetadata(ctx, meta)
		if err != nil {
			return err
		}
		if snapshotIsRunning(snap) {
			return fmt.Errorf("%w: %s", errAgentSessionBusy, sessionID)
		}
		runIDs = agentSessionPromptCacheRunIDs(sessionID, meta.Turns, snap.PathEntries)
		registry.mu.Lock()
		delete(registry.sessions, sessionID)
		registry.order = removeSessionID(registry.order, sessionID)
		registry.mu.Unlock()
	}
	if err := os.Remove(r.agentSessionMetadataPath(sessionID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.RemoveAll(filepath.Join(r.agentSessionTreeRoot(), safeSessionFileName(sessionID))); err != nil {
		return err
	}
	for _, runID := range runIDs {
		if err := os.RemoveAll(filepath.Join(r.Root, ".floret-test-ui", "prompt-cache", safeSessionFileName(runID))); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) AgentSession(ctx context.Context, sessionID string) (AgentSessionSnapshot, error) {
	sess, ok := r.sessionRegistry().get(sessionID)
	if !ok {
		meta, err := r.loadAgentSessionMetadata(sessionID)
		if err != nil {
			return AgentSessionSnapshot{}, fmt.Errorf("agent session %q not found", sessionID)
		}
		return r.sessionSnapshotFromMetadata(ctx, meta)
	}
	if !sess.mu.TryLock() {
		return runningAgentSessionSnapshot(sess), nil
	}
	defer sess.mu.Unlock()
	return r.sessionSnapshotLocked(ctx, sess)
}

func (sess *agentSession) prepareRuntime(ctx context.Context, r *Runner, selectedTools []string) (agentSessionRuntime, error) {
	p, err := r.providerFactory()(sess.cfg)
	if err != nil {
		return agentSessionRuntime{}, err
	}
	observed := newObservingProvider(p)
	rec := &streamingEventRecorder{}
	harnessRec := &streamingHarnessRecorder{repo: sess.repo, threadID: sess.id}
	registry := tools.NewRegistry()
	hostedTools, unavailableCapabilities, err := registerAgentSessionTools(registry, r.Root, r.EnvFile, selectedTools, sess.profile)
	if err != nil {
		return agentSessionRuntime{}, err
	}
	h := agentharness.New(agentharness.Options{
		Provider:      observed,
		ProviderName:  sess.cfg.Provider,
		Model:         sess.cfg.Model,
		SystemPrompt:  sess.systemPrompt,
		Tools:         registry,
		PromptStore:   sess.promptStore,
		Repo:          sess.repo,
		Sink:          rec,
		HarnessSink:   harnessRec,
		Approver:      testUIToolApprover,
		ContextPolicy: sess.contextPolicy,
		EngineOptions: engine.Options{
			CacheRetention:          config.PromptCacheRetention(sess.cfg),
			ContextPolicy:           sess.contextPolicy,
			MaxEmptyProviderRetries: sess.cfg.MaxEmptyProviderRetries,
			NoProgressLimit:         sess.cfg.NoProgressLimit,
			DuplicateToolLimit:      sess.cfg.DuplicateToolLimit,
			WallTime:                sess.cfg.WallTime,
			MaxCostUSD:              1.00,
			HostedToolDefinitions:   hostedTools,
		},
		NewID: r.agentSessionIDGenerator(ctx, sess.repo, sess.id),
		Now:   r.now,
	})
	thread, err := h.ResumeThread(ctx, sess.id, agentharness.ResumeOptions{})
	if err != nil {
		return agentSessionRuntime{}, err
	}
	return agentSessionRuntime{
		provider:                observed,
		recorder:                rec,
		harnessRecorder:         harnessRec,
		registry:                registry,
		hostedTools:             hostedTools,
		unavailableCapabilities: unavailableCapabilities,
		harness:                 h,
		thread:                  thread,
	}, nil
}

func (sess *agentSession) applyRuntime(runtime agentSessionRuntime) {
	sess.provider = runtime.provider
	sess.recorder = runtime.recorder
	sess.harnessRecorder = runtime.harnessRecorder
	sess.registry = runtime.registry
	sess.hostedTools = runtime.hostedTools
	sess.unavailableCapabilities = runtime.unavailableCapabilities
	sess.harness = runtime.harness
	sess.thread = runtime.thread
}

func setAgentSessionStreamSink(sess *agentSession, sink AgentStreamSink) {
	if sess == nil {
		return
	}
	if sess.provider != nil {
		sess.provider.SetStreamSink(sink)
	}
	if rec, ok := sess.recorder.(interface{ SetStreamSink(AgentStreamSink) }); ok {
		rec.SetStreamSink(sink)
	}
	if rec, ok := sess.harnessRecorder.(interface{ SetStreamSink(AgentStreamSink) }); ok {
		rec.SetStreamSink(sink)
	}
}

func isAgentSessionInputError(err error) bool {
	return errors.Is(err, errAgentSessionInput)
}

func (r *Runner) AgentSessions(ctx context.Context) []AgentSessionSnapshot {
	sessions := r.sessionRegistry().list()
	out := make([]AgentSessionSnapshot, 0, len(sessions))
	for _, sess := range sessions {
		if !sess.mu.TryLock() {
			out = append(out, runningAgentSessionSnapshot(sess))
			continue
		}
		snap, err := r.sessionSnapshotLocked(ctx, sess)
		sess.mu.Unlock()
		if err == nil {
			out = append(out, snap)
		}
	}
	seen := map[string]struct{}{}
	for _, snap := range out {
		seen[snap.ID] = struct{}{}
	}
	metas, err := r.listAgentSessionMetadata()
	if err != nil {
		return out
	}
	for _, meta := range metas {
		if _, ok := seen[meta.ID]; ok {
			continue
		}
		snap, err := r.sessionSnapshotFromMetadata(ctx, meta)
		if err == nil {
			out = append(out, snap)
		}
	}
	slices.SortFunc(out, func(a, b AgentSessionSnapshot) int {
		if a.UpdatedAt.Equal(b.UpdatedAt) {
			return strings.Compare(b.ID, a.ID)
		}
		if a.UpdatedAt.After(b.UpdatedAt) {
			return -1
		}
		return 1
	})
	return out
}

type agentSessionBuildOptions struct {
	ID            string
	Transient     bool
	CreatedAt     time.Time
	UpdatedAt     time.Time
	Profile       ProviderProfile
	SystemPrompt  string
	SelectedTools []string
	ContextPolicy contextpolicy.Policy
	Config        config.Config
	Turns         []AgentTurnSummary
	Start         bool
}

func (r *Runner) buildAgentSession(ctx context.Context, opts agentSessionBuildOptions) (*agentSession, error) {
	cfg := opts.Config
	cfg.RunID = opts.ID
	p, err := r.providerFactory()(cfg)
	if err != nil {
		return nil, err
	}
	observed := newObservingProvider(p)
	var repo sessiontree.Repo = sessiontree.NewFileRepo(r.agentSessionTreeRoot())
	var promptStore promptcache.Store = promptcache.NewFileStore(filepath.Join(r.Root, ".floret-test-ui", "prompt-cache"))
	if opts.Transient {
		repo = sessiontree.NewMemoryRepo()
		promptStore = promptcache.NewMemoryStore()
	}
	rec := &streamingEventRecorder{}
	harnessRec := &streamingHarnessRecorder{repo: repo, threadID: opts.ID}
	selectedTools, err := normalizeAgentSessionToolsForProfile(opts.SelectedTools, "", opts.Profile, r.EnvFile)
	if err != nil {
		return nil, err
	}
	registry := tools.NewRegistry()
	hostedTools, unavailableCapabilities, err := registerAgentSessionTools(registry, r.Root, r.EnvFile, selectedTools, opts.Profile)
	if err != nil {
		return nil, err
	}
	h := agentharness.New(agentharness.Options{
		Provider:      observed,
		ProviderName:  cfg.Provider,
		Model:         cfg.Model,
		SystemPrompt:  cfg.SystemPrompt,
		Tools:         registry,
		PromptStore:   promptStore,
		Repo:          repo,
		Sink:          rec,
		HarnessSink:   harnessRec,
		Approver:      testUIToolApprover,
		ContextPolicy: cfg.ContextPolicy,
		EngineOptions: engine.Options{
			CacheRetention:          config.PromptCacheRetention(cfg),
			ContextPolicy:           cfg.ContextPolicy,
			MaxEmptyProviderRetries: cfg.MaxEmptyProviderRetries,
			NoProgressLimit:         cfg.NoProgressLimit,
			DuplicateToolLimit:      cfg.DuplicateToolLimit,
			WallTime:                cfg.WallTime,
			MaxCostUSD:              1.00,
			HostedToolDefinitions:   hostedTools,
		},
		NewID: r.agentSessionIDGenerator(ctx, repo, opts.ID),
		Now:   r.now,
	})
	var thread *agentharness.Thread
	if opts.Start {
		thread, err = h.StartThread(ctx, agentharness.StartThreadOptions{ThreadID: opts.ID})
	} else {
		thread, err = h.ResumeThread(ctx, opts.ID, agentharness.ResumeOptions{})
	}
	if err != nil {
		return nil, err
	}
	createdAt := opts.CreatedAt
	if createdAt.IsZero() {
		createdAt = r.now()
	}
	updatedAt := opts.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = createdAt
	}
	return &agentSession{
		id:                      opts.ID,
		transient:               opts.Transient,
		profile:                 opts.Profile,
		systemPrompt:            cfg.SystemPrompt,
		selectedTools:           selectedTools,
		hostedTools:             hostedTools,
		unavailableCapabilities: unavailableCapabilities,
		contextPolicy:           cfg.ContextPolicy,
		cfg:                     cfg,
		provider:                observed,
		recorder:                rec,
		harnessRecorder:         harnessRec,
		repo:                    repo,
		promptStore:             promptStore,
		registry:                registry,
		harness:                 h,
		thread:                  thread,
		turns:                   append([]AgentTurnSummary(nil), opts.Turns...),
		createdAt:               createdAt,
		updatedAt:               updatedAt,
	}, nil
}

func (r *Runner) agentSessionIDGenerator(ctx context.Context, repo sessiontree.Repo, sessionID string) func(string) string {
	var mu sync.Mutex
	seqByPrefix := map[string]int{}
	if entries, err := repo.Entries(ctx, sessionID); err == nil {
		for _, entry := range entries {
			rememberPrefixedID(seqByPrefix, entry.TurnID)
			rememberPrefixedID(seqByPrefix, entry.CompactionID)
		}
	}
	return func(prefix string) string {
		mu.Lock()
		defer mu.Unlock()
		seqByPrefix[prefix]++
		return fmt.Sprintf("%s-%d", prefix, seqByPrefix[prefix])
	}
}

func testUIToolApprover(context.Context, tools.ApprovalRequest) (tools.PermissionDecision, error) {
	return tools.PermissionDecisionAllow, nil
}

func rememberPrefixedID(seqByPrefix map[string]int, value string) {
	idx := strings.LastIndex(value, "-")
	if idx < 0 || idx == len(value)-1 {
		return
	}
	n, err := strconv.Atoi(value[idx+1:])
	if err != nil {
		return
	}
	prefix := value[:idx]
	if n > seqByPrefix[prefix] {
		seqByPrefix[prefix] = n
	}
}

func (r *Runner) restoreAgentSession(ctx context.Context, sessionID string) (*agentSession, error) {
	registry := r.sessionRegistry()
	registry.mu.Lock()
	if sess, ok := registry.sessions[sessionID]; ok {
		registry.mu.Unlock()
		return sess, nil
	}
	meta, err := r.loadAgentSessionMetadata(sessionID)
	if err != nil {
		registry.mu.Unlock()
		return nil, fmt.Errorf("agent session %q not found", sessionID)
	}
	cfg, profile, err := r.cfgFromSessionMetadata(meta)
	if err != nil {
		registry.mu.Unlock()
		return nil, fmt.Errorf("restore agent session %q: %w", sessionID, err)
	}
	sess, err := r.buildAgentSession(ctx, agentSessionBuildOptions{
		ID:            meta.ID,
		CreatedAt:     meta.CreatedAt,
		UpdatedAt:     meta.UpdatedAt,
		Profile:       profile,
		SystemPrompt:  meta.SystemPrompt,
		SelectedTools: meta.SelectedTools,
		ContextPolicy: meta.ContextPolicy,
		Config:        cfg,
		Turns:         meta.Turns,
		Start:         false,
	})
	if err != nil {
		registry.mu.Unlock()
		return nil, fmt.Errorf("restore agent session %q: %w", sessionID, err)
	}
	if err := r.saveAgentSessionMetadata(r.metadataFromSession(sess)); err != nil {
		registry.mu.Unlock()
		return nil, err
	}
	if _, ok := registry.sessions[sess.id]; !ok {
		registry.order = append(registry.order, sess.id)
	}
	registry.sessions[sess.id] = sess
	registry.mu.Unlock()
	return sess, nil
}

func (r *Runner) sessionSnapshotFromMetadata(ctx context.Context, meta agentSessionMetadata) (AgentSessionSnapshot, error) {
	repo := sessiontree.NewFileRepo(r.agentSessionTreeRoot())
	thread, err := repo.Thread(ctx, meta.ID)
	if err != nil {
		return AgentSessionSnapshot{}, err
	}
	path, err := repo.Path(ctx, meta.ID, thread.LeafID)
	if err != nil {
		return AgentSessionSnapshot{}, err
	}
	entries, err := repo.Entries(ctx, meta.ID)
	if err != nil {
		return AgentSessionSnapshot{}, err
	}
	lifecycle := sessionlifecycle.Derive(path, sessionlifecycle.PhaseIdle)
	turns := append([]AgentTurnSummary(nil), meta.Turns...)
	active := observeMessages(sessiontree.BuildContext(path, sessiontree.ContextOptions{}))
	pathEntries := observeEntries(path)
	allEntries := observeEntries(entries)
	updatedAt := meta.UpdatedAt
	if thread.UpdatedAt.After(updatedAt) {
		updatedAt = thread.UpdatedAt
	}
	return AgentSessionSnapshot{
		ID:                      meta.ID,
		Status:                  lifecycle.Status(),
		Phase:                   lifecycle.Phase(),
		LeafID:                  thread.LeafID,
		CreatedAt:               meta.CreatedAt,
		UpdatedAt:               updatedAt,
		Profile:                 stripProfileSecret(meta.Profile),
		SystemPrompt:            meta.SystemPrompt,
		SelectedTools:           cloneSelectedTools(meta.SelectedTools),
		HostedTools:             append([]provider.HostedToolDefinition(nil), searchSnapshotHostedTools(meta.Profile, r.EnvFile, meta.SelectedTools)...),
		UnavailableCapabilities: searchSnapshotUnavailable(meta.Profile, r.EnvFile, meta.SelectedTools),
		ContextPolicy:           meta.ContextPolicy,
		LatestTurnID:            lifecycle.LatestTurnID(),
		WaitingPrompt:           lifecycle.WaitingPrompt(),
		Recoverable:             lifecycle.Recoverable(),
		CanAppendMessage:        lifecycle.CanAppendMessage(),
		Turns:                   turns,
		ActiveContext:           active,
		PathEntries:             pathEntries,
		AllEntries:              allEntries,
		AggregateMetrics:        aggregateTurnMetrics(turns),
		Compactions:             countCompactions(path),
	}, nil
}

func (r *Runner) runAgentTurn(ctx context.Context, sess *agentSession, resp AgentRunResponse, message string) AgentRunResponse {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	r.markAgentSessionRunningLocked(sess, resp.ID)
	return r.runAgentTurnLocked(ctx, sess, resp, message)
}

func (r *Runner) runAgentTurnLocked(ctx context.Context, sess *agentSession, resp AgentRunResponse, message string) AgentRunResponse {
	turn, err := sess.thread.Run(ctx, message, agentharness.RunOptions{})
	if err != nil && turn.Status == "" {
		return r.failAgentRun(resp, err)
	}
	finished := r.now()
	sess.updatedAt = finished
	resp.SessionID = sess.id
	resp.TurnID = turn.ID
	resp.ID = turn.ID
	resp.Status = string(turn.Status)
	resp.Output = turn.Output
	resp.Metrics = turn.Metrics
	resp.Events = sess.recorder.Snapshot()
	resp.HarnessEvents = sess.harnessRecorder.Snapshot()
	resp.Profile = sess.profile
	resp.CompletionReason = string(turn.CompletionReason)
	resp.ContinuationReason = string(turn.ContinuationReason)
	resp.FinishReason = string(turn.FinishReason)
	resp.RawFinishReason = turn.RawFinishReason
	resp.FinishInferred = turn.FinishInferred
	resp.CanAppendMessage = turn.Status == engine.Waiting || turn.Status == engine.Completed
	if turn.Status == engine.Waiting {
		resp.WaitingPrompt = turn.Output
	}
	if turn.Err != nil {
		resp.Error = turn.Err.Error()
	}
	result := engine.Result{
		Status:             turn.Status,
		Output:             turn.Output,
		Err:                turn.Err,
		Metrics:            turn.Metrics,
		CompletionReason:   turn.CompletionReason,
		ContinuationReason: turn.ContinuationReason,
		FinishReason:       turn.FinishReason,
		RawFinishReason:    turn.RawFinishReason,
		FinishInferred:     turn.FinishInferred,
	}
	summary := AgentTurnSummary{
		ID:                 turn.ID,
		Status:             string(turn.Status),
		Output:             turn.Output,
		Error:              resp.Error,
		StartedAt:          resp.StartedAt,
		FinishedAt:         finished,
		Metrics:            turn.Metrics,
		CompletionReason:   string(turn.CompletionReason),
		ContinuationReason: string(turn.ContinuationReason),
		FinishReason:       string(turn.FinishReason),
		RawFinishReason:    turn.RawFinishReason,
		FinishInferred:     turn.FinishInferred,
	}
	sess.turns = append(sess.turns, summary)
	snapshot, snapErr := r.sessionSnapshotLocked(ctx, sess)
	if snapErr != nil {
		return r.failAgentRun(resp, snapErr)
	}
	if !sess.transient {
		if err := r.saveAgentSessionMetadata(r.metadataFromSession(sess)); err != nil {
			return r.failAgentRun(resp, err)
		}
	}
	if sess.transient {
		snapshot.CanAppendMessage = false
	}
	resp.Session = snapshot
	resp.CanAppendMessage = snapshot.CanAppendMessage
	resp.WaitingPrompt = snapshot.WaitingPrompt
	resp.Observation = r.agentObservationLocked(sess, snapshot, result, turn.ID)
	resp.Summary = agentSummary(result)
	resp.FinishedAt = finished
	resp.DurationMS = resp.FinishedAt.Sub(resp.StartedAt).Milliseconds()
	storeAgentSessionSnapshot(sess, resp.Session)
	return resp
}

func (r *Runner) failAgentRun(resp AgentRunResponse, err error) AgentRunResponse {
	return r.failAgentRunWithStatus(resp, 0, err)
}

func (r *Runner) failAgentRunWithStatus(resp AgentRunResponse, statusCode int, err error) AgentRunResponse {
	resp.Status = "error"
	resp.StatusCode = statusCode
	resp.Error = err.Error()
	resp.Summary = err.Error()
	resp.FinishedAt = r.now()
	resp.DurationMS = resp.FinishedAt.Sub(resp.StartedAt).Milliseconds()
	return resp
}

func isMissingAgentSessionError(err error) bool {
	return errors.Is(err, os.ErrNotExist) ||
		errors.Is(err, sessiontree.ErrThreadNotFound) ||
		strings.Contains(err.Error(), "not found")
}

func (r *agentSessionRegistry) put(sess *agentSession) {
	if sess.transient {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.sessions[sess.id]; !ok {
		r.order = append(r.order, sess.id)
	}
	r.sessions[sess.id] = sess
}

func (r *agentSessionRegistry) get(id string) (*agentSession, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	sess, ok := r.sessions[id]
	return sess, ok
}

func (r *agentSessionRegistry) list() []*agentSession {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*agentSession, 0, len(r.order))
	for _, id := range r.order {
		if sess, ok := r.sessions[id]; ok {
			out = append(out, sess)
		}
	}
	return out
}

func removeSessionID(ids []string, id string) []string {
	out := ids[:0]
	for _, item := range ids {
		if item != id {
			out = append(out, item)
		}
	}
	return out
}

func snapshotIsRunning(snapshot AgentSessionSnapshot) bool {
	return sessionlifecycle.IsRunningStatus(snapshot.Status, snapshot.Phase)
}

func agentSessionPromptCacheRunIDs(sessionID string, turns []AgentTurnSummary, entries []ObservedSessionEntry) []string {
	seen := map[string]struct{}{}
	out := []string{}
	add := func(id string) {
		id = strings.TrimSpace(id)
		if id == "" {
			return
		}
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	add(sessionID)
	for _, turn := range turns {
		add(turn.ID)
	}
	for _, entry := range entries {
		add(entry.TurnID)
	}
	return out
}

func (r *Runner) sessionSnapshotLocked(ctx context.Context, sess *agentSession) (AgentSessionSnapshot, error) {
	snap, err := sess.thread.Read(ctx)
	if err != nil {
		return AgentSessionSnapshot{}, err
	}
	lifecycle := sessionlifecycle.Derive(snap.Path, snap.Phase)
	turns := append([]AgentTurnSummary(nil), sess.turns...)
	active := observeMessages(snap.Context)
	pathEntries := observeEntries(snap.Path)
	allEntries := observeEntries(snap.Entries)
	snapshot := AgentSessionSnapshot{
		ID:                      sess.id,
		Status:                  lifecycle.Status(),
		Phase:                   lifecycle.Phase(),
		LeafID:                  snap.Meta.LeafID,
		CreatedAt:               sess.createdAt,
		UpdatedAt:               sess.updatedAt,
		Profile:                 sess.profile,
		SystemPrompt:            sess.systemPrompt,
		SelectedTools:           cloneSelectedTools(sess.selectedTools),
		HostedTools:             append([]provider.HostedToolDefinition(nil), sess.hostedTools...),
		UnavailableCapabilities: append([]string(nil), sess.unavailableCapabilities...),
		ContextPolicy:           sess.contextPolicy,
		LatestTurnID:            lifecycle.LatestTurnID(),
		WaitingPrompt:           lifecycle.WaitingPrompt(),
		Recoverable:             lifecycle.Recoverable(),
		CanAppendMessage:        lifecycle.CanAppendMessage(),
		Turns:                   turns,
		ActiveContext:           active,
		PathEntries:             pathEntries,
		AllEntries:              allEntries,
		AggregateMetrics:        aggregateTurnMetrics(turns),
		Compactions:             countCompactions(snap.Path),
	}
	storeAgentSessionSnapshot(sess, snapshot)
	return snapshot, nil
}

func (r *Runner) markAgentSessionRunningLocked(sess *agentSession, turnID string) {
	now := r.now()
	sess.updatedAt = now
	lifecycle := sessionlifecycle.Running(turnID)
	snapshot := loadAgentSessionSnapshot(sess)
	if snapshot.ID == "" {
		snapshot = AgentSessionSnapshot{
			ID:                      sess.id,
			CreatedAt:               sess.createdAt,
			Profile:                 sess.profile,
			SystemPrompt:            sess.systemPrompt,
			SelectedTools:           cloneSelectedTools(sess.selectedTools),
			HostedTools:             append([]provider.HostedToolDefinition(nil), sess.hostedTools...),
			UnavailableCapabilities: append([]string(nil), sess.unavailableCapabilities...),
			ContextPolicy:           sess.contextPolicy,
		}
	}
	snapshot.Status = lifecycle.Status()
	snapshot.Phase = lifecycle.Phase()
	snapshot.UpdatedAt = now
	snapshot.LatestTurnID = lifecycle.LatestTurnID()
	snapshot.WaitingPrompt = lifecycle.WaitingPrompt()
	snapshot.Recoverable = lifecycle.Recoverable()
	snapshot.CanAppendMessage = lifecycle.CanAppendMessage()
	storeAgentSessionSnapshot(sess, snapshot)
}

func runningAgentSessionSnapshot(sess *agentSession) AgentSessionSnapshot {
	snapshot := loadAgentSessionSnapshot(sess)
	lifecycle := sessionlifecycle.Running(snapshot.LatestTurnID)
	if snapshot.ID == "" {
		snapshot = AgentSessionSnapshot{
			ID:                      sess.id,
			CreatedAt:               sess.createdAt,
			UpdatedAt:               sess.createdAt,
			Profile:                 sess.profile,
			SystemPrompt:            sess.systemPrompt,
			SelectedTools:           cloneSelectedTools(sess.selectedTools),
			HostedTools:             append([]provider.HostedToolDefinition(nil), sess.hostedTools...),
			UnavailableCapabilities: append([]string(nil), sess.unavailableCapabilities...),
			ContextPolicy:           sess.contextPolicy,
		}
	}
	snapshot.Status = lifecycle.Status()
	snapshot.Phase = lifecycle.Phase()
	snapshot.LatestTurnID = lifecycle.LatestTurnID()
	snapshot.WaitingPrompt = lifecycle.WaitingPrompt()
	snapshot.Recoverable = lifecycle.Recoverable()
	snapshot.CanAppendMessage = lifecycle.CanAppendMessage()
	return snapshot
}

func storeAgentSessionSnapshot(sess *agentSession, snapshot AgentSessionSnapshot) {
	sess.snapshotMu.Lock()
	defer sess.snapshotMu.Unlock()
	sess.lastSnapshot = snapshot
}

func loadAgentSessionSnapshot(sess *agentSession) AgentSessionSnapshot {
	sess.snapshotMu.Lock()
	defer sess.snapshotMu.Unlock()
	return sess.lastSnapshot
}

func (r *Runner) agentObservationLocked(sess *agentSession, snapshot AgentSessionSnapshot, result engine.Result, turnID string) AgentObservation {
	observation := sess.provider.Snapshot()
	observation.SessionMessages = sessionMessagesFromEntries(snapshot.PathEntries)
	observation.ActiveContext = snapshot.ActiveContext
	observation.PathEntries = snapshot.PathEntries
	observation.Transitions = buildTransitions(eventsForRun(sess.recorder.Snapshot(), turnID), result)
	return observation
}

func eventsForRun(events []event.Event, runID string) []event.Event {
	if runID == "" {
		return append([]event.Event(nil), events...)
	}
	out := make([]event.Event, 0, len(events))
	for _, ev := range events {
		if ev.RunID == runID {
			out = append(out, ev)
		}
	}
	return out
}

func aggregateTurnMetrics(turns []AgentTurnSummary) engine.RunMetrics {
	var out engine.RunMetrics
	for _, turn := range turns {
		out.AddUsage(turn.Metrics.Usage)
		out.Steps += turn.Metrics.Steps
		out.LLMRequests += turn.Metrics.LLMRequests
		out.ToolCalls += turn.Metrics.ToolCalls
		out.Compactions += turn.Metrics.Compactions
		out.Retries += turn.Metrics.Retries
		out.WallTimeMS += turn.Metrics.WallTimeMS
	}
	return out
}

func countCompactions(entries []sessiontree.Entry) int {
	count := 0
	for _, entry := range entries {
		if entry.Type == sessiontree.EntryCompaction {
			count++
		}
	}
	return count
}

func sessionMessagesFromEntries(entries []ObservedSessionEntry) []ObservedSessionMessage {
	out := []ObservedSessionMessage{}
	for _, entry := range entries {
		switch entry.Type {
		case sessiontree.EntryUserMessage, sessiontree.EntryAssistantMessage, sessiontree.EntryToolCall, sessiontree.EntryToolResult:
			if entry.Message.Role != "" {
				out = append(out, entry.Message)
			}
		case sessiontree.EntryCompaction:
			out = append(out, ObservedSessionMessage{
				Role:                 string(session.Assistant),
				Content:              entry.Summary,
				Kind:                 string(session.MessageKindCompactionSummary),
				EntryID:              entry.ID,
				ParentEntryID:        entry.ParentID,
				CompactionID:         entry.CompactionID,
				CompactionGeneration: entry.CompactionGeneration,
				CompactionWindowID:   entry.CompactionWindowID,
			})
		}
	}
	return out
}

func observeEntries(entries []sessiontree.Entry) []ObservedSessionEntry {
	out := make([]ObservedSessionEntry, 0, len(entries))
	for _, entry := range entries {
		out = append(out, ObservedSessionEntry{
			ID:                      entry.ID,
			ParentID:                entry.ParentID,
			ThreadID:                entry.ThreadID,
			TurnID:                  entry.TurnID,
			Type:                    entry.Type,
			CreatedAt:               entry.CreatedAt,
			Message:                 observeEntryMessage(entry.Message),
			TurnStatus:              entry.TurnStatus,
			CompactionID:            entry.CompactionID,
			PreviousCompactionID:    entry.PreviousCompactionID,
			CompactedThroughEntryID: entry.CompactedThroughEntryID,
			SummarySchemaVersion:    entry.SummarySchemaVersion,
			CompactionGeneration:    entry.CompactionGeneration,
			CompactionWindowID:      entry.CompactionWindowID,
			FirstKeptEntryID:        entry.FirstKeptEntryID,
			Summary:                 entry.Summary,
			CompactionTrigger:       entry.CompactionTrigger,
			CompactionReason:        entry.CompactionReason,
			CompactionPhase:         entry.CompactionPhase,
			TokensBefore:            entry.TokensBefore,
			TokensAfterEstimate:     entry.TokensAfterEstimate,
			ContextUsageBefore:      entry.ContextUsageBefore,
			ContextUsageAfter:       entry.ContextUsageAfter,
			Error:                   entry.Error,
			Metadata:                cloneStringMap(entry.Metadata),
			RawHash:                 entry.RawHash,
		})
	}
	return out
}

func observeEntryMessage(msg session.Message) ObservedSessionMessage {
	items := observeMessages([]session.Message{msg})
	if len(items) == 0 {
		return ObservedSessionMessage{}
	}
	return items[0]
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func (r Runner) profileForRun(req AgentRunRequest) (ProviderProfile, error) {
	if req.Profile.Provider == "" && req.Profile.Model == "" && req.Profile.ID == "" {
		return r.profileByID(req.ProfileID)
	}
	profile := req.Profile
	if profile.ID == "" {
		profile.ID = req.ProfileID
	}
	profile = normalizeProfile(profile, 0)
	if profile.APIKey == "" {
		if saved, err := r.profileByID(profile.ID); err == nil {
			profile.APIKey = saved.APIKey
			profile.APIKeySet = saved.APIKey != "" || saved.APIKeySet
		} else if req.ProfileID != "" && req.ProfileID != profile.ID {
			if saved, err := r.profileByID(req.ProfileID); err == nil {
				profile.APIKey = saved.APIKey
				profile.APIKeySet = saved.APIKey != "" || saved.APIKeySet
			}
		}
	}
	return profile, nil
}

func resolvedProfileFromConfig(profile ProviderProfile, cfg config.Config, apiKeySet bool) ProviderProfile {
	return stripProfileSecret(ProviderProfile{
		ID:           profile.ID,
		Name:         profile.Name,
		Provider:     cfg.Provider,
		Model:        cfg.Model,
		BaseURL:      cfg.BaseURL,
		APIKey:       cfg.APIKey,
		APIKeySet:    apiKeySet,
		FakeResponse: cfg.FakeResponse,
		WebSearch:    profile.WebSearch,
	})
}

func (r Runner) Run(ctx context.Context, target string) RunResponse {
	if target == "" {
		target = TargetUnit
	}
	started := r.now()
	resp := RunResponse{
		ID:        fmt.Sprintf("%d", started.UnixNano()),
		Target:    target,
		StartedAt: started,
	}
	switch target {
	case TargetUnit:
		resp.Title = "Go package tests"
		resp.Kind = "command"
		resp = r.runGoTest(ctx, resp, false)
	case TargetRace:
		resp.Title = "Race-enabled package tests"
		resp.Kind = "command"
		resp = r.runGoTest(ctx, resp, true)
	case TargetEvalDemo:
		resp.Title = "Deterministic agent eval demo"
		resp.Kind = "agent"
		resp = r.runEvalDemo(ctx, resp)
	case TargetProviderSmoke:
		resp.Title = "Configured provider smoke"
		resp.Kind = "agent"
		resp = r.runProviderSmoke(ctx, resp)
	case TargetAll:
		resp.Title = "Full local suite"
		resp.Kind = "suite"
		resp = r.runAll(ctx, resp)
	default:
		resp.Status = "error"
		resp.Summary = "Unknown target."
		resp.Error = fmt.Sprintf("unknown run target %q", target)
		resp.FinishedAt = r.now()
		resp.DurationMS = resp.FinishedAt.Sub(resp.StartedAt).Milliseconds()
	}
	return resp
}

func (r Runner) runAll(ctx context.Context, resp RunResponse) RunResponse {
	targets := []string{TargetUnit, TargetRace, TargetEvalDemo}
	cfg := r.ConfigInfo()
	if cfg.LiveProvider || cfg.Provider == config.ProviderFake {
		targets = append(targets, TargetProviderSmoke)
	}
	status := "pass"
	for _, target := range targets {
		part := r.Run(ctx, target)
		resp.Parts = append(resp.Parts, part)
		if part.Status != "pass" && status == "pass" {
			status = part.Status
		}
	}
	resp.Status = status
	resp.FinishedAt = r.now()
	resp.DurationMS = resp.FinishedAt.Sub(resp.StartedAt).Milliseconds()
	resp.Summary = fmt.Sprintf("%d checks finished, status %s.", len(resp.Parts), status)
	return resp
}

func (r Runner) runGoTest(ctx context.Context, resp RunResponse, race bool) RunResponse {
	args := []string{"test", "-json"}
	if race {
		args = append(args, "-race")
	}
	args = append(args, "./...")
	resp.Command = append([]string{"go"}, args...)
	env := append(os.Environ(), "GOFLAGS=")
	output, exitCode := r.exec(ctx, "go", args, r.Root, env)
	resp.Output = string(output)
	resp.ExitCode = exitCode
	resp.Packages, resp.TestTotals = parseGoTestJSON(output)
	if ctx.Err() != nil {
		resp.Status = "error"
		resp.Error = ctx.Err().Error()
	} else if exitCode == 0 && resp.TestTotals.Failed == 0 {
		resp.Status = "pass"
	} else {
		resp.Status = "fail"
		resp.Error = "go test reported failures"
	}
	resp.FinishedAt = r.now()
	resp.DurationMS = resp.FinishedAt.Sub(resp.StartedAt).Milliseconds()
	resp.Summary = fmt.Sprintf("%d packages, %d tests, %d failed.", resp.TestTotals.Packages, resp.TestTotals.Tests, resp.TestTotals.Failed)
	return resp
}

func (r Runner) runEvalDemo(ctx context.Context, resp RunResponse) RunResponse {
	workspace, err := r.newRunWorkspace("eval")
	if err != nil {
		return r.failAgent(resp, err)
	}
	if err := initDemoGitWorkspace(ctx, workspace); err != nil {
		return r.failAgent(resp, err)
	}
	rec := &event.Recorder{}
	registry := tools.NewRegistry()
	if err := builtintools.RegisterWorkspaceMutation(registry, builtintools.WorkspaceOptions{Root: workspace}); err != nil {
		return r.failAgent(resp, err)
	}
	prov := harness.NewScriptedProvider(
		harness.Step(
			harness.Usage(provider.Usage{InputTokens: 42, OutputTokens: 8, CostUSD: 0, Source: provider.UsageNative}),
			harness.Tool("write-readme", "write", `{"path":"RESULT.txt","content":"floret eval passed\n"}`),
			harness.Done(),
		),
		harness.Step(
			harness.Usage(provider.Usage{InputTokens: 18, OutputTokens: 5, Source: provider.UsageEstimated}),
			harness.Text("Created RESULT.txt and verified the eval oracle."),
			harness.Done(),
		),
	)
	eng := &engine.Engine{
		Provider: prov,
		Store:    session.NewMemoryStore(),
		Memory:   &memory.Manager{SystemPrompt: "You are a deterministic Floret eval agent."},
		Tools:    registry,
		Sink:     rec,
		Approver: func(context.Context, tools.ApprovalRequest) (tools.PermissionDecision, error) {
			return tools.PermissionDecisionAllow, nil
		},
		Options: engine.Options{
			RunID:              "testui-eval-demo",
			SessionID:          "testui-eval-demo",
			TraceID:            "testui-eval-demo",
			ProviderName:       "scripted",
			Model:              "scripted-eval",
			MaxTotalTokens:     200,
			DuplicateToolLimit: 3,
		},
	}
	artifactsDir := filepath.Join(workspace, "artifacts")
	result, err := eval.Runner{
		Suite:        "test-ui",
		AgentVersion: "local",
		Provider:     "scripted",
		Model:        "scripted-eval",
		Workspace:    workspace,
		ArtifactsDir: artifactsDir,
		Engine:       eng,
		Trace:        rec,
	}.Run(ctx, eval.Case{
		ID:       "write-result",
		Title:    "Write and verify RESULT.txt",
		Category: "smoke",
		Prompt:   "Create RESULT.txt with the expected text.",
		Budgets:  eval.Budgets{MaxTotalTokens: 200},
		Oracle:   eval.Oracle{ExpectedFiles: map[string]string{"RESULT.txt": "floret eval passed\n"}},
	})
	if err != nil {
		return r.failAgent(resp, err)
	}
	events := rec.Snapshot()
	resp.Agent = &AgentRun{
		EngineStatus: string(result.EngineStatus),
		Output:       "Created RESULT.txt and verified the eval oracle.",
		Metrics:      result.Metrics,
		Events:       events,
		Eval:         &result,
		Artifacts:    readArtifacts(result.Artifacts),
		Config:       ConfigInfo{Provider: "scripted", Model: "scripted-eval"},
	}
	resp.Status = string(result.Status)
	if result.Status == eval.Pass {
		resp.Summary = fmt.Sprintf("Oracle passed with %d steps, %d tool call, %d tokens.", result.Metrics.Steps, result.Metrics.ToolCalls, result.Metrics.Usage.Normalized().TotalTokens)
	} else {
		resp.Summary = fmt.Sprintf("Eval failed: %s.", result.FailureCategory)
		resp.Error = result.EngineError
	}
	resp.FinishedAt = r.now()
	resp.DurationMS = resp.FinishedAt.Sub(resp.StartedAt).Milliseconds()
	return resp
}

func (r Runner) runProviderSmoke(ctx context.Context, resp RunResponse) RunResponse {
	cfg, err := config.Load(config.WithPath(r.EnvFile))
	if err != nil {
		return r.failAgent(resp, err)
	}
	cfg.RunID = fmt.Sprintf("testui-provider-%d", r.now().UnixNano())
	if cfg.WallTime == 0 {
		cfg.WallTime = 45 * time.Second
	}
	p, err := adapters.NewProvider(cfg)
	if err != nil {
		return r.failAgent(resp, err)
	}
	rec := &event.Recorder{}
	registry := tools.NewRegistry()
	promptStore := promptcache.NewFileStore(filepath.Join(r.Root, ".floret-test-ui", "prompt-cache"))
	eng := &engine.Engine{
		Provider: p,
		Store:    session.NewMemoryStore(),
		Prompt:   promptStore,
		Memory: &memory.Manager{
			SystemPrompt: "You are Floret's smoke-test assistant. Reply with a short success message. Do not call normal tools unless you need to ask the user for missing information.",
		},
		Tools: registry,
		Sink:  rec,
		Options: engine.Options{
			RunID:                   cfg.RunID,
			SessionID:               cfg.RunID,
			TraceID:                 cfg.RunID,
			ProviderName:            cfg.Provider,
			Model:                   cfg.Model,
			CacheRetention:          config.PromptCacheRetention(cfg),
			ContextPolicy:           cfg.ContextPolicy,
			MaxEmptyProviderRetries: cfg.MaxEmptyProviderRetries,
			NoProgressLimit:         cfg.NoProgressLimit,
			DuplicateToolLimit:      cfg.DuplicateToolLimit,
			WallTime:                cfg.WallTime,
			MaxTotalTokens:          4000,
			MaxCostUSD:              0.25,
		},
	}
	result := eng.Run(ctx, "Reply with a concise confirmation that the configured provider can run Floret.")
	resp.Agent = &AgentRun{
		EngineStatus: string(result.Status),
		Output:       result.Output,
		Metrics:      result.Metrics,
		Events:       rec.Snapshot(),
		Config:       r.ConfigInfo(),
	}
	if result.Err != nil {
		resp.Error = result.Err.Error()
	}
	if result.Status == engine.Completed {
		resp.Status = "pass"
		resp.Summary = fmt.Sprintf("Provider completed in %d step(s) with %d total tokens.", result.Metrics.Steps, result.Metrics.Usage.Normalized().TotalTokens)
	} else {
		resp.Status = "fail"
		resp.Summary = fmt.Sprintf("Provider ended with engine status %s.", result.Status)
	}
	resp.FinishedAt = r.now()
	resp.DurationMS = resp.FinishedAt.Sub(resp.StartedAt).Milliseconds()
	return resp
}

func (r Runner) newRunWorkspace(prefix string) (string, error) {
	root := filepath.Join(r.Root, ".floret-test-ui", "runs")
	if err := os.MkdirAll(root, 0o755); err != nil {
		return "", err
	}
	return os.MkdirTemp(root, prefix+"-*")
}

func (r Runner) failAgent(resp RunResponse, err error) RunResponse {
	resp.Status = "error"
	resp.Error = err.Error()
	resp.Summary = err.Error()
	resp.FinishedAt = r.now()
	resp.DurationMS = resp.FinishedAt.Sub(resp.StartedAt).Milliseconds()
	return resp
}

func buildTransitions(events []event.Event, result engine.Result) []StateTransition {
	transitions := []StateTransition{}
	current := "created"
	add := func(at time.Time, step int, to string, reason string, details string) {
		if to == "" || to == current {
			return
		}
		transitions = append(transitions, StateTransition{
			At:      at,
			Step:    step,
			From:    current,
			To:      to,
			Reason:  reason,
			Details: details,
		})
		current = to
	}
	for _, ev := range events {
		switch ev.Type {
		case event.StepStart:
			add(ev.Timestamp, ev.Step, "step_running", "step_start", fmt.Sprintf("step %d started", ev.Step))
		case event.ProviderRequest:
			add(ev.Timestamp, ev.Step, "provider_waiting", "provider_request", ev.Message)
		case event.ProviderDelta:
			add(ev.Timestamp, ev.Step, "receiving_model_output", "provider_delta", trimForDisplay(ev.Message, 80))
		case event.ProviderFinish:
			add(ev.Timestamp, ev.Step, "provider_finished", "provider_finish", finishDetails(ev))
		case event.ProviderRetry:
			add(ev.Timestamp, ev.Step, "retrying_provider", "provider_retry", ev.Message)
		case event.ContextCompact:
			add(ev.Timestamp, ev.Step, "compacting_context", "context_compact", "context was compacted")
		case event.ContextContinue:
			add(ev.Timestamp, ev.Step, "continuing_context", "context_continue", eventDetails(ev))
		case event.ToolCall:
			add(ev.Timestamp, ev.Step, "tool_calling", "tool_call", ev.ToolName)
		case event.ToolResult:
			add(ev.Timestamp, ev.Step, "tool_result_received", "tool_result", eventDetails(ev))
		case event.BudgetExceeded:
			add(ev.Timestamp, ev.Step, "budget_exceeded", "budget_exceeded", ev.Message)
		case event.StepEnd:
			add(ev.Timestamp, ev.Step, "step_finished", "step_end", decisionDetails(ev))
		case event.RunEnd:
			status := strings.TrimSpace(ev.Message)
			if status == "" {
				status = string(result.Status)
			}
			add(ev.Timestamp, ev.Step, status, "run_end", eventDetails(ev))
		}
	}
	if len(transitions) == 0 {
		add(time.Now(), 0, string(result.Status), "run_end", "")
	}
	return transitions
}

func agentSummary(result engine.Result) string {
	switch result.Status {
	case engine.Completed:
		return fmt.Sprintf("Completed in %d step(s), %d provider request(s), %d normal tool call(s).", result.Metrics.Steps, result.Metrics.LLMRequests, result.Metrics.ToolCalls)
	case engine.Waiting:
		return "The agent stopped to ask the user for more information."
	case engine.Cancelled:
		return "The run was cancelled or timed out."
	case engine.Failed:
		if result.Err != nil {
			return "The agent failed: " + result.Err.Error()
		}
		return "The agent failed."
	default:
		return "The agent run finished."
	}
}

func trimForDisplay(value string, limit int) string {
	value = strings.TrimSpace(value)
	if len(value) <= limit {
		return value
	}
	return value[:limit] + "..."
}

func eventDetails(ev event.Event) string {
	if ev.Err != "" {
		return ev.Err
	}
	if ev.Result != "" {
		return trimForDisplay(ev.Result, 120)
	}
	return trimForDisplay(ev.Message, 120)
}

func finishDetails(ev event.Event) string {
	parts := []string{}
	if ev.FinishReason != "" {
		parts = append(parts, "finish="+ev.FinishReason)
	}
	if ev.RawFinishReason != "" && ev.RawFinishReason != ev.FinishReason {
		parts = append(parts, "raw="+ev.RawFinishReason)
	}
	if ev.FinishInferred {
		parts = append(parts, "inferred")
	}
	if len(parts) == 0 {
		return "provider stream reached a terminal event"
	}
	return strings.Join(parts, " · ")
}

func decisionDetails(ev event.Event) string {
	parts := []string{}
	if ev.CompletionReason != "" {
		parts = append(parts, "completion="+ev.CompletionReason)
	}
	if ev.ContinuationReason != "" {
		parts = append(parts, "continue="+ev.ContinuationReason)
	}
	if ev.FinishReason != "" {
		parts = append(parts, "finish="+ev.FinishReason)
	}
	if ev.Message != "" {
		parts = append(parts, trimForDisplay(ev.Message, 80))
	}
	if len(parts) == 0 {
		return fmt.Sprintf("step %d ended", ev.Step)
	}
	return strings.Join(parts, " · ")
}

func (r Runner) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r Runner) exec(ctx context.Context, name string, args []string, dir string, env []string) ([]byte, int) {
	if r.Exec != nil {
		return r.Exec(ctx, name, args, dir, env)
	}
	return execCommand(ctx, name, args, dir, env)
}

func execCommand(ctx context.Context, name string, args []string, dir string, env []string) ([]byte, int) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	cmd.Env = env
	output, err := cmd.CombinedOutput()
	if err == nil {
		return output, 0
	}
	var exitErr *exec.ExitError
	if ok := errors.As(err, &exitErr); ok {
		return output, exitErr.ExitCode()
	}
	return append(output, []byte(err.Error())...), 1
}

func initDemoGitWorkspace(ctx context.Context, workspace string) error {
	for _, args := range [][]string{
		{"init"},
		{"config", "user.email", "test@example.com"},
		{"config", "user.name", "Test"},
	} {
		if err := runGit(ctx, workspace, args...); err != nil {
			return err
		}
	}
	if err := os.WriteFile(filepath.Join(workspace, "RESULT.txt"), []byte("before\n"), 0o644); err != nil {
		return err
	}
	for _, args := range [][]string{
		{"add", "RESULT.txt"},
		{"commit", "-m", "initial"},
	} {
		if err := runGit(ctx, workspace, args...); err != nil {
			return err
		}
	}
	return nil
}

func runGit(ctx context.Context, dir string, args ...string) error {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("git %s failed: %w\n%s", strings.Join(args, " "), err, output)
	}
	return nil
}

func readArtifacts(paths map[string]string) map[string]ArtifactSnapshot {
	out := map[string]ArtifactSnapshot{}
	for key, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			out[key] = ArtifactSnapshot{Path: path, Content: err.Error()}
			continue
		}
		text := string(data)
		if len(text) > 20000 {
			text = text[:20000] + "\n...[truncated]"
		}
		out[key] = ArtifactSnapshot{Path: path, Content: text}
	}
	return out
}

func parseGoTestJSON(output []byte) ([]PackageSummary, TestTotals) {
	type testEvent struct {
		Action  string  `json:"Action"`
		Package string  `json:"Package"`
		Test    string  `json:"Test"`
		Elapsed float64 `json:"Elapsed"`
	}
	packages := map[string]*PackageSummary{}
	order := []string{}
	scanner := bufio.NewScanner(bytes.NewReader(output))
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		var ev testEvent
		if err := json.Unmarshal(line, &ev); err != nil || ev.Package == "" {
			continue
		}
		pkg := packages[ev.Package]
		if pkg == nil {
			pkg = &PackageSummary{Name: ev.Package}
			packages[ev.Package] = pkg
			order = append(order, ev.Package)
		}
		if ev.Test != "" {
			switch ev.Action {
			case "pass":
				pkg.Passed++
			case "fail":
				pkg.Failed++
			case "skip":
				pkg.Skipped++
			}
			if ev.Action == "pass" || ev.Action == "fail" || ev.Action == "skip" {
				pkg.Tests++
			}
		} else {
			switch ev.Action {
			case "pass", "fail", "skip":
				pkg.Status = ev.Action
				pkg.ElapsedSec = ev.Elapsed
			}
		}
	}
	out := make([]PackageSummary, 0, len(order))
	totals := TestTotals{}
	for _, name := range order {
		pkg := *packages[name]
		if pkg.Status == "" {
			pkg.Status = "unknown"
		}
		out = append(out, pkg)
		totals.Packages++
		totals.Tests += pkg.Tests
		totals.Passed += pkg.Passed
		totals.Failed += pkg.Failed
		totals.Skipped += pkg.Skipped
	}
	return out, totals
}

func redactURL(raw string) string {
	if raw == "" {
		return ""
	}
	if at := strings.LastIndex(raw, "@"); at >= 0 {
		if scheme := strings.Index(raw, "://"); scheme >= 0 {
			return raw[:scheme+3] + "redacted@" + raw[at+1:]
		}
		return "redacted@" + raw[at+1:]
	}
	return raw
}

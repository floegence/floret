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

	"github.com/floegence/floret/agentharness"
	"github.com/floegence/floret/config"
	"github.com/floegence/floret/engine"
	"github.com/floegence/floret/event"
	"github.com/floegence/floret/internal/control"
	"github.com/floegence/floret/internal/searchcap"
	"github.com/floegence/floret/internal/sessionlifecycle"
	"github.com/floegence/floret/provider"
	"github.com/floegence/floret/provider/adapters"
	"github.com/floegence/floret/provider/cache"
	"github.com/floegence/floret/provider/catalog"
	"github.com/floegence/floret/runtime/storage/sqlite"
	"github.com/floegence/floret/session"
	"github.com/floegence/floret/session/artifact"
	"github.com/floegence/floret/session/compaction"
	"github.com/floegence/floret/session/contextpolicy"
	"github.com/floegence/floret/sessiontree"
	"github.com/floegence/floret/testing/eval"
	"github.com/floegence/floret/testing/harness"
	"github.com/floegence/floret/tools"
	"github.com/floegence/floret/tools/builtin"
	"github.com/floegence/floret/tools/mcp"
	"github.com/floegence/floret/tools/skills"
)

const (
	TargetUnit              = "unit"
	TargetRace              = "race"
	TargetEvalDemo          = "eval-demo"
	TargetProviderSmoke     = "provider-smoke"
	TargetToolScenarios     = "tool-scenarios"
	TargetLiveToolScenarios = "live-tool-scenarios"
	TargetAll               = "all"
)

var (
	errAgentSessionBusy  = errors.New("agent session is running")
	errAgentSessionInput = errors.New("agent session input error")
)

const agentSessionTurnLockTimeout = 250 * time.Millisecond

type Runner struct {
	Root                string
	EnvFile             string
	Now                 func() time.Time
	Exec                func(context.Context, string, []string, string, []string) ([]byte, int)
	ProviderFactory     func(config.Config) (provider.Provider, error)
	Sessions            *agentSessionRegistry
	AllowDebugRaw       bool
	StorageMode         string
	StoragePath         string
	StorageImportLegacy bool
	storageSQLite       *sqlite.Store
	storageMemory       *memoryStorage
}

func NewRunner(root string) Runner {
	return Runner{
		Root:                root,
		EnvFile:             filepath.Join(root, config.DefaultEnvFile),
		Now:                 time.Now,
		Exec:                execCommand,
		ProviderFactory:     adapters.NewProvider,
		Sessions:            newAgentSessionRegistry(),
		StorageMode:         StorageModeSQLite,
		StorageImportLegacy: true,
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
	debugRaw                bool
	profile                 ProviderProfile
	systemPrompt            string
	selectedTools           []string
	hostedTools             []provider.HostedToolDefinition
	unavailableCapabilities []string
	capabilities            CapabilityState
	mcpManager              *mcp.Manager
	contextPolicy           contextpolicy.Policy
	cfg                     config.Config
	provider                *observingProvider
	recorder                agentEventRecorder
	harnessRecorder         agentHarnessRecorder
	repo                    sessiontree.Repo
	promptStore             cache.Store
	registry                *tools.Registry
	harness                 *agentharness.AgentHarness
	thread                  *agentharness.Thread
	nextID                  func(string) string
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
	capabilities            CapabilityState
	mcpManager              *mcp.Manager
	harness                 *agentharness.AgentHarness
	thread                  *agentharness.Thread
	nextID                  func(string) string
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

func (r Runner) debugRawAllowed(requested bool) bool {
	return requested && r.AllowDebugRaw
}

func agentSessionSinkPolicy(debugRaw bool) event.SinkPolicy {
	if !debugRaw {
		return event.SinkPolicy{}
	}
	return event.SinkPolicy{AllowRaw: true, Redactor: event.SafePathRefsText}
}

func (r *Runner) ConfigInfo() ConfigInfo {
	info := ConfigInfo{EnvFile: r.EnvFile}
	info.Storage = r.storageStatus(context.Background())
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
	return catalog.Providers()
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
		DebugRaw:      r.debugRawAllowed(req.DebugRaw),
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
	snapshot, err := r.sessionSnapshotLocked(ctx, sess)
	if err != nil {
		return AgentSessionSnapshot{}, err
	}
	return publicAgentSessionSnapshot(snapshot, r.debugRawAllowed(req.DebugRaw)), nil
}

func (r *Runner) RunInterfaceProbe(ctx context.Context, req AgentInterfaceProbeRequest) AgentRunResponse {
	started := r.now()
	resp := AgentRunResponse{
		ID:        fmt.Sprintf("%d", started.UnixNano()),
		Probe:     true,
		StartedAt: started,
	}
	profile, err := r.profileByID(req.ProfileID)
	if err != nil {
		return publicAgentRunResponse(r.failAgentRunWithStatus(resp, http.StatusBadRequest, err), r.debugRawAllowed(req.DebugRaw))
	}
	selectedTools, err := normalizeAgentSessionToolsForProfile(req.SelectedTools, "", profile, r.EnvFile)
	if err != nil {
		return publicAgentRunResponse(r.failAgentRunWithStatus(resp, http.StatusBadRequest, err), r.debugRawAllowed(req.DebugRaw))
	}
	probe := *r
	probe.ProviderFactory = func(config.Config) (provider.Provider, error) {
		if slices.Contains(selectedTools, builtin.ToolList) {
			return harness.NewScriptedProvider(
				harness.Step(
					provider.StreamEvent{Type: provider.Reasoning, Text: "Inspect selected tool contract."},
					harness.Text("Checking selected tool definitions before running a low-risk read probe."),
					harness.Tool("probe-list", builtin.ToolList, `{"path":null,"limit":5}`),
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
		Model:                   "fake-model",
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
		return publicAgentRunResponse(r.failAgentRun(resp, err), r.debugRawAllowed(req.DebugRaw))
	}
	sessionID := "testui-probe-" + resp.ID
	cfg.RunID = sessionID
	sess, err := probe.buildAgentSession(ctx, agentSessionBuildOptions{
		ID:            sessionID,
		Transient:     true,
		DebugRaw:      r.debugRawAllowed(req.DebugRaw),
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
		return publicAgentRunResponse(r.failAgentRun(resp, err), r.debugRawAllowed(req.DebugRaw))
	}
	resp.Profile = stripProfileSecret(profile)
	result := probe.runAgentTurn(ctx, sess, resp, "Run the test UI tool contract probe for the selected tools.")
	result.Probe = true
	if result.Status == string(engine.Completed) {
		result.Summary = "Interface probe passed: selected tools were bound to a transient session and captured in the provider request."
	}
	return publicAgentRunResponse(result, r.debugRawAllowed(req.DebugRaw))
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
		return publicAgentRunResponse(resp, r.debugRawAllowed(req.DebugRaw))
	}
	profile, err := r.profileForRun(req)
	if err != nil {
		return publicAgentRunResponse(r.failAgentRunWithStatus(resp, http.StatusBadRequest, err), r.debugRawAllowed(req.DebugRaw))
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
	}
	if cfg.SystemPrompt == "" {
		cfg.SystemPrompt = "You are Floret. Answer naturally when the user's request is complete, or call ask_user if you need missing information."
	}
	cfg, err = config.Resolve(cfg, nil)
	if err != nil {
		return publicAgentRunResponse(r.failAgentRun(resp, err), r.debugRawAllowed(req.DebugRaw))
	}
	sessionID := "testui-session-" + resp.ID
	cfg.RunID = sessionID
	resolvedProfile := resolvedProfileFromConfig(profile, cfg, cfg.APIKey != "" || profile.APIKeySet)
	resp.Profile = stripProfileSecret(resolvedProfile)
	selectedTools, err := normalizeAgentSessionToolsForProfile(req.SelectedTools, req.ToolMode, resolvedProfile, r.EnvFile)
	if err != nil {
		return publicAgentRunResponse(r.failAgentRunWithStatus(resp, http.StatusBadRequest, err), r.debugRawAllowed(req.DebugRaw))
	}
	sess, err := r.buildAgentSession(ctx, agentSessionBuildOptions{
		ID:            sessionID,
		DebugRaw:      r.debugRawAllowed(req.DebugRaw),
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
		return publicAgentRunResponse(r.failAgentRun(resp, err), r.debugRawAllowed(req.DebugRaw))
	}
	r.sessionRegistry().put(sess)
	if err := r.saveAgentSessionMetadata(r.metadataFromSession(sess)); err != nil {
		return publicAgentRunResponse(r.failAgentRun(resp, err), r.debugRawAllowed(req.DebugRaw))
	}
	debugRaw := r.debugRawAllowed(req.DebugRaw)
	sess.debugRaw = debugRaw
	defer func() {
		sess.debugRaw = false
	}()
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
	debugRaw := r.debugRawAllowed(req.DebugRaw)
	if strings.TrimSpace(req.Message) == "" {
		resp.Status = "error"
		resp.StatusCode = http.StatusBadRequest
		resp.Error = "message is required"
		resp.Summary = resp.Error
		resp.FinishedAt = r.now()
		resp.DurationMS = resp.FinishedAt.Sub(resp.StartedAt).Milliseconds()
		return publicAgentRunResponse(resp, debugRaw)
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
			return publicAgentRunResponse(r.failAgentRunWithStatus(resp, status, err), debugRaw)
		}
	}
	resp.Profile = stripProfileSecret(sess.profile)
	sess.debugRaw = debugRaw
	defer func() {
		sess.debugRaw = false
	}()
	if err := lockAgentSessionForTurn(ctx, sess); err != nil {
		return publicAgentRunResponse(r.failAgentRunWithStatus(resp, http.StatusConflict, fmt.Errorf("agent session %q is already running", sessionID)), debugRaw)
	}
	defer sess.mu.Unlock()
	snapshot, err := r.sessionSnapshotLocked(ctx, sess)
	if err != nil {
		return publicAgentRunResponse(r.failAgentRun(resp, err), debugRaw)
	}
	if !snapshot.CanAppendMessage {
		return publicAgentRunResponse(r.failAgentRunWithStatus(resp, http.StatusConflict, fmt.Errorf("agent session %q is %s and cannot accept a new message", sessionID, snapshot.Status)), debugRaw)
	}
	turnID := sess.nextTurnID()
	r.markAgentSessionRunningLocked(sess, turnID)
	if sink != nil {
		setAgentSessionStreamSink(sess, sink)
		defer setAgentSessionStreamSink(sess, nil)
	}
	result := r.runAgentTurnLocked(ctx, sess, resp, req.Message, turnID)
	if sink != nil {
		if result.Session.ID != "" {
			snapshotCopy := publicAgentSessionSnapshot(result.Session, debugRaw)
			sink.EmitAgentStream(AgentStreamEvent{
				Type:      AgentStreamSessionSnapshot,
				SessionID: sessionID,
				TurnID:    result.TurnID,
				At:        r.now(),
				Snapshot:  &snapshotCopy,
			})
		}
		resultCopy := publicAgentRunResponse(result, debugRaw)
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
	return publicAgentRunResponse(result, debugRaw)
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
		return publicAgentSessionSnapshot(current, false), nil
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
	next, err := r.sessionSnapshotLocked(ctx, sess)
	if err != nil {
		return AgentSessionSnapshot{}, err
	}
	return publicAgentSessionSnapshot(next, false), nil
}

func (r *Runner) DeleteAgentSession(ctx context.Context, sessionID string) error {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return fmt.Errorf("%w: session id is required", errAgentSessionInput)
	}
	registry := r.sessionRegistry()
	runIDs := []string{sessionID}
	store, err := r.sessionStorage(ctx)
	if err != nil {
		return err
	}
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
		if sess.mcpManager != nil {
			_ = sess.mcpManager.Close()
			sess.mcpManager = nil
		}
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
	if err := store.deleteSession(ctx, r.Root, sessionID, runIDs, func() error {
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
	}); err != nil {
		return err
	}
	return os.RemoveAll(toolOutputArtifactSessionDir(r.managedArtifactsRoot(), sessionID))
}

func (r *Runner) AgentSession(ctx context.Context, sessionID string, debugRaw bool) (AgentSessionSnapshot, error) {
	debugRaw = r.debugRawAllowed(debugRaw)
	sess, ok := r.sessionRegistry().get(sessionID)
	if !ok {
		meta, err := r.loadAgentSessionMetadata(sessionID)
		if err != nil {
			return AgentSessionSnapshot{}, fmt.Errorf("agent session %q not found", sessionID)
		}
		snapshot, err := r.sessionSnapshotFromMetadata(ctx, meta)
		if err != nil {
			return AgentSessionSnapshot{}, err
		}
		return publicAgentSessionSnapshot(snapshot, debugRaw), nil
	}
	if !sess.mu.TryLock() {
		return publicAgentSessionSnapshot(r.runningAgentSessionSnapshot(ctx, sess), debugRaw), nil
	}
	defer sess.mu.Unlock()
	snapshot, err := r.sessionSnapshotLocked(ctx, sess)
	if err != nil {
		return AgentSessionSnapshot{}, err
	}
	return publicAgentSessionSnapshot(snapshot, debugRaw), nil
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
	capabilities, skillPrompt, mcpManager, err := r.registerAgentCapabilities(registry, rec)
	if err != nil {
		return agentSessionRuntime{}, err
	}
	systemPrompt := appendCapabilityPrompt(sess.systemPrompt, skillPrompt)
	idGenerator := r.agentSessionIDGenerator(ctx, sess.repo, sess.id)
	h := agentharness.New(agentharness.Options{
		Provider:     observed,
		ProviderName: sess.cfg.Provider,
		Model:        sess.cfg.Model,
		SystemPrompt: systemPrompt,
		Tools:        registry,
		PromptStore:  sess.promptStore,
		Repo:         sess.repo,
		Artifacts:    newToolOutputArtifactStore(r.managedArtifactsRoot()),
		Sink:         rec,
		SinkPolicy:   agentSessionSinkPolicy(sess.debugRaw),
		HarnessSink:  harnessRec,
		Approver:     testUIToolApprover,
		TurnPolicy: agentharness.TurnPolicy{
			CacheRetention:        config.PromptCacheRetention(sess.cfg),
			ContextPolicy:         sess.contextPolicy,
			HostedToolDefinitions: hostedTools,
		},
		LoopLimits: agentharness.LoopLimits{
			MaxEmptyProviderRetries: sess.cfg.MaxEmptyProviderRetries,
			NoProgressLimit:         sess.cfg.NoProgressLimit,
			DuplicateToolLimit:      sess.cfg.DuplicateToolLimit,
			WallTime:                sess.cfg.WallTime,
			MaxCostUSD:              1.00,
		},
		NewID: idGenerator,
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
		capabilities:            capabilities,
		mcpManager:              mcpManager,
		harness:                 h,
		thread:                  thread,
		nextID:                  idGenerator,
	}, nil
}

func (sess *agentSession) applyRuntime(runtime agentSessionRuntime) {
	if sess.mcpManager != nil {
		_ = sess.mcpManager.Close()
	}
	sess.provider = runtime.provider
	sess.recorder = runtime.recorder
	sess.harnessRecorder = runtime.harnessRecorder
	sess.registry = runtime.registry
	sess.hostedTools = runtime.hostedTools
	sess.unavailableCapabilities = runtime.unavailableCapabilities
	sess.capabilities = runtime.capabilities
	sess.mcpManager = runtime.mcpManager
	sess.harness = runtime.harness
	sess.thread = runtime.thread
	sess.nextID = runtime.nextID
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
			out = append(out, publicAgentSessionSnapshot(r.runningAgentSessionSnapshot(ctx, sess), false))
			continue
		}
		snap, err := r.sessionSnapshotLocked(ctx, sess)
		sess.mu.Unlock()
		if err == nil {
			out = append(out, publicAgentSessionSnapshot(snap, false))
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
			out = append(out, publicAgentSessionSnapshot(snap, false))
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
	DebugRaw      bool
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
	var repo sessiontree.Repo
	var promptStore cache.Store
	if opts.Transient {
		repo = sessiontree.NewMemoryRepo()
		promptStore = cache.NewMemoryStore()
	} else {
		store, err := r.sessionStorage(ctx)
		if err != nil {
			return nil, err
		}
		repo = store.repo(r.Root)
		promptStore = store.prompt(r.Root)
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
	capabilities, skillPrompt, mcpManager, err := r.registerAgentCapabilities(registry, rec)
	if err != nil {
		return nil, err
	}
	capabilities.Diagnostics = append(capabilities.Diagnostics, modelRiskDiagnostics(opts.Profile, cfg.ContextPolicy)...)
	systemPrompt := appendCapabilityPrompt(cfg.SystemPrompt, skillPrompt)
	idGenerator := r.agentSessionIDGenerator(ctx, repo, opts.ID)
	h := agentharness.New(agentharness.Options{
		Provider:     observed,
		ProviderName: cfg.Provider,
		Model:        cfg.Model,
		SystemPrompt: systemPrompt,
		Tools:        registry,
		PromptStore:  promptStore,
		Repo:         repo,
		Artifacts:    newToolOutputArtifactStore(r.managedArtifactsRoot()),
		Sink:         rec,
		SinkPolicy:   agentSessionSinkPolicy(opts.DebugRaw),
		HarnessSink:  harnessRec,
		Approver:     testUIToolApprover,
		TurnPolicy: agentharness.TurnPolicy{
			CacheRetention:        config.PromptCacheRetention(cfg),
			ContextPolicy:         cfg.ContextPolicy,
			HostedToolDefinitions: hostedTools,
		},
		LoopLimits: agentharness.LoopLimits{
			MaxEmptyProviderRetries: cfg.MaxEmptyProviderRetries,
			NoProgressLimit:         cfg.NoProgressLimit,
			DuplicateToolLimit:      cfg.DuplicateToolLimit,
			WallTime:                cfg.WallTime,
			MaxCostUSD:              1.00,
		},
		NewID: idGenerator,
		Now:   r.now,
	})
	var thread *agentharness.Thread
	if opts.Start {
		thread, err = h.StartThread(ctx, agentharness.StartThreadOptions{ThreadID: opts.ID})
	} else {
		thread, err = h.ResumeThread(ctx, opts.ID, agentharness.ResumeOptions{})
	}
	if err != nil {
		if mcpManager != nil {
			_ = mcpManager.Close()
		}
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
		debugRaw:                opts.DebugRaw,
		profile:                 opts.Profile,
		systemPrompt:            cfg.SystemPrompt,
		selectedTools:           selectedTools,
		hostedTools:             hostedTools,
		unavailableCapabilities: unavailableCapabilities,
		capabilities:            capabilities,
		mcpManager:              mcpManager,
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
		nextID:                  idGenerator,
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

func testUIToolApprover(_ context.Context, req tools.ApprovalRequest) (tools.PermissionDecision, error) {
	if strings.HasPrefix(strings.TrimSpace(req.Name), "mcp__") {
		return tools.PermissionDecisionDeny, nil
	}
	return tools.PermissionDecisionAllow, nil
}

func (r *Runner) registerAgentCapabilities(registry *tools.Registry, sink event.Sink) (CapabilityState, string, *mcp.Manager, error) {
	state := CapabilityState{}
	cfg, err := config.Load(config.WithPath(r.EnvFile))
	if err != nil {
		state.Diagnostics = append(state.Diagnostics, CapabilityDiagnostic{Kind: "config_invalid", Message: err.Error(), NextAction: "Fix .env.local before enabling skills."})
		return state, "", nil, nil
	}
	mcpConfigured, mcpServers, mcpDiagnostics := r.loadMCPServersFromEnv()
	state.Diagnostics = append(state.Diagnostics, mcpDiagnostics...)
	var manager *mcp.Manager
	if mcpConfigured {
		manager = mcp.NewManager(mcp.Options{Sink: testUIMCPSink{sink: sink}})
		if err := manager.Start(context.Background(), mcpServers); err != nil {
			state.Diagnostics = append(state.Diagnostics, CapabilityDiagnostic{Kind: "mcp_required_failed", Message: err.Error(), NextAction: "Fix or disable the required MCP server in FLORET_MCP_CONFIG."})
			_ = manager.Close()
			return state, "", nil, err
		}
		if err := manager.RegisterTools(registry); err != nil {
			_ = manager.Close()
			return state, "", nil, err
		}
		state.MCPServers = mcpCapabilityStates(manager.Snapshots())
	} else {
		state.Diagnostics = append(state.Diagnostics, CapabilityDiagnostic{
			Kind:       "mcp_not_configured",
			Capability: "mcp",
			Message:    "No MCP servers provided by host.",
			NextAction: "Set FLORET_MCP_CONFIG to a host-managed MCP server config file.",
		})
	}
	if !cfg.SkillsEnabled {
		state.SkillSources = skillSourceStates(cfg.SkillSources, nil, r.managedSkillRoot(), false)
		state.Diagnostics = append(state.Diagnostics, managedSkillRootDiagnostic(r.managedSkillRoot()))
		return state, "", manager, nil
	}
	sources := make([]skills.Source, 0, len(cfg.SkillSources))
	for _, root := range cfg.SkillSources {
		sources = append(sources, skills.Source{Root: root, Kind: skills.SourceConfig, Enabled: true, DisplayLabel: "config"})
	}
	catalog, err := skills.Discover(sources)
	if err != nil {
		if manager != nil {
			_ = manager.Close()
		}
		return state, "", nil, err
	}
	state.SkillSources = skillSourceStates(cfg.SkillSources, catalog.Skills, r.managedSkillRoot(), true)
	for _, diagnostic := range catalog.Diagnostics {
		state.Diagnostics = append(state.Diagnostics, CapabilityDiagnostic{
			Kind:       diagnostic.Kind,
			Capability: diagnostic.SkillName,
			SourceKind: string(diagnostic.SourceKind),
			Message:    diagnostic.Message,
			NextAction: "Fix or remove the downstream skill source entry.",
		})
	}
	for _, skill := range catalog.Skills {
		state.Skills = append(state.Skills, SkillCapabilityState{
			Name:         skill.Name,
			Description:  skill.Description,
			SourceKind:   string(skill.SourceInfo.Kind),
			SourceLabel:  skill.SourceInfo.DisplayLabel,
			RelativePath: skill.SourceInfo.RelativePath,
			ContentHash:  skill.ContentHash,
			License:      licenseForInstalledSkill(filepath.Dir(skill.Path)),
			Status:       "detected",
		})
		if sink != nil {
			sink.Emit(event.Event{Type: event.SkillDetected, Metadata: map[string]any{
				"skill_id":     skill.Name,
				"source_kind":  string(skill.SourceInfo.Kind),
				"source_label": skill.SourceInfo.DisplayLabel,
				"content_hash": skill.ContentHash,
			}})
		}
	}
	prompt, promptDiagnostics := skills.BuildPrompt(catalog.Skills, skills.PromptOptions{MaxBytes: cfg.SkillPromptBudgetBytes})
	for _, diagnostic := range promptDiagnostics {
		state.Diagnostics = append(state.Diagnostics, CapabilityDiagnostic{Kind: diagnostic.Kind, Capability: "skills", Message: diagnostic.Message, NextAction: "Raise FLORET_SKILL_PROMPT_BUDGET_BYTES or reduce available skills."})
	}
	if prompt != "" && sink != nil {
		sink.Emit(event.Event{Type: event.SkillDisclosureApplied, Metadata: map[string]any{
			"skill_count":   len(catalog.Skills),
			"prompt_bytes":  len(prompt),
			"prompt_sha256": event.StableHash(prompt),
		}})
	}
	if len(catalog.Skills) == 0 {
		return state, prompt, manager, nil
	}
	tool, err := skills.DefineSkillTool(catalog.Skills, skills.ToolOptions{
		OutputPolicy: tools.OutputPolicy{VisibleMaxBytes: 64 * 1024, Strategy: tools.OutputHead, PreserveFull: true},
		OnLoad: func(load skills.SkillLoad) {
			if sink != nil {
				sink.Emit(event.Event{Type: event.SkillLoaded, Metadata: map[string]any{
					"skill_id":     load.Name,
					"source_kind":  string(load.SourceKind),
					"content_hash": load.ContentHash,
					"bytes":        load.Bytes,
				}})
			}
		},
	})
	if err != nil {
		if manager != nil {
			_ = manager.Close()
		}
		return state, "", nil, err
	}
	if err := registry.Register(tool); err != nil {
		if manager != nil {
			_ = manager.Close()
		}
		return state, "", nil, err
	}
	return state, prompt, manager, nil
}

type testUIMCPSink struct {
	sink event.Sink
}

func (s testUIMCPSink) EmitMCP(diag mcp.Diagnostic) {
	if s.sink == nil {
		return
	}
	s.sink.Emit(event.Event{Type: event.Type(diag.Type), Metadata: map[string]any{
		"server_id":        diag.ServerName,
		"transport":        string(diag.Transport),
		"status":           string(diag.Status),
		"tool_name":        diag.ToolName,
		"tool_count":       diag.ToolCount,
		"protocol_version": diag.ProtocolVersion,
		"failure_category": diag.FailureCategory,
		"next_action":      diag.NextAction,
		"message":          diag.Message,
	}})
}

type mcpConfigFile struct {
	Servers []mcp.ServerConfig `json:"servers"`
}

func (r *Runner) loadMCPServersFromEnv() (bool, []mcp.ServerConfig, []CapabilityDiagnostic) {
	values, err := readDotEnv(r.EnvFile)
	if err != nil && !os.IsNotExist(err) {
		return false, nil, []CapabilityDiagnostic{{Kind: "mcp_config_unreadable", Capability: "mcp", Message: err.Error(), NextAction: "Fix .env.local before configuring MCP servers."}}
	}
	path := strings.TrimSpace(values["FLORET_MCP_CONFIG"])
	if path == "" {
		return false, nil, nil
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(r.Root, path)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return true, nil, []CapabilityDiagnostic{{Kind: "mcp_config_unreadable", Capability: "mcp", Message: err.Error(), NextAction: "Create the host-managed MCP config file or update FLORET_MCP_CONFIG."}}
	}
	var servers []mcp.ServerConfig
	if err := json.Unmarshal(data, &servers); err != nil {
		var wrapped mcpConfigFile
		if wrappedErr := json.Unmarshal(data, &wrapped); wrappedErr != nil {
			return true, nil, []CapabilityDiagnostic{{Kind: "mcp_config_invalid", Capability: "mcp", Message: err.Error(), NextAction: "Use a JSON array of MCP server configs or an object with a servers array."}}
		}
		servers = wrapped.Servers
	}
	if len(servers) == 0 {
		return true, nil, []CapabilityDiagnostic{{Kind: "mcp_config_empty", Capability: "mcp", Message: "MCP config does not contain any servers.", NextAction: "Add host-managed MCP server configs or unset FLORET_MCP_CONFIG."}}
	}
	return true, servers, nil
}

func mcpCapabilityStates(snapshots []mcp.Snapshot) []MCPCapabilityState {
	out := make([]MCPCapabilityState, 0, len(snapshots))
	for _, snapshot := range snapshots {
		nextAction := ""
		failure := ""
		if snapshot.Status == mcp.StatusFailed {
			failure = "connection_failed"
			nextAction = "Check that the downstream host installed and enabled this MCP server."
		}
		out = append(out, MCPCapabilityState{
			Name:            snapshot.ServerName,
			Status:          string(snapshot.Status),
			Transport:       string(snapshot.Transport),
			ToolCount:       snapshot.ToolCount,
			PermissionMode:  string(snapshot.DefaultPermission),
			FailureCategory: failure,
			NextAction:      nextAction,
		})
	}
	return out
}

func appendCapabilityPrompt(base, addition string) string {
	base = strings.TrimRight(base, "\n")
	addition = strings.TrimSpace(addition)
	if addition == "" {
		return base
	}
	if base == "" {
		return addition
	}
	return base + "\n\n" + addition
}

func skillSourceStates(roots []string, discovered []skills.Skill, managedRoot string, enabled bool) []SkillSourceState {
	out := make([]SkillSourceState, 0, len(roots))
	counts := map[string]int{}
	for _, skill := range discovered {
		root := strings.TrimSpace(skill.SourceInfo.Root)
		if root != "" {
			counts[root]++
		}
	}
	for _, root := range roots {
		root = strings.TrimSpace(root)
		if root == "" {
			continue
		}
		out = append(out, SkillSourceState{
			Root:       root,
			Kind:       string(skills.SourceConfig),
			Label:      "config",
			Enabled:    enabled,
			Managed:    samePath(root, managedRoot),
			SkillCount: counts[root],
		})
	}
	if len(out) == 0 {
		out = append(out, SkillSourceState{
			Root:    managedRoot,
			Kind:    string(skills.SourceConfig),
			Label:   "test UI managed",
			Enabled: false,
			Managed: true,
		})
	}
	return out
}

func samePath(a, b string) bool {
	aa, errA := filepath.Abs(a)
	bb, errB := filepath.Abs(b)
	if errA != nil || errB != nil {
		return a == b
	}
	return aa == bb
}

func (r *Runner) capabilityStateFromEnv() CapabilityState {
	registry := tools.NewRegistry()
	state, _, manager, err := r.registerAgentCapabilities(registry, nil)
	if manager != nil {
		_ = manager.Close()
	}
	if err != nil {
		return CapabilityState{Diagnostics: []CapabilityDiagnostic{{Kind: "capability_error", Message: err.Error()}}}
	}
	return state
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
	store, err := r.sessionStorage(ctx)
	if err != nil {
		return AgentSessionSnapshot{}, err
	}
	repo := store.repo(r.Root)
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
	turns := summariesFromEntries(entries, meta.Turns)
	projection := observeContextProjection(sessiontree.BuildContextProjection(path, sessiontree.ContextProjectionOptions{Purpose: sessiontree.ProjectionTestUI}))
	active := projection.Messages
	pathEntries := observeEntries(path)
	allEntries := observeEntries(entries)
	updatedAt := meta.UpdatedAt
	if thread.UpdatedAt.After(updatedAt) {
		updatedAt = thread.UpdatedAt
	}
	if !slices.Equal(turns, meta.Turns) || updatedAt.After(meta.UpdatedAt) {
		meta.Turns = append([]AgentTurnSummary(nil), turns...)
		meta.UpdatedAt = updatedAt
		_ = r.saveAgentSessionMetadata(meta)
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
		Capabilities:            r.capabilityStateFromEnv(),
		ContextPolicy:           meta.ContextPolicy,
		LatestTurnID:            lifecycle.LatestTurnID(),
		WaitingPrompt:           lifecycle.WaitingPrompt(),
		Recoverable:             lifecycle.Recoverable(),
		CanAppendMessage:        lifecycle.CanAppendMessage(),
		Turns:                   turns,
		ActiveContext:           active,
		ContextProjection:       projection,
		PathEntries:             pathEntries,
		AllEntries:              allEntries,
		AggregateMetrics:        aggregateTurnMetrics(turns),
		Compactions:             countCompactions(path),
	}, nil
}

func (r *Runner) runAgentTurn(ctx context.Context, sess *agentSession, resp AgentRunResponse, message string) AgentRunResponse {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	turnID := sess.nextTurnID()
	r.markAgentSessionRunningLocked(sess, turnID)
	result := r.runAgentTurnLocked(ctx, sess, resp, message, turnID)
	return publicAgentRunResponse(result, sess.debugRaw)
}

func (r *Runner) runAgentTurnLocked(ctx context.Context, sess *agentSession, resp AgentRunResponse, message string, turnID string) AgentRunResponse {
	turn, err := sess.thread.Run(ctx, message, agentharness.RunOptions{TurnID: turnID})
	if err != nil && turn.Status == "" {
		return r.failAgentRun(resp, err)
	}
	finalCtx, cancelFinal := agentTurnResponseFinalizationContext(ctx)
	defer cancelFinal()
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
	resp.Diagnostics = cloneStringMap(turn.Diagnostics)
	resp.Diagnostics = withDiagnostics(resp.Diagnostics, modelRiskDiagnosticMap(sess.profile, sess.contextPolicy))
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
	sess.turns = upsertAgentTurnSummary(sess.turns, summary)
	snapshot, snapErr := r.sessionSnapshotLocked(finalCtx, sess)
	if snapErr != nil {
		resp.Diagnostics = withDiagnostic(resp.Diagnostics, "final_snapshot_error", snapErr.Error())
		snapshot = r.fallbackAgentSessionSnapshot(sess, turn.Status)
	} else {
		sess.updatedAt = snapshot.UpdatedAt
		if snap, err := sess.thread.Journal(finalCtx); err == nil {
			sess.turns = summariesFromEntries(snap.Entries, sess.turns)
		}
		snapshot.Turns = append([]AgentTurnSummary(nil), sess.turns...)
		snapshot.AggregateMetrics = aggregateTurnMetrics(snapshot.Turns)
	}
	if turn.Diagnostics != nil {
		resp.Diagnostics = withDiagnostics(resp.Diagnostics, turn.Diagnostics)
	}
	if !sess.transient {
		if err := r.saveAgentSessionMetadata(r.metadataFromSession(sess)); err != nil {
			resp.Diagnostics = withDiagnostic(resp.Diagnostics, "metadata_save_error", err.Error())
		}
	}
	if sess.transient {
		snapshot.CanAppendMessage = false
	}
	resp.Session = snapshot
	resp.CanAppendMessage = snapshot.CanAppendMessage
	resp.WaitingPrompt = snapshot.WaitingPrompt
	resp.Observation = r.agentObservationLocked(sess, snapshot, result, turn.ID)
	resp.Observation.Diagnostics = cloneStringMap(resp.Diagnostics)
	resp.Summary = agentSummary(result)
	resp.FinishedAt = finished
	resp.DurationMS = resp.FinishedAt.Sub(resp.StartedAt).Milliseconds()
	storeAgentSessionSnapshot(sess, resp.Session)
	return resp
}

func (r *Runner) fallbackAgentSessionSnapshot(sess *agentSession, status engine.Status) AgentSessionSnapshot {
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
			Capabilities:            sess.capabilities,
			ContextPolicy:           sess.contextPolicy,
		}
	}
	snapshot.Status = string(status)
	snapshot.Phase = sessionlifecycle.PhaseIdle
	snapshot.UpdatedAt = sess.updatedAt
	snapshot.Turns = append([]AgentTurnSummary(nil), sess.turns...)
	snapshot.AggregateMetrics = aggregateTurnMetrics(snapshot.Turns)
	snapshot.CanAppendMessage = status == engine.Waiting || status == engine.Completed
	snapshot.Recoverable = false
	return snapshot
}

func lockAgentSessionForTurn(ctx context.Context, sess *agentSession) error {
	deadline := time.NewTimer(agentSessionTurnLockTimeout)
	defer deadline.Stop()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		if sess.mu.TryLock() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return errAgentSessionBusy
		case <-tick.C:
		}
	}
}

func agentTurnResponseFinalizationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
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
	snap, err := sess.thread.Journal(ctx)
	if err != nil {
		return AgentSessionSnapshot{}, err
	}
	lifecycle := sessionlifecycle.Derive(snap.Path, snap.Phase)
	turns := summariesFromEntries(snap.Entries, sess.turns)
	projection := observeContextProjection(sessiontree.BuildContextProjection(snap.Path, sessiontree.ContextProjectionOptions{Purpose: sessiontree.ProjectionTestUI}))
	active := projection.Messages
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
		Capabilities:            sess.capabilities,
		ContextPolicy:           sess.contextPolicy,
		LatestTurnID:            lifecycle.LatestTurnID(),
		WaitingPrompt:           lifecycle.WaitingPrompt(),
		Recoverable:             lifecycle.Recoverable(),
		CanAppendMessage:        lifecycle.CanAppendMessage(),
		Turns:                   turns,
		ActiveContext:           active,
		ContextProjection:       projection,
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
			Capabilities:            sess.capabilities,
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

func (r *Runner) runningAgentSessionSnapshot(ctx context.Context, sess *agentSession) AgentSessionSnapshot {
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
			Capabilities:            sess.capabilities,
			ContextPolicy:           sess.contextPolicy,
		}
	}
	snapshot.Status = lifecycle.Status()
	snapshot.Phase = lifecycle.Phase()
	snapshot.LatestTurnID = lifecycle.LatestTurnID()
	snapshot.WaitingPrompt = lifecycle.WaitingPrompt()
	snapshot.Recoverable = lifecycle.Recoverable()
	snapshot.CanAppendMessage = lifecycle.CanAppendMessage()
	if refreshed, err := r.refreshRunningSnapshotFromThread(ctx, sess, snapshot); err == nil {
		snapshot = refreshed
	}
	snapshot.Observation = r.runningAgentObservation(sess, snapshot)
	return snapshot
}

func (r *Runner) refreshRunningSnapshotFromThread(ctx context.Context, sess *agentSession, snapshot AgentSessionSnapshot) (AgentSessionSnapshot, error) {
	snap, err := sess.thread.Journal(ctx)
	if err != nil {
		return snapshot, err
	}
	lifecycle := sessionlifecycle.Running(snapshot.LatestTurnID)
	snapshot.LeafID = snap.Meta.LeafID
	snapshot.Status = lifecycle.Status()
	snapshot.Phase = lifecycle.Phase()
	snapshot.WaitingPrompt = lifecycle.WaitingPrompt()
	snapshot.Recoverable = lifecycle.Recoverable()
	snapshot.CanAppendMessage = lifecycle.CanAppendMessage()
	projection := observeContextProjection(sessiontree.BuildContextProjection(snap.Path, sessiontree.ContextProjectionOptions{Purpose: sessiontree.ProjectionTestUI}))
	snapshot.ActiveContext = projection.Messages
	snapshot.ContextProjection = projection
	snapshot.PathEntries = observeEntries(snap.Path)
	snapshot.AllEntries = observeEntries(snap.Entries)
	snapshot.Compactions = countCompactions(snap.Path)
	return snapshot, nil
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
	observation.ContextProjection = snapshot.ContextProjection
	observation.PathEntries = snapshot.PathEntries
	observation.Transitions = buildTransitions(eventsForRun(sess.recorder.Snapshot(), turnID), result)
	return observation
}

func (r *Runner) runningAgentObservation(sess *agentSession, snapshot AgentSessionSnapshot) AgentObservation {
	observation := sess.provider.Snapshot()
	observation.SessionMessages = sessionMessagesFromEntries(snapshot.PathEntries)
	observation.ActiveContext = snapshot.ActiveContext
	observation.ContextProjection = snapshot.ContextProjection
	observation.PathEntries = snapshot.PathEntries
	observation.Transitions = buildRunningTransitions(eventsForRun(sess.recorder.Snapshot(), snapshot.LatestTurnID))
	return observation
}

func (sess *agentSession) nextTurnID() string {
	if sess.nextID != nil {
		return sess.nextID("turn")
	}
	return ""
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

func summariesFromEntries(entries []sessiontree.Entry, existing []AgentTurnSummary) []AgentTurnSummary {
	out := append([]AgentTurnSummary(nil), existing...)
	index := make(map[string]int, len(out))
	for i, turn := range out {
		if turn.ID != "" {
			index[turn.ID] = i
		}
	}
	for _, entry := range entries {
		if entry.TurnID == "" {
			continue
		}
		i, ok := index[entry.TurnID]
		if !ok {
			if !entryCreatesTurnSummary(entry) {
				continue
			}
			out = append(out, AgentTurnSummary{ID: entry.TurnID})
			i = len(out) - 1
			index[entry.TurnID] = i
		}
		turn := out[i]
		if turn.StartedAt.IsZero() || (entry.TurnStatus == sessiontree.TurnStarted && !entry.CreatedAt.IsZero()) {
			turn.StartedAt = entry.CreatedAt
		}
		if entry.Type == sessiontree.EntryRunFailure {
			turn.Error = entry.Error
		}
		if entry.Type == sessiontree.EntryAssistantMessage && entry.Message.Content != "" {
			turn.Output = entry.Message.Content
		}
		if entry.Type == sessiontree.EntryTurnMarker {
			switch entry.TurnStatus {
			case sessiontree.TurnCompleted, sessiontree.TurnWaiting, sessiontree.TurnFailed, sessiontree.TurnAborted:
				turn.Status = statusForTurnMarker(entry.TurnStatus)
				turn.FinishedAt = entry.CreatedAt
				if entry.TurnStatus == sessiontree.TurnWaiting && turn.Output == "" {
					turn.Output = waitingPromptForEntries(entries, entry.TurnID)
				}
				if entry.Metadata != nil {
					turn.CompletionReason = entry.Metadata["completion_reason"]
					turn.ContinuationReason = entry.Metadata["continuation_reason"]
					turn.FinishReason = entry.Metadata["finish_reason"]
					turn.RawFinishReason = entry.Metadata["raw_finish_reason"]
					turn.FinishInferred = entry.Metadata["finish_inferred"] == "true"
				}
			case sessiontree.TurnStarted:
				if turn.Status == "" {
					turn.Status = "running"
				}
			}
		}
		out[i] = turn
	}
	return out
}

func entryCreatesTurnSummary(entry sessiontree.Entry) bool {
	switch entry.Type {
	case sessiontree.EntryRunFailure, sessiontree.EntryAssistantMessage:
		return true
	case sessiontree.EntryTurnMarker:
		switch entry.TurnStatus {
		case sessiontree.TurnStarted, sessiontree.TurnCompleted, sessiontree.TurnWaiting, sessiontree.TurnFailed, sessiontree.TurnAborted:
			return true
		}
	}
	return false
}

func upsertAgentTurnSummary(turns []AgentTurnSummary, summary AgentTurnSummary) []AgentTurnSummary {
	if summary.ID == "" {
		return turns
	}
	out := append([]AgentTurnSummary(nil), turns...)
	for i, turn := range out {
		if turn.ID == summary.ID {
			out[i] = mergeAgentTurnSummary(turn, summary)
			return out
		}
	}
	return append(out, summary)
}

func mergeAgentTurnSummary(old, next AgentTurnSummary) AgentTurnSummary {
	if next.Status != "" {
		old.Status = next.Status
	}
	if next.Output != "" {
		old.Output = next.Output
	}
	if next.Error != "" {
		old.Error = next.Error
	}
	if !next.StartedAt.IsZero() {
		old.StartedAt = next.StartedAt
	}
	if !next.FinishedAt.IsZero() {
		old.FinishedAt = next.FinishedAt
	}
	if next.Metrics != (engine.RunMetrics{}) {
		old.Metrics = next.Metrics
	}
	if next.CompletionReason != "" {
		old.CompletionReason = next.CompletionReason
	}
	if next.ContinuationReason != "" {
		old.ContinuationReason = next.ContinuationReason
	}
	if next.FinishReason != "" {
		old.FinishReason = next.FinishReason
	}
	if next.RawFinishReason != "" {
		old.RawFinishReason = next.RawFinishReason
	}
	if next.FinishInferred {
		old.FinishInferred = true
	}
	return old
}

func statusForTurnMarker(status sessiontree.TurnMarkerStatus) string {
	switch status {
	case sessiontree.TurnCompleted:
		return string(engine.Completed)
	case sessiontree.TurnWaiting:
		return string(engine.Waiting)
	case sessiontree.TurnAborted:
		return string(engine.Cancelled)
	case sessiontree.TurnFailed:
		return string(engine.Failed)
	default:
		return ""
	}
}

func waitingPromptForEntries(entries []sessiontree.Entry, turnID string) string {
	for i := len(entries) - 1; i >= 0; i-- {
		entry := entries[i]
		if entry.TurnID == turnID && entry.Type == sessiontree.EntryAssistantMessage && entry.Message.ToolName == "ask_user" {
			if signal, ok, err := control.Project(provider.ToolCall{Name: entry.Message.ToolName, Args: entry.Message.ToolArgs}); ok && err == nil {
				return signal.Prompt
			}
			return entry.Message.ToolArgs
		}
	}
	return ""
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
			out = append(out, observedCompactionCheckpoint(entry, out))
		}
	}
	return out
}

func observedCompactionCheckpoint(entry ObservedSessionEntry, previous []ObservedSessionMessage) ObservedSessionMessage {
	msg := compaction.BuildCheckpointMessage(entry.Summary, observedKeptUsers(previous, entry.KeptUserEntryIDs), nil)
	msg.EntryID = entry.ID
	msg.ParentEntryID = entry.ParentID
	msg.CompactionID = entry.CompactionID
	msg.CompactionGeneration = entry.CompactionGeneration
	msg.CompactionWindowID = entry.CompactionWindowID
	return observeEntryMessage(msg)
}

func observedKeptUsers(messages []ObservedSessionMessage, ids []string) []session.Message {
	if len(ids) == 0 || len(messages) == 0 {
		return nil
	}
	byID := make(map[string]ObservedSessionMessage, len(messages))
	for _, msg := range messages {
		if msg.EntryID == "" || msg.Role != string(session.User) {
			continue
		}
		byID[msg.EntryID] = msg
	}
	out := make([]session.Message, 0, len(ids))
	for _, id := range ids {
		msg, ok := byID[id]
		if !ok {
			continue
		}
		out = append(out, session.Message{
			Role:          session.User,
			Content:       msg.Content,
			EntryID:       msg.EntryID,
			ParentEntryID: msg.ParentEntryID,
		})
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
			KeptUserEntryIDs:        append([]string(nil), entry.KeptUserEntryIDs...),
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

func withDiagnostic(in map[string]string, key, value string) map[string]string {
	if value == "" {
		return cloneStringMap(in)
	}
	out := cloneStringMap(in)
	if out == nil {
		out = map[string]string{}
	}
	out[key] = value
	return out
}

func withDiagnostics(in, extra map[string]string) map[string]string {
	out := cloneStringMap(in)
	for key, value := range extra {
		if value == "" {
			continue
		}
		if out == nil {
			out = map[string]string{}
		}
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
	rawSearch := profile.WebSearch
	profile = normalizeProfile(profile, 0)
	if err := validateProfileWebSearch(profile.ID, profile.Provider, rawSearch); err != nil {
		return ProviderProfile{}, fmt.Errorf("%w: %v", errAgentSessionInput, err)
	}
	profile.WebSearch = searchcap.NormalizeCapability(profile.Provider, rawSearch)
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
	return r.RunWithOptions(ctx, target, runOptions{})
}

func (r Runner) RunWithOptions(ctx context.Context, target string, opts runOptions) RunResponse {
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
	case TargetToolScenarios:
		resp = r.runToolScenarioSuite(ctx, resp)
	case TargetLiveToolScenarios:
		resp = r.runLiveToolScenarios(ctx, resp, opts)
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
	targets := []string{TargetUnit, TargetRace, TargetEvalDemo, TargetToolScenarios}
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
	if err := builtin.RegisterWorkspaceMutation(registry, builtin.WorkspaceOptions{Root: workspace}); err != nil {
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
	eng, err := engine.New(engine.Config{
		Provider:     prov,
		Store:        session.NewMemoryStore(),
		Artifacts:    artifact.NewMemoryStore(),
		SystemPrompt: "You are a deterministic Floret eval agent.",
		Tools:        registry,
		Sink:         rec,
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
	})
	if err != nil {
		return r.failAgent(resp, err)
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
	store, err := r.sessionStorage(ctx)
	if err != nil {
		return r.failAgent(resp, err)
	}
	promptStore := store.prompt(r.Root)
	eng, err := engine.New(engine.Config{
		Provider:     p,
		Store:        session.NewMemoryStore(),
		Prompt:       promptStore,
		Artifacts:    artifact.NewMemoryStore(),
		SystemPrompt: "You are Floret's smoke-test assistant. Reply with a short success message. Do not call normal tools unless you need to ask the user for missing information.",
		Tools:        registry,
		Sink:         rec,
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
	})
	if err != nil {
		return r.failAgent(resp, err)
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

func buildRunningTransitions(events []event.Event) []StateTransition {
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
		case event.ToolCall:
			add(ev.Timestamp, ev.Step, "tool_calling", "tool_call", ev.ToolName)
		case event.ToolResult:
			add(ev.Timestamp, ev.Step, "tool_result_received", "tool_result", eventDetails(ev))
		case event.ContextCompact:
			add(ev.Timestamp, ev.Step, "compacting_context", "context_compact", "context was compacted")
		}
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
	cmd.Env = eval.CleanCommandEnv(os.Environ())
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

package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/floegence/floret/v7/internal/providertransport"
	"github.com/floegence/floret/v7/internal/requestmeasure"
	stdRuntime "runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
	internalstorage "github.com/floegence/floret/v7/internal/storage"
	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/provider"
	publicstorage "github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/tools"
)

const (
	logicalSchemaNamespace            = "floret.system"
	logicalSchemaKey                  = "logical-schema"
	logicalSchemaVersion              = "11"
	logicalSchemaFingerprint          = "sha256:8c7b5edf140b2950900551ebf83cc70475a970109feba1c9148a6384691cf75a"
	legacyV10LogicalSchemaVersion     = "10"
	legacyV10LogicalSchemaFingerprint = "sha256:fc7461f9beda89a3ee63809596469cfdf44365dabec30c52c4d8568261fd9ce7"
	legacyV9LogicalSchemaVersion      = "9"
	legacyV9LogicalSchemaFingerprint  = "sha256:97a494530555ecf4e0a5309f418a4eef0cf45a89cc05fa5cb51009d388bc0794"
	legacyV8LogicalSchemaVersion      = "8"
	legacyV8LogicalSchemaFingerprint  = "sha256:292c0d3b42694e006e8667e16f43981f754577f67714e86c7a060fce4884b852"
	legacyV7LogicalSchemaVersion      = "7"
	legacyV7LogicalSchemaFingerprint  = "sha256:3d7951de5615bec14466950583faecbe375ff14c8cfd1b9c90dc8913c34bbc30"
	previousLogicalSchemaVersion      = "6"
	previousLogicalSchemaFingerprint  = "sha256:d0e7e9caae107d1c889163e6cb898b7fb5721fcafd32229733baa0cee5e502ee"
	legacyV5LogicalSchemaVersion      = "5"
	legacyV5LogicalSchemaFingerprint  = "sha256:55e73dedc2642ccb7f97d285c8720484b885f2050985092f62a8dd15e279385e"
	legacyLogicalSchemaVersion        = "3"
	legacyLogicalSchemaFingerprint    = "sha256:53e8fd256bfa05b6f31f73b8230455fd28d6bb4f3be1fce7d94a9af9b5838d28"
)

var (
	// ErrMigrationRequired reports an exact legacy schema that runtime.Open
	// refuses to migrate implicitly.
	ErrMigrationRequired = errors.New("floret storage migration required")
	// ErrUnsupportedSchema reports a nonempty schema outside the v3 contract.
	ErrUnsupportedSchema = errors.New("unsupported floret logical schema")
	// ErrStoreTooNew reports a store written by a newer storage contract.
	ErrStoreTooNew = errors.New("floret storage requires a newer runtime")
)

// MigrationRequiredError identifies the exact legacy logical schema observed
// by runtime.Open.
type MigrationRequiredError struct {
	Version string
}

// Error describes the required explicit migration.
func (failure *MigrationRequiredError) Error() string {
	if failure == nil {
		return ErrMigrationRequired.Error()
	}
	return fmt.Sprintf("%s: observed schema %q", ErrMigrationRequired, failure.Version)
}

// Is classifies MigrationRequiredError with ErrMigrationRequired.
func (failure *MigrationRequiredError) Is(target error) bool {
	return target == ErrMigrationRequired
}

// Options configures one runtime Host.
type Options struct {
	Storage publicstorage.Source
	// EnableParentEvents admits passive child reports and critical events at natural provider boundaries.
	EnableParentEvents bool
	// DeferExecution keeps provider and tool execution disabled until Activate.
	DeferExecution bool
	// StartupProgress observes product-neutral storage startup phases. The
	// callback runs synchronously and must return promptly.
	StartupProgress     StartupProgress
	StartupWorkProgress StartupWorkProgress
	// IDSource exists only for v3 source compatibility.
	// Deprecated: production hosts must leave this nil; tests should use
	// florettest.NewIDSource.
	IDSource IDSource
}

// StartupPhase identifies storage startup work performed by Open.
type StartupPhase string

const (
	StartupPhaseMigrating StartupPhase = "migrating"
	StartupPhaseVerifying StartupPhase = "verifying"
	// Deprecated: full-text indexing has been removed; Open never emits this phase.
	StartupPhaseIndexing StartupPhase = "indexing"
)

// StartupProgress observes changes in the storage startup phase.
type StartupProgress interface {
	OnStartupPhase(StartupPhase)
}

// StartupProgressFunc adapts a function to StartupProgress.
type StartupProgressFunc func(StartupPhase)

// OnStartupPhase reports one startup phase when the function is non-nil.
func (progress StartupProgressFunc) OnStartupPhase(phase StartupPhase) {
	if progress != nil {
		progress(phase)
	}
}

// Host is the composition-root owner of Floret storage and narrow capability
// issuance. Application services must retain only handles issued by Host.
type Host struct {
	maintenanceMu      sync.Mutex
	executionMu        sync.RWMutex
	executionDeferred  bool
	enableParentEvents bool
	store              *runtimeStore
	backend            spi.Backend
	idSource           IDSource
	idMu               sync.Mutex
	// mutationMu protects inventory-wide and cross-thread mutations only.
	mutationMu          sync.Mutex
	threadRuntimeMu     sync.Mutex
	threadRuntime       *threadRuntimeService
	closeMu             sync.Mutex
	closing             bool
	closed              bool
	closeErr            error
	closeDone           chan struct{}
	startup             *storageReadiness
	startupWorker       *storageStartupWorker
	startupSchema       logicalSchemaState
	startupStartMu      sync.Mutex
	backgroundMigration bool
	threadPreparations  *threadPreparationState
}

type storageStartupWorker struct {
	ctx     context.Context
	cancel  context.CancelFunc
	options Options
}

type keyedMutex struct {
	mu      sync.Mutex
	entries map[string]*keyedMutexEntry
}

type keyedMutexEntry struct {
	mu   sync.Mutex
	refs int
}

// runtime.Open owns one backend, so all domain and request-ledger transactions
// share this short fence instead of surfacing backend-specific write races.
type serializedBackend struct {
	gateOnce sync.Once
	gate     chan struct{}
	backend  spi.Backend
	startup  *storageReadiness
}

func (backend *serializedBackend) View(ctx context.Context, read func(spi.ReadTx) error) error {
	if err := backend.acquire(ctx); err != nil {
		return err
	}
	defer backend.release()
	return backend.backend.View(ctx, read)
}

func (backend *serializedBackend) Update(ctx context.Context, mutate func(spi.WriteTx) error) error {
	if err := backend.acquire(ctx); err != nil {
		return err
	}
	defer backend.release()
	return backend.backend.Update(ctx, mutate)
}

func (backend *serializedBackend) Close() error {
	if err := backend.acquire(context.Background()); err != nil {
		return err
	}
	defer backend.release()
	return backend.backend.Close()
}

func (backend *serializedBackend) acquire(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: backend operation context is required", spi.ErrInvalidArgument)
	}
	backend.gateOnce.Do(func() {
		backend.gate = make(chan struct{}, 1)
		backend.gate <- struct{}{}
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-backend.gate:
		if err := ctx.Err(); err != nil {
			backend.release()
			return err
		}
		return nil
	}
}

func (backend *serializedBackend) release() {
	backend.gate <- struct{}{}
}

func (mutex *keyedMutex) lock(key string) func() {
	mutex.mu.Lock()
	if mutex.entries == nil {
		mutex.entries = make(map[string]*keyedMutexEntry)
	}
	entry := mutex.entries[key]
	if entry == nil {
		entry = &keyedMutexEntry{}
		mutex.entries[key] = entry
	}
	entry.refs++
	mutex.mu.Unlock()

	entry.mu.Lock()
	return func() {
		entry.mu.Unlock()
		mutex.mu.Lock()
		entry.refs--
		if entry.refs == 0 {
			delete(mutex.entries, key)
		}
		mutex.mu.Unlock()
	}
}

type logicalSchemaEnvelope struct {
	Version     string `json:"version"`
	Fingerprint string `json:"fingerprint"`
}

// Open validates the logical schema and transfers exclusive backend lifecycle
// ownership to a new Host. Current stores use bounded format recognition and
// validate each thread when it is first prepared.
func Open(ctx context.Context, options Options) (*Host, error) {
	if ctx == nil {
		return nil, errors.New("runtime open context is required")
	}
	backend, err := storagebridge.Open(ctx, storagebridge.Source(options.Storage))
	if err != nil {
		if errors.Is(err, spi.ErrMigrationRequired) {
			return nil, &MigrationRequiredError{Version: "16"}
		}
		return nil, err
	}
	if backend == nil {
		return nil, errors.New("runtime storage source returned a nil backend")
	}
	startup := newStorageReadiness()
	coordinatedBackend := &serializedBackend{backend: backend, startup: startup}
	transferred := false
	defer func() {
		if !transferred {
			_ = coordinatedBackend.Close()
		}
	}()
	var logicalState logicalSchemaState
	var currentKernel *internalstorage.BackendKernel
	var initializedFreshStore bool
	err = backend.Update(ctx, func(tx spi.WriteTx) error {
		if _, err := storagebridge.PrepareStartup(tx, nil); err != nil {
			return err
		}
		var inspectErr error
		logicalState, inspectErr = inspectLogicalSchemaTransaction(tx)
		if inspectErr != nil {
			return inspectErr
		}
		if logicalState == logicalSchemaCurrent || logicalState == logicalSchemaV10 || logicalState == logicalSchemaV9 {
			var current bool
			currentKernel, current, inspectErr = internalstorage.NewCurrentBackendKernelInTransaction(ctx, coordinatedBackend, tx, time.Now, options.EnableParentEvents)
			if !current {
				currentKernel = nil
			}
			if inspectErr == nil && currentKernel != nil {
				if options.StartupProgress != nil {
					options.StartupProgress.OnStartupPhase(StartupPhaseVerifying)
				}
				return commitLogicalSchemaTransaction(tx, logicalState)
			}
			return inspectErr
		}
		if logicalState == logicalSchemaMissing {
			var progress internalstorage.StartupProgress
			if options.StartupProgress != nil {
				progress = func(phase internalstorage.StartupPhase) { options.StartupProgress.OnStartupPhase(StartupPhase(phase)) }
			}
			currentKernel, initializedFreshStore, inspectErr = internalstorage.NewFreshBackendKernelInTransaction(ctx, coordinatedBackend, tx, time.Now, options.EnableParentEvents, progress)
			if inspectErr != nil || !initializedFreshStore {
				return inspectErr
			}
			return commitLogicalSchemaTransaction(tx, logicalState)
		}
		return nil
	})
	if err != nil {
		return nil, runtimeHostError(err)
	}
	startupContext, startupCancel := context.WithCancel(context.Background())
	kernel, err := internalstorage.NewDeferredBackendKernel(coordinatedBackend, time.Now, startup.waitUsable)
	if err != nil {
		startupCancel()
		return nil, err
	}
	store, err := newBackendRuntimeStoreWithKernel(coordinatedBackend, kernel, startup)
	if err != nil {
		startupCancel()
		return nil, err
	}
	idSource := options.IDSource
	if idSource == nil {
		idSource = randomIDSource{}
	}
	host := &Host{
		enableParentEvents: options.EnableParentEvents, store: store, backend: coordinatedBackend, idSource: idSource, closeDone: make(chan struct{}), executionDeferred: options.DeferExecution,
		threadPreparations: &threadPreparationState{threads: make(map[identity.ThreadID]*threadPreparation)},
		startup:            startup, startupWorker: &storageStartupWorker{ctx: startupContext, cancel: startupCancel, options: options}, startupSchema: logicalState,
	}
	transferred = true
	if currentKernel != nil {
		if err := host.store.kernel.InstallBackendRepo(currentKernel.BackendRepo); err != nil {
			_ = host.Shutdown(context.Background())
			return nil, err
		}
		if host.enableParentEvents {
			host.store.kernel.SetParentEventObserver(func(ids []string) { go host.observeParentEvents(ids) })
		}
		if !initializedFreshStore || currentKernel.BackendRepo.HasPendingMigration() {
			host.startup.markUsable()
			host.startBackgroundThreadMigration()
		} else {
			host.startup.markReady()
		}
	} else if err := host.startStartupInitialization(); err != nil {
		_ = host.Shutdown(context.Background())
		return nil, err
	}
	return host, nil
}

func (host *Host) startBackgroundThreadMigration() {
	if host == nil || host.startupWorker == nil || host.startupWorker.ctx == nil {
		return
	}
	host.startupStartMu.Lock()
	defer host.startupStartMu.Unlock()
	if host.backgroundMigration || !host.startup.isUsable() {
		return
	}
	if host.startup.failed() {
		host.startup.begin()
	}
	migrator, ok := host.store.repo.(interface {
		MigrateNextThread(context.Context) (bool, error)
	})
	if !ok {
		host.startup.markReady()
		return
	}
	host.backgroundMigration = true
	go func() {
		var migrationErr error
		defer func() {
			if recovered := recover(); recovered != nil {
				migrationErr = fmt.Errorf("thread migration panicked: %v", recovered)
			}
			host.startupStartMu.Lock()
			host.backgroundMigration = false
			// A failed status must never be visible while the finished worker
			// still owns the start gate: an explicit retry may arrive immediately.
			host.startup.finish(migrationErr)
			host.startupStartMu.Unlock()
		}()
		work := sessiontree.WithStartupWork(host.startupWorker.ctx, func(phase sessiontree.StartupPhase, completed, total int64) {
			host.startup.update(StartupPhase(phase), completed, total)
			if observer := host.startupWorker.options.StartupWorkProgress; observer != nil {
				observer.OnStartupWork(StartupWork{Phase: StartupPhase(phase), Completed: completed, Total: total})
			}
		})
		failures := 0
		for {
			if err := host.startupWorker.ctx.Err(); err != nil {
				// Shutdown cancels the worker before waiting for its readiness
				// completion. Always close that lifecycle gate on cancellation so
				// Host shutdown cannot wait forever for a cancelled migration.
				migrationErr = err
				return
			}
			migrated, err := migrator.MigrateNextThread(work)
			if err != nil {
				if !errors.Is(err, sessiontree.ErrAuthorityCorrupt) && !errors.Is(err, context.Canceled) && failures < 3 {
					failures++
					timer := time.NewTimer(time.Duration(failures) * 50 * time.Millisecond)
					select {
					case <-timer.C:
					case <-host.startupWorker.ctx.Done():
						timer.Stop()
					}
					continue
				}
				migrationErr = err
				return
			}
			failures = 0
			if !migrated {
				return
			}
			stdRuntime.Gosched()
		}
	}()
}

func (host *Host) startStartupInitialization() error {
	if host == nil || host.startup == nil {
		return errors.New("runtime Host storage startup is unavailable")
	}
	host.startupStartMu.Lock()
	defer host.startupStartMu.Unlock()
	_, start := host.startup.begin()
	if !start {
		return nil
	}
	go host.initializeStorage()
	return nil
}

func (host *Host) initializeStorage() {
	var startupErr error
	var handedOff bool
	defer func() {
		if recovered := recover(); recovered != nil {
			startupErr = fmt.Errorf("storage startup panicked: %v", recovered)
		}
		if !handedOff {
			host.startup.finish(startupErr)
		}
	}()
	var previousPhase StartupPhase
	reportPhase := func(phase StartupPhase) {
		if previousPhase == phase {
			return
		}
		previousPhase = phase
		host.startup.update(phase, 0, 0)
		if host.startupWorker.options.StartupProgress != nil {
			host.startupWorker.options.StartupProgress.OnStartupPhase(phase)
		}
	}
	var progress internalstorage.StartupProgress
	if host.startupWorker.options.StartupProgress != nil {
		progress = func(phase internalstorage.StartupPhase) { reportPhase(StartupPhase(phase)) }
	}
	ctx := host.startupWorker.ctx
	ctx = sessiontree.WithStartupWork(ctx, func(phase sessiontree.StartupPhase, completed, total int64) {
		statusPhase := StartupPhase(phase)
		host.startup.update(statusPhase, completed, total)
		if observer := host.startupWorker.options.StartupWorkProgress; observer != nil {
			observer.OnStartupWork(StartupWork{Phase: statusPhase, Completed: completed, Total: total})
		}
	})
	if host.startupSchema != logicalSchemaCurrent {
		reportPhase(StartupPhaseMigrating)
	} else {
		reportPhase(StartupPhaseVerifying)
	}
	var candidate *internalstorage.BackendKernel
	backend := host.backend.(*serializedBackend)
	startupErr = sessiontree.PrepareHistoricalSnapshot(ctx, backend, time.Now)
	if startupErr != nil {
		return
	}
	startupErr = backend.backend.Update(ctx, func(tx spi.WriteTx) error {
		logicalState, err := inspectLogicalSchemaTransaction(tx)
		if err != nil {
			return err
		}
		if logicalState != host.startupSchema {
			return errors.New("logical storage schema changed during startup")
		}
		var prepared bool
		candidate, prepared, err = internalstorage.NewPreparingBackendKernelInTransaction(sessiontree.WithParentEvents(ctx, host.enableParentEvents), backend, tx, time.Now, host.enableParentEvents, progress)
		if err != nil {
			return err
		}
		if prepared {
			reportPhase(StartupPhaseVerifying)
			return commitLogicalSchemaTransaction(tx, logicalState)
		}
		candidate, err = internalstorage.NewBackendKernelInTransaction(sessiontree.WithParentEvents(ctx, host.enableParentEvents), backend, tx, time.Now, logicalState != logicalSchemaCurrent, progress)
		if err != nil {
			return err
		}
		if err = commitLogicalSchemaTransaction(tx, logicalState); err != nil {
			return err
		}
		if err = candidate.PrepareParentEventsInTransaction(ctx, tx, host.enableParentEvents); err != nil {
			return err
		}
		reportPhase(StartupPhaseVerifying)
		if err := candidate.VerifyCurrentStateInTransaction(ctx, tx); err != nil {
			return err
		}
		// Historical transforms never become the live repository owner. Publish
		// the same canonical backend owner used by current-store startup before
		// allowing operations or background per-thread preparation to proceed.
		var current bool
		candidate, current, err = internalstorage.NewCurrentBackendKernelInTransaction(ctx, backend, tx, time.Now, host.enableParentEvents)
		if err != nil {
			return err
		}
		if !current {
			return errors.New("historical migration did not produce current canonical storage")
		}
		return nil
	})
	if startupErr == nil {
		startupErr = host.store.kernel.InstallBackendRepo(candidate.BackendRepo)
	}
	if startupErr == nil && host.enableParentEvents {
		host.store.kernel.SetParentEventObserver(func(ids []string) { go host.observeParentEvents(ids) })
	}
	if startupErr == nil {
		host.startup.markUsable()
		host.startBackgroundThreadMigration()
		handedOff = true
	}
}

func (host *Host) turnRunner(ctx context.Context, threadID identity.ThreadID, agent *Agent) (*turnRunnerHandle, error) {
	if err := host.requireExecution(); err != nil {
		return nil, err
	}
	if err := host.available(); err != nil {
		return nil, err
	}
	if agent == nil {
		return nil, errors.New("turn runner requires an Agent")
	}
	if _, err := host.store.repo.Thread(ctx, threadID.String()); err != nil {
		return nil, runtimeHostError(err)
	}
	opts := agent.turnExecutionOptions()
	if err := opts.validate(); err != nil {
		return nil, err
	}
	provider, err := newProviderHost(providerHostOptions{
		GoalsEnabled: opts.goalsEnabled,
		Config:       opts.config, modelGateway: opts.modelGateway, modelGatewayIdentity: opts.modelGatewayIdentity,
		modelGatewayCapabilities: opts.modelGatewayCapabilities, store: host.store, Tools: opts.tools,
		EffectAuthorizationGate: opts.effectAuthorizationGate, Sink: opts.sink,
		ToolSurfaceProvider: opts.toolSurfaceProvider, IDGenerator: opts.idGenerator,
		ProviderRequestTimeoutPolicy: opts.providerRequestTimeoutPolicy, ProviderRetryPolicy: opts.providerRetryPolicy, LoopLimits: opts.loopLimits, RunLabels: opts.runLabels, Capabilities: opts.capabilities, ThreadTitleMode: opts.threadTitleMode,
	})
	if err != nil {
		return nil, err
	}
	provider.steering = agent.steering
	return &turnRunnerHandle{inner: &turnExecutionCapability{threadID: threadID, host: provider}, threadID: threadID}, nil
}

func (host *Host) available() error {
	if host == nil {
		return errors.New("runtime Host is required")
	}
	host.closeMu.Lock()
	defer host.closeMu.Unlock()
	if host.closing || host.closed {
		return ErrHostClosed
	}
	return nil
}

// Shutdown stops in-memory executions and closes the owned canonical store.
func (host *Host) Shutdown(ctx context.Context) error {
	if host == nil {
		return nil
	}
	if ctx == nil {
		return errors.New("runtime shutdown context is required")
	}
	host.closeMu.Lock()
	if host.closed {
		err := host.closeErr
		host.closeMu.Unlock()
		return err
	}
	if host.closing {
		done := host.closeDone
		host.closeMu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
		}
		host.closeMu.Lock()
		err := host.closeErr
		host.closeMu.Unlock()
		return err
	}
	host.closing = true
	done := host.closeDone
	host.closeMu.Unlock()
	go host.finishShutdown()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
	}
	host.closeMu.Lock()
	err := host.closeErr
	host.closeMu.Unlock()
	return err
}

func (host *Host) finishShutdown() {
	host.maintenanceMu.Lock()
	defer host.maintenanceMu.Unlock()
	if host.startupWorker != nil && host.startupWorker.cancel != nil {
		host.startupWorker.cancel()
	}
	if host.startup != nil {
		host.startup.waitAttempt()
	}
	host.mutationMu.Lock()
	defer host.mutationMu.Unlock()
	host.threadRuntimeMu.Lock()
	service := host.threadRuntime
	host.threadRuntimeMu.Unlock()
	var shutdownErr error
	if service != nil {
		shutdownErr = service.close()
	}
	err := errors.Join(shutdownErr, host.store.Close())
	host.closeMu.Lock()
	host.closeErr, host.closed, host.closing = err, true, false
	close(host.closeDone)
	host.closeMu.Unlock()
}

func (host *Host) applyThreadMutation(ctx context.Context, threadID identity.ThreadID, mutate func() error) error {
	if host == nil {
		return errors.New("runtime Host is required")
	}
	if _, err := identity.ParseThreadID(threadID.String()); err != nil {
		return err
	}
	if ctx == nil {
		return errors.New("thread mutation context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	host.mutationMu.Lock()
	defer host.mutationMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	return mutate()
}

// turnExecutionRequest describes one provider execution after ThreadID is bound by a
// turnRunnerHandle.
type turnExecutionRequest struct {
	ContinueGoal                bool
	TurnKind                    TurnKind
	LogicalRequestID            identity.LogicalRequestID
	RunID                       identity.RunID
	TurnID                      identity.TurnID
	Input                       TurnInput
	SupplementalContext         []TurnSupplementalContextItem
	Signals                     TurnSignalSpec
	Limits                      TurnLimits
	Reasoning                   config.ReasoningSelection
	ManualCompactions           ManualCompactionSource
	ToolSurfaceProvider         ToolSurfaceProvider
	PromotedQueueID             string
	PromotionRequestKey         string
	PromotionRequestFingerprint string
	InputFingerprint            string
	RetrySourceTurnID           identity.TurnID
	RetrySourceEntryID          string
}

// ResumeInputRequest continues an existing waiting turn without admitting a
// second canonical user message.
type resumeInputRequest struct {
	Steering     bool
	TurnID       identity.TurnID
	RunID        identity.RunID
	WaitingRunID identity.RunID
	Answer       string
	Options      turnExecutionRequest
}

type acceptedTurnExecutionRequest struct {
	TurnKind                    TurnKind
	Accepted                    acceptedTurn
	LogicalRequestID            identity.LogicalRequestID
	RunID                       identity.RunID
	TurnID                      identity.TurnID
	Input                       TurnInput
	SupplementalContext         []TurnSupplementalContextItem
	Signals                     TurnSignalSpec
	Limits                      TurnLimits
	Reasoning                   config.ReasoningSelection
	ManualCompactions           ManualCompactionSource
	ToolSurfaceProvider         ToolSurfaceProvider
	PromotedQueueID             string
	PromotionRequestKey         string
	PromotionRequestFingerprint string
	InputFingerprint            string
}

// turnRunnerHandle is the engine effect adapter bound to one thread runtime.
type turnRunnerHandle struct {
	inner    *turnExecutionCapability
	threadID identity.ThreadID
}

// ExecuteAccepted runs provider execution after canonical acceptance.
func (runner *turnRunnerHandle) ExecuteAccepted(ctx context.Context, request acceptedTurnExecutionRequest) (TurnResult, error) {
	if runner == nil || runner.inner == nil {
		return TurnResult{}, errors.New("turn runner is required")
	}
	return runner.inner.ExecuteAcceptedTurn(ctx, request.Accepted, runTurnRequest{
		TurnKind:         request.TurnKind,
		LogicalRequestID: request.LogicalRequestID, RunID: request.RunID, ThreadID: runner.threadID, TurnID: request.TurnID,
		Input: request.Input, SupplementalContext: request.SupplementalContext,
		Signals: request.Signals,
		Limits:  request.Limits, Reasoning: request.Reasoning,
		ManualCompactions: request.ManualCompactions, ToolSurfaceProvider: request.ToolSurfaceProvider,
		PromotedQueueID:             request.PromotedQueueID,
		PromotionRequestKey:         request.PromotionRequestKey,
		PromotionRequestFingerprint: request.PromotionRequestFingerprint,
		InputFingerprint:            request.InputFingerprint,
	})
}

func (runner *turnRunnerHandle) ResumeInput(ctx context.Context, request resumeInputRequest) (TurnResult, error) {
	if runner == nil || runner.inner == nil {
		return TurnResult{}, errors.New("turn runner is required")
	}
	return runner.inner.ResumeInput(ctx, request)
}

type logicalSchemaState string

const (
	logicalSchemaMissing logicalSchemaState = "missing"
	logicalSchemaCurrent logicalSchemaState = "current"
	logicalSchemaV10     logicalSchemaState = "v10"
	logicalSchemaV7      logicalSchemaState = "v7"
	logicalSchemaV8      logicalSchemaState = "v8"
	logicalSchemaV9      logicalSchemaState = "v9"
	logicalSchemaV6      logicalSchemaState = "v6"
	logicalSchemaV5      logicalSchemaState = "v5"
	logicalSchemaV3      logicalSchemaState = "v3"
)

func inspectLogicalSchema(ctx context.Context, backend spi.Backend) (logicalSchemaState, error) {
	var state logicalSchemaState
	err := backend.View(ctx, func(tx spi.ReadTx) error {
		var err error
		state, err = inspectLogicalSchemaTransaction(tx)
		return err
	})
	return state, err
}

func inspectLogicalSchemaTransaction(tx spi.ReadTx) (logicalSchemaState, error) {
	encoded, err := tx.Get(logicalSchemaNamespace, []byte(logicalSchemaKey))
	if errors.Is(err, spi.ErrNotFound) {
		return logicalSchemaMissing, nil
	}
	if err != nil {
		return "", err
	}
	decoder := json.NewDecoder(strings.NewReader(string(encoded)))
	decoder.DisallowUnknownFields()
	var envelope logicalSchemaEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		return "", fmt.Errorf("%w: invalid schema envelope: %v", ErrUnsupportedSchema, err)
	}
	if decoder.More() {
		return "", fmt.Errorf("%w: trailing schema data", ErrUnsupportedSchema)
	}
	currentVersion, _ := strconv.Atoi(logicalSchemaVersion)
	if version, parseErr := strconv.Atoi(envelope.Version); parseErr == nil && version > currentVersion && envelope.Version != "16" {
		return "", errors.Join(ErrUnsupportedSchema, ErrStoreTooNew)
	}
	if envelope.Version == "16" {
		return "", &MigrationRequiredError{Version: envelope.Version}
	}
	if envelope.Version == legacyV10LogicalSchemaVersion {
		if envelope.Fingerprint != legacyV10LogicalSchemaFingerprint {
			return "", fmt.Errorf("%w: version 10 fingerprint mismatch", ErrUnsupportedSchema)
		}
		return logicalSchemaV10, nil
	}
	if envelope.Version == legacyV9LogicalSchemaVersion {
		if envelope.Fingerprint != legacyV9LogicalSchemaFingerprint {
			return "", fmt.Errorf("%w: version 9 fingerprint mismatch", ErrUnsupportedSchema)
		}
		return logicalSchemaV9, nil
	}
	if envelope.Version == legacyV8LogicalSchemaVersion {
		if envelope.Fingerprint != legacyV8LogicalSchemaFingerprint {
			return "", fmt.Errorf("%w: version 8 fingerprint mismatch", ErrUnsupportedSchema)
		}
		return logicalSchemaV8, nil
	}
	if envelope.Version == legacyV7LogicalSchemaVersion {
		if envelope.Fingerprint != legacyV7LogicalSchemaFingerprint {
			return "", fmt.Errorf("%w: version %q fingerprint %q", ErrUnsupportedSchema, envelope.Version, envelope.Fingerprint)
		}
		return logicalSchemaV7, nil
	}
	if envelope.Version == previousLogicalSchemaVersion {
		if envelope.Fingerprint != previousLogicalSchemaFingerprint {
			return "", fmt.Errorf("%w: version %q fingerprint %q", ErrUnsupportedSchema, envelope.Version, envelope.Fingerprint)
		}
		return logicalSchemaV6, nil
	}
	if envelope.Version == legacyV5LogicalSchemaVersion {
		if envelope.Fingerprint != legacyV5LogicalSchemaFingerprint {
			return "", fmt.Errorf("%w: version %q fingerprint %q", ErrUnsupportedSchema, envelope.Version, envelope.Fingerprint)
		}
		return logicalSchemaV5, nil
	}
	if envelope.Version == legacyLogicalSchemaVersion {
		if envelope.Fingerprint != legacyLogicalSchemaFingerprint {
			return "", fmt.Errorf("%w: version %q fingerprint %q", ErrUnsupportedSchema, envelope.Version, envelope.Fingerprint)
		}
		return logicalSchemaV3, nil
	}
	if envelope.Version != logicalSchemaVersion || envelope.Fingerprint != logicalSchemaFingerprint {
		return "", fmt.Errorf("%w: version %q fingerprint %q", ErrUnsupportedSchema, envelope.Version, envelope.Fingerprint)
	}
	return logicalSchemaCurrent, nil
}

func commitLogicalSchema(ctx context.Context, backend spi.Backend, observed logicalSchemaState) error {
	if observed == logicalSchemaCurrent {
		return nil
	}
	return backend.Update(ctx, func(tx spi.WriteTx) error {
		return commitLogicalSchemaTransaction(tx, observed)
	})
}

func commitLogicalSchemaTransaction(tx spi.WriteTx, observed logicalSchemaState) error {
	if observed == logicalSchemaCurrent {
		return nil
	}
	envelope, err := json.Marshal(logicalSchemaEnvelope{Version: logicalSchemaVersion, Fingerprint: logicalSchemaFingerprint})
	if err != nil {
		return err
	}
	return tx.Put(logicalSchemaNamespace, []byte(logicalSchemaKey), envelope)
}

func (agent *Agent) turnExecutionOptions() turnExecutionOptions {
	identity := agent.gateway.Identity()
	capabilities := agent.gateway.Capabilities()
	_, prepares := agent.gateway.(provider.RequestPreparer)
	reasoning := capabilities.ReasoningCapability
	if capabilities.Reasoning == provider.ReasoningUnsupported {
		reasoning = config.ReasoningCapability{Kind: config.ReasoningKindNone}
	}
	attachmentMode := modelGatewayAttachmentPayloadDescriptors
	if capabilities.AttachmentPayload == provider.AttachmentExpanded {
		attachmentMode = modelGatewayAttachmentPayloadExpanded
	}
	return turnExecutionOptions{
		goalsEnabled: agent.goalsEnabled,
		config: runtimeConfig{
			SystemPrompt:  agent.configuration.SystemPrompt,
			ContextPolicy: agent.configuration.Context, Reasoning: agent.configuration.Reasoning,
		},
		modelGateway:             agentGatewayAdapter{gateway: agent.gateway},
		modelGatewayIdentity:     modelGatewayIdentity(identity),
		modelGatewayCapabilities: modelGatewayCapabilities{OutputControl: capabilities.OutputControl, PreparedRequests: prepares, Reasoning: &reasoning, AttachmentPayload: attachmentMode, ReasoningHistory: capabilities.ReasoningHistory},
		tools:                    agent.tools, effectAuthorizationGate: agent.effectAuthorization,
		sink: agent.eventSink, toolSurfaceProvider: agent.toolSurface, idGenerator: agent.idGenerator,
		providerRequestTimeoutPolicy: agent.providerRequestTimeoutPolicy, providerRetryPolicy: agent.providerRetryPolicy, loopLimits: agent.loopLimits, capabilities: agent.capabilities,
		runLabels:       cloneRunLabels(agent.runLabels),
		threadTitleMode: agent.threadTitleMode, initialized: true,
	}
}

type agentGatewayAdapter struct {
	gateway provider.Gateway
}

func (adapter agentGatewayAdapter) StreamModel(ctx context.Context, request modelRequest) (<-chan modelEvent, error) {
	stream, err := adapter.gateway.Stream(ctx, providerRequest(request))
	if err != nil {
		return nil, err
	}
	return modelEventStream(ctx, stream), nil
}

func (adapter agentGatewayAdapter) PrepareModelRequest(ctx context.Context, request modelRequest) (preparedModelRequest, error) {
	preparer, ok := adapter.gateway.(provider.RequestPreparer)
	if !ok {
		return nil, errors.New("provider gateway does not prepare requests")
	}
	prepared, err := preparer.Prepare(ctx, providerRequest(request))
	if err != nil {
		return nil, err
	}
	if prepared == nil {
		return nil, errors.New("provider gateway returned a nil prepared request")
	}
	return agentPreparedRequest{prepared: prepared}, nil
}

type agentPreparedRequest struct {
	prepared provider.PreparedRequest
}

func (request agentPreparedRequest) StreamModel(ctx context.Context) (<-chan modelEvent, error) {
	stream, err := request.prepared.Stream(ctx)
	if err != nil {
		return nil, err
	}
	return modelEventStream(ctx, stream), nil
}

func (request agentPreparedRequest) TokenEstimate() modelRequestTokenEstimate {
	estimate := request.prepared.TokenEstimate()
	return modelRequestTokenEstimate{
		Characters:       estimate.Characters.Clone(),
		Breakdown:        estimate.Breakdown,
		AttachmentTokens: estimate.AttachmentTokens,
		PrefixTokens:     estimate.PrefixTokens, MessageTokens: estimate.MessageTokens,
		ToolDefinitionTokens: estimate.ToolDefinitionTokens, EstimatedInputTokens: estimate.EstimatedInputTokens,
		Source: estimate.Source, Method: estimate.Method, Confidence: estimate.Confidence,
		Coverage: modelRequestTokenEstimateCoverage(estimate.Coverage),
	}
}

func (request agentPreparedRequest) RenderedPayloadFingerprint() string {
	return request.prepared.RenderedPayloadFingerprint()
}

func (request agentPreparedRequest) Close() error { return request.prepared.Close() }

func providerRequest(request modelRequest) provider.Request {
	messages := make([]provider.Message, len(request.Messages))
	for index, message := range request.Messages {
		messages[index] = providerMessage(message)
	}
	definitions := make([]tools.ToolDefinition, len(request.Tools))
	for index, definition := range request.Tools {
		definitions[index] = tools.ToolDefinition{
			Name: definition.Name, Title: definition.Title, Description: definition.Description,
			InputSchema: definition.InputSchema, OutputSchema: definition.OutputSchema,
			Strict: definition.Strict, Annotations: definition.Annotations,
		}
	}
	hosted := append([]provider.HostedToolDefinition(nil), request.HostedTools...)
	return provider.Request{
		RunID: identity.RunID(request.RunID), ThreadID: identity.ThreadID(request.ThreadID), TurnID: identity.TurnID(request.TurnID),
		TraceID: identity.TraceID(request.TraceID), PromptScopeID: identity.PromptScopeID(request.PromptScopeID), LogicalRequestID: identity.LogicalRequestID(request.LogicalRequestID), AttemptID: request.AttemptID, AttemptEpoch: request.AttemptEpoch, Step: request.Step,
		Messages: messages, Tools: definitions, HostedTools: hosted, MaxOutputTokens: request.MaxOutputTokens,
		Reasoning: request.Reasoning, PreviousState: gatewayProviderState(request.PreviousState), ReusePolicy: request.ReusePolicy,
		Labels: provider.Labels{Correlation: request.Labels.Correlation, Host: request.Labels.Host},
	}
}

func providerMessage(message modelMessage) provider.Message {
	attachments := make([]provider.Attachment, len(message.Attachments))
	for index, attachment := range message.Attachments {
		attachments[index] = provider.Attachment{
			ResourceRef: attachment.ResourceRef, Name: attachment.Name, MIMEType: attachment.MIMEType, SizeBytes: attachment.SizeBytes,
		}
		if attachment.TextStats != nil {
			attachments[index].TextStats = &provider.AttachmentTextStats{
				UnicodeCodePointCount: attachment.TextStats.UnicodeCodePointCount,
				LogicalLineCount:      attachment.TextStats.LogicalLineCount,
			}
		}
	}
	calls := make([]provider.ToolCall, len(message.ToolCalls))
	for index, call := range message.ToolCalls {
		calls[index] = provider.ToolCall{ID: call.ID, Name: call.Name, Args: call.Args}
	}
	var result *provider.ToolResult
	if message.ToolResult != nil {
		attachments := make([]provider.Attachment, len(message.ToolResult.Attachments))
		for index, attachment := range message.ToolResult.Attachments {
			attachments[index] = provider.Attachment{ResourceRef: attachment.ResourceRef, Name: attachment.Name, MIMEType: attachment.MIMEType, SizeBytes: attachment.SizeBytes}
		}
		result = &provider.ToolResult{CallID: message.ToolResult.CallID, ToolName: message.ToolResult.ToolName, Text: message.ToolResult.Text, Attachments: attachments}
	}
	return provider.Message{
		Attribution: message.Attribution, Role: provider.MessageRole(message.Role), Text: message.Text, Attachments: attachments,
		Reasoning: message.Reasoning, ToolCalls: calls, ToolResult: result,
	}
}

func gatewayProviderState(state *modelStateEnvelope) *provider.State {
	if state == nil {
		return nil
	}
	attributes := make(map[string]string, len(state.Attributes))
	for key, value := range state.Attributes {
		attributes[key] = value
	}
	return &provider.State{ReusePolicy: state.ReusePolicy, Kind: state.Kind, ID: state.ID, Attributes: attributes}
}

func modelEventStream(ctx context.Context, source <-chan provider.Event) <-chan modelEvent {
	output := make(chan modelEvent)
	go func() {
		defer close(output)
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-source:
				if !ok {
					return
				}
				select {
				case output <- projectModelEvent(event):
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return output
}

func projectModelEvent(event provider.Event) modelEvent {
	calls := make([]tools.ToolCall, len(event.ToolCalls))
	for index, call := range event.ToolCalls {
		calls[index] = tools.ToolCall{ID: call.ID, Name: call.Name, Args: call.Args}
	}
	sources := append([]provider.Source(nil), event.Sources...)
	var stream *ToolCallStream
	if event.ToolCallStream != nil {
		stream = &ToolCallStream{ID: event.ToolCallStream.ID, Name: event.ToolCallStream.Name}
	}
	var state *modelStateEnvelope
	if event.ResponseState != nil {
		attributes := make(map[string]string, len(event.ResponseState.Attributes))
		for key, value := range event.ResponseState.Attributes {
			attributes[key] = value
		}
		state = &modelStateEnvelope{ReusePolicy: event.ResponseState.ReusePolicy, Kind: event.ResponseState.Kind, ID: event.ResponseState.ID, Attributes: attributes}
	}
	var hostedCall *provider.ToolCall
	if event.HostedToolCall != nil {
		call := *event.HostedToolCall
		hostedCall = &call
	}
	var hostedResult *provider.HostedToolResult
	if event.HostedResult != nil {
		result := *event.HostedResult
		result.Results = append([]provider.HostedToolResultItem(nil), event.HostedResult.Results...)
		if event.HostedResult.Error != nil {
			resultError := *event.HostedResult.Error
			result.Error = &resultError
		}
		result.Metadata = cloneAnyMap(event.HostedResult.Metadata)
		for index := range result.Results {
			result.Results[index].Metadata = cloneAnyMap(result.Results[index].Metadata)
		}
		hostedResult = &result
	}
	return modelEvent{
		Transport: providertransport.Clone(event.Transport), OutputPart: cloneOutputPart(event.OutputPart), Type: modelEventType(event.Type), Text: event.Text, ToolCallStream: stream, ToolCalls: calls,
		HostedToolCall: hostedCall, HostedResult: hostedResult,
		Sources: sources, Reason: event.Reason,
		Usage:      event.Usage,
		ResponseID: event.ResponseID, ResponseState: state, Err: event.Err,
	}
}

func (request agentPreparedRequest) PrepareRetryModel(ctx context.Context, next modelRequest) (preparedModelRequest, error) {
	replayer, ok := request.prepared.(provider.PreparedRequestReplay)
	if !ok {
		return nil, errors.New("expanded provider request does not support frozen retry preparation")
	}
	prepared, err := replayer.PrepareRetry(ctx, providerRequest(next))
	if err != nil {
		return nil, err
	}
	if prepared == nil {
		return nil, errors.New("provider returned nil retry preparation")
	}
	return agentPreparedRequest{prepared: prepared}, nil
}

func (request agentPreparedRequest) WithMaxOutputTokensModel(ctx context.Context, limit int64) (preparedModelRequest, error) {
	limiter, ok := request.prepared.(provider.PreparedRequestOutputLimiter)
	if !ok {
		return nil, errors.New("prepared gateway cannot set output limit")
	}
	next, err := limiter.WithMaxOutputTokens(ctx, limit)
	if err != nil {
		return nil, err
	}
	return agentPreparedRequest{prepared: next}, nil
}

func cloneOutputPart(part *provider.OutputPart) *provider.OutputPart {
	if part == nil {
		return nil
	}
	copy := *part
	return &copy
}

func (request agentPreparedRequest) InputMeasurementPrefix() requestmeasure.Prefix {
	if p, ok := request.prepared.(provider.PreparedRequestMeasurement); ok {
		return requestmeasure.Prefix(p.InputMeasurementPrefix())
	}
	return requestmeasure.Prefix{}
}
func (request agentPreparedRequest) MeasurementSuffix(prefix requestmeasure.Prefix) ([]byte, bool) {
	if p, ok := request.prepared.(provider.PreparedRequestMeasurement); ok {
		return p.MeasurementSuffix(provider.MeasurementPrefix(prefix))
	}
	return nil, false
}

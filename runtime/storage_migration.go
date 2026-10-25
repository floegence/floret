package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/floegence/floret/v7/identity"
)

// StorageMigrationState describes canonical storage preparation owned by a
// Host. A Host is available while this state is migrating or verifying.
type StorageMigrationState string

const (
	StorageMigrationPending   StorageMigrationState = "pending"
	StorageMigrationRunning   StorageMigrationState = "running"
	StorageMigrationReady     StorageMigrationState = "ready"
	StorageMigrationFailed    StorageMigrationState = "failed"
	StorageMigrationCancelled StorageMigrationState = "cancelled"
)

// StorageMigrationStatus is a point-in-time view of Host-owned startup work.
// Progress totals may be zero when a source format cannot be counted cheaply.
type StorageMigrationStatus struct {
	State     StorageMigrationState `json:"state"`
	Phase     StartupPhase          `json:"phase,omitempty"`
	Completed int64                 `json:"completed,omitempty"`
	Total     int64                 `json:"total,omitempty"`
	Error     string                `json:"error,omitempty"`
}

// ThreadReadiness reports whether a thread may use the canonical runtime.
// Reading this status never prepares a thread or waits for historical work.
type ThreadReadiness struct {
	ThreadID identity.ThreadID `json:"thread_id"`
	Ready    bool              `json:"ready"`
	// DisplayReady certifies prepared browsing sources without changing Ready.
	DisplayReady bool `json:"display_ready"`
	// DisplayError reports an optional browsing-source failure. It never changes
	// Ready or the existing canonical error result of Host.Readiness.
	DisplayError string                 `json:"display_error,omitempty"`
	Storage      StorageMigrationStatus `json:"storage"`
}

type storageReadiness struct {
	mu         sync.Mutex
	status     StorageMigrationStatus
	done       chan struct{}
	usableDone chan struct{}
	running    bool
	ready      bool
	usable     bool
	err        error
}

func newStorageReadiness() *storageReadiness {
	return &storageReadiness{status: StorageMigrationStatus{State: StorageMigrationPending}, usableDone: make(chan struct{})}
}

func (readiness *storageReadiness) markReady() {
	readiness.mu.Lock()
	defer readiness.mu.Unlock()
	if readiness.running && readiness.done != nil {
		close(readiness.done)
	}
	if !readiness.usable {
		close(readiness.usableDone)
	}
	readiness.running, readiness.ready, readiness.usable, readiness.err = false, true, true, nil
	readiness.status = StorageMigrationStatus{State: StorageMigrationReady}
}

// markUsable publishes the Host before the optional historical backfill has
// completed. Operation gates observe usability, while status remains running until
// the background worker calls markReady.
func (readiness *storageReadiness) markUsable() {
	readiness.mu.Lock()
	defer readiness.mu.Unlock()
	if !readiness.usable {
		readiness.usable = true
		close(readiness.usableDone)
	}
	if readiness.running {
		return
	}
	readiness.running = true
	readiness.done = make(chan struct{})
	readiness.status = StorageMigrationStatus{State: StorageMigrationRunning, Phase: StartupPhaseMigrating}
}

func (readiness *storageReadiness) begin() (chan struct{}, bool) {
	readiness.mu.Lock()
	defer readiness.mu.Unlock()
	if readiness.ready {
		return nil, false
	}
	if readiness.running {
		return readiness.done, false
	}
	readiness.running, readiness.err = true, nil
	readiness.done = make(chan struct{})
	readiness.status = StorageMigrationStatus{State: StorageMigrationRunning, Phase: StartupPhaseMigrating}
	return readiness.done, true
}

func (readiness *storageReadiness) update(phase StartupPhase, completed, total int64) {
	readiness.mu.Lock()
	defer readiness.mu.Unlock()
	if !readiness.running {
		return
	}
	readiness.status.State = StorageMigrationRunning
	readiness.status.Phase = phase
	readiness.status.Completed = completed
	readiness.status.Total = total
}

func (readiness *storageReadiness) finish(err error) {
	readiness.mu.Lock()
	defer readiness.mu.Unlock()
	if !readiness.running {
		return
	}
	readiness.running = false
	readiness.err = err
	if err == nil {
		if !readiness.usable {
			close(readiness.usableDone)
		}
		readiness.ready, readiness.usable = true, true
		readiness.status = StorageMigrationStatus{State: StorageMigrationReady}
	} else {
		state := StorageMigrationFailed
		if errors.Is(err, context.Canceled) {
			state = StorageMigrationCancelled
		}
		readiness.status.State = state
		readiness.status.Error = err.Error()
	}
	close(readiness.done)
}

func (readiness *storageReadiness) snapshot() StorageMigrationStatus {
	readiness.mu.Lock()
	defer readiness.mu.Unlock()
	return readiness.status
}

// waitUsable gates operations on owner installation, independently of backfill.
func (readiness *storageReadiness) waitUsable(ctx context.Context) error {
	if ctx == nil {
		return errors.New("storage readiness requires a context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for {
		readiness.mu.Lock()
		if readiness.usable {
			readiness.mu.Unlock()
			return nil
		}
		if readiness.err != nil {
			err := readiness.err
			readiness.mu.Unlock()
			return err
		}
		if !readiness.running || readiness.done == nil {
			readiness.mu.Unlock()
			return errors.New("storage migration has not been started")
		}
		done, usable := readiness.done, readiness.usableDone
		readiness.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
		case <-usable:
		}
	}
}

func (readiness *storageReadiness) wait(ctx context.Context) error {
	if ctx == nil {
		return errors.New("storage readiness requires a context")
	}
	for {
		readiness.mu.Lock()
		if readiness.ready {
			readiness.mu.Unlock()
			return nil
		}
		if readiness.err != nil {
			err := readiness.err
			readiness.mu.Unlock()
			return err
		}
		if !readiness.running || readiness.done == nil {
			readiness.mu.Unlock()
			return errors.New("storage migration has not been started")
		}
		done := readiness.done
		readiness.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
		}
	}
}

func (host *Host) StorageMigrationStatus(ctx context.Context) (StorageMigrationStatus, error) {
	if host == nil || ctx == nil {
		return StorageMigrationStatus{}, errors.New("storage migration status requires a Host and context")
	}
	if err := ctx.Err(); err != nil {
		return StorageMigrationStatus{}, err
	}
	if err := host.available(); err != nil {
		return StorageMigrationStatus{}, err
	}
	return host.startup.snapshot(), nil
}

func (host *Host) Readiness(ctx context.Context, threadID identity.ThreadID) (ThreadReadiness, error) {
	if ctx == nil {
		return ThreadReadiness{}, errors.New("thread readiness requires a context")
	}
	if _, err := identity.ParseThreadID(threadID.String()); err != nil {
		return ThreadReadiness{}, err
	}
	status, err := host.StorageMigrationStatus(ctx)
	if err != nil {
		return ThreadReadiness{}, err
	}
	readiness := ThreadReadiness{ThreadID: threadID, Storage: status}
	host.threadPreparations.mu.Lock()
	preparation := host.threadPreparations.threads[threadID]
	preparing := false
	if preparation != nil {
		select {
		case <-preparation.done:
			err = preparation.err
		default:
			preparing = true
		}
	}
	host.threadPreparations.mu.Unlock()
	if preparing || err != nil {
		return readiness, err
	}
	if status.State == StorageMigrationPending || !host.startup.isUsable() {
		return readiness, nil
	}
	if reader, ok := host.store.repo.(interface {
		ThreadPrepared(context.Context, string) (bool, error)
	}); ok {
		readiness.Ready, err = reader.ThreadPrepared(ctx, threadID.String())
		if err == nil && readiness.Ready {
			if display, ok := host.store.repo.(interface {
				DisplayPrepared(context.Context, string) (bool, error)
			}); ok {
				var displayErr error
				readiness.DisplayReady, displayErr = display.DisplayPrepared(ctx, threadID.String())
				if displayErr != nil {
					readiness.DisplayReady = false
					readiness.DisplayError = runtimeHostError(displayErr).Error()
				}
			}
		}
		return readiness, runtimeHostError(err)
	}
	return readiness, nil
}

// PrepareThread prepares only the requested thread and its explicit canonical
// dependencies. Cancelling the wait does not cancel Host-owned preparation.
func (host *Host) PrepareThread(ctx context.Context, threadID identity.ThreadID) error {
	if host == nil || ctx == nil {
		return errors.New("thread preparation requires a Host and context")
	}
	if _, err := identity.ParseThreadID(threadID.String()); err != nil {
		return err
	}
	if err := host.available(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if host.startup.failed() && !host.startup.isUsable() {
		if err := host.startStartupInitialization(); err != nil {
			return err
		}
	}
	host.threadPreparations.mu.Lock()

	preparation := host.threadPreparations.threads[threadID]
	if preparation != nil {
		select {
		case <-preparation.done:
			// Each explicit retry gets a new attempt; other threads remain usable.
			delete(host.threadPreparations.threads, threadID)
			preparation = nil
		default:
		}
	}
	if preparation == nil {
		workContext, finish, err := host.store.beginLifetimeOperationContext()
		if err != nil {
			host.threadPreparations.mu.Unlock()
			return err
		}
		preparation = &threadPreparation{done: make(chan struct{})}
		host.threadPreparations.threads[threadID] = preparation
		go func() {
			defer finish()
			var workErr error
			defer func() {
				if recovered := recover(); recovered != nil {
					workErr = fmt.Errorf("thread preparation panicked: %v", recovered)
				}
				host.threadPreparations.mu.Lock()
				preparation.err = workErr
				close(preparation.done)
				if workErr == nil {
					// Canonical readiness may change after deletion or a new leaf.
					// Successful attempts never become a second readiness owner.
					delete(host.threadPreparations.threads, threadID)
				} else {
					host.trimThreadPreparationFailures()
				}
				host.threadPreparations.mu.Unlock()
			}()
			workErr = host.prepareThread(workContext, threadID)
			if workErr == nil && host.startup.failed() && host.startup.isUsable() {
				host.startBackgroundThreadMigration()
			}
		}()
	}
	host.threadPreparations.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-preparation.done:
		return preparation.err
	}
}

type threadPreparationState struct {
	mu      sync.Mutex
	threads map[identity.ThreadID]*threadPreparation
}

type threadPreparation struct {
	done chan struct{}
	err  error
}

func preparationFinished(preparation *threadPreparation) bool {
	select {
	case <-preparation.done:
		return true
	default:
		return false
	}
}

// Keep recent failures observable without retaining every attempted identity
// for the Host lifetime. Active preparations are never evicted.
func (host *Host) trimThreadPreparationFailures() {
	const retainedFailures = 256
	completed := 0
	for _, attempt := range host.threadPreparations.threads {
		if preparationFinished(attempt) {
			completed++
		}
	}
	for id, attempt := range host.threadPreparations.threads {
		if completed <= retainedFailures {
			break
		}
		if preparationFinished(attempt) {
			delete(host.threadPreparations.threads, id)
			completed--
		}
	}
}

func (host *Host) prepareThread(ctx context.Context, threadID identity.ThreadID) error {
	if preparer, ok := host.store.repo.(interface {
		PrepareThread(context.Context, string) error
	}); ok {
		if err := preparer.PrepareThread(ctx, threadID.String()); err != nil {
			return runtimeHostError(err)
		}
	}
	if _, err := host.store.repo.Thread(ctx, threadID.String()); err != nil {
		return runtimeHostError(err)
	}
	if preparer, ok := host.store.repo.(interface {
		PrepareQueryThread(context.Context, string) error
	}); ok {
		if err := preparer.PrepareQueryThread(ctx, threadID.String()); err != nil {
			return runtimeHostError(err)
		}
	}
	return nil
}

// PrepareDisplayThread prepares canonical browsing sources after ordinary thread
// preparation. It does not activate execution. Cancellation stops this explicit
// display wait/work; the automatic Host worker may continue it later. Existing
// PrepareThread and Readiness.Ready retain their published runtime semantics.
func (host *Host) PrepareDisplayThread(ctx context.Context, threadID identity.ThreadID) error {
	if err := host.PrepareThread(ctx, threadID); err != nil {
		return err
	}
	if preparer, ok := host.store.repo.(interface {
		PrepareDisplayThread(context.Context, string) error
	}); ok {
		work, finish, err := host.store.beginOperationContext(ctx)
		if err != nil {
			return runtimeHostError(err)
		}
		defer finish()
		return runtimeHostError(preparer.PrepareDisplayThread(work, threadID.String()))
	}
	return ErrUnsupportedStoreCapability
}

func (readiness *storageReadiness) isUsable() bool {
	readiness.mu.Lock()
	defer readiness.mu.Unlock()
	return readiness.usable
}

func (readiness *storageReadiness) failed() bool {
	readiness.mu.Lock()
	defer readiness.mu.Unlock()
	return !readiness.ready && !readiness.running && readiness.err != nil
}

func (readiness *storageReadiness) statusError() error {
	readiness.mu.Lock()
	defer readiness.mu.Unlock()
	if readiness.err == nil {
		return nil
	}
	return fmt.Errorf("storage migration failed: %w", readiness.err)
}

func (readiness *storageReadiness) waitAttempt() {
	readiness.mu.Lock()
	done := readiness.done
	readiness.mu.Unlock()
	if done != nil {
		<-done
	}
}

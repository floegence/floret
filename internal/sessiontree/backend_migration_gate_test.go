package sessiontree

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestMigrationGatePrioritizesForegroundAndCancelsOnlyBackgroundAttempt(t *testing.T) {
	var gate migrationGate
	background, releaseBackground, err := gate.acquire(t.Context(), false)
	if err != nil {
		t.Fatal(err)
	}
	foregroundEntered := make(chan struct{})
	releaseForeground := make(chan struct{})
	foregroundDone := make(chan error, 1)
	go func() {
		_, release, err := gate.acquire(t.Context(), true)
		if err == nil {
			close(foregroundEntered)
			<-releaseForeground
			release()
		}
		foregroundDone <- err
	}()
	select {
	case <-background.Done():
	case <-time.After(time.Second):
		t.Fatal("foreground did not preempt background validation")
	}
	backgroundEntered := make(chan struct{})
	backgroundDone := make(chan error, 1)
	go func() {
		_, release, err := gate.acquire(t.Context(), false)
		if err == nil {
			close(backgroundEntered)
			release()
		}
		backgroundDone <- err
	}()
	releaseBackground()
	select {
	case <-foregroundEntered:
	case <-backgroundEntered:
		t.Fatal("background bypassed pending foreground preparation")
	case <-time.After(time.Second):
		t.Fatal("foreground did not acquire migration owner")
	}
	select {
	case <-backgroundEntered:
		t.Fatal("two migration attempts ran simultaneously")
	default:
	}
	close(releaseForeground)
	if err := <-foregroundDone; err != nil {
		t.Fatal(err)
	}
	if err := <-backgroundDone; err != nil {
		t.Fatal(err)
	}
	if err := t.Context().Err(); err != nil {
		t.Fatal("preemption cancelled the Host context")
	}
}

func TestMigrationGateCancelledWaitReleasesForegroundPriority(t *testing.T) {
	var gate migrationGate
	_, release, err := gate.acquire(t.Context(), true)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := gate.acquire(ctx, true); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled wait = %v", err)
	}
	release()
	ctx, cancel = context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	_, release, err = gate.acquire(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	release()
}

func TestMigrationGateForegroundReadPausesBackgroundWithoutOwningMigration(t *testing.T) {
	var gate migrationGate
	background, releaseBackground, err := gate.acquire(t.Context(), false)
	if err != nil {
		t.Fatal(err)
	}
	releaseFirst, err := gate.foregroundRead(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	releaseSecond, err := gate.foregroundRead(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !errors.Is(background.Err(), context.Canceled) {
		t.Fatal("foreground read did not preempt active background work")
	}
	releaseBackground()

	// Reads must not block the same thread's explicit migration or deletion.
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	_, releaseForeground, err := gate.acquire(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	releaseForeground()

	entered := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, release, err := gate.acquire(t.Context(), false)
		if err == nil {
			close(entered)
			release()
		}
		done <- err
	}()
	releaseFirst()
	select {
	case <-entered:
		t.Fatal("background resumed with another foreground reader active")
	case <-time.After(20 * time.Millisecond):
	}
	releaseSecond()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("background did not resume after all reads settled")
	}
	if err := t.Context().Err(); err != nil {
		t.Fatal("foreground read canceled the Host")
	}
}

func TestMigrationGateCancelledReadDoesNotBlockBackground(t *testing.T) {
	var gate migrationGate
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := gate.foregroundRead(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	ctx, cancel = context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	_, release, err := gate.acquire(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	release()
}

func TestInventoryPagesYieldToForegroundAndHonorCancellation(t *testing.T) {
	var gate migrationGate
	release, err := gate.foregroundRead(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := gate.waitForForegroundReads(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("inventory did not yield: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- gate.waitForForegroundReads(t.Context()) }()
	select {
	case err := <-done:
		t.Fatalf("inventory passed active projection: %v", err)
	default:
	}
	release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("inventory did not resume")
	}
}

func TestDelegatedInventoryReadUsesPreparedRepositoryForegroundGate(t *testing.T) {
	_, _, repo := linearPageFixture(t, false)
	wrapper := &BackendRepo{}
	wrapper.delegate.Store(repo)
	release, err := repo.migrationGate.foregroundRead(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	_, err = wrapper.PathMetadataWindow(ctx, PathWindowOptions{ThreadID: "thread", Direction: PathWindowBefore, Limit: 1, YieldToForeground: true})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("delegated inventory bypassed foreground owner: %v", err)
	}
}

func TestCurrentSummaryYieldsToSelectedWindowAndResumes(t *testing.T) {
	_, _, repo := linearPageFixture(t, false)
	options := ItemMembershipWindowOptions{ThreadID: "thread", Limit: 1, SummaryOnly: true, Summary: true}
	fields := func(PathMetadata, bool) []string { return nil }
	if _, err := repo.ReadWindowProjection(t.Context(), options, fields); err != nil {
		t.Fatal(err)
	}
	wrapper := &BackendRepo{}
	wrapper.delegate.Store(repo)
	release, err := repo.BeginForegroundRead(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	release = sync.OnceFunc(release)
	defer release()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if _, err := wrapper.ReadWindowProjection(ctx, options, fields); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("current summary bypassed selected window: %v", err)
	}
	// The selected window itself must stay eligible while summaries yield.
	selected := options
	selected.SummaryOnly = false
	if _, err := wrapper.ReadWindowProjection(t.Context(), selected, fields); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := wrapper.ReadWindowProjection(t.Context(), options, fields); done <- err }()
	release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("summary did not resume after selected window")
	}
}

func TestSelectedReadPrioritySurvivesProjectionUntilBodyReadCompletes(t *testing.T) {
	_, _, repo := linearPageFixture(t, false)
	wrapper := &BackendRepo{}
	wrapper.delegate.Store(repo)
	release, err := wrapper.BeginForegroundRead(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	projectionRelease, err := repo.migrationGate.foregroundRead(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	projectionRelease()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := repo.migrationGate.waitForForegroundReads(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("inventory resumed before selected body reads: %v", err)
	}
	release()
	if err := repo.migrationGate.waitForForegroundReads(t.Context()); err != nil {
		t.Fatal(err)
	}
}

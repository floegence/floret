package sessiontree

import (
	"context"
	"errors"
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

package sessiontree

import (
	"context"
	"sync"
)

// One in-process owner coordinates migration batches. A foreground request
// cancels background validation at its next read boundary; staged source pages
// remain committed and reusable. Normal canonical operations bypass this gate.
type migrationGate struct {
	mu                sync.Mutex
	changed           chan struct{}
	active            bool
	foregroundWaiters int
	backgroundCancel  context.CancelFunc
}

func (gate *migrationGate) acquire(ctx context.Context, foreground bool) (context.Context, func(), error) {
	gate.mu.Lock()
	if gate.changed == nil {
		gate.changed = make(chan struct{})
	}
	if foreground {
		gate.foregroundWaiters++
		if gate.backgroundCancel != nil {
			gate.backgroundCancel()
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			if foreground {
				gate.foregroundWaiters--
				gate.signal()
			}
			gate.mu.Unlock()
			return nil, nil, err
		}
		if !gate.active && (foreground || gate.foregroundWaiters == 0) {
			break
		}
		changed := gate.changed
		gate.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-changed:
		}
		gate.mu.Lock()
	}
	if foreground {
		gate.foregroundWaiters--
	}
	gate.active = true
	work, cancel := context.WithCancel(ctx)
	if !foreground {
		gate.backgroundCancel = cancel
	}
	gate.mu.Unlock()
	return work, func() {
		cancel()
		gate.mu.Lock()
		gate.active, gate.backgroundCancel = false, nil
		gate.signal()
		gate.mu.Unlock()
	}, nil
}

func (gate *migrationGate) signal() {
	close(gate.changed)
	gate.changed = make(chan struct{})
}

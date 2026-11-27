package threadsqlite

import (
	"container/list"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

type poolEntry struct {
	file    FileID
	db      *sql.DB
	refs    int
	idle    *list.Element
	since   time.Time
	closing bool
	private bool
}

type filePool struct {
	mu       sync.Mutex
	entries  map[FileID]*poolEntry
	idle     list.List
	waiting  list.List
	granted  int
	changed  chan struct{}
	timer    *time.Timer
	limit    int
	maxIdle  int
	ttl      time.Duration
	open     func(context.Context, FileID) (*sql.DB, error)
	stopped  bool
	closeErr error
}

func newFilePool(limit, maxIdle int, ttl time.Duration, open func(context.Context, FileID) (*sql.DB, error)) (*filePool, error) {
	if limit < 1 || maxIdle < 0 || maxIdle > limit || ttl <= 0 || open == nil {
		return nil, spi.ErrInvalidArgument
	}
	return &filePool{entries: make(map[FileID]*poolEntry), changed: make(chan struct{}), limit: limit, maxIdle: maxIdle, ttl: ttl, open: open}, nil
}

func (pool *filePool) notifyLocked() {
	close(pool.changed)
	pool.changed = make(chan struct{})
}

func (pool *filePool) withFile(ctx context.Context, file FileID, use func(*sql.DB) error) error {
	return pool.access(ctx, file, false, pool.open, use)
}

func (pool *filePool) access(ctx context.Context, file FileID, create bool, open func(context.Context, FileID) (*sql.DB, error), use func(*sql.DB) error) error {
	if ctx == nil || file == (FileID{}) || use == nil {
		return spi.ErrInvalidArgument
	}
	return pool.admit(ctx, 1, func() error { return pool.accessGranted(ctx, file, create, open, use) })
}

func (pool *filePool) admit(ctx context.Context, count int, use func() error) error {
	if ctx == nil || count < 1 || count > pool.limit {
		return spi.ErrInvalidArgument
	}
	pool.mu.Lock()
	var place *list.Element
	for {
		err := ctx.Err()
		if pool.stopped || pool.closeErr != nil {
			err = errors.Join(spi.ErrClosed, pool.closeErr)
		}
		if err != nil {
			if place != nil {
				pool.waiting.Remove(place)
				pool.notifyLocked()
			}
			pool.mu.Unlock()
			return err
		}
		if place == pool.waiting.Front() && pool.limit-pool.granted >= count {
			if place != nil {
				pool.waiting.Remove(place)
				pool.notifyLocked()
			}
			pool.granted += count
			pool.mu.Unlock()
			defer func() {
				pool.mu.Lock()
				pool.granted -= count
				pool.notifyLocked()
				pool.mu.Unlock()
			}()
			return use()
		}
		if place == nil {
			place = pool.waiting.PushBack(count)
		}
		changed := pool.changed
		pool.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
		}
		pool.mu.Lock()
	}
}

func (pool *filePool) accessGranted(ctx context.Context, file FileID, create bool, open func(context.Context, FileID) (*sql.DB, error), use func(*sql.DB) error) error {
	return pool.accessFile(ctx, file, create, false, open, use)
}

func (pool *filePool) accessFile(ctx context.Context, file FileID, create, private bool, open func(context.Context, FileID) (*sql.DB, error), use func(*sql.DB) error) error {
	for {
		pool.mu.Lock()
		if err := ctx.Err(); err != nil {
			pool.mu.Unlock()
			return err
		}
		if pool.stopped || pool.closeErr != nil {
			err := errors.Join(spi.ErrClosed, pool.closeErr)
			pool.mu.Unlock()
			return err
		}
		entry := pool.entries[file]
		switch {
		case entry != nil && entry.db != nil && !entry.closing:
			if create {
				pool.mu.Unlock()
				return os.ErrExist
			}
			if entry.idle != nil {
				pool.idle.Remove(entry.idle)
				entry.idle = nil
			}
			entry.refs++
			pool.mu.Unlock()
			defer pool.release(entry)
			return use(entry.db)
		case entry == nil && len(pool.entries) < pool.limit:
			entry = &poolEntry{file: file, refs: 1, private: private}
			pool.entries[file] = entry
			pool.mu.Unlock()
			db, err := open(ctx, file)
			pool.mu.Lock()
			if err != nil || db == nil {
				delete(pool.entries, file)
				pool.notifyLocked()
				pool.mu.Unlock()
				if err == nil {
					return fmt.Errorf("%w: missing thread database", spi.ErrInvalidArgument)
				}
				return err
			}
			db.SetMaxOpenConns(1)
			db.SetMaxIdleConns(1)
			entry.db = db
			stopped := pool.stopped || pool.closeErr != nil
			pool.notifyLocked()
			pool.mu.Unlock()
			defer pool.release(entry)
			if stopped {
				return spi.ErrClosed
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			return use(db)
		case entry == nil && pool.idle.Len() != 0:
			old := pool.detachOldestLocked()
			pool.mu.Unlock()
			pool.closeFile(old)
		default:
			changed := pool.changed
			pool.mu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-changed:
			}
		}
	}
}

func (pool *filePool) release(entry *poolEntry) {
	pool.mu.Lock()
	entry.refs--
	if entry.refs != 0 {
		pool.mu.Unlock()
		return
	}
	if entry.private {
		entry.closing = true
		pool.mu.Unlock()
		pool.closeFile(entry)
		return
	}
	entry.since = time.Now()
	entry.idle = pool.idle.PushBack(entry)
	var old *poolEntry
	if pool.stopped || pool.closeErr != nil || pool.idle.Len() > pool.maxIdle {
		old = pool.detachOldestLocked()
	}
	pool.armLocked()
	pool.notifyLocked()
	pool.mu.Unlock()
	if old != nil {
		pool.closeFile(old)
	}
}

func (pool *filePool) detachOldestLocked() *poolEntry {
	old := pool.idle.Remove(pool.idle.Front()).(*poolEntry)
	old.idle = nil
	old.closing = true
	return old
}

func (pool *filePool) evict(ctx context.Context, file FileID) error {
	for {
		pool.mu.Lock()
		entry := pool.entries[file]
		if entry == nil {
			err := pool.closeErr
			pool.mu.Unlock()
			return err
		}
		if entry.idle != nil {
			pool.idle.Remove(entry.idle)
			entry.idle, entry.closing = nil, true
			pool.mu.Unlock()
			pool.closeFile(entry)
			continue
		}
		changed := pool.changed
		pool.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

func (pool *filePool) closeFile(entry *poolEntry) {
	err := entry.db.Close()
	pool.mu.Lock()
	defer pool.mu.Unlock()
	pool.closeErr = errors.Join(pool.closeErr, err)
	delete(pool.entries, entry.file)
	pool.notifyLocked()
}

func (pool *filePool) armLocked() {
	if pool.timer != nil || pool.stopped || pool.idle.Len() == 0 {
		return
	}
	old := pool.idle.Front().Value.(*poolEntry)
	pool.timer = time.AfterFunc(time.Until(old.since.Add(pool.ttl)), pool.expire)
}

func (pool *filePool) expire() {
	pool.mu.Lock()
	pool.timer = nil
	for !pool.stopped && pool.idle.Len() != 0 {
		old := pool.idle.Front().Value.(*poolEntry)
		if time.Since(old.since) < pool.ttl {
			break
		}
		pool.detachOldestLocked()
		pool.mu.Unlock()
		pool.closeFile(old)
		pool.mu.Lock()
	}
	pool.armLocked()
	pool.mu.Unlock()
}

func (pool *filePool) shutdown(ctx context.Context) error {
	if ctx == nil {
		return spi.ErrInvalidArgument
	}
	pool.mu.Lock()
	pool.stopped = true
	if pool.timer != nil {
		pool.timer.Stop()
		pool.timer = nil
	}
	pool.notifyLocked()
	for len(pool.entries) != 0 || pool.granted != 0 {
		if pool.idle.Len() != 0 {
			old := pool.detachOldestLocked()
			pool.mu.Unlock()
			pool.closeFile(old)
			pool.mu.Lock()
			continue
		}
		changed := pool.changed
		pool.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
		pool.mu.Lock()
	}
	err := pool.closeErr
	pool.mu.Unlock()
	return err
}

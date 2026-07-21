package sqlite

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

type writerAdmissionEntry struct {
	token        chan struct{}
	handles      int
	reservations int
}

var writerAdmissions = struct {
	sync.Mutex
	entries map[string]*writerAdmissionEntry
}{entries: map[string]*writerAdmissionEntry{}}

var memoryWriterAdmissionID atomic.Uint64

// WriterAdmission serializes writers opened by this process for one physical
// SQLite database while keeping admission responsive to context cancellation.
type WriterAdmission struct {
	key       string
	entry     *writerAdmissionEntry
	closed    bool
	closedCh  chan struct{}
	closeOnce sync.Once
}

func NewWriterAdmission(path string) (*WriterAdmission, error) {
	var key string
	if path == ":memory:" {
		key = path + ":" + strconv.FormatUint(memoryWriterAdmissionID.Add(1), 10)
	} else {
		var err error
		key, err = CanonicalDatabasePath(path)
		if err != nil {
			return nil, err
		}
	}
	writerAdmissions.Lock()
	defer writerAdmissions.Unlock()
	entry := writerAdmissions.entries[key]
	if entry == nil {
		entry = &writerAdmissionEntry{token: make(chan struct{}, 1)}
		entry.token <- struct{}{}
		writerAdmissions.entries[key] = entry
	}
	entry.handles++
	return &WriterAdmission{key: key, entry: entry, closedCh: make(chan struct{})}, nil
}

func (a *WriterAdmission) Reserve(ctx context.Context) (func(), error) {
	if ctx == nil {
		return nil, errors.New("sqlite writer context is required")
	}
	if a == nil || a.entry == nil {
		return nil, errors.New("sqlite writer admission is required")
	}

	writerAdmissions.Lock()
	if a.closed {
		writerAdmissions.Unlock()
		return nil, errors.New("sqlite writer admission is closed")
	}
	a.entry.reservations++
	writerAdmissions.Unlock()

	releaseReservation := func() {
		writerAdmissions.Lock()
		a.entry.reservations--
		deleteWriterAdmissionIfUnused(a.key, a.entry)
		writerAdmissions.Unlock()
	}

	select {
	case <-a.closedCh:
		releaseReservation()
		return nil, errors.New("sqlite writer admission is closed")
	case <-ctx.Done():
		releaseReservation()
		return nil, ctx.Err()
	case <-a.entry.token:
		writerAdmissions.Lock()
		closed := a.closed
		writerAdmissions.Unlock()
		if closed {
			a.entry.token <- struct{}{}
			releaseReservation()
			return nil, errors.New("sqlite writer admission is closed")
		}
		if err := ctx.Err(); err != nil {
			a.entry.token <- struct{}{}
			releaseReservation()
			return nil, err
		}
	}

	var once sync.Once
	return func() {
		once.Do(func() {
			a.entry.token <- struct{}{}
			releaseReservation()
		})
	}, nil
}

func (a *WriterAdmission) Close() {
	if a == nil {
		return
	}
	a.closeOnce.Do(func() {
		writerAdmissions.Lock()
		a.closed = true
		close(a.closedCh)
		a.entry.handles--
		deleteWriterAdmissionIfUnused(a.key, a.entry)
		writerAdmissions.Unlock()
	})
}

func deleteWriterAdmissionIfUnused(key string, entry *writerAdmissionEntry) {
	if entry.handles == 0 && entry.reservations == 0 && writerAdmissions.entries[key] == entry {
		delete(writerAdmissions.entries, key)
	}
}

func CanonicalDatabasePath(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", errors.New("sqlite writer path is required")
	}
	if path == ":memory:" {
		return path, nil
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	return canonicalDatabasePath(abs, map[string]struct{}{})
}

func canonicalDatabasePath(abs string, followed map[string]struct{}) (string, error) {
	cursor := abs
	var missing []string
	for {
		resolved, err := filepath.EvalSymlinks(cursor)
		if err == nil {
			for i := len(missing) - 1; i >= 0; i-- {
				resolved = filepath.Join(resolved, missing[i])
			}
			return resolved, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		info, lstatErr := os.Lstat(cursor)
		if lstatErr == nil && info.Mode()&os.ModeSymlink != 0 {
			if _, duplicate := followed[cursor]; duplicate {
				return "", errors.New("sqlite database path contains a symlink cycle")
			}
			followed[cursor] = struct{}{}
			target, err := os.Readlink(cursor)
			if err != nil {
				return "", err
			}
			if !filepath.IsAbs(target) {
				target = filepath.Join(filepath.Dir(cursor), target)
			}
			resolved, err := canonicalDatabasePath(filepath.Clean(target), followed)
			if err != nil {
				return "", err
			}
			for i := len(missing) - 1; i >= 0; i-- {
				resolved = filepath.Join(resolved, missing[i])
			}
			return resolved, nil
		}
		if lstatErr != nil && !errors.Is(lstatErr, os.ErrNotExist) {
			return "", lstatErr
		}
		parent := filepath.Dir(cursor)
		if parent == cursor {
			return "", err
		}
		missing = append(missing, filepath.Base(cursor))
		cursor = parent
	}
}

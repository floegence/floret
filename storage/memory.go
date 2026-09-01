package storage

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/floegence/floret/v7/storage/spi"
)

type memorySource struct{}

// Memory returns an in-memory Source suitable for production ephemeral use
// and deterministic tests.
func Memory() Source {
	return NewSource(memorySource{})
}

func (memorySource) Open(ctx context.Context) (spi.Backend, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: open context is required", spi.ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &memoryBackend{records: make(recordSet)}, nil
}

type recordSet map[string]map[string][]byte

type memoryBackend struct {
	mu      sync.RWMutex
	records recordSet
	version uint64
	closed  bool
}

func (backend *memoryBackend) View(ctx context.Context, callback func(spi.ReadTx) error) error {
	if err := validateCallback(ctx, callback != nil); err != nil {
		return err
	}
	records, _, err := backend.snapshot()
	if err != nil {
		return err
	}
	tx := &memoryTx{ctx: ctx, records: records, active: true, readOnly: true}
	if err := func() error {
		defer tx.expire()
		return callback(tx)
	}(); err != nil {
		return err
	}
	return ctx.Err()
}

func (backend *memoryBackend) Update(ctx context.Context, callback func(spi.WriteTx) error) error {
	if err := validateCallback(ctx, callback != nil); err != nil {
		return err
	}
	records, version, err := backend.snapshot()
	if err != nil {
		return err
	}
	tx := &memoryTx{ctx: ctx, records: records, active: true}
	if err := func() error {
		defer tx.expire()
		return callback(tx)
	}(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !tx.isDirty() {
		return nil
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if backend.closed {
		return spi.ErrClosed
	}
	if backend.version != version {
		return spi.ErrConflict
	}
	backend.records = records
	backend.version++
	return nil
}

func (backend *memoryBackend) Close() error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.closed = true
	return nil
}

func (backend *memoryBackend) ResetFloretStorage(ctx context.Context) (bool, error) {
	if ctx == nil {
		return false, fmt.Errorf("%w: reset context is required", spi.ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if backend.closed {
		return false, spi.ErrClosed
	}
	hadRecords := len(backend.records) > 0
	backend.records = make(recordSet)
	backend.version++
	return hadRecords, nil
}

func (backend *memoryBackend) snapshot() (recordSet, uint64, error) {
	backend.mu.RLock()
	defer backend.mu.RUnlock()
	if backend.closed {
		return nil, 0, spi.ErrClosed
	}
	return cloneRecordSet(backend.records), backend.version, nil
}

type memoryTx struct {
	mu       sync.Mutex
	ctx      context.Context
	records  recordSet
	active   bool
	readOnly bool
	dirty    bool
}

func (tx *memoryTx) Get(namespace string, key []byte) ([]byte, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.validate(namespace, key); err != nil {
		return nil, err
	}
	value, ok := tx.records[namespace][string(key)]
	if !ok {
		return nil, spi.ErrNotFound
	}
	return bytes.Clone(value), nil
}

func (tx *memoryTx) Scan(request spi.ScanRequest) (spi.ScanPage, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.usable(); err != nil {
		return spi.ScanPage{}, err
	}
	if request.Namespace == "" || request.Limit <= 0 {
		return spi.ScanPage{}, fmt.Errorf("%w: namespace and positive limit are required", spi.ErrInvalidArgument)
	}
	if len(request.End) > 0 && len(request.Start) > 0 && bytes.Compare(request.Start, request.End) >= 0 {
		return spi.ScanPage{}, fmt.Errorf("%w: scan start must precede end", spi.ErrInvalidArgument)
	}
	keys := make([][]byte, 0, len(tx.records[request.Namespace]))
	for encoded := range tx.records[request.Namespace] {
		key := []byte(encoded)
		if len(request.Start) > 0 && bytes.Compare(key, request.Start) < 0 {
			continue
		}
		if len(request.End) > 0 && bytes.Compare(key, request.End) >= 0 {
			continue
		}
		if len(request.After) > 0 && bytes.Compare(key, request.After) <= 0 {
			continue
		}
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })
	hasMore := len(keys) > request.Limit
	if hasMore {
		keys = keys[:request.Limit]
	}
	page := spi.ScanPage{Records: make([]spi.Record, 0, len(keys)), HasMore: hasMore}
	for _, key := range keys {
		page.Records = append(page.Records, spi.Record{
			Key:   bytes.Clone(key),
			Value: bytes.Clone(tx.records[request.Namespace][string(key)]),
		})
	}
	if hasMore {
		page.Next = bytes.Clone(keys[len(keys)-1])
	}
	return page, nil
}

func (tx *memoryTx) Put(namespace string, key, value []byte) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.validateWrite(namespace, key); err != nil {
		return err
	}
	if tx.records[namespace] == nil {
		tx.records[namespace] = make(map[string][]byte)
	}
	tx.records[namespace][string(key)] = bytes.Clone(value)
	tx.dirty = true
	return nil
}

func (tx *memoryTx) Delete(namespace string, key []byte) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.validateWrite(namespace, key); err != nil {
		return err
	}
	if _, ok := tx.records[namespace][string(key)]; !ok {
		return spi.ErrNotFound
	}
	delete(tx.records[namespace], string(key))
	tx.dirty = true
	return nil
}

func (tx *memoryTx) validate(namespace string, key []byte) error {
	if err := tx.usable(); err != nil {
		return err
	}
	if namespace == "" || len(key) == 0 {
		return fmt.Errorf("%w: namespace and key are required", spi.ErrInvalidArgument)
	}
	return nil
}

func (tx *memoryTx) validateWrite(namespace string, key []byte) error {
	if err := tx.validate(namespace, key); err != nil {
		return err
	}
	if tx.readOnly {
		return fmt.Errorf("%w: read transaction cannot write", spi.ErrInvalidArgument)
	}
	return nil
}

func (tx *memoryTx) usable() error {
	if !tx.active {
		return spi.ErrTransactionClosed
	}
	return tx.ctx.Err()
}

func (tx *memoryTx) isDirty() bool {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.dirty
}

func (tx *memoryTx) expire() {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	tx.active = false
	tx.records = nil
}

func validateCallback(ctx context.Context, present bool) error {
	if ctx == nil {
		return fmt.Errorf("%w: transaction context is required", spi.ErrInvalidArgument)
	}
	if !present {
		return fmt.Errorf("%w: transaction callback is required", spi.ErrInvalidArgument)
	}
	return ctx.Err()
}

func cloneRecordSet(source recordSet) recordSet {
	clone := make(recordSet, len(source))
	for namespace, records := range source {
		clone[namespace] = make(map[string][]byte, len(records))
		for key, value := range records {
			clone[namespace][key] = bytes.Clone(value)
		}
	}
	return clone
}

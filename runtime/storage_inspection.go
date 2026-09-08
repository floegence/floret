package runtime

import (
	"bytes"
	"context"
	"errors"
	"os"
	"sort"
	"time"

	internalstorage "github.com/floegence/floret/v7/internal/storage"
	"github.com/floegence/floret/v7/internal/storagebridge"
	publicstorage "github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
)

// StorageInspection describes compatibility without exposing domain records.
type StorageInspection struct {
	Exists            bool `json:"exists"`
	MigrationRequired bool `json:"migration_required"`
}

// InspectSQLite validates the committed SQLite state and supported migration
// path without creating a Host or writing to the source. Migration writes are
// evaluated in a disposable memory overlay of one read transaction. Callers
// must exclude all writers until they finish their coordinated startup.
func InspectSQLite(ctx context.Context, path string) (result StorageInspection, err error) {
	if ctx == nil || path == "" {
		return result, errors.New("storage inspection requires a context and path")
	}
	if err = ctx.Err(); err != nil {
		return result, err
	}
	if _, err = os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return result, nil
	} else if err != nil {
		return result, err
	}
	result.Exists = true
	backend, err := storagebridge.OpenReadOnly(ctx, storagebridge.Source(publicstorage.SQLite(path)))
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, backend.Close()) }()
	err = backend.View(ctx, func(read spi.ReadTx) error {
		tx := &inspectionOverlay{ReadTx: read, writes: make(map[string]map[string]*[]byte)}
		logical, err := inspectLogicalSchemaTransaction(tx)
		if err != nil {
			return err
		}
		kernel, err := internalstorage.NewBackendKernelInTransaction(ctx, backend, tx, time.Now, nil)
		if err != nil {
			return err
		}
		if err = commitLogicalSchemaTransaction(tx, logical); err != nil {
			return err
		}
		if err = kernel.VerifyCurrentStateInTransaction(ctx, tx); err != nil {
			return err
		}
		result.MigrationRequired = len(tx.writes) > 0
		return ctx.Err()
	})
	return result, runtimeHostError(err)
}

// inspectionOverlay is transaction-local, never a second storage owner.
type inspectionOverlay struct {
	spi.ReadTx
	writes map[string]map[string]*[]byte
}

func (tx *inspectionOverlay) Get(namespace string, key []byte) ([]byte, error) {
	if value, ok := tx.writes[namespace][string(key)]; ok {
		if value == nil {
			return nil, spi.ErrNotFound
		}
		return bytes.Clone(*value), nil
	}
	return tx.ReadTx.Get(namespace, key)
}

func (tx *inspectionOverlay) Put(namespace string, key, value []byte) error {
	if tx.writes[namespace] == nil {
		tx.writes[namespace] = make(map[string]*[]byte)
	}
	copy := bytes.Clone(value)
	tx.writes[namespace][string(key)] = &copy
	return nil
}

func (tx *inspectionOverlay) Delete(namespace string, key []byte) error {
	if tx.writes[namespace] == nil {
		tx.writes[namespace] = make(map[string]*[]byte)
	}
	tx.writes[namespace][string(key)] = nil
	return nil
}

func (tx *inspectionOverlay) Scan(request spi.ScanRequest) (spi.ScanPage, error) {
	if request.Limit <= 0 {
		return spi.ScanPage{}, spi.ErrInvalidArgument
	}
	if len(tx.writes[request.Namespace]) == 0 {
		return tx.ReadTx.Scan(request)
	}
	rows := make(map[string][]byte)
	readRequest := request
	readRequest.Limit = 256
	for {
		page, err := tx.ReadTx.Scan(readRequest)
		if err != nil {
			return spi.ScanPage{}, err
		}
		for _, row := range page.Records {
			rows[string(row.Key)] = row.Value
		}
		if !page.HasMore {
			break
		}
		readRequest.After = page.Next
	}
	for key, value := range tx.writes[request.Namespace] {
		if (len(request.Start) > 0 && bytes.Compare([]byte(key), request.Start) < 0) || (len(request.End) > 0 && bytes.Compare([]byte(key), request.End) >= 0) || (len(request.After) > 0 && bytes.Compare([]byte(key), request.After) <= 0) {
			continue
		}
		if value == nil {
			delete(rows, key)
		} else {
			rows[key] = bytes.Clone(*value)
		}
	}
	keys := make([]string, 0, len(rows))
	for key := range rows {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	page := spi.ScanPage{HasMore: len(keys) > request.Limit}
	if page.HasMore {
		keys = keys[:request.Limit]
	}
	for _, key := range keys {
		page.Records = append(page.Records, spi.Record{Key: []byte(key), Value: rows[key]})
	}
	if page.HasMore {
		page.Next = []byte(keys[len(keys)-1])
	}
	return page, nil
}

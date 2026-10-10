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
		physicalMigration, err := storagebridge.PhysicalMigrationRequired(read)
		if err != nil {
			return err
		}
		tx := &inspectionOverlay{ReadTx: read, writes: make(map[string]map[string]*[]byte)}
		logical, err := inspectLogicalSchemaTransaction(tx)
		if err != nil {
			return err
		}
		kernel, err := internalstorage.NewBackendKernelInTransaction(ctx, backend, tx, time.Now, logical != logicalSchemaCurrent, nil)
		if err != nil {
			return err
		}
		if err = commitLogicalSchemaTransaction(tx, logical); err != nil {
			return err
		}
		if err = kernel.VerifyCurrentStateInTransaction(ctx, tx); err != nil {
			return err
		}
		result.MigrationRequired = len(tx.writes) > 0 || physicalMigration
		return ctx.Err()
	})
	return result, runtimeHostError(err)
}

// inspectionOverlay is transaction-local, never a second storage owner.
type inspectionOverlay struct {
	spi.ReadTx
	writes map[string]map[string]*[]byte
	merged map[string][]spi.Record
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
	delete(tx.merged, namespace)
	if tx.writes[namespace] == nil {
		tx.writes[namespace] = make(map[string]*[]byte)
	}
	copy := bytes.Clone(value)
	tx.writes[namespace][string(key)] = &copy
	return nil
}

func (tx *inspectionOverlay) Delete(namespace string, key []byte) error {
	delete(tx.merged, namespace)
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
	if tx.merged == nil {
		tx.merged = make(map[string][]spi.Record)
	}
	merged, found := tx.merged[request.Namespace]
	if !found {
		rows := make(map[string][]byte)
		readRequest := spi.ScanRequest{Namespace: request.Namespace, Limit: 256}
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
			if value == nil {
				delete(rows, key)
			} else {
				rows[key] = *value
			}
		}
		for key, value := range rows {
			merged = append(merged, spi.Record{Key: []byte(key), Value: value})
		}
		sort.Slice(merged, func(i, j int) bool { return bytes.Compare(merged[i].Key, merged[j].Key) < 0 })
		tx.merged[request.Namespace] = merged
	}
	first := sort.Search(len(merged), func(i int) bool {
		return bytes.Compare(merged[i].Key, request.Start) >= 0 && (len(request.After) == 0 || bytes.Compare(merged[i].Key, request.After) > 0)
	})
	page := spi.ScanPage{}
	for _, row := range merged[first:] {
		if len(request.End) > 0 && bytes.Compare(row.Key, request.End) >= 0 {
			break
		}
		if len(page.Records) == request.Limit {
			page.HasMore = true
			page.Next = bytes.Clone(page.Records[len(page.Records)-1].Key)
			break
		}
		page.Records = append(page.Records, spi.Record{Key: bytes.Clone(row.Key), Value: bytes.Clone(row.Value)})
	}
	return page, nil
}

//go:build floret_layout_cutover

package cutover

import (
	"context"
	"errors"
	"os"
	"syscall"

	"github.com/floegence/floret/v7/internal/sqliterecords"
	flstorage "github.com/floegence/floret/v7/internal/storage"
	"github.com/floegence/floret/v7/runtime"
	"github.com/floegence/floret/v7/storage/spi"
)

type CleanupInventory struct {
	Live, LiveScopes, Deleted, DeletedScopes int
}

func scopeInventory(tasks []ThreadImport) CleanupInventory {
	var counts CleanupInventory
	for _, task := range tasks {
		if task.Tombstone == nil {
			counts.Live++
			counts.LiveScopes += len(task.Scopes)
		} else {
			counts.Deleted++
			counts.DeletedScopes += len(task.Scopes)
		}
	}
	return counts
}

func CleanupDeleted(ctx context.Context, path string, expected CleanupInventory, expectedRecords uint64) (uint64, error) {
	original, err := os.Lstat(path)
	if err != nil {
		return 0, err
	}
	stat, ok := original.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 1 || !original.Mode().IsRegular() {
		return 0, spi.ErrConflict
	}
	for _, suffix := range []string{"-shm", "-wal", "-journal"} {
		if _, err := os.Lstat(path + suffix); !errors.Is(err, os.ErrNotExist) {
			return 0, errors.Join(spi.ErrConflict, err)
		}
	}
	var removed uint64
	err = withExclusiveSource(ctx, path, true, func(source *Source) error {
		var secure int
		if err := source.transaction.QueryRowContext(ctx, `PRAGMA secure_delete=ON`).Scan(&secure); err != nil || secure != 1 {
			return errors.Join(spi.ErrConflict, err)
		}
		records := sqliterecords.New(ctx, source.transaction, false)
		defer sqliterecords.End(records)
		version, err := runtime.OfflineSourceDomainVersion(records)
		if err != nil {
			return err
		}
		tasks, err := source.importOrder(ctx, version)
		if err != nil || scopeInventory(tasks) != expected {
			return errors.Join(spi.ErrConflict, err)
		}
		for _, task := range tasks {
			if task.Tombstone == nil {
				continue
			}
			if len(task.Scopes) > 1 || (len(task.Scopes) == 1 && task.Scopes[0] != task.Thread.String()) {
				return spi.ErrConflict
			}
			count, err := flstorage.RemoveDeletedPromptScope(ctx, records, task.Thread.String())
			if err != nil || (count != 0) != (len(task.Scopes) == 1) {
				return errors.Join(spi.ErrConflict, err)
			}
			removed += count
		}
		expected.DeletedScopes = 0
		if removed != expectedRecords {
			return spi.ErrConflict
		}
		tasks, err = source.importOrder(ctx, version)
		if err != nil || scopeInventory(tasks) != expected {
			return errors.Join(spi.ErrConflict, err)
		}
		current, err := os.Lstat(path)
		if err != nil || !os.SameFile(original, current) || current.Sys().(*syscall.Stat_t).Nlink != 1 {
			return errors.Join(spi.ErrConflict, err)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return removed, nil
}

//go:build floret_layout_cutover

package cutover

import (
	"context"
	"database/sql"
	"errors"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/internal/sqliterecords"
	flstorage "github.com/floegence/floret/v7/internal/storage"
	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/internal/threadsqlite"
	"github.com/floegence/floret/v7/runtime"
	"github.com/floegence/floret/v7/storage/spi"
)

func (source *Source) ThreadInventory(ctx context.Context) (sessiontree.OfflineThreadInventory, error) {
	reader := sqliterecords.New(ctx, source.transaction, true)
	defer sqliterecords.End(reader)
	return sessiontree.CheckOfflineThreadInventory(ctx, reader)
}

type ThreadImport struct {
	Thread    identity.ThreadID
	Scopes    []string
	Tombstone []byte
}

type ImportFailure struct {
	Step  string
	Cause error
}

func (failure *ImportFailure) Unwrap() error { return failure.Cause }
func (failure *ImportFailure) Error() string { return "offline import: " + failure.Step }

func (source *Source) ImportOrder(ctx context.Context) ([]ThreadImport, error) {
	return source.importOrder(ctx, 43)
}

func (source *Source) importOrder(ctx context.Context, domainVersion int) ([]ThreadImport, error) {
	reader := sqliterecords.New(ctx, source.transaction, true)
	defer sqliterecords.End(reader)
	var ordered []ThreadImport
	positions := make(map[string]int)
	_, err := sessiontree.WalkOfflineSourceThreads(ctx, reader, domainVersion, func(thread string) error {
		positions[thread] = len(ordered)
		ordered = append(ordered, ThreadImport{Thread: identity.ThreadID(thread)})
		return nil
	})
	if err != nil {
		return nil, err
	}
	err = sessiontree.WalkOfflineTombstones(ctx, reader, func(thread string, raw []byte) error {
		if _, found := positions[thread]; found {
			return sessiontree.ErrAuthorityCorrupt
		}
		positions[thread] = len(ordered)
		ordered = append(ordered, ThreadImport{Thread: identity.ThreadID(thread), Tombstone: raw})
		return nil
	})
	if err != nil {
		return nil, err
	}
	err = flstorage.WalkOfflinePromptOwners(ctx, reader, func(scope, thread string) error {
		pos, found := positions[thread]
		if !found {
			return spi.ErrConflict
		}
		ordered[pos].Scopes = append(ordered[pos].Scopes, scope)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ordered, nil
}

func (source *Source) ImportDeleted(ctx context.Context, target *threadsqlite.Backend, task ThreadImport) error {
	reader := sqliterecords.New(ctx, source.transaction, true)
	defer sqliterecords.End(reader)
	for _, scope := range task.Scopes {
		if err := flstorage.VerifyDeletedPromptScope(ctx, reader, scope, task.Thread.String()); err != nil {
			return err
		}
	}
	err := target.ImportDeletedThread(ctx, task.Thread, func(tx spi.WriteTx) error {
		return sessiontree.PutOfflineTombstone(tx, task.Thread.String(), task.Tombstone)
	})
	if err != nil {
		return err
	}
	if _, err := target.Catalog.Resolve(ctx, task.Thread); !errors.Is(err, storagebridge.ErrRouteDeleted) {
		return errors.Join(sessiontree.ErrAuthorityCorrupt, err)
	}
	return nil
}

func (source *Source) StartImport(ctx context.Context, target *threadsqlite.Backend) error {
	reader := sqliterecords.New(ctx, source.transaction, true)
	defer sqliterecords.End(reader)
	if err := runtime.CheckOfflineLayoutLogicalSchema(reader); err != nil {
		return err
	}
	return target.Update(ctx, func(tx spi.WriteTx) error { return sessiontree.InitializeOfflineSequence(reader, tx) })
}

func (source *Source) ImportThread(ctx context.Context, target *threadsqlite.Backend, thread identity.ThreadID, promptScopes []string) error {
	reader := sqliterecords.New(ctx, source.transaction, true)
	defer sqliterecords.End(reader)
	return target.CreateThreadRecords(ctx, storagebridge.ThreadCreation{Thread: thread}, func(tx spi.WriteTx) error {
		id := thread.String()
		if err := sessiontree.StageOfflineJournal(ctx, reader, tx, id); err != nil {
			return &ImportFailure{"stage journal", err}
		}
		if err := sessiontree.RegisterOfflineThread(ctx, reader, tx, id); err != nil {
			return &ImportFailure{"register thread", err}
		}
		for _, scope := range promptScopes {
			proof, err := flstorage.OfflinePromptScope(ctx, reader, tx, scope, id)
			if err != nil {
				return &ImportFailure{"copy prompt scope", err}
			}
			actual, err := flstorage.OfflinePromptScope(ctx, tx, nil, scope, id)
			if err != nil || actual != proof {
				return &ImportFailure{"verify prompt scope", errors.Join(sessiontree.ErrAuthorityCorrupt, err)}
			}
			if err := threadsqlite.BindPromptScope(tx, scope, thread); err != nil {
				return &ImportFailure{"bind prompt scope", err}
			}
		}
		before, err := sessiontree.VerifyOfflineJournal(ctx, reader, id)
		if err != nil {
			return &ImportFailure{"verify source journal", err}
		}
		after, err := sessiontree.VerifyOfflineJournal(ctx, tx, id)
		if err != nil || before != after {
			return &ImportFailure{"verify destination journal", errors.Join(sessiontree.ErrAuthorityCorrupt, err)}
		}
		return nil
	})
}

func (source *Source) StagePromptScope(ctx context.Context, files *threadsqlite.Files, ref threadsqlite.Reference, scope string) (flstorage.OfflineScopeProof, error) {
	reader := sqliterecords.New(ctx, source.transaction, true)
	defer sqliterecords.End(reader)
	var proof flstorage.OfflineScopeProof
	file := threadsqlite.LocalRecords{Files: files, Reference: ref}
	err := file.Update(ctx, func(target spi.WriteTx) error {
		var err error
		proof, err = flstorage.OfflinePromptScope(ctx, reader, target, scope, ref.Thread.String())
		return err
	})
	if err == nil {
		err = file.View(ctx, func(target spi.ReadTx) error {
			actual, err := flstorage.OfflinePromptScope(ctx, target, nil, scope, ref.Thread.String())
			if err == nil && actual != proof {
				return sessiontree.ErrAuthorityCorrupt
			}
			return err
		})
	}
	return proof, err
}

func (source *Source) VerifyJournal(ctx context.Context, files *threadsqlite.Files, ref threadsqlite.Reference) (sessiontree.OfflineJournalProof, error) {
	reader := sqliterecords.New(ctx, source.transaction, true)
	defer sqliterecords.End(reader)
	proof, err := sessiontree.VerifyOfflineJournal(ctx, reader, string(ref.Thread))
	if err != nil {
		return sessiontree.OfflineJournalProof{}, err
	}
	err = (threadsqlite.LocalRecords{Files: files, Reference: ref}).View(ctx, func(dest spi.ReadTx) error {
		actual, err := sessiontree.VerifyOfflineJournal(ctx, dest, string(ref.Thread))
		if err == nil && actual != proof {
			return sessiontree.ErrAuthorityCorrupt
		}
		return err
	})
	if err != nil {
		return sessiontree.OfflineJournalProof{}, err
	}
	return proof, nil
}

func (source *Source) VerifyItemWindows(ctx context.Context, files *threadsqlite.Files, ref threadsqlite.Reference) (int, error) {
	reader := sqliterecords.New(ctx, source.transaction, true)
	defer sqliterecords.End(reader)
	var pages int
	err := (threadsqlite.LocalRecords{Files: files, Reference: ref}).View(ctx, func(dest spi.ReadTx) error {
		var err error
		pages, err = runtime.VerifyOfflineItemWindows(ctx, reader, dest, string(ref.Thread))
		return err
	})
	return pages, err
}

func (source *Source) StageJournal(ctx context.Context, files *threadsqlite.Files, target threadsqlite.Reference) error {
	if ctx == nil || files == nil || source == nil || source.transaction == nil {
		return spi.ErrInvalidArgument
	}
	reader := sqliterecords.New(ctx, source.transaction, true)
	defer sqliterecords.End(reader)
	if err := runtime.CheckOfflineLayoutLogicalSchema(reader); err != nil {
		return err
	}
	return files.Update(ctx, target, func(tx *sql.Tx) error {
		var occupied bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM floret_backend_records) OR EXISTS(SELECT 1 FROM floret_thread_preparation)`).Scan(&occupied); err != nil {
			return err
		}
		if occupied {
			return spi.ErrConflict
		}
		writer := sqliterecords.New(ctx, tx, false)
		defer sqliterecords.End(writer)
		return sessiontree.StageOfflineJournal(ctx, reader, writer, string(target.Thread))
	})
}

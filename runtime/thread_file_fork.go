package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/json"

	"github.com/floegence/floret/v7/internal/sessiontree"
	internalstorage "github.com/floegence/floret/v7/internal/storage"
	"github.com/floegence/floret/v7/internal/threadsqlite"
	"github.com/floegence/floret/v7/storage/spi"
)

type reservedFileFork struct {
	Source  threadsqlite.Reference
	Target  threadsqlite.Reference
	Options sessiontree.ForkOptions
}

func (request reservedFileFork) fingerprint() ([32]byte, error) {
	opts := request.Options
	if opts.SourceThreadID != string(request.Source.Thread) || opts.NewThreadID != string(request.Target.Thread) || opts.OriginRequestKey == "" || opts.OriginFingerprint == "" || opts.Now.IsZero() || !opts.EntryIDPinned {
		return [32]byte{}, spi.ErrInvalidArgument
	}
	request.Options.Now = opts.Now.UTC()
	encoded, err := json.Marshal(request)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(encoded), nil
}

func (request reservedFileFork) prepare(ctx context.Context, files *threadsqlite.Files) error {
	fingerprint, err := request.fingerprint()
	if err != nil {
		return err
	}
	return files.PrepareFork(ctx, request.Source, request.Target, fingerprint, func(reader spi.ReadTx, writer spi.WriteTx) error {
		state, err := inspectLogicalSchemaTransaction(reader)
		if err != nil {
			return err
		}
		if state != logicalSchemaCurrent {
			return spi.ErrMigrationRequired
		}
		if err := internalstorage.StageIndependentFork(ctx, reader, writer, request.Options); err != nil {
			return err
		}
		return commitLogicalSchemaTransaction(writer, logicalSchemaMissing)
	})
}

func (request reservedFileFork) publish(ctx context.Context, catalog *threadsqlite.Catalog, files *threadsqlite.Files, operation string) error {
	fingerprint, err := request.fingerprint()
	if err != nil {
		return err
	}
	return catalog.PublishFork(ctx, files, operation, request.Source, request.Target, fingerprint)
}

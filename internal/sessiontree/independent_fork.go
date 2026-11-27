package sessiontree

import (
	"context"
	"errors"

	"github.com/floegence/floret/v7/storage/spi"
)

// StageIndependentFork only writes an invisible, freshly initialized destination.
// Publication, request reservation and parent ownership belong to the caller.
func StageIndependentFork(ctx context.Context, source spi.ReadTx, target spi.WriteTx, opts ForkOptions) (ThreadMeta, error) {
	if ctx == nil || source == nil || target == nil || opts.SourceThreadID == "" || opts.NewThreadID == "" || opts.NewThreadID == opts.SourceThreadID || !opts.EntryIDPinned || opts.DestinationMeta != nil && opts.DestinationMeta.ParentThreadID != "" {
		return ThreadMeta{}, spi.ErrInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		return ThreadMeta{}, err
	}
	manifest, err := readV32Manifest(source)
	if err != nil {
		return ThreadMeta{}, err
	}
	if manifest.Version != backendDomainV43Version || manifest.SourceVersion != 0 {
		return ThreadMeta{}, spi.ErrMigrationRequired
	}
	if _, err := readPublishedThreadMeta(target, backendDomainV32Format, opts.NewThreadID); !errors.Is(err, ErrThreadNotFound) {
		if err == nil {
			err = ErrThreadExists
		}
		return ThreadMeta{}, err
	}
	if opts.ExpectedSourceLeafID != "" {
		meta, err := readPublishedThreadMeta(source, backendDomainV32Format, opts.SourceThreadID)
		if err != nil {
			return ThreadMeta{}, err
		}
		if meta.LeafID != opts.ExpectedSourceLeafID {
			return ThreadMeta{}, ErrStaleAuthority
		}
	}
	mutation := canonicalJournalMutation{ctx: ctx, repo: &BackendRepo{}, tx: target}
	err = mutation.forkThread(opts, source, func(origin OutputSource, pin OutputSnapshot) error {
		return retainDetachedOutput(ctx, source, target, origin, pin)
	})
	if err != nil {
		return ThreadMeta{}, err
	}
	return mutation.meta, nil
}

func retainDetachedOutput(ctx context.Context, source spi.ReadTx, target spi.WriteTx, origin OutputSource, pin OutputSnapshot) error {
	if !validOutputSnapshot(pin) || pin.Bytes > origin.Bytes || pin.Chunks > origin.Chunks {
		return ErrAuthorityCorrupt
	}
	_, err := target.Get(outputSourceNamespace, outputSourceKey(origin.ID))
	if err == nil {
		stored, err := readOutputSource(target, origin.ID)
		if err != nil {
			return err
		}
		if snapshotOutput(stored) != pin {
			return ErrAuthorityCorrupt
		}
		return retainOutputSource(target, origin.ID)
	}
	if !errors.Is(err, spi.ErrNotFound) {
		return err
	}
	local, err := newOutputSource(target, origin.ID)
	if err != nil {
		return err
	}
	for seq := int64(1); seq <= pin.Chunks; seq++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		chunk, err := readOutputChunk(source, origin, seq)
		if err != nil {
			return err
		}
		var indexed int64
		if err := readOutputRecord(source, outputChunkOffsetKey(origin.ID, seq), &indexed); err != nil {
			return err
		}
		if local.Bytes != chunk.Offset || indexed != chunk.Offset || int64(len(chunk.Text)) > pin.Bytes-local.Bytes {
			return ErrAuthorityCorrupt
		}
		local, err = appendOutputSource(ctx, target, local, chunk.Text)
		if err != nil {
			return err
		}
		if local.Reason != "" {
			return ErrAuthorityCorrupt
		}
	}
	if local.Bytes != pin.Bytes || local.Chunks != pin.Chunks || local.SHA256 != pin.SHA256 {
		return ErrAuthorityCorrupt
	}
	local.ObservedBytes = pin.ObservedBytes
	local.Sealed = true
	local.Reason = pin.Reason
	return putOutputRecord(target, outputSourceKey(origin.ID), local)
}

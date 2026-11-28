//go:build floret_layout_cutover

package sessiontree

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"

	"github.com/floegence/floret/v7/internal/session/artifact"
	"github.com/floegence/floret/v7/internal/threadsqlite/member"
	"github.com/floegence/floret/v7/storage/spi"
)

func VerifyMemberOutputs(ctx context.Context, source spi.ReadTx, target *member.Journal) (MemberOutputInventory, error) {
	var result MemberOutputInventory
	needs := offlineOutputSet{}
	_, err := visitStoredJournal(ctx, source, target.Thread(), func(_ spi.Record, stored canonicalStoredEntry) error {
		entry := stored.Entry
		if entry.Type == EntryHostWork {
			work, err := decodeHostWork(entry)
			if err != nil {
				return err
			}
			if work.OutputStream != "" {
				old, err := hostWorkOutputSource(source, entry.ThreadID, work.ID)
				if err != nil {
					return err
				}
				output, fixed, err := target.WorkOutput(entry.ID, work.OutputStream)
				if err != nil || fixed != (work.OutputSnapshot != nil) || !matchesMemberOutput(old, output) {
					return errors.Join(ErrAuthorityCorrupt, err)
				}
				result.Bindings++
			}
		}
		return needs.collect(source, entry, func(original artifact.Record, _ []byte) error {
			ref := original.Ref
			installed, err := target.Artifact(ref.ID, ref.SizeBytes, ref.SHA256)
			if err != nil || installed.Entry != entry.ID || installed.Stream != original.OutputSourceID || installed.At != original.CreatedAt.UnixNano() {
				return errors.Join(ErrAuthorityCorrupt, err)
			}
			if installed.Stream == "" {
				if err := target.CopyBody("artifact", installed.Body, io.Discard); err != nil {
					return err
				}
			}
			result.Artifacts++
			return nil
		})
	})
	if err != nil {
		return result, err
	}
	for id, need := range needs {
		original, err := readOutputSource(source, id)
		if err != nil || original.References != need.references {
			return result, errors.Join(ErrAuthorityCorrupt, err)
		}
		copied, err := target.OutputAt(id, 0)
		if err != nil || !matchesMemberOutput(original, copied) || !bytes.Equal(original.HashState, copied.HashState) {
			return result, errors.Join(ErrAuthorityCorrupt, err)
		}
		digest := sha256.New()
		var offset int64
		for number := int64(0); number < copied.Blocks; number++ {
			chunk, err := readOutputChunk(source, original, number+1)
			if err != nil {
				return result, err
			}
			block, err := target.OutputBlock(copied, number)
			if err != nil || chunk.Offset != offset || block.Start != offset || string(block.Bytes) != chunk.Text {
				return result, errors.Join(ErrAuthorityCorrupt, err)
			}
			digest.Write(block.Bytes)
			offset += int64(len(block.Bytes))
		}
		if offset != original.Bytes || hex.EncodeToString(digest.Sum(nil)) != original.SHA256 || offset > outputStoreMaxBytes-result.RetainedBytes {
			return result, ErrAuthorityCorrupt
		}
		result.Streams++
		result.RetainedBytes += offset
	}
	usage, err := outputStoreUsage(source)
	if err != nil || result.RetainedBytes != usage {
		return result, errors.Join(ErrAuthorityCorrupt, err)
	}
	streams, artifacts, bindings, err := target.OutputCounts()
	if err != nil || result.Streams != streams || result.Artifacts != artifacts || result.Bindings != bindings {
		return result, errors.Join(ErrAuthorityCorrupt, err)
	}
	return result, nil
}

func matchesMemberOutput(source OutputSource, result member.Output) bool {
	return result.Retained == source.Bytes && result.Observed == source.ObservedBytes && result.Blocks == source.Chunks && result.Sealed == source.Sealed && result.Reason == source.Reason && result.Digest == source.SHA256
}

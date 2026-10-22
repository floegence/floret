package sessiontree

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding"
	"errors"
	"hash"
	"io"

	"github.com/floegence/floret/v7/internal/storagecodec"
	"github.com/floegence/floret/v7/storage/spi"
)

// Only unfinished indexed values retain hash state. Complete locators carry a
// digest of their exact original bytes, without retaining a second transcript.
type snapshotPartialDigest struct {
	Offset int64
	State  []byte
}

func snapshotOpenValueOffsets(scan storagecodec.JSONScanState) []int64 {
	var offsets []int64
	for _, frame := range scan.Frames {
		if frame.Path != nil && len(frame.Path) <= 4 {
			offsets = append(offsets, frame.Start)
		}
	}
	if scan.Token != 0 && !scan.TokenKey && scan.TokenPath != nil && len(scan.TokenPath) <= 4 {
		offsets = append(offsets, scan.TokenStart)
	}
	return offsets
}

func (checkpoint snapshotImportCheckpoint) validateDigests() error {
	if checkpoint.VerifyOffset < 0 || checkpoint.VerifyOffset > checkpoint.SourceBytes || checkpoint.Version == 0 && checkpoint.VerifyOffset != 0 || checkpoint.Complete && checkpoint.VerifyOffset != checkpoint.SourceBytes {
		return ErrAuthorityCorrupt
	}
	if checkpoint.VerifyOffset == 0 && len(checkpoint.VerifyDigest) != 0 {
		return ErrAuthorityCorrupt
	}
	if checkpoint.VerifyOffset > 0 {
		if _, err := restoreSnapshotDigest(checkpoint.VerifyDigest); err != nil {
			return err
		}
	}
	if checkpoint.Scan.Offset == 0 && len(checkpoint.ScanDigest) != 0 {
		return ErrAuthorityCorrupt
	}
	if checkpoint.Scan.Offset > 0 {
		if _, err := restoreSnapshotDigest(checkpoint.ScanDigest); err != nil {
			return err
		}
	}
	if checkpoint.Complete {
		scanned, err := restoreSnapshotDigest(checkpoint.ScanDigest)
		if err != nil {
			return err
		}
		verified, err := restoreSnapshotDigest(checkpoint.VerifyDigest)
		if err != nil || !bytes.Equal(scanned.Sum(nil), verified.Sum(nil)) {
			return errors.Join(ErrAuthorityCorrupt, err)
		}
	}
	offsets := snapshotOpenValueOffsets(checkpoint.Scan)
	if len(offsets) != len(checkpoint.Digests) {
		return ErrAuthorityCorrupt
	}
	for i, offset := range offsets {
		partial := checkpoint.Digests[i]
		if partial.Offset != offset || offset < 0 || offset >= checkpoint.Scan.Offset {
			return ErrAuthorityCorrupt
		}
		if _, err := restoreSnapshotDigest(partial.State); err != nil {
			return err
		}
	}
	return nil
}

func restoreSnapshotDigest(state []byte) (hash.Hash, error) {
	digest := sha256.New()
	if err := digest.(encoding.BinaryUnmarshaler).UnmarshalBinary(state); err != nil {
		return nil, errors.Join(ErrAuthorityCorrupt, err)
	}
	return digest, nil
}

// A batch is read once in a transaction. Each overlapping indexed value hashes
// just its part of that batch; large open objects and strings resume in place.
func snapshotSpanDigest(checkpoint snapshotImportCheckpoint, batch []byte, span storagecodec.JSONSpan) (hash.Hash, error) {
	start, end := span.Offset, span.Offset+span.Length
	batchStart, batchEnd := checkpoint.Scan.Offset, checkpoint.Scan.Offset+int64(len(batch))
	if span.Offset < 0 || span.Length <= 0 || end < start || end > batchEnd || end < batchStart {
		return nil, ErrAuthorityCorrupt
	}
	digest := sha256.New()
	if start < batchStart {
		found := false
		for _, partial := range checkpoint.Digests {
			if partial.Offset != start {
				continue
			}
			var err error
			digest, err = restoreSnapshotDigest(partial.State)
			if err != nil {
				return nil, err
			}
			found = true
			break
		}
		if !found {
			return nil, ErrAuthorityCorrupt
		}
		start = batchStart
	}
	_, _ = digest.Write(batch[start-batchStart : end-batchStart])
	return digest, nil
}

func snapshotNextDigests(previous snapshotImportCheckpoint, batch []byte, next storagecodec.JSONScanState) ([]snapshotPartialDigest, error) {
	var partials []snapshotPartialDigest
	for _, offset := range snapshotOpenValueOffsets(next) {
		digest, err := snapshotSpanDigest(previous, batch, storagecodec.JSONSpan{Offset: offset, Length: next.Offset - offset})
		if err != nil {
			return nil, err
		}
		state, err := digest.(encoding.BinaryMarshaler).MarshalBinary()
		if err != nil {
			return nil, err
		}
		partials = append(partials, snapshotPartialDigest{Offset: offset, State: state})
	}
	return partials, nil
}

func verifySnapshotSpanDigest(span snapshotSourceSpan, digest hash.Hash) error {
	if len(span.Digest) != sha256.Size || !bytes.Equal(span.Digest, digest.Sum(nil)) {
		return ErrAuthorityCorrupt
	}
	return nil
}

// Re-read the full indexed source in bounded batches before making any locator
// eligible for publication. This catches mixed snapshots across scan restarts.
func verifyJSONSourceBatch(ctx context.Context, tx spi.ReadTx, namespace string, key []byte, checkpoint *snapshotImportCheckpoint) error {
	digest := sha256.New()
	if checkpoint.VerifyOffset > 0 {
		var err error
		digest, err = restoreSnapshotDigest(checkpoint.VerifyDigest)
		if err != nil {
			return err
		}
	}
	count := min(int64(backendThreadMigrationBatchBytes), checkpoint.SourceBytes-checkpoint.VerifyOffset)
	reader, err := storagecodec.OpenRecordRange(ctx, tx, namespace, key, checkpoint.VerifyOffset, count)
	if err != nil {
		return err
	}
	copied, err := io.Copy(digest, reader)
	if err != nil {
		return err
	}
	if copied != count {
		return ErrAuthorityCorrupt
	}
	checkpoint.VerifyOffset += copied
	checkpoint.VerifyDigest, err = digest.(encoding.BinaryMarshaler).MarshalBinary()
	if err != nil {
		return err
	}
	if checkpoint.VerifyOffset == checkpoint.SourceBytes {
		scanned, err := restoreSnapshotDigest(checkpoint.ScanDigest)
		if err != nil {
			return err
		}
		if !bytes.Equal(scanned.Sum(nil), digest.Sum(nil)) {
			return ErrAuthorityCorrupt
		}
		checkpoint.Complete = true
	}
	return nil
}

func verifyIndexedJSONRecord(ctx context.Context, tx spi.ReadTx, namespace string, key []byte, checkpoint snapshotImportCheckpoint) error {
	size, err := historicalRecordSize(tx, namespace, key)
	if err != nil || size != checkpoint.SourceBytes {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	reader, err := storagecodec.OpenRecord(ctx, tx, namespace, key)
	if err != nil {
		return err
	}
	digest := sha256.New()
	if _, err := io.Copy(digest, reader); err != nil {
		return err
	}
	scanned, err := restoreSnapshotDigest(checkpoint.ScanDigest)
	if err != nil || !bytes.Equal(digest.Sum(nil), scanned.Sum(nil)) {
		return errors.Join(ErrAuthorityCorrupt, err)
	}
	return nil
}

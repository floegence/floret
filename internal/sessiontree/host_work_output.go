package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/floegence/floret/v7/storage/spi"
)

// OutputSnapshot pins an inherited capture without copying its immutable chunks.
// The source keeps its own references; later appends cannot expand the snapshot.
type OutputSnapshot struct {
	Bytes         int64  `json:"bytes"`
	ObservedBytes int64  `json:"observed_bytes"`
	Chunks        int64  `json:"chunks"`
	SHA256        string `json:"sha256"`
	Complete      bool   `json:"complete"`
	Reason        string `json:"reason,omitempty"`
}

func validOutputSnapshot(s OutputSnapshot) bool {
	return s.Bytes >= 0 && s.Bytes <= OutputSourceMaxBytes && s.ObservedBytes >= s.Bytes && s.Chunks >= 0 && s.Chunks <= s.Bytes && (s.Bytes == 0) == (s.Chunks == 0) && len(s.SHA256) == 64 && (!s.Complete || s.Reason == "")
}
func snapshotOutput(s OutputSource) OutputSnapshot {
	reason := s.Reason
	if !s.Sealed && reason == "" {
		reason = "interrupted"
	}
	return OutputSnapshot{Bytes: s.Bytes, ObservedBytes: s.ObservedBytes, Chunks: s.Chunks, SHA256: s.SHA256, Complete: s.Sealed && s.Reason == "", Reason: reason}
}
func HostWorkOutputSourceID(thread, work string) string {
	return outputSourceID(thread, hostWorkEntryID(work, false))
}

type HostWorkOutputAppend struct {
	ThreadID, WorkID, ExecutorID, Text string
	Offset                             int64
}
type HostWorkOutputRequest struct {
	ThreadID, WorkID string
	Offset           int64
	MaxBytes         int
}
type HostWorkOutputChunks struct {
	Source  OutputSource
	Chunks  []OutputSourceChunk
	HasMore bool
}
type HostWorkOutputRepo interface {
	AppendHostWorkOutput(context.Context, HostWorkOutputAppend) (OutputSource, error)
	ReadHostWorkOutputPage(context.Context, HostWorkOutputRequest) (OutputSourcePage, error)
	ReadHostWorkOutputChunks(context.Context, HostWorkOutputRequest, int64) (HostWorkOutputChunks, error)
}

// Membership is checked against the canonical binding on every operation, not
// against a host cache, output identifier or storage path.
func boundHostWork(tx spi.ReadTx, thread, work string) (HostWork, error) {
	entry, err := readCanonicalEntryMetadata(tx, thread, hostWorkEntryID(work, false))
	if err != nil {
		return HostWork{}, err
	}
	w, err := decodeHostWork(entry)
	if err != nil || w.ID != work || w.State != "bound" {
		return HostWork{}, errors.Join(ErrAuthorityCorrupt, err)
	}
	return w, nil
}
func (r *BackendRepo) AppendHostWorkOutput(ctx context.Context, in HostWorkOutputAppend) (OutputSource, error) {
	var result OutputSource
	err := r.canonicalThreadUpdate(ctx, in.ThreadID, func(tx spi.WriteTx, meta ThreadMeta) error {
		if err := lifecycleRejectsWrite(meta); err != nil {
			return err
		}
		work, err := boundHostWork(tx, meta.ID, in.WorkID)
		if err != nil {
			return err
		}
		if work.Inherited || work.OutputStream == "" || work.ExecutorID != in.ExecutorID {
			return ErrInvalidThreadAuthority
		}
		source, err := readOutputSource(tx, work.OutputStream)
		if err != nil {
			return err
		}
		if in.Offset != source.ObservedBytes {
			return ErrRequestConflict
		}
		result, err = appendOutputSource(ctx, tx, source, in.Text)
		return err
	}, func(*MemoryRepo) error { return spi.ErrInvalidArgument })
	return result, err
}
func hostWorkOutputSource(tx spi.ReadTx, thread, work string) (OutputSource, error) {
	binding, err := boundHostWork(tx, thread, work)
	if err != nil {
		return OutputSource{}, err
	}
	if binding.OutputStream == "" {
		return OutputSource{}, ErrArtifactNotFound
	}
	source, err := readOutputSource(tx, binding.OutputStream)
	if err != nil {
		return source, err
	}
	if pin := binding.OutputSnapshot; pin != nil {
		if !validOutputSnapshot(*pin) || pin.Bytes > source.Bytes || pin.Chunks > source.Chunks {
			return source, ErrAuthorityCorrupt
		}
		source.Bytes, source.ObservedBytes, source.Chunks, source.SHA256 = pin.Bytes, pin.ObservedBytes, pin.Chunks, pin.SHA256
		source.Sealed, source.Reason = true, pin.Reason
	}
	return source, nil
}
func (r *BackendRepo) ReadHostWorkOutputPage(ctx context.Context, req HostWorkOutputRequest) (OutputSourcePage, error) {
	var page OutputSourcePage
	err := r.canonicalThreadView(ctx, req.ThreadID, func(tx spi.ReadTx, meta ThreadMeta) error {
		source, err := hostWorkOutputSource(tx, meta.ID, req.WorkID)
		if err != nil {
			return err
		}
		page, err = readOutputSourcePage(ctx, tx, source, req.Offset, req.MaxBytes)
		return err
	}, func(*MemoryRepo) error { return spi.ErrInvalidArgument })
	return page, err
}
func (r *BackendRepo) ReadHostWorkOutputChunks(ctx context.Context, req HostWorkOutputRequest, after int64) (HostWorkOutputChunks, error) {
	var page HostWorkOutputChunks
	if after < 0 || req.MaxBytes < 4 || req.MaxBytes > 256<<10 {
		return page, spi.ErrInvalidArgument
	}
	err := r.canonicalThreadView(ctx, req.ThreadID, func(tx spi.ReadTx, meta ThreadMeta) error {
		source, err := hostWorkOutputSource(tx, meta.ID, req.WorkID)
		if err != nil {
			return err
		}
		page.Source = source
		if after > source.Chunks {
			return spi.ErrInvalidArgument
		}
		size := 0
		for seq := after + 1; seq <= source.Chunks; seq++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			chunk, err := readOutputChunk(tx, source, seq)
			if err != nil {
				return err
			}
			if size+len(chunk.Text) > req.MaxBytes {
				if size == 0 {
					return spi.ErrInvalidArgument
				}
				page.HasMore = true
				break
			}
			page.Chunks = append(page.Chunks, chunk)
			size += len(chunk.Text)
		}
		return nil
	}, func(*MemoryRepo) error { return spi.ErrInvalidArgument })
	return page, err
}
func validOutputReason(reason string) bool {
	switch reason {
	case "", "output_limit", "storage_limit", "storage_error", "capture_error", "cancelled", "interrupted":
		return true
	}
	return false
}

// pinForkOutputStreams installs a per-fork immutable watermark in both binding
// and final facts. Retaining once per binding protects the common source.
func pinForkOutputStreams(ctx context.Context, tx spi.WriteTx, entries []Entry) error {
	snapshots := map[string]*OutputSnapshot{}
	for i := range entries {
		if entries[i].Type != EntryHostWork {
			continue
		}
		work, err := decodeHostWork(entries[i])
		if err != nil {
			return err
		}
		if work.OutputStream == "" {
			continue
		}
		pin := snapshots[work.ID]
		if pin == nil {
			if work.State != "bound" {
				return ErrAuthorityCorrupt
			}
			source, err := readOutputSource(tx, work.OutputStream)
			if err != nil {
				return err
			}
			if work.OutputSnapshot != nil {
				copy := *work.OutputSnapshot
				pin = &copy
			} else {
				copy := snapshotOutput(source)
				pin = &copy
			}
			pinned := source
			pinned.Bytes, pinned.Chunks, pinned.SHA256 = pin.Bytes, pin.Chunks, pin.SHA256
			if err := verifyOutputSource(ctx, tx, pinned); err != nil {
				return err
			}
			snapshots[work.ID] = pin
			if err := retainOutputSource(tx, source.ID); err != nil {
				return err
			}
		}
		work.OutputSnapshot = pin
		entries[i].Payload, _ = json.Marshal(work)
		entries[i].Raw = rawForEntry(entries[i])
		entries[i].RawHash = StableHash(entries[i].Raw)
	}
	return nil
}
func validateStreamIdentity(id string) bool {
	return strings.HasPrefix(id, "output-") && len(id) == len("output-")+64
}

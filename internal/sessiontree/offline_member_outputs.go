//go:build floret_layout_cutover

package sessiontree

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"maps"
	"slices"
	"time"

	"github.com/floegence/floret/v7/internal/session/artifact"
	"github.com/floegence/floret/v7/internal/threadsqlite/member"
	"github.com/floegence/floret/v7/storage/spi"
)

type MemberOutputInventory struct {
	Streams, Artifacts, Bindings int
	RetainedBytes                int64
}

type memberWorkLink struct {
	entry, stream string
	pin           *OutputSnapshot
}
type memberArtifactLink struct {
	id, entry, stream string
	at, boundary      int64
}

func StageMemberOutputs(ctx context.Context, source spi.ReadTx, target *member.Journal, importedAt time.Time) (MemberOutputInventory, error) {
	var inventory MemberOutputInventory
	at := importedAt.UnixNano()
	if importedAt.IsZero() || !time.Unix(0, at).Equal(importedAt) {
		return inventory, spi.ErrInvalidArgument
	}
	needs := offlineOutputSet{}
	var workLinks []memberWorkLink
	var artifacts []memberArtifactLink
	_, err := visitStoredJournal(ctx, source, target.Thread(), func(_ spi.Record, stored canonicalStoredEntry) error {
		entry := stored.Entry
		if entry.Type == EntryHostWork {
			work, err := decodeHostWork(entry)
			if err != nil {
				return err
			}
			if work.OutputStream != "" {
				workLinks = append(workLinks, memberWorkLink{entry.ID, work.OutputStream, work.OutputSnapshot})
			}
		}
		return needs.collect(source, entry, func(record artifact.Record, _ []byte) error {
			at := record.CreatedAt.UnixNano()
			if !time.Unix(0, at).Equal(record.CreatedAt) {
				return ErrAuthorityCorrupt
			}
			link := memberArtifactLink{id: record.Ref.ID, entry: entry.ID, stream: record.OutputSourceID, at: at}
			if link.stream == "" {
				ref, err := target.PutBody("artifact", at, []byte(record.Text))
				if err != nil {
					return err
				}
				link.boundary = ref.Boundary
			}
			artifacts = append(artifacts, link)
			return nil
		})
	})
	if err != nil {
		return inventory, err
	}
	installed := map[string]memberOutputPins{}
	prefix := backendDomainKey("source")
	request := spi.ScanRequest{Namespace: outputSourceNamespace, Start: prefix, End: backendKeyRangeEnd(prefix), Limit: 128}
	for {
		page, err := source.Scan(request)
		if err != nil {
			return inventory, err
		}
		for _, row := range page.Records {
			var header OutputSource
			if err := readOutputRecord(source, row.Key, &header); err != nil || !bytes.Equal(row.Key, outputSourceKey(header.ID)) {
				return inventory, errors.Join(ErrAuthorityCorrupt, err)
			}
			need := needs[header.ID]
			if need == nil {
				return inventory, ErrAuthorityCorrupt
			}
			pins, err := importMemberOutput(ctx, source, target, header.ID, need, at)
			if err != nil {
				return inventory, err
			}
			installed[header.ID] = pins
			delete(needs, header.ID)
			if pins.Current.Retained > outputStoreMaxBytes-inventory.RetainedBytes {
				return inventory, ErrAuthorityCorrupt
			}
			inventory.RetainedBytes += pins.Current.Retained
			inventory.Streams++
		}
		if !page.HasMore {
			break
		}
		if len(page.Records) == 0 || !bytes.Equal(page.Next, page.Records[len(page.Records)-1].Key) || bytes.Compare(page.Next, request.After) <= 0 {
			return inventory, ErrAuthorityCorrupt
		}
		request.After = page.Next
	}
	usage, err := outputStoreUsage(source)
	if err != nil || usage != inventory.RetainedBytes || len(needs) != 0 {
		return inventory, errors.Join(ErrAuthorityCorrupt, err)
	}
	for _, link := range workLinks {
		pins := installed[link.stream]
		var pin int64
		if link.pin != nil {
			output, ok := pins.Frozen[*link.pin]
			if !ok {
				return inventory, ErrAuthorityCorrupt
			}
			pin = output.BoundarySeq
		}
		if err := target.LinkWorkOutput(link.entry, link.stream, pin); err != nil {
			return inventory, err
		}
		inventory.Bindings++
	}
	for _, link := range artifacts {
		if link.stream != "" {
			output, ok := installed[link.stream]
			if !ok || !output.Current.Sealed {
				return inventory, ErrAuthorityCorrupt
			}
			link.boundary = output.Current.BoundarySeq
		}
		if err := target.BindArtifact(link.id, link.entry, link.boundary, link.at); err != nil {
			return inventory, err
		}
		inventory.Artifacts++
	}
	return inventory, nil
}

type memberOutputPins struct {
	Current member.Output
	Frozen  map[OutputSnapshot]member.Output
}

func importMemberOutput(ctx context.Context, source spi.ReadTx, target *member.Journal, id string, need *offlineOutputNeed, at int64) (memberOutputPins, error) {
	var result memberOutputPins
	if need == nil || need.references <= 0 || !need.full && len(need.pins) == 0 {
		return result, ErrAuthorityCorrupt
	}
	origin, err := readOutputSource(source, id)
	if err != nil || origin.References != need.references {
		return result, errors.Join(ErrAuthorityCorrupt, err)
	}
	pins := slices.Collect(maps.Keys(need.pins))
	slices.SortFunc(pins, func(left, right OutputSnapshot) int {
		return cmp.Or(cmp.Compare(left.Chunks, right.Chunks), cmp.Compare(left.ObservedBytes, right.ObservedBytes), cmp.Compare(left.Reason, right.Reason))
	})
	boundaries := make([]member.OutputBoundary, 0, len(pins)+1)
	for _, pin := range pins {
		if !validOutputSnapshot(pin) || pin.Bytes > origin.Bytes || pin.Chunks > origin.Chunks || pin.ObservedBytes > origin.ObservedBytes {
			return result, ErrAuthorityCorrupt
		}
		boundaries = append(boundaries, member.OutputBoundary{Retained: pin.Bytes, Observed: pin.ObservedBytes, Blocks: pin.Chunks, Digest: pin.SHA256, Sealed: true, Reason: pin.Reason})
	}
	if need.full {
		boundaries = append(boundaries, member.OutputBoundary{Retained: origin.Bytes, Observed: origin.ObservedBytes, Blocks: origin.Chunks, Digest: origin.SHA256, Sealed: origin.Sealed, Reason: origin.Reason, HashState: origin.HashState})
	} else if longest := pins[len(pins)-1]; origin.Bytes != longest.Bytes || origin.ObservedBytes != longest.ObservedBytes || origin.Chunks != longest.Chunks || origin.SHA256 != longest.SHA256 || !origin.Sealed || origin.Reason != longest.Reason {
		return result, ErrAuthorityCorrupt
	}
	installed, err := target.InstallOutput(id, at, boundaries, func(number int64) (member.OutputBlock, error) {
		if err := ctx.Err(); err != nil {
			return member.OutputBlock{}, err
		}
		seq := number + 1
		chunk, err := readOutputChunk(source, origin, seq)
		if err != nil {
			return member.OutputBlock{}, err
		}
		var indexed int64
		if err := readOutputRecord(source, outputChunkOffsetKey(id, seq), &indexed); err != nil || indexed != chunk.Offset {
			return member.OutputBlock{}, errors.Join(ErrAuthorityCorrupt, err)
		}
		return member.OutputBlock{Number: number, Start: chunk.Offset, Bytes: []byte(chunk.Text)}, nil
	})
	if err != nil {
		return result, err
	}
	result.Current = installed[len(installed)-1]
	result.Frozen = make(map[OutputSnapshot]member.Output, len(pins))
	for index, pin := range pins {
		result.Frozen[pin] = installed[index]
	}
	return result, nil
}

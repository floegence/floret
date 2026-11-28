package sessiontree

import (
	"bytes"
	"errors"
	"maps"
	"slices"
	"time"

	"github.com/floegence/floret/v7/internal/session/artifact"
	"github.com/floegence/floret/v7/internal/threadsqlite/member"
	"github.com/floegence/floret/v7/storage/spi"
)

type memberOutputCut struct {
	stream   string
	boundary int64
}

func memberOutputSource(stream string, output member.Output) OutputSource {
	return OutputSource{ID: stream, Bytes: output.Retained, ObservedBytes: output.Observed, Chunks: output.Blocks, Sealed: output.Sealed, Reason: output.Reason, SHA256: output.Digest, HashState: output.HashState, References: 1}
}

func memberOutputPrefix(journal *member.Journal, source OutputSource) (member.Output, error) {
	output, err := journal.OutputAt(source.ID, 0)
	if err != nil || source.Bytes > output.Retained || source.Chunks > output.Blocks {
		return member.Output{}, errors.Join(ErrAuthorityCorrupt, err)
	}
	output.Blocks, output.Retained = source.Chunks, source.Bytes
	return output, nil
}

func readMemberArtifact(journal *member.Journal, id string) (artifact.Record, bool, error) {
	entryID, found, err := journal.ArtifactEntry(id)
	if err != nil || !found {
		return artifact.Record{}, false, err
	}
	event, err := journal.Entry(entryID)
	if err != nil {
		return artifact.Record{}, false, err
	}
	entry, err := materializeMemberEntry(journal, event)
	if err != nil || entry.Message.ToolResult == nil || entry.Message.ToolResult.FullOutput == nil || entry.Message.ToolResult.FullOutput.ID != id {
		return artifact.Record{}, false, errors.Join(ErrAuthorityCorrupt, err)
	}
	ref := *entry.Message.ToolResult.FullOutput
	source, err := journal.Artifact(id, ref.SizeBytes, ref.SHA256)
	if err != nil || source.Entry != entryID {
		return artifact.Record{}, false, errors.Join(ErrAuthorityCorrupt, err)
	}
	record := artifact.Record{ThreadID: journal.Thread(), Ref: ref, CanonicalEntryID: entryID, CreatedAt: time.Unix(0, source.At).UTC(), OutputSourceID: source.Stream}
	if source.Stream == "" {
		var text bytes.Buffer
		if err := journal.CopyBody("artifact", source.Body, &text); err != nil {
			return artifact.Record{}, false, err
		}
		record.Text = text.String()
	}
	return record, true, nil
}

func copyMemberForkOutputs(source *member.Journal, tx spi.WriteTx, entries []Entry, artifacts map[string]artifact.Record, sourceEntries map[string]string, now time.Time) (map[string]int64, error) {
	target := member.From(tx)
	owner, ok := tx.(scopedOutputWriter)
	if target == nil || !ok || owner.ThreadOwner().String() != target.Thread() {
		return nil, ErrInvalidThreadAuthority
	}
	pins := map[string][]int64{}
	workCuts := map[string]memberOutputCut{}
	err := freezeForkWork(entries, func(entry Entry, work HostWork) (OutputSnapshot, error) {
		output, pinned, err := source.WorkOutput(entry.ID, work.OutputStream)
		if err != nil {
			return OutputSnapshot{}, err
		}
		cut := snapshotOutput(memberOutputSource(work.OutputStream, output))
		if pinned != (work.OutputSnapshot != nil) || pinned && *work.OutputSnapshot != cut {
			return OutputSnapshot{}, ErrAuthorityCorrupt
		}
		workCuts[work.ID] = memberOutputCut{work.OutputStream, output.BoundarySeq}
		pins[work.OutputStream] = append(pins[work.OutputStream], output.BoundarySeq)
		return cut, nil
	})
	if err != nil {
		return nil, err
	}
	for _, record := range artifacts {
		if record.OutputSourceID == "" {
			continue
		}
		from, err := source.Artifact(record.Ref.ID, record.Ref.SizeBytes, record.Ref.SHA256)
		if err != nil || from.Stream != record.OutputSourceID || sourceEntries[from.Entry] != record.CanonicalEntryID {
			return nil, errors.Join(ErrAuthorityCorrupt, err)
		}
		pins[from.Stream] = append(pins[from.Stream], from.Output.BoundarySeq)
	}
	remap := map[memberOutputCut]int64{}
	for _, stream := range slices.Sorted(maps.Keys(pins)) {
		boundaries := pins[stream]
		copies, err := source.ForkOutput(target, stream, boundaries, now.UnixNano())
		if err != nil {
			return nil, err
		}
		var retained int64
		for index, copy := range copies {
			remap[memberOutputCut{stream, boundaries[index]}] = copy.BoundarySeq
			retained = max(retained, copy.Retained)
		}
		allowed, err := changeOutputCharge(owner.CatalogWriter(), outputCharge{target.Thread(), stream, retained}, 0)
		if err != nil || !allowed {
			return nil, errors.Join(ErrAuthorityCorrupt, err)
		}
	}
	workPins := map[string]int64{}
	for id, cut := range workCuts {
		workPins[id] = remap[cut]
	}
	return workPins, nil
}

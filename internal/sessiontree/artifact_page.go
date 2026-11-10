package sessiontree

import (
	"context"
	"errors"
	"reflect"
	"unicode/utf8"

	"github.com/floegence/floret/v7/internal/session/artifact"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/tools"
)

type ArtifactPageRequest struct {
	ArtifactReadRequest
	Offset   int64
	MaxBytes int
}
type ArtifactPage struct {
	Ref           artifact.Ref
	Offset        int64
	NextOffset    int64
	Text          string
	HasMore       bool
	Complete      bool
	Reason        string
	ObservedBytes int64
}
type ArtifactPageReader interface {
	ReadArtifactPage(context.Context, ArtifactPageRequest) (ArtifactPage, error)
}

func validateToolOutputReference(entry Entry) error {
	view := entry.Message.ToolResult
	if view == nil {
		return nil
	}
	if view.OutputUnavailable != "" {
		if view.FullOutput != nil || view.OutputRef != "" {
			return ErrAuthorityCorrupt
		}
		switch view.OutputUnavailable {
		case "output_limit", "storage_limit", "storage_error":
		default:
			return ErrAuthorityCorrupt
		}
	}
	if view.OutputRef != "" {
		ref, err := tools.ParseOutputReference(view.OutputRef)
		if err != nil || view.FullOutput == nil || ref.Kind != "artifact" || ref.ID != view.FullOutput.ID || ref.ThreadID.String() != entry.ThreadID {
			return ErrAuthorityCorrupt
		}
	}
	return nil
}

func (repo *BackendRepo) ReadArtifactPage(ctx context.Context, req ArtifactPageRequest) (ArtifactPage, error) {
	var result ArtifactPage
	if req.Offset < 0 || req.MaxBytes < utf8.UTFMax || req.MaxBytes > OutputSourceChunkBytes {
		return result, spi.ErrInvalidArgument
	}
	err := repo.canonicalArtifactView(ctx, req.ThreadID, req.ParentThreadID, func(tx spi.ReadTx) error {
		reader := canonicalArtifactReader(ctx, tx)
		if err := reader.validateArtifactReadAuthorityLocked(req.ParentThreadID, req.ThreadID); err != nil {
			return err
		}
		key := artifactRecordKey(req.ThreadID, req.ArtifactID)
		raw, found, err := readBackendDomainRecord(tx, backendDomainV32Format, backendDomainKey(backendDomainRecordArtifact, key), backendDomainRecordArtifact, key, "", 0)
		if err != nil {
			return err
		}
		if !found {
			return ErrArtifactNotFound
		}
		var record artifact.Record
		if err := decodeBackendDomainValue(backendDomainV32Format, raw, &record); err != nil {
			return err
		}
		if record.ThreadID != req.ThreadID || record.Ref.ID != req.ArtifactID || record.CanonicalEntryID == "" || record.CreatedAt.IsZero() || artifact.ValidateRef(record.Ref) != nil {
			return ErrAuthorityCorrupt
		}
		entry, ok, err := reader.entry(req.ThreadID, record.CanonicalEntryID)
		if err != nil {
			return err
		}
		if !ok || entry.ThreadID != record.ThreadID || (entry.Type != EntryToolResult && entry.Type != EntryHostWork) || entry.Message.ToolResult == nil || !reflect.DeepEqual(entry.Message.ToolResult.FullOutput, &record.Ref) {
			return ErrAuthorityCorrupt
		}
		if record.OutputSourceID == "" {
			if err := reader.validateArtifactRecordLocked(record); err != nil {
				return err
			}
			result, err = inlineArtifactPage(record, req)
			return err
		}
		if record.Text != "" {
			return ErrAuthorityCorrupt
		}
		source, err := readOutputSource(tx, record.OutputSourceID)
		if err != nil {
			return err
		}
		if !source.Sealed || source.Bytes != record.Ref.SizeBytes || source.SHA256 != record.Ref.SHA256 {
			return ErrAuthorityCorrupt
		}
		page, err := readOutputSourcePage(ctx, tx, source, req.Offset, req.MaxBytes)
		if err != nil {
			return err
		}
		result = ArtifactPage{Ref: record.Ref, Offset: page.Offset, NextOffset: page.NextOffset, Text: page.Text, HasMore: page.HasMore, Complete: source.Reason == "", Reason: source.Reason, ObservedBytes: source.ObservedBytes}
		return nil
	}, func(memory *MemoryRepo) error {
		content, err := memory.ReadArtifact(ctx, req.ArtifactReadRequest)
		if err != nil {
			return err
		}
		result, err = inlineArtifactPage(artifact.Record{Ref: content.Ref, Text: content.Text}, req)
		return err
	})
	return result, err
}
func inlineArtifactPage(record artifact.Record, req ArtifactPageRequest) (ArtifactPage, error) {
	page := ArtifactPage{Ref: record.Ref, Offset: req.Offset, NextOffset: req.Offset, Complete: true, ObservedBytes: int64(len(record.Text))}
	if req.Offset < 0 || req.Offset > int64(len(record.Text)) || req.Offset < int64(len(record.Text)) && !utf8.RuneStart(record.Text[req.Offset]) {
		return page, spi.ErrInvalidArgument
	}
	end := min(int64(len(record.Text)), req.Offset+int64(req.MaxBytes))
	for end < int64(len(record.Text)) && !utf8.RuneStart(record.Text[end]) {
		end--
	}
	page.Text = record.Text[req.Offset:end]
	page.NextOffset = end
	page.HasMore = end < int64(len(record.Text))
	if !utf8.ValidString(page.Text) {
		return ArtifactPage{}, errors.Join(ErrAuthorityCorrupt, errors.New("artifact text is not UTF-8"))
	}
	return page, nil
}

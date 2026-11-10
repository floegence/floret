package runtime

import (
	"context"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/tools"
)

// ThreadOutputService provides bounded access to canonical text, not storage
// paths or execution authority. Hosts authorize the exact thread on every call.
// Text is the host-sanitized UTF-8 stream, not a claim about original raw bytes.
type ThreadOutputService interface {
	ReadToolOutput(context.Context, ToolOutputReadInput) (ToolOutputPage, error)
	SearchToolOutput(context.Context, ToolOutputSearchInput) (ToolOutputSearchResult, error)
	AppendHostWorkOutput(context.Context, AppendHostWorkOutputInput) (ToolOutputRetention, error)
	ReadHostWorkOutputChunks(context.Context, HostWorkOutputChunksInput) (HostWorkOutputChunkPage, error)
}
type ToolOutputReadInput struct {
	ThreadID identity.ThreadID
	Ref      string
	Offset   int64
	// MaxBytes defaults to 8000 and is at most 65536. Offset must be a UTF-8 boundary.
	MaxBytes int
}
type ToolOutputRetention struct {
	RetainedBytes int64  `json:"retained_bytes"`
	ObservedBytes int64  `json:"observed_bytes"`
	SHA256        string `json:"sha256"`
	Sealed        bool   `json:"sealed"`
	Complete      bool   `json:"complete"`
	Reason        string `json:"reason,omitempty"`
}
type ToolOutputPage struct {
	ToolOutputRetention
	Ref        string `json:"ref"`
	Text       string `json:"text"`
	Offset     int64  `json:"offset"`
	NextOffset int64  `json:"next_offset"`
	HasMore    bool   `json:"has_more"`
}
type ToolOutputSearchInput struct {
	ToolOutputReadInput
	// Literal is case-sensitive UTF-8, from 1 to 256 bytes; no regular expressions.
	Literal string
	// MaxMatches defaults to 20, at most 32. One call scans at most 1 MiB.
	MaxMatches int
}
type ToolOutputMatch struct {
	Offset    int64 `json:"offset"`
	EndOffset int64 `json:"end_offset"`
}
type ToolOutputSearchResult struct {
	ToolOutputRetention
	Ref          string            `json:"ref"`
	Matches      []ToolOutputMatch `json:"matches"`
	NextOffset   int64             `json:"next_offset"`
	HasMore      bool              `json:"has_more"`
	ScannedBytes int64             `json:"scanned_bytes"`
}
type AppendHostWorkOutputInput struct {
	ThreadID identity.ThreadID
	WorkID   string
	// Offset must equal the previous observed watermark; mismatches never append.
	Offset int64
	// Text is one complete UTF-8 chunk, at most 64 KiB. Empty chunks add no sequence.
	Text string
}
type HostWorkOutputChunksInput struct {
	ThreadID identity.ThreadID
	WorkID   string
	AfterSeq int64
	MaxBytes int
}
type HostWorkOutputChunk struct {
	Seq    int64  `json:"seq"`
	Offset int64  `json:"offset"`
	Text   string `json:"text"`
}
type HostWorkOutputChunkPage struct {
	ToolOutputRetention
	Chunks    []HostWorkOutputChunk `json:"chunks"`
	LastSeq   int64                 `json:"last_seq"`
	LatestSeq int64                 `json:"latest_seq"`
	HasMore   bool                  `json:"has_more"`
}

func outputRetention(s sessiontree.OutputSource) ToolOutputRetention {
	return ToolOutputRetention{RetainedBytes: s.Bytes, ObservedBytes: s.ObservedBytes, SHA256: s.SHA256, Sealed: s.Sealed, Complete: s.Sealed && s.Reason == "", Reason: s.Reason}
}
func (s *threadRuntimeService) ReadToolOutput(ctx context.Context, in ToolOutputReadInput) (ToolOutputPage, error) {
	var out ToolOutputPage
	ref, err := tools.ParseOutputReference(in.Ref)
	if err != nil {
		return out, err
	}
	if ref.ThreadID != in.ThreadID {
		return out, runtimeHostError(sessiontree.ErrInvalidThreadAuthority)
	}
	if in.MaxBytes == 0 {
		in.MaxBytes = 8000
	}
	if in.Offset < 0 || in.MaxBytes < 4 || in.MaxBytes > 65536 {
		return out, errors.New("invalid output page budget")
	}
	meta, err := s.ensureThread(ctx, in.ThreadID)
	if err != nil {
		return out, err
	}
	out.Ref, out.Offset = in.Ref, in.Offset
	if ref.Kind == "host_work" {
		repo, ok := s.host.store.repo.(sessiontree.HostWorkOutputRepo)
		if !ok {
			return out, ErrUnsupportedStoreCapability
		}
		page, err := repo.ReadHostWorkOutputPage(ctx, sessiontree.HostWorkOutputRequest{ThreadID: ref.ThreadID.String(), WorkID: ref.ID, Offset: in.Offset, MaxBytes: in.MaxBytes})
		if err != nil {
			return out, runtimeHostError(err)
		}
		out.ToolOutputRetention = outputRetention(page.Source)
		out.Text, out.NextOffset, out.HasMore = page.Text, page.NextOffset, page.HasMore
	} else {
		repo, ok := s.host.store.repo.(sessiontree.ArtifactPageReader)
		if !ok {
			return out, ErrUnsupportedStoreCapability
		}
		page, err := repo.ReadArtifactPage(ctx, sessiontree.ArtifactPageRequest{ArtifactReadRequest: sessiontree.ArtifactReadRequest{ThreadID: ref.ThreadID.String(), ParentThreadID: meta.ParentThreadID, ArtifactID: ref.ID}, Offset: in.Offset, MaxBytes: in.MaxBytes})
		if err != nil {
			return out, runtimeHostError(err)
		}
		out.ToolOutputRetention = ToolOutputRetention{RetainedBytes: page.Ref.SizeBytes, ObservedBytes: page.ObservedBytes, SHA256: page.Ref.SHA256, Sealed: true, Complete: page.Complete, Reason: page.Reason}
		out.Text, out.NextOffset, out.HasMore = page.Text, page.NextOffset, page.HasMore
	}
	return out, nil
}
func (s *threadRuntimeService) AppendHostWorkOutput(ctx context.Context, in AppendHostWorkOutputInput) (ToolOutputRetention, error) {
	if err := s.host.available(); err != nil {
		return ToolOutputRetention{}, err
	}
	repo, ok := s.host.store.repo.(sessiontree.HostWorkOutputRepo)
	if !ok {
		return ToolOutputRetention{}, ErrUnsupportedStoreCapability
	}
	source, err := repo.AppendHostWorkOutput(ctx, sessiontree.HostWorkOutputAppend{ThreadID: in.ThreadID.String(), WorkID: in.WorkID, ExecutorID: s.host.executorID, Offset: in.Offset, Text: in.Text})
	return outputRetention(source), runtimeHostError(err)
}
func (s *threadRuntimeService) ReadHostWorkOutputChunks(ctx context.Context, in HostWorkOutputChunksInput) (HostWorkOutputChunkPage, error) {
	var out HostWorkOutputChunkPage
	if _, err := s.ensureThread(ctx, in.ThreadID); err != nil {
		return out, err
	}
	repo, ok := s.host.store.repo.(sessiontree.HostWorkOutputRepo)
	if !ok {
		return out, ErrUnsupportedStoreCapability
	}
	if in.MaxBytes == 0 {
		in.MaxBytes = 8000
	}
	page, err := repo.ReadHostWorkOutputChunks(ctx, sessiontree.HostWorkOutputRequest{ThreadID: in.ThreadID.String(), WorkID: in.WorkID, MaxBytes: in.MaxBytes}, in.AfterSeq)
	if err != nil {
		return out, runtimeHostError(err)
	}
	out.ToolOutputRetention = outputRetention(page.Source)
	out.LastSeq, out.LatestSeq, out.HasMore = in.AfterSeq, page.Source.Chunks, page.HasMore
	out.Chunks = make([]HostWorkOutputChunk, 0, len(page.Chunks))
	for _, chunk := range page.Chunks {
		out.Chunks = append(out.Chunks, HostWorkOutputChunk{Seq: chunk.Seq, Offset: chunk.Offset, Text: chunk.Text})
		out.LastSeq = chunk.Seq
	}
	return out, nil
}
func (s *threadRuntimeService) SearchToolOutput(ctx context.Context, in ToolOutputSearchInput) (ToolOutputSearchResult, error) {
	out := ToolOutputSearchResult{Ref: in.Ref, NextOffset: in.Offset, Matches: []ToolOutputMatch{}}
	if len(in.Literal) == 0 || len(in.Literal) > 256 || !utf8.ValidString(in.Literal) || in.MaxMatches < 0 || in.MaxMatches > 32 {
		return out, errors.New("invalid output search budget")
	}
	if in.MaxMatches == 0 {
		in.MaxMatches = 20
	}
	const scanLimit = 1 << 20
	read := in.ToolOutputReadInput
	read.MaxBytes = 65536
	carry := ""
	for out.ScannedBytes < scanLimit {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		remaining := scanLimit - out.ScannedBytes
		if remaining < utf8.UTFMax {
			break
		}
		read.MaxBytes = int(min(65536, remaining))
		page, err := s.ReadToolOutput(ctx, read)
		if err != nil {
			return out, err
		}
		out.ToolOutputRetention = page.ToolOutputRetention
		out.ScannedBytes += int64(len(page.Text))
		text := carry + page.Text
		base := page.Offset - int64(len(carry))
		cursor := 0
		for cursor < len(text) {
			index := strings.Index(text[cursor:], in.Literal)
			if index < 0 {
				break
			}
			index += cursor
			start := base + int64(index)
			end := start + int64(len(in.Literal))
			out.Matches = append(out.Matches, ToolOutputMatch{Offset: start, EndOffset: end})
			cursor = index + len(in.Literal)
			if len(out.Matches) == in.MaxMatches {
				out.NextOffset = end
				out.HasMore = end < page.RetainedBytes
				return out, nil
			}
		}
		out.NextOffset = page.NextOffset
		out.HasMore = page.HasMore
		if !page.HasMore {
			return out, nil
		}
		if page.NextOffset <= read.Offset {
			return out, ErrAuthorityCorrupt
		}
		// Carry fewer than one complete match; no match can be duplicated.
		start := max(cursor, len(text)-len(in.Literal)+1)
		for start < len(text) && !utf8.RuneStart(text[start]) {
			start++
		}
		carry = text[start:]
		read.Offset = page.NextOffset
	}
	out.NextOffset = read.Offset - int64(len(carry))
	out.HasMore = true
	return out, nil
}

var _ ThreadOutputService = (*threadRuntimeService)(nil)

package sessiontree

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/floegence/floret/v7/storage/spi"
)

const bodyTextFactsNamespace = "floret.sessiontree.body-text-facts.v1"

// CanonicalTextFacts contains only extent and classification metadata. Boundary
// bytes preserve exact rune offsets across UTF-8 fragments; no preview is stored.
type CanonicalTextFacts struct {
	Bytes          int
	Runes          int
	NonSpace       bool
	Head           []byte
	Incomplete     []byte
	SummaryStart   int
	SummaryEnd     int
	SummaryHasTail bool
	PrefixEnd      int
	PrefixHasTail  bool
	// Prefix and Summary are transient bounded read results.
	Prefix  string `json:"-"`
	Summary string `json:"-"`
}
type storedBodyTextFacts struct {
	Source canonicalBodyRef
	Facts  CanonicalTextFacts
}

func canonicalTextFacts(text string) CanonicalTextFacts {
	facts := CanonicalTextFacts{Bytes: len(text), Runes: utf8.RuneCountInString(text), NonSpace: strings.IndexFunc(text, func(r rune) bool { return !unicode.IsSpace(r) }) >= 0}
	for count := 0; facts.PrefixEnd < len(text) && count < 160; count++ {
		_, size := utf8.DecodeRuneInString(text[facts.PrefixEnd:])
		facts.PrefixEnd += size
	}
	facts.PrefixHasTail = strings.TrimSpace(text[facts.PrefixEnd:]) != ""
	trimmed := strings.TrimLeftFunc(text, unicode.IsSpace)
	facts.SummaryStart = len(text) - len(trimmed)
	end, count := 0, 0
	for end < len(trimmed) && count < 160 {
		_, size := utf8.DecodeRuneInString(trimmed[end:])
		end += size
		count++
	}
	facts.SummaryEnd = facts.SummaryStart + end
	facts.SummaryHasTail = strings.TrimSpace(trimmed[end:]) != ""
	facts.Head = bytes.Clone([]byte(text[:min(3, len(text))]))
	if len(text) > 0 {
		start := len(text) - 1
		for start > 0 && len(text)-start < utf8.UTFMax && !utf8.RuneStart(text[start]) {
			start--
		}
		if !utf8.FullRuneInString(text[start:]) {
			facts.Incomplete = bytes.Clone([]byte(text[start:]))
		}
	}
	return facts
}
func writeBodyTextFacts(tx spi.WriteTx, ref canonicalBodyRef, text string) error {
	raw, err := encodeActivityMetadata("body-text-facts-v1", storedBodyTextFacts{Source: ref, Facts: canonicalTextFacts(text)})
	if err != nil {
		return err
	}
	return tx.Put(bodyTextFactsNamespace, canonicalBodyKey(ref.SHA256), raw)
}
func readBodyTextFacts(tx spi.ReadTx, ref canonicalBodyRef) (CanonicalTextFacts, error) {
	raw, err := tx.Get(bodyTextFactsNamespace, canonicalBodyKey(ref.SHA256))
	if err != nil {
		return CanonicalTextFacts{}, err
	}
	var stored storedBodyTextFacts
	if decodeActivityMetadata(raw, "body-text-facts-v1", &stored) != nil || stored.Source != ref || stored.Facts.Bytes != ref.Bytes || stored.Facts.Runes < 1 || stored.Facts.Runes > ref.Bytes || len(stored.Facts.Head) != min(3, ref.Bytes) || len(stored.Facts.Incomplete) > 3 || stored.Facts.SummaryStart < 0 || stored.Facts.SummaryEnd < stored.Facts.SummaryStart || stored.Facts.SummaryEnd > ref.Bytes || stored.Facts.SummaryEnd-stored.Facts.SummaryStart > 640 || stored.Facts.PrefixEnd < 1 || stored.Facts.PrefixEnd > min(640, ref.Bytes) {
		return CanonicalTextFacts{}, ErrAuthorityCorrupt
	}
	return stored.Facts, nil
}

type missingBodyTextFacts struct {
	entry, field string
	ref          canonicalBodyRef
}

type missingWindowTextFacts []missingBodyTextFacts

func (missing missingWindowTextFacts) Error() string {
	return "canonical window text facts require preparation"
}

func (missing missingBodyTextFacts) Error() string { return "canonical text facts require preparation" }

// The canonical owner validates immutable ancestry and hashes before publishing
// one rebuildable record. No writer is held while reading the historical body.
func (repo *BackendRepo) prepareBodyTextFacts(ctx context.Context, thread string, missing missingBodyTextFacts) error {
	return repo.prepareWindowTextFacts(ctx, thread, missingWindowTextFacts{missing})
}
func (repo *BackendRepo) prepareWindowTextFacts(ctx context.Context, thread string, missing missingWindowTextFacts) error {
	work, release, err := repo.migrationGate.acquire(ctx, true)
	if err != nil {
		return err
	}
	defer release()
	for _, source := range missing {
		if err := repo.prepareBodyTextFactsOwned(work, thread, source); err != nil {
			return err
		}
	}
	return nil
}
func (repo *BackendRepo) prepareBodyTextFactsOwned(work context.Context, thread string, missing missingBodyTextFacts) error {
	reader := &canonicalSnapshotReader{migrationSourceReader: migrationSourceReader{ctx: work, backend: repo.backend}, format: backendDomainV32Format, metadata: map[string]migrationMetadataRead{}}
	meta, err := readPublishedThreadMeta(reader, backendDomainV32Format, thread)
	if err != nil {
		return err
	}
	entry, err := activeLocatedEntry(work, reader, meta, missing.entry)
	if err != nil {
		return reader.validationError(err)
	}
	if entry.bodyFields[missing.field] != missing.ref {
		return ErrAuthorityCorrupt
	}
	if _, err := readBodyTextFacts(reader, missing.ref); err == nil {
		return nil
	} else if !errors.Is(err, spi.ErrNotFound) {
		return err
	}
	text, err := materializeCanonicalBody(work, reader, missing.ref)
	if err != nil {
		return reader.validationError(err)
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	return repo.backend.Update(work, func(tx spi.WriteTx) error {
		current, err := readPublishedThreadMeta(tx, backendDomainV32Format, thread)
		if err != nil {
			return err
		}
		source, err := activeLocatedEntry(work, tx, current, entry.ID)
		if err != nil {
			return err
		}
		if source.RawHash != entry.RawHash || source.bodyFields[missing.field] != missing.ref {
			return ErrAuthorityCorrupt
		}
		return writeBodyTextFacts(tx, missing.ref, text)
	})
}
func windowTextField(entry Entry, field string) bool {
	if entry.Message.Kind == "control_signal" || entry.Message.ControlSignal != nil {
		return false
	}
	return field == "content" && (entry.Type == EntryUserMessage || entry.Type == EntryAssistantMessage) || field == "reasoning" && (entry.Type == EntryAssistantMessage || entry.Type == EntryToolCall)
}

// Preview offsets avoid scanning leading whitespace or trailing padding.
func readBodyTextPreview(ctx context.Context, tx spi.ReadTx, ref canonicalBodyRef, facts CanonicalTextFacts) (CanonicalTextFacts, error) {
	if _, err := readCanonicalBodyRecord(tx, ref); err != nil {
		return CanonicalTextFacts{}, err
	}
	prefix, err := readCanonicalBodyBytes(ctx, tx, ref, 0, facts.PrefixEnd)
	if err != nil {
		return CanonicalTextFacts{}, err
	}
	facts.Prefix = string(prefix)
	if facts.SummaryEnd <= facts.PrefixEnd {
		facts.Summary = facts.Prefix[facts.SummaryStart:facts.SummaryEnd]
	} else {
		summary, err := readCanonicalBodyBytes(ctx, tx, ref, facts.SummaryStart, facts.SummaryEnd-facts.SummaryStart)
		if err != nil {
			return CanonicalTextFacts{}, err
		}
		facts.Summary = string(summary)
	}
	return facts, nil
}

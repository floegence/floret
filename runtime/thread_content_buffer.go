package runtime

import (
	"github.com/floegence/floret/v7/internal/sessiontree"
	"strings"
	"unicode"
	"unicode/utf8"
)

// A full-view request may assemble many immutable segments into one item. Keep
// one append buffer and count only new runes; citations use character offsets.
// An incomplete suffix is counted exactly as RuneCountInString would count it
// until a later segment supplies its remaining bytes.
type threadContentBuffer struct {
	text       strings.Builder
	runes      int
	incomplete string
	bytes      int
	nonSpace   bool
}

func (buffer *threadContentBuffer) append(text string) {
	buffer.bytes += len(text)
	if !buffer.nonSpace {
		buffer.nonSpace = strings.IndexFunc(text, func(r rune) bool { return !unicode.IsSpace(r) }) >= 0
	}
	tail := text
	if buffer.incomplete == "" {
		buffer.runes += utf8.RuneCountInString(text)
	} else {
		tail = buffer.incomplete + text
		buffer.runes -= utf8.RuneCountInString(buffer.incomplete)
		buffer.runes += utf8.RuneCountInString(buffer.incomplete + text)
	}
	buffer.text.WriteString(text)
	buffer.incomplete = ""
	full := tail
	if len(full) == 0 {
		return
	}
	start := len(full) - 1
	for start > 0 && len(full)-start < utf8.UTFMax && !utf8.RuneStart(full[start]) {
		start--
	}
	if !utf8.FullRuneInString(full[start:]) {
		buffer.incomplete = strings.Clone(full[start:])
	}
}

// Source metadata replaces body decoding only for the read-only window reducer.
func (buffer *threadContentBuffer) appendFacts(facts sessiontree.CanonicalTextFacts) {
	previous := buffer.incomplete
	buffer.runes += facts.Runes
	if previous != "" {
		head := string(facts.Head)
		buffer.runes += utf8.RuneCountInString(previous+head) - utf8.RuneCountInString(previous) - utf8.RuneCountInString(head)
	}
	buffer.bytes += facts.Bytes
	buffer.nonSpace = buffer.nonSpace || facts.NonSpace
	buffer.incomplete = string(facts.Incomplete)
	if facts.Bytes < utf8.UTFMax && previous != "" {
		combined := previous + string(facts.Head)
		start := len(combined) - 1
		for start > 0 && len(combined)-start < utf8.UTFMax && !utf8.RuneStart(combined[start]) {
			start--
		}
		buffer.incomplete = ""
		if !utf8.FullRuneInString(combined[start:]) {
			buffer.incomplete = strings.Clone(combined[start:])
		}
	}
}

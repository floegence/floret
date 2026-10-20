package runtime

import (
	"strings"
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
}

func (buffer *threadContentBuffer) append(text string) {
	if buffer.incomplete == "" {
		buffer.runes += utf8.RuneCountInString(text)
	} else {
		buffer.runes -= utf8.RuneCountInString(buffer.incomplete)
		buffer.runes += utf8.RuneCountInString(buffer.incomplete + text)
	}
	buffer.text.WriteString(text)
	buffer.incomplete = ""
	full := buffer.text.String()
	if len(full) == 0 {
		return
	}
	start := len(full) - 1
	for start > 0 && len(full)-start < utf8.UTFMax && !utf8.RuneStart(full[start]) {
		start--
	}
	if !utf8.FullRuneInString(full[start:]) {
		buffer.incomplete = full[start:]
	}
}

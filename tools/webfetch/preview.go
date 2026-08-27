package webfetch

import (
	"strings"
	"unicode"
)

func boundedPreview(content string) (string, bool) {
	cleaned := cleanPreviewText(content)
	runes := []rune(cleaned)
	if len(runes) <= activityPreviewRunes {
		return cleaned, false
	}
	return string(runes[:activityPreviewRunes]), true
}

func cleanPreviewText(content string) string {
	content = strings.ReplaceAll(strings.ToValidUTF8(content, "\uFFFD"), "\r\n", "\n")
	content = strings.ReplaceAll(content, "\r", "\n")
	var output strings.Builder
	const (
		stateText = iota
		stateEscape
		stateCSI
		stateOSC
		stateOSCEscape
	)
	state := stateText
	for _, value := range content {
		switch state {
		case stateEscape:
			switch value {
			case '[':
				state = stateCSI
			case ']':
				state = stateOSC
			default:
				state = stateText
			}
			continue
		case stateCSI:
			if value >= 0x40 && value <= 0x7e {
				state = stateText
			}
			continue
		case stateOSC:
			if value == '\a' || value == '\u009c' {
				state = stateText
			} else if value == '\x1b' {
				state = stateOSCEscape
			}
			continue
		case stateOSCEscape:
			if value == '\\' {
				state = stateText
			} else if value != '\x1b' {
				state = stateOSC
			}
			continue
		}
		switch value {
		case '\x1b':
			state = stateEscape
		case '\u009b':
			state = stateCSI
		case '\u009d':
			state = stateOSC
		case '\n', '\t':
			output.WriteRune(value)
		default:
			if !unicode.IsControl(value) && value != '\u200b' && value != '\ufeff' {
				output.WriteRune(value)
			}
		}
	}
	return strings.TrimSpace(output.String())
}

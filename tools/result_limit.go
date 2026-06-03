package tools

import (
	"strings"
	"unicode/utf8"
)

type ResultLimit struct {
	MaxBytes int
	MaxLines int
	Strategy string
}

func ApplyResultLimit(result Result, limit ResultLimit) Result {
	text := result.Text
	if text == "" {
		return result
	}
	limited := text
	truncated := false
	if limit.MaxLines > 0 {
		lines := strings.Split(limited, "\n")
		if len(lines) > limit.MaxLines {
			truncated = true
			if limit.Strategy == "head" {
				limited = strings.Join(lines[:limit.MaxLines], "\n")
			} else {
				limited = strings.Join(lines[len(lines)-limit.MaxLines:], "\n")
			}
		}
	}
	if limit.MaxBytes > 0 && len(limited) > limit.MaxBytes {
		truncated = true
		if limit.Strategy == "head" {
			limited = safeHead(limited, limit.MaxBytes)
		} else {
			limited = safeTail(limited, limit.MaxBytes)
		}
	}
	if !truncated {
		return result
	}
	if result.Metadata == nil {
		result.Metadata = map[string]any{}
	}
	result.Metadata["truncated"] = true
	result.Metadata["original_bytes"] = len(text)
	result.Metadata["returned_bytes"] = len(limited)
	result.Metadata["truncation_strategy"] = limit.Strategy
	if result.Metadata["truncation_strategy"] == "" {
		result.Metadata["truncation_strategy"] = "tail"
	}
	result.Text = limited
	return result
}

func safeHead(s string, max int) string {
	if max <= 0 || len(s) <= max {
		return s
	}
	for max > 0 && !utf8.ValidString(s[:max]) {
		max--
	}
	return s[:max]
}

func safeTail(s string, max int) string {
	if max <= 0 || len(s) <= max {
		return s
	}
	start := len(s) - max
	for start < len(s) && !utf8.RuneStart(s[start]) {
		start++
	}
	return s[start:]
}

// Package citation owns provider-neutral assistant source metadata.
package citation

import (
	"net/url"
	"strings"
	"unicode"
	"unicode/utf8"
)

type Source struct {
	Title      string `json:"title,omitempty"`
	URL        string `json:"url,omitempty"`
	StartIndex *int   `json:"start_index,omitempty"`
	EndIndex   *int   `json:"end_index,omitempty"`
}

func Clone(in []Source) []Source {
	if in == nil {
		return nil
	}
	out := make([]Source, len(in))
	for i, s := range in {
		out[i] = s
		if s.StartIndex != nil {
			v := *s.StartIndex
			out[i].StartIndex = &v
		}
		if s.EndIndex != nil {
			v := *s.EndIndex
			out[i].EndIndex = &v
		}
	}
	return out
}

func SafeURL(value string) bool {
	if strings.IndexFunc(value, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 {
		return false
	}
	u, err := url.Parse(value)
	return err == nil && (u.Scheme == "https" || u.Scheme == "http") && u.Hostname() != "" && u.User == nil && u.Opaque == ""
}

// Normalize drops unsafe links and removes malformed positional claims.
func Normalize(in []Source) []Source {
	var out []Source
	for _, s := range Clone(in) {
		s.URL = strings.TrimSpace(s.URL)
		s.Title = strings.TrimSpace(s.Title)
		if !SafeURL(s.URL) {
			continue
		}
		if s.StartIndex == nil || s.EndIndex == nil || *s.StartIndex < 0 || *s.EndIndex <= *s.StartIndex {
			s.StartIndex, s.EndIndex = nil, nil
		}
		duplicate := false
		for _, previous := range out {
			if Equal(previous, s) {
				duplicate = true
				break
			}
		}
		if !duplicate {
			out = append(out, s)
		}
	}
	return out
}

func Equal(a, b Source) bool {
	return a.Title == b.Title && a.URL == b.URL && equalIndex(a.StartIndex, b.StartIndex) && equalIndex(a.EndIndex, b.EndIndex)
}
func equalIndex(a, b *int) bool { return a == nil && b == nil || a != nil && b != nil && *a == *b }

// Fragment maps end-exclusive Unicode code-point ranges to a byte-delimited
// assistant fragment. Unlocated and cross-fragment sources belong to the last
// text fragment, where hosts can present them without inventing a binding.
func Fragment(in []Source, text string, start, end int, last bool, boundaries []int) []Source {
	lo, hi := utf8.RuneCountInString(text[:start]), utf8.RuneCountInString(text[:end])
	total := utf8.RuneCountInString(text)
	var out []Source
	for _, s := range Normalize(in) {
		located := s.StartIndex != nil && *s.EndIndex <= total
		if located {
			for _, boundary := range boundaries {
				at := utf8.RuneCountInString(text[:boundary])
				if *s.StartIndex < at && *s.EndIndex > at {
					located = false
					break
				}
			}
		}
		if located {
			if *s.StartIndex < lo || *s.EndIndex > hi {
				continue
			}
			a, b := *s.StartIndex-lo, *s.EndIndex-lo
			s.StartIndex, s.EndIndex = &a, &b
		} else {
			if !last {
				continue
			}
			s.StartIndex, s.EndIndex = nil, nil
		}
		out = append(out, s)
	}
	return out
}

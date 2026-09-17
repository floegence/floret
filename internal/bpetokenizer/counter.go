// Package bpetokenizer provides bounded offline byte-pair counting.
package bpetokenizer

import (
	"errors"
	"github.com/dlclark/regexp2"
	tiktoken "github.com/pkoukk/tiktoken-go"
	"time"
)

func New(ranks map[string]int, patterns []string) (*Counter, error) {
	bpe, err := tiktoken.NewCoreBPE(ranks, nil, `(?s).+`)
	if err != nil {
		return nil, err
	}
	out := &Counter{bpe: tiktoken.NewTiktoken(bpe, nil, nil)}
	for _, pattern := range patterns {
		re, err := regexp2.Compile(pattern, regexp2.None)
		if err != nil {
			return nil, err
		}
		re.MatchTimeout = time.Second
		out.patterns = append(out.patterns, re)
	}
	return out, nil
}

type Counter struct {
	bpe      *tiktoken.Tiktoken
	patterns []*regexp2.Regexp
}

// Count splits unusually long spans to bound work on untrusted model input.
func (c *Counter) Count(text string) (int64, error) { return c.count([]rune(text), 0) }

func (c *Counter) count(text []rune, stage int) (int64, error) {
	if len(text) == 0 {
		return 0, nil
	}
	if stage == len(c.patterns) {
		var total int64
		for len(text) > 0 {
			n := min(len(text), 512)
			total += int64(len(c.bpe.EncodeOrdinary(string(text[:n]))))
			text = text[n:]
		}
		return total, nil
	}
	var total int64
	offset := 0
	match, err := c.patterns[stage].FindRunesMatch(text)
	for match != nil && err == nil {
		if match.Index > offset {
			n, e := c.count(text[offset:match.Index], stage+1)
			if e != nil {
				return 0, e
			}
			total += n
		}
		n, e := c.count(text[match.Index:match.Index+match.Length], stage+1)
		if e != nil {
			return 0, e
		}
		total += n
		offset = match.Index + match.Length
		match, err = c.patterns[stage].FindNextMatch(match)
	}
	if err != nil {
		// regexp2 errors include the input. Never expose model context in failures.
		return 0, errors.New("input tokenization exceeded its work limit")
	}
	n, err := c.count(text[offset:], stage+1)
	return total + n, err
}

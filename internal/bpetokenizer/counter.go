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
func (c *Counter) Count(text string) (int64, error) { return c.count([]rune(text), 0, nil) }

func (c *Counter) count(text []rune, stage int, emit func([]int)) (int64, error) {
	if len(text) == 0 {
		return 0, nil
	}
	if stage == len(c.patterns) {
		var total int64
		for len(text) > 0 {
			n := min(len(text), 512)
			tokens := c.bpe.EncodeOrdinary(string(text[:n]))
			total += int64(len(tokens))
			if emit != nil {
				emit(tokens)
			}
			text = text[n:]
		}
		return total, nil
	}
	var total int64
	offset := 0
	match, err := c.patterns[stage].FindRunesMatch(text)
	for match != nil && err == nil {
		if match.Index > offset {
			n, e := c.count(text[offset:match.Index], stage+1, emit)
			if e != nil {
				return 0, e
			}
			total += n
		}
		n, e := c.count(text[match.Index:match.Index+match.Length], stage+1, emit)
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
	n, err := c.count(text[offset:], stage+1, emit)
	return total + n, err
}

// CountParts assigns each token to the span containing its first byte. Span
// boundaries do not change tokenization or the total, even across BPE merges.
func (c *Counter) CountParts(text string, ends []int) ([]int64, error) {
	out := make([]int64, len(ends))
	if len(ends) == 0 || ends[len(ends)-1] != len(text) {
		return nil, errors.New("invalid token attribution spans")
	}
	offset, part := 0, 0
	_, err := c.count([]rune(text), 0, func(tokens []int) {
		for _, token := range tokens {
			for part+1 < len(ends) && offset >= ends[part] {
				part++
			}
			out[part]++
			offset += len(c.bpe.Decode([]int{token}))
		}
	})
	return out, err
}

// Package deepseektokenizer counts text with the official DeepSeek V4 vocabulary.
// It performs no network requests and does not retain input text.
package deepseektokenizer

import (
	"bytes"
	"compress/gzip"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dlclark/regexp2"
	tiktoken "github.com/pkoukk/tiktoken-go"
)

//go:embed v4.json.gz
var vocabulary []byte

var load = sync.OnceValues(func() (*counter, error) {
	reader, err := gzip.NewReader(bytes.NewReader(vocabulary))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	var data struct {
		Patterns []string       `json:"patterns"`
		Ranks    map[string]int `json:"ranks"`
	}
	if err := json.NewDecoder(reader).Decode(&data); err != nil {
		return nil, err
	}
	ranks := make(map[string]int, len(data.Ranks))
	for encoded, rank := range data.Ranks {
		raw, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return nil, err
		}
		ranks[string(raw)] = rank
	}
	bpe, err := tiktoken.NewCoreBPE(ranks, nil, `(?s).+`)
	if err != nil {
		return nil, err
	}
	out := &counter{bpe: tiktoken.NewTiktoken(bpe, nil, nil)}
	for _, pattern := range data.Patterns {
		re, err := regexp2.Compile(pattern, regexp2.None)
		if err != nil {
			return nil, err
		}
		re.MatchTimeout = time.Second
		out.patterns = append(out.patterns, re)
	}
	return out, nil
})

type counter struct {
	bpe      *tiktoken.Tiktoken
	patterns []*regexp2.Regexp
}

// Count follows the official sequential isolated splits. Unusually long spans
// are segmented to bound BPE work on untrusted tool output (including base64).
// Such segmentation is why this remains a request estimate, not native usage.
func Count(text string) (int64, error) {
	tokenizer, err := load()
	if err != nil {
		return 0, fmt.Errorf("load DeepSeek V4 vocabulary: %w", err)
	}
	return tokenizer.count([]rune(text), 0)
}

func (c *counter) count(text []rune, stage int) (int64, error) {
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
		return 0, errors.New("DeepSeek V4 input tokenization exceeded its work limit")
	}
	n, err := c.count(text[offset:], stage+1)
	return total + n, err
}

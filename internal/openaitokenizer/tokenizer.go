// Package openaitokenizer embeds the official OpenAI text vocabularies.
// It never downloads data or retains request text.
package openaitokenizer

import (
	"bufio"
	"bytes"
	"compress/gzip"
	_ "embed"
	"encoding/base64"
	"errors"
	"strconv"
	"strings"
	"sync"

	"github.com/floegence/floret/v7/internal/bpetokenizer"
)

//go:embed cl100k_base.tiktoken.gz
var cl100k []byte

//go:embed o200k_base.tiktoken.gz
var o200k []byte

// The equivalent non-possessive cl100k pattern works with Go regexp2.
const clPattern = `'(?i:[sdmt]|ll|ve|re)|[^\r\n\p{L}\p{N}]?\p{L}+|\p{N}{1,3}| ?[^\s\p{L}\p{N}]+[\r\n]*|\s+$|\s*[\r\n]|\s+(?!\S)|\s`
const oPattern = `[^\r\n\p{L}\p{N}]?[\p{Lu}\p{Lt}\p{Lm}\p{Lo}\p{M}]*[\p{Ll}\p{Lm}\p{Lo}\p{M}]+(?i:'s|'t|'re|'ve|'m|'ll|'d)?|[^\r\n\p{L}\p{N}]?[\p{Lu}\p{Lt}\p{Lm}\p{Lo}\p{M}]+[\p{Ll}\p{Lm}\p{Lo}\p{M}]*(?i:'s|'t|'re|'ve|'m|'ll|'d)?|\p{N}{1,3}| ?[^\s\p{L}\p{N}]+[\r\n/]*|\s*[\r\n]+|\s+(?!\S)|\s+`

var loadCL = sync.OnceValues(func() (*bpetokenizer.Counter, error) { return load(cl100k, clPattern) })
var loadO = sync.OnceValues(func() (*bpetokenizer.Counter, error) { return load(o200k, oPattern) })

func load(data []byte, pattern string) (*bpetokenizer.Counter, error) {
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	ranks := map[string]int{}
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		parts := strings.Fields(scanner.Text())
		if len(parts) != 2 {
			return nil, errors.New("invalid embedded vocabulary")
		}
		token, err := base64.StdEncoding.DecodeString(parts[0])
		if err != nil {
			return nil, err
		}
		rank, err := strconv.Atoi(parts[1])
		if err != nil {
			return nil, err
		}
		ranks[string(token)] = rank
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return bpetokenizer.New(ranks, []string{pattern})
}

func Count(encoding, text string) (int64, error) {
	var counter *bpetokenizer.Counter
	var err error
	switch encoding {
	case "cl100k_base":
		counter, err = loadCL()
	case "o200k_base":
		counter, err = loadO()
	default:
		return 0, errors.New("unsupported text encoding")
	}
	if err != nil {
		return 0, err
	}
	return counter.Count(text)
}

// Encoding follows the published tiktoken model mappings. Unknown models do not
// inherit an encoding merely because their transport is OpenAI-compatible.
func Encoding(provider, model string) string {
	if provider == "openrouter" && strings.HasPrefix(model, "openai/") {
		provider = "openai"
		model = strings.TrimPrefix(model, "openai/")
	}
	if provider != "openai" {
		return ""
	}
	for _, prefix := range []string{"gpt-5", "gpt-4.5-", "gpt-4.1-", "gpt-4o-", "chatgpt-4o-", "o1-", "o3-", "o4-mini-", "ft:gpt-4o"} {
		if strings.HasPrefix(model, prefix) {
			return "o200k_base"
		}
	}
	switch model {
	case "gpt-4.1", "gpt-4o", "o1", "o3", "o4-mini":
		return "o200k_base"
	}
	for _, prefix := range []string{"gpt-4-", "gpt-3.5-turbo-", "gpt-35-turbo-", "ft:gpt-4:", "ft:gpt-3.5-turbo:"} {
		if strings.HasPrefix(model, prefix) {
			return "cl100k_base"
		}
	}
	switch model {
	case "gpt-4", "gpt-3.5-turbo", "gpt-35-turbo":
		return "cl100k_base"
	}
	return ""
}

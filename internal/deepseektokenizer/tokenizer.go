// Package deepseektokenizer counts text with the official DeepSeek V4 vocabulary.
// It performs no network requests and does not retain input text.
package deepseektokenizer

import (
	"bytes"
	"compress/gzip"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/floegence/floret/v7/internal/bpetokenizer"
)

//go:embed v4.json.gz
var vocabulary []byte

var load = sync.OnceValues(func() (*bpetokenizer.Counter, error) {
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
	return bpetokenizer.New(ranks, data.Patterns)
})

// Count uses the official V4 splits and bounded BPE work.
func Count(text string) (int64, error) {
	counter, err := load()
	if err != nil {
		return 0, fmt.Errorf("load DeepSeek V4 vocabulary: %w", err)
	}
	return counter.Count(text)
}

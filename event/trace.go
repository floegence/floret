package event

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"strings"
	"sync"
)

type TraceWriter struct {
	mu sync.Mutex
	w  *bufio.Writer
}

func NewTraceWriter(w io.Writer) *TraceWriter {
	return &TraceWriter{w: bufio.NewWriter(w)}
}

func (t *TraceWriter) Emit(e Event) {
	t.mu.Lock()
	defer t.mu.Unlock()
	e = Sanitize(e)
	_ = json.NewEncoder(t.w).Encode(e)
	_ = t.w.Flush()
}

func StableHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func Redact(value string) string {
	if value == "" {
		return value
	}
	fields := []string{"api_key", "apikey", "authorization", "password", "token", "secret"}
	lower := strings.ToLower(value)
	for _, field := range fields {
		idx := strings.Index(lower, field)
		for idx >= 0 {
			end := idx + len(field)
			for end < len(value) && (value[end] == ' ' || value[end] == ':' || value[end] == '=' || value[end] == '"' || value[end] == '\'') {
				end++
			}
			stop := end
			for stop < len(value) && value[stop] != ' ' && value[stop] != ',' && value[stop] != '\n' && value[stop] != '"' && value[stop] != '\'' {
				stop++
			}
			if stop > end {
				value = value[:end] + "[REDACTED]" + value[stop:]
				lower = strings.ToLower(value)
			}
			nextFrom := idx + len(field)
			if nextFrom >= len(lower) {
				break
			}
			next := strings.Index(lower[nextFrom:], field)
			if next < 0 {
				break
			}
			idx = nextFrom + next
		}
	}
	return value
}

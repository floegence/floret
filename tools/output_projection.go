package tools

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/floegence/floret/session/artifact"
)

const (
	DefaultToolVisibleMaxBytes = 64 * 1024
	DefaultToolVisibleMaxLines = 0
	DefaultToolOutputStrategy  = OutputTail
	DefaultPreserveFull        = true
)

type OutputStrategy string

const (
	OutputHead OutputStrategy = "head"
	OutputTail OutputStrategy = "tail"
)

type OutputPolicy struct {
	VisibleMaxBytes int
	VisibleMaxLines int
	Strategy        OutputStrategy
	PreserveFull    bool
	PreserveFullSet bool
	ArtifactKind    string
	ArtifactMIME    string
}

type OutputProjection struct {
	VisibleText   string
	Truncated     bool
	OriginalBytes int
	VisibleBytes  int
	OriginalLines int
	VisibleLines  int
	Strategy      OutputStrategy
	ContentSHA256 string
	FullOutput    *artifact.Ref
}

func DefaultOutputPolicy() OutputPolicy {
	return OutputPolicy{
		VisibleMaxBytes: DefaultToolVisibleMaxBytes,
		VisibleMaxLines: DefaultToolVisibleMaxLines,
		Strategy:        DefaultToolOutputStrategy,
		PreserveFull:    DefaultPreserveFull,
		PreserveFullSet: true,
		ArtifactKind:    artifact.DefaultKind,
		ArtifactMIME:    artifact.DefaultMIME,
	}
}

func NormalizeOutputPolicy(policy OutputPolicy) OutputPolicy {
	if policy == (OutputPolicy{}) {
		return DefaultOutputPolicy()
	}
	if policy.VisibleMaxBytes <= 0 {
		policy.VisibleMaxBytes = DefaultToolVisibleMaxBytes
	}
	if policy.VisibleMaxLines < 0 {
		policy.VisibleMaxLines = DefaultToolVisibleMaxLines
	}
	switch policy.Strategy {
	case OutputHead, OutputTail:
	default:
		policy.Strategy = DefaultToolOutputStrategy
	}
	if !policy.PreserveFullSet {
		policy.PreserveFull = DefaultPreserveFull
	}
	if policy.ArtifactKind == "" {
		policy.ArtifactKind = artifact.DefaultKind
	}
	if policy.ArtifactMIME == "" {
		policy.ArtifactMIME = artifact.DefaultMIME
	}
	return policy
}

func MergeOutputPolicy(base OutputPolicy, override *OutputPolicy) OutputPolicy {
	out := NormalizeOutputPolicy(base)
	if override == nil {
		return out
	}
	if override.VisibleMaxBytes > 0 {
		out.VisibleMaxBytes = override.VisibleMaxBytes
	}
	if override.VisibleMaxLines > 0 {
		out.VisibleMaxLines = override.VisibleMaxLines
	}
	if override.Strategy != "" {
		out.Strategy = override.Strategy
	}
	if override.PreserveFull || override.PreserveFullSet {
		out.PreserveFull = override.PreserveFull
		out.PreserveFullSet = true
	}
	if override.ArtifactKind != "" {
		out.ArtifactKind = override.ArtifactKind
	}
	if override.ArtifactMIME != "" {
		out.ArtifactMIME = override.ArtifactMIME
	}
	return NormalizeOutputPolicy(out)
}

func BuildOutputProjection(ctx context.Context, result Result, policy OutputPolicy, store artifact.Store) (OutputProjection, error) {
	policy = NormalizeOutputPolicy(policy)
	text := result.Text
	limited := text
	truncated := false
	if policy.VisibleMaxLines > 0 {
		next, ok := limitLines(limited, policy.VisibleMaxLines, policy.Strategy)
		if ok {
			limited = next
			truncated = true
		}
	}
	if policy.VisibleMaxBytes > 0 && len(limited) > policy.VisibleMaxBytes {
		truncated = true
		if policy.Strategy == OutputHead {
			limited = safeHead(limited, policy.VisibleMaxBytes)
		} else {
			limited = safeTail(limited, policy.VisibleMaxBytes)
		}
	}
	projection := OutputProjection{
		VisibleText:   limited,
		Truncated:     truncated,
		OriginalBytes: len(text),
		VisibleBytes:  len(limited),
		OriginalLines: countLines(text),
		VisibleLines:  countLines(limited),
		Strategy:      policy.Strategy,
		ContentSHA256: stableTextHash(text),
	}
	if !truncated {
		return projection, nil
	}
	if policy.PreserveFull {
		if store == nil {
			return OutputProjection{}, errors.New("artifact store is required to preserve truncated tool output")
		}
		ref, err := store.PutToolOutput(ctx, artifact.ToolOutputArtifact{
			RunID:     result.MetadataString("run_id"),
			SessionID: result.MetadataString("session_id"),
			Step:      result.MetadataInt("step"),
			CallID:    result.CallID,
			ToolName:  result.Name,
			Text:      text,
			MIME:      policy.ArtifactMIME,
			Kind:      policy.ArtifactKind,
			Metadata:  result.Metadata,
		})
		if err != nil {
			return OutputProjection{}, err
		}
		projection.FullOutput = &ref
		return projection, nil
	}
	return projection, nil
}

func (r Result) MetadataString(key string) string {
	if r.Metadata == nil {
		return ""
	}
	value, _ := r.Metadata[key].(string)
	return value
}

func (r Result) MetadataInt(key string) int {
	if r.Metadata == nil {
		return 0
	}
	switch value := r.Metadata[key].(type) {
	case int:
		return value
	case int64:
		return int(value)
	case float64:
		return int(value)
	default:
		return 0
	}
}

func limitLines(text string, maxLines int, strategy OutputStrategy) (string, bool) {
	if maxLines <= 0 {
		return text, false
	}
	lines := strings.Split(text, "\n")
	if len(lines) <= maxLines {
		return text, false
	}
	if strategy == OutputHead {
		return strings.Join(lines[:maxLines], "\n"), true
	}
	return strings.Join(lines[len(lines)-maxLines:], "\n"), true
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

func countLines(text string) int {
	if text == "" {
		return 0
	}
	return strings.Count(text, "\n") + 1
}

func stableTextHash(text string) string {
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:])
}

func shortHash(hash string) string {
	if len(hash) <= 12 {
		return hash
	}
	return hash[:12]
}

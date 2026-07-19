package tools

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"unicode/utf8"
)

const (
	DefaultToolVisibleMaxBytes = 64 * 1024
	DefaultToolVisibleMaxLines = 0
	DefaultToolOutputStrategy  = OutputTail
	DefaultPreserveFull        = true
	DefaultArtifactKind        = "tool_output"
	DefaultArtifactMIME        = "text/plain; charset=utf-8"
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
	VisibleText    string
	Truncated      bool
	OriginalBytes  int
	VisibleBytes   int
	OriginalLines  int
	VisibleLines   int
	Strategy       OutputStrategy
	ContentSHA256  string
	FullOutput     *ArtifactRef
	FullOutputPlan *FullOutputPlan
}

// FullOutputPlan is a side-effect-free request to admit the complete tool
// output together with its canonical result. It is not a durable artifact ref.
type FullOutputPlan struct {
	Text string
	Kind string
	MIME string
}

func DefaultOutputPolicy() OutputPolicy {
	return OutputPolicy{
		VisibleMaxBytes: DefaultToolVisibleMaxBytes,
		VisibleMaxLines: DefaultToolVisibleMaxLines,
		Strategy:        DefaultToolOutputStrategy,
		PreserveFull:    DefaultPreserveFull,
		PreserveFullSet: true,
		ArtifactKind:    DefaultArtifactKind,
		ArtifactMIME:    DefaultArtifactMIME,
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
		policy.ArtifactKind = DefaultArtifactKind
	}
	if policy.ArtifactMIME == "" {
		policy.ArtifactMIME = DefaultArtifactMIME
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

func BuildOutputProjection(result Result, policy OutputPolicy) OutputProjection {
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
		return projection
	}
	if policy.PreserveFull {
		projection.FullOutputPlan = &FullOutputPlan{Text: text, Kind: policy.ArtifactKind, MIME: policy.ArtifactMIME}
	}
	return projection
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

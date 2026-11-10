package tools

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
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
	// OutputHeadTail preserves both ends and reports the omitted middle.
	OutputHeadTail OutputStrategy = "head_tail"
)

type OutputPolicy struct {
	// ArchiveAboveBytes independently preserves outputs larger than this threshold.
	// Zero disables threshold-based archival; PreserveFull still controls archival.
	ArchiveAboveBytes int
	VisibleMaxBytes   int
	VisibleMaxLines   int
	Strategy          OutputStrategy
	PreserveFull      bool
	PreserveFullSet   bool
	ArtifactKind      string
	ArtifactMIME      string
	TruncationNotice  string
}

type OutputProjection struct {
	// OutputUnavailable reports a committed retention failure, not tool failure.
	OutputUnavailable string
	VisibleText       string
	Truncated         bool
	OriginalBytes     int
	VisibleBytes      int
	OriginalLines     int
	VisibleLines      int
	Strategy          OutputStrategy
	ContentSHA256     string
	FullOutput        *ArtifactRef
	FullOutputPlan    *FullOutputPlan
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
	if policy.ArchiveAboveBytes < 0 {
		policy.ArchiveAboveBytes = 0
	}
	if policy.VisibleMaxBytes <= 0 {
		policy.VisibleMaxBytes = DefaultToolVisibleMaxBytes
	}
	if policy.VisibleMaxLines < 0 {
		policy.VisibleMaxLines = DefaultToolVisibleMaxLines
	}
	switch policy.Strategy {
	case OutputHead, OutputTail, OutputHeadTail:
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
	if override.ArchiveAboveBytes > 0 {
		out.ArchiveAboveBytes = override.ArchiveAboveBytes
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
	if override.TruncationNotice != "" {
		out.TruncationNotice = override.TruncationNotice
	}
	return NormalizeOutputPolicy(out)
}

func BuildOutputProjection(result Result, policy OutputPolicy) OutputProjection {
	policy = NormalizeOutputPolicy(policy)
	text := result.Text
	limited, truncated := text, false
	if policy.Strategy == OutputHeadTail {
		truncated = len(text) > policy.VisibleMaxBytes || policy.VisibleMaxLines > 0 && countLines(text) > policy.VisibleMaxLines
		if truncated {
			limited = headTailOutput(text, policy.VisibleMaxBytes, policy.VisibleMaxLines, policy.TruncationNotice)
		}
	} else {
		limited, truncated = applyOutputLimits(text, policy.VisibleMaxBytes, policy.VisibleMaxLines, policy.Strategy)
	}
	if truncated && policy.Strategy != OutputHeadTail && strings.TrimSpace(policy.TruncationNotice) != "" {
		limited = outputWithTruncationNotice(text, policy)
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
	if policy.PreserveFull && (truncated || (policy.ArchiveAboveBytes > 0 && len(text) > policy.ArchiveAboveBytes)) {
		projection.FullOutputPlan = &FullOutputPlan{Text: text, Kind: policy.ArtifactKind, MIME: policy.ArtifactMIME}
	}
	if result.FullOutput != nil {
		plan := *result.FullOutput
		if plan.Kind == "" {
			plan.Kind = policy.ArtifactKind
		}
		if plan.MIME == "" {
			plan.MIME = policy.ArtifactMIME
		}
		projection.FullOutputPlan = &plan
		projection.OriginalBytes = len(plan.Text)
		projection.OriginalLines = countLines(plan.Text)
		projection.ContentSHA256 = stableTextHash(plan.Text)
		projection.Truncated = truncated || plan.Text != limited
	}
	return projection
}

func applyOutputLimits(text string, maxBytes, maxLines int, strategy OutputStrategy) (string, bool) {
	if strategy == OutputHeadTail {
		if len(text) <= maxBytes && (maxLines <= 0 || countLines(text) <= maxLines) {
			return text, false
		}
		return headTailOutput(text, maxBytes, maxLines, ""), true
	}
	limited := text
	truncated := false
	if maxLines > 0 {
		next, ok := limitLines(limited, maxLines, strategy)
		if ok {
			limited = next
			truncated = true
		}
	}
	if maxBytes > 0 && len(limited) > maxBytes {
		truncated = true
		if strategy == OutputHead {
			limited = safeHead(limited, maxBytes)
		} else {
			limited = safeTail(limited, maxBytes)
		}
	}
	return limited, truncated
}

// The omission marker participates in both budgets. Byte facts count the
// original UTF-8 source, not tokens or the length of an escaped JSON envelope.
func headTailOutput(text string, maxBytes, maxLines int, notice string) string {
	notice = strings.TrimSpace(notice)
	marker := func(omitted int) string {
		if notice != "" {
			return strings.TrimSpace(notice)
		}
		return fmt.Sprintf("[... %d bytes omitted ...]", omitted)
	}
	reserved := len(marker(len(text))) + 2
	markerLines := countLines(marker(len(text)))
	if maxBytes <= reserved || maxLines > 0 && maxLines < markerLines+2 {
		value := marker(len(text))
		if maxLines > 0 {
			value, _ = limitLines(value, maxLines, OutputHead)
		}
		return safeHead(value, maxBytes)
	}
	budget := maxBytes - reserved
	head := safeHead(text, (budget+1)/2)
	tail := ""
	if budget/2 > 0 {
		tail = safeTail(text, budget/2)
	}
	// Prefer a nearby complete line without discarding most of either half.
	if index := strings.LastIndexByte(head, '\n'); index >= len(head)/2 && index >= 0 {
		head = head[:index]
	}
	if index := strings.IndexByte(tail, '\n'); index >= 0 && index < len(tail)/2 {
		tail = tail[index+1:]
	}
	if maxLines > 0 {
		lines := maxLines - markerLines
		head, _ = limitLines(head, (lines+1)/2, OutputHead)
		tail, _ = limitLines(tail, lines/2, OutputTail)
	}
	head = strings.TrimSuffix(head, "\n")
	tail = strings.TrimPrefix(tail, "\n")
	return head + "\n" + marker(len(text)-len(head)-len(tail)) + "\n" + tail
}

func outputWithTruncationNotice(text string, policy OutputPolicy) string {
	notice := strings.TrimSpace(policy.TruncationNotice)
	if notice == "" {
		limited, _ := applyOutputLimits(text, policy.VisibleMaxBytes, policy.VisibleMaxLines, policy.Strategy)
		return limited
	}
	if noticeOnly, truncated := applyOutputLimits(notice, policy.VisibleMaxBytes, policy.VisibleMaxLines, policy.Strategy); truncated {
		return noticeOnly
	}
	separator := "\n\n"
	contentMaxBytes := policy.VisibleMaxBytes
	if contentMaxBytes > 0 {
		contentMaxBytes -= len(separator) + len(notice)
		if contentMaxBytes <= 0 {
			return notice
		}
	}
	contentMaxLines := policy.VisibleMaxLines
	if contentMaxLines > 0 {
		contentMaxLines -= strings.Count(separator+notice, "\n")
		if contentMaxLines <= 0 {
			return notice
		}
	}
	content, _ := applyOutputLimits(text, contentMaxBytes, contentMaxLines, policy.Strategy)
	if content == "" {
		return notice
	}
	if policy.Strategy == OutputHead {
		return content + separator + notice
	}
	return notice + separator + content
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

package artifact

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

const (
	DefaultKind              = "tool_output"
	DefaultMIME              = "text/plain; charset=utf-8"
	DefaultSafeLabelMaxChars = 80
)

// Ref is the immutable public-safe identity of one Floret-owned artifact.
// It intentionally contains no host route, filesystem path, or product key.
type Ref struct {
	ID        string `json:"id,omitempty"`
	SafeLabel string `json:"safe_label,omitempty"`
	Kind      string `json:"kind,omitempty"`
	MIME      string `json:"mime,omitempty"`
	SizeBytes int64  `json:"size_bytes,omitempty"`
	SHA256    string `json:"sha256,omitempty"`
}

// FullOutput is the immutable full-text payload presented to the artifact
// authority as part of one effect-dispatch terminal commit.
type FullOutput struct {
	Text string `json:"text"`
	Kind string `json:"kind"`
	MIME string `json:"mime"`
}

// Content is the only artifact read result exposed above the storage kernel.
type Content struct {
	Ref  Ref
	Text string
}

// Record is one immutable thread-scoped artifact row. CanonicalEntryID binds
// the payload to the exact tool-result entry admitted in the same transaction.
type Record struct {
	ThreadID         string
	Ref              Ref
	Text             string
	CanonicalEntryID string
	CreatedAt        time.Time
}

// ManifestItem pins one on-path canonical entry/reference/payload relation for
// a root or SubAgent fork copy.
type ManifestItem struct {
	SourceEntryID  string `json:"source_entry_id"`
	ArtifactID     string `json:"artifact_id"`
	Ref            Ref    `json:"ref"`
	RefFingerprint string `json:"ref_fingerprint"`
	PayloadSHA256  string `json:"payload_sha256"`
}

// Closure is the deterministic deduplicated artifact manifest for one exact
// source path and one destination thread.
type Closure struct {
	SourceThreadID      string         `json:"source_thread_id"`
	DestinationThreadID string         `json:"destination_thread_id"`
	Items               []ManifestItem `json:"items"`
	Fingerprint         string         `json:"fingerprint"`
}

// ValidateClosure rejects non-canonical manifests. Callers must bind even an
// empty fork path to its exact source and destination thread identities.
func ValidateClosure(closure Closure) error {
	if strings.TrimSpace(closure.SourceThreadID) == "" || strings.TrimSpace(closure.DestinationThreadID) == "" ||
		closure.SourceThreadID != strings.TrimSpace(closure.SourceThreadID) ||
		closure.DestinationThreadID != strings.TrimSpace(closure.DestinationThreadID) {
		return errors.New("artifact closure requires canonical source and destination threads")
	}
	if closure.Items == nil {
		return errors.New("artifact closure requires a canonical manifest")
	}
	previousID := ""
	for _, item := range closure.Items {
		if strings.TrimSpace(item.SourceEntryID) == "" || item.SourceEntryID != strings.TrimSpace(item.SourceEntryID) ||
			strings.TrimSpace(item.ArtifactID) == "" || item.ArtifactID != strings.TrimSpace(item.ArtifactID) ||
			item.ArtifactID != item.Ref.ID {
			return errors.New("artifact closure manifest item is incomplete")
		}
		if previousID != "" && item.ArtifactID <= previousID {
			return errors.New("artifact closure manifest must be uniquely sorted")
		}
		previousID = item.ArtifactID
		if err := ValidateRef(item.Ref); err != nil {
			return err
		}
		refFingerprint, err := RefFingerprint(item.Ref)
		if err != nil || item.RefFingerprint != refFingerprint || item.PayloadSHA256 != item.Ref.SHA256 {
			return errors.New("artifact closure manifest fingerprint is invalid")
		}
	}
	fingerprint, err := ClosureFingerprint(closure.SourceThreadID, closure.DestinationThreadID, closure.Items)
	if err != nil {
		return err
	}
	if closure.Fingerprint != fingerprint {
		return errors.New("artifact closure fingerprint is invalid")
	}
	return nil
}

func IsZeroClosure(closure Closure) bool {
	return closure.SourceThreadID == "" && closure.DestinationThreadID == "" && closure.Items == nil && closure.Fingerprint == ""
}

func EqualClosure(left, right Closure) bool {
	if left.SourceThreadID != right.SourceThreadID || left.DestinationThreadID != right.DestinationThreadID ||
		left.Fingerprint != right.Fingerprint || len(left.Items) != len(right.Items) ||
		(left.Items == nil) != (right.Items == nil) {
		return false
	}
	for index := range left.Items {
		if left.Items[index] != right.Items[index] {
			return false
		}
	}
	return true
}

// NormalizeFullOutput returns the canonical payload policy values.
func NormalizeFullOutput(in FullOutput) FullOutput {
	in.Kind = strings.TrimSpace(in.Kind)
	if in.Kind == "" {
		in.Kind = DefaultKind
	}
	in.MIME = strings.TrimSpace(in.MIME)
	if in.MIME == "" {
		in.MIME = DefaultMIME
	}
	return in
}

// RefForEffect deterministically derives a thread-scoped reference from the
// Store-assigned effect-attempt identity and immutable payload.
func RefForEffect(effectAttemptID, toolName string, full FullOutput) (Ref, error) {
	effectAttemptID = strings.TrimSpace(effectAttemptID)
	toolName = strings.TrimSpace(toolName)
	if effectAttemptID == "" || toolName == "" {
		return Ref{}, errors.New("artifact reference requires effect attempt and tool name")
	}
	full = NormalizeFullOutput(full)
	hash := TextSHA256(full.Text)
	id := SafeLabel("tool-output-"+effectAttemptID, DefaultSafeLabelMaxChars)
	label := SafeLabel(fmt.Sprintf("%s-%s.log", toolName, effectAttemptID), DefaultSafeLabelMaxChars)
	ref := Ref{
		ID:        id,
		SafeLabel: label,
		Kind:      full.Kind,
		MIME:      full.MIME,
		SizeBytes: int64(len(full.Text)),
		SHA256:    hash,
	}
	if err := ValidateRef(ref); err != nil {
		return Ref{}, err
	}
	return ref, nil
}

func ValidateRef(ref Ref) error {
	if strings.TrimSpace(ref.ID) == "" || strings.TrimSpace(ref.SafeLabel) == "" ||
		strings.TrimSpace(ref.Kind) == "" || strings.TrimSpace(ref.MIME) == "" ||
		ref.SizeBytes < 0 || len(strings.TrimSpace(ref.SHA256)) != 64 {
		return errors.New("artifact reference is incomplete")
	}
	return nil
}

func TextSHA256(text string) string {
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:])
}

func RefFingerprint(ref Ref) (string, error) {
	if err := ValidateRef(ref); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(ref)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

func ClosureFingerprint(sourceThreadID, destinationThreadID string, items []ManifestItem) (string, error) {
	payload := struct {
		SourceThreadID      string         `json:"source_thread_id"`
		DestinationThreadID string         `json:"destination_thread_id"`
		Items               []ManifestItem `json:"items"`
	}{
		SourceThreadID:      strings.TrimSpace(sourceThreadID),
		DestinationThreadID: strings.TrimSpace(destinationThreadID),
		Items:               items,
	}
	if payload.SourceThreadID == "" || payload.DestinationThreadID == "" {
		return "", errors.New("artifact closure requires source and destination threads")
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

func CloneRefPtr(in *Ref) *Ref {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func CloneRecord(in Record) Record { return in }

func CloneManifest(items []ManifestItem) []ManifestItem {
	if items == nil {
		return nil
	}
	out := make([]ManifestItem, len(items))
	copy(out, items)
	return out
}

func CloneClosure(in Closure) Closure {
	in.Items = CloneManifest(in.Items)
	return in
}

var unsafeLabelChars = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

func SafeLabel(value string, maxChars int) string {
	value = strings.TrimSpace(value)
	if value == "" {
		value = "artifact"
	}
	value = unsafeLabelChars.ReplaceAllString(value, "-")
	value = strings.Trim(value, ".-_")
	if value == "" {
		value = "artifact"
	}
	if maxChars <= 0 {
		maxChars = DefaultSafeLabelMaxChars
	}
	runes := []rune(value)
	if len(runes) <= maxChars {
		return value
	}
	return string(runes[:maxChars])
}

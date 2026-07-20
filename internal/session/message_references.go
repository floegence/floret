package session

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

const (
	MaxMessageReferencesPerTurn         = 128
	MaxMessageReferenceIDBytes          = 128
	MaxMessageReferenceLabelRunes       = 256
	MaxMessageReferenceTextRunes        = 12_000
	MaxMessageReferenceResourceRefBytes = 8_192
	MaxMessageReferencesPayloadBytes    = 256 * 1024
)

func (r MessageReference) Validate() error {
	for field, value := range map[string]string{
		"reference id": r.ReferenceID,
		"label":        r.Label,
		"text":         r.Text,
		"resource ref": r.ResourceRef,
	} {
		if !utf8.ValidString(value) {
			return fmt.Errorf("%s must be valid UTF-8", field)
		}
	}
	if r.ReferenceID == "" || r.ReferenceID != strings.TrimSpace(r.ReferenceID) || containsReferenceLineBreak(r.ReferenceID) {
		return errors.New("reference id must be non-empty, trim-stable, and single-line")
	}
	if len(r.ReferenceID) > MaxMessageReferenceIDBytes {
		return fmt.Errorf("reference id exceeds %d bytes", MaxMessageReferenceIDBytes)
	}
	if r.Label == "" || r.Label != strings.TrimSpace(r.Label) || containsReferenceLineBreak(r.Label) {
		return errors.New("reference label must be non-empty, trim-stable, and single-line")
	}
	if utf8.RuneCountInString(r.Label) > MaxMessageReferenceLabelRunes {
		return fmt.Errorf("reference label exceeds %d Unicode characters", MaxMessageReferenceLabelRunes)
	}
	if utf8.RuneCountInString(r.Text) > MaxMessageReferenceTextRunes {
		return fmt.Errorf("reference text exceeds %d Unicode characters", MaxMessageReferenceTextRunes)
	}
	if len(r.ResourceRef) > MaxMessageReferenceResourceRefBytes {
		return fmt.Errorf("reference resource ref exceeds %d bytes", MaxMessageReferenceResourceRefBytes)
	}
	switch r.Kind {
	case MessageReferenceText, MessageReferenceTerminal, MessageReferenceProcess:
		if strings.TrimSpace(r.Text) == "" {
			return fmt.Errorf("%s reference requires text", r.Kind)
		}
		if r.ResourceRef != "" {
			return fmt.Errorf("%s reference cannot contain a resource ref", r.Kind)
		}
	case MessageReferenceFile, MessageReferenceDirectory:
		if strings.TrimSpace(r.ResourceRef) == "" {
			return fmt.Errorf("%s reference requires a resource ref", r.Kind)
		}
	default:
		return fmt.Errorf("unsupported message reference kind %q", r.Kind)
	}
	if r.Truncated && r.Text == "" {
		return errors.New("truncated reference requires display text")
	}
	return nil
}

func ValidateMessageReferences(references []MessageReference) error {
	if len(references) > MaxMessageReferencesPerTurn {
		return fmt.Errorf("message contains %d references, maximum is %d", len(references), MaxMessageReferencesPerTurn)
	}
	seen := make(map[string]struct{}, len(references))
	for index, reference := range references {
		if err := reference.Validate(); err != nil {
			return fmt.Errorf("message reference %d: %w", index, err)
		}
		if _, ok := seen[reference.ReferenceID]; ok {
			return fmt.Errorf("message contains duplicate reference id %q", reference.ReferenceID)
		}
		seen[reference.ReferenceID] = struct{}{}
	}
	raw, err := json.Marshal(references)
	if err != nil {
		return fmt.Errorf("encode message references: %w", err)
	}
	if len(raw) > MaxMessageReferencesPayloadBytes {
		return fmt.Errorf("message reference payload is %d bytes, maximum is %d", len(raw), MaxMessageReferencesPayloadBytes)
	}
	return nil
}

func containsReferenceLineBreak(value string) bool {
	return strings.ContainsAny(value, "\r\n\u0085\u2028\u2029")
}

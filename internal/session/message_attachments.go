package session

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

const (
	MaxMessageAttachmentsPerTurn      = 32
	MaxMessageAttachmentResourceBytes = 16 * 1024
	MaxMessageAttachmentNameRunes     = 1024
	MaxMessageAttachmentMIMETypeBytes = 512
	MaxMessageAttachmentSizeBytes     = 64 * 1024 * 1024
	MaxMessageAttachmentsTotalBytes   = 256 * 1024 * 1024
	MaxMessageAttachmentsPayloadBytes = 512 * 1024
)

func (a MessageAttachment) Validate() error {
	for field, value := range map[string]string{
		"resource ref": a.ResourceRef,
		"name":         a.Name,
		"MIME type":    a.MIMEType,
	} {
		if !utf8.ValidString(value) {
			return fmt.Errorf("message attachment %s must be valid UTF-8", field)
		}
		if value == "" || value != strings.TrimSpace(value) || containsAttachmentControl(value) {
			return fmt.Errorf("message attachment %s must be non-empty, trim-stable, and single-line", field)
		}
	}
	if len(a.ResourceRef) > MaxMessageAttachmentResourceBytes {
		return fmt.Errorf("message attachment resource ref exceeds %d bytes", MaxMessageAttachmentResourceBytes)
	}
	if utf8.RuneCountInString(a.Name) > MaxMessageAttachmentNameRunes {
		return fmt.Errorf("message attachment name exceeds %d Unicode characters", MaxMessageAttachmentNameRunes)
	}
	if len(a.MIMEType) > MaxMessageAttachmentMIMETypeBytes {
		return fmt.Errorf("message attachment MIME type exceeds %d bytes", MaxMessageAttachmentMIMETypeBytes)
	}
	if a.SizeBytes < 0 {
		return errors.New("message attachment size must be non-negative")
	}
	if a.SizeBytes > MaxMessageAttachmentSizeBytes {
		return fmt.Errorf("message attachment size exceeds %d bytes", MaxMessageAttachmentSizeBytes)
	}
	return validateMessageAttachmentTextStats(a.SizeBytes, a.TextStats)
}

func ValidateMessageAttachments(attachments []MessageAttachment) error {
	if len(attachments) > MaxMessageAttachmentsPerTurn {
		return fmt.Errorf("message contains %d attachments, maximum is %d", len(attachments), MaxMessageAttachmentsPerTurn)
	}
	seen := make(map[string]struct{}, len(attachments))
	var totalSize int64
	for index, attachment := range attachments {
		if err := attachment.Validate(); err != nil {
			return fmt.Errorf("message attachment %d: %w", index, err)
		}
		if _, ok := seen[attachment.ResourceRef]; ok {
			return fmt.Errorf("message contains duplicate attachment resource ref %q", attachment.ResourceRef)
		}
		seen[attachment.ResourceRef] = struct{}{}
		if attachment.SizeBytes > MaxMessageAttachmentsTotalBytes-totalSize {
			return fmt.Errorf("message attachment content exceeds %d total bytes", MaxMessageAttachmentsTotalBytes)
		}
		totalSize += attachment.SizeBytes
	}
	raw, err := json.Marshal(attachments)
	if err != nil {
		return fmt.Errorf("encode message attachments: %w", err)
	}
	if len(raw) > MaxMessageAttachmentsPayloadBytes {
		return fmt.Errorf("message attachment payload is %d bytes, maximum is %d", len(raw), MaxMessageAttachmentsPayloadBytes)
	}
	return nil
}

// ValidateStoredMessageAttachments preserves the attachment shape accepted by
// earlier schema-v16 journals. New admission must use ValidateMessageAttachments.
func ValidateStoredMessageAttachments(attachments []MessageAttachment) error {
	for index, attachment := range attachments {
		if strings.TrimSpace(attachment.ResourceRef) == "" || strings.TrimSpace(attachment.Name) == "" || strings.TrimSpace(attachment.MIMEType) == "" {
			return fmt.Errorf("stored message attachment %d requires resource ref, name, and MIME type", index)
		}
		if attachment.SizeBytes < 0 {
			return fmt.Errorf("stored message attachment %d size must be non-negative", index)
		}
		if attachment.TextStats != nil {
			if err := validateMessageAttachmentTextStats(attachment.SizeBytes, attachment.TextStats); err != nil {
				return fmt.Errorf("stored message attachment %d: %w", index, err)
			}
		}
	}
	return nil
}

func validateMessageAttachmentTextStats(sizeBytes int64, stats *MessageAttachmentTextStats) error {
	if stats == nil {
		return nil
	}
	if stats.UnicodeCodePointCount < 0 || stats.LogicalLineCount < 0 {
		return errors.New("message attachment text statistics must be non-negative")
	}
	if sizeBytes == 0 && (stats.UnicodeCodePointCount != 0 || stats.LogicalLineCount != 0) {
		return errors.New("zero-byte message attachment requires empty text statistics")
	}
	if stats.LogicalLineCount == 0 && stats.UnicodeCodePointCount != 0 {
		return errors.New("zero logical lines require zero Unicode code points")
	}
	return nil
}

func CloneMessageAttachment(attachment MessageAttachment) MessageAttachment {
	if attachment.TextStats != nil {
		stats := *attachment.TextStats
		attachment.TextStats = &stats
	}
	return attachment
}

func CloneMessageAttachments(attachments []MessageAttachment) []MessageAttachment {
	if attachments == nil {
		return nil
	}
	out := make([]MessageAttachment, len(attachments))
	for index, attachment := range attachments {
		out[index] = CloneMessageAttachment(attachment)
	}
	return out
}

func containsAttachmentControl(value string) bool {
	return strings.ContainsAny(value, "\r\n\x00\u0085\u2028\u2029")
}

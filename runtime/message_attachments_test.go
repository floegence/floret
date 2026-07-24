package runtime

import (
	"fmt"
	"strings"
	"testing"

	"github.com/floegence/floret/internal/session"
)

func TestPublicMessageAttachmentLimitsMatchInternalAdmissionLimits(t *testing.T) {
	if MaxMessageAttachmentsPerTurn != session.MaxMessageAttachmentsPerTurn ||
		MaxMessageAttachmentResourceRefBytes != session.MaxMessageAttachmentResourceBytes ||
		MaxMessageAttachmentNameRunes != session.MaxMessageAttachmentNameRunes ||
		MaxMessageAttachmentMIMETypeBytes != session.MaxMessageAttachmentMIMETypeBytes ||
		MaxMessageAttachmentSizeBytes != session.MaxMessageAttachmentSizeBytes ||
		MaxMessageAttachmentsTotalSizeBytes != session.MaxMessageAttachmentsTotalBytes ||
		MaxMessageAttachmentsPayloadBytes != session.MaxMessageAttachmentsPayloadBytes {
		t.Fatal("public and internal message attachment limits diverged")
	}
}

func TestTurnInputValidatesMessageAttachmentContract(t *testing.T) {
	valid := MessageAttachment{
		ResourceRef: "host-resource:v1:asset",
		Name:        "notes.txt",
		MIMEType:    "text/plain; charset=utf-8",
		SizeBytes:   12,
		TextStats:   &MessageAttachmentTextStats{UnicodeCodePointCount: 10, LogicalLineCount: 2},
	}
	if err := (TurnInput{Attachments: []MessageAttachment{valid}}).Validate(); err != nil {
		t.Fatalf("valid attachment: %v", err)
	}

	tests := []struct {
		name       string
		attachment MessageAttachment
	}{
		{name: "invalid resource UTF-8", attachment: withAttachment(valid, func(a *MessageAttachment) { a.ResourceRef = string([]byte{0xff}) })},
		{name: "resource whitespace", attachment: withAttachment(valid, func(a *MessageAttachment) { a.ResourceRef = " resource" })},
		{name: "name line break", attachment: withAttachment(valid, func(a *MessageAttachment) { a.Name = "a\nb.txt" })},
		{name: "MIME NUL", attachment: withAttachment(valid, func(a *MessageAttachment) { a.MIMEType = "text/plain\x00" })},
		{name: "resource too long", attachment: withAttachment(valid, func(a *MessageAttachment) {
			a.ResourceRef = strings.Repeat("r", MaxMessageAttachmentResourceRefBytes+1)
		})},
		{name: "name too long", attachment: withAttachment(valid, func(a *MessageAttachment) { a.Name = strings.Repeat("n", MaxMessageAttachmentNameRunes+1) })},
		{name: "MIME too long", attachment: withAttachment(valid, func(a *MessageAttachment) { a.MIMEType = strings.Repeat("m", MaxMessageAttachmentMIMETypeBytes+1) })},
		{name: "negative size", attachment: withAttachment(valid, func(a *MessageAttachment) { a.SizeBytes = -1 })},
		{name: "item too large", attachment: withAttachment(valid, func(a *MessageAttachment) { a.SizeBytes = MaxMessageAttachmentSizeBytes + 1 })},
		{name: "negative code points", attachment: withAttachment(valid, func(a *MessageAttachment) { a.TextStats.UnicodeCodePointCount = -1 })},
		{name: "negative lines", attachment: withAttachment(valid, func(a *MessageAttachment) { a.TextStats.LogicalLineCount = -1 })},
		{name: "zero bytes nonempty stats", attachment: withAttachment(valid, func(a *MessageAttachment) { a.SizeBytes = 0 })},
		{name: "code points without lines", attachment: withAttachment(valid, func(a *MessageAttachment) { a.TextStats.LogicalLineCount = 0 })},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := (TurnInput{Attachments: []MessageAttachment{tc.attachment}}).Validate(); err == nil {
				t.Fatal("expected validation failure")
			}
		})
	}

	for _, attachment := range []MessageAttachment{
		withAttachment(valid, func(a *MessageAttachment) { a.TextStats = nil }),
		withAttachment(valid, func(a *MessageAttachment) {
			a.SizeBytes = 0
			a.TextStats = &MessageAttachmentTextStats{}
		}),
	} {
		if err := attachment.Validate(); err != nil {
			t.Fatalf("valid unknown/empty text stats %#v: %v", attachment, err)
		}
	}
}

func TestTurnInputValidatesMessageAttachmentCollectionLimits(t *testing.T) {
	attachments := make([]MessageAttachment, MaxMessageAttachmentsPerTurn)
	for index := range attachments {
		attachments[index] = MessageAttachment{
			ResourceRef: fmt.Sprintf("resource:%d", index),
			Name:        fmt.Sprintf("file-%d.bin", index),
			MIMEType:    "application/octet-stream",
		}
	}
	if err := (TurnInput{Attachments: attachments}).Validate(); err != nil {
		t.Fatalf("attachments at count limit: %v", err)
	}
	if err := (TurnInput{Attachments: append(attachments, MessageAttachment{ResourceRef: "extra", Name: "extra.bin", MIMEType: "application/octet-stream"})}).Validate(); err == nil {
		t.Fatal("expected count limit failure")
	}

	duplicate := append([]MessageAttachment(nil), attachments[:2]...)
	duplicate[1].ResourceRef = duplicate[0].ResourceRef
	if err := (TurnInput{Attachments: duplicate}).Validate(); err == nil {
		t.Fatal("expected duplicate resource ref failure")
	}

	total := make([]MessageAttachment, 5)
	for index := range total {
		total[index] = MessageAttachment{ResourceRef: fmt.Sprintf("large:%d", index), Name: "large.bin", MIMEType: "application/octet-stream", SizeBytes: MaxMessageAttachmentSizeBytes}
	}
	if err := (TurnInput{Attachments: total[:4]}).Validate(); err != nil {
		t.Fatalf("attachments at total size limit: %v", err)
	}
	if err := (TurnInput{Attachments: total}).Validate(); err == nil {
		t.Fatal("expected total attachment size failure")
	}

	payload := make([]MessageAttachment, MaxMessageAttachmentsPerTurn)
	for index := range payload {
		payload[index] = MessageAttachment{
			ResourceRef: strings.Repeat(string(rune('a'+index%26)), MaxMessageAttachmentResourceRefBytes-16) + fmt.Sprintf(":%015d", index),
			Name:        "file.bin",
			MIMEType:    "application/octet-stream",
		}
	}
	if err := (TurnInput{Attachments: payload}).Validate(); err == nil {
		t.Fatal("expected descriptor payload limit failure")
	}
}

func TestRuntimeModelMessagesProjectsStoredLegacyAttachmentOutsideNewAdmissionLimits(t *testing.T) {
	legacy := session.MessageAttachment{
		ResourceRef: strings.Repeat("r", session.MaxMessageAttachmentResourceBytes+1),
		Name:        strings.Repeat("n", session.MaxMessageAttachmentNameRunes+1),
		MIMEType:    strings.Repeat("m", session.MaxMessageAttachmentMIMETypeBytes+1),
		SizeBytes:   session.MaxMessageAttachmentSizeBytes + 1,
	}
	messages, err := runtimeModelMessages([]session.Message{{Role: session.User, Attachments: []session.MessageAttachment{legacy}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 || len(messages[0].Attachments) != 1 || messages[0].Attachments[0].ResourceRef != legacy.ResourceRef {
		t.Fatalf("legacy attachment projection = %#v", messages)
	}
	if err := messages[0].Validate(); err == nil {
		t.Fatal("public new-request model message validation accepted legacy oversized attachment")
	}
}

func TestNormalizeTurnInputDeepCopiesAttachmentTextStats(t *testing.T) {
	stats := &MessageAttachmentTextStats{UnicodeCodePointCount: 5, LogicalLineCount: 1}
	input := TurnInput{Attachments: []MessageAttachment{{
		ResourceRef: "resource", Name: "file.txt", MIMEType: "text/plain", SizeBytes: 5, TextStats: stats,
	}}}
	normalized, err := normalizeTurnInput(input)
	if err != nil {
		t.Fatal(err)
	}
	stats.UnicodeCodePointCount = 99
	input.Attachments[0].TextStats.LogicalLineCount = 99
	if got := normalized.Attachments[0].TextStats; got.UnicodeCodePointCount != 5 || got.LogicalLineCount != 1 {
		t.Fatalf("normalized attachment aliases caller text stats: %#v", got)
	}
}

func withAttachment(base MessageAttachment, mutate func(*MessageAttachment)) MessageAttachment {
	copy := cloneMessageAttachment(base)
	mutate(&copy)
	return copy
}

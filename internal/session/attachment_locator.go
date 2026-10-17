package session

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
)

// Locator identifies the complete canonical descriptor and conveys no authority.
func (a MessageAttachment) Locator() (string, error) {
	if err := ValidateStoredMessageAttachments([]MessageAttachment{a}); err != nil {
		return "", err
	}
	return a.locator(), nil
}

func (a MessageAttachment) locator() string {
	h := sha256.New()
	h.Write([]byte("floret.message-attachment.v1\x00"))
	for _, value := range []string{a.ResourceRef, a.Name, a.MIMEType} {
		binary.Write(h, binary.BigEndian, uint64(len(value)))
		h.Write([]byte(value))
	}
	binary.Write(h, binary.BigEndian, a.SizeBytes)
	if a.TextStats == nil {
		h.Write([]byte{0})
	} else {
		h.Write([]byte{1})
		binary.Write(h, binary.BigEndian, a.TextStats.UnicodeCodePointCount)
		binary.Write(h, binary.BigEndian, a.TextStats.LogicalLineCount)
	}
	return fmt.Sprintf("media:v1:%x", h.Sum(nil))
}

// AttachmentLabel renders an admitted descriptor without its opaque resource.
func AttachmentLabel(a MessageAttachment) string {
	label, _ := json.Marshal(struct {
		Locator   string `json:"locator"`
		Name      string `json:"name"`
		MIMEType  string `json:"mime_type"`
		SizeBytes int64  `json:"size_bytes"`
	}{a.locator(), a.Name, a.MIMEType, a.SizeBytes})
	return "Attachment: " + string(label)
}

package runtime_test

import (
	"encoding/json"
	. "github.com/floegence/floret/v7/runtime"
	"testing"
)

func TestMessageAttachmentLocatorUsesCompleteStableDescriptor(t *testing.T) {
	original := MessageAttachment{ResourceRef: "upload:private", Name: "same.png", MIMEType: "image/png", SizeBytes: 10}
	want, err := original.Locator()
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(original)
	var restored MessageAttachment
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	if got, err := restored.Locator(); err != nil || got != want {
		t.Fatalf("restart changed locator: %q %v", got, err)
	}
	variants := []MessageAttachment{original, original, original, original, original, original}
	variants[0].ResourceRef += "different"
	variants[1].Name = "different.png"
	variants[2].MIMEType = "image/jpeg"
	variants[3].SizeBytes++
	variants[4].TextStats = &MessageAttachmentTextStats{}
	variants[5].TextStats = &MessageAttachmentTextStats{UnicodeCodePointCount: 3, LogicalLineCount: 1}
	seen := map[string]bool{want: true}
	for _, variant := range variants {
		got, err := variant.Locator()
		if err != nil || seen[got] {
			t.Fatalf("descriptor collision: %q %v", got, err)
		}
		seen[got] = true
	}
	if _, err := (MessageAttachment{}).Locator(); err == nil {
		t.Fatal("invalid descriptor gained locator")
	}
}

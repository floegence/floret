package sessiontree

import (
	"context"
	"testing"
	"time"

	"github.com/floegence/floret/internal/session"
)

func TestMemoryAppendRejectsAttachmentOutsideNewAdmissionLimits(t *testing.T) {
	now := time.Date(2026, 7, 24, 14, 0, 0, 0, time.UTC)
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(context.Background(), ThreadMeta{ID: "thread", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	entry := Entry{
		ThreadID: "thread", Type: EntryUserMessage,
		Message: session.Message{Role: session.User, Attachments: []session.MessageAttachment{{
			ResourceRef: "resource:oversized", Name: "oversized.bin", MIMEType: "application/octet-stream",
			SizeBytes: session.MaxMessageAttachmentSizeBytes + 1,
		}}},
	}
	if _, err := repo.Append(context.Background(), entry, AppendOptions{Now: now}); err == nil {
		t.Fatal("oversized attachment append unexpectedly succeeded")
	}
	entries, err := repo.Entries(context.Background(), "thread")
	if err != nil {
		t.Fatal(err)
	}
	meta, err := repo.Thread(context.Background(), "thread")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 || meta.LeafID != "" {
		t.Fatalf("rejected attachment append mutated thread: entries=%#v meta=%#v", entries, meta)
	}
}

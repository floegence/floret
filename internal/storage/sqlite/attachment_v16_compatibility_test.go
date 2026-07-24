package sqlite

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/sessiontree"
)

func TestSQLiteV16ReopensForksAndRetriesLegacyAttachmentJSON(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "legacy-attachment-v16.db")
	store, err := Open(path, WithAuthorityClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	admitRequest := sessiontree.AdmitTurnRequest{
		ThreadID: "thread", TurnID: "turn-original", RunID: "run-original", OwnerID: "owner-original",
		Input: session.Message{Role: session.User, Attachments: []session.MessageAttachment{{
			ResourceRef: "legacy:placeholder", Name: "placeholder.txt", MIMEType: "text/plain", SizeBytes: 1,
		}}},
		Now: now,
	}
	admitRequest.RequestFingerprint, err = sessiontree.TurnAdmissionRequestFingerprint(admitRequest)
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := store.AdmitTurn(ctx, admitRequest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.FinishTurn(ctx, sessiontree.FinishTurnRequest{
		Lease: admitted.Lease, RunID: admitRequest.RunID, TerminalEntryID: "terminal-original", Status: sessiontree.TurnFailed,
		Metadata:       map[string]string{sessiontree.TurnFailureCodeMetadataKey: sessiontree.TurnFailureProvider},
		FailureMessage: "provider failed", OutcomeFingerprint: "outcome-original", Now: now,
	}); err != nil {
		t.Fatal(err)
	}

	legacyAttachments := make([]session.MessageAttachment, session.MaxMessageAttachmentsPerTurn+1)
	for index := range legacyAttachments {
		legacyAttachments[index] = session.MessageAttachment{
			ResourceRef: fmt.Sprintf("legacy:%02d", index),
			Name:        fmt.Sprintf("legacy-%02d.bin", index),
			MIMEType:    "application/octet-stream",
			SizeBytes:   session.MaxMessageAttachmentSizeBytes + 1,
		}
	}
	legacyAttachments[0].ResourceRef = strings.Repeat("r", session.MaxMessageAttachmentResourceBytes+1)
	legacyAttachments[0].Name = strings.Repeat("n", session.MaxMessageAttachmentNameRunes+1)
	legacyAttachments[0].MIMEType = strings.Repeat("m", session.MaxMessageAttachmentMIMETypeBytes+1)
	legacyUser := admitted.UserMessage
	legacyUser.Message.Attachments = legacyAttachments
	legacyUser = sessiontree.PrepareEntry(legacyUser)
	legacyAdmissionRequest := admitRequest
	legacyAdmissionRequest.Input = session.CloneMessage(legacyUser.Message)
	legacyAdmissionRequest.RequestFingerprint, err = sessiontree.TurnAdmissionRequestFingerprint(legacyAdmissionRequest)
	if err != nil {
		t.Fatal(err)
	}
	messageJSON, err := json.Marshal(legacyUser.Message)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(messageJSON), "text_stats") || strings.Contains(legacyUser.Raw, "text_stats") {
		t.Fatalf("seeded legacy v16 attachment unexpectedly contains text_stats: message=%s raw=%s", messageJSON, legacyUser.Raw)
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	entryUpdate, err := tx.ExecContext(ctx, `UPDATE entries SET message_json = ?, raw = ?, raw_hash = ? WHERE thread_id = ? AND id = ?`,
		string(messageJSON), legacyUser.Raw, legacyUser.RawHash, legacyUser.ThreadID, legacyUser.ID)
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if affected, err := entryUpdate.RowsAffected(); err != nil || affected != 1 {
		_ = tx.Rollback()
		t.Fatalf("legacy entry update affected %d rows: %v", affected, err)
	}
	admissionUpdate, err := tx.ExecContext(ctx, `UPDATE turn_admissions SET request_fingerprint = ? WHERE thread_id = ? AND turn_id = ?`,
		legacyAdmissionRequest.RequestFingerprint, legacyAdmissionRequest.ThreadID, legacyAdmissionRequest.TurnID)
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if affected, err := admissionUpdate.RowsAffected(); err != nil || affected != 1 {
		_ = tx.Rollback()
		t.Fatalf("legacy admission update affected %d rows: %v", affected, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path, WithAuthorityClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	entries, err := reopened.Entries(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	assertLegacyAttachmentEntry := func(t *testing.T, entries []sessiontree.Entry, exactSourceIdentity bool) {
		t.Helper()
		for _, entry := range entries {
			if entry.Type != sessiontree.EntryUserMessage || len(entry.Message.Attachments) != len(legacyAttachments) {
				continue
			}
			if exactSourceIdentity && (entry.ID != legacyUser.ID || entry.Raw != legacyUser.Raw || entry.RawHash != legacyUser.RawHash) {
				t.Fatalf("legacy attachment entry changed: %#v", entry)
			}
			if entry.Message.Attachments[0].TextStats != nil || entry.RawHash != sessiontree.StableHash(entry.Raw) || sessiontree.ValidateEntryIntegrity(entry) != nil {
				t.Fatalf("legacy attachment entry is not compatible: %#v", entry)
			}
			return
		}
		t.Fatal("legacy attachment user entry missing")
	}
	assertLegacyAttachmentEntry(t, entries, true)
	var storedRequestFingerprint string
	if err := reopened.db.QueryRowContext(ctx, `SELECT request_fingerprint FROM turn_admissions WHERE thread_id = ? AND turn_id = ?`,
		legacyAdmissionRequest.ThreadID, legacyAdmissionRequest.TurnID).Scan(&storedRequestFingerprint); err != nil {
		t.Fatal(err)
	}
	if storedRequestFingerprint != legacyAdmissionRequest.RequestFingerprint {
		t.Fatalf("stored legacy admission fingerprint = %q, want %q", storedRequestFingerprint, legacyAdmissionRequest.RequestFingerprint)
	}
	beforeReplay := entries
	replayed, err := reopened.AdmitTurn(ctx, legacyAdmissionRequest)
	if err != nil {
		t.Fatal(err)
	}
	if !replayed.Replayed || replayed.UserMessage.ID != legacyUser.ID || replayed.Terminal == nil || replayed.Terminal.Terminal.ID != "terminal-original" {
		t.Fatalf("legacy attachment exact replay = %#v", replayed)
	}
	afterReplay, err := reopened.Entries(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(afterReplay, beforeReplay) {
		t.Fatalf("legacy attachment exact replay mutated journal: before=%#v after=%#v", beforeReplay, afterReplay)
	}

	if _, err := reopened.Fork(ctx, sessiontree.ForkOptions{SourceThreadID: "thread", NewThreadID: "fork", Now: now.Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	forkEntries, err := reopened.Entries(ctx, "fork")
	if err != nil {
		t.Fatal(err)
	}
	assertLegacyAttachmentEntry(t, forkEntries, false)

	retryRequest := sessiontree.AdmitTurnRequest{
		ThreadID: "thread", TurnID: "turn-retry", RunID: "run-retry", OwnerID: "owner-retry",
		RetrySourceTurnID: "turn-original", RetrySourceEntryID: legacyUser.ID, Now: now.Add(2 * time.Minute),
	}
	retryRequest.RequestFingerprint, err = sessiontree.TurnAdmissionRequestFingerprint(retryRequest)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := reopened.AdmitTurn(ctx, retryRequest)
	if err != nil {
		t.Fatal(err)
	}
	if retry.UserMessage.ID != "" || retry.TurnStarted.Metadata[sessiontree.RetrySourceEntryIDMetadataKey] != legacyUser.ID {
		t.Fatalf("legacy attachment retry admission = %#v", retry)
	}
}

func TestSQLiteAppendRejectsAttachmentOutsideNewAdmissionLimits(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 24, 14, 0, 0, 0, time.UTC)
	store, err := Open(filepath.Join(t.TempDir(), "strict-attachment-admission.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.CreateThread(ctx, sessiontree.ThreadMeta{ID: "thread", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	entry := sessiontree.Entry{
		ThreadID: "thread", Type: sessiontree.EntryUserMessage,
		Message: session.Message{Role: session.User, Attachments: []session.MessageAttachment{{
			ResourceRef: "resource:oversized", Name: "oversized.bin", MIMEType: "application/octet-stream",
			SizeBytes: session.MaxMessageAttachmentSizeBytes + 1,
		}}},
	}
	if _, err := store.Append(ctx, entry, sessiontree.AppendOptions{Now: now}); err == nil {
		t.Fatal("oversized attachment append unexpectedly succeeded")
	}
	entries, err := store.Entries(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	meta, err := store.Thread(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 || meta.LeafID != "" {
		t.Fatalf("rejected attachment append mutated thread: entries=%#v meta=%#v", entries, meta)
	}
}

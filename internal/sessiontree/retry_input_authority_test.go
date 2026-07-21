package sessiontree

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/internal/session"
)

func TestValidateRetrySourcePathRequiresDurableUserInput(t *testing.T) {
	references := []session.MessageReference{{
		ReferenceID: "reference-1", Kind: session.MessageReferenceText, Label: "selected text", Text: "selection",
	}}
	for _, tc := range []struct {
		name      string
		message   session.Message
		savePoint bool
		wantErr   bool
	}{
		{name: "reference-only user", message: session.Message{Role: session.User, References: references}, wantErr: true},
		{name: "reference-only save point", message: session.Message{Role: session.User, References: references}, savePoint: true, wantErr: true},
		{name: "text and references", message: session.Message{Role: session.User, Content: "inspect", References: references}},
		{name: "attachment and references", message: session.Message{Role: session.User, Attachments: []session.MessageAttachment{{ResourceRef: "upload:1", Name: "input.txt", MIMEType: "text/plain"}}, References: references}, savePoint: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := []Entry{
				{ID: "started", ThreadID: "thread", TurnID: "turn", Type: EntryTurnMarker, TurnStatus: TurnStarted},
				{ID: "user", ParentID: "started", ThreadID: "thread", TurnID: "turn", Type: EntryUserMessage, Message: tc.message},
			}
			sourceID := "user"
			if tc.savePoint {
				path = append(path,
					Entry{ID: "assistant", ParentID: "user", ThreadID: "thread", TurnID: "turn", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "partial"}},
					Entry{ID: "save-point", ParentID: "assistant", ThreadID: "thread", TurnID: "turn", Type: EntryTurnMarker, TurnStatus: TurnSavePoint},
				)
				sourceID = "assistant"
			}
			_, err := ValidateRetrySourcePath(path, "turn", sourceID)
			if tc.wantErr && !errors.Is(err, ErrInvalidThreadAuthority) {
				t.Fatalf("ValidateRetrySourcePath error = %v, want ErrInvalidThreadAuthority", err)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("ValidateRetrySourcePath error = %v", err)
			}
		})
	}
}

func TestMemoryReferenceOnlyRetryAdmissionAndProjectionHaveZeroMutation(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 21, 8, 0, 0, 0, time.UTC)
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	original, err := repo.AdmitTurn(ctx, AdmitTurnRequest{
		ThreadID: "thread", TurnID: "turn", RunID: "run", OwnerID: "owner", RequestFingerprint: "request", Now: now,
		Input: session.Message{Role: session.User, References: []session.MessageReference{{
			ReferenceID: "reference-1", Kind: session.MessageReferenceText, Label: "selected text", Text: "selection",
		}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.FinishTurn(ctx, FinishTurnRequest{
		Lease: original.Lease, RunID: "run", TerminalEntryID: "terminal", Status: TurnFailed,
		Metadata: map[string]string{TurnFailureCodeMetadataKey: TurnFailureProvider}, FailureMessage: "provider failed",
		OutcomeFingerprint: "outcome", Now: now,
	}); err != nil {
		t.Fatal(err)
	}
	page, err := repo.ListCanonicalTurns(ctx, ListCanonicalTurnsOptions{ThreadID: "thread", Tail: 1})
	if err != nil || len(page.Turns) != 1 || page.HasRetryTarget {
		t.Fatalf("reference-only canonical page = %#v, err = %v", page, err)
	}

	beforeEntries, err := repo.Entries(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	beforeMeta, err := repo.Thread(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	beforeSeq := repo.seq
	beforeGeneration := repo.leaseGeneration["thread"]
	beforeAdmissions := len(repo.turnAdmissions)
	beforeFinishes := len(repo.turnFinishes)

	_, err = repo.AdmitTurn(ctx, AdmitTurnRequest{
		ThreadID: "thread", TurnID: "retry", RunID: "retry-run", OwnerID: "retry-owner", RequestFingerprint: "retry-request",
		RetrySourceTurnID: "turn", RetrySourceEntryID: original.UserMessage.ID, Now: now,
	})
	if !errors.Is(err, ErrInvalidThreadAuthority) {
		t.Fatalf("reference-only retry admission error = %v, want ErrInvalidThreadAuthority", err)
	}
	afterEntries, entriesErr := repo.Entries(ctx, "thread")
	afterMeta, metaErr := repo.Thread(ctx, "thread")
	if entriesErr != nil || metaErr != nil || !reflect.DeepEqual(afterEntries, beforeEntries) || !reflect.DeepEqual(afterMeta, beforeMeta) ||
		repo.seq != beforeSeq || repo.leaseGeneration["thread"] != beforeGeneration || len(repo.turnAdmissions) != beforeAdmissions ||
		len(repo.turnFinishes) != beforeFinishes || len(repo.leases) != 0 {
		t.Fatalf("rejected retry mutated authority: entries_equal=%v meta_equal=%v seq=%d/%d generation=%d/%d admissions=%d/%d finishes=%d/%d leases=%#v errors=%v/%v",
			reflect.DeepEqual(afterEntries, beforeEntries), reflect.DeepEqual(afterMeta, beforeMeta), repo.seq, beforeSeq,
			repo.leaseGeneration["thread"], beforeGeneration, len(repo.turnAdmissions), beforeAdmissions,
			len(repo.turnFinishes), beforeFinishes, repo.leases, entriesErr, metaErr)
	}
}

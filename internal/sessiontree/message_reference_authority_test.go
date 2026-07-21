package sessiontree

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/internal/session"
)

func TestMessageReferenceAuthorityValidatorsRejectMalformedReferences(t *testing.T) {
	invalid := []session.MessageReference{{
		ReferenceID: "ref-1",
		Kind:        session.MessageReferenceKind("unsupported"),
		Label:       "invalid",
	}}
	message := session.Message{Role: session.User, Content: "inspect", References: invalid}
	now := time.Date(2026, 7, 20, 9, 0, 0, 0, time.UTC)

	tests := map[string]func() error{
		"turn admission": func() error {
			return ValidateAdmitTurnRequest(AdmitTurnRequest{
				ThreadID: "thread", TurnID: "turn", RunID: "run", OwnerID: "owner",
				Input: message, RequestFingerprint: "fingerprint",
			})
		},
		"subagent publication": func() error {
			return validateSubAgentPublicationRequest(PublishSubAgentRequest{
				PublicationID: "publication", RequestFingerprint: "fingerprint", ParentThreadID: "parent",
				ChildMeta: ThreadMeta{
					ID: "child", ParentThreadID: "parent", TaskName: "worker", AgentPath: "/root/worker",
					CreatedAt: now, UpdatedAt: now,
				},
				Message: message,
			})
		},
		"subagent input": func() error {
			return ValidatePublishSubAgentInputRequest(PublishSubAgentInputRequest{
				InputRequestID: "input", RequestFingerprint: "fingerprint", ParentThreadID: "parent", ChildThreadID: "child",
				Message: message,
			})
		},
		"pending tool completion": func() error {
			return ValidateAdmitPendingToolCompletionRequest(AdmitPendingToolCompletionRequest{
				CompletionRequestID: "completion", RequestFingerprint: "fingerprint", SettlementFingerprint: "settlement",
				ContinuationTurnID: "next-turn", ContinuationRunID: "next-run", OwnerID: "owner", Input: message,
			})
		},
		"subagent pending tool completion": func() error {
			return ValidatePublishSubAgentPendingToolCompletionRequest(PublishSubAgentPendingToolCompletionRequest{
				InputRequestID: "input", RequestFingerprint: "fingerprint", SettlementFingerprint: "settlement",
				ParentThreadID: "parent", ChildThreadID: "child", Target: PendingToolSettlementTarget{ThreadID: "child"}, Message: message,
			})
		},
	}

	for name, validate := range tests {
		t.Run(name, func(t *testing.T) {
			err := validate()
			if err == nil || !strings.Contains(err.Error(), "message reference") {
				t.Fatalf("validation error = %v, want malformed message reference", err)
			}
		})
	}
}

func TestMemoryAdmitTurnRejectsMalformedReferencesWithoutMutation(t *testing.T) {
	now := time.Date(2026, 7, 20, 9, 30, 0, 0, time.UTC)
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(context.Background(), ThreadMeta{ID: "thread", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}

	_, err := repo.AdmitTurn(context.Background(), AdmitTurnRequest{
		ThreadID: "thread", TurnID: "turn", RunID: "run", OwnerID: "owner", RequestFingerprint: "fingerprint",
		Input: session.Message{Role: session.User, Content: "inspect", References: []session.MessageReference{{
			ReferenceID: "ref-1", Kind: session.MessageReferenceText, Label: "missing text",
		}}},
	})
	if err == nil || !strings.Contains(err.Error(), "message reference") {
		t.Fatalf("AdmitTurn error = %v, want malformed message reference", err)
	}
	entries, entriesErr := repo.Entries(context.Background(), "thread")
	if entriesErr != nil {
		t.Fatal(entriesErr)
	}
	if len(entries) != 0 || len(repo.turnAdmissions) != 0 || len(repo.leases) != 0 || repo.seq != 0 {
		t.Fatalf("rejected admission mutated authority: entries=%#v admissions=%#v leases=%#v seq=%d", entries, repo.turnAdmissions, repo.leases, repo.seq)
	}
}

func TestValidateAdmitTurnRequestRequiresCompleteRetrySource(t *testing.T) {
	base := AdmitTurnRequest{
		ThreadID: "thread", TurnID: "turn-retry", RunID: "run-retry", OwnerID: "owner", RequestFingerprint: "fingerprint",
	}
	for name, mutate := range map[string]func(*AdmitTurnRequest){
		"missing source entry": func(req *AdmitTurnRequest) { req.RetrySourceTurnID = "turn-original" },
		"missing source turn":  func(req *AdmitTurnRequest) { req.RetrySourceEntryID = "entry-original" },
		"same source turn": func(req *AdmitTurnRequest) {
			req.RetrySourceTurnID = req.TurnID
			req.RetrySourceEntryID = "entry-original"
		},
	} {
		t.Run(name, func(t *testing.T) {
			req := base
			mutate(&req)
			if err := ValidateAdmitTurnRequest(req); err == nil {
				t.Fatal("invalid retry source was accepted")
			}
		})
	}
}

func TestMemoryAppendRestrictsReferencesToValidUserMessagesWithoutMutation(t *testing.T) {
	now := time.Date(2026, 7, 20, 10, 0, 0, 0, time.UTC)
	valid := []session.MessageReference{{ReferenceID: "ref-1", Kind: session.MessageReferenceText, Label: "quote", Text: "selected"}}
	tests := map[string]Entry{
		"malformed user reference": {
			ThreadID: "thread", Type: EntryUserMessage,
			Message: session.Message{Role: session.User, Content: "inspect", References: []session.MessageReference{{
				ReferenceID: "ref-1", Kind: session.MessageReferenceText, Label: "missing text",
			}}},
		},
		"assistant reference": {
			ThreadID: "thread", Type: EntryAssistantMessage,
			Message: session.Message{Role: session.Assistant, Content: "answer", References: valid},
		},
	}

	for name, entry := range tests {
		t.Run(name, func(t *testing.T) {
			repo := NewMemoryRepo()
			if _, err := repo.CreateThread(context.Background(), ThreadMeta{ID: "thread", CreatedAt: now, UpdatedAt: now}); err != nil {
				t.Fatal(err)
			}
			if _, err := repo.Append(context.Background(), entry, AppendOptions{Now: now}); err == nil || !strings.Contains(err.Error(), "message reference") {
				t.Fatalf("Append error = %v, want message reference rejection", err)
			}
			entries, err := repo.Entries(context.Background(), "thread")
			if err != nil {
				t.Fatal(err)
			}
			meta, err := repo.Thread(context.Background(), "thread")
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 0 || meta.LeafID != "" || repo.seq != 0 {
				t.Fatalf("rejected append mutated authority: entries=%#v meta=%#v seq=%d", entries, meta, repo.seq)
			}
		})
	}
}

package testui

import (
	"context"
	"reflect"
	"testing"

	"github.com/floegence/floret/config"
	"github.com/floegence/floret/internal/session"
	"github.com/floegence/floret/internal/sessiontree"
	flruntime "github.com/floegence/floret/runtime"
)

func TestObservedSessionMessagesPreserveCanonicalAttachmentsAndReferences(t *testing.T) {
	attachments := []session.MessageAttachment{{
		ResourceRef: "upload:asset", Name: "report.txt", MIMEType: "text/plain", SizeBytes: 42,
	}}
	references := []session.MessageReference{
		{ReferenceID: "quote-1", Kind: session.MessageReferenceText, Label: "Selection", Text: "selected text"},
		{ReferenceID: "file-1", Kind: session.MessageReferenceFile, Label: "main.go", Text: "/workspace/main.go", ResourceRef: "host:v1:file", Truncated: true},
	}

	observed := observeMessages([]session.Message{{
		Role: session.User, Content: "review", Attachments: attachments, References: references,
	}})
	if len(observed) != 1 || !reflect.DeepEqual(observed[0].Attachments, attachments) || !reflect.DeepEqual(observed[0].References, references) {
		t.Fatalf("observed message = %#v", observed)
	}
	if public := publicObservedMessage(observed[0]); !reflect.DeepEqual(public.Attachments, attachments) || !reflect.DeepEqual(public.References, references) {
		t.Fatalf("public observed message lost canonical references: %#v", public)
	}

	attachments[0].Name = "mutated"
	references[0].Label = "mutated"
	if observed[0].Attachments[0].Name != "report.txt" || observed[0].References[0].Label != "Selection" {
		t.Fatalf("observed message aliases mutable input: %#v", observed[0])
	}
}

func TestRuntimeDetailMessagePreservesCanonicalReferencesForObservation(t *testing.T) {
	detail := flruntime.ThreadDetailEvent{
		ID: "entry", ThreadID: "thread", TurnID: "turn", Kind: flruntime.ThreadDetailEventUserMessage,
		Message: &flruntime.ThreadDetailMessage{
			Role: "user", Content: "inspect",
			Attachments: []flruntime.MessageAttachment{{
				ResourceRef: "upload:asset", Name: "report.txt", MIMEType: "text/plain", SizeBytes: 42,
			}},
			References: []flruntime.MessageReference{{
				ReferenceID: "terminal-1", Kind: flruntime.MessageReferenceTerminal,
				Label: "Build output", Text: "go test ./...", Truncated: true,
			}},
		},
	}

	entry := sessionEntryFromRuntimeDetail(detail)
	observed := observeEntryMessage(entry.Message)
	if len(observed.Attachments) != 1 || observed.Attachments[0].ResourceRef != "upload:asset" ||
		len(observed.References) != 1 || observed.References[0].ReferenceID != "terminal-1" ||
		observed.References[0].Kind != session.MessageReferenceTerminal || !observed.References[0].Truncated {
		t.Fatalf("runtime detail observation = %#v", observed)
	}
}

func TestRunnerPreservesAllCanonicalReferenceKindsAcrossObservationAndReopen(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	runner := NewRunner(root)
	runner.Now = fixedClock()

	created, err := runner.CreateIdleAgentSession(ctx, AgentRunRequest{
		Profile:      ProviderProfile{ID: "fake", Name: "Fake", Provider: config.ProviderFake, Model: "fake-model"},
		SystemPrompt: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	sess, ok := runner.Sessions.get(created.ID)
	if !ok {
		t.Fatal("session not registered")
	}
	references := []flruntime.MessageReference{
		{ReferenceID: "reference:0", Kind: flruntime.MessageReferenceText, Label: "Selected text", Text: "the selected text"},
		{ReferenceID: "reference:1", Kind: flruntime.MessageReferenceFile, Label: "main.go", Text: "/workspace/main.go", ResourceRef: "host-resource:v1:file", Truncated: true},
		{ReferenceID: "reference:2", Kind: flruntime.MessageReferenceDirectory, Label: "workspace", Text: "/workspace", ResourceRef: "host-resource:v1:directory"},
		{ReferenceID: "reference:3", Kind: flruntime.MessageReferenceTerminal, Label: "Terminal selection", Text: "go test ./...\nPASS", Truncated: true},
		{ReferenceID: "reference:4", Kind: flruntime.MessageReferenceProcess, Label: "Server process", Text: "pid 42 running"},
	}
	if _, err := sess.turn.RunTurn(ctx, flruntime.RunTurnRequest{
		RunID:    "run-references",
		ThreadID: flruntime.ThreadID(created.ID),
		TurnID:   "turn-references",
		Input: flruntime.TurnInput{
			Text:       "inspect the referenced context",
			References: references,
		},
		Limits: flruntime.TurnLimits{MaxCostUSD: 1},
	}); err != nil {
		t.Fatal(err)
	}
	expected := []session.MessageReference{
		{ReferenceID: "reference:0", Kind: session.MessageReferenceText, Label: "Selected text", Text: "the selected text"},
		{ReferenceID: "reference:1", Kind: session.MessageReferenceFile, Label: "main.go", Text: "/workspace/main.go", ResourceRef: "host-resource:v1:file", Truncated: true},
		{ReferenceID: "reference:2", Kind: session.MessageReferenceDirectory, Label: "workspace", Text: "/workspace", ResourceRef: "host-resource:v1:directory"},
		{ReferenceID: "reference:3", Kind: session.MessageReferenceTerminal, Label: "Terminal selection", Text: "go test ./...\nPASS", Truncated: true},
		{ReferenceID: "reference:4", Kind: session.MessageReferenceProcess, Label: "Server process", Text: "pid 42 running"},
	}

	assertSnapshot := func(t *testing.T, snapshot AgentSessionSnapshot) {
		t.Helper()
		assertEntryReferences(t, snapshot.PathEntries, expected)
		assertMessageReferences(t, snapshot.Observation.SessionMessages, expected)
		assertMessageReferences(t, snapshot.ActiveContext, expected)
		assertMessageReferences(t, snapshot.ContextProjection.Messages, expected)
		assertMessageReferences(t, snapshot.Observation.ActiveContext, expected)
		assertMessageReferences(t, snapshot.Observation.ContextProjection.Messages, expected)
	}

	snapshot, err := runner.AgentSession(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertSnapshot(t, snapshot)
	if err := runner.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := NewRunner(root)
	reopened.Now = fixedClock()
	t.Cleanup(func() { _ = reopened.Close() })
	snapshot, err = reopened.AgentSession(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertSnapshot(t, snapshot)
}

func assertEntryReferences(t *testing.T, entries []ObservedSessionEntry, expected []session.MessageReference) {
	t.Helper()
	for _, entry := range entries {
		if entry.Type == sessiontree.EntryUserMessage && entry.Message.Content == "inspect the referenced context" {
			if !reflect.DeepEqual(entry.Message.References, expected) {
				t.Fatalf("user entry references = %#v, want %#v", entry.Message.References, expected)
			}
			return
		}
	}
	t.Fatalf("user entry missing from observations: %#v", entries)
}

func assertMessageReferences(t *testing.T, messages []ObservedSessionMessage, expected []session.MessageReference) {
	t.Helper()
	for _, message := range messages {
		if message.Role == string(session.User) && message.Content == "inspect the referenced context" {
			if !reflect.DeepEqual(message.References, expected) {
				t.Fatalf("user message references = %#v, want %#v", message.References, expected)
			}
			return
		}
	}
	t.Fatalf("user message missing from observations: %#v", messages)
}

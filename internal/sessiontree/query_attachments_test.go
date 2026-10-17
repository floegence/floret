package sessiontree

import (
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/threadquery"
)

func TestCanonicalQueryDiscoversUserAndConfirmedToolAttachments(t *testing.T) {
	repo := NewMemoryRepo()
	queryFixture(t, repo)
	image := session.MessageAttachment{ResourceRef: "private-original", Name: "receipt.png", MIMEType: "image/png", SizeBytes: 12}
	at := time.Now()
	appendQueryFixture(t, repo, "a", Entry{ID: "image-user", Type: EntryUserMessage, TurnID: "image-turn", RunID: "image-run", Message: session.Message{Role: session.User, Attachments: []session.MessageAttachment{image}}}, at)
	appendQueryFixture(t, repo, "a", Entry{ID: "image-call", Type: EntryToolCall, TurnID: "image-turn", RunID: "image-run", Message: session.Message{Role: session.Assistant, ToolName: "image.read", ToolCallID: "image-1", ToolArgs: "{}"}}, at)
	appendQueryFixture(t, repo, "a", Entry{ID: "image-result", Type: EntryToolResult, TurnID: "image-turn", RunID: "image-run", Message: session.Message{Role: session.Tool, ToolName: "image.read", ToolCallID: "image-1", ToolResult: &session.ToolResultView{Status: "success", Attachments: []session.MessageAttachment{image}}}}, at)
	locator, _ := image.Locator()
	for _, object := range []threadquery.Object{threadquery.Messages, threadquery.ToolCalls} {
		for _, term := range []string{image.Name, image.MIMEType, locator} {
			page, err := repo.Query(t.Context(), queryScope("a"), threadquery.QueryInput{Object: object, Filter: threadquery.Filter{Text: threadquery.TextMatch{Contains: term}}})
			if err != nil || len(page.Records) != 1 {
				t.Fatalf("%s/%s: %+v %v", object, term, page, err)
			}
			read, err := repo.Read(t.Context(), queryScope("a"), threadquery.ReadInput{Ref: page.Records[0].Ref})
			if err != nil {
				t.Fatal(err)
			}
			var text strings.Builder
			for _, chunk := range read.Chunks {
				text.WriteString(chunk.Text)
			}
			if !strings.Contains(text.String(), locator) || strings.Contains(text.String(), image.ResourceRef) {
				t.Fatalf("unsafe or missing attachment label: %s", text.String())
			}
		}
	}
	denied, err := repo.Query(t.Context(), queryScope("b"), threadquery.QueryInput{Object: threadquery.Messages, Filter: threadquery.Filter{Text: threadquery.TextMatch{Contains: locator}}})
	if err != nil || len(denied.Records) != 0 {
		t.Fatalf("locator granted cross-thread access: %+v %v", denied, err)
	}
}

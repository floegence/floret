package event

import (
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/floegence/floret/v5/identity"
	"github.com/floegence/floret/v5/tools"
)

func TestSanitizeActivityPresentationPreservesEveryTypedPayload(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		renderer tools.ActivityRenderer
		payload  tools.ActivityPayload
	}{
		{name: "structured", renderer: tools.ActivityRendererStructured, payload: tools.StructuredActivityPayload{}},
		{name: "terminal", renderer: tools.ActivityRendererTerminal, payload: tools.TerminalActivityPayload{}},
		{name: "file", renderer: tools.ActivityRendererFile, payload: tools.FileActivityPayload{}},
		{name: "patch", renderer: tools.ActivityRendererPatch, payload: tools.PatchActivityPayload{}},
		{name: "web search", renderer: tools.ActivityRendererWebSearch, payload: tools.WebSearchActivityPayload{}},
		{name: "web fetch", renderer: tools.ActivityRendererWebFetch, payload: tools.WebFetchActivityPayload{}},
		{name: "todos", renderer: tools.ActivityRendererTodos, payload: tools.TodosActivityPayload{}},
		{name: "question", renderer: tools.ActivityRendererQuestion, payload: tools.QuestionActivityPayload{}},
		{name: "completion", renderer: tools.ActivityRendererCompletion, payload: tools.CompletionActivityPayload{}},
		{name: "subagent", renderer: tools.ActivityRendererSubAgent, payload: tools.SubAgentActivityPayload{
			ThreadID: identity.ThreadID("thread-child"), ParentThreadID: identity.ThreadID("thread-parent"), Status: "running",
		}},
		{name: "subagent operation", renderer: tools.ActivityRendererSubAgentOperation, payload: tools.SubAgentOperationActivityPayload{
			Action: tools.SubAgentOperationWait, Status: "running", RequestedCount: 1,
			Targets: []tools.SubAgentOperationTarget{{ThreadID: identity.ThreadID("thread-child"), TaskName: "research"}},
		}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := Sanitize(Event{Activity: &tools.ActivityPresentation{Renderer: test.renderer, Payload: test.payload}})
			if got.Activity == nil || got.Activity.Payload == nil || reflect.TypeOf(got.Activity.Payload) != reflect.TypeOf(test.payload) {
				t.Fatalf("payload %T was not preserved: %#v", test.payload, got.Activity)
			}
		})
	}
}

func TestSanitizeWebFetchActivityPreservesBoundedPreviewAndIcon(t *testing.T) {
	t.Parallel()

	iconData := []byte{'\x89', 'P', 'N', 'G', '\r', '\n', '\x1a', '\n'}
	got := Sanitize(Event{Activity: &tools.ActivityPresentation{
		Label:    "Web fetch · https://example.com/page",
		Renderer: tools.ActivityRendererWebFetch,
		Payload: tools.WebFetchActivityPayload{
			URL: "https://example.com/page", FinalURL: "https://example.com/final", Status: "success",
			StatusCode: 200, ContentType: "text/html", Format: "markdown", ContentPreview: "# Preview",
			PreviewTruncated: true, SiteIcon: &tools.WebFetchActivityIcon{ContentType: "image/png", Data: iconData},
			BytesRead: 42, Truncated: true,
		},
	}})
	if got.Activity == nil {
		t.Fatal("web fetch activity was dropped")
	}
	payload, ok := got.Activity.Payload.(tools.WebFetchActivityPayload)
	if !ok || payload.URL != "https://example.com/page" || payload.FinalURL != "https://example.com/final" || payload.StatusCode != 200 || payload.ContentPreview != "# Preview" || !payload.PreviewTruncated || payload.SiteIcon == nil || payload.SiteIcon.Data[0] != '\x89' || payload.BytesRead != 42 || !payload.Truncated {
		t.Fatalf("web fetch payload = %#v", got.Activity.Payload)
	}
	iconData[0] = 'X'
	if payload.SiteIcon.Data[0] != '\x89' {
		t.Fatal("sanitized web fetch icon shares input bytes")
	}
}

func TestSanitizeActivityPresentationNormalizesInvalidUTF8(t *testing.T) {
	invalid := "valid prefix " + string([]byte{0xe8, 0xa2})
	got := Sanitize(Event{Activity: &tools.ActivityPresentation{
		Renderer: tools.ActivityRendererTerminal,
		Payload:  tools.TerminalActivityPayload{Output: invalid},
	}})
	if got.Activity == nil {
		t.Fatal("typed activity was dropped")
	}
	payload, ok := got.Activity.Payload.(tools.TerminalActivityPayload)
	if !ok || !utf8.ValidString(payload.Output) || !strings.Contains(payload.Output, "\uFFFD") {
		t.Fatalf("terminal payload=%#v, want valid UTF-8 replacement", got.Activity.Payload)
	}
}

func TestSanitizeActivityPresentationPreservesTypedNumbers(t *testing.T) {
	exitCode := 7
	got := Sanitize(Event{Activity: &tools.ActivityPresentation{
		Renderer: tools.ActivityRendererTerminal,
		Payload:  tools.TerminalActivityPayload{ExitCode: &exitCode, DurationMS: 1500},
	}})
	if got.Activity == nil {
		t.Fatal("typed activity was dropped")
	}
	payload, ok := got.Activity.Payload.(tools.TerminalActivityPayload)
	if !ok || payload.ExitCode == nil || *payload.ExitCode != 7 || payload.DurationMS != 1500 {
		t.Fatalf("typed numeric activity payload was not preserved: %#v", got.Activity)
	}
}

func TestSanitizePatchActivityDetachesMutations(t *testing.T) {
	t.Parallel()

	mutations := tools.FileMutationActivityPayloads{{
		DisplayName: "app.ts", ChangeType: "update", Additions: 1,
		UnifiedDiff: "@@ -0,0 +1 @@\n+new\n",
	}}
	input := &tools.ActivityPresentation{
		Renderer: tools.ActivityRendererPatch,
		Payload:  tools.PatchActivityPayload{Mutations: &mutations},
	}
	got := Sanitize(Event{Activity: input})
	if got.Activity == nil {
		t.Fatal("patch activity was dropped")
	}
	payload, ok := got.Activity.Payload.(tools.PatchActivityPayload)
	if !ok || payload.Mutations == nil || len(*payload.Mutations) != 1 {
		t.Fatalf("patch payload = %#v", got.Activity.Payload)
	}
	mutations[0].DisplayName = "changed.ts"
	if (*payload.Mutations)[0].DisplayName != "app.ts" {
		t.Fatal("sanitized patch mutations share input storage")
	}
}

func TestSafePathRefsTextSanitizesLocalPathsAndKeepsURLs(t *testing.T) {
	path := "/Users/alice/work/floret/secret.txt"
	homePath := "~/work/floret/secret.txt"
	windowsPath := `C:\Users\alice\work\secret.txt`
	got := SafePathRefsText("read " + path + " and " + homePath + " and " + windowsPath + " then open https://example.com/docs/path and /artifacts/session/run/output.txt")
	if strings.Contains(got, path) {
		t.Fatalf("local path was not sanitized: %q", got)
	}
	if !strings.Contains(got, SafePathLabel(path)) {
		t.Fatalf("safe label missing from sanitized text: %q", got)
	}
	if strings.Contains(got, homePath) || !strings.Contains(got, SafePathLabel(homePath)) {
		t.Fatalf("home path was not sanitized: %q", got)
	}
	if strings.Contains(got, windowsPath) || !strings.Contains(got, SafePathLabel(windowsPath)) {
		t.Fatalf("windows path was not sanitized: %q", got)
	}
	if !strings.Contains(got, "https://example.com/docs/path") {
		t.Fatalf("URL should remain inspectable: %q", got)
	}
	if !strings.Contains(got, "/artifacts/session/run/output.txt") {
		t.Fatalf("artifact route should remain usable: %q", got)
	}
}

func TestSafePathRefsTextKeepsRepositoryNamesAndSlashSeparatedText(t *testing.T) {
	input := "Compare HeyPuter/puter, linuxserver/docker-webtop, and Ubuntu/Alpine/Arch/Fedora."
	if got := SafePathRefsText(input); got != input {
		t.Fatalf("SafePathRefsText(%q) = %q, want unchanged", input, got)
	}
}

func TestSanitizeActivityPresentationRedactsPathsAndSecrets(t *testing.T) {
	path := "/Users/alice/work/floret/secret.txt"
	got := Sanitize(Event{
		Type: ToolResult,
		Activity: &tools.ActivityPresentation{
			Label:       "cat " + path,
			Description: "token sk-test-secret",
			Renderer:    tools.ActivityRendererTerminal,
			Chips:       []tools.ActivityChip{{Kind: "effect", Label: "shell"}},
			TargetRefs:  []tools.ActivityTargetRef{{Kind: "file", Label: path, Path: path}},
			Payload: tools.TerminalActivityPayload{
				Command: "cat " + path,
				Stdout:  "token sk-test-secret",
			},
		},
	})
	if got.Activity == nil {
		t.Fatalf("activity missing after sanitize")
	}
	payload, ok := got.Activity.Payload.(tools.TerminalActivityPayload)
	if !ok {
		t.Fatalf("terminal payload type = %T", got.Activity.Payload)
	}
	data := strings.Join([]string{got.Activity.Label, got.Activity.Description, got.Activity.TargetRefs[0].Label, got.Activity.TargetRefs[0].Path, payload.Command, payload.Stdout}, "\n")
	if strings.Contains(data, path) {
		t.Fatalf("activity still contains raw path: %#v", got.Activity)
	}
	if strings.Contains(data, "sk-test-secret") {
		t.Fatalf("activity still contains secret: %#v", got.Activity)
	}
	if !strings.Contains(got.Activity.TargetRefs[0].Path, SafePathLabel(path)) {
		t.Fatalf("activity target path missing safe path label: %#v", got.Activity)
	}
}

func TestSanitizeQuestionActivityAnswersRedactsPathsAndSecrets(t *testing.T) {
	path := "/Users/alice/work/floret/secret.txt"
	got := Sanitize(Event{Activity: &tools.ActivityPresentation{
		Renderer: tools.ActivityRendererQuestion,
		Payload: tools.QuestionActivityPayload{
			Answers: []tools.QuestionActivityAnswer{
				{QuestionID: "target", Values: []string{"open " + path, "token sk-test-secret"}},
				{QuestionID: "credential", Redacted: true},
			},
		},
	}})
	if got.Activity == nil {
		t.Fatal("activity missing after sanitize")
	}
	payload, ok := got.Activity.Payload.(tools.QuestionActivityPayload)
	if !ok || len(payload.Answers) != 2 || !payload.Answers[1].Redacted {
		t.Fatalf("question payload = %#v", got.Activity.Payload)
	}
	data := strings.Join(payload.Answers[0].Values, "\n")
	if strings.Contains(data, path) || strings.Contains(data, "sk-test-secret") {
		t.Fatalf("question answers were not sanitized: %#v", payload.Answers)
	}
}

func TestSanitizePathRefsCoversRawEventStrings(t *testing.T) {
	path := "/Users/alice/work/floret/secret.txt"
	got := SanitizePathRefs(Event{
		Message: "message " + path,
		Args:    `{"path":"` + path + `"}`,
		Result:  "result " + path,
		Err:     "err " + path,
	})
	for name, value := range map[string]string{
		"message": got.Message,
		"args":    got.Args,
		"result":  got.Result,
		"err":     got.Err,
	} {
		if strings.Contains(value, path) {
			t.Fatalf("%s still contains local path: %q", name, value)
		}
	}
}

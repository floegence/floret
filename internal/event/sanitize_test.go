package event

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/floegence/floret/v5/tools"
)

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

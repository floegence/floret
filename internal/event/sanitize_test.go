package event

import (
	"strings"
	"testing"

	"github.com/floegence/floret/observation"
)

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
		Activity: &observation.ActivityPresentation{
			Label:       "cat " + path,
			Description: "token sk-test-secret",
			Renderer:    observation.ActivityRendererTerminal,
			Chips:       []observation.ActivityChip{{Kind: "effect", Label: "shell"}},
			TargetRefs:  []observation.ActivityTargetRef{{Kind: "file", Label: path, Path: path}},
			Payload: map[string]any{
				"command": "cat " + path,
				"stdout":  "token sk-test-secret",
				"items": []any{map[string]any{
					"path": path,
					"url":  "https://example.test/docs",
				}},
			},
		},
	})
	if got.Activity == nil {
		t.Fatalf("activity missing after sanitize")
	}
	data := got.Activity.Label + "\n" + got.Activity.Description + "\n" + got.Activity.TargetRefs[0].Label + "\n" + got.Activity.TargetRefs[0].Path + "\n"
	for _, value := range got.Activity.Payload {
		data += strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(strings.TrimSpace(anyString(value)), "\\n", " "), "\\t", " "))
	}
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

func anyString(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case []any:
		var out string
		for _, item := range v {
			out += anyString(item)
		}
		return out
	case map[string]any:
		var out string
		for _, item := range v {
			out += anyString(item)
		}
		return out
	default:
		return ""
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

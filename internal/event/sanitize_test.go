package event

import (
	"strings"
	"testing"
)

func TestSafePathRefsTextSanitizesLocalPathsAndKeepsURLs(t *testing.T) {
	path := "/Users/alice/work/floret/secret.txt"
	got := SafePathRefsText("read " + path + " then open https://example.com/docs/path and /artifacts/session/run/output.txt")
	if strings.Contains(got, path) {
		t.Fatalf("local path was not sanitized: %q", got)
	}
	if !strings.Contains(got, SafePathLabel(path)) {
		t.Fatalf("safe label missing from sanitized text: %q", got)
	}
	if !strings.Contains(got, "https://example.com/docs/path") {
		t.Fatalf("URL should remain inspectable: %q", got)
	}
	if !strings.Contains(got, "/artifacts/session/run/output.txt") {
		t.Fatalf("artifact route should remain usable: %q", got)
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

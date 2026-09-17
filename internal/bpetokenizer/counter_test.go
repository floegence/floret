package bpetokenizer

import (
	"github.com/dlclark/regexp2"
	"strings"
	"testing"
	"time"
)

func TestTimeoutDoesNotExposeInput(t *testing.T) {
	pattern := regexp2.MustCompile(`(x+)+y`, regexp2.None)
	pattern.MatchTimeout = time.Millisecond
	c := Counter{patterns: []*regexp2.Regexp{pattern}}
	_, err := c.count([]rune("private-context-marker"+strings.Repeat("x", 80)), 0)
	if err == nil {
		t.Fatal("expected bounded timeout")
	}
	if strings.Contains(err.Error(), "private-context-marker") || strings.Contains(err.Error(), "xxxx") {
		t.Fatal("timeout exposed input")
	}
}

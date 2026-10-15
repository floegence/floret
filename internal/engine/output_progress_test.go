package engine

import (
	"errors"
	"testing"

	"github.com/floegence/floret/v7/tools"
)

func TestOutputEstimateIsIndependentOfChunkPartition(t *testing.T) {
	for _, text := range []string{"abcd", "中文和代码 x := 1\n", "🙂👩‍💻", `{"content":"hello\n世界"}`} {
		var whole outputEstimate
		whole.append("text", text)
		for split := 0; split <= len(text); split++ {
			var fragmented outputEstimate
			fragmented.append("text", text[:split])
			fragmented.append("other", "abcd")
			fragmented.append("text", text[split:])
			if fragmented.tokens() != whole.tokens()+1 {
				t.Fatalf("partition %d of %q: %d != %d", split, text, fragmented.tokens(), whole.tokens()+1)
			}
		}
	}
}

func TestArgumentPreviewFailuresAreIsolated(t *testing.T) {
	for _, parser := range []tools.ArgumentPreviewer{
		func(string, bool) (*tools.ArgumentPreview, error) { return nil, errors.New("secret arguments") },
		func(string, bool) (*tools.ArgumentPreview, error) { panic("secret arguments") },
	} {
		call := &argumentPreviewCall{parser: parser}
		preview, updated, failed := call.append("secret", false)
		if preview != nil || !updated || !failed || !call.disabled {
			t.Fatal("failed preview was not discarded")
		}
		if preview, _, failed := call.append("more", true); preview != nil || !failed {
			t.Fatal("failed parser restarted")
		}
	}
}

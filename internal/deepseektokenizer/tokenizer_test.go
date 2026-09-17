package deepseektokenizer

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// Fixtures come from the official tokenizer.json with tokenizers 0.23.2,
// add_special_tokens=false. They cover all three pre-tokenizer stages.
func TestOfficialReference(t *testing.T) {
	raw, err := os.ReadFile("reference_test.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures []struct {
		Text   string
		Tokens int64
	}
	if err := json.Unmarshal(raw, &fixtures); err != nil {
		t.Fatal(err)
	}
	for _, f := range fixtures {
		n, err := Count(f.Text)
		if err != nil || n != f.Tokens {
			t.Errorf("%q: got %d, %v; want %d", f.Text, n, err, f.Tokens)
		}
	}
}
func TestLongInput(t *testing.T) {
	for _, value := range []string{strings.Repeat("A", 1<<20), strings.Repeat("上下文", 10000)} {
		n, err := Count(value)
		if err != nil || n <= 0 || n >= int64(len(value)) {
			t.Fatalf("count=%d err=%v bytes=%d", n, err, len(value))
		}
	}
}
func BenchmarkRequest(b *testing.B) {
	text := strings.Repeat("Review the following code: func main() { fmt.Println(1234) }\n", 5000)
	for b.Loop() {
		if _, err := Count(text); err != nil {
			b.Fatal(err)
		}
	}
}

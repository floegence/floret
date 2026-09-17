package openaitokenizer

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
)

func TestOfficialReference(t *testing.T) {
	raw, err := os.ReadFile("reference_test.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures []struct {
		Encoding, Text string
		Tokens         int64
	}
	if err = json.Unmarshal(raw, &fixtures); err != nil {
		t.Fatal(err)
	}
	for _, f := range fixtures {
		n, err := Count(f.Encoding, f.Text)
		if err != nil || n != f.Tokens {
			t.Errorf("%s %q: %d, %v; want %d", f.Encoding, f.Text, n, err, f.Tokens)
		}
	}
}
func TestBoundedLongSpans(t *testing.T) {
	for _, encoding := range []string{"cl100k_base", "o200k_base"} {
		n, err := Count(encoding, strings.Repeat("A", 1<<20))
		if err != nil || n <= 0 || n >= 1<<20 {
			t.Fatalf("long span count=%d error=%v", n, err)
		}
	}
}

func TestOfficialVocabularyDigests(t *testing.T) {
	for _, tc := range []struct {
		data   []byte
		digest string
	}{
		{cl100k, "223921b76ee99bde995b7ff738513eef100fb51d18c93597a113bcffe865b2a7"},
		{o200k, "446a9538cb6c348e3516120d7c08b09f57c36495e2acfffe59a5bf8b0cfb1a2d"},
	} {
		reader, err := gzip.NewReader(bytes.NewReader(tc.data))
		if err != nil {
			t.Fatal(err)
		}
		raw, err := io.ReadAll(reader)
		reader.Close()
		if err != nil {
			t.Fatal(err)
		}
		if fmt.Sprintf("%x", sha256.Sum256(raw)) != tc.digest {
			t.Fatal("official ranks differ from published digest")
		}
	}
}

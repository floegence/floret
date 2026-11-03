package bpetokenizer

import (
	"crypto/sha256"
	"fmt"
	"github.com/dlclark/regexp2"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestTimeoutDoesNotExposeInput(t *testing.T) {
	pattern := regexp2.MustCompile(`(x+)+y`, regexp2.None)
	pattern.MatchTimeout = time.Millisecond
	c := Counter{patterns: []*regexp2.Regexp{pattern}}
	_, err := c.count([]rune("private-context-marker"+strings.Repeat("x", 80)), 0, nil)
	if err == nil {
		t.Fatal("expected bounded timeout")
	}
	if strings.Contains(err.Error(), "private-context-marker") || strings.Contains(err.Error(), "xxxx") {
		t.Fatal("timeout exposed input")
	}
}

func byteCounter(t *testing.T) *Counter {
	t.Helper()
	ranks := make(map[string]int, 256)
	for i := range 256 {
		ranks[string([]byte{byte(i)})] = i
	}
	c, err := New(ranks, []string{`(?s).+`})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestCountCacheIsBoundedAndRetainsFrequentlyUsedCounts(t *testing.T) {
	c := byteCounter(t)
	for i := range maxCachedCounts {
		if _, err := c.Count(fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.Count("0"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Count("new"); err != nil {
		t.Fatal(err)
	}
	if len(c.counts) != maxCachedCounts || c.recent.Len() != maxCachedCounts {
		t.Fatal("count cache exceeded its bound")
	}
	if c.counts[sha256.Sum256([]byte("0"))] == nil || c.counts[sha256.Sum256([]byte("1"))] != nil {
		t.Fatal("count recency was not retained")
	}
	// A cache hit or eviction cannot change attribution at arbitrary UTF-8 spans.
	text := "prefix 中文 🌸 suffix"
	ends := []int{7, 10, len(text)}
	want, err := c.CountParts(text, ends)
	if err != nil {
		t.Fatal(err)
	}
	n, err := c.Count(text)
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.CountParts(text, ends)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("attribution changed: %v %v", got, err)
	}
	var total int64
	for _, part := range got {
		total += part
	}
	if total != n {
		t.Fatal("cached total differs from complete tokenization")
	}
}

func TestConcurrentCountsAreExact(t *testing.T) {
	c := byteCounter(t)
	var workers sync.WaitGroup
	for i := range 12 {
		workers.Go(func() {
			for j := range 80 {
				text := fmt.Sprintf("shared 中文 🌸 %d %d", i%3, j%5)
				n, err := c.Count(text)
				if err != nil || n != int64(len(text)) {
					t.Errorf("concurrent count: %d %v", n, err)
				}
			}
		})
	}
	workers.Wait()
}

func TestFailedCountsAreNotCached(t *testing.T) {
	pattern := regexp2.MustCompile(`(x+)+y`, regexp2.None)
	pattern.MatchTimeout = time.Millisecond
	c := Counter{patterns: []*regexp2.Regexp{pattern}}
	if _, err := c.Count(strings.Repeat("x", 80)); err == nil {
		t.Fatal("expected bounded timeout")
	}
	if len(c.counts) != 0 || c.recent.Len() != 0 {
		t.Fatal("failed tokenization entered the count cache")
	}
}

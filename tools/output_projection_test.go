package tools

import (
	"context"
	"strings"
	"testing"
	"unicode/utf8"
)

type memoryArtifactStore struct {
	items map[string]ToolOutputArtifact
}

func newMemoryArtifactStore() *memoryArtifactStore {
	return &memoryArtifactStore{items: map[string]ToolOutputArtifact{}}
}

func (s *memoryArtifactStore) PutToolOutput(_ context.Context, output ToolOutputArtifact) (ArtifactRef, error) {
	if output.MIME == "" {
		output.MIME = DefaultArtifactMIME
	}
	if output.Kind == "" {
		output.Kind = DefaultArtifactKind
	}
	id := output.ToolName + "-output"
	ref := ArtifactRef{
		ID:        id,
		SafeLabel: output.ToolName + "-output.log",
		URL:       "/artifacts/" + id,
		Kind:      output.Kind,
		MIME:      output.MIME,
		SizeBytes: int64(len(output.Text)),
		SHA256:    stableTextHash(output.Text),
	}
	s.items[id] = output
	return ref, nil
}

func (s *memoryArtifactStore) Text(id string) (string, bool) {
	item, ok := s.items[id]
	return item.Text, ok
}

func TestBuildOutputProjectionIsUTF8Safe(t *testing.T) {
	store := newMemoryArtifactStore()
	got, err := BuildOutputProjection(context.Background(), Result{Name: "demo", Text: "alpha 世界 omega"}, OutputPolicy{VisibleMaxBytes: 9, Strategy: OutputTail, PreserveFull: true}, store)
	if err != nil {
		t.Fatal(err)
	}
	if !utf8.ValidString(got.VisibleText) {
		t.Fatalf("projected text is invalid utf8: %q", got.VisibleText)
	}
	if !strings.Contains(got.VisibleText, "omega") {
		t.Fatalf("tail projection = %q", got.VisibleText)
	}
}

func TestBuildOutputProjectionHeadTailByBytesAndLines(t *testing.T) {
	store := newMemoryArtifactStore()
	text := "one\ntwo\nthree\nfour"
	head, err := BuildOutputProjection(context.Background(), Result{Name: "demo", Text: text}, OutputPolicy{VisibleMaxLines: 2, Strategy: OutputHead, PreserveFull: true}, store)
	if err != nil {
		t.Fatal(err)
	}
	if head.VisibleText != "one\ntwo" {
		t.Fatalf("head lines = %q", head.VisibleText)
	}
	tail, err := BuildOutputProjection(context.Background(), Result{Name: "demo", Text: text}, OutputPolicy{VisibleMaxLines: 2, Strategy: OutputTail, PreserveFull: true}, store)
	if err != nil {
		t.Fatal(err)
	}
	if tail.VisibleText != "three\nfour" {
		t.Fatalf("tail lines = %q", tail.VisibleText)
	}
	bytes, err := BuildOutputProjection(context.Background(), Result{Name: "demo", Text: "0123456789"}, OutputPolicy{VisibleMaxBytes: 4, Strategy: OutputHead, PreserveFull: true}, store)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.VisibleText != "0123" {
		t.Fatalf("head bytes = %q", bytes.VisibleText)
	}
}

func TestBuildOutputProjectionWritesArtifactWhenTruncated(t *testing.T) {
	store := newMemoryArtifactStore()
	got, err := BuildOutputProjection(context.Background(), Result{CallID: "call-1", Name: "demo", Text: "0123456789"}, OutputPolicy{VisibleMaxBytes: 4, Strategy: OutputTail}, store)
	if err != nil {
		t.Fatal(err)
	}
	if got.VisibleText != "6789" || !got.Truncated || got.FullOutput == nil {
		t.Fatalf("projection = %#v", got)
	}
	if got.OriginalBytes != 10 || got.VisibleBytes != 4 || got.Strategy != OutputTail {
		t.Fatalf("projection metadata = %#v", got)
	}
	full, ok := store.Text(got.FullOutput.ID)
	if !ok || full != "0123456789" {
		t.Fatalf("stored artifact = %q, %v", full, ok)
	}
	if strings.Contains(got.VisibleText, "/") || strings.Contains(got.FullOutput.SafeLabel, "/") {
		t.Fatalf("unsafe visible artifact data: %#v", got)
	}
}

func TestBuildOutputProjectionAllowsExplicitNoPreserve(t *testing.T) {
	got, err := BuildOutputProjection(context.Background(), Result{Name: "demo", Text: "0123456789"}, OutputPolicy{VisibleMaxBytes: 4, Strategy: OutputTail, PreserveFullSet: true, PreserveFull: false}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.VisibleText != "6789" || !got.Truncated || got.FullOutput != nil {
		t.Fatalf("projection = %#v", got)
	}
}

func TestBuildOutputProjectionRequiresStoreWhenPreservingFullOutput(t *testing.T) {
	_, err := BuildOutputProjection(context.Background(), Result{Name: "demo", Text: "0123456789"}, OutputPolicy{VisibleMaxBytes: 4, Strategy: OutputTail, PreserveFull: true}, nil)
	if err == nil || !strings.Contains(err.Error(), "artifact store") {
		t.Fatalf("err = %v, want artifact store error", err)
	}
}

package sessiontree

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestSourceBodyPagesPreserveFrozenSegmentsWithoutBodyReads(t *testing.T) {
	backend := newMigrationTestBackend()
	repo, err := NewBackendRepo(t.Context(), backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	for i := range 311 {
		if _, err := repo.Append(t.Context(), Entry{ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: fmt.Sprintf("%d中文🌸\r\n", i), ContentSourceID: "answer", Reasoning: "reason", ReasoningSourceID: "reason"}}, AppendOptions{ID: fmt.Sprint(i)}); err != nil {
			t.Fatal(err)
		}
	}
	options := SourceBodyPageOptions{ThreadID: "thread", LeafID: "299", SourceID: "answer", Field: "content", Limit: 7}
	for restart := range 2 {
		options.AfterEntryID = ""
		repo.backend = failingBodyReadBackend{Backend: backend, failure: errors.New("manifest read source")}
		var got []string
		for {
			page, err := repo.SourceBodyPage(t.Context(), options)
			if err != nil {
				t.Fatal(err)
			}
			if len(page.Bodies) > options.Limit || page.LeafID != options.LeafID {
				t.Fatal("unbounded or unfrozen page")
			}
			for _, body := range page.Bodies {
				text := fmt.Sprintf("%d中文🌸\r\n", len(got))
				if body.EntryID != fmt.Sprint(len(got)) || body.Field != "content" || body.Bytes != len(text) || body.SHA256 != stableHash(text) {
					t.Fatalf("incorrect segment %+v", body)
				}
				got = append(got, body.EntryID)
			}
			if !page.HasMore {
				break
			}
			if page.NextEntryID == options.AfterEntryID || page.NextEntryID == "" {
				t.Fatal("manifest did not advance")
			}
			options.AfterEntryID = page.NextEntryID
		}
		if len(got) != 300 {
			t.Fatalf("segments=%d", len(got))
		}
		reason, err := repo.SourceBodyPage(t.Context(), SourceBodyPageOptions{ThreadID: "thread", LeafID: "299", SourceID: "reason", Field: "reasoning", Limit: 2})
		if err != nil || len(reason.Bodies) != 1 || reason.HasMore || reason.Bodies[0].EntryID != "0" {
			t.Fatalf("reason=%+v err=%v", reason, err)
		}
		repo.backend = backend
		if restart == 0 {
			repo, err = NewBackendRepo(t.Context(), backend, time.Now)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := repo.MoveLeaf(t.Context(), "thread", "100"); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.SourceBodyPage(t.Context(), options); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("changed path=%v", err)
	}
}

func TestSourceBodyPageBoundsInactiveBranchScanning(t *testing.T) {
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	appendPathTestEntry(t, repo, "root", "")
	for i := range 600 {
		if _, err := repo.Append(t.Context(), Entry{ThreadID: "thread", ParentID: "root", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "sibling", ContentSourceID: "shared"}}, AppendOptions{ID: fmt.Sprint(i)}); err != nil {
			t.Fatal(err)
		}
	}
	options := SourceBodyPageOptions{ThreadID: "thread", LeafID: "599", SourceID: "shared", Field: "content", Limit: 150}
	var sizes []int
	for {
		page, err := repo.SourceBodyPage(t.Context(), options)
		if err != nil {
			t.Fatal(err)
		}
		sizes = append(sizes, len(page.Bodies))
		for _, body := range page.Bodies {
			if body.EntryID != "599" {
				t.Fatal("inactive sibling escaped")
			}
		}
		if !page.HasMore {
			break
		}
		options.AfterEntryID = page.NextEntryID
	}
	if !reflect.DeepEqual(sizes, []int{0, 0, 1}) {
		t.Fatalf("scan sizes=%v", sizes)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := repo.SourceBodyPage(ctx, options); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	options.Limit = 151
	if _, err := repo.SourceBodyPage(t.Context(), options); !errors.Is(err, spi.ErrInvalidArgument) {
		t.Fatal(err)
	}
}

func TestSourceBodyManifestControlOutputPreservesRawBytesAndOwnership(t *testing.T) {
	for _, text := range []string{"", "  \r\n", strings.Repeat(" 中文🌸\r\n", 40000)} {
		t.Run(fmt.Sprint(len(text)), func(t *testing.T) {
			backend := newMigrationTestBackend()
			repo, err := NewBackendRepo(t.Context(), backend, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
				t.Fatal(err)
			}
			signal := &session.ControlSignalView{Name: "continue", CallID: "call", Disposition: "continue", OutputText: text}
			entry := Entry{ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolCall, CreatedAt: time.Unix(1700000000, 0).UTC(), Message: session.Message{Role: session.Assistant, Kind: session.MessageKindControlSignal, Content: "tool_call", ToolCallID: "call", ToolName: "continue", ControlSignal: signal}}
			saved, err := repo.Append(t.Context(), entry, AppendOptions{ID: "control"})
			if err != nil {
				t.Fatal(err)
			}
			if signal.OutputText != text {
				t.Fatal("append mutated caller control")
			}
			for restart := range 2 {
				retained := repo.domainMemory.entries["thread"][0]
				if retained.Message.ControlSignal.OutputText != "" {
					t.Fatal("control output retained in journal")
				}
				page, err := repo.SourceBodyPage(t.Context(), SourceBodyPageOptions{ThreadID: "thread", SourceID: saved.Message.ContentSourceID, Field: "content", Limit: 1})
				if err != nil || len(page.Bodies) != 1 {
					t.Fatalf("page=%+v err=%v", page, err)
				}
				part := page.Bodies[0]
				if part.Field != "control_output" || part.Bytes != len(text) || part.SHA256 != stableHash(text) {
					t.Fatalf("part=%+v", part)
				}
				var got strings.Builder
				for {
					chunk, err := repo.ReadCanonicalBody(t.Context(), "thread", "control", part.Field, got.Len(), 16<<10)
					if err != nil {
						t.Fatal(err)
					}
					got.WriteString(chunk.Text)
					if got.Len() == chunk.Bytes {
						break
					}
					if chunk.Text == "" {
						t.Fatal("read made no progress")
					}
				}
				if got.String() != text {
					t.Fatal("source changed")
				}
				full, err := repo.Entry(t.Context(), "thread", "control")
				if err != nil || !reflect.DeepEqual(full, saved) {
					t.Fatalf("complete source differs after restart %d: %v", restart, err)
				}
				metadata, err := repo.PathMetadataPage(t.Context(), "thread", "", "", 1)
				if err != nil || metadata.Entries[0].Entry.Message.ControlSignal.OutputText != "" {
					t.Fatalf("metadata retained source: %v", err)
				}
				if retained.Message.ControlSignal.OutputText != "" || signal.OutputText != text {
					t.Fatal("read mutated source ownership")
				}
				if restart == 0 {
					repo, err = NewBackendRepo(t.Context(), backend, time.Now)
					if err != nil {
						t.Fatal(err)
					}
				}
			}
		})
	}
}

package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
)

type exchangeReadBackend struct {
	spi.Backend
	exact, scans int
}
type exchangeReadTx struct {
	spi.ReadTx
	owner *exchangeReadBackend
}

func (b *exchangeReadBackend) View(ctx context.Context, read func(spi.ReadTx) error) error {
	return b.Backend.View(ctx, func(tx spi.ReadTx) error { return read(exchangeReadTx{ReadTx: tx, owner: b}) })
}
func (tx exchangeReadTx) QueryIndex(spi.IndexQuery) ([]spi.IndexDocument, error) {
	tx.owner.scans++
	return nil, errors.New("exact tool lookup scanned the index")
}
func (tx exchangeReadTx) ReadIndexDocument(threadID, key string) (spi.IndexDocument, error) {
	tx.owner.exact++
	return (&kvQueryIndex{read: tx.ReadTx}).ReadIndexDocument(threadID, key)
}

func TestCanonicalToolExchangeUsesExactSourcesAcrossUnrelatedHistory(t *testing.T) {
	backend := newMigrationTestBackend()
	repo, err := NewBackendRepo(t.Context(), backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.UpdateDomain(t.Context(), func(memory *MemoryRepo, _ spi.WriteTx) error {
		if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
			return err
		}
		for i := range 1096 {
			if i == 548 {
				for _, entry := range []Entry{
					{ID: "call", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: "inspect", ToolArgs: "{}"}},
					{ID: "result", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "inspect", ToolCallID: "inspect", Content: "confirmed中文🌸", ToolResult: &session.ToolResultView{Status: "success"}}},
				} {
					if _, err := memory.Append(t.Context(), entry, AppendOptions{}); err != nil {
						return err
					}
				}
			}
			if _, err := memory.Append(t.Context(), Entry{ThreadID: "thread", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "unrelated"}}, AppendOptions{ID: fmt.Sprint(i)}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	path, err := repo.Path(t.Context(), "thread", "")
	if err != nil {
		t.Fatal(err)
	}
	var wantCall, wantResult Entry
	for _, entry := range path {
		if entry.ID == "call" {
			wantCall = entry
		}
		if entry.ID == "result" {
			wantResult = entry
		}
	}
	for restart := range 2 {
		tracked := &exchangeReadBackend{Backend: backend}
		repo.backend = tracked
		call, result, found, err := repo.CanonicalToolExchange(t.Context(), "thread", "turn", "run", "inspect")
		got, _ := json.Marshal([]Entry{call, result})
		want, _ := json.Marshal([]Entry{wantCall, wantResult})
		if err != nil || !found || string(got) != string(want) {
			t.Fatalf("exchange=%+v %+v found=%t err=%v", call, result, found, err)
		}
		if tracked.exact != 1 || tracked.scans != 0 {
			t.Fatalf("exact=%d scans=%d", tracked.exact, tracked.scans)
		}
		for _, identity := range [][3]string{{"wrong", "run", "inspect"}, {"turn", "wrong", "inspect"}, {"turn", "run", "missing"}} {
			if _, _, found, err := repo.CanonicalToolExchange(t.Context(), "thread", identity[0], identity[1], identity[2]); err != nil || found {
				t.Fatalf("wrong identity found=%t err=%v", found, err)
			}
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, _, _, err := repo.CanonicalToolExchange(ctx, "thread", "turn", "run", "inspect"); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel=%v", err)
		}
		if restart == 0 {
			repo, err = NewBackendRepo(t.Context(), backend, time.Now)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := repo.MoveLeaf(t.Context(), "thread", "547"); err != nil {
		t.Fatal(err)
	}
	if _, _, found, err := repo.CanonicalToolExchange(t.Context(), "thread", "turn", "run", "inspect"); err != nil || found {
		t.Fatalf("inactive exchange found=%t err=%v", found, err)
	}
}

func BenchmarkSQLiteFixedHistoryToolExchange(b *testing.B) {
	for _, count := range []int{1096, 10960, 109600} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			repo := newFixedHistorySQLiteRepo(b, count)
			for _, entry := range []Entry{
				{ID: "call", ThreadID: "history", TurnID: "turn", RunID: "run", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: "inspect", ToolArgs: "{}"}},
				{ID: "result", ThreadID: "history", TurnID: "turn", RunID: "run", Type: EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "inspect", ToolCallID: "inspect", Content: "confirmed", ToolResult: &session.ToolResultView{Status: "success"}}},
			} {
				if _, err := repo.Append(b.Context(), entry, AppendOptions{}); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, _, found, err := repo.CanonicalToolExchange(b.Context(), "history", "turn", "run", "inspect"); err != nil || !found {
					b.Fatalf("exchange found=%t err=%v", found, err)
				}
			}
		})
	}
}

func TestCanonicalToolExchangeRequiresConfirmedDetachedSources(t *testing.T) {
	backend := newMigrationTestBackend()
	repo, err := NewBackendRepo(t.Context(), backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	call := Entry{ID: "call", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolCall,
		Metadata: map[string]string{"source": "canonical"},
		Message:  session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: "inspect", ToolArgs: "{}"}}
	if _, err := repo.Append(t.Context(), call, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, _, found, err := repo.CanonicalToolExchange(t.Context(), "thread", "turn", "run", "inspect"); err != nil || found {
		t.Fatalf("unconfirmed found=%t err=%v", found, err)
	}
	if _, err := repo.Append(t.Context(), Entry{ID: "result", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolResult,
		Message: session.Message{Role: session.Tool, ToolName: "inspect", ToolCallID: "inspect", Content: "complete",
			ToolResult: &session.ToolResultView{Status: "success"}}}, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	gotCall, gotResult, found, err := repo.CanonicalToolExchange(t.Context(), "thread", "turn", "run", "inspect")
	if err != nil || !found {
		t.Fatalf("confirmed found=%t err=%v", found, err)
	}
	gotCall.Metadata["source"] = "changed"
	gotResult.Message.ToolResult.Status = "changed"
	gotCall, gotResult, found, err = repo.CanonicalToolExchange(t.Context(), "thread", "turn", "run", "inspect")
	if err != nil || !found || gotCall.Metadata["source"] != "canonical" || gotResult.Message.ToolResult.Status != "success" {
		t.Fatalf("sources were not detached: call=%+v result=%+v found=%t err=%v", gotCall, gotResult, found, err)
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		key := "thread\x00tool_calls\x00turn\x00run\x00inspect"
		document, exists, err := readQueryDocumentByKey(queryIndexReader(tx), "thread", key)
		if err != nil || !exists {
			t.Fatalf("document found=%t err=%v", exists, err)
		}
		var metadata queryDocumentMetadata
		if err := json.Unmarshal(document.Metadata, &metadata); err != nil {
			return err
		}
		metadata.Row.RunID = "other-run"
		document.Metadata, err = json.Marshal(metadata)
		if err != nil {
			return err
		}
		return queryIndexWriter(tx).PutIndexDocument(document)
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, found, err := repo.CanonicalToolExchange(t.Context(), "thread", "turn", "run", "inspect"); found || !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("corrupt locator found=%t err=%v", found, err)
	}
}

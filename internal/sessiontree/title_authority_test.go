package sessiontree

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v5/internal/session"
)

func TestAcceptTurnInstallsCanonicalFallbackTitle(t *testing.T) {
	tests := []struct {
		name  string
		input session.Message
		want  string
	}{
		{name: "text", input: session.Message{Role: session.User, Content: "  inspect\n\tthe   runtime  "}, want: "inspect the runtime"},
		{name: "attachment only", input: session.Message{Role: session.User, Attachments: []session.MessageAttachment{{ResourceRef: "attachment-1", Name: "trace.json", MIMEType: "application/json"}}}, want: "trace.json"},
		{name: "reference only", input: session.Message{Role: session.User, References: []session.MessageReference{{ReferenceID: "reference-1", Kind: session.MessageReferenceFile, Label: "runtime.go", ResourceRef: "file-1"}}}, want: "runtime.go"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repo := NewMemoryRepo()
			now := time.Date(2026, 8, 15, 1, 2, 3, 0, time.UTC)
			if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "thread", CreatedAt: now}); err != nil {
				t.Fatal(err)
			}
			acceptTestTurn(t, repo, test.input, now.Add(time.Second))
			meta, err := repo.Thread(t.Context(), "thread")
			if err != nil {
				t.Fatal(err)
			}
			if meta.Title != test.want || meta.TitleStatus != ThreadTitleReady || meta.TitleSource != ThreadTitleSourceFallback {
				t.Fatalf("fallback title state = %#v, want title %q ready/fallback", meta, test.want)
			}
		})
	}
}

func TestAcceptTurnTruncatesFallbackTitleAtCanonicalLimit(t *testing.T) {
	repo := NewMemoryRepo()
	now := time.Date(2026, 8, 15, 1, 2, 3, 0, time.UTC)
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "thread", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	acceptTestTurn(t, repo, session.Message{Role: session.User, Content: strings.Repeat("界", MaxThreadTitleRunes+20)}, now.Add(time.Second))
	meta, err := repo.Thread(t.Context(), "thread")
	if err != nil {
		t.Fatal(err)
	}
	if got := len([]rune(meta.Title)); got != MaxThreadTitleRunes {
		t.Fatalf("fallback title runes = %d, want %d", got, MaxThreadTitleRunes)
	}
}

func TestAcceptTurnTrimsWhitespaceAtFallbackTitleLimit(t *testing.T) {
	repo := NewMemoryRepo()
	now := time.Date(2026, 8, 15, 1, 2, 3, 0, time.UTC)
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "thread", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	input := strings.Repeat("a", MaxThreadTitleRunes-1) + " " + strings.Repeat("b", 20)
	acceptTestTurn(t, repo, session.Message{Role: session.User, Content: input}, now.Add(time.Second))
	meta, err := repo.Thread(t.Context(), "thread")
	if err != nil {
		t.Fatal(err)
	}
	if want := strings.Repeat("a", MaxThreadTitleRunes-1); meta.Title != want {
		t.Fatalf("fallback title = %q, want %q", meta.Title, want)
	}
}

func TestAutomaticTitleTransitionsRetainFallbackUntilProviderSuccess(t *testing.T) {
	repo := NewMemoryRepo()
	now := time.Date(2026, 8, 15, 1, 2, 3, 0, time.UTC)
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "thread", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	acceptTestTurn(t, repo, session.Message{Role: session.User, Content: "fallback title"}, now.Add(time.Second))

	begun, err := repo.BeginAutomaticThreadTitle(t.Context(), BeginAutomaticThreadTitleRequest{ThreadID: "thread", Token: "claim-1", Now: now.Add(2 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if begun.Thread.Title != "fallback title" || begun.Thread.TitleStatus != ThreadTitlePending || begun.Thread.TitleSource != ThreadTitleSourceFallback {
		t.Fatalf("pending title state = %#v", begun.Thread)
	}
	failed, err := repo.FailAutomaticThreadTitle(t.Context(), FailAutomaticThreadTitleRequest{ThreadID: "thread", Generation: begun.Thread.TitleGeneration, Token: begun.Thread.TitleToken, Error: "provider unavailable", Now: now.Add(3 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if failed.Thread.Title != "fallback title" || failed.Thread.TitleStatus != ThreadTitleFailed || failed.Thread.TitleSource != ThreadTitleSourceFallback {
		t.Fatalf("failed title state = %#v", failed.Thread)
	}

	restarted, err := repo.BeginAutomaticThreadTitle(t.Context(), BeginAutomaticThreadTitleRequest{ThreadID: "thread", Token: "claim-2", Now: now.Add(4 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	completed, err := repo.CompleteAutomaticThreadTitle(t.Context(), CompleteAutomaticThreadTitleRequest{ThreadID: "thread", Generation: restarted.Thread.TitleGeneration, Token: restarted.Thread.TitleToken, Title: "provider title", Now: now.Add(5 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if completed.Thread.Title != "provider title" || completed.Thread.TitleStatus != ThreadTitleReady || completed.Thread.TitleSource != ThreadTitleSourceProvider {
		t.Fatalf("completed title state = %#v", completed.Thread)
	}
}

func TestHostTitleOverridesPendingAutomaticTitle(t *testing.T) {
	repo := NewMemoryRepo()
	now := time.Date(2026, 8, 15, 1, 2, 3, 0, time.UTC)
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "thread", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	acceptTestTurn(t, repo, session.Message{Role: session.User, Content: "fallback title"}, now.Add(time.Second))
	begun, err := repo.BeginAutomaticThreadTitle(t.Context(), BeginAutomaticThreadTitleRequest{ThreadID: "thread", Token: "claim", Now: now.Add(2 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	manual, err := repo.SetThreadTitle(t.Context(), SetThreadTitleRequest{ThreadID: "thread", Title: "manual title", Now: now.Add(3 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if manual.Thread.Title != "manual title" || manual.Thread.TitleSource != ThreadTitleSourceHost {
		t.Fatalf("manual title state = %#v", manual.Thread)
	}
	if _, err := repo.CompleteAutomaticThreadTitle(t.Context(), CompleteAutomaticThreadTitleRequest{ThreadID: "thread", Generation: begun.Thread.TitleGeneration, Token: begun.Thread.TitleToken, Title: "late provider title", Now: now.Add(4 * time.Second)}); err != ErrStaleAuthority {
		t.Fatalf("late automatic completion error = %v, want %v", err, ErrStaleAuthority)
	}
}

func TestDecodeMemoryStateRepairsLegacyEmptyTitleFromFirstUserMessage(t *testing.T) {
	repo := NewMemoryRepo()
	now := time.Date(2026, 8, 15, 1, 2, 3, 0, time.UTC)
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "thread", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	acceptTestTurn(t, repo, session.Message{Role: session.User, Content: "recover this title"}, now.Add(time.Second))
	repo.mu.Lock()
	meta := repo.threads["thread"]
	meta.Title, meta.TitleStatus, meta.TitleSource = "", "", ""
	meta.TitleUpdatedAt, meta.TitleError, meta.TitleGeneration, meta.TitleToken = time.Time{}, "", 0, ""
	repo.threads["thread"] = meta
	repo.mu.Unlock()
	encoded, err := repo.EncodeMemoryState()
	if err != nil {
		t.Fatal(err)
	}
	recovered, migrated, err := decodeMemoryState(encoded, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if !migrated {
		t.Fatal("legacy empty title repair was not marked for durable persistence")
	}
	meta, err = recovered.Thread(t.Context(), "thread")
	if err != nil {
		t.Fatal(err)
	}
	if meta.Title != "recover this title" || meta.TitleStatus != ThreadTitleReady || meta.TitleSource != ThreadTitleSourceFallback {
		t.Fatalf("recovered title state = %#v", meta)
	}
}

func TestDecodeMemoryStateRepairsLegacyPendingTitleWithoutEndingClaim(t *testing.T) {
	repo := NewMemoryRepo()
	now := time.Date(2026, 8, 15, 1, 2, 3, 0, time.UTC)
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "thread", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	acceptTestTurn(t, repo, session.Message{Role: session.User, Content: "restart fallback"}, now.Add(time.Second))
	begun, err := repo.BeginAutomaticThreadTitle(t.Context(), BeginAutomaticThreadTitleRequest{ThreadID: "thread", Token: "claim", Now: now.Add(2 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	repo.mu.Lock()
	meta := repo.threads["thread"]
	meta.Title, meta.TitleSource = "", ""
	repo.threads["thread"] = meta
	repo.mu.Unlock()
	encoded, err := repo.EncodeMemoryState()
	if err != nil {
		t.Fatal(err)
	}
	recovered, migrated, err := decodeMemoryState(encoded, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if !migrated {
		t.Fatal("legacy pending title repair was not marked for durable persistence")
	}
	meta, err = recovered.Thread(t.Context(), "thread")
	if err != nil {
		t.Fatal(err)
	}
	if meta.Title != "restart fallback" || meta.TitleStatus != ThreadTitlePending || meta.TitleSource != ThreadTitleSourceFallback || meta.TitleToken != begun.Thread.TitleToken {
		t.Fatalf("recovered pending title state = %#v", meta)
	}
}

func acceptTestTurn(t *testing.T, repo *MemoryRepo, input session.Message, now time.Time) AcceptTurnResult {
	t.Helper()
	req := AcceptTurnRequest{
		ThreadID: "thread", TurnID: "turn-1", RunID: "run-1", LogicalRequestID: "request-1",
		RequestFingerprint: "turn-fingerprint", InputRequestFingerprint: "input-fingerprint", Input: input, Now: now,
	}
	result, err := repo.AcceptTurn(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

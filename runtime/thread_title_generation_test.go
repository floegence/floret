package runtime

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/sessiontree"
)

func TestThreadSummaryTitleGenerationOrdersIndependentTitleUpdates(t *testing.T) {
	gateway := newBlockingThreadGateway()
	host, service := testThreadService(t, gateway)
	created, err := service.Create(t.Context(), CreateThreadInput{RequestKey: "create-title-generation"})
	if err != nil {
		t.Fatal(err)
	}
	assertSummary := func(wantTitle string, wantStatus ThreadTitleStatus, wantGeneration int64) {
		t.Helper()
		summaries, err := service.List(t.Context(), ThreadScope{})
		if err != nil || len(summaries) != 1 {
			t.Fatalf("summaries=%#v err=%v", summaries, err)
		}
		summary := summaries[0]
		data, err := json.Marshal(summary)
		if err != nil {
			t.Fatal(err)
		}
		var wire map[string]json.RawMessage
		if err := json.Unmarshal(data, &wire); err != nil {
			t.Fatal(err)
		}
		var generation int64
		if err := json.Unmarshal(wire["title_generation"], &generation); err != nil {
			t.Fatalf("title generation missing from public summary: %s: %v", data, err)
		}
		if summary.Title != wantTitle || summary.TitleStatus != wantStatus || generation != wantGeneration {
			t.Fatalf("title snapshot=%s, want %q/%q/%d", data, wantTitle, wantStatus, wantGeneration)
		}
		if _, exposed := wire["title_token"]; exposed {
			t.Fatal("private title token exposed")
		}
	}
	assertSummary("", ThreadTitleStatusUnset, 0)
	if _, err := service.Send(t.Context(), SendInput{ThreadID: created.ThreadID, Input: UserInput{Text: "fallback"}, RequestKey: "send-title-generation"}); err != nil {
		t.Fatal(err)
	}
	waitClosed(t, gateway.started, "provider did not start")
	before, err := service.View(t.Context(), created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	assertSummary("fallback", ThreadTitleStatusReady, 1)
	authority := host.store.repo.(sessiontree.ThreadTitleAuthorityRepo)
	begin := func(token string) int64 {
		t.Helper()
		result, err := authority.BeginAutomaticThreadTitle(t.Context(), sessiontree.BeginAutomaticThreadTitleRequest{ThreadID: created.ThreadID.String(), Token: token, Now: time.Now()})
		if err != nil {
			t.Fatal(err)
		}
		return result.Thread.TitleGeneration
	}
	generation := begin("first")
	assertSummary("fallback", ThreadTitleStatusPending, generation)
	if _, err := authority.FailAutomaticThreadTitle(t.Context(), sessiontree.FailAutomaticThreadTitleRequest{ThreadID: created.ThreadID.String(), Token: "first", Generation: generation, Error: "provider unavailable", Now: time.Now()}); err != nil {
		t.Fatal(err)
	}
	assertSummary("fallback", ThreadTitleStatusFailed, generation)
	retryGeneration := begin("retry")
	if retryGeneration <= generation {
		t.Fatal("retry did not advance generation")
	}
	assertSummary("fallback", ThreadTitleStatusPending, retryGeneration)
	completion := sessiontree.CompleteAutomaticThreadTitleRequest{ThreadID: created.ThreadID.String(), Token: "retry", Generation: retryGeneration, Title: "Generated", Now: time.Now()}
	if _, err := authority.CompleteAutomaticThreadTitle(t.Context(), completion); err != nil {
		t.Fatal(err)
	}
	assertSummary("Generated", ThreadTitleStatusReady, retryGeneration)
	if _, err := service.SetTitle(t.Context(), SetTitleInput{ThreadID: created.ThreadID, Title: "Manual", RequestKey: "rename-title-generation"}); err != nil {
		t.Fatal(err)
	}
	assertSummary("Manual", ThreadTitleStatusReady, retryGeneration+1)
	if _, err := authority.CompleteAutomaticThreadTitle(t.Context(), completion); !errors.Is(err, sessiontree.ErrStaleAuthority) {
		t.Fatalf("late completion error=%v", err)
	}
	assertSummary("Manual", ThreadTitleStatusReady, retryGeneration+1)
	after, err := service.View(t.Context(), created.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	if after.ViewVersion != before.ViewVersion {
		t.Fatalf("title updates changed runtime version: %d -> %d", before.ViewVersion, after.ViewVersion)
	}
}

package sessiontree

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
)

func TestForkInstallsIndependentCanonicalTitle(t *testing.T) {
	for _, test := range []struct {
		name  string
		input session.Message
		want  string
	}{
		{name: "empty"},
		{name: "text", input: session.Message{Role: session.User, Content: "  inspect\n the runtime  "}, want: "inspect the runtime"},
		{name: "attachment", input: session.Message{Role: session.User, Attachments: []session.MessageAttachment{{ResourceRef: "attachment-1", Name: "trace.json", MIMEType: "application/json"}}}, want: "trace.json"},
		{name: "reference", input: session.Message{Role: session.User, References: []session.MessageReference{{ReferenceID: "ref-1", Kind: session.MessageReferenceFile, Label: "runtime.go", ResourceRef: "file-1"}}}, want: "runtime.go"},
		{name: "unicode limit", input: session.Message{Role: session.User, Content: strings.Repeat("界", 220)}, want: strings.Repeat("界", 200)},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := t.Context()
			now := time.Date(2026, 9, 8, 8, 0, 0, 0, time.UTC)
			backend := newMigrationTestBackend()
			repo, err := NewBackendRepo(ctx, backend, func() time.Time { return now })
			if err != nil {
				t.Fatal(err)
			}
			if _, err := repo.CreateThread(ctx, ThreadMeta{ID: "thread", CreatedAt: now}); err != nil {
				t.Fatal(err)
			}
			if test.want != "" {
				if _, err := repo.AcceptTurn(ctx, AcceptTurnRequest{ThreadID: "thread", TurnID: "turn", RunID: "run", LogicalRequestID: "request", RequestFingerprint: "fingerprint", InputRequestFingerprint: "input-fingerprint", Input: test.input, Now: now}); err != nil {
					t.Fatal(err)
				}
				if _, err := repo.BeginAutomaticThreadTitle(ctx, BeginAutomaticThreadTitleRequest{ThreadID: "thread", Token: "source-title-task", Now: now.Add(time.Second)}); err != nil {
					t.Fatal(err)
				}
			}
			source, err := repo.Thread(ctx, "thread")
			if err != nil {
				t.Fatal(err)
			}
			for index, sourceID := range []string{"thread", "fork-0"} {
				destination := []string{"fork-0", "fork-1"}[index]
				forked, err := repo.Fork(ctx, ForkOptions{SourceThreadID: sourceID, NewThreadID: destination, Now: now.Add(2 * time.Second)})
				if err != nil {
					t.Fatal(err)
				}
				if forked.Title != test.want {
					t.Fatalf("fork title=%q, want %q", forked.Title, test.want)
				}
				if test.want != "" && (forked.TitleStatus != ThreadTitleReady || forked.TitleSource != ThreadTitleSourceFallback || forked.TitleGeneration != 1) {
					t.Fatalf("fork title authority=%#v", forked)
				}
				if forked.TitleToken != "" || forked.TitleRequestKey != "" || forked.TitleError != "" {
					t.Fatalf("fork copied source title lifecycle: %#v", forked)
				}
				if err := ValidateThreadTitleState(forked); err != nil {
					t.Fatal(err)
				}
				reopened, err := NewBackendRepo(ctx, backend, func() time.Time { return now.Add(time.Minute) })
				if err != nil {
					t.Fatal(err)
				}
				stored, err := reopened.Thread(ctx, destination)
				if err != nil || !reflect.DeepEqual(stored, forked) {
					t.Fatalf("restart title changed: %#v, %v", stored, err)
				}
			}
			unchanged, err := repo.Thread(ctx, "thread")
			if err != nil || !reflect.DeepEqual(source, unchanged) {
				t.Fatalf("source changed: %#v, %v", unchanged, err)
			}
			// Pinning the empty boundary must not derive a title from later input.
			path, err := repo.Path(ctx, "thread", source.LeafID)
			if err != nil {
				t.Fatal(err)
			}
			boundary := ""
			if len(path) > 0 {
				boundary = path[0].ID
			}
			empty, err := repo.Fork(ctx, ForkOptions{SourceThreadID: "thread", NewThreadID: "before-input", EntryID: boundary, EntryIDPinned: true, Now: now.Add(time.Minute)})
			if err != nil || empty.Title != "" || empty.TitleStatus != "" {
				t.Fatalf("empty boundary=%#v, %v", empty, err)
			}
		})
	}
}

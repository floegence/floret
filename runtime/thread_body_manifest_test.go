package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/threadquery"
)

func TestThreadBodyManifestReconstructsExactCanonicalSourceAcrossRestart(t *testing.T) {
	path := t.TempDir() + "/manifest.sqlite"
	open := func() (*Host, *threadRuntimeService) {
		host, err := Open(t.Context(), Options{Storage: storage.SQLite(path)})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
		agent, err := testAgent(newBlockingThreadGateway())
		if err != nil {
			t.Fatal(err)
		}
		full, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
		if err != nil {
			t.Fatal(err)
		}
		return host, full.(*threadRuntimeService)
	}
	host, service := open()
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: "thread-manifest"}); err != nil {
		t.Fatal(err)
	}
	var expected strings.Builder
	for i := range 153 {
		text := fmt.Sprintf("%d中文🌸\r\n", i)
		expected.WriteString(text)
		if _, err := host.store.repo.Append(t.Context(), sessiontree.Entry{ThreadID: "thread-manifest", TurnID: "turn", RunID: "run", Type: sessiontree.EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: text, ContentSourceID: "answer", Reasoning: "reason", ReasoningSourceID: "reason"}}, sessiontree.AppendOptions{ID: fmt.Sprint(i)}); err != nil {
			t.Fatal(err)
		}
	}
	scope := threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{"thread-manifest"}}
	input := ThreadBodyManifestInput{ThreadID: "thread-manifest", LeafID: "152", SourceID: "answer", Field: ThreadBodyContent, Limit: 7}
	for restart := range 2 {
		input.AfterEntryID = ""
		hash := sha256.New()
		count := 0
		for {
			page, err := service.BodyManifest(t.Context(), scope, input)
			if err != nil {
				t.Fatal(err)
			}
			wire, _ := json.Marshal(page)
			if len(wire) > 64<<10 || len(page.Parts) > input.Limit {
				t.Fatal("manifest budget exceeded")
			}
			for _, part := range page.Parts {
				chunk, err := service.ReadBody(t.Context(), scope, ThreadBodyInput{ThreadID: input.ThreadID, EntryID: part.EntryID, Field: part.Field, SHA256: part.SHA256})
				if err != nil || chunk.Bytes != part.Bytes || chunk.SHA256 != part.SHA256 {
					t.Fatalf("part=%+v err=%v", part, err)
				}
				hash.Write([]byte(chunk.Text))
				count++
			}
			if _, err := service.BodyManifest(t.Context(), threadquery.Scope{Authority: scope.Authority}, input); !errors.Is(err, threadquery.ErrRecordUnavailable) {
				t.Fatalf("revoked scope=%v", err)
			}
			if !page.HasMore {
				break
			}
			input.AfterEntryID = page.NextEntryID
		}
		want := sha256.Sum256([]byte(expected.String()))
		if count != 153 || hex.EncodeToString(hash.Sum(nil)) != hex.EncodeToString(want[:]) {
			t.Fatal("canonical source changed")
		}
		if len(service.runtimes) != 0 {
			t.Fatal("manifest hydrated complete presentation actor")
		}
		if restart == 0 {
			if err := host.Shutdown(t.Context()); err != nil {
				t.Fatal(err)
			}
			host, service = open()
		}
	}
	input.AfterEntryID, input.LeafID = "", ""
	input.Field = ThreadBodyArguments
	if _, err := service.BodyManifest(t.Context(), scope, input); !errors.Is(err, threadquery.ErrInvalidQuery) {
		t.Fatal(err)
	}
	input.Field, input.Limit = ThreadBodyContent, 151
	if _, err := service.BodyManifest(t.Context(), scope, input); !errors.Is(err, threadquery.ErrInvalidQuery) {
		t.Fatal(err)
	}
	input.Limit, input.AfterEntryID = 7, "1"
	if _, err := service.BodyManifest(t.Context(), scope, input); !errors.Is(err, threadquery.ErrInvalidQuery) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := service.BodyManifest(ctx, scope, input); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/threadquery"
)

func TestThreadBodyReaderAuthorizesEveryChunkAndSurvivesRestart(t *testing.T) {
	path := t.TempDir() + "/body.sqlite"
	text := strings.Repeat("hello 中文🌸\r\n", 10000)
	open := func() (*Host, ThreadBodyReader) {
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
		return host, full.(ThreadBodyReader)
	}
	host, reader := open()
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: "thread-body"}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.store.repo.Append(t.Context(), sessiontree.Entry{ThreadID: "thread-body", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: text}}, sessiontree.AppendOptions{ID: "source"}); err != nil {
		t.Fatal(err)
	}
	scope := threadquery.Scope{Authority: "authorized-user", ThreadIDs: []identity.ThreadID{"thread-body"}}
	input := ThreadBodyInput{ThreadID: "thread-body", EntryID: "source", Field: ThreadBodyContent}
	expected := sha256.Sum256([]byte(text))
	input.SHA256 = hex.EncodeToString(expected[:])
	for restart := range 2 {
		hash := sha256.New()
		input.OffsetBytes = 0
		for input.OffsetBytes < len(text) {
			part, err := reader.ReadBody(t.Context(), scope, input)
			if err != nil {
				t.Fatal(err)
			}
			if part.OffsetBytes != input.OffsetBytes || part.Bytes != len(text) || part.SHA256 != input.SHA256 || len(part.Text) == 0 || len(part.Text) > 16<<10 {
				t.Fatal("invalid source chunk")
			}
			hash.Write([]byte(part.Text))
			input.OffsetBytes += len(part.Text)
			if _, err := reader.ReadBody(t.Context(), threadquery.Scope{Authority: scope.Authority}, input); !errors.Is(err, threadquery.ErrRecordUnavailable) {
				t.Fatalf("revoked scope readable: %v", err)
			}
		}
		if hex.EncodeToString(hash.Sum(nil)) != input.SHA256 {
			t.Fatal("source bytes changed")
		}
		if restart == 0 {
			if err := host.Shutdown(t.Context()); err != nil {
				t.Fatal(err)
			}
			host, reader = open()
		}
	}
	input.OffsetBytes = 0
	input.SHA256 = "wrong"
	if _, err := reader.ReadBody(t.Context(), scope, input); !errors.Is(err, threadquery.ErrRecordUnavailable) {
		t.Fatalf("stale source accepted: %v", err)
	}
	input.SHA256 = ""
	if _, err := reader.ReadBody(t.Context(), threadquery.Scope{ThreadIDs: scope.ThreadIDs}, input); !errors.Is(err, threadquery.ErrInvalidQuery) {
		t.Fatalf("empty authority accepted: %v", err)
	}
	input.LimitBytes = 65536
	if _, err := reader.ReadBody(t.Context(), scope, input); !errors.Is(err, threadquery.ErrInvalidQuery) {
		t.Fatalf("oversized source accepted: %v", err)
	}
}

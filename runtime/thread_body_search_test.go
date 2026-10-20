package runtime

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/threadquery"
)

func bodySearchFixture(t *testing.T, source string) (*threadRuntimeService, threadquery.Scope) {
	t.Helper()
	host, err := Open(t.Context(), Options{Storage: storage.SQLite(t.TempDir() + "/body.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	agent, err := testAgent(newBlockingThreadGateway())
	if err != nil {
		t.Fatal(err)
	}
	service, err := host.ThreadService(AgentFactoryFunc(func(context.Context, AgentRequest) (*Agent, error) { return agent, nil }))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.store.repo.CreateThread(t.Context(), sessiontree.ThreadMeta{ID: "thread-search"}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.store.repo.Append(t.Context(), sessiontree.Entry{ThreadID: "thread-search", Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: source}}, sessiontree.AppendOptions{ID: "source"}); err != nil {
		t.Fatal(err)
	}
	return service.(*threadRuntimeService), threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{"thread-search"}}
}

func TestBodySearchMatchesCompleteSourceAcrossBoundaries(t *testing.T) {
	source := strings.Repeat("x", (64<<10)-2) + "中文🌸\r\n" + strings.Repeat("ababa中文🌸\r\n", 10000) + "end"
	service, scope := bodySearchFixture(t, source)
	for _, query := range []string{"中文🌸", "aba", "\r\n", strings.Repeat("x", 4090) + "中文", "missing", "end"} {
		t.Run(query[:min(20, len(query))], func(t *testing.T) {
			input := ThreadBodySearchInput{Body: ThreadBodyInput{ThreadID: "thread-search", EntryID: "source", Field: ThreadBodyContent}, Query: query, Limit: 7}
			var want, got []int
			for offset := 0; offset < len(source); {
				i := strings.Index(source[offset:], query)
				if i < 0 {
					break
				}
				offset += i
				want = append(want, offset)
				_, width := utf8.DecodeRuneInString(source[offset:])
				offset += width
			}
			for {
				page, err := service.SearchBody(t.Context(), scope, input)
				if err != nil {
					t.Fatal(err)
				}
				if len(page.Matches) > input.Limit || page.NextOffsetBytes < input.Body.OffsetBytes || page.NextOffsetBytes-input.Body.OffsetBytes > threadBodySearchBytes {
					t.Fatalf("unbounded or regressing page: %+v", page)
				}
				if page.HasMore && page.NextOffsetBytes == input.Body.OffsetBytes {
					t.Fatal("non-progressing search")
				}
				for _, match := range page.Matches {
					if match.LengthBytes != len(query) || source[match.OffsetBytes:match.OffsetBytes+match.LengthBytes] != query {
						t.Fatal("inexact match")
					}
					got = append(got, match.OffsetBytes)
				}
				if !page.HasMore {
					if page.NextOffsetBytes != len(source) {
						t.Fatal("incomplete source coverage")
					}
					break
				}
				input.Body.OffsetBytes, input.Body.SHA256 = page.NextOffsetBytes, page.SHA256
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("search differs: got %d want %d", len(got), len(want))
			}
		})
	}
}

func TestBodySearchValidationAndAuthority(t *testing.T) {
	service, scope := bodySearchFixture(t, strings.Repeat("中文\x00\r\n", 20000))
	valid := ThreadBodySearchInput{Body: ThreadBodyInput{ThreadID: "thread-search", EntryID: "source", Field: ThreadBodyContent}, Query: "中文", Limit: 2}
	page, err := service.SearchBody(t.Context(), scope, valid)
	if err != nil || len(page.Matches) != 2 {
		t.Fatalf("first page: %+v %v", page, err)
	}
	for _, mutate := range []func(*ThreadBodySearchInput){
		func(i *ThreadBodySearchInput) { i.Query = "" },
		func(i *ThreadBodySearchInput) { i.Query = strings.Repeat("x", 4097) },
		func(i *ThreadBodySearchInput) { i.Query = string([]byte{0xff}) },
		func(i *ThreadBodySearchInput) { i.Limit = 101 },
		func(i *ThreadBodySearchInput) { i.Limit = -1 },
		func(i *ThreadBodySearchInput) { i.Body.LimitBytes = 16 },
		func(i *ThreadBodySearchInput) { i.Body.OffsetBytes = 1 },
	} {
		input := valid
		mutate(&input)
		if _, err := service.SearchBody(t.Context(), scope, input); !errors.Is(err, threadquery.ErrInvalidQuery) {
			t.Fatalf("invalid search accepted: %+v %v", input, err)
		}
	}
	valid.Body.OffsetBytes, valid.Body.SHA256 = page.NextOffsetBytes, page.SHA256
	if _, err := service.SearchBody(t.Context(), threadquery.Scope{Authority: scope.Authority}, valid); !errors.Is(err, threadquery.ErrRecordUnavailable) {
		t.Fatalf("revoked scope: %v", err)
	}
	valid.Body.SHA256 = "changed"
	if _, err := service.SearchBody(t.Context(), scope, valid); !errors.Is(err, threadquery.ErrRecordUnavailable) {
		t.Fatalf("source mismatch: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := service.SearchBody(ctx, scope, valid); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
}

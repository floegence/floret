package runtime

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/outputbody"
	"github.com/floegence/floret/v7/threadquery"
)

func liveBodySearchFixture(t *testing.T, source string) (*threadRuntimeService, *threadRuntimeState, *outputbody.Body, threadquery.Scope) {
	t.Helper()
	service, actor, sink, full := nativeUpdatesFixture(t)
	full.Close()
	service.runtimes = map[string]*threadRuntimeState{"thread-live": actor}
	owner := outputbody.NewOwner()
	t.Cleanup(func() { _ = owner.Close() })
	body := owner.NewBody()
	if err := body.Append(source); err != nil {
		t.Fatal(err)
	}
	sink.EmitEvent(Event{ThreadID: "thread-live", TurnID: "turn-live", RunID: "run-live", outputSourceID: "source", outputBody: body, Stream: &StreamObservation{Type: StreamObservationAssistantDelta, Text: source}})
	return service, actor, body, threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{"thread-live"}}
}

func TestLiveBodySearchMatchesFrozenSourceAcrossBoundaries(t *testing.T) {
	source := strings.Repeat("x", (64<<10)-2) + "中文🌸\r\n" + strings.Repeat("ababa中文🌸\r\n", 1000) + "end"
	service, _, body, scope := liveBodySearchFixture(t, source)
	// Appends may grow the owner without extending the search's accepted extent.
	if err := body.Append("future-only中文"); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{"中文🌸", "aba", "\r\n", strings.Repeat("x", 4090) + "中文", "missing", "end", "future-only"} {
		t.Run(query[:min(20, len(query))], func(t *testing.T) {
			input := ThreadLiveBodySearchInput{Body: ThreadLiveBodyInput{ThreadID: "thread-live", RunID: "run-live", SourceID: "source", ThroughBytes: len(source)}, Query: query, Limit: 7}
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
				page, err := service.SearchLiveBody(t.Context(), scope, input)
				if err != nil {
					t.Fatal(err)
				}
				if page.ThreadID != input.Body.ThreadID || page.RunID != input.Body.RunID || page.SourceID != input.Body.SourceID || page.ThroughBytes != len(source) || len(page.Matches) > input.Limit || page.NextOffsetBytes < input.Body.OffsetBytes || page.NextOffsetBytes-input.Body.OffsetBytes > threadBodySearchBytes {
					t.Fatalf("unbounded or changed frozen page: %+v", page)
				}
				for _, match := range page.Matches {
					if match.LengthBytes != len(query) || source[match.OffsetBytes:match.OffsetBytes+match.LengthBytes] != query {
						t.Fatal("inexact match")
					}
					got = append(got, match.OffsetBytes)
				}
				if !page.HasMore {
					if page.NextOffsetBytes != len(source) {
						t.Fatal("incomplete coverage")
					}
					break
				}
				if page.NextOffsetBytes == input.Body.OffsetBytes {
					t.Fatal("non-progressing page")
				}
				input.Body.OffsetBytes = page.NextOffsetBytes
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("search differs: got %d want %d", len(got), len(want))
			}
		})
	}
}

func TestLiveBodySearchValidationAuthorityAndExpiry(t *testing.T) {
	source := strings.Repeat("中文\x00\r\n", 20000)
	service, actor, _, scope := liveBodySearchFixture(t, source)
	input := ThreadLiveBodySearchInput{Body: ThreadLiveBodyInput{ThreadID: "thread-live", RunID: "run-live", SourceID: "source", ThroughBytes: len(source)}, Query: "中文", Limit: 2}
	for _, mutate := range []func(*ThreadLiveBodySearchInput){
		func(i *ThreadLiveBodySearchInput) { i.Query = "" },
		func(i *ThreadLiveBodySearchInput) { i.Query = strings.Repeat("x", 4097) },
		func(i *ThreadLiveBodySearchInput) { i.Query = string([]byte{255}) },
		func(i *ThreadLiveBodySearchInput) { i.Limit = -1 },
		func(i *ThreadLiveBodySearchInput) { i.Limit = 101 },
		func(i *ThreadLiveBodySearchInput) { i.Body.LimitBytes = 16 },
		func(i *ThreadLiveBodySearchInput) { i.Body.OffsetBytes = 1 },
	} {
		invalid := input
		mutate(&invalid)
		if _, err := service.SearchLiveBody(t.Context(), scope, invalid); !errors.Is(err, threadquery.ErrInvalidQuery) {
			t.Fatalf("invalid accepted: %+v %v", invalid, err)
		}
	}
	if _, err := service.SearchLiveBody(t.Context(), threadquery.Scope{Authority: "revoked"}, input); !errors.Is(err, threadquery.ErrRecordUnavailable) {
		t.Fatal("revoked accepted", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := service.SearchLiveBody(ctx, scope, input); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := actor.apply(t.Context(), func() error { actor.discardAttemptDraft(); return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := service.SearchLiveBody(t.Context(), scope, input); !errors.Is(err, threadquery.ErrRecordUnavailable) {
		t.Fatal("expired accepted", err)
	}
}

func TestLiveBodySearchReadsSpilledSource(t *testing.T) {
	source := strings.Repeat("x", (4<<20)+3) + "中文🌸end"
	service, _, _, scope := liveBodySearchFixture(t, source)
	input := ThreadLiveBodySearchInput{Body: ThreadLiveBodyInput{ThreadID: "thread-live", RunID: "run-live", SourceID: "source", ThroughBytes: len(source), OffsetBytes: len(source) - 32}, Query: "中文🌸"}
	page, err := service.SearchLiveBody(t.Context(), scope, input)
	if err != nil || page.HasMore || len(page.Matches) != 1 || page.Matches[0].OffsetBytes != (4<<20)+3 {
		t.Fatalf("spilled source differs: %+v %v", page, err)
	}
}

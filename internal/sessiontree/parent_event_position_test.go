package sessiontree

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/floegence/floret/v7/internal/session"
)

func TestParentEventOrdinalWindowMatchesCanonicalPath(t *testing.T) {
	memory, ids := parentSourceFixture(t, 137)
	backend, repo := openV31ReviewFixture(t, memory)
	if err := repo.PrepareThread(t.Context(), "parent"); err != nil {
		t.Fatal(err)
	}
	page, err := memory.ParentEvents(t.Context(), ParentEventQuery{ThreadID: "parent", AfterEventID: ids[60], Limit: 7})
	if err != nil {
		t.Fatal(err)
	}
	// Journal ordinals also cover ordinary entries. The query must seek between
	// messages and receipts without reading every preceding receipt or its body.
	ordinal := page.Events[0].CanonicalOrdinal
	for _, query := range []ParentEventQuery{
		{ThreadID: "parent", AfterOrdinal: ordinal - 1, Limit: 7},
		{ThreadID: "parent", BeforeOrdinal: ordinal + 8, Limit: 7},
		{ThreadID: "parent", AfterOrdinal: ordinal - 1, BeforeOrdinal: ordinal + 3, Limit: 7},
	} {
		want, err := memory.ParentEvents(t.Context(), query)
		if err != nil {
			t.Fatal(err)
		}
		got, err := repo.ParentEvents(t.Context(), query)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("query=%+v got=%+v want=%+v err=%v", query, got, want, err)
		}
		for _, event := range got.Events {
			if query.AfterOrdinal > 0 && event.CanonicalOrdinal <= query.AfterOrdinal || query.BeforeOrdinal > 0 && event.CanonicalOrdinal >= query.BeforeOrdinal {
				t.Fatalf("out of window: %+v", event)
			}
		}
	}
	repo = reopenCurrentStartupRepo(t, backend, false)
	counted := &parentPageReadBackend{Backend: backend}
	repo.backend = counted
	meta, err := repo.Thread(t.Context(), "parent")
	if err != nil {
		t.Fatal(err)
	}
	query := ParentEventQuery{ThreadID: "parent", LeafID: meta.LeafID, AfterOrdinal: ordinal - 1, BeforeOrdinal: ordinal + 3, Limit: 7}
	got, err := repo.ParentEvents(t.Context(), query)
	if err != nil || len(got.Events) != 3 {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	if counted.scans != 0 || counted.gets > 200 {
		t.Fatalf("unbounded seek: gets=%d scans=%d", counted.gets, counted.scans)
	}
	// An append beyond the selected span does not change its rows.
	repo.backend = backend
	for _, store := range []Repo{memory, repo} {
		if _, err := store.Append(t.Context(), Entry{ID: "later", ThreadID: "parent", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "later"}}, AppendOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	after, err := repo.ParentEvents(t.Context(), query)
	if err != nil || !reflect.DeepEqual(after, got) {
		t.Fatalf("append changed interval: %v", err)
	}
	for i, q := range []ParentEventQuery{
		{ThreadID: "parent", AfterOrdinal: ordinal, AfterEventID: ids[0]},
		{ThreadID: "parent", BeforeOrdinal: ordinal, BeforeEventID: ids[1]},
		{ThreadID: "parent", AfterOrdinal: ordinal, BeforeOrdinal: ordinal},
		{ThreadID: "parent", AfterOrdinal: ordinal, Latest: true},
	} {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			for _, store := range []ParentEventRepo{memory, repo} {
				if _, err := store.ParentEvents(t.Context(), q); !errors.Is(err, ErrRequestConflict) {
					t.Fatalf("conflicting position accepted: %v", err)
				}
			}
		})
	}
}

func TestParentEventOrdinalSeeksPreserveBranchesAndEmptyRanges(t *testing.T) {
	memory, ids := parentSourceFixture(t, 137)
	backend, repo := openV31ReviewFixture(t, memory)
	if err := repo.PrepareThread(t.Context(), "parent"); err != nil {
		t.Fatal(err)
	}
	check := func(thread string) {
		t.Helper()
		for _, bounds := range [][2]uint64{{1, 0}, {0, 1}, {0, 2}, {1, 2}, {67, 72}, {60, 75}, {200, 0}, {0, 200}, {200, 201}} {
			for _, limit := range []int{1, 7, 100} {
				q := ParentEventQuery{ThreadID: thread, AfterOrdinal: bounds[0], BeforeOrdinal: bounds[1], Limit: limit}
				want, err := memory.ParentEvents(t.Context(), q)
				if err != nil {
					t.Fatal(err)
				}
				got, err := repo.ParentEvents(t.Context(), q)
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("query=%+v got=%+v want=%+v err=%v", q, got, want, err)
				}
			}
		}
	}
	check("parent")
	for _, store := range []Repo{memory, repo} {
		if _, err := store.Fork(t.Context(), ForkOptions{SourceThreadID: "parent", EntryID: ids[69], NewThreadID: "fork"}); err != nil {
			t.Fatal(err)
		}
		if err := store.MoveLeaf(t.Context(), "parent", ids[80]); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Append(t.Context(), Entry{ID: "new-path", ThreadID: "parent", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "new path"}}, AppendOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	check("parent")
	check("fork")
	repo = reopenCurrentStartupRepo(t, backend, false)
	check("parent")
	check("fork")
}

func TestParentEventSeekFrozenLeafRejectsReplacedPath(t *testing.T) {
	memory, ids := parentSourceFixture(t, 20)
	_, repo := openV31ReviewFixture(t, memory)
	for _, store := range []ParentEventRepo{memory, repo} {
		full, err := store.ParentEvents(t.Context(), ParentEventQuery{ThreadID: "parent", Limit: 100})
		if err != nil || full.LeafID != ids[19] {
			t.Fatalf("full leaf=%+v err=%v", full, err)
		}
		q := ParentEventQuery{ThreadID: "parent", LeafID: ids[10], AfterOrdinal: 1, Limit: 100}
		frozen, err := store.ParentEvents(t.Context(), q)
		if err != nil || len(frozen.Events) != 11 || frozen.LeafID != ids[10] || frozen.LastEventID != ids[10] {
			t.Fatalf("frozen=%+v err=%v", frozen, err)
		}
	}
	for _, store := range []Repo{memory, repo} {
		if err := store.MoveLeaf(t.Context(), "parent", ids[5]); err != nil {
			t.Fatal(err)
		}
	}
	for _, store := range []ParentEventRepo{memory, repo} {
		if _, err := store.ParentEvents(t.Context(), ParentEventQuery{ThreadID: "parent", LeafID: ids[10], Limit: 7}); !errors.Is(err, ErrEntryNotFound) {
			t.Fatalf("accepted removed leaf: %v", err)
		}
	}
}

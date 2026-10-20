package sessiontree

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/floegence/floret/v7/internal/session"
)

func TestOutputSourceIndexMatchesCanonicalBranches(t *testing.T) {
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	root := appendPathTestEntry(t, repo, "root", "")
	random := rand.New(rand.NewPCG(31, 67))
	parents := []Entry{root}
	for i := range 600 {
		parent := parents[random.IntN(len(parents))]
		source := fmt.Sprintf("source-%d", random.IntN(12))
		entry := Entry{ID: fmt.Sprintf("entry-%d", i), ThreadID: "thread", ParentID: parent.ID, TurnID: fmt.Sprintf("turn-%d", random.IntN(3)), RunID: "run", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Reasoning: fmt.Sprintf("reason-%d", random.IntN(2)), ReasoningSourceID: source}}
		want := false
		for current := entry.ParentID; current != ""; {
			previous := repo.entries["thread"][repo.entryOrdinals["thread"][current]]
			if previous.Message.ReasoningSourceID == source {
				old, _ := outputSourceFacts(previous)
				next, _ := outputSourceFacts(entry)
				want = want || !sameOutputSource(old[source], next[source])
			}
			current = previous.ParentID
		}
		saved, err := repo.Append(t.Context(), entry, AppendOptions{})
		if want {
			if !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("conflict admitted at %d: %v", i, err)
			}
		} else {
			if err != nil {
				t.Fatal(err)
			}
			parents = append(parents, saved)
		}
	}
	if err := validateOutputSources(repo); err != nil {
		t.Fatal(err)
	}
	// A full replacement and a legacy checkpoint decode rebuild the same derived
	// locators without changing canonical identities or persisting the index.
	if err := repo.replaceIndexedEntriesLocked("thread", repo.entries["thread"]); err != nil {
		t.Fatal(err)
	}
	encoded, err := repo.EncodeMemoryState()
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := DecodeMemoryState(encoded, repo.now)
	if err != nil {
		t.Fatal(err)
	}
	for _, candidate := range []*MemoryRepo{repo, reopened} {
		leaf := parents[len(parents)-1]
		entry := Entry{ID: "continued", ThreadID: "thread", ParentID: leaf.ID, Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "continued", ContentSourceID: "new-source"}}
		if _, err := candidate.Append(t.Context(), entry, AppendOptions{}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestOutputSourceIndexRollbackDoesNotPoisonFutureAdmission(t *testing.T) {
	repo := NewMemoryRepo()
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	appendPathTestEntry(t, repo, "root", "")
	for i := range 3 {
		source := fmt.Sprintf("source-%d", i)
		if _, err := repo.AppendRuntimeFacts(t.Context(), "thread", []Entry{
			{ID: source, Type: EntryAssistantMessage, TurnID: "old", RunID: "run", Message: session.Message{Role: session.Assistant, Reasoning: "discard", ReasoningSourceID: source}},
			{ID: "invalid", ParentID: "missing", Type: EntryQueueAdded},
		}); !errors.Is(err, ErrInvalidParent) {
			t.Fatal(err)
		}
		if len(repo.outputSources["thread"][source]) != 0 {
			t.Fatal("rollback retained a source locator")
		}
		if _, err := repo.Append(t.Context(), Entry{ThreadID: "thread", Type: EntryAssistantMessage, TurnID: "new", RunID: "run", Message: session.Message{Role: session.Assistant, Reasoning: "keep", ReasoningSourceID: source}}, AppendOptions{ID: source}); err != nil {
			t.Fatal(err)
		}
	}
}

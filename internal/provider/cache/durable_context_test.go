package cache

import (
	"errors"
	"testing"

	"github.com/floegence/floret/v7/internal/session"
)

func TestCanonicalLineageFreezesRenderedReferencesAndContext(t *testing.T) {
	store := NewMemoryStore()
	canonical := []session.Message{{Role: session.User, EntryID: "selected", References: []session.MessageReference{
		{ReferenceID: "one", Kind: session.MessageReferenceText, Label: "udesk26", Text: "ssh:host:udesk26"},
		{ReferenceID: "two", Kind: session.MessageReferenceText, Label: "Scope", Text: "memory"},
	}, Context: []session.MessageContextItem{{Kind: "runtime", Title: "Execution", Text: "local"}}}}
	history, _, err := session.ProjectProviderHistory(canonical, "")
	if err != nil {
		t.Fatal(err)
	}
	first := lineagePlan("test", "model", "ns", "selected")
	if err := ValidateCanonicalLineage(t.Context(), store, "thread", &first, "system", nil, nil, nil, history); err != nil {
		t.Fatal(err)
	}
	if _, err := RecordRequest(t.Context(), store, PromptScopeRef{PromptScopeID: "thread", ThreadID: "thread", RunID: "run", TurnID: "turn"}, 1, "test", "model", CachePolicy{}, first); err != nil {
		t.Fatal(err)
	}
	for _, change := range []string{"delete_reference", "reorder", "replace_context", "delete_message"} {
		t.Run(change, func(t *testing.T) {
			changed := session.CloneMessages(canonical)
			switch change {
			case "delete_reference":
				changed[0].References = changed[0].References[1:]
			case "reorder":
				changed[0].References[0], changed[0].References[1] = changed[0].References[1], changed[0].References[0]
			case "replace_context":
				changed[0].Context[0].Text = "remote"
			case "delete_message":
				changed = nil
			}
			projected, _, err := session.ProjectProviderHistory(changed, "")
			if err != nil {
				t.Fatal(err)
			}
			plan := lineagePlan("test", "model", "ns", "changed")
			if err := ValidateCanonicalLineage(t.Context(), store, "thread", &plan, "system", nil, nil, nil, projected); !errors.Is(err, ErrContextPrefixDrift) {
				t.Fatalf("unexpected drift result: %v", err)
			}
		})
	}
}

func TestCanonicalRetryBoundaryAllowsOneExactReset(t *testing.T) {
	store := NewMemoryStore()
	history := []session.Message{{Role: session.User, EntryID: "source", Content: "input"}, {Role: session.Assistant, Content: "previous attempt"}}
	first := lineagePlan("test", "model", "ns", "source", "answer")
	if err := ValidateCanonicalLineage(t.Context(), store, "thread", &first, "system", nil, nil, nil, history); err != nil {
		t.Fatal(err)
	}
	if _, err := RecordRequest(t.Context(), store, PromptScopeRef{PromptScopeID: "thread", RunID: "first"}, 2, "test", "model", CachePolicy{}, first); err != nil {
		t.Fatal(err)
	}
	retry := lineagePlan("test", "model", "ns", "source")
	retry.RetryRunID = "retry"
	retry.RetrySourceEntryID = "wrong"
	if err := ValidateCanonicalLineage(t.Context(), store, "thread", &retry, "system", nil, nil, nil, history[:1]); !errors.Is(err, ErrContextPrefixDrift) {
		t.Fatalf("wrong source accepted: %v", err)
	}
	retry.RetrySourceEntryID = "source"
	if err := ValidateCanonicalLineage(t.Context(), store, "thread", &retry, "system", nil, nil, nil, history[:1]); err != nil || !retry.CanonicalLineageReset {
		t.Fatalf("canonical retry rejected: %v", err)
	}
	if _, err := RecordRequest(t.Context(), store, PromptScopeRef{PromptScopeID: "thread", RunID: "retry"}, 1, "test", "model", CachePolicy{}, retry); err != nil {
		t.Fatal(err)
	}
	changed := session.CloneMessages(history[:1])
	changed[0].Content = "rewritten"
	retry.CanonicalLineageReset = false
	if err := ValidateCanonicalLineage(t.Context(), store, "thread", &retry, "system", nil, nil, nil, changed); !errors.Is(err, ErrContextPrefixDrift) {
		t.Fatalf("retry disabled prefix checks: %v", err)
	}
}

func TestCanonicalRetryAllowsOnlyAdmittedFactsAfterSource(t *testing.T) {
	for _, kind := range []session.MessageKind{session.MessageKindModelContext, session.MessageKindGoalContext, session.MessageKindHostWork, session.MessageKindNormal} {
		store := NewMemoryStore()
		history := []session.Message{{Role: session.User, EntryID: "source", Content: "input"}}
		first := lineagePlan("test", "model", "ns", "source")
		if err := ValidateCanonicalLineage(t.Context(), store, "thread", &first, "system", nil, nil, nil, history); err != nil {
			t.Fatal(err)
		}
		if _, err := RecordRequest(t.Context(), store, PromptScopeRef{PromptScopeID: "thread", RunID: "first"}, 1, "test", "model", CachePolicy{}, first); err != nil {
			t.Fatal(err)
		}
		next := append(session.CloneMessages(history), session.Message{Role: session.User, Kind: kind, EntryID: "new", Content: "fresh runtime facts"})
		retry := lineagePlan("test", "model", "ns", "source", "new")
		retry.RetryRunID, retry.RetrySourceEntryID = "retry", "source"
		err := ValidateCanonicalLineage(t.Context(), store, "thread", &retry, "system", nil, nil, nil, next)
		if kind != session.MessageKindNormal && err != nil {
			t.Fatal(err)
		}
		if kind == session.MessageKindNormal && !errors.Is(err, ErrContextPrefixDrift) {
			t.Fatalf("ordinary suffix accepted: %v", err)
		}
		if kind == session.MessageKindHostWork {
			next[len(next)-1].EntryID = ""
			if err := ValidateCanonicalLineage(t.Context(), store, "thread", &retry, "system", nil, nil, nil, next); !errors.Is(err, ErrContextPrefixDrift) {
				t.Fatalf("host work without canonical identity accepted: %v", err)
			}
		}
	}
}

func TestMessageRevisionResetsLineageOnceAndRetainsAudit(t *testing.T) {
	store := NewMemoryStore()
	original := []session.Message{{Role: session.User, EntryID: "old", Content: "old"}}
	first := lineagePlan("test", "model", "ns", "old")
	if err := ValidateCanonicalLineage(t.Context(), store, "thread", &first, "system", nil, nil, nil, original); err != nil {
		t.Fatal(err)
	}
	if _, err := RecordRequest(t.Context(), store, PromptScopeRef{PromptScopeID: "thread", RunID: "old"}, 1, "test", "model", CachePolicy{}, first); err != nil {
		t.Fatal(err)
	}
	changed := []session.Message{{Role: session.User, EntryID: "new", Content: "replacement"}}
	next := lineagePlan("test", "model", "ns", "new")
	next.MessageRevisionID = "revision"
	if err := ValidateCanonicalLineage(t.Context(), store, "thread", &next, "system", nil, nil, nil, changed); err != nil || !next.CanonicalLineageReset {
		t.Fatalf("revision %v %+v", err, next)
	}
	if _, err := RecordRequest(t.Context(), store, PromptScopeRef{PromptScopeID: "thread", RunID: "new"}, 1, "test", "model", CachePolicy{}, next); err != nil {
		t.Fatal(err)
	}
	next.CanonicalLineageReset = false
	changed[0].Content = "illegal rewrite"
	if err := ValidateCanonicalLineage(t.Context(), store, "thread", &next, "system", nil, nil, nil, changed); !errors.Is(err, ErrContextPrefixDrift) {
		t.Fatalf("unchecked later rewrite: %v", err)
	}
}

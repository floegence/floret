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

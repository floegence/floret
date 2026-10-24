package sessiontree

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/session/contextpolicy"
	"github.com/floegence/floret/v7/observation"
	"github.com/floegence/floret/v7/storage/spi"
)

func contextProjectionFixture(t testing.TB) []Entry {
	t.Helper()
	var entries []Entry
	turn, run := "turn", "run"
	add := func(entry Entry, err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		entry.ID = fmt.Sprintf("context-%d", len(entries))
		entry.CreatedAt = time.Unix(100+int64(len(entries)), 0).UTC()
		entries = append(entries, entry)
	}
	policy := func(model string) {
		add(NewThreadContextPolicyEntry("thread", turn, run, "test", model, contextpolicy.Policy{ContextWindowTokens: 128000}))
	}
	status := func(model string, confirmed bool) {
		s := observation.ContextStatus{ThreadID: "thread", TurnID: identity.TurnID(turn), RunID: identity.RunID(run), Provider: "test", Model: model, Phase: observation.ContextPhaseProjectedRequest, Status: observation.ContextStatusEstimated}
		if confirmed {
			s.Phase = observation.ContextPhaseProviderUsage
			s.Status = observation.ContextStatusStable
			s.Usage = observation.ProviderUsage{Available: true, InputTokensReported: true, InputTokenScope: "request_input", InputTokens: 80, OutputTokens: 20, CacheReadTokens: 15, CacheWriteTokens: 5}
		}
		add(NewThreadContextStatusEntry(s))
	}
	compact := func(id, run, phase, status string) {
		add(NewThreadContextCompactionEntry(ThreadContextCompaction{ThreadID: "thread", TurnID: turn, RunID: run, OperationID: id, RequestID: "request", Phase: phase, Status: status, TokensBefore: 1000, TokensAfterEstimate: 100}))
	}
	policy("first")
	status("first", false)
	status("first", true)
	status("first", false)
	policy("first") // Identical policy clears legacy Usage, preserving Confirmed/Estimate.
	compact("complete", "run", "start", "running")
	compact("complete", "run", "complete", "compacted")
	status("first", true)
	compact("cancelled", "run", "start", "running")
	compact("other", "other", "start", "running")
	add(Entry{ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryTurnMarker, TurnStatus: TurnAborted, Metadata: map[string]string{TurnFailureCodeMetadataKey: TurnFailureCancelled}}, nil)
	turn, run = "second", "second-run"
	add(Entry{ThreadID: "thread", TurnID: turn, RunID: run, Type: EntryTurnMarker, TurnStatus: TurnStarted}, nil)
	policy("second")
	status("second", false)
	status("second", true)
	compact("later", "second-run", "start", "running")
	compact("later", "second-run", "complete", "compacted")
	add(Entry{ThreadID: "thread", TurnID: turn, RunID: run, Type: EntryTurnMarker, TurnStatus: TurnCompleted}, nil)
	return entries
}

func assertContextProjection(t *testing.T, repo *BackendRepo, thread string) ThreadContextSnapshot {
	t.Helper()
	path, err := repo.Path(t.Context(), thread, "")
	if err != nil {
		t.Fatal(err)
	}
	var full ThreadContextProjection
	for _, entry := range path {
		if err := full.Observe(entry); err != nil {
			t.Fatal(err)
		}
	}
	want, err := full.Snapshot(time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	got, err := repo.ReadThreadContextProjection(t.Context(), thread, "")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		a, _ := json.Marshal(got)
		b, _ := json.Marshal(want)
		t.Fatalf("context differs\ngot: %s\nwant: %s", a, b)
	}
	return got
}
func TestStoredContextProjectionMatchesEveryPrefixAndRestart(t *testing.T) {
	backend, repo := projectionSourcesFixture(t, 0)
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "context"}); err != nil {
		t.Fatal(err)
	}
	assertContextProjection(t, repo, "context")
	for i, entry := range contextProjectionFixture(t) {
		entry.ThreadID = "context"
		if _, err := repo.Append(t.Context(), entry, AppendOptions{ID: entry.ID, Now: entry.CreatedAt}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			assertContextProjection(t, repo, "context")
			if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
				var err error
				repo, _, err = NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, false)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			assertContextProjection(t, repo, "context")
		})
	}
	if err := repo.MoveLeaf(t.Context(), "context", "context-4"); err != nil {
		t.Fatal(err)
	}
	assertContextProjection(t, repo, "context")
	if _, err := repo.Append(t.Context(), Entry{ThreadID: "context", TurnID: "third", RunID: "third-run", Type: EntryTurnMarker, TurnStatus: TurnStarted}, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	next, err := NewThreadContextPolicyEntry("context", "third", "third-run", "test", "third", contextpolicy.Policy{ContextWindowTokens: 64000})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Append(t.Context(), next, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	assertContextProjection(t, repo, "context")
	if _, err := repo.Fork(t.Context(), ForkOptions{SourceThreadID: "context", NewThreadID: "context-fork"}); err != nil {
		t.Fatal(err)
	}
	assertContextProjection(t, repo, "context-fork")
}

func TestStoredContextProjectionColdReadDoesNotGrowWithUsageHistory(t *testing.T) {
	reads := 0
	for _, count := range []int{32, 320} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			memory := newMemoryRepo(time.Now)
			if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
				t.Fatal(err)
			}
			entries := contextProjectionFixture(t)
			if _, err := memory.Append(t.Context(), entries[0], AppendOptions{}); err != nil {
				t.Fatal(err)
			}
			for i := range count {
				e := entries[2]
				e.ID = fmt.Sprint("usage-", i)
				if _, err := memory.Append(t.Context(), e, AppendOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			backend, repo := openV31ReviewFixture(t, memory)
			want := assertContextProjection(t, repo, "thread")
			if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
				var err error
				repo, _, err = NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, false)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			counted := &membershipReadBackend{Backend: backend}
			repo.backend = counted
			got, err := repo.ReadThreadContextProjection(t.Context(), "thread", "")
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("error=%v got=%+v want=%+v", err, got, want)
			}
			if counted.scans != 0 {
				t.Fatal("context scanned history")
			}
			if reads == 0 {
				reads = counted.sources
			} else if reads != counted.sources {
				t.Fatalf("source reads grew %d -> %d", reads, counted.sources)
			}
		})
	}
}

func TestStoredContextProjectionRollbackAndCorruptSources(t *testing.T) {
	backend, repo := projectionSourcesFixture(t, 0)
	entries := contextProjectionFixture(t)
	for _, e := range entries[:4] {
		if _, err := repo.Append(t.Context(), e, AppendOptions{ID: e.ID}); err != nil {
			t.Fatal(err)
		}
	}
	before := assertContextProjection(t, repo, "thread")
	records := cloneMigrationRecords(backend.records)
	failure := errors.New("commit failed")
	repo.backend = &domainCommitBoundaryBackend{Backend: backend, afterCallback: func() error { return failure }}
	if _, err := repo.Append(t.Context(), entries[4], AppendOptions{ID: entries[4].ID}); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(records, backend.records) {
		t.Fatal("rollback changed canonical data or context metadata")
	}
	repo.backend = backend
	got := assertContextProjection(t, repo, "thread")
	if !reflect.DeepEqual(before, got) {
		t.Fatal("rollback changed context")
	}
	for _, kind := range []string{"hash", "source", "extra", "kind"} {
		t.Run(kind, func(t *testing.T) {
			backend.records = cloneMigrationRecords(records)
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				var stored storedContextProjection
				if err := readItemJSON(tx, contextProjectionKey("thread"), &stored); err != nil {
					return err
				}
				switch kind {
				case "hash":
					stored.Refs[0].Hash = "wrong"
				case "source":
					stored.Sources.Usage = "absent"
				case "extra":
					stored.Refs = append(stored.Refs, stored.Refs[0])
				case "kind":
					stored.Sources.Usage = stored.Sources.Policy
				}
				return putItemJSON(tx, contextProjectionKey("thread"), stored)
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := repo.ReadThreadContextProjection(t.Context(), "thread", ""); !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("accepted %s: %v", kind, err)
			}
		})
	}
}

func TestStoredContextProjectionPreservesDeferredActivityAuthority(t *testing.T) {
	_, repo := projectionSourcesFixture(t, 0)
	if _, err := repo.CreateThread(t.Context(), ThreadMeta{ID: "activity"}); err != nil {
		t.Fatal(err)
	}
	observation := Entry{ID: "observation", ThreadID: "activity", TurnID: "turn", Type: EntryCustom, Metadata: map[string]string{"kind": "tool_activity", "tool_id": " work "}}
	if _, err := repo.Append(t.Context(), observation, AppendOptions{ID: observation.ID}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.ReadThreadContextProjection(t.Context(), "activity", ""); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("unresolved activity accepted: %v", err)
	}
	call := Entry{ID: "authority", ThreadID: "activity", TurnID: "turn", RunID: "run", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, ToolCallID: "work", ToolName: "inspect", ToolArgs: "{}"}}
	if _, err := repo.Append(t.Context(), call, AppendOptions{ID: call.ID}); err != nil {
		t.Fatal(err)
	}
	assertContextProjection(t, repo, "activity")
}

var _ ThreadContextProjectionReader = (*BackendRepo)(nil)
var _ ThreadContextProjectionReader = (*MemoryRepo)(nil)

package sessiontree

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/storagebridge"
	publicstorage "github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/tools"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestItemRangeMatchesFullMembershipAndTransactionalUpdates(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	add := func(repo Repo, e Entry) {
		t.Helper()
		e.ThreadID = "thread"
		if _, err := repo.Append(t.Context(), e, AppendOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	add(memory, Entry{ID: "user", TurnID: "turn", RunID: "run", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "inspect"}})
	for i := range 50 {
		id := fmt.Sprint("call-", i)
		add(memory, Entry{ID: id, TurnID: "turn", RunID: "run", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: id}})
	}
	add(memory, Entry{ID: "end", TurnID: "turn", RunID: "run", Type: EntryAssistantMessage, Message: session.Message{Role: session.Assistant, Content: "complete"}})
	backend, repo := openV31ReviewFixture(t, memory)
	if err := repo.PrepareThread(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	check := func(anchor string) {
		t.Helper()
		input := ItemMembershipRangeOptions{ThreadID: "thread", AnchorID: anchor, Kinds: []string{"thinking", "tool"}}
		want, err := memory.ItemMembershipRange(t.Context(), input)
		if err != nil {
			t.Fatal(err)
		}
		got, err := repo.ItemMembershipRange(t.Context(), input)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("got=%+v want=%+v err=%v", got, want, err)
		}
	}
	check("tool:turn:call-25")
	entry := Entry{ID: "new-call", ThreadID: "thread", TurnID: "second", RunID: "other", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: "new"}}
	before := cloneMigrationRecords(backend.records)
	failure := errors.New("rollback")
	repo.backend = &domainCommitBoundaryBackend{Backend: backend, afterCallback: func() error { return failure }}
	if _, err := repo.Append(t.Context(), entry, AppendOptions{}); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, backend.records) {
		t.Fatal("failed append published range metadata")
	}
	repo.backend = backend
	add(memory, entry)
	add(repo, entry)
	check("tool:second:new")
	repo = reopenCurrentStartupRepo(t, backend, false)
	check("tool:turn:call-10")
	if err := repo.MoveLeaf(t.Context(), "thread", "call-20"); err != nil {
		t.Fatal(err)
	}
	if err := memory.MoveLeaf(t.Context(), "thread", "call-20"); err != nil {
		t.Fatal(err)
	}
	check("tool:turn:call-10")
}

// The large fixture isolates the interval data structure. Canonical membership
// parity and publication are covered above; this test is not product timing.
func TestItemRangeTreeUpdatesAndReadBudget(t *testing.T) {
	for _, count := range []int{1000, 10000, 100000} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			backend := newMigrationTestBackend()
			members := make([]ItemMember, count)
			for i := range members {
				members[i] = ItemMember{ID: fmt.Sprint(i), Kind: "tool", TurnID: "turn", RunID: "run", Slot: i}
			}
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return storeItemRanges(t.Context(), tx, "thread", members) }); err != nil {
				t.Fatal(err)
			}
			measured := &rangeReadTx{}
			if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
				measured.ReadTx = tx
				root, err := readItemRangeSummary(measured, "thread", rangeLevel(count), 0)
				if err != nil {
					return err
				}
				if root.Counts.Tool != count || root.Mixed {
					t.Fatalf("bad aggregate: %+v", root)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if measured.gets != 1 || measured.body != 0 {
				t.Fatal("range root read members")
			}
			members[count/2].Hidden = true
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return updateItemRange(tx, "thread", count, members[count/2]) }); err != nil {
				t.Fatal(err)
			}
			if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
				root, err := readItemRangeSummary(tx, "thread", rangeLevel(count), 0)
				if err == nil && root.Counts.Tool != count-1 {
					t.Fatal("hidden member retained")
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
			members[count/2].Hidden = false
			members[count/2].Kind = "interaction"
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return updateItemRange(tx, "thread", count, members[count/2]) }); err != nil {
				t.Fatal(err)
			}
			if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
				root, err := readItemRangeSummary(tx, "thread", rangeLevel(count), 0)
				if err == nil && (root.Counts.Tool != count-1 || root.Counts.Interaction != 1) {
					t.Fatal("revealed boundary missing")
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
			t.Logf("members=%d root_metadata_reads=%d body_reads=%d", count, measured.gets, measured.body)
		})
	}
}
func rangeLevel(count int) int {
	level := 0
	for size := 1; size < count; size *= 2 {
		level++
	}
	return level
}

func TestItemRangeActivityCountsFollowLateResults(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	for i := range 12 {
		id := fmt.Sprint("call-", i)
		if _, err := memory.Append(t.Context(), Entry{ID: id, ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: id}}, AppendOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	backend, repo := openV31ReviewFixture(t, memory)
	if err := repo.PrepareThread(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	input := ItemMembershipRangeOptions{ThreadID: "thread", AnchorID: "tool:turn:call-6", Kinds: []string{"tool"}}
	first, err := repo.ItemMembershipRange(t.Context(), input)
	if err != nil {
		t.Fatal(err)
	}
	if first.Activity.Running != 12 || first.Activity.Error != 0 {
		t.Fatalf("initial facts: %+v", first.Activity)
	}
	entry := Entry{ID: "late-result", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "inspect", ToolCallID: "call-2", ToolResult: &session.ToolResultView{Status: "error"}}}
	before := cloneMigrationRecords(backend.records)
	rollback := errors.New("range rollback")
	repo.backend = &domainCommitBoundaryBackend{Backend: backend, afterCallback: func() error { return rollback }}
	if _, err := repo.Append(t.Context(), entry, AppendOptions{}); !errors.Is(err, rollback) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, backend.records) {
		t.Fatal("late result published counts before transaction commit")
	}
	repo.backend = backend
	if _, err := repo.Append(t.Context(), entry, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := memory.Append(t.Context(), entry, AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	for _, reader := range []ItemMembershipRangeReader{repo, memory, reopenCurrentStartupRepo(t, backend, false)} {
		got, err := reader.ItemMembershipRange(t.Context(), input)
		if err != nil {
			t.Fatal(err)
		}
		if got.Counts.Tool != 12 || got.Activity.Running != 11 || got.Activity.Error != 1 || got.First.ID != first.First.ID || got.Last.ID != first.Last.ID {
			t.Fatalf("late result changed range or lost status: %+v", got)
		}
	}
}

func TestItemRangeExcludesCanonicalSafetyFacts(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	for i := range 10 {
		id := fmt.Sprint("call-", i)
		if _, err := memory.Append(t.Context(), Entry{ID: id, ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: id}}, AppendOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	backend, repo := openV31ReviewFixture(t, memory)
	if err := repo.PrepareThread(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	input := ItemMembershipRangeOptions{ThreadID: "thread", AnchorID: "tool:turn:call-6", Kinds: []string{"tool"}, Exclude: ItemRangeUncertainEffect}
	if _, err := repo.ItemMembershipRange(t.Context(), input); err != nil {
		t.Fatal(err)
	}
	appendBoth := func(id string, presentation *tools.ActivityPresentation) {
		if err := presentation.Validate(); err != nil {
			t.Fatalf("fixture %s: %v", id, err)
		}
		entry := Entry{ID: id, ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "inspect", ToolCallID: "call-4", Activity: presentation}}
		for _, r := range []Repo{repo, memory} {
			if _, err := r.Append(t.Context(), entry, AppendOptions{}); err != nil {
				t.Fatalf("append %s to %T: %v", id, r, err)
			}
		}
	}
	appendBoth("uncertain", &tools.ActivityPresentation{Renderer: tools.ActivityRendererTerminal, Chips: []tools.ActivityChip{{Kind: "effect_outcome", Label: "Effect", Value: "uncertain"}}, Payload: tools.TerminalActivityPayload{Output: strings.Repeat("hidden", 1000)}})
	for _, r := range []ItemMembershipRangeReader{repo, memory, reopenCurrentStartupRepo(t, backend, false)} {
		got, err := r.ItemMembershipRange(t.Context(), input)
		if err != nil || got.First.ID != "tool:turn:call-5" || got.Counts.Tool != 5 {
			t.Fatalf("unsafe item folded into range: %+v %v", got, err)
		}
	}
	appendBoth("resolved", &tools.ActivityPresentation{Chips: []tools.ActivityChip{{Kind: "status", Label: "Complete"}}})
	got, err := repo.ItemMembershipRange(t.Context(), input)
	if err != nil || got.First.ID != "tool:turn:call-0" || got.Counts.Tool != 10 {
		t.Fatalf("updated safety facts did not merge range: %+v %v", got, err)
	}
}

type rangeReadTx struct {
	spi.ReadTx
	gets, body int
}

func (tx *rangeReadTx) Get(ns string, key []byte) ([]byte, error) {
	tx.gets++
	if ns == canonicalBodyNamespace {
		tx.body++
	}
	return tx.ReadTx.Get(ns, key)
}
func TestItemRangeRejectsInvalidQueryAndCorruptNode(t *testing.T) {
	backend, repo := projectionSourcesFixture(t, 8)
	input := ItemMembershipRangeOptions{ThreadID: "thread", AnchorID: "tool:turn:work", Kinds: []string{"tool"}}
	if _, err := repo.ItemMembershipRange(t.Context(), input); err != nil {
		t.Fatal(err)
	}
	input.Kinds = []string{"unknown"}
	if _, err := repo.ItemMembershipRange(t.Context(), input); !errors.Is(err, spi.ErrInvalidArgument) {
		t.Fatal(err)
	}
	input.Kinds = []string{"tool"}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		return tx.Put(itemMembershipNamespace, itemRangeKey("thread", 4, 0), []byte("{}"))
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.ItemMembershipRange(t.Context(), input); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("corrupt node accepted: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := repo.ItemMembershipRange(ctx, input); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestItemRangeRejectsDriftedManifestAndRoot(t *testing.T) {
	for _, damage := range []string{"hash", "visible", "root"} {
		t.Run(damage, func(t *testing.T) {
			backend, repo := projectionSourcesFixture(t, 8)
			input := ItemMembershipRangeOptions{ThreadID: "thread", AnchorID: "tool:turn:work", Kinds: []string{"tool"}}
			if _, err := repo.ItemMembershipRange(t.Context(), input); err != nil {
				t.Fatal(err)
			}
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				m, _, err := readItemManifest(tx, "thread")
				if err != nil {
					return err
				}
				switch damage {
				case "hash":
					m.Hash = "wrong"
				case "visible":
					m.Visible--
				case "root":
					level := rangeLevel(m.Slots)
					root, err := readItemRangeSummary(tx, "thread", level, 0)
					if err != nil {
						return err
					}
					root.Counts.User++
					return putItemRangeSummary(tx, "thread", level, 0, root)
				}
				return putItemJSON(tx, itemManifestKey("thread"), m)
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := repo.ItemMembershipRange(t.Context(), input); !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("accepted drifted %s: %v", damage, err)
			}
		})
	}
}

// Exercise the public internal reader, including canonical leaf/source checks,
// both boundary searches, exact counts, and rank resolution. This is a source
// access budget, never a claim about click-to-interactive product latency.
func TestItemRangeCompleteQueryReadBudget(t *testing.T) {
	for _, count := range []int{1000, 10000, 100000} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			memory := newMemoryRepo(time.Now)
			if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
				t.Fatal(err)
			}
			for i := range count + 2 {
				entry := Entry{ID: fmt.Sprint("source-", i), ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolCall,
					Message: session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: fmt.Sprint(i)}}
				if i == 0 || i == count+1 {
					entry.Type = EntryUserMessage
					entry.Message = session.Message{Role: session.User, Content: "boundary"}
				}
				if _, err := memory.Append(t.Context(), entry, AppendOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			backend, err := storagebridge.Open(t.Context(), storagebridge.Source(publicstorage.SQLite(t.TempDir()+"/ranges.sqlite")))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = backend.Close() })
			// Build with the canonical writer on the transactional fixture backend,
			// then copy its exact records into SQLite. The measured reopened reader
			// still performs every ancestry, source and range check on real SQLite;
			// repeated race-instrumented SQL during fixture construction is unrelated
			// to this query budget and previously exhausted the CI shard timeout.
			fixture := newMigrationTestBackend()
			var repo *BackendRepo
			if err := fixture.Update(t.Context(), func(tx spi.WriteTx) error {
				if err := saveCompleteBackendDomain(tx, memory, backendDomainV32Format); err != nil {
					return err
				}
				var err error
				repo, _, err = NewCurrentBackendRepoInTransaction(t.Context(), fixture, tx, time.Now, false)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if err := repo.PrepareThread(t.Context(), "thread"); err != nil {
				t.Fatal(err)
			}
			input := ItemMembershipRangeOptions{ThreadID: "thread", AnchorID: fmt.Sprint("tool:turn:", count/2), Kinds: []string{"tool", "thinking"}}
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				for namespace, records := range fixture.records {
					batch := make([]spi.Record, 0, storagebridge.MaxRecordBatch)
					for key, value := range records {
						batch = append(batch, spi.Record{Key: []byte(key), Value: value})
						if len(batch) == storagebridge.MaxRecordBatch {
							if err := storagebridge.WriteRecords(tx, namespace, batch); err != nil {
								return err
							}
							batch = batch[:0]
						}
					}
					if len(batch) > 0 {
						if err := storagebridge.WriteRecords(tx, namespace, batch); err != nil {
							return err
						}
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			// Query preparation uses SQLite's bounded transactions, not repeated
			// whole-map copies in the rollback-oriented in-memory fixture backend.
			repo.backend = backend
			if _, err := repo.ItemMembershipRange(t.Context(), input); err != nil {
				t.Fatal(err)
			}
			if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
				var err error
				repo, _, err = NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, false)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			measured := &rangeQueryBackend{Backend: backend}
			repo.backend = measured
			got, err := repo.ItemMembershipRange(t.Context(), input)
			if err != nil {
				t.Fatal(err)
			}
			if got.Counts.Tool != count || got.First.ID != "tool:turn:1" || got.Last.ID != fmt.Sprint("tool:turn:", count) || got.FirstOrdinal != 2 || got.LastOrdinal != uint64(count+1) || !got.HasBefore || !got.HasAfter {
				t.Fatalf("wrong range: %+v", got)
			}
			if measured.scans != 0 || measured.bodies != 0 || measured.gets > 80*rangeLevel(count+2) {
				t.Fatalf("unbounded reads: %+v", measured)
			}
			t.Logf("members=%d gets=%d metadata_bytes=%d canonical_reads=%d scans=%d body_reads=%d", count, measured.gets, measured.bytes, measured.sources, measured.scans, measured.bodies)
		})
	}
}

type rangeQueryBackend struct {
	spi.Backend
	gets, bytes, sources, scans, bodies int
}
type rangeQueryTx struct {
	spi.ReadTx
	owner *rangeQueryBackend
}

func (b *rangeQueryBackend) View(ctx context.Context, f func(spi.ReadTx) error) error {
	return b.Backend.View(ctx, func(tx spi.ReadTx) error { return f(rangeQueryTx{tx, b}) })
}
func (tx rangeQueryTx) Get(ns string, key []byte) ([]byte, error) {
	tx.owner.gets++
	if ns == canonicalBodyNamespace {
		tx.owner.bodies++
		return nil, errors.New("range read a body")
	}
	if ns == backendDomainV32Namespace {
		tx.owner.sources++
	}
	raw, err := tx.ReadTx.Get(ns, key)
	tx.owner.bytes += len(raw)
	return raw, err
}
func (tx rangeQueryTx) Scan(req spi.ScanRequest) (spi.ScanPage, error) {
	tx.owner.scans++
	return spi.ScanPage{}, errors.New("range scanned history")
}

func TestItemRangeApprovalBoundarySurvivesResultAndRestart(t *testing.T) {
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	for i := range 8 {
		id := fmt.Sprint("call-", i)
		if _, err := memory.Append(t.Context(), Entry{ID: id, ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: id}}, AppendOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	backend, repo := openV31ReviewFixture(t, memory)
	input := ItemMembershipRangeOptions{ThreadID: "thread", AnchorID: "tool:turn:call-6", Kinds: []string{"tool"}, Exclude: ItemRangeApproval}
	if _, err := repo.ItemMembershipRange(t.Context(), input); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(map[string]any{"id": "approval", "kind": "approval", "turn_id": "turn", "run_id": "run", "tool_call_id": "call-4", "approval": map[string]string{"label": "Inspect", "tool_name": "inspect", "tool_call_id": "call-4"}})
	entries := []Entry{
		{ID: "interaction-requested:approval", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryInteractionAsked, Payload: raw},
		{ID: "late", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolResult, Message: session.Message{Role: session.Tool, ToolName: "inspect", ToolCallID: "call-4"}},
	}
	for _, entry := range entries {
		for _, r := range []Repo{repo, memory} {
			if _, err := r.Append(t.Context(), entry, AppendOptions{}); err != nil {
				t.Fatal(err)
			}
		}
		for _, r := range []ItemMembershipRangeReader{repo, memory, reopenCurrentStartupRepo(t, backend, false)} {
			got, err := r.ItemMembershipRange(t.Context(), input)
			if err != nil || got.Counts.Tool != 3 || got.First.ID != "tool:turn:call-5" {
				t.Fatalf("approval was folded: %+v %v", got, err)
			}
		}
	}
}

func TestItemRangeRejectsForgedApprovalBinding(t *testing.T) {
	for _, damage := range []string{"missing", "wrong-tool", "wrong-hash", "wrong-depth"} {
		t.Run(damage, func(t *testing.T) {
			backend, repo := projectionSourcesFixture(t, 8)
			input := ItemMembershipRangeOptions{ThreadID: "thread", AnchorID: "tool:turn:work", Kinds: []string{"tool"}}
			if _, err := repo.ItemMembershipRange(t.Context(), input); err != nil {
				t.Fatal(err)
			}
			raw, _ := json.Marshal(map[string]string{"id": "approval", "kind": "approval", "turn_id": "turn", "run_id": "run", "tool_call_id": "different"})
			entry, err := repo.Append(t.Context(), Entry{ID: "interaction-requested:approval", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryInteractionAsked, Payload: raw}, AppendOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				var slot int
				if err := readItemJSON(tx, itemIdentityKey("thread", input.AnchorID), &slot); err != nil {
					return err
				}
				member, err := readItemSlot(tx, "thread", slot)
				if err != nil {
					return err
				}
				member.RangeFacts |= ItemRangeApproval
				member.InteractionSource = contextSourceRef{ID: entry.ID, Hash: entry.RawHash, Depth: entry.PathDepth}
				switch damage {
				case "missing":
					member.InteractionSource = contextSourceRef{}
				case "wrong-hash":
					member.InteractionSource.Hash = "wrong"
				case "wrong-depth":
					member.InteractionSource.Depth++
				}
				return validateItemActivitySource(t.Context(), tx, ThreadMeta{ID: "thread", LeafID: entry.ID}, member.ItemMember)
			}); !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("accepted forged %s: %v", damage, err)
			}
		})
	}
}

func TestItemRangeMissingOrdinaryActivityMetadataFailsClosed(t *testing.T) {
	backend, repo := projectionSourcesFixture(t, 8)
	input := ItemMembershipRangeOptions{ThreadID: "thread", AnchorID: "tool:turn:work", Kinds: []string{"tool"}}
	selected, err := repo.ItemMembershipRange(t.Context(), input)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		if err := tx.Delete(itemMembershipNamespace, activitySourcesKey("thread", selected.First.SourceID)); err != nil {
			return err
		}
		_, err := itemRangeActivityFacts(tx, "thread", selected.First)
		return err
	}); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("missing ordinary metadata became an unknown status: %v", err)
	}
}

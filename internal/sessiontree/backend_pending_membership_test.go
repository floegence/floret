package sessiontree

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/tools"
)

func pendingReasoningFixture(t *testing.T) (*migrationTestBackend, *BackendRepo) {
	t.Helper()
	memory := newMemoryRepo(time.Now)
	if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "thread"}); err != nil {
		t.Fatal(err)
	}
	for i := range 5 {
		id := fmt.Sprint("call-", i)
		entry := Entry{ID: id, ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryToolCall, Message: session.Message{Role: session.Assistant, ToolName: "inspect", ToolCallID: id, ToolArgs: "{}", Reasoning: "retained thought", ReasoningSourceID: fmt.Sprint("thought-", i), Activity: &tools.ActivityPresentation{Label: "Inspect", Renderer: tools.ActivityRendererStructured, Payload: tools.StructuredActivityPayload{Status: "running", Summary: "Inspect fixture"}}}}
		if _, err := memory.Append(t.Context(), entry, AppendOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	backend, repo := openV31ReviewFixture(t, memory)
	if err := repo.PrepareThread(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	// Released historical stores can have canonical Activity bodies without the
	// independently prepared field index. Remove only that derived preparation.
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		meta, err := readPublishedThreadMeta(tx, backendDomainV32Format, "thread")
		if err != nil {
			return err
		}
		if err := visitCanonicalPathMetadata(t.Context(), tx, "thread", meta.LeafID, func(entry Entry) error {
			if ref, ok := entry.bodyFields["activity_json"]; ok {
				return deleteActivityFieldIndex(t.Context(), tx, ref.SHA256)
			}
			return nil
		}); err != nil {
			return err
		}
		return deleteItemMembership(t.Context(), tx, "thread")
	}); err != nil {
		t.Fatal(err)
	}
	return backend, repo
}

func TestHistoricalPendingActivityBelongsOnlyToTools(t *testing.T) {
	backend, repo := pendingReasoningFixture(t)
	if err := repo.prepareWindowProjection(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		manifest, found, err := readItemManifest(tx, "thread")
		if err != nil {
			return err
		}
		if !found {
			t.Fatal("membership not published")
		}
		pending := 0
		for slot := range manifest.Slots {
			member, err := readItemSlot(tx, "thread", slot)
			if err != nil {
				return err
			}
			if member.ActivityPending {
				pending++
				if member.Kind != "tool" {
					t.Errorf("%s member incorrectly awaits tool activity", member.Kind)
				}
			}
		}
		if pending != 5 {
			t.Errorf("pending = %d, want 5 tools", pending)
		}
		summary, err := readItemRangeSummary(tx, "thread", rangeLevel(manifest.Slots), 0)
		if err != nil {
			return err
		}
		if summary.Pending != 5 || summary.Counts.Tool != 5 || summary.Counts.Thinking != 5 {
			t.Errorf("invalid range: %+v", summary)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestPendingActivityReleasedIndexRebuildAndRestart(t *testing.T) {
	for _, mode := range []string{"success", "failure", "cancel", "panic"} {
		t.Run(mode, func(t *testing.T) {
			backend, repo := pendingReasoningFixture(t)
			if err := repo.prepareWindowProjection(t.Context(), "thread"); err != nil {
				t.Fatal(err)
			}
			// Recreate the exact released defect in derived records, keeping canonical
			// source bytes intact. Old preparation markers cannot certify these slots.
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				m, _, err := readItemManifest(tx, "thread")
				if err != nil {
					return err
				}
				for i := range m.Slots {
					member, err := readItemSlot(tx, "thread", i)
					if err != nil {
						return err
					}
					member.ActivityPending = true
					if err := putItemJSON(tx, itemSlotKey("thread", i), member); err != nil {
						return err
					}
				}
				m.ActivityPendingVersion = 0
				if err := putItemJSON(tx, itemManifestKey("thread"), m); err != nil {
					return err
				}
				summary, err := readItemRangeSummary(tx, "thread", rangeLevel(m.Slots), 0)
				if err != nil {
					return err
				}
				summary.Pending = m.Slots
				return putItemRangeSummary(tx, "thread", rangeLevel(m.Slots), 0, summary)
			}); err != nil {
				t.Fatal(err)
			}
			canonical := cloneMigrationRecords(backend.records)
			sentinel := errors.New("interrupted index repair")
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			wrapped := &windowPreparationBackend{Backend: backend, after: func(int) error {
				switch mode {
				case "failure":
					return sentinel
				case "cancel":
					cancel()
					return ctx.Err()
				case "panic":
					panic(sentinel)
				}
				return nil
			}}
			repo.backend = wrapped
			failed := false
			func() {
				defer func() {
					if value := recover(); value != nil {
						if mode != "panic" || value != sentinel {
							panic(value)
						}
						failed = true
					}
				}()
				err := repo.prepareWindowProjection(ctx, "thread")
				if err != nil && !errors.Is(err, sentinel) && !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
				failed = err != nil
			}()
			if failed != (mode != "success") {
				t.Fatalf("failure = %v for %s", failed, mode)
			}
			if wrapped.maxWrites > 128 || wrapped.maxDeletes > 128 {
				t.Fatal("repair exceeded bounded transactions")
			}
			for ns, records := range canonical {
				if ns != itemMembershipNamespace && !reflect.DeepEqual(records, backend.records[ns]) {
					t.Fatalf("source namespace changed: %s", ns)
				}
			}
			repo = reopenCurrentStartupRepo(t, backend, false)
			if err := repo.prepareWindowProjection(t.Context(), "thread"); err != nil {
				t.Fatal(err)
			}
			if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
				m, found, err := readItemManifest(tx, "thread")
				if err != nil {
					return err
				}
				if !found {
					t.Fatal("repair not published")
				}
				rangeSummary, err := readItemRangeSummary(tx, "thread", rangeLevel(m.Slots), 0)
				if err == nil && (rangeSummary.Pending != 5 || rangeSummary.Counts.Thinking != 5) {
					t.Fatal("repair lost membership")
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
			before := cloneMigrationRecords(backend.records)
			repo = reopenCurrentStartupRepo(t, backend, false)
			if err := repo.prepareWindowProjection(t.Context(), "thread"); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, backend.records) {
				t.Fatal("ready restart rewrote metadata")
			}
		})
	}
}

func TestItemRangeRejectsPendingNonToolBeforePublication(t *testing.T) {
	for _, hidden := range []bool{false, true} {
		if _, err := itemRangeLeaf(ItemMember{Kind: "thinking", Hidden: hidden, ActivityPending: true}); !errors.Is(err, ErrAuthorityCorrupt) {
			t.Fatalf("invalid pending member accepted: %v", err)
		}
	}
}

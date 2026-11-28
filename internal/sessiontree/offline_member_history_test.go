//go:build floret_layout_cutover

package sessiontree

import (
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/threadsqlite/member"
	"github.com/floegence/floret/v7/storage/spi"
	_ "modernc.org/sqlite"
)

func TestMemberJournalMigrationRoundTripsBranchesAndEffects(t *testing.T) {
	ctx := t.Context()
	source, repo, attempt := canonicalSettlementFixture(t, true)
	finished, err := repo.FinishEffectDispatch(ctx, canonicalEffectRequest(attempt))
	if err != nil {
		t.Fatal(err)
	}
	first, err := repo.Append(ctx, Entry{ThreadID: "thread", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "first branch"}}, AppendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	long := strings.Repeat("long text ", 20000) + "end"
	selected, err := repo.Append(ctx, Entry{ThreadID: "thread", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: long}, Metadata: map[string]string{"opaque": strings.Repeat("abcdef", 5000)}}, AppendOptions{ParentID: finished.Result.ID})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "target.sqlite")
	if _, err := repo.CompareAndSwapAgentTodoState(ctx, AgentTodoState{ThreadID: "thread"}, 0); err != nil {
		t.Fatal(err)
	}
	state := ProviderStateRecord{ThreadID: "thread", LeafEntryID: selected.ID, CompatibilityKey: "provider", CreatedByRunID: "run", CreatedByTurnID: "turn", UpdatedAt: time.Now().UTC(), State: provider.State{Kind: "test", ID: "previous", Attributes: map[string]string{"receipt": strings.Repeat("abc", 8000)}}}
	if err := repo.PutProviderState(ctx, state); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path+"?_pragma=foreign_keys(ON)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var total int
	if err := source.View(ctx, func(old spi.ReadTx) error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := member.Initialize(ctx, tx, [16]byte{1}, [16]byte{2}, "thread"); err != nil {
			return err
		}
		journal, err := member.OpenJournal(ctx, tx, "thread", false)
		if err != nil {
			return err
		}
		total, err = StageMemberJournal(ctx, old, journal)
		if err != nil {
			return err
		}
		return tx.Commit()
	}); err != nil {
		t.Fatal(err)
	}
	if total < 7 {
		t.Fatalf("unexpected journal length: %d", total)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	journal, err := member.OpenJournal(ctx, tx, "thread", false)
	if err != nil {
		t.Fatal(err)
	}
	if err := source.View(ctx, func(old spi.ReadTx) error {
		proof, err := VerifyMemberJournal(ctx, old, journal)
		if err == nil && (proof.Entries != total || proof.Digest == ([32]byte{})) {
			t.Fatal("unverified journal")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	var leaf string
	var open, activeTurns, storedBlocks, metadataSize int
	if err := tx.QueryRow(`SELECT leaf.event_id,(SELECT count(*) FROM history_events WHERE work_head=1 AND attempt_id IS NOT NULL),(SELECT count(*) FROM history_events WHERE work_head=1 AND kind='turn_marker' AND turn_id='turn' AND run_id='run'),(SELECT count(*) FROM content_blocks),length(leaf.payload) FROM floret_thread_header h JOIN history_events leaf ON leaf.seq=h.leaf_seq`).Scan(&leaf, &open, &activeTurns, &storedBlocks, &metadataSize); err != nil || leaf != selected.ID || open != 0 || activeTurns != 1 || storedBlocks < 4 || metadataSize >= 16384 {
		t.Fatalf("leaf=%s pending attempts=%d active turns=%d blocks=%d inline=%d: %v", leaf, open, activeTurns, storedBlocks, metadataSize, err)
	}
	for _, want := range []Entry{finished.Result, first, selected} {
		event, err := journal.Entry(want.ID)
		if err != nil {
			t.Fatal(err)
		}
		got, err := materializeMemberEntry(journal, event)
		if err != nil || got.RawHash != want.RawHash || got.ParentID != want.ParentID || got.Raw != want.Raw {
			t.Fatalf("entry %s changed: %v", want.ID, err)
		}
	}
	var states string
	if err := tx.QueryRow(`SELECT group_concat(state,',' ORDER BY ordinal) FROM history_events WHERE attempt_id=?`, attempt.EffectAttemptID).Scan(&states); err != nil || states != "prepared,dispatching,completed" {
		t.Fatalf("attempt state sequence=%s: %v", states, err)
	}
	if _, err := tx.Exec(`DELETE FROM history_events WHERE kind='todo_updated'`); err != nil {
		t.Fatal(err)
	}
	if err := source.View(ctx, func(old spi.ReadTx) error { _, err := VerifyMemberJournal(ctx, old, journal); return err }); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatalf("missing todo accepted: %v", err)
	}
	if _, err := tx.Exec(`UPDATE content_blocks SET bytes=zeroblob(length(bytes)) WHERE block_no=0`); err != nil {
		t.Fatal(err)
	}
	event, err := journal.Entry(selected.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := materializeMemberEntry(journal, event); !errors.Is(err, member.ErrContentIntegrity) {
		t.Fatalf("damaged target accepted: %v", err)
	}
}

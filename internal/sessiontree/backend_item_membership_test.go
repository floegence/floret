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
)

type membershipReadBackend struct {
	spi.Backend
	sources, scans int
}
type membershipReadTx struct {
	spi.ReadTx
	owner *membershipReadBackend
}

func (b *membershipReadBackend) View(ctx context.Context, f func(spi.ReadTx) error) error {
	return b.Backend.View(ctx, func(tx spi.ReadTx) error { return f(membershipReadTx{tx, b}) })
}
func (tx membershipReadTx) Get(ns string, key []byte) ([]byte, error) {
	if ns == canonicalBodyNamespace {
		return nil, errors.New("membership window read a body")
	}
	if ns == backendDomainV32Namespace {
		tx.owner.sources++
	}
	return tx.ReadTx.Get(ns, key)
}
func (tx membershipReadTx) Scan(req spi.ScanRequest) (spi.ScanPage, error) {
	tx.owner.scans++
	return spi.ScanPage{}, errors.New("membership window scanned history")
}

func TestItemMembershipColdWindowReadsOnlySelectedSources(t *testing.T) {
	var reads int
	for _, count := range []int{128, 1280} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			backend, repo := projectionSourcesFixture(t, count)
			options := ItemMembershipWindowOptions{ThreadID: "thread", Limit: 8}
			want, err := repo.ItemMembershipWindow(t.Context(), options)
			if err != nil {
				t.Fatal(err)
			}
			if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
				var err error
				repo, _, err = NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, false)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			counted := &membershipReadBackend{Backend: backend}
			repo.backend = counted
			got, err := repo.ItemMembershipWindow(t.Context(), options)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("got=%+v want=%+v error=%v", got, want, err)
			}
			if counted.scans != 0 {
				t.Fatalf("scans=%d", counted.scans)
			}
			if reads == 0 {
				reads = counted.sources
			} else if counted.sources != reads {
				t.Fatalf("source reads grew %d -> %d", reads, counted.sources)
			}
		})
	}
}

func TestItemMembershipAppendRollbackBranchAndDelete(t *testing.T) {
	backend, repo := projectionSourcesFixture(t, 16)
	options := ItemMembershipWindowOptions{ThreadID: "thread", Limit: 256}
	before, err := repo.ItemMembershipWindow(t.Context(), options)
	if err != nil {
		t.Fatal(err)
	}
	records := cloneMigrationRecords(backend.records)
	failure := errors.New("commit failed")
	repo.backend = &domainCommitBoundaryBackend{Backend: backend, afterCallback: func() error { return failure }}
	entry := Entry{ID: "new", ThreadID: "thread", TurnID: "turn", RunID: "run", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "next"}}
	if _, err := repo.Append(t.Context(), entry, AppendOptions{ID: entry.ID}); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(records, backend.records) {
		t.Fatal("failed append changed source or membership")
	}
	repo.backend = backend
	if _, err := repo.Append(t.Context(), entry, AppendOptions{ID: entry.ID}); err != nil {
		t.Fatal(err)
	}
	got, err := repo.ItemMembershipWindow(t.Context(), options)
	if err != nil || len(got.Members) != len(before.Members)+1 {
		t.Fatalf("append count=%d err=%v", len(got.Members), err)
	}
	if err := repo.MoveLeaf(t.Context(), "thread", "call"); err != nil {
		t.Fatal(err)
	}
	got, err = repo.ItemMembershipWindow(t.Context(), options)
	if err != nil || len(got.Members) != 1 || got.Members[0].ID != "tool:turn:work" {
		t.Fatalf("branch=%+v err=%v", got, err)
	}
	if _, err := repo.DeleteRootTree(t.Context(), "thread"); err != nil {
		t.Fatal(err)
	}
	if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
		page, err := tx.Scan(spi.ScanRequest{Namespace: itemMembershipNamespace, Limit: 1})
		if len(page.Records) != 0 {
			t.Fatal("deleted thread retained membership")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestItemMembershipWindowRejectsCorruptMetadata(t *testing.T) {
	for _, kind := range []string{"rank", "slot", "source", "hash"} {
		t.Run(kind, func(t *testing.T) {
			backend, repo := projectionSourcesFixture(t, 8)
			options := ItemMembershipWindowOptions{ThreadID: "thread", Limit: 1}
			page, err := repo.ItemMembershipWindow(t.Context(), options)
			if err != nil {
				t.Fatal(err)
			}
			slot := page.Members[0].Slot
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				if kind == "rank" {
					return tx.Delete(itemMembershipNamespace, itemRankKey("thread", slot+1))
				}
				if kind == "slot" {
					return tx.Delete(itemMembershipNamespace, itemSlotKey("thread", slot))
				}
				member, err := readItemSlot(tx, "thread", slot)
				if err != nil {
					return err
				}
				if kind == "hash" {
					member.RawHash = "wrong"
				} else {
					member.SourceID = "call"
				}
				return putItemJSON(tx, itemSlotKey("thread", slot), member)
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := repo.ItemMembershipWindow(t.Context(), options); !errors.Is(err, ErrAuthorityCorrupt) {
				t.Fatalf("accepted %s: %v", kind, err)
			}
		})
	}
}

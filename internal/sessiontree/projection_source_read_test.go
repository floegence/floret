package sessiontree

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/floegence/floret/v7/storage/spi"
)

func projectionSetIDs(set ProjectionSourceSet) []string {
	ids := make([]string, len(set.Sources))
	for i, source := range set.Sources {
		ids[i] = source.Entry.ID
	}
	return ids
}

func TestProjectionSourcesClosureColdReadIsIndependentOfUnrelatedHistory(t *testing.T) {
	var reads int
	for _, count := range []int{128, 1280} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			backend, repo := projectionSourcesFixture(t, count)
			if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
				var err error
				repo, _, err = NewCurrentBackendRepoInTransaction(t.Context(), backend, tx, time.Now, false)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			counted := &projectionReadBackend{Backend: backend}
			repo.backend = counted
			got, err := repo.ProjectionSources(t.Context(), "thread", "", []string{"result"})
			if err != nil || !reflect.DeepEqual(projectionSetIDs(got), []string{"start", "call", "result"}) {
				t.Fatalf("sources=%v err=%v", projectionSetIDs(got), err)
			}
			if reads == 0 {
				reads = counted.sources
			} else if counted.sources != reads {
				t.Fatalf("source reads grew with unrelated history: %d -> %d", reads, counted.sources)
			}
		})
	}
}

func TestProjectionSourcesClosureRespectsFrozenLeaf(t *testing.T) {
	_, repo := projectionSourcesFixture(t, 8)
	got, err := repo.ProjectionSources(t.Context(), "thread", "call", []string{"call"})
	if err != nil || !reflect.DeepEqual(projectionSetIDs(got), []string{"start", "call"}) {
		t.Fatalf("sources=%v err=%v", projectionSetIDs(got), err)
	}
	if _, err := repo.ProjectionSources(t.Context(), "thread", "call", []string{"result"}); !errors.Is(err, ErrEntryNotFound) {
		t.Fatal(err)
	}
	if err := repo.MoveLeaf(t.Context(), "thread", "call"); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.ProjectionSources(t.Context(), "thread", "result", []string{"call"}); !errors.Is(err, ErrEntryNotFound) {
		t.Fatal(err)
	}
}

func TestProjectionSourcesClosureRejectsEntireMissingSeedGroup(t *testing.T) {
	backend, repo := projectionSourcesFixture(t, 2)
	group := projectionSourceGroup{"tool", "turn", "run", "work"}
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
		for i := range 2 {
			if err := tx.Delete(projectionSourcesNamespace, projectionMemberKey("thread", group, i)); err != nil {
				return err
			}
		}
		return tx.Delete(projectionSourcesNamespace, projectionGroupKey("thread", group))
	}); err != nil {
		t.Fatal(err)
	}
	before := cloneMigrationRecords(backend.records)
	if _, err := repo.ProjectionSources(t.Context(), "thread", "", []string{"call"}); !errors.Is(err, ErrAuthorityCorrupt) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, backend.records) {
		t.Fatal("corrupt lookup modified canonical data")
	}
}

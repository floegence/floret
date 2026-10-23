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

type projectionScanBackend struct {
	spi.Backend
	rows int
}

func (backend *projectionScanBackend) View(ctx context.Context, read func(spi.ReadTx) error) error {
	return backend.Backend.View(ctx, func(tx spi.ReadTx) error { return read(projectionScanTx{ReadTx: tx, backend: backend}) })
}

type projectionScanTx struct {
	spi.ReadTx
	backend *projectionScanBackend
}

func (tx projectionScanTx) Scan(request spi.ScanRequest) (spi.ScanPage, error) {
	page, err := tx.ReadTx.Scan(request)
	if request.Namespace == backendDomainV32Namespace {
		tx.backend.rows += len(page.Records)
	}
	return page, err
}

func TestProjectionScanReusesMetadataAndRechecksEveryBodyPage(t *testing.T) {
	for _, count := range []int{32, 320} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			memory := newMemoryRepo(time.Now)
			if _, err := memory.CreateThread(t.Context(), ThreadMeta{ID: "scan"}); err != nil {
				t.Fatal(err)
			}
			for i := range count {
				if _, err := memory.Append(t.Context(), Entry{ThreadID: "scan", Type: EntryUserMessage, Message: session.Message{Role: session.User, Content: "source 🌸"}}, AppendOptions{ID: fmt.Sprint(i), Now: time.Date(2026, 10, 23, 0, 0, i, 0, time.UTC)}); err != nil {
					t.Fatal(err)
				}
			}
			base, repo := openV31ReviewFixture(t, memory)
			if err := repo.PrepareThread(t.Context(), "scan"); err != nil {
				t.Fatal(err)
			}
			counted := &projectionScanBackend{Backend: base}
			repo.backend = counted
			for restart := range 2 {
				if restart > 0 {
					if err := base.View(t.Context(), func(tx spi.ReadTx) error {
						var err error
						repo, _, err = NewCurrentBackendRepoInTransaction(t.Context(), counted, tx, time.Now, false)
						return err
					}); err != nil {
						t.Fatal(err)
					}
				}
				counted.rows = 0
				var got, want []PathProjectionPage
				prepare := func(metadata []PathMetadata) error {
					if len(metadata) != count {
						t.Fatalf("metadata count=%d want=%d", len(metadata), count)
					}
					// Mutating callback-owned metadata cannot alter source authority.
					metadata[0].Entry.ID = "changed"
					metadata[0].Bodies[0].SHA256 = "changed"
					return nil
				}
				fields := func(PathMetadata) []string { return []string{"content"} }
				if err := repo.ScanPathProjection(t.Context(), "scan", "", prepare, fields, func(page PathProjectionPage) error { got = append(got, page); return nil }); err != nil {
					t.Fatal(err)
				}
				if err := memory.ScanPathProjection(t.Context(), "scan", "", prepare, fields, func(page PathProjectionPage) error { want = append(want, page); return nil }); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatal("frozen scan differs from canonical memory")
				}
				// One complete metadata pass and one final leaf fence; selected
				// body pages never scan the journal a second time.
				if counted.rows != count+1 {
					t.Fatalf("source rows=%d want=%d", counted.rows, count+1)
				}
			}
		})
	}
}

func TestProjectionScanRejectsInvalidationWithoutReturningSuccess(t *testing.T) {
	for _, mutation := range []string{"cancel", "branch", "delete", "body", "callback"} {
		t.Run(mutation, func(t *testing.T) {
			_, base, repo := linearPageFixture(t, false)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			injected := errors.New("injected callback failure")
			prepare := func([]PathMetadata) error {
				if mutation == "body" {
					return base.Update(t.Context(), func(tx spi.WriteTx) error {
						return tx.Delete(canonicalBodyNamespace, canonicalBodyBlockKey(stableHash("source"), 0))
					})
				}
				return nil
			}
			err := repo.ScanPathProjection(ctx, "thread", "", prepare, func(PathMetadata) []string { return []string{"content"} }, func(PathProjectionPage) error {
				switch mutation {
				case "cancel":
					cancel()
				case "branch":
					return repo.MoveLeaf(t.Context(), "thread", "entry-04")
				case "delete":
					_, err := repo.DeleteRootTree(t.Context(), "thread")
					return err
				case "callback":
					return injected
				}
				return nil
			})
			want := map[string]error{"cancel": context.Canceled, "branch": ErrEntryNotFound, "delete": ErrThreadDeleted, "body": ErrAuthorityCorrupt, "callback": injected}[mutation]
			if !errors.Is(err, want) {
				t.Fatalf("mutation=%s got=%v want=%v", mutation, err, want)
			}
			ctx, stop := context.WithTimeout(t.Context(), time.Second)
			defer stop()
			_, release, err := repo.migrationGate.acquire(ctx, false)
			if err != nil {
				t.Fatalf("failed scan retained foreground priority: %v", err)
			}
			release()
		})
	}
}

func TestProjectionScanPanicReleasesForegroundPriority(t *testing.T) {
	_, _, repo := linearPageFixture(t, false)
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected injected callback panic")
			}
		}()
		_ = repo.ScanPathProjection(t.Context(), "thread", "", func([]PathMetadata) error { panic("injected") }, func(PathMetadata) []string { return nil }, func(PathProjectionPage) error { return nil })
	}()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	_, release, err := repo.migrationGate.acquire(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	release()
}

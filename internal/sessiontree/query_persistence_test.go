package sessiontree

import (
	"bytes"
	"context"
	"errors"
	"github.com/floegence/floret/v7/internal/storagebridge"
	publicstorage "github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/threadquery"
	"testing"
	"time"
)

func TestPersistentQueryReopenReusesIndexAndCanonicalBytes(t *testing.T) {
	for _, native := range []bool{false, true} {
		t.Run(map[bool]string{false: "custom", true: "sqlite"}[native], func(t *testing.T) {
			var backend spi.Backend = newMigrationTestBackend()
			if native {
				var err error
				backend, err = storagebridge.Open(t.Context(), storagebridge.Source(publicstorage.SQLite(t.TempDir()+"/index.sqlite")))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = backend.Close() })
			}
			repo, err := NewBackendRepo(t.Context(), backend, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			queryFixture(t, repo)
			before := map[string][]byte{}
			if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
				for _, ns := range []string{backendDomainNamespace, "floret.domain.sessiontree.v16", queryManifestNamespace} {
					page, err := tx.Scan(spi.ScanRequest{Namespace: ns, Limit: 10000})
					if err != nil {
						return err
					}
					for _, record := range page.Records {
						before[ns+string(record.Key)] = record.Value
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			var reopened *BackendRepo
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				var err error
				reopened, err = NewBackendRepoInTransaction(t.Context(), backend, tx, time.Now, func(phase StartupPhase) {
					if phase == StartupPhaseIndexing {
						t.Error("ordinary reopen rebuilt index")
					}
				})
				if err != nil {
					return err
				}
				return reopened.VerifyCurrentStateInTransaction(t.Context(), tx)
			}); err != nil {
				t.Fatal(err)
			}
			if reopened.domainMemory.querySnapshots != nil {
				t.Fatal("startup retained query state")
			}
			if err := backend.View(t.Context(), func(tx spi.ReadTx) error {
				for _, ns := range []string{backendDomainNamespace, "floret.domain.sessiontree.v16", queryManifestNamespace} {
					page, err := tx.Scan(spi.ScanRequest{Namespace: ns, Limit: 10000})
					if err != nil {
						return err
					}
					for _, record := range page.Records {
						if !bytes.Equal(before[ns+string(record.Key)], record.Value) {
							t.Error("reopen rewrote canonical or manifest bytes")
						}
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			result, err := reopened.Query(t.Context(), queryScope("a", "b"), threadquery.QueryInput{Object: threadquery.Messages, Filter: threadquery.Filter{Text: threadquery.TextMatch{Contains: "中文"}}})
			if err != nil || result.Total != 4 {
				t.Fatalf("query=%#v err=%v", result, err)
			}
		})
	}
}

func TestPersistentQueryMutationRollsBack(t *testing.T) {
	for _, failure := range []string{"error", "cancel", "panic"} {
		t.Run(failure, func(t *testing.T) {
			backend := newMigrationTestBackend()
			repo, err := NewBackendRepo(t.Context(), backend, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			queryFixture(t, repo)
			before := cloneMigrationRecords(backend.records)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			func() {
				defer func() {
					if failure == "panic" && recover() == nil {
						t.Error("missing panic")
					}
				}()
				err = repo.UpdateDomain(ctx, func(memory *MemoryRepo, tx spi.WriteTx) error {
					if _, err := memory.CreateThread(ctx, ThreadMeta{ID: "rollback"}); err != nil {
						return err
					}
					if err := memory.mutation.persistQuery(ctx, tx, nil); err != nil {
						return err
					}
					switch failure {
					case "panic":
						panic("injected")
					case "cancel":
						cancel()
						return ctx.Err()
					}
					return errors.New("injected")
				})
			}()
			if !equalMigrationRecords(before, backend.records) {
				t.Fatal("partial canonical or index commit")
			}
			if _, ok := repo.domainMemory.threads["rollback"]; ok {
				t.Fatal("failed memory mutation published")
			}
		})
	}
}

func TestPersistentQueryBuildCancellationRollsBackAndRetries(t *testing.T) {
	backend := newMigrationTestBackend()
	memory := newMemoryRepo(time.Now)
	queryFixture(t, memory)
	if err := backend.Update(t.Context(), func(tx spi.WriteTx) error { return saveCompleteBackendDomainV20(tx, memory) }); err != nil {
		t.Fatal(err)
	}
	before := cloneMigrationRecords(backend.records)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	err := backend.Update(ctx, func(tx spi.WriteTx) error {
		repo, err := NewBackendRepoInTransaction(ctx, backend, tx, time.Now, func(phase StartupPhase) {
			if phase == StartupPhaseIndexing {
				cancel()
			}
		})
		if err != nil {
			return err
		}
		return repo.VerifyCurrentStateInTransaction(ctx, tx)
	})
	if !errors.Is(err, context.Canceled) || !equalMigrationRecords(before, backend.records) {
		t.Fatalf("cancelled build mutated storage: %v", err)
	}
	if _, err := NewBackendRepo(t.Context(), backend, time.Now); err != nil {
		t.Fatal(err)
	}
}

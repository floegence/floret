package sessiontree

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/floegence/floret/v7/internal/storagebridge"
	publicstorage "github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/threadquery"
	"strings"
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

func TestQueryMetadataMigrationCancellationRollsBackAndRetries(t *testing.T) {
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
			if phase == StartupPhaseVerifying {
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

func TestQueryMetadataUpgradeRetiresLegacyTermsAtomically(t *testing.T) {
	for _, failure := range []string{"none", "error", "cancel", "panic"} {
		t.Run(failure, func(t *testing.T) {
			backend := newMigrationTestBackend()
			repo, err := NewBackendRepo(t.Context(), backend, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			queryFixture(t, repo)
			if err := backend.Update(t.Context(), func(tx spi.WriteTx) error {
				for i := range 260 {
					if err := tx.Put("floret.query.documents.v1", []byte(fmt.Sprintf("orphan-%03d", i)), []byte("released-opaque-term-data")); err != nil {
						return err
					}
				}
				for id := range repo.domainMemory.threads {
					if err := queryMetadataWriter(tx).deleteThread(id); err != nil {
						return err
					}
					raw, _ := json.Marshal(queryManifest{Version: 2, Leaf: repo.domainMemory.threads[id].LeafID})
					if err := tx.Put(queryManifestNamespace, []byte(id), raw); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			before := cloneMigrationRecords(backend.records)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			func() {
				defer func() {
					if failure == "panic" && recover() == nil {
						t.Fatal("missing panic")
					}
				}()
				err = backend.Update(ctx, func(tx spi.WriteTx) error {
					// Metadata preparation cannot hydrate historical message or tool bodies.
					repo.domainMemory.bodyTx = failingBodyReadTx{ReadTx: tx, failure: errors.New("metadata read canonical body")}
					repo.domainMemory.bodyContext = ctx
					defer func() { repo.domainMemory.bodyTx = nil; repo.domainMemory.bodyContext = nil }()
					if err := repo.prepareQueryStorage(ctx, tx, false); err != nil {
						return err
					}
					switch failure {
					case "error":
						return errors.New("injected commit failure")
					case "cancel":
						cancel()
						return ctx.Err()
					case "panic":
						panic("injected commit panic")
					}
					return nil
				})
				if (failure == "none") != (err == nil) {
					t.Fatalf("failure=%s error=%v", failure, err)
				}
			}()
			if failure != "none" && !equalMigrationRecords(before, backend.records) {
				t.Fatal("failed removal committed partial changes")
			}
			reopened, err := NewBackendRepo(t.Context(), backend, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			if len(backend.records["floret.query.documents.v1"]) != 0 {
				t.Fatal("legacy text terms remain")
			}
			for key, raw := range backend.records[queryDocumentNamespace] {
				var document map[string]json.RawMessage
				if err := json.Unmarshal(raw, &document); err != nil {
					t.Fatal(err)
				}
				if len(document) != 4 || document["Terms"] != nil {
					t.Fatalf("metadata contains text index fields: %s", key)
				}
				var metadata queryDocumentMetadata
				var stored queryStoredDocument
				if err := json.Unmarshal(raw, &stored); err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(stored.Metadata, &metadata); err != nil {
					t.Fatal(err)
				}
				if metadata.Row.Snippet != "" {
					t.Fatal("metadata retained a transcript preview")
				}
			}
			for ns, records := range before {
				if strings.HasPrefix(ns, "floret.query.") {
					continue
				}
				if !equalMigrationRecords(map[string]map[string][]byte{ns: records}, map[string]map[string][]byte{ns: backend.records[ns]}) {
					t.Fatalf("canonical namespace changed: %s", ns)
				}
			}
			page, err := reopened.Query(t.Context(), queryScope("a", "b"), threadquery.QueryInput{Object: threadquery.Messages, Filter: threadquery.Filter{Text: threadquery.TextMatch{Contains: "中文"}}})
			if err != nil || page.Total != 4 {
				t.Fatalf("query after removal=%+v %v", page, err)
			}
			after := cloneMigrationRecords(backend.records)
			if _, err := NewBackendRepo(t.Context(), backend, time.Now); err != nil {
				t.Fatal(err)
			}
			if !equalMigrationRecords(after, backend.records) {
				t.Fatal("reopen rewrote current metadata")
			}
		})
	}
}

func TestMetadataBrowseKeepsOnlyFrozenLocators(t *testing.T) {
	backend := newMigrationTestBackend()
	repo, err := NewBackendRepo(t.Context(), backend, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	queryFixture(t, repo)
	page, err := repo.Query(t.Context(), queryScope("a"), threadquery.QueryInput{Object: threadquery.Messages, Limit: 1})
	if err != nil || len(page.Records) != 1 || page.NextCursor == "" {
		t.Fatalf("page=%+v %v", page, err)
	}
	for _, snapshot := range repo.domainMemory.querySnapshots.snapshots {
		for _, record := range snapshot.records {
			if !record.lazy || len(record.parts) != 0 || len(record.activities) != 0 {
				t.Fatal("browse retained full transcript bodies")
			}
		}
	}
	read, err := repo.Read(t.Context(), queryScope("a"), threadquery.ReadInput{Ref: page.Records[0].Ref})
	if err != nil || len(read.Chunks) == 0 || read.Chunks[0].Text == "" {
		t.Fatalf("read=%+v %v", read, err)
	}
	next, err := repo.Query(t.Context(), queryScope("a"), threadquery.QueryInput{Object: threadquery.Messages, Limit: 1, Cursor: page.NextCursor})
	if err != nil || len(next.Records) != 1 || next.Records[0].Ref == page.Records[0].Ref {
		t.Fatalf("continuation=%+v %v", next, err)
	}
}

//go:build floret_layout_cutover

package cutover

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/provider"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/internal/storagecodec"
	"github.com/floegence/floret/v7/internal/threadsqlite"
	"github.com/floegence/floret/v7/runtime"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestOfflineJournalPreservesInactivePathsAndSharedBodies(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	path := filepath.Join(root, "source.sqlite")
	host, err := runtime.Open(ctx, runtime.Options{Storage: storage.SQLite(path), DeferExecution: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	backend, err := storagebridge.Open(ctx, storagebridge.Source(storage.SQLite(path)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Close() })
	var repo *sessiontree.BackendRepo
	if err := backend.Update(ctx, func(tx spi.WriteTx) error {
		var current bool
		repo, current, err = sessiontree.NewCurrentBackendRepoInTransaction(ctx, backend, tx, time.Now, false)
		if err == nil && !current {
			return spi.ErrMigrationRequired
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	threads := []identity.ThreadID{"first", "second", "empty"}
	for _, thread := range threads {
		if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: string(thread)}); err != nil {
			t.Fatal(err)
		}
	}
	shared := strings.Repeat("abcde", 43_100)
	appendText := func(thread, id, text string) {
		t.Helper()
		_, err := repo.Append(ctx, sessiontree.Entry{ID: id, ThreadID: thread, Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: text}}, sessiontree.AppendOptions{})
		if err != nil {
			t.Fatal(err)
		}
	}
	appendText("first", "root", shared)
	appendText("first", "inactive", "must retain the old branch")
	if err := repo.MoveLeaf(ctx, "first", "root"); err != nil {
		t.Fatal(err)
	}
	appendText("first", "active", shared)
	appendText("second", "other", shared)
	if err := repo.PutProviderState(ctx, sessiontree.ProviderStateRecord{ThreadID: "first", LeafEntryID: "inactive", CompatibilityKey: "fixture", State: provider.State{Kind: "test", ID: "opaque"}, CreatedByRunID: "run", CreatedByTurnID: "turn", UpdatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CompareAndSwapAgentTodoState(ctx, sessiontree.AgentTodoState{ThreadID: "second"}, 0); err != nil {
		t.Fatal(err)
	}
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
	catalog, err := threadsqlite.CreateCatalog(ctx, filepath.Join(root, "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = catalog.Close() })
	files, err := threadsqlite.NewFiles(root, catalog.Store, 2, 0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = files.Close(context.Background()) })
	digest := sha256.Sum256([]byte(shared))
	bodyKey := storagecodec.Tuple(storagecodec.TupleString("body"), storagecodec.TupleString(hex.EncodeToString(digest[:])))
	const bodies = "floret.domain.sessiontree.bodies.v30"
	const domain = "floret.domain.sessiontree.v17"
	for _, thread := range threads {
		ref, err := threadsqlite.Allocate(thread)
		if err != nil {
			t.Fatal(err)
		}
		if err := files.Create(ctx, ref); err != nil {
			t.Fatal(err)
		}
		if err := WithSource(ctx, path, func(source *Source) error {
			before, err := source.Records(ctx)
			if err != nil {
				return err
			}
			if err := source.StageJournal(ctx, files, ref); err != nil {
				return err
			}
			if err := source.StageJournal(ctx, files, ref); !errors.Is(err, spi.ErrConflict) {
				t.Fatalf("overwriting staged %s: %v", thread, err)
			}
			after, err := source.Records(ctx)
			if err != nil || before != after {
				t.Fatalf("source changed: %v", err)
			}
			return files.View(ctx, ref, func(target *sql.Tx) error {
				var checked int
				rows, err := target.QueryContext(ctx, `SELECT key,value FROM floret_backend_records WHERE namespace=?`, domain)
				if err != nil {
					return err
				}
				defer rows.Close()
				for rows.Next() {
					var key, value, original []byte
					if err := rows.Scan(&key, &value); err != nil {
						return err
					}
					if err := source.transaction.QueryRowContext(ctx, `SELECT value FROM floret_backend_records WHERE namespace=? AND key=?`, domain, key).Scan(&original); err != nil {
						return err
					}
					if !bytes.Equal(value, original) {
						t.Fatalf("journal identity changed for %s", thread)
					}
					checked++
				}
				if err := rows.Err(); err != nil {
					return err
				}
				if checked != map[identity.ThreadID]int{"first": 5, "second": 3, "empty": 1}[thread] {
					t.Fatalf("%s: %d journal and metadata records", thread, checked)
				}
				var localCount int
				err = target.QueryRowContext(ctx, `SELECT json_extract(CAST(value AS TEXT),'$.payload.References') FROM floret_backend_records WHERE namespace=? AND key=?`, bodies, bodyKey).Scan(&localCount)
				if thread == "empty" && errors.Is(err, sql.ErrNoRows) {
					return nil
				}
				if err != nil || localCount != map[identity.ThreadID]int{"first": 2, "second": 1}[thread] {
					t.Fatalf("%s local body references=%d: %v", thread, localCount, err)
				}
				return nil
			})
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := catalog.Resolve(ctx, thread); !errors.Is(err, spi.ErrNotFound) {
			t.Fatalf("staging published %s: %v", thread, err)
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	blockKey := storagecodec.Tuple(storagecodec.TupleString("block"), storagecodec.TupleString(hex.EncodeToString(digest[:])), storagecodec.TupleOrdinal(4))
	encoded, err := json.Marshal(bytes.Repeat([]byte{'Z'}, 16<<10))
	if err != nil {
		t.Fatal(err)
	}
	wrong, err := storagecodec.EncodeEnvelope("sessiontree-v30-body-block", encoded)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE floret_backend_records SET value=? WHERE namespace=? AND key=?`, wrong, bodies, blockKey); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	ref, err := threadsqlite.Allocate("first")
	if err != nil {
		t.Fatal(err)
	}
	if err := files.Create(ctx, ref); err != nil {
		t.Fatal(err)
	}
	err = WithSource(ctx, path, func(source *Source) error { return source.StageJournal(ctx, files, ref) })
	if !errors.Is(err, sessiontree.ErrAuthorityCorrupt) {
		t.Fatalf("accepted changed block: %v", err)
	}
	if err := files.View(ctx, ref, func(tx *sql.Tx) error {
		var count int
		err := tx.QueryRowContext(ctx, `SELECT count(*) FROM floret_backend_records`).Scan(&count)
		if count != 0 {
			t.Fatalf("failed stage committed %d records", count)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

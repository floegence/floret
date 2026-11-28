//go:build floret_layout_cutover

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/internal/threadsqlite"
	"github.com/floegence/floret/v7/runtime"
	cutover "github.com/floegence/floret/v7/scripts/testdata/thread-layout-cutover"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/threadquery"
)

func rehearse(ctx context.Context, path string, thread identity.ThreadID) (err error) {
	root, err := filepath.EvalSymlinks(os.Getenv("FLOEGENCE_TEST_RUN_DIR"))
	if err != nil || !filepath.IsAbs(root) || filepath.Dir(filepath.Dir(filepath.Dir(path))) != root || filepath.Base(path) != "new.sqlite" {
		return spi.ErrInvalidArgument
	}
	directory, err := os.MkdirTemp(filepath.Dir(path), "unpublished-")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, os.RemoveAll(directory)) }()
	catalog, err := threadsqlite.CreateCatalog(ctx, filepath.Join(directory, "catalog.sqlite"))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, catalog.Close()) }()
	files, err := threadsqlite.NewFiles(directory, catalog.Store, 2, 0, time.Minute)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, files.Close(context.Background())) }()
	ref, err := threadsqlite.Allocate(thread)
	if err != nil {
		return err
	}
	err = cutover.WithSource(ctx, path, func(source *cutover.Source) error {
		inventory, err := source.ThreadInventory(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("source_heads=%d roots=%d empty=%d journal_entries=%d head_inventory_sha256=%x\n", inventory.Threads, inventory.Roots, inventory.Empty, inventory.Entries, inventory.Digest)
		if err := files.Create(ctx, ref); err != nil {
			return err
		}
		if err := source.StageJournal(ctx, files, ref); err != nil {
			return err
		}
		proof, err := source.VerifyJournal(ctx, files, ref)
		if err != nil {
			return err
		}
		fmt.Printf("journal_entries=%d matching_journal_sha256=%x catalog_published=false\n", proof.Entries, proof.Digest)
		pages, err := source.VerifyItemWindows(ctx, files, ref)
		if err != nil {
			return err
		}
		fmt.Printf("matching_projection_pages=%d\n", pages)
		prompt, err := source.StagePromptScope(ctx, files, ref, thread.String())
		if err != nil {
			return err
		}
		fmt.Printf("matching_prompt_counts=%v bytes=%d sha256=%x\n", prompt.Records, prompt.Bytes, prompt.Digest)
		return nil
	})
	if err != nil {
		return err
	}
	if err := errors.Join(files.Close(ctx), catalog.Close()); err != nil {
		return err
	}
	pathToFence := filepath.Join(directory, "catalog.sqlite")
	location := threadsqlite.Source{CatalogPath: pathToFence, Directory: pathToFence + ".d", Limit: 3, MaxIdle: 1, IdleTTL: time.Minute}
	options := runtime.Options{Storage: storage.NewSource(location), DeferExecution: true, EnableParentEvents: true}
	bootstrap, err := runtime.Open(ctx, options)
	if err != nil {
		return err
	}
	if err := bootstrap.Shutdown(ctx); err != nil {
		return err
	}
	backend, err := location.Open(ctx)
	if err != nil {
		return err
	}
	err = cutover.WithSource(ctx, path, func(source *cutover.Source) error {
		target := backend.(*threadsqlite.Backend)
		if err := source.StartImport(ctx, target); err != nil {
			return err
		}
		order, err := source.ImportOrder(ctx)
		if err != nil {
			return fmt.Errorf("import inventory: %w", err)
		}
		full := os.Getenv("OFFLINE_FULL_REHEARSAL") == "1"
		imported, scopes, deleted, retiredScopes := 0, 0, 0, 0
		for index, task := range order {
			if !full && task.Thread != thread {
				continue
			}
			if task.Tombstone != nil {
				if err := source.ImportDeleted(ctx, target, task); err != nil {
					return err
				}
				deleted++
				retiredScopes += len(task.Scopes)
				continue
			}
			if err := source.ImportThread(ctx, target, task.Thread, task.Scopes); err != nil {
				fmt.Fprintf(os.Stderr, "import_failed_position=%d of %d thread_fingerprint=%x\n", index+1, len(order), sha256.Sum256([]byte(task.Thread)))
				return err
			}
			imported++
			scopes += len(task.Scopes)
			if imported%25 == 0 {
				fmt.Printf("imported=%d/%d prompt_scopes=%d\n", imported, len(order), scopes)
			}
		}
		if imported == 0 || full && imported+deleted != len(order) {
			return sessiontree.ErrAuthorityCorrupt
		}
		fmt.Printf("imported=%d inventory=%d prompt_scopes=%d tombstones=%d retired_scopes=%d full_rehearsal=%t\n", imported, len(order), scopes, deleted, retiredScopes, full)
		return nil
	})
	if err := errors.Join(err, backend.Close()); err != nil {
		return err
	}
	for range 2 {
		if err := verifyDirectoryHost(ctx, options, thread); err != nil {
			return err
		}
	}
	return nil
}

func verifyDirectoryHost(ctx context.Context, options runtime.Options, thread identity.ThreadID) (err error) {
	host, err := runtime.Open(ctx, options)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, host.Shutdown(context.WithoutCancel(ctx))) }()
	service, err := host.ThreadService(runtime.AgentFactoryFunc(func(context.Context, runtime.AgentRequest) (*runtime.Agent, error) {
		return nil, errors.New("offline qualification cannot execute a model")
	}))
	if err != nil {
		return err
	}
	state, err := host.Readiness(ctx, thread)
	if err != nil || !state.Ready || !state.DisplayReady {
		return errors.Join(errors.New("imported readiness mismatch"), err)
	}
	page, err := service.(runtime.ThreadItemPageReader).ItemPage(ctx, threadquery.Scope{Authority: "offline-qualification", ThreadIDs: []identity.ThreadID{thread}}, runtime.ThreadItemPageInput{ThreadID: thread, Limit: 20})
	if err != nil {
		return err
	}
	scheduling, err := service.(runtime.ThreadSchedulingReader).SchedulingState(ctx, thread)
	if err != nil {
		return err
	}
	raw, err := json.Marshal([]any{page, scheduling.TurnID.String(), scheduling.Activity, scheduling.LastOutcome, scheduling.QueuedRequestKeys})
	if err != nil {
		return err
	}
	digest := sha256.Sum256(raw)
	if hex.EncodeToString(digest[:]) != os.Getenv("OFFLINE_PAGE_DIGEST") {
		return errors.New("published reader and candidate page differ")
	}
	fmt.Printf("directory_host page_items=%d ready=true sha256=%x\n", len(page.Items), digest)
	return nil
}

func main() {
	if len(os.Args) != 3 {
		os.Exit(2)
	}
	thread, err := identity.ParseThreadID(os.Args[2])
	if err != nil {
		os.Exit(2)
	}
	limit := 30 * time.Minute
	if os.Getenv("OFFLINE_FULL_REHEARSAL") == "1" {
		limit = 12 * time.Hour
	}
	ctx, cancel := context.WithTimeout(context.Background(), limit)
	defer cancel()
	if err := rehearse(ctx, os.Args[1], thread); err != nil {
		var failure *cutover.ImportFailure
		if errors.As(err, &failure) {
			fmt.Fprintln(os.Stderr, failure.Error())
		}
		fmt.Fprintf(os.Stderr, "journal rehearsal failed: corrupt=%t unprepared=%t timed_out=%t\n", errors.Is(err, sessiontree.ErrAuthorityCorrupt), errors.Is(err, spi.ErrMigrationRequired), errors.Is(err, context.DeadlineExceeded))
		os.Exit(1)
	}
}

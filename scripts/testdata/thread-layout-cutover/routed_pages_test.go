//go:build floret_layout_cutover

package cutover

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/session"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/internal/threadsqlite"
	"github.com/floegence/floret/v7/runtime"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/threadquery"
)

func TestOfflineOneHostReadsTwoPublishedThreadFiles(t *testing.T) {
	ctx := t.Context()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "writer.sqlite")
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
	defer backend.Close()
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
	threads := []identity.ThreadID{"one", "two"}
	for _, thread := range threads {
		if _, err := repo.CreateThread(ctx, sessiontree.ThreadMeta{ID: thread.String()}); err != nil {
			t.Fatal(err)
		}
		if _, err := repo.Append(ctx, sessiontree.Entry{ThreadID: thread.String(), TurnID: "turn-" + thread.String(), RunID: "run-" + thread.String(), Type: sessiontree.EntryUserMessage, Message: session.Message{Role: session.User, Content: thread.String()}}, sessiontree.AppendOptions{ID: thread.String() + "-item"}); err != nil {
			t.Fatal(err)
		}
		if err := repo.PrepareDisplayThread(ctx, thread.String()); err != nil {
			t.Fatal(err)
		}
	}
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
	openReader := func(source storage.Source) (*runtime.Host, runtime.ThreadItemPageReader) {
		t.Helper()
		owner, err := runtime.Open(ctx, runtime.Options{Storage: source, DeferExecution: true})
		if err != nil {
			t.Fatal(err)
		}
		service, err := owner.ThreadService(runtime.AgentFactoryFunc(func(context.Context, runtime.AgentRequest) (*runtime.Agent, error) {
			t.Error("page read dispatched execution")
			return nil, errors.New("unexpected execution")
		}))
		if err != nil {
			t.Fatal(err)
		}
		return owner, service.(runtime.ThreadItemPageReader)
	}
	readPage := func(service runtime.ThreadItemPageReader, allowed, selected identity.ThreadID) (runtime.ThreadItemPage, error) {
		return service.ItemPage(ctx, threadquery.Scope{Authority: "reader", ThreadIDs: []identity.ThreadID{allowed}}, runtime.ThreadItemPageInput{ThreadID: selected})
	}
	baseline, control := openReader(storage.SQLite(path))
	for _, thread := range threads {
		if page, err := readPage(control, thread, thread); err != nil || len(page.Items) != 1 || page.Items[0].Text != thread.String() {
			t.Fatalf("single-file control for %s: %v", thread, err)
		}
	}
	if err := baseline.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	source := threadsqlite.Source{CatalogPath: filepath.Join(root, "catalog.sqlite"), Directory: root, Limit: 1, MaxIdle: 0, IdleTTL: time.Minute}
	catalog, err := threadsqlite.CreateCatalog(ctx, source.CatalogPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.Close(); err != nil {
		t.Fatal(err)
	}
	fresh, freshReader := openReader(storage.NewSource(source))
	createInput := runtime.CreateThreadInput{RequestKey: "durable-root"}
	create := func(reader runtime.ThreadItemPageReader) runtime.ThreadCommandAck {
		t.Helper()
		commands := reader.(runtime.ThreadCommandService)
		ack, err := commands.CreateAck(ctx, createInput)
		if err != nil || ack.ThreadID == "" || ack.RequestKey != createInput.RequestKey {
			t.Fatalf("create: %+v, %v", ack, err)
		}
		conflicting := createInput
		conflicting.TaskName = "different"
		if _, err := commands.CreateAck(ctx, conflicting); !errors.Is(err, runtime.ErrRequestConflict) {
			t.Fatalf("conflicting creation: %v", err)
		}
		page, err := readPage(reader, ack.ThreadID, ack.ThreadID)
		if err != nil || page.ThreadID != ack.ThreadID || len(page.Items) != 0 {
			t.Fatalf("empty thread: %+v, %v", page, err)
		}
		return ack
	}
	created := create(freshReader)
	title := runtime.SetTitleInput{ThreadID: created.ThreadID, Title: "Joined files", RequestKey: "durable-title"}
	if _, err := freshReader.(runtime.ThreadCommandService).SetTitleAck(ctx, title); err != nil {
		t.Fatalf("set title: %v", err)
	}
	if replay := create(freshReader); replay != created {
		t.Fatalf("duplicate create: %+v != %+v", replay, created)
	}
	if err := fresh.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	physical, err := source.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer physical.Close()
	destination := physical.(*threadsqlite.Backend)
	if err := WithSource(ctx, path, func(source *Source) error {
		if err := source.StartImport(ctx, destination); err != nil {
			return err
		}
		for _, thread := range threads {
			if err := source.ImportThread(ctx, destination, thread, nil); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := physical.Close(); err != nil {
		t.Fatal(err)
	}
	reader, service := openReader(storage.NewSource(source))
	if replay := create(service); replay != created {
		t.Fatalf("duplicate create after restart: %+v != %+v", replay, created)
	}
	listed, err := service.(runtime.ThreadService).List(ctx, runtime.ThreadScope{})
	titles := map[identity.ThreadID]string{}
	for _, thread := range listed {
		titles[thread.ID] = thread.Title
	}
	if err != nil || len(listed) != 3 || titles[created.ThreadID] != title.Title {
		t.Fatalf("title after restart: %+v, %v", listed, err)
	}
	read := func(allowed, selected identity.ThreadID) (runtime.ThreadItemPage, error) {
		return readPage(service, allowed, selected)
	}
	for _, thread := range []identity.ThreadID{"one", "two", "one"} {
		page, err := read(thread, thread)
		if err != nil || page.ThreadID != thread || len(page.Items) != 1 || page.Items[0].Text != thread.String() {
			t.Fatalf("incorrect page for %s: %v", thread, err)
		}
	}
	if _, err := read("one", "two"); !errors.Is(err, threadquery.ErrRecordUnavailable) {
		t.Fatalf("unauthorized page: %v", err)
	}
	if err := reader.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	catalog, err = threadsqlite.OpenCatalog(ctx, source.CatalogPath)
	if err != nil {
		t.Fatal(err)
	}
	files, err := threadsqlite.NewFiles(root, catalog.Store, 1, 0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	previous, err := catalog.Resolve(ctx, "one")
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.Publish(ctx, files, "delete", []threadsqlite.Replacement{{Thread: "one", Expected: previous.File}}); err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(files.Close(ctx), catalog.Close()); err != nil {
		t.Fatal(err)
	}
	reader, service = openReader(storage.NewSource(source))
	defer reader.Shutdown(context.Background())
	if _, err := read("one", "one"); !errors.Is(err, runtime.ErrThreadDeleted) {
		t.Fatalf("deleted page returned %v", err)
	}
	if page, err := read("two", "two"); err != nil || len(page.Items) != 1 {
		t.Fatalf("unrelated page changed: %v", err)
	}
}

//go:build floret_layout_cutover

package main

import (
	"bufio"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/floegence/floret/v7/internal/threadsqlite"
	"github.com/floegence/floret/v7/runtime"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestOfflineInstallRecovery(t *testing.T) {
	ctx := t.Context()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	live := filepath.Join(root, "threads.sqlite")
	owner, err := threadsqlite.CreateCatalog(ctx, live)
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	location := threadsqlite.Source{CatalogPath: live, Directory: live + ".d", Limit: 2, MaxIdle: 1, IdleTTL: time.Minute}
	open := func(src storage.Source) (*runtime.Host, runtime.ThreadService) {
		t.Helper()
		host, err := runtime.Open(ctx, runtime.Options{Storage: src, DeferExecution: true})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { host.Shutdown(context.Background()) })
		service, err := host.ThreadService(runtime.AgentFactoryFunc(func(context.Context, runtime.AgentRequest) (*runtime.Agent, error) {
			t.Error("unexpected execution")
			return nil, spi.ErrInvalidArgument
		}))
		if err != nil {
			t.Fatal(err)
		}
		return host, service
	}
	host, service := open(storage.NewSource(location))
	thread, err := service.Create(ctx, runtime.CreateThreadInput{RequestKey: "kept"})
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot := filepath.Join(root, "snapshot")
	if raw, err := exec.CommandContext(ctx, "python3", "snapshot.py", live, snapshot).CombinedOutput(); err != nil {
		t.Fatalf("snapshot: %v: %s", err, raw)
	}
	stage := filepath.Join(root, "stage")
	if err := prepare(ctx, live, stage); err == nil {
		t.Fatal("accepted unsealed input")
	}
	if err := prepare(ctx, filepath.Join(snapshot, filepath.Base(live)), stage); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{live, filepath.Join(stage, filepath.Base(live))} {
		err := func() error {
			locker := exec.CommandContext(ctx, "python3", "-c", `import fcntl,sys; f=open(sys.argv[1],'rb'); fcntl.lockf(f,fcntl.LOCK_SH,512,0x40000000); print('locked',flush=True); sys.stdin.read()`, path)
			input, err := locker.StdinPipe()
			if err != nil {
				return err
			}
			output, err := locker.StdoutPipe()
			if err != nil {
				return err
			}
			if err := locker.Start(); err != nil {
				return err
			}
			defer func() {
				input.Close()
				if err := locker.Wait(); err != nil {
					t.Error(err)
				}
			}()
			line, err := bufio.NewReader(output).ReadString('\n')
			if err != nil || line != "locked\n" {
				return errors.Join(errors.New("missing independent process lock"), err)
			}
			err = install(ctx, live, stage, nil)
			if !errors.Is(err, syscall.EAGAIN) && !errors.Is(err, syscall.EACCES) {
				return errors.Join(errors.New("install did not refuse an active reader"), err)
			}
			return nil
		}()
		if err != nil {
			t.Fatal(err)
		}
	}
	stopped, cancel := context.WithCancel(ctx)
	cancel()
	if err := install(stopped, live, stage, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	extra := filepath.Join(live+".d", "unexpected")
	if err := os.WriteFile(extra, []byte("unclassified"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := install(ctx, live, stage, nil); !errors.Is(err, spi.ErrConflict) {
		t.Fatalf("source changed: %v", err)
	}
	if err := os.Remove(extra); err != nil {
		t.Fatal(err)
	}
	stop := syscall.ENOSPC
	if err := install(ctx, live, stage, func() error { return stop }); !errors.Is(err, stop) {
		t.Fatal(err)
	}
	for _, openCatalog := range []func(context.Context, string) (*threadsqlite.Catalog, error){threadsqlite.OpenCatalog, threadsqlite.OpenMemberCatalog} {
		catalog, err := openCatalog(ctx, live)
		if catalog != nil {
			catalog.Close()
		}
		if !errors.Is(err, threadsqlite.ErrFileFormat) {
			t.Fatalf("mixed format opened: %v", err)
		}
	}
	members, err := filepath.Glob(filepath.Join(live+".d", "??", "*.sqlite"))
	if err != nil || len(members) != 1 {
		t.Fatal("missing fixture member", err)
	}
	moved := filepath.Join(root, "held")
	if err := os.Rename(members[0], moved); err != nil {
		t.Fatal(err)
	}
	if err := install(ctx, live, stage, nil); !errors.Is(err, spi.ErrConflict) {
		t.Fatal("published incomplete candidate", err)
	}
	if err := os.Rename(moved, members[0]); err != nil {
		t.Fatal(err)
	}
	held := filepath.Join(root, "locked-entrance")
	if err := install(ctx, live, stage, func() error {
		if err := os.Rename(live, held); err != nil {
			return err
		}
		raw, err := os.ReadFile(held)
		if err != nil {
			return err
		}
		return os.WriteFile(live, raw, 0600)
	}); !errors.Is(err, spi.ErrConflict) {
		t.Fatal("exchanged an unlocked replacement", err)
	}
	if err := os.Rename(held, live); err != nil {
		t.Fatal(err)
	}
	if err := install(ctx, live, stage, nil); err != nil {
		t.Fatal(err)
	}
	if err := install(ctx, live, stage, nil); !errors.Is(err, spi.ErrConflict) {
		t.Fatal("replayed installation", err)
	}
	if err := verifyWindow(ctx, live, thread.ThreadID.String()); err != nil {
		t.Fatal("public cold subscription", err)
	}
	host, service = open(storage.ThreadSQLite(live))
	view, err := service.View(ctx, thread.ThreadID)
	if err != nil || view.ThreadID != thread.ThreadID {
		t.Fatal("unreadable installed thread", err)
	}
	if err := host.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	for _, directory := range []string{stage, snapshot} {
		location.CatalogPath = filepath.Join(directory, filepath.Base(live))
		location.Directory = location.CatalogPath + ".d"
		host, service = open(storage.NewSource(location))
		if _, err := service.View(ctx, thread.ThreadID); err != nil {
			t.Fatalf("cannot restore previous store: %v", err)
		}
		if err := host.Shutdown(ctx); err != nil {
			t.Fatal(err)
		}
	}
}

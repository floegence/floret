//go:build darwin && floret_layout_cutover

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
	"github.com/floegence/floret/v7/internal/threadsqlite"
	"github.com/floegence/floret/v7/runtime"
	cutover "github.com/floegence/floret/v7/scripts/testdata/thread-layout-cutover"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/threadquery"
	"golang.org/x/sys/unix"
)

type fileIdentity struct {
	Device                  int32
	Inode                   uint64
	Size, Modified, Changed int64
}

func identityOf(entry unix.Stat_t) fileIdentity {
	return fileIdentity{entry.Dev, entry.Ino, entry.Size, entry.Mtim.Nano(), entry.Ctim.Nano()}
}

func checkSource(path string, descriptor int) (fileIdentity, error) {
	var pathStat, fdStat unix.Stat_t
	if err := unix.Lstat(path, &pathStat); err != nil {
		return fileIdentity{}, err
	}
	if err := unix.Fstat(descriptor, &fdStat); err != nil {
		return fileIdentity{}, err
	}
	if identityOf(pathStat) != identityOf(fdStat) || fdStat.Nlink != 1 || fdStat.Mode&unix.S_IFMT != unix.S_IFREG || fdStat.Size == 0 {
		return fileIdentity{}, spi.ErrConflict
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err := os.Lstat(path + suffix); !errors.Is(err, os.ErrNotExist) {
			return fileIdentity{}, errors.Join(spi.ErrConflict, err)
		}
	}
	return identityOf(fdStat), nil
}

func readWindow(ctx context.Context, source storage.Source, thread identity.ThreadID) (digest string, err error) {
	host, err := runtime.Open(ctx, runtime.Options{Storage: source, DeferExecution: true, EnableParentEvents: true})
	if err != nil {
		return "", err
	}
	defer func() { err = errors.Join(err, host.Shutdown(context.WithoutCancel(ctx))) }()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		status, err := host.StorageMigrationStatus(ctx)
		if err != nil || status.State == runtime.StorageMigrationFailed || status.State == runtime.StorageMigrationCancelled {
			return "", errors.Join(errors.New("offline source not ready"), err)
		}
		if status.State == runtime.StorageMigrationReady {
			break
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-ticker.C:
		}
	}
	if err := host.PrepareDisplayThread(ctx, thread); err != nil {
		return "", fmt.Errorf("preparing display: %w", err)
	}
	service, err := host.ThreadService(runtime.AgentFactoryFunc(func(context.Context, runtime.AgentRequest) (*runtime.Agent, error) {
		return nil, errors.New("offline conversion cannot execute")
	}))
	if err != nil {
		return "", err
	}
	page, err := service.(runtime.ThreadItemPageReader).ItemPage(ctx, threadquery.Scope{Authority: "offline-qualification", ThreadIDs: []identity.ThreadID{thread}}, runtime.ThreadItemPageInput{ThreadID: thread, Limit: 20})
	if err != nil || page.ThreadID != thread {
		return "", errors.Join(errors.New("verification thread not readable"), err)
	}
	schedule, err := service.(runtime.ThreadSchedulingReader).SchedulingState(ctx, thread)
	if err != nil {
		return "", fmt.Errorf("reading schedule: %w", err)
	}
	raw, err := json.Marshal([]any{page, schedule.TurnID.String(), schedule.Activity, schedule.LastOutcome, schedule.QueuedRequestKeys})
	if err != nil {
		return "", err
	}
	checksum := sha256.Sum256(raw)
	return hex.EncodeToString(checksum[:]), nil
}

type preparation struct {
	Source                         fileIdentity
	Threads, Roots, Empty, Entries int64
	Tombstones, PromptScopes       int
	ThreadWindow                   string
}

func fill(ctx context.Context, snapshot, fence string, result *preparation) (err error) {
	catalog, err := threadsqlite.CreateCatalog(ctx, fence)
	if err != nil {
		return err
	}
	if err := catalog.Close(); err != nil {
		return err
	}
	host, err := runtime.Open(ctx, runtime.Options{Storage: formatSix(fence), DeferExecution: true, EnableParentEvents: true})
	if err != nil {
		return err
	}
	if err := host.Shutdown(ctx); err != nil {
		return err
	}
	physical, err := (threadsqlite.Source{CatalogPath: fence, Directory: fence + ".d", Limit: 3, MaxIdle: 1, IdleTTL: time.Minute}).Open(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, physical.Close()) }()
	target := physical.(*threadsqlite.Backend)
	return cutover.WithSource(ctx, snapshot, func(source *cutover.Source) error {
		inventory, err := source.ThreadInventory(ctx)
		if err != nil {
			return err
		}
		result.Threads, result.Roots, result.Entries, result.Empty = inventory.Threads, inventory.Roots, inventory.Entries, inventory.Empty
		if err := source.StartImport(ctx, target); err != nil {
			return err
		}
		order, err := source.ImportOrder(ctx)
		if err != nil {
			return err
		}
		live := int64(0)
		for _, task := range order {
			if task.Tombstone != nil {
				if err := source.ImportDeleted(ctx, target, task); err != nil {
					return err
				}
				result.Tombstones++
				continue
			}
			if err := source.ImportThread(ctx, target, task.Thread, task.Scopes); err != nil {
				return fmt.Errorf("thread %d: %w", live+1, err)
			}
			live++
			result.PromptScopes += len(task.Scopes)
			if live%100 == 0 {
				fmt.Printf("prepared_threads=%d/%d\n", live, inventory.Threads)
			}
		}
		if live != inventory.Threads || int(live)+result.Tombstones != len(order) {
			return spi.ErrConflict
		}
		return nil
	})
}

func syncPath(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(file.Sync(), file.Close())
}

func prepare(ctx context.Context, path, pending string, thread identity.ThreadID, expected string) (err error) {
	for _, check := range []string{path, filepath.Dir(pending)} {
		actual, err := filepath.EvalSymlinks(check)
		if err != nil || !filepath.IsAbs(check) || actual != check {
			return errors.Join(spi.ErrInvalidArgument, err)
		}
	}
	if filepath.Clean(pending) != pending || filepath.Base(pending) == "." {
		return spi.ErrInvalidArgument
	}
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	lock := unix.Flock_t{Type: unix.F_WRLCK, Whence: unix.SEEK_SET, Start: 0x40000000, Len: 512}
	if err := unix.FcntlFlock(uintptr(fd), unix.F_SETLK, &lock); err != nil {
		return err
	}
	initial, err := checkSource(path, fd)
	if err != nil {
		return err
	}
	if err := os.Mkdir(pending, 0700); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, os.RemoveAll(pending))
		}
	}()
	snapshot := filepath.Join(pending, "upgrade-source.sqlite")
	if filepath.Base(path) == filepath.Base(snapshot) {
		return spi.ErrInvalidArgument
	}
	if err := unix.Clonefile(path, snapshot, unix.CLONE_NOFOLLOW); err != nil {
		return err
	}
	if err := os.Chmod(snapshot, 0600); err != nil {
		return err
	}
	var cloneStat unix.Stat_t
	if err := unix.Lstat(snapshot, &cloneStat); err != nil || cloneStat.Ino == initial.Inode || cloneStat.Dev != initial.Device || cloneStat.Size != initial.Size {
		return errors.Join(spi.ErrConflict, err)
	}
	if err := cutover.WithSource(ctx, snapshot, func(*cutover.Source) error { return nil }); err != nil {
		return err
	}
	observed, err := readWindow(ctx, storage.SQLite(snapshot), thread)
	if err != nil || observed != expected {
		return errors.Join(errors.New("source window differs from approved reference"), err)
	}
	fence := filepath.Join(pending, filepath.Base(path))
	result := preparation{Source: initial, ThreadWindow: observed}
	if err := fill(ctx, snapshot, fence, &result); err != nil {
		return err
	}
	for range 2 {
		digest, err := readWindow(ctx, formatSix(fence), thread)
		if err != nil || digest != observed {
			return errors.Join(errors.New("prepared store window differs"), err)
		}
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err := os.Lstat(snapshot + suffix); !errors.Is(err, os.ErrNotExist) {
			return errors.Join(spi.ErrConflict, err)
		}
	}
	if err := os.Remove(snapshot); err != nil {
		return err
	}
	final, err := checkSource(path, fd)
	if err != nil || final != initial {
		return errors.Join(spi.ErrConflict, err)
	}
	raw, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	receipt := filepath.Join(pending, "prepared.json")
	if err := os.WriteFile(receipt, raw, 0600); err != nil {
		return err
	}
	if err := errors.Join(syncPath(receipt), syncPath(pending), syncPath(filepath.Dir(pending))); err != nil {
		return err
	}
	fmt.Printf("prepared=true activated=false threads=%d entries=%d prompt_scopes=%d tombstones=%d\n", result.Threads, result.Entries, result.PromptScopes, result.Tombstones)
	return nil
}

func main() {
	if len(os.Args) == 4 && os.Args[1] == "install" {
		if err := install(context.Background(), os.Args[2], os.Args[3], nil); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Println("installed=true previous_source_retained_until_desktop_verification=true")
		return
	}
	operation := prepare
	if len(os.Args) == 6 && os.Args[1] == "seal" {
		operation = seal
		os.Args = os.Args[1:]
	}
	if len(os.Args) != 5 {
		fmt.Fprintln(os.Stderr, "usage: prepare source.sqlite new-staging-directory thread-id expected-window-sha256")
		os.Exit(2)
	}
	thread, err := identity.ParseThreadID(os.Args[3])
	digest, decodeErr := hex.DecodeString(os.Args[4])
	if err != nil || decodeErr != nil || len(digest) != sha256.Size {
		os.Exit(2)
	}
	if err := operation(context.Background(), os.Args[1], os.Args[2], thread, hex.EncodeToString(digest)); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

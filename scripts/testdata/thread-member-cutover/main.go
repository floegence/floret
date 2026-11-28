//go:build floret_layout_cutover

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"syscall"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/internal/storage"
	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/internal/threadsqlite"
	"github.com/floegence/floret/v7/internal/threadsqlite/member"
	"github.com/floegence/floret/v7/runtime"
	publicstorage "github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
	"github.com/floegence/floret/v7/threadquery"
)

type sealedFile struct {
	Name   string `json:"name"`
	Size   int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}
type preparation struct {
	Format            int `json:"format"`
	Threads           int `json:"threads"`
	Deleted           int `json:"tombstones"`
	Source, Candidate []sealedFile
}

func validateFile(path string) (os.FileInfo, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !info.Mode().IsRegular() || !ok || stat.Nlink != 1 {
		return nil, threadsqlite.ErrFileIdentity
	}
	return info, nil
}

func layoutFiles(ctx context.Context, path string, members map[identity.ThreadID]string, catalog string) ([]sealedFile, error) {
	wanted := map[string]bool{path: true, catalog: true}
	for _, file := range members {
		if wanted[file] {
			return nil, threadsqlite.ErrFileIdentity
		}
		wanted[file] = true
	}
	area := path + ".d"
	var files []sealedFile
	check := func(name string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !wanted[name] {
			return fmt.Errorf("unexpected file in layout: %w", threadsqlite.ErrFileIdentity)
		}
		info, err := validateFile(name)
		if err != nil {
			return err
		}
		reader, err := os.Open(name)
		if err != nil {
			return err
		}
		h := sha256.New()
		size, err := io.Copy(h, reader)
		err = errors.Join(err, reader.Close())
		if err != nil {
			return err
		}
		after, err := validateFile(name)
		if err != nil || !os.SameFile(info, after) || size != after.Size() || size != info.Size() || after.ModTime() != info.ModTime() {
			return errors.Join(threadsqlite.ErrFileIdentity, err)
		}
		relative, err := filepath.Rel(filepath.Dir(path), name)
		if err != nil {
			return err
		}
		files = append(files, sealedFile{relative, size, hex.EncodeToString(h.Sum(nil))})
		delete(wanted, name)
		return nil
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err := os.Lstat(path + suffix); !errors.Is(err, fs.ErrNotExist) {
			return nil, errors.Join(threadsqlite.ErrFileIdentity, err)
		}
	}
	if err := check(path); err != nil {
		return nil, err
	}
	err := filepath.WalkDir(area, func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if name == filepath.Join(area, ".DS_Store") {
			_, err := validateFile(name)
			return err
		}
		return check(name)
	})
	if err != nil || len(wanted) != 0 {
		return nil, errors.Join(threadsqlite.ErrFileIdentity, err)
	}
	return files, nil
}

func convert(ctx context.Context, old, candidate *threadsqlite.Backend, id identity.ThreadID, original string) error {
	ref, err := old.Catalog.Resolve(ctx, id)
	if err != nil {
		return err
	}
	if err := candidate.Files.Create(ctx, ref); err != nil {
		return err
	}
	return threadsqlite.ViewRecordFile(ctx, original, old.Catalog.Store, ref, func(source spi.ReadTx) error {
		var expected sessiontree.MemberOutputInventory
		if err := candidate.UpdateThreadRecords(ctx, id, func(tx spi.WriteTx) error {
			journal := member.From(tx)
			if _, err := sessiontree.StageMemberJournal(ctx, source, journal); err != nil {
				return fmt.Errorf("history: %w", err)
			}
			var err error
			expected, err = sessiontree.StageMemberOutputs(ctx, source, journal, time.Now())
			if err != nil {
				return fmt.Errorf("outputs: %w", err)
			}
			if err := storage.StageMemberPrompts(ctx, source, journal); err != nil {
				return fmt.Errorf("provider replay: %w", err)
			}
			return sessiontree.StageMemberReadItems(ctx, source, tx)
		}); err != nil {
			return err
		}
		return candidate.ViewThreadRecords(ctx, storagebridge.ThreadRead{Thread: id}, func(tx spi.ReadTx) error {
			journal := member.From(tx)
			if _, err := sessiontree.VerifyMemberJournal(ctx, source, journal); err != nil {
				return err
			}
			actual, err := sessiontree.VerifyMemberOutputs(ctx, source, journal)
			if err != nil || actual != expected {
				return errors.Join(sessiontree.ErrAuthorityCorrupt, err)
			}
			if _, err := storage.VerifyMemberPrompts(ctx, source, journal); err != nil {
				return err
			}
			if _, _, err := sessiontree.VerifyMemberProviderProjection(ctx, source, journal, ""); err != nil {
				return err
			}
			_, err = runtime.VerifyOfflineItemWindows(ctx, source, tx, id.String())
			return err
		})
	})
}

func prepare(ctx context.Context, sourcePath, pending string) (err error) {
	for _, path := range []string{sourcePath, pending} {
		parent, err := filepath.EvalSymlinks(filepath.Dir(path))
		if err != nil || !filepath.IsAbs(path) || filepath.Clean(path) != path || parent != filepath.Dir(path) {
			return errors.Join(spi.ErrInvalidArgument, err)
		}
	}
	inside, err := filepath.Rel(sourcePath+".d", pending)
	if err != nil || pending == sourcePath || inside == "." || filepath.IsLocal(inside) {
		return errors.Join(spi.ErrInvalidArgument, err)
	}
	proofPath := filepath.Join(filepath.Dir(sourcePath), "snapshot.json")
	if _, err := validateFile(proofPath); err != nil {
		return err
	}
	raw, err := os.ReadFile(proofPath)
	if err != nil {
		return err
	}
	var snapshot struct {
		Files map[string]sealedFile `json:"files"`
	}
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return err
	}
	if _, ok := snapshot.Files[filepath.Base(sourcePath)]; !ok {
		return spi.ErrConflict
	}
	if err := os.Mkdir(pending, 0700); err != nil {
		return err
	}
	destination := filepath.Join(pending, filepath.Base(sourcePath))
	physical, err := (threadsqlite.Source{CatalogPath: sourcePath, Directory: sourcePath + ".d", Limit: 4, MaxIdle: 1, IdleTTL: time.Minute}).Open(ctx)
	if err != nil {
		return err
	}
	source := physical.(*threadsqlite.Backend)
	defer func() { err = errors.Join(err, source.Close()) }()
	if err := source.View(ctx, runtime.CheckOfflineLayoutLogicalSchema); err != nil {
		return err
	}
	members, oldCatalog, deleted, err := source.OfflineMembers(ctx)
	if err != nil {
		return err
	}
	candidate, err := threadsqlite.PrepareMemberCatalog(ctx, source, destination)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, candidate.Close()) }()
	converted, newCatalog, newDeleted, err := candidate.OfflineMembers(ctx)
	if err != nil || len(converted) != len(members) || deleted != newDeleted {
		return errors.Join(spi.ErrConflict, err)
	}
	var failures []error
	for position, id := range slices.Sorted(maps.Keys(members)) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if converted[id] == "" {
			return threadsqlite.ErrFileIdentity
		}
		if err := convert(ctx, source, candidate, id, members[id]); err != nil {
			failure := fmt.Errorf("member %s (%d/%d): %w", id, position+1, len(members), err)
			fmt.Fprintln(os.Stderr, failure)
			failures = append(failures, failure)
			continue
		}
		fmt.Printf("verified_member=%d/%d\n", position+1, len(members))
	}
	if err := errors.Join(failures...); err != nil {
		return err
	}
	if err := errors.Join(candidate.Close(), source.Close()); err != nil {
		return err
	}
	original, err := layoutFiles(ctx, sourcePath, members, oldCatalog)
	if err != nil {
		return err
	}
	if len(original) != len(snapshot.Files) {
		return spi.ErrConflict
	}
	for _, file := range original {
		if snapshot.Files[file.Name] != file {
			return fmt.Errorf("offline source no longer matches snapshot: %w", spi.ErrConflict)
		}
	}
	sealed, err := layoutFiles(ctx, destination, converted, newCatalog)
	if err != nil {
		return err
	}
	report, err := os.OpenFile(filepath.Join(pending, "prepared.json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	err = json.NewEncoder(report).Encode(preparation{2, len(members), deleted, original, sealed})
	if err == nil {
		err = report.Sync()
	}
	return errors.Join(err, report.Close())
}

var installCommand func(context.Context, string, string) error

func verifyWindow(ctx context.Context, path, thread string) (err error) {
	id, err := identity.ParseThreadID(thread)
	if err != nil {
		return err
	}
	if _, err := validateFile(path); err != nil {
		return err
	}
	host, err := runtime.Open(ctx, runtime.Options{Storage: publicstorage.ThreadSQLite(path), DeferExecution: true, EnableParentEvents: true})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, host.Shutdown(context.WithoutCancel(ctx))) }()
	service, err := host.ThreadService(runtime.AgentFactoryFunc(func(context.Context, runtime.AgentRequest) (*runtime.Agent, error) {
		return nil, errors.New("offline reader cannot execute")
	}))
	if err != nil {
		return err
	}
	scope := threadquery.Scope{Authority: "offline-verification", ThreadIDs: []identity.ThreadID{id}}
	sub, err := service.(runtime.ThreadWindowSource).SubscribeWindowUpdates(ctx, scope)
	if err != nil {
		return err
	}
	defer sub.Close()
	if err := sub.SetWindow(ctx, scope, runtime.ThreadWindowInput{ThreadID: id, Limit: 20, PreviewBytes: 2000, ActivityFields: true, IncludeSummary: true, RequestID: "offline"}); err != nil {
		return err
	}
	for {
		update, err := sub.Next(ctx)
		if err != nil {
			return err
		}
		if update.Window == nil {
			continue
		}
		page := update.Window.Value
		if page == nil || page.ThreadID != id || page.RequestID != "offline" || page.Summary == nil || len(page.Items) > 20 {
			return spi.ErrConflict
		}
		fmt.Printf("verified_public_window=%s items=%d has_before=%t\n", id, len(page.Items), page.HasBefore)
		return nil
	}
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	commands := map[string]func(context.Context, string, string) error{"prepare": prepare, "install": installCommand, "verify-window": verifyWindow}
	if len(os.Args) != 4 || commands[os.Args[1]] == nil {
		fmt.Fprintln(os.Stderr, "usage: thread-member-cutover prepare|install FENCE PRIVATE_DIRECTORY; verify-window INSTALLED_FENCE THREAD_ID")
		os.Exit(2)
	}
	if err := commands[os.Args[1]](ctx, os.Args[2], os.Args[3]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

//go:build darwin && floret_layout_cutover

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
	"os"
	"path/filepath"
	"sort"
	"syscall"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/internal/threadsqlite"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
	"golang.org/x/sys/unix"
)

type sealedFile struct {
	Name   string
	Bytes  int64
	SHA256 string
}
type sealedLayout struct {
	Preparation        preparation
	Files              []sealedFile
	LogicalOutputBytes int64
}

func fileDigest(path string) (sealedFile, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return sealedFile{}, err
	}
	file := os.NewFile(uintptr(fd), path)
	defer file.Close()
	before, err := file.Stat()
	if err != nil {
		return sealedFile{}, err
	}
	if !before.Mode().IsRegular() || before.Sys().(*syscall.Stat_t).Nlink != 1 {
		return sealedFile{}, spi.ErrConflict
	}
	digest := sha256.New()
	size, err := io.Copy(digest, file)
	if err != nil {
		return sealedFile{}, err
	}
	after, err := os.Lstat(path)
	if err != nil || !os.SameFile(before, after) || size != before.Size() || size != after.Size() || after.ModTime() != before.ModTime() {
		return sealedFile{}, errors.Join(spi.ErrConflict, err)
	}
	return sealedFile{Bytes: size, SHA256: hex.EncodeToString(digest.Sum(nil))}, nil
}

func seal(ctx context.Context, sourcePath, pending string, thread identity.ThreadID, expected string) error {
	for _, path := range []string{sourcePath, pending} {
		actual, err := filepath.EvalSymlinks(path)
		if err != nil || !filepath.IsAbs(path) || actual != path {
			return errors.Join(spi.ErrInvalidArgument, err)
		}
	}
	info, err := os.Lstat(pending)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0700 {
		return errors.Join(spi.ErrConflict, err)
	}
	fence := filepath.Join(pending, filepath.Base(sourcePath))
	if fence == sourcePath {
		return spi.ErrConflict
	}
	receiptPath := filepath.Join(pending, "prepared.json")
	if _, err := fileDigest(receiptPath); err != nil {
		return err
	}
	raw, err := os.ReadFile(receiptPath)
	if err != nil {
		return err
	}
	var record sealedLayout
	if err := json.Unmarshal(raw, &record.Preparation); err != nil || record.Preparation.ThreadWindow != expected || record.Preparation.Threads <= 0 {
		return errors.Join(spi.ErrConflict, err)
	}
	fd, err := unix.Open(sourcePath, unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	if err := unix.FcntlFlock(uintptr(fd), unix.F_SETLK, &unix.Flock_t{Type: unix.F_WRLCK, Whence: unix.SEEK_SET, Start: 0x40000000, Len: 512}); err != nil {
		return err
	}
	check := func() error {
		current, err := checkSource(sourcePath, fd)
		if err != nil || current != record.Preparation.Source {
			return errors.Join(spi.ErrConflict, err)
		}
		return nil
	}
	if err := check(); err != nil {
		return err
	}
	sealPath := filepath.Join(pending, "sealed.json")
	if _, err := os.Lstat(sealPath); !errors.Is(err, os.ErrNotExist) {
		return errors.Join(os.ErrExist, err)
	}
	area := fence + ".d"
	physical, err := (threadsqlite.Source{CatalogPath: fence, Directory: area, Limit: 3, MaxIdle: 1, IdleTTL: time.Minute}).Open(ctx)
	if err != nil {
		return err
	}
	backend := physical.(*threadsqlite.Backend)
	wanted := map[string]bool{fence: true}
	err = func() error {
		members, catalog, deleted, err := backend.OfflineMembers(ctx)
		if err != nil {
			return err
		}
		if int64(len(members)) != record.Preparation.Threads || deleted != record.Preparation.Tombstones {
			return spi.ErrConflict
		}
		wanted[catalog] = true
		peak := map[string]int64{}
		for id, path := range members {
			if err := ctx.Err(); err != nil {
				return err
			}
			if wanted[path] {
				return spi.ErrConflict
			}
			wanted[path] = true
			if err := backend.UpdateThreadRecords(ctx, id, func(tx spi.WriteTx) error { return sessiontree.PrepareOfflineCharges(ctx, tx, id.String(), peak) }); err != nil {
				return err
			}
		}
		return backend.View(ctx, func(tx spi.ReadTx) error {
			var err error
			record.LogicalOutputBytes, err = sessiontree.VerifyOfflineCharges(tx, peak)
			return err
		})
	}()
	if err := errors.Join(err, physical.Close()); err != nil {
		return err
	}
	digest, err := readWindow(ctx, storage.ThreadSQLite(fence), thread)
	if err != nil || digest != expected {
		return errors.Join(spi.ErrConflict, err)
	}
	err = filepath.WalkDir(pending, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if path == receiptPath {
			return nil
		}
		if !wanted[path] {
			return fmt.Errorf("unrecognized prepared file: %s", filepath.Base(path))
		}
		file, err := fileDigest(path)
		if err != nil {
			return err
		}
		file.Name, err = filepath.Rel(pending, path)
		if err != nil {
			return err
		}
		record.Files = append(record.Files, file)
		delete(wanted, path)
		return nil
	})
	if err != nil || len(wanted) != 0 {
		return errors.Join(spi.ErrConflict, err)
	}
	if err := check(); err != nil {
		return err
	}
	sort.Slice(record.Files, func(left, right int) bool { return record.Files[left].Name < record.Files[right].Name })
	temporary, err := os.CreateTemp(pending, ".sealing-")
	if err != nil {
		return err
	}
	defer os.Remove(temporary.Name())
	err = json.NewEncoder(temporary).Encode(record)
	if err := errors.Join(err, temporary.Sync(), temporary.Close()); err != nil {
		return err
	}
	if err := unix.RenamexNp(temporary.Name(), sealPath, unix.RENAME_EXCL); err != nil {
		return err
	}
	if err := syncPath(pending); err != nil {
		return err
	}
	fmt.Printf("sealed=true activated=false sqlite_files=%d logical_output_bytes=%d\n", len(record.Files), record.LogicalOutputBytes)
	return nil
}

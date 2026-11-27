//go:build darwin && floret_layout_cutover

package main

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/floegence/floret/v7/storage/spi"
	"golang.org/x/sys/unix"
)

func verifySealedTree(ctx context.Context, record sealedLayout, fence, directory string) error {
	expected := map[string]sealedFile{}
	name := filepath.Base(fence)
	for _, file := range record.Files {
		var path string
		switch {
		case file.Name == name:
			path = fence
		case strings.HasPrefix(file.Name, name+".d/"):
			inside := strings.TrimPrefix(file.Name, name+".d/")
			if !filepath.IsLocal(inside) || filepath.Clean(inside) != inside {
				return spi.ErrConflict
			}
			path = filepath.Join(directory, inside)
		default:
			return spi.ErrConflict
		}
		if _, exists := expected[path]; exists {
			return spi.ErrConflict
		}
		expected[path] = file
	}
	if int64(len(expected)) != record.Preparation.Threads+2 {
		return spi.ErrConflict
	}
	check := func(path string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		want, ok := expected[path]
		if !ok {
			return spi.ErrConflict
		}
		actual, err := fileDigest(path)
		if err != nil || want.SHA256 != actual.SHA256 || want.Bytes != actual.Bytes {
			return errors.Join(spi.ErrConflict, err)
		}
		delete(expected, path)
		return nil
	}
	if err := check(fence); err != nil {
		return err
	}
	err := filepath.WalkDir(directory, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		return check(path)
	})
	if err != nil || len(expected) != 0 {
		return errors.Join(spi.ErrConflict, err)
	}
	return nil
}

func install(ctx context.Context, live, pending string, afterDirectory func() error) error {
	for _, path := range []string{live, pending} {
		resolved, err := filepath.EvalSymlinks(path)
		if err != nil || !filepath.IsAbs(path) || path != resolved {
			return errors.Join(spi.ErrInvalidArgument, err)
		}
	}
	info, err := os.Stat(pending)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0700 {
		return errors.Join(spi.ErrConflict, err)
	}
	sealPath := filepath.Join(pending, "sealed.json")
	if _, err := fileDigest(sealPath); err != nil {
		return err
	}
	raw, err := os.ReadFile(sealPath)
	if err != nil {
		return err
	}
	var record sealedLayout
	if err := json.Unmarshal(raw, &record); err != nil {
		return err
	}
	fence := filepath.Join(pending, filepath.Base(live))
	if fence == live {
		return spi.ErrConflict
	}
	fd, err := unix.Open(live, unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	if err := unix.FcntlFlock(uintptr(fd), unix.F_SETLK, &unix.Flock_t{Type: unix.F_WRLCK, Whence: unix.SEEK_SET, Start: 0x40000000, Len: 512}); err != nil {
		return err
	}
	current, err := checkSource(live, fd)
	if err != nil {
		return err
	}
	if current != record.Preparation.Source {
		return errors.New("source identity changed or entrance already switched; do not replay installation")
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err := os.Lstat(fence + suffix); !errors.Is(err, os.ErrNotExist) {
			return errors.Join(spi.ErrConflict, err)
		}
	}
	pendingDir, liveDir := fence+".d", live+".d"
	_, pendingErr := os.Lstat(pendingDir)
	_, liveErr := os.Lstat(liveDir)
	move := pendingErr == nil && errors.Is(liveErr, os.ErrNotExist)
	resume := errors.Is(pendingErr, os.ErrNotExist) && liveErr == nil
	if !move && !resume {
		return errors.Join(spi.ErrConflict, pendingErr, liveErr)
	}
	directory := pendingDir
	if resume {
		directory = liveDir
	}
	if err := verifySealedTree(ctx, record, fence, directory); err != nil {
		return err
	}
	if move {
		if err := unix.RenamexNp(pendingDir, liveDir, unix.RENAME_EXCL); err != nil {
			return err
		}
	}
	if err := errors.Join(syncPath(pending), syncPath(filepath.Dir(live))); err != nil {
		return err
	}
	if afterDirectory != nil {
		if err := afterDirectory(); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	current, err = checkSource(live, fd)
	if err != nil || current != record.Preparation.Source {
		return errors.Join(spi.ErrConflict, err)
	}
	if err := unix.RenamexNp(fence, live, unix.RENAME_SWAP); err != nil {
		return err
	}
	return errors.Join(syncPath(pending), syncPath(filepath.Dir(live)))
}

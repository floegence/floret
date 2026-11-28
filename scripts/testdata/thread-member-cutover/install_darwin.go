//go:build floret_layout_cutover

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/floegence/floret/v7/storage/spi"
	"golang.org/x/sys/unix"
)

func init() {
	installCommand = func(ctx context.Context, live, stage string) error { return install(ctx, live, stage, nil) }
}

func lockFence(path string) (*os.File, error) {
	info, err := validateFile(path)
	if err != nil {
		return nil, err
	}
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	actual, err := file.Stat()
	if err != nil || !os.SameFile(actual, info) {
		return nil, errors.Join(spi.ErrConflict, err, file.Close())
	}
	if err := unix.FcntlFlock(file.Fd(), unix.F_SETLK, &unix.Flock_t{Type: unix.F_WRLCK, Whence: unix.SEEK_SET, Start: 0x40000000, Len: 512}); err != nil {
		return nil, errors.Join(err, file.Close())
	}
	return file, nil
}

func syncDirs(paths ...string) error {
	for _, path := range paths {
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		if err := errors.Join(file.Sync(), file.Close()); err != nil {
			return err
		}
	}
	return nil
}

func checkFencePath(file *os.File) error {
	opened, err := file.Stat()
	if err != nil {
		return err
	}
	named, err := validateFile(file.Name())
	if err != nil || !os.SameFile(opened, named) {
		return errors.Join(spi.ErrConflict, err)
	}
	return nil
}

func checkSeal(ctx context.Context, reader *os.File, proof sealedFile) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	info, err := reader.Stat()
	if err != nil {
		return err
	}
	if proof.Size != info.Size() {
		return spi.ErrConflict
	}
	digest := sha256.New()
	n, err := io.Copy(digest, io.NewSectionReader(reader, 0, info.Size()))
	if err != nil {
		return err
	}
	if n != proof.Size || hex.EncodeToString(digest.Sum(nil)) != proof.SHA256 {
		return spi.ErrConflict
	}
	return nil
}

func verifySealedMembers(ctx context.Context, area, name string, files []sealedFile) error {
	wanted := map[string]sealedFile{}
	for _, file := range files {
		relative, ok := strings.CutPrefix(file.Name, name+".d/")
		if !ok || !filepath.IsLocal(relative) || relative != filepath.Clean(relative) {
			return spi.ErrConflict
		}
		if _, ok := wanted[relative]; ok {
			return spi.ErrConflict
		}
		wanted[relative] = file
	}
	err := filepath.WalkDir(area, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(area, path)
		if err != nil {
			return err
		}
		if _, err := validateFile(path); err != nil {
			return err
		}
		if rel == ".DS_Store" {
			return nil
		}
		proof, ok := wanted[rel]
		if !ok {
			return spi.ErrConflict
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		if err := errors.Join(checkSeal(ctx, file, proof), file.Close()); err != nil {
			return err
		}
		delete(wanted, rel)
		return nil
	})
	if err != nil {
		return err
	}
	if len(wanted) != 0 {
		return spi.ErrConflict
	}
	return nil
}

func install(ctx context.Context, live, stage string, afterDirectory func() error) (err error) {
	for _, path := range []string{live, stage} {
		resolved, err := filepath.EvalSymlinks(path)
		if err != nil || !filepath.IsAbs(path) || path != resolved {
			return errors.Join(spi.ErrInvalidArgument, err)
		}
	}
	info, err := os.Stat(stage)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0700 {
		return errors.Join(spi.ErrConflict, err)
	}
	name := filepath.Base(live)
	candidate := filepath.Join(stage, name)
	if candidate == live {
		return spi.ErrInvalidArgument
	}
	receipt := filepath.Join(stage, "prepared.json")
	if _, err := validateFile(receipt); err != nil {
		return err
	}
	raw, err := os.ReadFile(receipt)
	if err != nil {
		return err
	}
	var state preparation
	if err := json.Unmarshal(raw, &state); err != nil {
		return err
	}
	if state.Format != 2 || state.Threads < 0 || state.Deleted < 0 || len(state.Source) < 2 || len(state.Source) != state.Threads+2 || len(state.Candidate) != len(state.Source) || state.Source[0].Name != name || state.Candidate[0].Name != name || state.Source[0] == state.Candidate[0] {
		return spi.ErrConflict
	}
	old, err := lockFence(live)
	if err != nil {
		return err
	}
	defer old.Close()
	next, err := lockFence(candidate)
	if err != nil {
		return err
	}
	defer next.Close()
	for _, path := range []string{live, candidate} {
		for _, suffix := range []string{"-wal", "-shm", "-journal"} {
			if _, err := os.Lstat(path + suffix); !errors.Is(err, fs.ErrNotExist) {
				return errors.Join(spi.ErrConflict, err)
			}
		}
	}
	if err := checkSeal(ctx, old, state.Source[0]); err != nil {
		return err
	}
	if err := checkSeal(ctx, next, state.Candidate[0]); err != nil {
		return err
	}
	liveDir, nextDir := live+".d", candidate+".d"
	verify := func(source, candidate string) error {
		if err := verifySealedMembers(ctx, source, name, state.Source[1:]); err != nil {
			return err
		}
		return verifySealedMembers(ctx, candidate, name, state.Candidate[1:])
	}
	if err := verify(liveDir, nextDir); err == nil {
		if err := errors.Join(ctx.Err(), checkFencePath(old), checkFencePath(next)); err != nil {
			return err
		}
		if err := unix.RenamexNp(liveDir, nextDir, unix.RENAME_SWAP); err != nil {
			return err
		}
	} else if !errors.Is(err, spi.ErrConflict) {
		return err
	} else if err := verify(nextDir, liveDir); err != nil {
		return err
	}
	if err := syncDirs(stage, filepath.Dir(live)); err != nil {
		return err
	}
	if afterDirectory != nil {
		if err := afterDirectory(); err != nil {
			return err
		}
	}
	if err := errors.Join(ctx.Err(), checkFencePath(old), checkFencePath(next)); err != nil {
		return err
	}
	if err := unix.RenamexNp(candidate, live, unix.RENAME_SWAP); err != nil {
		return err
	}
	return syncDirs(stage, filepath.Dir(live))
}

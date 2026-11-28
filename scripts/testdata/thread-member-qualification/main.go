//go:build floret_layout_cutover

package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"time"

	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/internal/sessiontree"
	"github.com/floegence/floret/v7/internal/storage"
	"github.com/floegence/floret/v7/internal/storagebridge"
	"github.com/floegence/floret/v7/internal/threadsqlite"
	"github.com/floegence/floret/v7/internal/threadsqlite/member"
	"github.com/floegence/floret/v7/runtime"
	"github.com/floegence/floret/v7/storage/spi"
)

func rehearse(ctx context.Context, args []string) (err error) {
	root, err := filepath.EvalSymlinks(os.Getenv("FLOEGENCE_TEST_RUN_DIR"))
	if err != nil || !filepath.IsAbs(root) || len(args) != 3 {
		return spi.ErrInvalidArgument
	}
	thread, err := identity.ParseThreadID(args[2])
	if err != nil {
		return err
	}
	var store, file threadsqlite.FileID
	for index, dst := range []*threadsqlite.FileID{&store, &file} {
		if len(args[index]) != hex.EncodedLen(len(dst)) {
			return spi.ErrInvalidArgument
		}
		if _, err := hex.Decode(dst[:], []byte(args[index])); err != nil {
			return err
		}
	}
	source := filepath.Join(root, "source.sqlite")
	directory, err := os.MkdirTemp(root, "member-qualification-")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, os.RemoveAll(directory)) }()
	path := filepath.Join(directory, "store.sqlite")
	open := func(create bool) (*threadsqlite.Backend, error) {
		owner := threadsqlite.OpenMemberCatalog
		if create {
			owner = threadsqlite.CreateMemberCatalog
		}
		catalog, err := owner(ctx, path)
		if err != nil {
			return nil, err
		}
		files, err := threadsqlite.NewMemberFiles(path+".d", catalog.Store, 2, 0, time.Minute)
		if err != nil {
			return nil, errors.Join(err, catalog.Close())
		}
		return &threadsqlite.Backend{ThreadViews: threadsqlite.ThreadViews{Catalog: catalog, Files: files}}, nil
	}
	return threadsqlite.ViewRecordFile(ctx, source, store, threadsqlite.Reference{Thread: thread, File: file}, func(old spi.ReadTx) error {
		backend, err := open(true)
		if err != nil {
			return err
		}
		start := time.Now()
		var count int
		var outputs sessiontree.MemberOutputInventory
		err = backend.CreateThreadRecords(ctx, storagebridge.ThreadCreation{Thread: thread}, func(tx spi.WriteTx) error {
			journal := member.From(tx)
			count, err = sessiontree.StageMemberJournal(ctx, old, journal)
			if err == nil {
				outputs, err = sessiontree.StageMemberOutputs(ctx, old, journal, start)
			}
			if err != nil {
				return err
			}
			if err := storage.StageMemberPrompts(ctx, old, journal); err != nil {
				return err
			}
			return sessiontree.StageMemberReadItems(ctx, old, tx)
		})
		if err := errors.Join(err, backend.Close()); err != nil {
			return err
		}
		fmt.Printf("journal_entries=%d stage_elapsed=%s\n", count, time.Since(start))
		fmt.Printf("staged_streams=%d artifacts=%d work_links=%d retained_bytes=%d\n", outputs.Streams, outputs.Artifacts, outputs.Bindings, outputs.RetainedBytes)
		start = time.Now()
		reader, err := open(false)
		if err != nil {
			return err
		}
		err = reader.ViewThreadRecords(ctx, storagebridge.ThreadRead{Thread: thread}, func(tx spi.ReadTx) error {
			journal := member.From(tx)
			proof, err := sessiontree.VerifyMemberJournal(ctx, old, journal)
			if err != nil {
				return err
			}
			fmt.Printf("verified_entries=%d history_sha256=%x verify_elapsed=%s published=false\n", proof.Entries, proof.Digest, time.Since(start))
			messages, digest, err := sessiontree.VerifyMemberProviderProjection(ctx, old, journal, "")
			if err != nil {
				return err
			}
			fmt.Printf("verified_model_messages=%d projection_sha256=%x provider_dispatched=false\n", messages, digest)
			checked, err := sessiontree.VerifyMemberOutputs(ctx, old, journal)
			if err != nil || checked != outputs {
				return errors.Join(sessiontree.ErrAuthorityCorrupt, err)
			}
			fmt.Printf("verified_streams=%d verified_artifacts=%d verified_work_links=%d verified_output_bytes=%d\n", checked.Streams, checked.Artifacts, checked.Bindings, checked.RetainedBytes)
			prompts, err := storage.VerifyMemberPrompts(ctx, old, journal)
			if err != nil {
				return err
			}
			fmt.Printf("verified_segments=%d toolsets=%d requests=%d responses=%d replay_bytes=%d replay_sha256=%x\n", prompts.Records[0], prompts.Records[1], prompts.Records[2], prompts.Records[3], prompts.Bytes, prompts.Digest)
			pages, err := runtime.VerifyOfflineItemWindows(ctx, old, tx, thread.String())
			if err != nil {
				return fmt.Errorf("compare reopened reading windows: %w", err)
			}
			fmt.Printf("verified_window_pages=%d\n", pages)
			return nil
		})
		return errors.Join(err, reader.Close())
	})
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	err := rehearse(ctx, os.Args[1:])
	cancel()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

//go:build floret_layout_cutover

package main

import (
	"context"
	"database/sql"
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
	"github.com/floegence/floret/v7/internal/threadsqlite"
	"github.com/floegence/floret/v7/internal/threadsqlite/member"
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
	files, err := threadsqlite.NewMemberFiles(directory, store, 2, 0, time.Minute)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, files.Close(context.Background())) }()
	target, err := threadsqlite.Allocate(thread)
	if err != nil {
		return err
	}
	if err := files.Create(ctx, target); err != nil {
		return err
	}
	return threadsqlite.ViewRecordFile(ctx, source, store, threadsqlite.Reference{Thread: thread, File: file}, func(old spi.ReadTx) error {
		start := time.Now()
		var count int
		var outputs sessiontree.MemberOutputInventory
		if err := files.Update(ctx, target, func(tx *sql.Tx) error {
			journal, err := member.OpenJournal(ctx, tx, thread, false)
			if err != nil {
				return err
			}
			count, err = sessiontree.StageMemberJournal(ctx, old, journal)
			if err == nil {
				outputs, err = sessiontree.StageMemberOutputs(ctx, old, journal, start)
			}
			if err != nil {
				return err
			}
			return storage.StageMemberPrompts(ctx, old, journal)
		}); err != nil {
			return err
		}
		fmt.Printf("journal_entries=%d stage_elapsed=%s\n", count, time.Since(start))
		fmt.Printf("staged_streams=%d artifacts=%d work_links=%d retained_bytes=%d\n", outputs.Streams, outputs.Artifacts, outputs.Bindings, outputs.RetainedBytes)
		start = time.Now()
		return files.View(ctx, target, func(tx *sql.Tx) error {
			journal, err := member.OpenJournal(ctx, tx, thread, false)
			if err != nil {
				return err
			}
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
			return nil
		})
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

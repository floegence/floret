//go:build darwin && floret_layout_cutover

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/floegence/floret/v7/runtime"
	"github.com/floegence/floret/v7/storage"
	"github.com/floegence/floret/v7/storage/spi"
)

func TestPreparationNeverInstallsOrOverwrites(t *testing.T) {
	ctx := t.Context()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "source.sqlite")
	host, err := runtime.Open(ctx, runtime.Options{Storage: storage.SQLite(path), DeferExecution: true, EnableParentEvents: true})
	if err != nil {
		t.Fatal(err)
	}
	service, err := host.ThreadService(runtime.AgentFactoryFunc(func(context.Context, runtime.AgentRequest) (*runtime.Agent, error) {
		return nil, errors.New("must not execute")
	}))
	if err != nil {
		t.Fatal(err)
	}
	thread, err := service.Create(ctx, runtime.CreateThreadInput{RequestKey: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	expected, err := readWindow(ctx, storage.SQLite(path), thread.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	pending := filepath.Join(root, "pending")
	if err := prepare(ctx, path, pending, thread.ThreadID, "invalid"); err == nil {
		t.Fatal("accepted mismatched baseline")
	}
	if _, err := os.Lstat(pending); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("failed conversion published output", err)
	}
	if err := prepare(ctx, path, pending, thread.ThreadID, expected); err != nil {
		t.Fatal(err)
	}
	manifest, err := os.ReadFile(filepath.Join(pending, "prepared.json"))
	if err != nil {
		t.Fatal(err)
	}
	var saved preparation
	if err := json.Unmarshal(manifest, &saved); err != nil || saved.Threads != 1 || saved.Empty != 1 || saved.ThreadWindow != expected {
		t.Fatal("unexpected prepared membership", err)
	}
	if _, err := os.Stat(filepath.Join(pending, "upgrade-source.sqlite")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("source snapshot retained", err)
	}
	if err := prepare(ctx, path, pending, thread.ThreadID, expected); !errors.Is(err, os.ErrExist) {
		t.Fatal("replaced occupied staging", err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(after, before) {
		t.Fatal("original database changed", err)
	}
	if _, err := os.Lstat(path + ".d"); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("installed without release", err)
	}
	if err := seal(ctx, path, pending, thread.ThreadID, expected); err != nil {
		t.Fatal(err)
	}
	if err := seal(ctx, path, pending, thread.ThreadID, expected); !errors.Is(err, os.ErrExist) {
		t.Fatal("overwrote a sealed layout", err)
	}
	stop := errors.New("injected before entry swap")
	if err := install(ctx, path, pending, func() error { return stop }); !errors.Is(err, stop) {
		t.Fatal(err)
	}
	stillOriginal, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(stillOriginal, before) {
		t.Fatal("original changed before swap", err)
	}
	unexpected := filepath.Join(path+".d", "unplanned")
	if err := os.WriteFile(unexpected, []byte("test"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := install(ctx, path, pending, nil); !errors.Is(err, spi.ErrConflict) {
		t.Fatal("accepted altered directory", err)
	}
	if err := os.Remove(unexpected); err != nil {
		t.Fatal(err)
	}
	if err := install(ctx, path, pending, nil); err != nil {
		t.Fatal(err)
	}
	if err := install(ctx, path, pending, nil); err == nil {
		t.Fatal("replayed completed swap")
	}
	installed, err := readWindow(ctx, formatSix(path), thread.ThreadID)
	if err != nil || installed != expected {
		t.Fatal("unreadable installed history", err)
	}
	previous, err := os.ReadFile(filepath.Join(pending, filepath.Base(path)))
	if err != nil || !bytes.Equal(previous, before) {
		t.Fatal("could not verify original", err)
	}
}

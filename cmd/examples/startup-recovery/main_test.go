package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

const (
	helperModeEnv = "FLORET_STARTUP_RECOVERY_HELPER"
	helperDBEnv   = "FLORET_STARTUP_RECOVERY_DB"
)

func TestExample(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "floret.db")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	launcher := func(path string) error {
		command := exec.CommandContext(ctx, os.Args[0], "-test.run=TestInterruptedChild$")
		command.Env = append(os.Environ(), helperModeEnv+"=1", helperDBEnv+"="+path)
		return command.Run()
	}
	if err := runRecovery(ctx, databasePath, launcher); err != nil {
		t.Fatal(err)
	}
}

func TestInterruptedChild(t *testing.T) {
	if os.Getenv(helperModeEnv) != "1" {
		return
	}
	if err := runCrashChild(context.Background(), os.Getenv(helperDBEnv)); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	os.Exit(3)
}

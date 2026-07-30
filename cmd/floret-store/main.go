// Command floret-store performs explicit offline Floret store operations.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/floegence/floret/v3/storage"
)

const usage = "usage: floret-store <preflight-v3|preview-v3|apply-v3> [options]"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if ctx == nil || stdout == nil || stderr == nil {
		return errors.New("floret-store requires context and output streams")
	}
	if len(args) == 0 {
		return errors.New(usage)
	}
	switch args[0] {
	case "preflight-v3":
		return runPreflight(ctx, args[1:], stdout, stderr)
	case "preview-v3":
		return runPlanned(ctx, args[0], args[1:], stdout, stderr, false)
	case "apply-v3":
		return runPlanned(ctx, args[0], args[1:], stdout, stderr, true)
	default:
		return errors.New(usage)
	}
}

func runPreflight(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("preflight-v3", flag.ContinueOnError)
	flags.SetOutput(stderr)
	path := flags.String("path", "", "v2.2 SQLite store path")
	operationID := flags.String("operation-id", "", "joint migration operation identity")
	commitment := flags.String("coordinator-commitment", "", "opaque coordinator commitment")
	if err := flags.Parse(args); err != nil {
		return fmt.Errorf("%s: %w", usage, err)
	}
	if flags.NArg() != 0 || !canonicalArgument(*path) || !canonicalArgument(*operationID) || !canonicalArgument(*commitment) {
		return errors.New(usage)
	}
	plan, err := storage.PreflightV2Migration(ctx, storage.V2MigrationPreflightRequest{
		Path: *path, OperationID: *operationID, CoordinatorCommitment: *commitment,
	})
	if err != nil {
		return err
	}
	return encodeJSON(stdout, plan)
}

func runPlanned(ctx context.Context, command string, args []string, stdout, stderr io.Writer, apply bool) error {
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(stderr)
	path := flags.String("path", "", "v2.2 SQLite store path")
	planPath := flags.String("plan", "", "immutable migration plan JSON path")
	if err := flags.Parse(args); err != nil {
		return fmt.Errorf("%s: %w", usage, err)
	}
	if flags.NArg() != 0 || !canonicalArgument(*path) || !canonicalArgument(*planPath) {
		return errors.New(usage)
	}
	encoded, err := os.ReadFile(*planPath)
	if err != nil {
		return fmt.Errorf("read migration plan: %w", err)
	}
	var plan storage.V2MigrationPlan
	if err := json.Unmarshal(encoded, &plan); err != nil {
		return fmt.Errorf("decode migration plan: %w", err)
	}
	if apply {
		receipt, err := storage.ApplyV2Migration(ctx, storage.V2MigrationApplyRequest{Path: *path, Plan: plan})
		if err != nil {
			return err
		}
		return encodeJSON(stdout, receipt)
	}
	preview, err := storage.PreviewV2Migration(ctx, storage.V2MigrationPreviewRequest{Path: *path, Plan: plan})
	if err != nil {
		return err
	}
	return encodeJSON(stdout, preview)
}

func canonicalArgument(value string) bool {
	return value != "" && value == strings.TrimSpace(value)
}

func encodeJSON(output io.Writer, value any) error {
	encoder := json.NewEncoder(output)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(value)
}
